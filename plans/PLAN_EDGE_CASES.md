# PLAN: Edge Cases — Catalogue, Workarounds & Prioritised Remediation

**Status:** Proposed
**Target milestone:** v0.3.0+
**Last updated:** 2026-06-05

---

## Motivation

pg_trickle's documentation (FAQ, SQL Reference, Architecture) describes 36
discrete edge cases and limitations. Some are fundamental trade-offs,
others are fixable engineering gaps. This document:

1. Catalogues every known edge case in one place.
2. Proposes concrete workarounds or fixes for each.
3. Assigns a priority tier so the team can focus effort where it matters most.

---

## Priority Tiers

| Tier | Meaning | Guidance |
|------|---------|----------|
| **P0** | Data correctness risk — users can silently get wrong results | Fix before v1.0 |
| **P1** | Operational surprise — confusing failure, silent non-behaviour | Fix before v1.0 |
| **P2** | Usability gap — documented but painful; strong workaround exists | Schedule for a minor release |
| **P3** | Accepted trade-off — inherent to the design; document clearly | Keep documentation up to date |

---

## Edge Case Catalogue

### EC-01 — JOIN key change + simultaneous right-side DELETE

| Field | Value |
|-------|-------|
| **Area** | DIFFERENTIAL — JOIN delta |
| **Tier** | **P0** |
| **Impact** | Stale row remains in stream table until next full refresh |
| **Current mitigation** | Adaptive FULL fallback (`pg_trickle.adaptive_full_threshold`) |
| **Documented in** | FAQ § "Known edge cases"; SQL_REFERENCE § "Known Delta Computation Limitations" |
| **Status** | ✅ **IMPLEMENTED** — Part 1 split (R₀ via EXCEPT ALL) in `diff_inner_join`, `diff_left_join`, `diff_full_join` |

**Root cause:** The delta query reads `current_right` after all changes are
applied. When the old join partner is deleted before the delta runs, the
DELETE half of the join finds no partner and is silently dropped.

**Proposed fix (chosen approach: R₀ via EXCEPT ALL — medium-term):**

The short-term threshold adjustment is **not viable**: it trades query
correctness for a heuristic that can still silently miss deletes when
co-occurring changes fall below the threshold.

**Primary fix — R₀ via EXCEPT ALL (4–6 days):**

The delta formula in `src/dvm/operators/join.rs` is:

```
ΔJ = (ΔQ ⋈ R₁) + (L₀ ⋈ ΔR)        [current code]
```

Part 2 already computes `L₀` (pre-change left state) via EXCEPT ALL for
Scan children (`left_part2_source` in `diff_inner_join`). The fix mirrors
this symmetrically for Part 1's right side — splitting Part 1 into:

```
Part 1a: ΔQ_inserts ⋈ R₁           (post-change right — unchanged)
Part 1b: ΔQ_deletes ⋈ R₀           (pre-change right via EXCEPT ALL)
```

`R₀` is computed identically to the existing `L₀`:
```sql
R_current EXCEPT ALL ΔR_inserts UNION ALL ΔR_deletes
```

`build_snapshot_sql()` already exists in `join_common.rs` and is reused
here. The correction term (Part 3) was added for the `L₁` case in Part 2
and is unaffected — Part 2 stays unchanged.

**Implementation plan:**
- Day 1–2: Add `right_part1_source` decision + EXCEPT ALL CTE analogous
  to `use_l0` / `left_part2_source`. Mirror `use_r0` logic for nested
  right children (no SemiJoin, ≤ 2 scan nodes → R₀; otherwise R₁).
- Day 2–3: Split Part 1 SQL into two UNION ALL arms; update row ID hashing
  for Part 1b arm.
- Day 4–5: Integration tests — co-delete scenario, UPDATE-then-delete,
  multi-cycle correctness.
- Day 5–6: TPC-H regression suite (Q07 is the critical path).

**Deferred — pre-image capture from change buffer:**

The long-term approach (using `old_col` values already stored in the
change buffer trigger to reconstruct R₀ from ΔR directly, skipping the
live table scan) is deferred. The `old_col` values **are** already
captured by the CDC trigger (`OLD."col"` → `"old_col"` for UPDATE/DELETE),
but surfacing them through `DiffContext`/`DeltaSource` requires structural
changes to the delta CTE column representation that carry higher risk.
Estimated additional effort: 7–10 days. Schedule for v1.x when the EXCEPT
ALL approach is already correct and benchmarked.

---

### EC-02 — CUBE/ROLLUP expansion limit (> 64 branches)

| Field | Value |
|-------|-------|
| **Area** | DIFFERENTIAL — grouping sets |
| **Tier** | **P3** |
| **Impact** | Creation error |
| **Current mitigation** | Use explicit `GROUPING SETS(…)` |
| **Documented in** | SQL_REFERENCE § "CUBE/ROLLUP Expansion Limit" |
| **Status** | ✅ **Done** — `pg_trickle.max_grouping_set_branches` GUC (default 64, range 1–65536) |

**Proposed fix:** ~~The 64-branch limit is a sensible guard. Keep it but make
the limit configurable via a GUC~~ **Implemented:** `PGS_MAX_GROUPING_SET_BRANCHES`
in `src/config.rs`; parser.rs reads GUC dynamically instead of const.
Documented in CONFIGURATION.md.

---

### EC-03 — Window functions inside expressions

| Field | Value |
|-------|-------|
| **Area** | DIFFERENTIAL — window functions |
| **Tier** | **P2** |
| **Impact** | Creation error |
| **Current mitigation** | Keep window functions as top-level SELECT columns |
| **Documented in** | FAQ § "Window Functions" |
| **Status** | ✅ **IMPLEMENTED** — `rewrite_nested_window_exprs()` subquery-lift in `src/dvm/parser.rs`; helper `collect_all_window_func_nodes()` for recursive window func harvesting; `deparse_select_window_clause()` for WINDOW clause passthrough; exported from `src/dvm/mod.rs`; wired in `src/api.rs` before DISTINCT ON rewrite |

**Implementation: nested-subquery lift from the completed transactional IVM work:**

Window functions nested inside expressions are lifted into a synthetic column
in an inner subquery, then the outer SELECT applies the original expression
with the window function replaced by the synthetic column alias:

```sql
-- Before (rejected):
SELECT id, ABS(ROW_NUMBER() OVER (ORDER BY score) - 5) AS adjusted_rank
FROM players

-- After rewrite (supported):
SELECT id, ABS("__pgt_wf_inner"."__pgt_wf_1" - 5) AS "adjusted_rank"
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY score) AS "__pgt_wf_1"
    FROM players
) "__pgt_wf_inner"
```

The subquery approach is preferred over the earlier CTE approach because a
`WITH` clause produces a `WithQuery` node in the OpTree, requiring the DVM
engine to handle CTEs wrapping window functions — an untested path. The
subquery produces a nested `Scan → Window → Project` chain that the existing
DVM path already handles correctly.

**Test coverage:** 9 E2E tests (`e2e_window_tests.rs`):
- 6 creation-acceptance tests: `test_window_in_case_expression_rejected`,
  `test_window_in_coalesce_rejected`, `test_window_in_arithmetic_rejected`,
  `test_window_in_cast_rejected`, `test_window_deeply_nested_rejected`,
  `test_top_level_window_still_works` (regression).
- 3 data-correctness tests: `test_ec03_case_window_data_correctness` —
  CASE + ROW_NUMBER() produces correct tier labels;
  `test_ec03_arithmetic_window_data_correctness` — ROW_NUMBER() * 10
  verified with a FULL refresh after INSERT;
  `test_ec03_coalesce_window_data_correctness` — COALESCE(SUM() OVER, 0)
  produces correct partition sums.

**Known limitation:** ~~Window-in-expression queries are accepted in
DIFFERENTIAL mode (creation succeeds), but differential *refresh* fails with
a `column st.* does not exist` error.~~ **Fixed (EC-03):** AUTO mode now
correctly falls back to FULL refresh with an informational message. Explicit
DIFFERENTIAL mode emits a `WARNING` at creation time alerting the user that
the pattern will fall back to full recomputation at refresh time. The DVM delta
engine cannot currently resolve the `st.*` alias inside the inner subquery
introduced by the rewrite. FULL refresh works correctly. Differential support
for the rewritten form is deferred.

---

### EC-04 — Non-monotone recursive CTEs rejected

| Field | Value |
|-------|-------|
| **Area** | DIFFERENTIAL — recursive CTEs |
| **Tier** | **P3** |
| **Impact** | Creation error |
| **Current mitigation** | Restructure as non-recursive CTE; or use FULL mode |
| **Documented in** | FAQ § "Recursive CTEs" |

**Proposed fix:** This is a fundamental limitation of semi-naive evaluation
— non-monotone operators in the recursive term can "un-derive" rows,
breaking the incremental fixpoint. **No code fix proposed.** Keep the clear
error message and document the reasoning. Users who need non-monotone
recursion should use FULL mode.

---

### EC-05 — Foreign tables in DIFFERENTIAL mode

| Field | Value |
|-------|-------|
| **Area** | DIFFERENTIAL — foreign data wrappers |
| **Tier** | **P2** |
| **Impact** | Creation error |
| **Current mitigation** | Use FULL mode |
| **Documented in** | SQL_REFERENCE § "Source Tables" |
| **Status** | ✅ **IMPLEMENTED** — error message improved + polling CDC implemented |

**Proposed fix:**

1. ✅ **Short term (Done):** Improved the error message to suggest `FULL` mode
   explicitly and mention the `postgres_fdw` + `IMPORT FOREIGN SCHEMA`
   pattern.
2. ✅ **Medium term (Done):** Polling-based CDC for foreign tables. Enabled via
   `pg_trickle.foreign_table_polling = on` GUC. Creates a snapshot table per
   foreign table source; before each differential refresh, computes EXCEPT ALL
   deltas and populates the standard change buffer. Snapshot is refreshed each
   cycle. Cleanup path drops snapshot tables when STs are removed.

---

### EC-06 — Keyless tables with exact duplicate rows

| Field | Value |
|-------|-------|
| **Area** | DIFFERENTIAL — row identity |
| **Tier** | **P0** |
| **Impact** | Phantom or missed deletes in stream table |
| **Current mitigation** | Add a primary key; add a synthetic unique column; or use FULL mode |
| **Documented in** | FAQ § "Keyless Tables"; SQL_REFERENCE § "Row Identity" |
| **Status** | ✅ **IMPLEMENTED** — Full EC-06 fix: catalog `has_keyless_source` flag, non-unique index for keyless sources, net-counting delta SQL in scan.rs (decompose changes into atomic +1/-1 per content hash, expand via `generate_series`), counted DELETE (ROW_NUMBER matching) in refresh.rs and ivm.rs, plain INSERT (no ON CONFLICT) for keyless sources. WARNING at creation time also implemented. |

**Implementation (completed):**

1. ~~**Short term (P0):** Emit a `WARNING` at `create_stream_table()` time~~ ✅ Done
2. **Full fix (P0):** ✅ Implemented via net-counting approach:
   - **Catalog:** Added `has_keyless_source BOOLEAN NOT NULL DEFAULT FALSE`
     to `pgtrickle.pgt_stream_tables` (set at creation time).
   - **Index:** Non-unique `CREATE INDEX` for keyless sources (instead of
     `CREATE UNIQUE INDEX`), allowing duplicate `__pgt_row_id` values.
   - **Delta SQL (scan.rs):** For keyless tables, decomposes all change
     events into atomic `+1` (INSERT) / `-1` (DELETE) operations per
     content hash. UPDATEs are split into their DELETE-old + INSERT-new
     components. Net counts are computed via `GROUP BY content_hash` +
     `SUM(delta_sign)`, then expanded to the correct number of D/I rows
     using `generate_series`.
   - **Apply (refresh.rs):** For keyless sources, forces explicit DML path
     (cannot use MERGE with non-unique row_ids). Counted DELETE uses
     `ROW_NUMBER` matching to pair delta deletes 1:1 with stream table
     rows. Plain INSERT without NOT EXISTS / ON CONFLICT.
   - **Apply (ivm.rs):** Same counted DELETE + plain INSERT for IMMEDIATE mode.
   - **Upgrade SQL:** `ALTER TABLE ... ADD COLUMN IF NOT EXISTS has_keyless_source`

**Bug fixes (post-implementation):**

- **Guard trigger DELETE cancel:** The DML guard trigger (EC-26) used
  `RETURN NEW` which is NULL for DELETE operations, silently cancelling
  managed-refresh DELETEs on the stream table. Fixed to use
  `IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;`.
- **Keyless aggregate UPDATE no-op:** The keyless UPDATE template was a
  no-op (`SELECT 1 WHERE false`), which is correct for keyless scans but
  wrong for aggregate queries on keyless sources (where `'I'`-action rows
  update existing groups). Fixed to use the normal UPDATE template, which
  naturally matches 0 rows for scan-level keyless deltas.

**Test coverage:** 7 E2E tests (`e2e_keyless_duplicate_tests.rs`) — all
passing. Covers: duplicate rows basic, delete-one-of-duplicates,
update-one-of-duplicates, unique content multi-cycle DML, delete-all-
then-reinsert, mixed DML stress, aggregate with duplicates.

---

### EC-07 — IMMEDIATE mode write serialization

| Field | Value |
|-------|-------|
| **Area** | IMMEDIATE mode — concurrency |
| **Tier** | **P3** |
| **Impact** | Reduced write throughput under high concurrency |
| **Current mitigation** | Use DIFFERENTIAL mode for high-throughput OLTP |
| **Documented in** | FAQ § "IMMEDIATE Mode Trade-offs" |

**Proposed fix:** This is an inherent trade-off of synchronous IVM — you
cannot maintain consistency without serialization. The `IvmLockMode`
already uses lighter `pg_try_advisory_xact_lock` for simple scan chains.
**No code fix proposed.** Document the throughput characteristics in the
performance guide and add a benchmark that quantifies the write-side
overhead so users can make informed mode choices.

---

### EC-08 — IMMEDIATE mode write amplification

| Field | Value |
|-------|-------|
| **Area** | IMMEDIATE mode — latency |
| **Tier** | **P3** |
| **Impact** | Increased per-DML latency |
| **Current mitigation** | Use DIFFERENTIAL mode |
| **Documented in** | FAQ § "IMMEDIATE Mode Trade-offs" |

**Proposed fix:** Inherent trade-off. Potential optimisation: batch multiple
trigger invocations within a single statement into one delta application
(the current implementation already uses statement-level triggers with
transition tables, which provides this batching naturally). Add a benchmark
to `benches/` that measures per-statement overhead for 1, 10, 100, 1000
affected rows.

---

### EC-09 — IMMEDIATE mode unsupported constructs

| Field | Value |
|-------|-------|
| **Area** | IMMEDIATE mode — SQL support |
| **Tier** | **P2** |
| **Impact** | Creation / alter error |
| **Current mitigation** | Use DIFFERENTIAL mode |
| **Documented in** | FAQ § "What SQL features are NOT supported in IMMEDIATE mode?" |
| **Status** | ✅ **Done** — recursive CTEs + TopK now supported in IMMEDIATE mode |

**Proposed fix (recursive CTEs + TopK): ~~deferred to~~ implemented in the completed transactional IVM work.**

Both constructs are now implemented:

- **Recursive CTEs (G10 / Task 5.1):** `check_immediate_support()` changed from
  rejection to warning about stack depth. Semi-naive evaluation proceeds with
  `DeltaSource::TransitionTable`.
- **TopK (G11 / Task 5.2):** `apply_topk_micro_refresh()` in `ivm.rs` implements
  statement-level micro-refresh. Bounded by `pg_trickle.ivm_topk_max_limit` GUC
  (default 1000).
  (default 1000) to prevent inline recomputation latency spikes for large K.

For materialised view sources: use a polling-change-detection wrapper (same
approach as EC-05 for foreign tables). No code fix proposed for this case.

---

### EC-10 — IMMEDIATE mode: no throttling

| Field | Value |
|-------|-------|
| **Area** | IMMEDIATE mode — back-pressure |
| **Tier** | **P3** |
| **Impact** | No rate limiting for high-frequency writes |
| **Current mitigation** | Use DIFFERENTIAL mode with a schedule interval |
| **Documented in** | FAQ § "IMMEDIATE Mode Trade-offs" |

**Proposed fix:** By design — throttling IMMEDIATE would break the
same-transaction consistency guarantee. **No code fix.** Ensure the FAQ
clearly explains that DIFFERENTIAL with a short interval (e.g. `'1s'`) is
the right answer when back-pressure is needed.

---

### EC-11 — Schedule interval shorter than refresh duration

| Field | Value |
|-------|-------|
| **Area** | Scheduler — timing |
| **Tier** | **P1** |
| **Impact** | Perpetual latency spiral; cascading DAG chains fall behind |
| **Current mitigation** | Widen the interval; use IMMEDIATE mode; or split the DAG |
| **Documented in** | FAQ § "Performance and Tuning" |
| **Status** | ✅ **IMPLEMENTED** — `scheduler_falling_behind` NOTIFY alert at 80% threshold |

**Test coverage:** `SchedulerFallingBehind` variant covered in
`test_alert_event_as_str` and `test_alert_event_all_variants_unique`
(monitor.rs unit tests).

**Chosen fix: `scheduler_falling_behind` NOTIFY alert (short term):**

When the refresh duration exceeds 80% of the schedule interval for 3
consecutive cycles, emit a NOTIFY on the channel `pgtrickle_alerts` with a
JSON payload containing the stream table name, average duration, and
configured interval. Operators can LISTEN on the channel and wire it into
their alerting stack.

**Deferred:**
- `auto_backoff` GUC (double effective interval when falling behind) — medium term, schedule for a later sprint.
- `explain_st(name)` recommendation from `pgt_refresh_history` — long term.

---

### EC-12 — No read-your-writes (DIFFERENTIAL/FULL)

| Field | Value |
|-------|-------|
| **Area** | Scheduler — consistency model |
| **Tier** | **P3** |
| **Impact** | Stale reads immediately after write |
| **Current mitigation** | Use IMMEDIATE mode or call `refresh_stream_table()` manually |
| **Documented in** | FAQ § "Consistency" |

**Proposed fix:** Inherent to the scheduled refresh model. **No code fix.**
Consider a convenience function `pgtrickle.write_and_refresh(dml_sql,
st_name)` that executes the DML and triggers a manual refresh in a single
call. Low priority — manual `refresh_stream_table()` already covers this.

---

### EC-13 — Diamond dependency split-version reads

| Field | Value |
|-------|-------|
| **Area** | Diamond dependencies — consistency |
| **Tier** | **P1** |
| **Impact** | Fan-in node reads inconsistent upstream versions |
| **Current mitigation** | `diamond_consistency = 'atomic'` |
| **Documented in** | FAQ § "Diamond Dependencies"; SQL_REFERENCE |
| **Status** | ✅ **FULLY IMPLEMENTED** — default `'atomic'` + scheduler sub-transaction fix |

**Proposed fix:** The `atomic` mode already fixes this. Remaining work:

1. **Short term (P1):** Default `diamond_consistency` to `'atomic'` for new
   stream tables (currently `'none'`). This is a safer default — users who
   don't need atomicity can opt out. ✅ Done.
2. **Medium term:** Add a `health_check()` warning when a diamond graph
   exists but `diamond_consistency = 'none'`.

**Scheduler bug fixed (follow-on):** When EC-13 changed the default to
`'atomic'`, a latent bug surfaced: the scheduler's diamond group path used
`Spi::run("SAVEPOINT …")` which PostgreSQL rejects in background workers
(`SPI_ERROR_TRANSACTION`). This silently skipped every diamond group refresh.
Fixed by replacing the SPI SAVEPOINT calls with
`pg_sys::BeginInternalSubTransaction` / `ReleaseCurrentSubTransaction` /
`RollbackAndReleaseCurrentSubTransaction`, which bypass SPI. See
`src/scheduler.rs`.

---

### EC-14 — Diamond upstream failure skips downstream

| Field | Value |
|-------|-------|
| **Area** | Diamond dependencies — failure handling |
| **Tier** | **P3** |
| **Impact** | Downstream stays stale for one cycle |
| **Current mitigation** | Automatic retry on next cycle |
| **Documented in** | FAQ § "Diamond Dependencies" |

**Proposed fix:** Current behaviour is correct. Add a NOTIFY
`diamond_group_partial_failure` when a group member fails so operators
can investigate. **No urgency** — the automatic retry handles recovery.

---

### EC-15 — SELECT * on source table + DDL

| Field | Value |
|-------|-------|
| **Area** | DDL — schema evolution |
| **Tier** | **P1** |
| **Impact** | Refresh fails; stream table marked `needs_reinit` |
| **Current mitigation** | Use explicit column lists in defining queries |
| **Documented in** | FAQ § "Schema Changes" |
| **Status** | ✅ **IMPLEMENTED** — WARNING at creation time for `SELECT *` |

**Test coverage:** 10 unit tests for `detect_select_star()` pure function
(api.rs): bare `*`, table-qualified `t.*`, explicit columns, `count(*)`,
`sum(*)`, mixed agg+bare star, no asterisk, subquery star, case
insensitivity, no-FROM clause.

**Proposed fix:**

1. **Short term (P1):** ✅ Done — Emits `WARNING` at `create_stream_table()`
   time when the defining query contains `SELECT *`. Detection logic
   extracted to pure `detect_select_star()` function for unit testability.
2. **Medium term:** When `needs_reinit` is triggered by a column-count
   mismatch, attempt an automatic reinitialisation: re-parse the query,
   verify the new column set is compatible, and refresh. If compatible,
   skip the `needs_reinit` flag and refresh directly.

---

### EC-16 — ALTER FUNCTION body change not detected

| Field | Value |
|-------|-------|
| **Area** | DDL — function dependency tracking |
| **Tier** | **P1** |
| **Impact** | Stream table silently uses new function logic without a full rebase |
| **Current mitigation** | Manual full refresh after function changes |
| **Documented in** | FAQ § "Schema Changes" |
| **Status** | ✅ **IMPLEMENTED** — `function_hashes TEXT` column on `pgt_stream_tables` (migration 0.2.1→0.2.2); `check_proc_hashes_changed()` in `refresh.rs` queries `md5(prosrc \|\| coalesce(probin::text,''))` from `pg_proc` on each diff refresh; on mismatch calls `mark_for_reinitialize` + NOTICE; on first call stores baseline silently |

**Implemented fix:**

Polling via `pg_proc` on each differential refresh:
- On first call (stored hashes = NULL): compute and persist baseline. No reinit.
- On following calls: recompute hashes. If any differ → `mark_for_reinitialize` + NOTICE. The next scheduler cycle performs a full refresh.
- Uses `md5(string_agg(prosrc || coalesce(probin::text,''), ',' ORDER BY oid))` to handle polymorphic overloads robustly.

**Test coverage:** 3 E2E tests (`e2e_multi_cycle_tests.rs`):
- `test_ec16_function_body_change_marks_reinit` — verifies that
  `CREATE OR REPLACE FUNCTION` triggers `needs_reinit = true` on the next
  differential refresh.
- `test_ec16_function_change_full_refresh_recovery` — verifies that after
  reinit, a full refresh produces correct results with the new function logic.
- `test_ec16_no_functions_unaffected` — verifies that stream tables without
  user-defined functions are never affected by the hash polling mechanism.

---

### EC-17 — Schema change during active refresh

| Field | Value |
|-------|-------|
| **Area** | DDL — concurrency |
| **Tier** | **P2** |
| **Impact** | Refresh errors; stream table suspended |
| **Documented in** | FAQ § "Schema Changes" |
| **Status** | ✅ **Done** — FAQ section documents lock interaction |

**Proposed fix:** ~~No code fix needed~~ **Implemented:** FAQ section added
explaining `ShareLock` on source tables during refresh blocks concurrent
`ALTER TABLE` (`AccessExclusiveLock`). The window is only open if the DDL
sneaks in between lock acquisition and first read.

---

### EC-18 — `auto` CDC mode silent non-upgrade

| Field | Value |
|-------|-------|
| **Area** | CDC — mode transition |
| **Tier** | **P1** |
| **Impact** | User thinks WAL CDC is active; actually stuck on triggers forever |
| **Current mitigation** | Default is `TRIGGER` mode; `auto` must be explicitly set |
| **Documented in** | FAQ § "CDC Architecture" |
| **Status** | ✅ **IMPLEMENTED** — rate-limited LOG every ~60 scheduler ticks |

**Proposed fix:**

1. **Short term (P1):** When `cdc_mode = 'auto'`, emit a `LOG`-level
   message on every scheduler cycle that explains *why* the transition
   hasn't happened (e.g. "wal_level is 'replica', need 'logical'"). Rate-
   limit to once per 5 minutes.
2. **Medium term:** Add a `check_cdc_health()` finding for "auto mode stuck
   in TRIGGER phase for > 1 hour".

**Test coverage:** 2 E2E tests (`e2e_wal_cdc_tests.rs`) + 1 existing test:
- `test_ec18_check_cdc_health_shows_trigger_for_stuck_auto` — verifies
  `check_cdc_health()` reports TRIGGER mode for a keyless auto-CDC source
  and no alert fires for healthy TRIGGER-mode sources.
- `test_ec18_health_check_ok_with_trigger_auto_sources` — verifies
  `health_check()` does not flag TRIGGER-mode auto-CDC sources as errors.
- `test_wal_keyless_table_stays_on_triggers` (pre-existing) — verifies
  keyless tables remain on TRIGGER mode in auto-CDC configuration.

---

### EC-19 — WAL mode + keyless tables need REPLICA IDENTITY FULL

| Field | Value |
|-------|-------|
| **Area** | CDC — WAL decoding |
| **Tier** | **P0** |
| **Impact** | Silent data loss for UPDATEs/DELETEs on keyless tables in WAL mode |
| **Current mitigation** | Set `REPLICA IDENTITY FULL` before switching to WAL CDC |
| **Documented in** | FAQ § "CDC Architecture" |
| **Status** | ✅ **IMPLEMENTED** — `setup_cdc_for_source()` rejects at creation time |

**Proposed fix:**

1. **Short term (P0):** At `create_stream_table()` / `alter_stream_table()`
   time, when `cdc_mode` involves WAL and any source table is keyless, check
   `pg_class.relreplident` and **reject** with a clear error if not `'f'`
   (full). This prevents the silent data loss entirely.
2. **Medium term:** Automatically issue `ALTER TABLE … REPLICA IDENTITY
   FULL` for keyless tables when entering WAL mode (with a NOTICE).

**Test coverage:** 2 E2E tests (`e2e_wal_cdc_tests.rs`):
- `test_ec19_wal_keyless_without_replica_identity_full_rejected` — verifies
  that `create_stream_table(cdc_mode => 'wal')` on a keyless table without
  REPLICA IDENTITY FULL is rejected with a clear error.
- `test_ec19_wal_keyless_with_replica_identity_full_accepted` — verifies
  that the same combination succeeds once REPLICA IDENTITY FULL is set.

---

### EC-20 — CDC TRANSITIONING phase complexity

| Field | Value |
|-------|-------|
| **Area** | CDC — mode transition |
| **Tier** | **P2** |
| **Impact** | Potential duplicate capture during transition; rollback to triggers on failure |
| **Current mitigation** | LSN-based deduplication; automatic rollback |
| **Documented in** | FAQ § "CDC Architecture" |
| **Status** | ✅ **Done** — `check_cdc_transition_health()` in scheduler.rs |

**Proposed fix:** ~~Add a `trigger_inventory()` + `check_cdc_health()` combined check~~
**Implemented:** `check_cdc_transition_health()` runs after `recover_from_crash()`
in the scheduler. Loads all stream table deps, filters TRANSITIONING entries,
checks `pg_replication_slots` for slot existence, rolls back to TRIGGER mode
if slot is missing, logs status.

---

### EC-21 — Stream tables not propagated to logical replication subscriber

| Field | Value |
|-------|-------|
| **Area** | Replication — architecture |
| **Tier** | **P3** |
| **Impact** | Subscriber gets static snapshot without refresh capability |
| **Current mitigation** | Run pg_trickle only on primary |
| **Documented in** | FAQ § "Replication" |
| **Status** | ✅ **Done** — FAQ documentation sweep completed |

**Proposed fix:** Inherent to the architecture — CDC triggers and the
scheduler only run on the primary. **No code fix.** Consider a future
"subscriber mode" that detects replicated source tables and runs its own
CDC + refresh cycle, but this is a major undertaking (v2.0+).

---

### EC-22 — Change buffers not published to subscribers

| Field | Value |
|-------|-------|
| **Area** | Replication — change buffers |
| **Tier** | **P3** |
| **Impact** | Subscriber cannot drive its own refresh |
| **Documented in** | FAQ § "Replication" |
| **Status** | ✅ **Done** — FAQ documentation sweep completed |

**Proposed fix:** By design. Same as EC-21 — keep documentation clear.

---

### EC-23 — Scheduler does not run on standby / replica

| Field | Value |
|-------|-------|
| **Area** | Replication — standby |
| **Tier** | **P3** |
| **Impact** | No IVM on read replicas |
| **Current mitigation** | Query the primary; or promote the replica |
| **Documented in** | FAQ § "HA / Replication" |
| **Status** | ✅ **Done** — FAQ documentation sweep completed |

**Proposed fix:** By design — standbys are read-only. **No code fix.** A
future enhancement could add a read-only health endpoint on standby that
reports the last refresh timestamp from the replicated catalog, so
monitoring can detect staleness.

---

### EC-24 — TRUNCATE on source table bypasses CDC triggers

| Field | Value |
|-------|-------|
| **Area** | CDC — TRUNCATE handling |
| **Tier** | **P3** |
| **Impact** | Stream table not updated (until reinit on next cycle) |
| **Current mitigation** | TRUNCATE event trigger marks `needs_reinit`; next cycle does full recompute |
| **Documented in** | SQL_REFERENCE § "TRUNCATE" |

**Proposed fix:** Already handled by the TRUNCATE event trigger. The only
gap is latency — between the TRUNCATE and the next cycle, the stream table
is stale. For IMMEDIATE mode, TRUNCATE triggers a same-transaction full
refresh (already implemented). For DIFFERENTIAL/FULL, the current behaviour
is acceptable. **No further code fix needed.**

---

### EC-25 — TRUNCATE on a stream table itself

| Field | Value |
|-------|-------|
| **Area** | Stream table — direct manipulation |
| **Tier** | **P1** |
| **Impact** | Frontier / buffer desync; future refreshes produce incorrect deltas |
| **Current mitigation** | Use `refresh_stream_table()` to reset |
| **Documented in** | SQL_REFERENCE § "What Is NOT Allowed" |
| **Status** | ✅ **IMPLEMENTED** — BEFORE TRUNCATE guard trigger blocks direct TRUNCATE |

**Test coverage:** `test_guard_trigger_blocks_truncate` E2E test
(`e2e_guard_trigger_tests.rs`).

**Proposed fix:**

1. **Short term (P1):** ✅ Done — BEFORE TRUNCATE trigger installed on
   storage tables at creation time. Checks `pg_trickle.internal_refresh`
   session variable to allow managed refreshes. The `internal_refresh`
   flag is set in all refresh code paths: `execute_manual_refresh`,
   `execute_manual_full_refresh`, `execute_full_refresh`,
   `execute_topk_refresh`, and `pgt_ivm_handle_truncate`.
2. **Medium term:** Instead of blocking, intercept TRUNCATE and
   automatically perform a full refresh (same approach as IMMEDIATE mode
   TRUNCATE handling). This is friendlier but may surprise users who expect
   TRUNCATE to be instant.

---

### EC-26 — Direct DML on stream table

| Field | Value |
|-------|-------|
| **Area** | Stream table — direct manipulation |
| **Tier** | **P1** |
| **Impact** | Data overwritten or duplicated on next refresh |
| **Current mitigation** | Documentation; never write directly |
| **Documented in** | SQL_REFERENCE § "What Is NOT Allowed" |
| **Status** | ✅ **IMPLEMENTED** — BEFORE INSERT/UPDATE/DELETE guard trigger on storage table |

**Test coverage:** 4 E2E tests (`e2e_guard_trigger_tests.rs`) —
`test_guard_trigger_blocks_direct_insert`, `_update`, `_delete` +
`test_guard_trigger_allows_managed_refresh`.

**Proposed fix:**

1. **Short term (P1):** ✅ Done — Row-level BEFORE trigger installed on
   storage tables at creation time. Uses `pg_trickle.internal_refresh`
   session variable (checked via `current_setting(..., true)`) to allow
   managed refreshes. Returns OLD for DELETE operations (returning NEW
   would be NULL, silently cancelling the DELETE). The `internal_refresh`
   flag is set across all refresh code paths.
2. **Medium term:** Use a PostgreSQL row-level security policy or a
   `pg_trickle.allow_direct_dml` session variable as an escape hatch for
   power users.

---

### EC-27 — Foreign keys on stream tables

| Field | Value |
|-------|-------|
| **Area** | Stream table — constraints |
| **Tier** | **P3** |
| **Impact** | FK violations during bulk MERGE |
| **Current mitigation** | Do not define FKs |
| **Documented in** | SQL_REFERENCE § "What Is NOT Allowed" |

**Proposed fix:** Reject `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` on
stream tables via event trigger (same pattern as EC-25/EC-26). **P3** — the
current documentation is sufficient, and FK creation on stream tables is
rare.

---

### EC-28 — PgBouncer transaction pooling

| Field | Value |
|-------|-------|
| **Area** | Infrastructure — connection pooling |
| **Tier** | **P2** |
| **Impact** | Prepared statement errors, lock escapes, GUC not applied |
| **Current mitigation** | Use session-mode pooling; use `SET LOCAL` |
| **Documented in** | FAQ § "PgBouncer" |
| **Status** | ✅ **Done** — documentation already exists |

**Proposed fix:** pg_trickle's scheduler uses a single dedicated backend
connection (not pooled), so the scheduler itself is unaffected. The edge
case applies to application queries that use `SET pg_trickle.*` GUCs via a
pooled connection. **No code fix** — this is a PgBouncer limitation.
Improve documentation with a recommended PgBouncer configuration snippet.

---

### EC-29 — TopK + set operations / GROUPING SETS

| Field | Value |
|-------|-------|
| **Area** | TopK — SQL restrictions |
| **Tier** | **P3** |
| **Impact** | Creation error |
| **Documented in** | SQL_REFERENCE § "TopK" |

**Proposed fix:** TopK uses scoped recomputation via MERGE, which
fundamentally requires a single `ORDER BY … LIMIT` at the top level. Set
operations and multi-level grouping sets break this invariant. **No code
fix.** Keep the clear rejection error and document the workaround (split
into separate stream tables).

---

### EC-30 — Circular dependencies

| Field | Value |
|-------|-------|
| **Area** | DAG — cycle detection |
| **Tier** | **P3** |
| **Impact** | Creation error with cycle path in message |
| **Documented in** | FAQ § "Dependencies"; SQL_REFERENCE |

**Proposed fix:** Already handled — rejected at creation time with a clear
error listing the cycle path. See `plans/sql/PLAN_CIRCULAR_REFERENCES.md`
for future exploration of safe fixed-point evaluation. **No further work.**

---

### EC-31 — FOR UPDATE / FOR SHARE in defining query

| Field | Value |
|-------|-------|
| **Area** | SQL — row locking |
| **Tier** | **P3** |
| **Impact** | Creation error |
| **Documented in** | SQL_REFERENCE § "Rejected Constructs" |

**Proposed fix:** By design — row locks serve no purpose in stream table
defining queries. **No code fix.** The error message is clear.

---

### EC-32 — ALL (subquery) not supported

| Field | Value |
|-------|-------|
| **Area** | DIFFERENTIAL — subquery operators |
| **Tier** | **P2** |
| **Impact** | Creation error |
| **Documented in** | FAQ § "Why Are These SQL Features Not Supported?" |
| **Status** | ✅ **IMPLEMENTED** — `parse_all_sublink()` in `src/dvm/parser.rs` updated with NULL-safe anti-join condition `(col IS NULL OR NOT (x op col))`; full AntiJoin pipeline was already wired (`parse_sublink_to_wrapper` → `parse_all_sublink` → `AntiJoin SublinkWrapper` → `diff_anti_join`) |

**Fix applied:** NULL-safe anti-join condition.

`ALL (subquery)` is mathematically a conjunction of per-row comparisons.
The full pipeline through `parse_all_sublink()` → `AntiJoin` OpTree →
`diff_anti_join` was already wired. The outstanding issue was NULL-safety:
the previous condition `NOT (x op col)` evaluated to NULL (not TRUE) when
`col IS NULL`, so NULL rows in the subquery were silently ignored, giving
wrong results. Fixed by changing the condition to
`(col IS NULL OR NOT (x op col))`, which correctly excludes the outer row
whenever any subquery row is NULL or fails the comparison.

**Test coverage:** 8 E2E tests (`e2e_all_subquery_tests.rs`):
- `test_all_subquery_less_than_differential` — basic `price < ALL` filter
  matching the SQL Reference worked example.
- `test_all_subquery_differential_inner_insert` — differential refresh after
  inserting a cheaper competitor price that disqualifies a product.
- `test_all_subquery_differential_outer_change` — outer INSERT + inner DELETE
  with recomputation.
- `test_all_subquery_null_in_inner` — NULL-safety: ALL returns false when
  subquery contains NULL; removing NULL re-qualifies rows.
- `test_all_subquery_empty_inner` — empty subquery: ALL against empty set
  yields true (SQL standard); inserting reduces result.
- `test_all_subquery_full_refresh` — `>= ALL` with FULL refresh mode.
- `test_all_subquery_equals_operator` — `= ALL` (value equals every row).
- `test_all_subquery_not_equals_operator` — `<> ALL` (equivalent to NOT IN).

---

### EC-33 — Statistical aggregates (CORR, COVAR_*, REGR_*)

| Field | Value |
|-------|-------|
| **Area** | DIFFERENTIAL — aggregates |
| **Tier** | **P3** |
| **Impact** | Creation error in DIFFERENTIAL mode |
| **Current mitigation** | Use FULL mode; or pre-aggregate in DIFFERENTIAL and compute in a view |
| **Documented in** | FAQ § "Unsupported Aggregates" |

**Chosen fix: group-rescan from the completed transactional IVM work:**

Implement `CORR`/`COVAR_POP`/`COVAR_SAMP`/`REGR_*` using the proven
group-rescan strategy already used for `BOOL_AND`, `STRING_AGG`, etc.:

1. Detect affected groups from the delta (groups where any row was
   inserted, updated, or deleted).
2. For each affected group, re-aggregate from source:
   `SELECT group_key, CORR(y, x) FROM source GROUP BY group_key`
3. Apply the result as an UPDATE to the stream table.

This requires no stream table schema changes and no new OpTree variants.

**Deferred — Welford auxiliary accumulator columns:**

The earlier proposal of maintaining `sum_x`, `sum_y`, `sum_xy`, `sum_x2`,
`count` accumulator columns in the stream table using Welford's online
algorithm adapted to SQL EXCEPT/UNION was explored but is deferred.
Rationale: it requires a breaking schema change to existing stream tables,
the SQL delta-algebra correctness of the Welford adaptation is non-trivial
to verify, and group-rescan is already correct and benchmarked for
analogue aggregates. Revisit for v2.0 if group-rescan performance is
insufficient for very large groups.

---

### EC-34 — Backup / restore loses WAL replication slots

| Field | Value |
|-------|-------|
| **Area** | Operations — disaster recovery |
| **Tier** | **P1** |
| **Impact** | CDC stuck; slot-missing error after restore |
| **Current mitigation** | Set `cdc_mode = 'trigger'` after restore; let auto recreate slots |
| **Documented in** | FAQ § "Operations" |
| **Status** | ✅ **IMPLEMENTED** — auto-detect missing slot in health check; fall back to TRIGGER |

**Proposed fix:**

1. **Short term (P1):** Add a `health_check()` finding that detects
   "cdc_mode includes WAL but replication slot does not exist" and
   recommends the trigger fallback.
2. **Medium term:** On scheduler startup, when a source's `cdc_phase` is
   `WAL` but the slot is missing, automatically fall back to TRIGGER mode
   with a WARNING log and a NOTIFY.

**Test coverage:** 2 E2E tests (`e2e_wal_cdc_tests.rs`):
- `test_wal_fallback_on_missing_slot` (pre-existing) — full lifecycle test:
  creates WAL-mode ST, drops replication slot externally, waits for automatic
  fallback to TRIGGER, verifies data integrity after fallback.
- `test_ec34_check_cdc_health_detects_missing_slot` — verifies
  `check_cdc_health()` surfaces `replication_slot_missing` alert immediately
  after slot drop, then confirms fallback to TRIGGER and post-fallback
  data integrity.

---

### EC-35 — Managed cloud PostgreSQL restrictions

| Field | Value |
|-------|-------|
| **Area** | Infrastructure — cloud compatibility |
| **Tier** | **P3** |
| **Impact** | Extension cannot be installed |
| **Documented in** | FAQ § "Deployment" |

**Proposed fix:** pg_trickle cannot control cloud provider policies. Keep
the FAQ entry updated with a compatibility matrix (RDS, Cloud SQL,
AlloyDB, Azure Flexible, Supabase, Neon). Note that trigger-based CDC
avoids the most common restriction (`wal_level = logical`).

---

### EC-36 — Managed cloud: shared_preload_libraries restriction

| Field | Value |
|-------|-------|
| **Area** | Infrastructure — cloud compatibility |
| **Tier** | **P3** |
| **Impact** | Background worker + shared memory unavailable |
| **Documented in** | FAQ § "Deployment" |

**Proposed fix:** Investigate a "no-bgworker" mode where
`refresh_stream_table()` is called by an external cron (e.g. `pg_cron` or
an application timer). The scheduler would be optional when
`shared_preload_libraries` is unavailable. See
`plans/infra/REPORT_EXTERNAL_PROCESS.md` for the sidecar feasibility
study. **Long term (v2.0+).**

---

## Prioritised Recommendations

### P0 — Must fix (data correctness)

| # | Edge Case | Action | Estimated Effort |
|---|-----------|--------|------------------|
| 1 | ✅ EC-01: JOIN key change + right-side DELETE | R₀ via EXCEPT ALL — implemented in `diff_inner/left/full_join` | Done |
| 2 | ✅ EC-06: Keyless table duplicate rows | WARNING at creation + `has_keyless_source` flag | Done |
| 3 | EC-19: WAL + keyless without REPLICA IDENTITY FULL | Reject at creation time with clear error | 0.5 day |

### P1 — Should fix (operational surprise)

| # | Edge Case | Action | Estimated Effort |
|---|-----------|--------|------------------|
| 4 | ✅ EC-11: Schedule < refresh duration | `scheduler_falling_behind` NOTIFY alert at 80% threshold | Done |
| 5 | ✅ EC-13: Diamond split-version reads | Default `'atomic'` + `pg_sys::BeginInternalSubTransaction` in scheduler | Done |
| 6 | ✅ EC-15: SELECT * + DDL breakage | WARNING at creation for `SELECT *` queries | Done |
| 7 | EC-16: ALTER FUNCTION undetected | `pg_proc` hash polling on refresh cycle | 2 days |
| 8 | EC-18: `auto` CDC stuck in TRIGGER | Rate-limited LOG explaining why; health_check finding | 1 day |
| 9 | ✅ EC-25: TRUNCATE on stream table | Event trigger blocks TRUNCATE on pgtrickle schema | Done |
| 10 | ✅ EC-26: Direct DML on stream table | Guard trigger detecting non-engine writes | Done |
| 11 | EC-34: WAL slots lost after restore | Auto-detect missing slot; fall back to TRIGGER + WARNING | 1 day |

### P2 — Nice to have (usability gaps)

| # | Edge Case | Action | Estimated Effort |
|---|-----------|--------|------------------|
| 12 | EC-03: Window functions in expressions | CTE extraction in parser | 3–5 days |
| 13 | EC-05: Foreign tables | ✅ IMPLEMENTED — error msg + polling CDC | — |
| 14 | ✅ EC-09: IMMEDIATE unsupported constructs | Done — recursive CTEs + TopK now supported | Done |
| 15 | ✅ EC-17: DDL during active refresh | FAQ documentation added | Done |
| 16 | ✅ EC-20: CDC TRANSITIONING complexity | `check_cdc_transition_health()` in scheduler.rs | Done |
| 17 | ✅ EC-28: PgBouncer | Documentation already exists | Done |
| 18 | EC-32: ALL (subquery) | Parser rewrite to NOT EXISTS (… EXCEPT …) | 2–3 days |

### P3 — Accepted trade-offs (document, no code change)

✅ EC-02 (GUC implemented), EC-04, EC-07, EC-08, EC-10, EC-12, EC-14,
✅ EC-21/22/23 (docs done), EC-24, EC-27, EC-29, EC-30, EC-31, EC-35, EC-36.

These are either fundamental design trade-offs or have adequate existing
mitigations. Keep documentation current and revisit if user demand
surfaces.

### P3 — Committed long-term implementation

| # | Edge Case | Action | Estimated Effort |
|---|-----------|--------|------------------|
| 19 | EC-33: CORR, COVAR_*, REGR_* | Auxiliary accumulator columns; Welford-based SQL delta templates | 5–8 days |

---

## Implementation Order

**Recommended sprint sequence** (assuming 2-week sprints):

**Sprint 1 — P0 corrections:** ✅ DONE
- ✅ EC-19: WAL + keyless rejection (done: `has_keyless_source` flag + WARNING)
- ✅ EC-06: Keyless duplicate warning + `has_keyless_source` catalog column
- ✅ EC-01: R₀ via EXCEPT ALL (implemented in `diff_inner/left/full_join`)

**Sprint 2 — P1 operational safety:** ✅ DONE
- ✅ EC-25: Block TRUNCATE on stream tables (event trigger)
- ✅ EC-26: Guard trigger for direct DML
- ✅ EC-15: Warn on `SELECT *` (WARNING at creation)
- ✅ EC-11: `scheduler_falling_behind` alert (NOTIFY at 80% threshold)
- ✅ EC-13: Default `diamond_consistency = 'atomic'` + scheduler SAVEPOINT → `pg_sys::BeginInternalSubTransaction` fix

**Sprint 3 — Remaining P1 + P2 starts:** ✅ DONE (v0.6.0)
- ✅ EC-18: Rate-limited LOG for stuck auto mode (1 day)
- ✅ EC-34: Auto-detect missing WAL slot (1 day)
- ✅ EC-16: `pg_proc` hash polling for function changes (2 days)
- ✅ EC-03: Window function CTE extraction (3–5 days)

**Sprint 4+ — P2 + P3 long-term:** ✅ DONE (v0.6.0)
- ✅ EC-05, EC-32: Foreign table change detection; `ALL (subquery)` → `NOT EXISTS (… EXCEPT …)` rewrite
- EC-33: Statistical aggregates via auxiliary accumulator columns
- Documentation sweep for all remaining P3 items

---

## Open Questions

The following questions are unresolved design or validation unknowns that
must be answered before the associated fixes can be fully specified or
committed to.

---

### OQ-01 — EC-01: What is the correct new default for `adaptive_full_threshold`? ✅ RESOLVED

**Resolution (2026-05-30):** The short-term threshold adjustment is dropped
as not viable. It is a heuristic that can still silently miss deletes when
co-occurring changes fall below the threshold, providing false safety. The
primary fix is the R₀ via EXCEPT ALL approach (see EC-01). The
`adaptive_full_threshold` GUC remains as-is for users who want it as an
escape valve, but it is no longer positioned as an EC-01 mitigation path.

---

### OQ-02 — EC-01: Is pre-image capture viable without a breaking change buffer schema change? ✅ RESOLVED

**Resolution (2026-05-30):** Schema changes are NOT needed. The CDC trigger
(`src/cdc.rs`) already stores `OLD."col"` → `"old_col"` typed columns in the
change buffer for every UPDATE and DELETE event. The old join key values are
therefore already available. The question is now about *when* to use them.

The R₀ via EXCEPT ALL approach (chosen for Sprint 1) does not need `old_col`
column access from the delta CTE at all — it reconstructs the pre-change right
state from the live table + ΔR change set. The pre-image (`old_col`) approach
is still viable as a future optimisation (avoids the live-table EXCEPT ALL
scan) but is deferred to v1.x because it requires structural changes to
`DiffContext`/`DeltaSource` to surface `old_col` values in the delta CTE
output. Effort: 7–10 days when the time comes.

---

### OQ-03 — EC-01: What is the overhead of the R₀ via EXCEPT ALL approach?

**Updated analysis (2026-05-30):**

Code archaeology shows the overhead is similar to what Part 2 already pays
for `L₀`:

- Part 1 already does a semi-join-filtered scan of `R₁` (full right table,
  filtered by delta-left join keys). Adding R₀ = `R₁ EXCEPT ALL ΔR_inserts
  UNION ALL ΔR_deletes` adds **one extra pass over the ΔR CTE** (which is
  small — only changed rows). The live table is not scanned an extra time
  because `R₁` is already in the query plan; the EXCEPT ALL just subtracts a
  small set from it.
- The semi-join filter that limits R₁ also applies to R₀, so the set
  difference operates on already-filtered rows.
- The correction term (Part 3) is unaffected by this change.

**Expected overhead:** < 10% additional latency on the delta query for
typical workloads where ΔR is small relative to R. The concern about "one
extra temp-table scan per join source" from the original write-up was
over-estimated — the EXCEPT ALL operates on the already-materialized ΔR CTE,
not the full right table.

**Recommendation:** Confirm with a micro-benchmark (1M right table, 0.1%
change rate) before the Sprint 1 PR merges. Accept if overhead is under 30%
of current DIFFERENTIAL latency; otherwise fall back to applying R₀ only
when `ΔR` is non-empty (easy conditional).

---

### OQ-04 — EC-03: Where exactly do window-function-in-expression rewrites fail?

The CTE extraction rewrite for EC-03 is described as covering "~80% of
user demand", with complex cases (nested window calls inside arithmetic)
remaining unsupported.

**Open questions:**
- Is `SUM(ROW_NUMBER() OVER (…))` rejectable at parse time, or does it
  require a full semantic analysis?
- Can `NTILE(4) OVER (…) * 100` be rewritten to a CTE without changing
  query semantics?
- What are the most common real-world patterns users hit? (Survey the
  GitHub issues / Discord.)

**Recommendation:** Before implementing the rewrite, define a formal
boundary: "any expression where a window function appears as a direct
child of the SELECT list is rewriteable; anything deeper is rejected with
a clear error." Enumerate the cases explicitly in a test file
(`tests/e2e_window_expression_tests.rs`) before writing the rewrite code.
This keeps the scope bounded.

---

### OQ-05 — EC-06: Is `ctid`-based row identity viable for trigger-mode CDC?

The long-term fix for EC-06 proposes using `ctid` as a stable row
identifier for keyless tables in trigger mode. `ctid` is the physical
heap location and changes after `VACUUM FULL` / `CLUSTER`.

**Open questions:**
- Does pgrx expose the `ctid` of `OLD` in a row-level trigger? (Need to
  verify via `SPI_gettypeid` or `HeapTupleHeader`.)
- What happens if `VACUUM FULL` runs between the CDC trigger fire and the
  next refresh cycle? Would the stale `ctid` silently match the wrong row?
- Is `ctid` reset to the same value when the same physical slot is reused?
  (Yes in PostgreSQL — `ctid` is reused after `VACUUM`, not unique across
  time.)

**Recommendation:** Do **not** pursue `ctid`-based identity. The reuse
semantics make it unsafe as a durable row identifier. Instead, focus
Sprint 1 effort on the count-based hash approach (OQ-05 is answered: ctid
is off the table). If a unique row identity is required, recommend users
add a `DEFAULT gen_random_uuid()` column.

---

### OQ-06 — EC-08: Do statement-level transition tables actually batch multiple row changes?

The plan for EC-08 states that statement-level triggers with transition
tables "provide batching naturally". This is true for a single `UPDATE`
that affects N rows — the transition table contains all N old/new rows and
the IVM trigger fires once. But it is **not** true for N separate
single-row `UPDATE` statements in a loop — each fires the trigger once.

**Open questions:**
- What is the actual p50/p99 latency overhead per DML statement in
  IMMEDIATE mode on a stream table with a 3-way join? (No benchmark
  exists.)
- Does `pg_advisory_xact_lock` inside the trigger add measurable
  contention under 100+ concurrent single-row updates?

**Recommendation:** Add `benches/ivm_write_overhead.rs` as a Criterion
benchmark that measures per-statement latency for IMMEDIATE vs
DIFFERENTIAL mode at 1, 10, 100 concurrent writers. Run it as part of the
weekly benchmark schedule. Make the results visible in
`docs/BENCHMARK.md` so users can make informed mode choices. This answers
the question empirically rather than by assumption.

---

### OQ-07 — EC-36: How much of the scheduler can be decoupled for cloud deployments?

The long-term fix for EC-36 proposes a "no-bgworker" mode where
`refresh_stream_table()` is driven by `pg_cron` or an application timer.
`plans/infra/REPORT_EXTERNAL_PROCESS.md` covers the sidecar feasibility
research but reaches no conclusion.

**Open questions:**
- The scheduler currently holds a `pg_advisory_lock` during refresh to
  prevent concurrent refreshes. Would a cron-driven caller need to acquire
  this lock explicitly? (Yes — this is straightforward.)
- The scheduler tracks refresh history in `pgt_refresh_history`. Would a
  cron-driven caller need to insert rows there manually, or can this be
  encapsulated in `refresh_stream_table()`? (Likely yes — needs a small
  `refresh.rs` refactor.)
- What happens to the background worker registration if `pg_trickle` is in
  `shared_preload_libraries` but `pg_trickle.enabled = off`? The worker
  starts but immediately sleeps — is this acceptable for cloud providers
  that bill per background worker slot?

**Recommendation:** The prerequisite for EC-36 is verifying that
`refresh_stream_table()` already records to `pgt_refresh_history` (check
`src/refresh.rs`). If it does, the cron-mode is already 80% implemented —
users just need to disable the bgworker and call `refresh_stream_table()`
on a schedule. Document this pattern in `docs/FAQ.md` under "Deployment on
managed PostgreSQL" as an interim workaround before a formal no-bgworker
mode is built.
