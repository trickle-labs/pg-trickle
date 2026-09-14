# PLAN — Frontier Visibility Holdback (Issue #536)

**Status:** Implemented
**Owner:** TBD
**Tracking issue:** [#536](https://github.com/trickle-labs/pg-trickle/issues/536)
**Related:** [PLAN_OVERALL_ASSESSMENT_2.md](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/PLAN_OVERALL_ASSESSMENT_2.md) (frontier/buffer non-atomic commit), ADR-001 / ADR-002 in [plans/adrs/PLAN_ADRS.md](../adrs/PLAN_ADRS.md)

---

## 1. Problem Statement

The CDC frontier (`pgt_stream_tables.frontier`) is advanced based on **LSN
ordering only**, while the change buffer is read under standard **MVCC
visibility**. These two dimensions are orthogonal: a change buffer row may
have an LSN below the new frontier yet still be invisible (uncommitted) at
the moment the scheduler queries the buffer.

### Failure scenario (verified against current code)

| Step | Actor | Action |
|------|-------|--------|
| T1 | User session A | Begins txn, modifies tracked source table. Trigger inserts row into `pgtrickle_changes.changes_<oid>` with `lsn = pg_current_wal_insert_lsn()` ≈ `0/100`. **Txn A does not commit.** |
| T2 | Scheduler tick N | Captures `tick_watermark = pg_current_wal_lsn()` ≈ `0/500` ([src/scheduler.rs#L2634](../../src/scheduler.rs#L2634)). |
| T3 | Refresh worker | Runs `WHERE lsn > prev_lsn AND lsn <= 0/500` ([src/refresh/mod.rs#L3865](../../src/refresh/mod.rs#L3865)). MVCC hides A's uncommitted row. Frontier advanced and persisted to `0/500`. |
| T4 | User session A | Commits. Row at `lsn = 0/100` becomes visible. |
| T5 | Scheduler tick N+1 | Runs `WHERE lsn > 0/500 AND lsn <= …`. **Row at `0/100` is permanently skipped.** |

This is silent data loss, and there are currently **zero safeguards**:

- No `pg_stat_activity.backend_xmin` check
- No snapshot-based visibility filter
- No xid/txid column on the change buffer
- No hold-back margin on the frontier
- The reporter's `pg_trickle.tick_watermark_enabled` GUC ([src/config.rs#L663](../../src/config.rs#L663)) only enforces *cross-source* consistency within a tick — it does **not** address this race

### Practical likelihood

- Sub-second OLTP transactions: vanishingly rare (txn must straddle a full
  scheduler tick — typically hundreds of ms to seconds)
- Long batch jobs / interactive psql sessions / 2PC prepared txns: realistic
- Logical-decoding CDC mode (`src/wal_decoder.rs`) is **immune** because
  logical replication only emits committed changes ordered by commit LSN

### Out of scope for this plan

- The reporter's "Executor Hook" suggestion — requires kernel patches; not
  applicable to a contrib-style extension
- The `BIGSERIAL` cache-1 contention claim — real but a separate tuning
  topic; tracked elsewhere
- The non-atomic frontier-vs-buffer commit window already covered in
  [PLAN_OVERALL_ASSESSMENT_2.md](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/PLAN_OVERALL_ASSESSMENT_2.md)

---

## 2. Goals

1. **Eliminate the silent data-loss path** under default configuration.
2. Preserve current throughput in the common case (no long transactions).
3. Provide observability so operators can see when holdback is active.
4. Keep the fix entirely inside the extension boundary (no kernel patches,
   no `wal_level = logical` requirement for the trigger path).

---

## 3. Proposed Solution

A two-layer defence:

### Layer A — Snapshot xmin holdback (primary fix, default ON)

Before computing `new_lsn` for a refresh cycle, query the cluster's oldest
in-progress transaction xmin and translate it into a safe upper-bound LSN.
The frontier is never allowed to advance past LSNs that could still be
written by a not-yet-committed transaction.

**Mechanism:**

```sql
-- One probe per scheduler tick (cheap, ~µs):
SELECT
    pg_current_wal_lsn()                     AS write_lsn,
    coalesce(min(backend_xmin), txid_current()) AS oldest_xmin
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL
  AND state <> 'idle'
  AND pid <> pg_backend_pid();
```

If `oldest_xmin` is older than the xmin observed at the *previous* tick,
hold the new frontier at the previous tick's `tick_watermark` instead of
advancing to today's `write_lsn`. Concretely:

- Track per-tick `(tick_watermark_lsn, oldest_xmin)` in shared memory.
- For tick N, allowed upper bound =
  `min(write_lsn_N, last_lsn_with_no_older_xmin)`.
- This is conservative — it may delay visibility of new changes by one
  tick when long transactions are active, but it never skips a row.

**Edge cases:**

- 2PC prepared transactions: covered by `pg_prepared_xacts` — must be
  unioned into the xmin probe.
- Hot standby feedback / replication slots: their xmin already shows up in
  `pg_stat_activity`; no extra logic needed.
- Replication / logical-decoding CDC mode: skip the holdback (commit-LSN
  ordering is already safe).

### Layer B — Defensive xid stamping (secondary, opt-in)

Add an optional `xmin xid8` column to new change-buffer tables (gated by
`pg_trickle.cdc_buffer_track_xid`, default `false` for v1). When set, the
trigger writes `pg_current_xact_id()` alongside `lsn`. The refresh delta
query then becomes:

```sql
WHERE lsn > prev_lsn
  AND lsn <= new_lsn
  AND pg_xact_status(xmin) = 'committed'   -- belt-and-suspenders
```

This is redundant under READ COMMITTED but provides:

- An audit trail (every change row carries its source xid)
- A path to point-in-time / snapshot-consistent reads
- Forward compatibility with a future CSN-based scheme

Layer B is not required to close the bug; it's documented here so we don't
pick a column layout that would block it later.

### Layer C — Operator escape hatch

GUC: `pg_trickle.frontier_holdback_mode` with values:

| Value | Meaning |
|-------|---------|
| `xmin` (default) | Layer A enabled |
| `none` | Today's behaviour — fast, can lose rows under long txns |
| `lsn:<bytes>` | Hold back frontier by a fixed N bytes (debugging) |

This lets benchmark runs disable the probe and lets operators tune for
known-clean OLTP workloads.

---

## 4. Implementation Steps

1. **Add probe helper** — `src/cdc.rs::compute_safe_upper_bound(write_lsn, prev_oldest_xmin)`
   - Single SPI roundtrip per tick.
   - Returns `(safe_lsn, current_oldest_xmin)`.
   - Pure-logic helper (`classify_holdback`) split out so it's unit-testable
     without a backend (per AGENTS.md SPI rules).

2. **Wire into scheduler** — modify [src/scheduler.rs#L2630-2636](../../src/scheduler.rs#L2630-L2636)
   to consult `compute_safe_upper_bound` and feed the result into the
   existing `tick_watermark` capping path at
   [src/scheduler.rs#L5037-L5041](../../src/scheduler.rs#L5037-L5041).

3. **Persist last tick xmin** — add `last_tick_oldest_xmin: u64` to
   `PgTrickleSharedState` in [src/shmem.rs](../../src/shmem.rs).

4. **GUC plumbing** — add `pg_trickle.frontier_holdback_mode` in
   [src/config.rs](../../src/config.rs) (string GUC parsed once per tick).

5. **Metrics** — emit two counters via the existing monitoring path:
   - `pg_trickle_frontier_holdback_lsn_bytes` (gauge: how far behind write_lsn)
   - `pg_trickle_frontier_holdback_seconds` (gauge: oldest in-progress txn age)

6. **Docs** —
   - Add ADR-XX explaining the choice of probe-based holdback over
     xid stamping or executor hooks.
   - Update [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) CDC section.
   - Add troubleshooting entry in [docs/TROUBLESHOOTING.md](../../docs/TROUBLESHOOTING.md)
     for "stream table appears stuck behind a long transaction".

7. **Tests**
   - **Unit:** `classify_holdback` logic tables (xmin advances, xmin frozen,
     new oldest xmin appears, prepared xact present).
   - **Integration (Testcontainers):** spawn a backend that opens a
     transaction, performs DML on a tracked table, sleeps; verify
     scheduler does not advance frontier past the row's LSN; commit;
     verify next tick consumes it.
   - **E2E:** add `tests/e2e_long_txn_visibility_tests.rs` covering:
     - Standard READ COMMITTED txn straddling a tick
     - REPEATABLE READ txn straddling multiple ticks
     - 2PC prepared transaction held across many ticks
     - GUC `frontier_holdback_mode = none` reproduces the data loss
       (regression guard documenting the unsafe mode)
   - **Bench:** measure overhead of the per-tick probe (expect <100 µs).

---

## 5. Risks & Trade-offs

| Risk | Mitigation |
|------|-----------|
| Long-lived backend xmin (e.g. forgotten psql session) freezes frontier indefinitely | Emit `WARNING` once per minute when holdback exceeds `pg_trickle.frontier_holdback_warn_seconds` (default 60s); expose as metric |
| `pg_stat_activity` scan cost on busy clusters | One probe per scheduler tick (default ≥1s); negligible. Cache result for tick duration |
| Replication slots holding xmin | Same effect as a long txn — correct behaviour is to wait; document it |
| Bench results regress slightly | Layer C `none` mode preserves the old fast path for benchmarks |

---

## 6. Acceptance Criteria

- [ ] New E2E test `e2e_long_txn_visibility_tests.rs` passes with default GUCs.
- [ ] Same test demonstrably fails with `frontier_holdback_mode = none`.
- [ ] No regression on `e2e_bench_cdc_overhead` workload (≤2% throughput delta).
- [ ] `just test-all` green.
- [ ] ADR added; ARCHITECTURE.md and TROUBLESHOOTING.md updated.
- [ ] Issue #536 closed with link to release notes.

---

## 7. Out-of-scope follow-ups (separate issues)

1. ~~**Sequence cache contention on `change_id`** — evaluate `CACHE 32+`
   default; document that gaps are harmless.~~
   **Retracted (2026-04-20):** Increasing `CACHE` above 1 is **unsafe**.
   With `CACHE > 1`, concurrent transactions modifying the same row can
   commit in an order that inverts their pre-cached sequence values,
   causing compaction/delta pipelines (`ORDER BY change_id`) to pick stale
   data as the final state — silent data corruption.  `CACHE 1` is a hard
   correctness requirement for the trigger-based CDC path; the throughput
   ceiling this imposes is a fundamental limitation that only the
   WAL/logical-decoding backend can eliminate.  See issue #536 (last
   comment by @Teletele-Lin) for the full analysis.
2. **Atomic frontier+buffer commit** — covered by
   [PLAN_OVERALL_ASSESSMENT_2.md](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/PLAN_OVERALL_ASSESSMENT_2.md).
3. **WAL/logical-decoding CDC as default** — already on roadmap; this fix
   is for the trigger path that will remain the fallback.
