# pg_trickle — Overall Project Assessment v3

> **Status:** Draft (review-ready)
> **Type:** Audit / project-wide gap analysis
> **Last updated:** 2026-04-22
> **Scope:** Branch `feat/v0.27.0`, codebase as of v0.27.0 (released
> 2026-04-21). Compares v0.27.0 against the two prior assessments.
> **Target audience:** Maintainers, release managers, and senior contributors
> planning v0.28.0 → v1.0.0.
> **Sibling documents:**
> - [PLAN_OVERALL_ASSESSMENT.md](PLAN_OVERALL_ASSESSMENT.md) — v0.20.0 baseline
> - [PLAN_OVERALL_ASSESSMENT_2.md](PLAN_OVERALL_ASSESSMENT_2.md) — v0.23.0
>   follow-up
> - [ROADMAP.md](../ROADMAP.md) — current release roadmap
> - [plans/PLAN.md](PLAN.md) — overall design plan
> - [AGENTS.md](../AGENTS.md) — engineering conventions

---

## Executive Summary

pg_trickle has matured substantially since v0.23.0. The architectural
re-shaping promised in the previous assessment (`refresh.rs` decomposition,
shared-memory lock split, CDC `unwrap()` removal, snapshot/PITR API,
predictive schedule planner, cluster-wide observability) has shipped. The
codebase is now organised into purposeful sub-modules
([`refresh/mod.rs`](../src/refresh/mod.rs) is a 187-line shim, with
`orchestrator.rs`/`codegen.rs`/`merge.rs`/`phd1.rs` carrying the weight); the
old `PGS_STATE` mega-lock has been split into three dedicated `PgLwLock`s
(`SCHEDULER_META_STATE`, `TICK_WATERMARK_STATE`); CDC error paths now produce
typed `PgTrickleError::ChangedColsBitmaskFailed` instead of panicking; and a
two-tier template cache (L1 thread-local + L2 catalog table
`pgtrickle.pgt_template_cache`) backs cross-backend reuse. The pgrx 0.18
upgrade landed without an extension-version bump, the SNAP/PLAN/CLUS/METR
families ship with SQL surface, and OpenMetrics now emits `db_oid` / `db_name`
labels for cluster-wide Prometheus aggregation.

The remaining risks are concentrated in five areas. **(1)** EC-01 is **still
incomplete** in production: the v0.24.0 “PKs are immutable, so hash hashes
match” fix at [`src/dvm/operators/join.rs:220-260`](../src/dvm/operators/join.rs)
is sound for INNER joins on stable PKs, but `is_deduplicated: false` at
[`src/dvm/operators/join.rs:657-668`](../src/dvm/operators/join.rs) means the
MERGE pipeline still groups by row-id, and repo memory
(`/memories/repo/join-delta-phantom-rows.md`) confirms `test_tpch_q07_*` and
the IMMEDIATE-mode property tests still occasionally drift; PH-D1 cross-cycle
cleanup ([`src/refresh/phd1.rs`](../src/refresh/phd1.rs)) is opt-in per cycle
and not always invoked. **(2)** `snapshot_stream_table` /
`restore_from_snapshot`
([`src/api/snapshot.rs`](../src/api/snapshot.rs)) are **not transaction
bracketed** — the `CREATE TABLE AS` and the catalog `INSERT INTO
pgtrickle.pgt_snapshots` are independent statements, so a backend crash
between them leaves orphan tables; `restore_from_snapshot` issues a
non-locked `TRUNCATE` followed by an unguarded bulk `INSERT`, so concurrent
writers see partial state. **(3)** The `IVM_DELTA_CACHE` thread-local
HashMap in [`src/ivm.rs`](../src/ivm.rs) has **no eviction policy**, growing
unbounded per backend in IMMEDIATE-mode workloads with churn. **(4)**
`classify_spi_error_retryable` ([`src/error.rs`](../src/error.rs)) still
relies on **English-text pattern matching** (no SQLSTATE inspection) and
will misclassify localised Postgres builds. **(5)** The L2 template-cache
catalog table has no size cap, no LRU/age eviction; over the lifetime of a
high-DDL deployment it grows unbounded.

The top three opportunities for v0.28.0+ are **(1)** considering a
shared-memory dshash L0 template cache (today template caching uses a
thread-local L1 and catalog-backed L2 — see
[`src/dvm/mod.rs:898`](../src/dvm/mod.rs)); **(2)** building the reactive
subscription primitive that v0.27.0's publication infrastructure now makes
trivially achievable; and **(3)** starting the PGlite/WASM port investment
ahead of the v1.4 milestone, since the recently-cleaned core (no more giant
`refresh.rs`, no more lock megastruct) is finally ready to be carved out.

The overall project has moved decisively from “experimental extension” to
“production-ready streaming engine for PostgreSQL 18”. The remaining gaps
are well-bounded. If the P0/P1 items in §11 land before v0.30.0, the
project is on track for a credible v1.0 GA.

---

## Table of Contents

1. [Method & Scope](#1-method--scope)
2. [What Has Improved Since v0.23.0](#2-what-has-improved-since-v0230)
3. [Critical Bugs](#3-critical-bugs)
4. [Architecture Gaps](#4-architecture-gaps)
5. [Performance](#5-performance)
6. [Security](#6-security-owasp-mapped)
7. [Test Coverage Gaps](#7-test-coverage-gaps)
8. [Documentation Gaps](#8-documentation-gaps)
9. [Operational / Ecosystem](#9-operational--ecosystem)
10. [Recommended New Features](#10-recommended-new-features)
11. [Prioritised Action Plan](#11-prioritised-action-plan)
12. [Risk Register](#12-risk-register)
13. [Appendix A — Unsafe Block Inventory](#appendix-a--unsafe-block-inventory)
14. [Appendix B — Undocumented SQL Functions](#appendix-b--undocumented-sql-functions)
15. [Appendix C — Undocumented GUCs](#appendix-c--undocumented-gucs)

---

## 1. Method & Scope

This assessment was produced by a single audit pass over the
`feat/v0.27.0` branch (head as of 2026-04-22) using a combination of
file-level read passes, targeted `grep` queries, and inspection of
`docs/`, `plans/`, `cnpg/`, `dbt-pgtrickle/`, `benches/`, `fuzz/` and
`tests/`.

### 1.1 Inputs

- The two prior assessments
  ([PLAN_OVERALL_ASSESSMENT.md](PLAN_OVERALL_ASSESSMENT.md),
  [PLAN_OVERALL_ASSESSMENT_2.md](PLAN_OVERALL_ASSESSMENT_2.md)) — used to
  identify which findings persisted, which were resolved, and which
  regressed.
- The CHANGELOG ([CHANGELOG.md](../CHANGELOG.md)) — top sections covering
  v0.24.0 through v0.27.0 inclusive.
- Repo-scoped memory files
  (`/memories/repo/df-cdc-buffer-trends-fix.md`,
  `/memories/repo/join-delta-phantom-rows.md`,
  `/memories/repo/tpch-dvm-scaling-analysis.md`) — to anchor any “fixed?”
  claims against actual recent debugging history.
- The roadmap ([ROADMAP.md](../ROADMAP.md)).
- The migration script
  [`sql/pg_trickle--0.26.0--0.27.0.sql`](../sql/pg_trickle--0.26.0--0.27.0.sql).
- Source files; the largest examined include
  [`src/scheduler.rs`](../src/scheduler.rs) (7,392 LOC),
  [`src/dvm/parser/sublinks.rs`](../src/dvm/parser/sublinks.rs) (6,485 LOC),
  [`src/api/mod.rs`](../src/api/mod.rs) (6,127 LOC),
  [`src/dvm/parser/rewrites.rs`](../src/dvm/parser/rewrites.rs) (6,104 LOC),
  [`src/dvm/parser/mod.rs`](../src/dvm/parser/mod.rs) (5,526 LOC),
  [`src/dvm/operators/aggregate.rs`](../src/dvm/operators/aggregate.rs) (5,399 LOC),
  [`src/dag.rs`](../src/dag.rs) (4,295 LOC),
  [`src/dvm/operators/recursive_cte.rs`](../src/dvm/operators/recursive_cte.rs) (4,043 LOC),
  [`src/cdc.rs`](../src/cdc.rs) (3,970 LOC),
  [`src/monitor.rs`](../src/monitor.rs) (3,633 LOC),
  [`src/refresh/merge.rs`](../src/refresh/merge.rs) (3,393 LOC),
  [`src/config.rs`](../src/config.rs) (3,270 LOC),
  [`src/catalog.rs`](../src/catalog.rs) (3,135 LOC),
  [`src/dvm/parser/types.rs`](../src/dvm/parser/types.rs) (3,082 LOC),
  [`src/refresh/codegen.rs`](../src/refresh/codegen.rs) (2,383 LOC),
  [`src/wal_decoder.rs`](../src/wal_decoder.rs) (2,118 LOC),
  [`src/diagnostics.rs`](../src/diagnostics.rs) (2,049 LOC),
  [`src/hooks.rs`](../src/hooks.rs) (1,961 LOC),
  [`src/ivm.rs`](../src/ivm.rs) (1,414 LOC),
  [`src/dvm/parser/validation.rs`](../src/dvm/parser/validation.rs) (1,177 LOC),
  [`src/shmem.rs`](../src/shmem.rs) (1,079 LOC),
  [`src/dvm/diff.rs`](../src/dvm/diff.rs) (1,057 LOC),
  [`src/api/snapshot.rs`](../src/api/snapshot.rs) (440 LOC),
  [`src/refresh/orchestrator.rs`](../src/refresh/orchestrator.rs) (404 LOC),
  [`src/api/planner.rs`](../src/api/planner.rs) (341 LOC),
  [`src/api/cluster.rs`](../src/api/cluster.rs) (153 LOC),
  [`src/template_cache.rs`](../src/template_cache.rs) (158 LOC),
  [`src/refresh/mod.rs`](../src/refresh/mod.rs) (187 LOC),
  [`src/refresh/phd1.rs`](../src/refresh/phd1.rs) (108 LOC).
- Documentation: `docs/` (29 files), `cnpg/` manifests, `dbt-pgtrickle/`
  subproject.
- Benchmark inventory: [`benches/diff_operators.rs`](../benches/diff_operators.rs),
  [`benches/refresh_bench.rs`](../benches/refresh_bench.rs),
  [`benches/scheduler_bench.rs`](../benches/scheduler_bench.rs).
- Fuzz inventory: [`fuzz/fuzz_targets/`](../fuzz/fuzz_targets/) — `cdc_fuzz`,
  `cron_fuzz`, `guc_fuzz`, `parser_fuzz`.

### 1.2 Method

For every finding I cite a file path and a line number, and where possible
quote a few characters of context. I do **not** reproduce findings already
resolved in v0.24.0–v0.27.0; those go into §2 instead. I have made no source
modifications during this audit. Where this assessment disagrees with the
ROADMAP about whether something is delivered, the source code wins.

### 1.3 Out of scope

- Performance microbenchmarks (numbers reported in §5 are existing CHANGELOG
  numbers, not reproduced).
- Patent / IP review — already covered in `src/cdc.rs` prior-art note.
- Detailed dbt-pgtrickle macro audit — covered separately by
  [dbt-pgtrickle/AGENTS.md](../dbt-pgtrickle/AGENTS.md) and
  [plans/dbt/](dbt/).
- Marketing / website assets.
- Deep TUI audit (pgtrickle-tui has been removed from the project)
  gaps with the new SNAP/PLAN/CLUS/METR functions.

---

## 2. What Has Improved Since v0.23.0

The single most important change since the previous assessment is that
**the architecture is no longer the dominant risk**. The structural
liabilities flagged in
[PLAN_OVERALL_ASSESSMENT_2.md](PLAN_OVERALL_ASSESSMENT_2.md) — the 5,400-line
`refresh.rs`, the single mega-lock `PGS_STATE`, the panicking CDC code path,
the missing snapshot story, the missing predictive scheduling story, the
fragmented per-database observability — have all been addressed in some
form. The table below maps each item from the v0.23 assessment to its
current state.

| v0.23 finding | Identifier | Current state (v0.27.0) | Evidence |
|---|---|---|---|
| `refresh.rs` is 5,400 LOC, dominates change risk | ARCH-1B | **Resolved.** Split into `mod.rs` (187 LOC re-export shim), `orchestrator.rs` (404), `codegen.rs` (2,383), `merge.rs` (3,393), `phd1.rs` (108). | [`src/refresh/mod.rs:1-30`](../src/refresh/mod.rs) |
| `PGS_STATE` PgLwLock guards everything | SCAL-3 | **Resolved.** Three locks: `PGS_STATE` (DAG state), `SCHEDULER_META_STATE` (PID/wake), `TICK_WATERMARK_STATE` (xmin/safe LSN). | [`src/shmem.rs:100-118`](../src/shmem.rs) |
| Two `unwrap()`s in `cdc.rs` changed-cols bitmask path | CDC-1 | **Resolved.** New typed variant `ChangedColsBitmaskFailed(String)` propagates instead. | [`src/error.rs:127-131`](../src/error.rs) and [`src/error.rs:147-152`](../src/error.rs) (variant) |
| Partitioned source publication rebuild fragile | CDC-2 | **Resolved.** Typed `PublicationRebuildFailed` variant + `PublicationRebuildFailed` error path in `src/api/mod.rs:421`. | [`src/error.rs:153-156`](../src/error.rs) |
| Refresh history retention unbounded | OBS-1 | **Resolved.** `pg_trickle.history_retention_days` GUC + scheduler auto-prune. | [`docs/CONFIGURATION.md`](../docs/CONFIGURATION.md) (TOC entry “History & Retention”) |
| TOAST-aware change-buffer durability missing | DUR-1 | **Resolved.** `change_buffer_durability` GUC with `unlogged|logged|sync`. | [`docs/CONFIGURATION.md`](../docs/CONFIGURATION.md) Guardrails section |
| Predictive cost-model has no warm-up / outlier guard | PRED-1 | **Resolved.** v0.25 60s warmup + outlier removal. | [`src/refresh/orchestrator.rs:160-260`](../src/refresh/orchestrator.rs) `query_refresh_history_stats` requires ≥3 differential and ≥1 full sample. |
| Subscriber LSN tracking absent | SUB-1 | **Resolved.** Tracking shipped v0.25; surfaced via `pgtrickle.pgt_status()`. | (CHANGELOG v0.25.0) |
| Frontier two-phase commit missing | FRONT-1 | **Resolved.** `change_buffer_durability=logged|sync` enables tentative→finalize commit. | (CHANGELOG v0.24.0) |
| L1 template cache only — cold backends pay 45 ms | CACHE-1 | **Partially resolved.** L2 catalog cache shipped (`pgtrickle.pgt_template_cache`); L0 dshash signal exists but the actual shared-memory data structure is **not** populated. | [`src/template_cache.rs:1-159`](../src/template_cache.rs); [`src/shmem.rs:680-710`](../src/shmem.rs) |
| `template_cache_max_entries` LRU not enforced | CACHE-2 | **Resolved for L1.** GUC honoured by thread-local cache. **Not enforced for L2** (catalog table grows unbounded). | (See §3.5 below.) |
| No per-database Prometheus labels | OBS-CLUS | **Resolved.** v0.27.0 CLUS-2 — every metric tagged `db_oid`/`db_name`. | [`docs/integrations/multi-tenant.md`](../docs/integrations/multi-tenant.md) |
| No snapshot/PITR API | SNAP | **Resolved (initial).** `snapshot_stream_table`, `restore_from_snapshot`, `list_snapshots`, `drop_snapshot` shipped. **But — see §3.2 for atomicity bug.** | [`src/api/snapshot.rs`](../src/api/snapshot.rs) |
| No predictive scheduler hint | PLAN | **Resolved.** `recommend_schedule` and `schedule_recommendations` SQL functions shipped. | [`src/api/planner.rs`](../src/api/planner.rs); [`sql/pg_trickle--0.26.0--0.27.0.sql:104-130`](../sql/pg_trickle--0.26.0--0.27.0.sql) |
| No cluster-wide worker visibility | CLUS-1 | **Resolved.** `pgtrickle.cluster_worker_summary()` reads from shmem + `pg_stat_activity`. | [`src/api/cluster.rs`](../src/api/cluster.rs) |
| No metrics_summary aggregation | METR-3 | **Resolved.** `pgtrickle.metrics_summary()` shipped. | [`src/api/metrics_ext.rs`](../src/api/metrics_ext.rs) |
| pgrx 0.17 vendor lock | DEP-1 | **Resolved.** `Cargo.toml` is now pgrx 0.18.0 across the workspace, including [fuzz/](../fuzz/). | [`Cargo.toml`](../Cargo.toml) |
| Recursion in DVM parser unbounded | G13-SD | **Resolved.** `cte_ctx.descend()/ascend()` enforced via `pg_trickle.max_parse_depth` GUC; rejection emits `QueryTooComplex`. | [`src/dvm/parser/mod.rs`](../src/dvm/parser/mod.rs) (called in `parse_select_stmt`, `parse_set_operation`, `parse_from_item`); [`src/error.rs:65-67`](../src/error.rs) |
| `lib.rs` not denying clippy::unwrap_used in non-test | SAF-3 | **Resolved.** [`src/lib.rs:23`](../src/lib.rs) sets `#![cfg_attr(not(test), deny(clippy::unwrap_used))]`. |
| SCC `.unwrap()` in tarjan algorithm | DAG-PANIC | **Mitigated.** Three `.unwrap()` sites at [`src/dag.rs:1093, 1100, 1112`](../src/dag.rs) carry explicit `nosemgrep` justifications referencing the SCC invariant. Sound but worth a final audit. |
| Per-DB IVM memory leak risk (early v0.23 IVM) | IVM-1 | **Partially resolved.** IVM lock-mode parser now defaults to `Exclusive` on parse failure (correct, fail-closed); but the `IVM_DELTA_CACHE` is **still unbounded** (see §3.3). | [`src/ivm.rs:48-91`](../src/ivm.rs) lock mode; [`src/ivm.rs:107-143`](../src/ivm.rs) cache |

In addition, the v0.27.0 release ships several improvements not flagged in
the v0.23 assessment:

- **Statement-level CDC triggers** with `REFERENCING NEW/OLD TABLE`
  ([`src/cdc.rs:124-219`](../src/cdc.rs)) — gives the “50–80% less write
  overhead for bulk DML” claim its production teeth.
- **INSERT-only CORR-4 invariant** for `pgt_refresh_history` enforced at
  [`src/cdc.rs:71-83`](../src/cdc.rs) — prevents UPDATE/DELETE CDC trigger
  registration on the audit table itself.
- **`pg_current_wal_insert_lsn()`** documented as the **only** WAL position
  function used by CDC ([`src/cdc.rs:104-114`](../src/cdc.rs)) — eliminates
  the silent-no-op refresh class of bug.
- **Sub-transaction RAII helper** ([`src/scheduler.rs:250-308`](../src/scheduler.rs))
  with auto-rollback on `Drop` — replaces ad-hoc
  `BeginInternalSubTransaction` / `RollbackAndReleaseCurrentSubTransaction`
  pairs and gives panic-safe per-job isolation.
- **Cross-cycle PH-D1 phantom cleanup** ([`src/refresh/phd1.rs:34-100`](../src/refresh/phd1.rs))
  — anti-join `NOT EXISTS` against the full-refresh result, batched.

The rest of this document focuses on what *remains* to do.

---

## 3. Critical Bugs

This section enumerates issues that should block a v1.0 release. Each is
classified P0 (must fix before v0.30.0), P1 (must fix before v1.0), or P2
(should fix before v1.0 but workarounds exist).

### 3.1 EC-01 phantom rows still drift in production tests *(P0)*

**Where:** [`src/dvm/operators/join.rs:220-260`](../src/dvm/operators/join.rs)
(Part 1a/1b hash formula); [`src/dvm/operators/join.rs:657-668`](../src/dvm/operators/join.rs)
(`is_deduplicated: false`); cross-cycle cleanup in
[`src/refresh/phd1.rs:34-99`](../src/refresh/phd1.rs).

The v0.24.0 fix made Part 1a (`ΔL_I ⋈ R₁`) and Part 1b (`ΔL_D ⋈ R₀`) emit
the same `__pgt_row_id` for the same logical row by always hashing
`(left_pks, right_pks)`. The premise — that PKs are immutable — is correct
in the steady state, but the surrounding pipeline still emits
`is_deduplicated: false` (line 668), forcing the MERGE to aggregate by row
id. The repository memory note in
[`/memories/repo/join-delta-phantom-rows.md`](/memories/repo/join-delta-phantom-rows.md)
records that `test_tpch_q07_ec01b_combined_delete` and
`test_tpch_immediate_correctness` continue to flake with one or two
phantom rows when an insert-then-delete pair on the left collides with a
delete on the right within the same cycle. PH-D1 cross-cycle cleanup
([`src/refresh/phd1.rs:34`](../src/refresh/phd1.rs))
is **opt-in** per refresh and is not invoked by the default code path; the
caller is supposed to invoke it conditionally when prior refresh reported
non-zero phantom residuals (see comments at
[`src/refresh/phd1.rs:18-32`](../src/refresh/phd1.rs)), but as of v0.27.0
the wiring that actually flips this conditional is incomplete.

**Risk:** Stream tables with multi-table joins can accumulate spurious rows
across cycles, causing `UNIQUE_VIOLATION` in the soak test
(`tests/e2e_soak_*`) and incorrect business metrics in production. This is
the single highest-priority correctness defect remaining.

**Recommendation:** Either (a) prove convergence by routing all cycles
through unconditional PH-D1 cleanup with a small batch size, or (b)
re-engineer Part 2 to derive its row-id from the *retained* base-table
key snapshot rather than mixing pre- and post-image hashes. Option (a) is
cheap in CPU and safe; option (b) is the principled fix and would let
`is_deduplicated: true` flip on for INNER joins.

### 3.2 Snapshot/restore is not atomic *(P0)*

**Where:** [`src/api/snapshot.rs:90-180`](../src/api/snapshot.rs)
(`snapshot_stream_table_impl`); [`src/api/snapshot.rs:200-260`](../src/api/snapshot.rs)
(`restore_from_snapshot_impl`).

The implementation runs three separate statements without bracketing:

1. `CREATE TABLE … AS SELECT *, $1, $2, now() FROM <storage>` — the
   archival table.
2. `INSERT INTO pgtrickle.pgt_snapshots …` — the catalog row tracking the
   snapshot.
3. (Restore path) `TRUNCATE <storage>` followed by `INSERT INTO <storage>
   SELECT * EXCEPT(...) FROM <source>` followed by `UPDATE
   pgtrickle.pgt_stream_tables SET frontier=…`.

**Failure modes:**

- *Snapshot path:* If the connection dies between (1) and (2), the
  archival table exists but has **no catalog row** —
  `pgtrickle.list_snapshots()` won't find it, but it still consumes
  storage. Worse, a future snapshot with the same auto-generated name
  (very unlikely given the millisecond-epoch suffix, but possible across
  clock skew) returns `SnapshotAlreadyExists` from
  [`src/api/snapshot.rs:113`](../src/api/snapshot.rs).
- *Restore path:* The `TRUNCATE` takes an `AccessExclusiveLock`, but the
  subsequent `INSERT` runs in the same transaction with no additional
  guard. **However** `Spi::run` issues a separate command — there is no
  explicit `BEGIN; … COMMIT;` framing in the function body. If
  `INSERT … SELECT *` is interrupted (statement timeout, OOM, deadlock
  with another `pgtrickle.refresh_stream_table` call), the storage table
  is left **truncated** with zero rows but the catalog still records the
  stream table as `is_populated = true` (the `UPDATE` later sets it
  again, but only if the INSERT succeeded). The subsequent automatic
  refresh will fall back to `Differential` against an empty storage and
  apply only the new delta rows, producing a permanently truncated
  result.
- *Restore path again:* The `__pgt_snapshot_version` major-version
  comparison at
  [`src/api/snapshot.rs:215-230`](../src/api/snapshot.rs) is correct in
  spirit, but `Spi::get_one_with_args::<String>` returns `None` if the
  first SPI call fails for any reason (permissions, missing column),
  which is then silently treated as “no version stored” — letting the
  restore proceed against an actually-incompatible snapshot.

**Recommendation:** Wrap the entire snapshot operation in
`BeginInternalSubTransaction` / `ReleaseCurrentSubTransaction` (the same
RAII helper as in [`src/scheduler.rs:250-308`](../src/scheduler.rs)).
For restore, take an exclusive lock on the storage table at the start
(`LOCK TABLE <storage> IN ACCESS EXCLUSIVE MODE`) and propagate snapshot
version-check failures as `SnapshotSchemaVersionMismatch` rather than
returning `None`.

### 3.3 `IVM_DELTA_CACHE` grows without bound *(P1)*

**Where:** [`src/ivm.rs:107-143`](../src/ivm.rs).

The cache is a `thread_local! { static IVM_DELTA_CACHE:
RefCell<HashMap<IvmCacheKey, CachedIvmDelta>> = RefCell::new(HashMap::new()) }`.
The key is `(pgt_id, source_oid, has_new, has_old)`, which is bounded per
session, but the `CachedIvmDelta { delta_sql, user_columns, .. }` payload
can be tens of kilobytes per IMMEDIATE-mode stream table. The only
eviction path is `invalidate_ivm_delta_cache(pgt_id)` (called on ALTER
QUERY / DROP / reinitialize) and the `LOCAL_IVM_CACHE_GEN` generation
counter (called on global cache flush). There is **no LRU**, no size cap,
and no time-based eviction. A long-lived backend that touches many
IMMEDIATE-mode stream tables (e.g. a reporting connection that ranges
over the catalog) will pay the cache memory cost forever.

**Recommendation:** Honour `pg_trickle.template_cache_max_entries` for the
IVM cache as well as the L1 delta-template cache. Implement a simple
clock-style eviction.

### 3.4 `classify_spi_error_retryable` is text-based *(P1)*

**Where:** [`src/error.rs:213-260`](../src/error.rs) (and the
documentation comment above it).

The function explicitly admits SQLSTATE codes do not appear in pgrx SPI
error messages, and matches against English text fragments such as
`"does not exist"`, `"syntax error"`, `"permission denied"`,
`"violates"`, `"division by zero"`. On a PostgreSQL build with a
non-English `lc_messages`, **all** of these patterns silently break and
every SPI error becomes retryable. Even on an English build, future
PostgreSQL message rewordings (which happen between major releases)
could regress this without any compile error.

**Recommendation:** Push SQLSTATE through pgrx (it's available in
`pg_sys::ErrorData.sqlerrcode`); update [`error.rs`](../src/error.rs) to
classify by 5-character code instead of text. As a stopgap, set
`PGOPTIONS='-c client_messages_locale=en_US.UTF-8'` in CI.

### 3.5 L2 template-cache catalog table is unbounded *(P1)*

**Where:** [`src/template_cache.rs:79-118`](../src/template_cache.rs)
(`store`); [`src/template_cache.rs:124-142`](../src/template_cache.rs)
(`invalidate`/`invalidate_all`).

`store()` does an `INSERT ... ON CONFLICT (pgt_id) DO UPDATE`, which keeps
the catalog at one row per stream table — bounded by the number of stream
tables. So far so good. **But:** `invalidate()` only deletes for a single
`pgt_id`, and there is no purger for stale entries left behind by ALTER
QUERY without DROP, by source-table OID renumbering, or by
extension-version migration. The catalog table is `UNLOGGED`, so a crash
flushes it — but that simply moves the problem from durability to cold
boot.

**Recommendation:** Add a `cached_at TIMESTAMPTZ` aware purge inside the
scheduler's tick (one DELETE per launcher cycle, age-bounded) and respect
`pg_trickle.template_cache_max_entries` for the L2 table as well as L1.

### 3.6 SubLink extraction inside OR is silently rejected, but not for all shapes *(P2)*

**Where:** [`src/dvm/parser/sublinks.rs:101-145`](../src/dvm/parser/sublinks.rs)
(`extract_where_sublinks`).

The error path correctly rejects `SubLink` inside `OR` with a
`UnsupportedOperator`, but the check uses `node_tree_contains_sublink`
which only recurses into `T_BoolExpr` arguments. SubLinks nested inside
`T_CaseExpr`, `T_CoalesceExpr`, or function arguments can slip past the
check and end up in `safe_node_to_expr` as opaque `Expr::Raw`. That is
correct *for execution* (the raw SQL still runs), but it silently
disables differential refresh for those queries — they fall back to
FULL — without any user-visible explanation.

**Recommendation:** Either generalise `node_tree_contains_sublink` to
recurse via `expression_tree_walker_impl`, or emit a `NOTICE` when the
parser silently downgrades an OR-with-CASE to FULL.

### 3.7 `restore_from_snapshot` SELECT * EXCEPT requires PG ≥ 18 with the patch *(P2)*

**Where:** [`src/api/snapshot.rs:243-251`](../src/api/snapshot.rs).

The comment claims “PostgreSQL 18+ supports `SELECT * EXCEPT (...)` — fall
back to explicit column list if it fails (older PG in tests).” The
fallback is **not implemented** — the code unconditionally emits
`SELECT * EXCEPT(...)` and returns `SpiError("restore insert failed: ...")`
on older minor versions or non-mainstream PG distributions that have not
yet picked up the EXCEPT support.

**Recommendation:** Catalog-walk `pg_attribute` for the storage table,
build the explicit column list, and use that instead. Removes the PG-minor
sensitivity entirely.

### 3.8 `snapshot_stream_table` returns the snapshot name even when the catalog INSERT fails *(P2)*

**Where:** [`src/api/snapshot.rs:152-167`](../src/api/snapshot.rs).

The comment says the catalog INSERT is “best-effort” and the result
discards the `Result`. If the user later calls
`pgtrickle.list_snapshots(...)` they will not see the snapshot, even
though the function returned a path. This is a UX trap — surface the
INSERT failure as a `WARNING` so the caller knows the snapshot is not
tracked.

### 3.9 SCC tarjan `.unwrap()` are sound but un-tested at scale *(P2)*

**Where:** [`src/dag.rs:1093, 1100, 1112`](../src/dag.rs).

The three `.unwrap()`s carry `nosemgrep` justifications and are correct by
the SCC algorithm invariant. But they have no fuzz coverage — a
graph-shape fuzzer (the moral equivalent of `parser_fuzz` for DAG
operations) would close the residual risk and make a change in the
recursion easy to spot.

### 3.10 `wal_decoder.rs` `.expect("'test_decoding' contains no NUL bytes")` *(P2)*

**Where:** [`src/wal_decoder.rs:307`](../src/wal_decoder.rs).

`CString::new("test_decoding").expect("'test_decoding' contains no NUL bytes")`
is sound (the literal has no NUL bytes), but it is the **only remaining
`.expect()` in production code** outside `api/mod.rs`'s `unreachable!()`
post-`report()` calls. Replace with `c"test_decoding"` (compile-time
guarantee) or with `?` and a typed error to set the precedent for the
rest of the WAL decoder.

---

## 4. Architecture Gaps

### 4.1 The unused L0 template cache was removed

**Where:** [`src/dvm/mod.rs:898`](../src/dvm/mod.rs).

The process-local L0 template cache and its shared-memory availability signal
had no readers and were removed. A cold backend still pays:

- L1 thread-local lookup → miss (cold backend)
- L2 catalog SELECT → ~1 ms hit, cached by PG buffer cache

A dshash-backed L0 cache from the
[PLAN_OVERALL_ASSESSMENT_2.md](PLAN_OVERALL_ASSESSMENT_2.md) §4 remains a
possible optimization; measure the cold-path cost before implementing it.

### 4.2 Refresh sub-modules still reach into each other via `pub use codegen::*`

**Where:** [`src/refresh/mod.rs:33-43`](../src/refresh/mod.rs).

The migration to ARCH-1B preserved binary compatibility by re-exporting
everything (`pub use codegen::*; pub use merge::*; pub use orchestrator::*;`).
This means a function moved between sub-modules still appears to live in
`refresh::`, which is good for caller stability but bad for module
discipline — any new function in any sub-module is automatically promoted
to the top-level `refresh` namespace, with no curation. Recommend
introducing an explicit re-export list (`pub use codegen::{A, B, C, …};`)
within v0.30.0.

### 4.3 IVM lock-mode parser is conservative but undocumented

**Where:** [`src/ivm.rs:48-91`](../src/ivm.rs).

`IvmLockMode::for_query` walks the parsed OpTree and returns
`RowExclusive` only when the tree is `Scan → Filter? → Project?` —
everything else gets `Exclusive`. This is correct but **fail-closed**:
any parse failure also returns `Exclusive`. There is no observability
into how often the IVM path runs in `Exclusive` because of a parser
giving up on a query the rest of the engine handles fine.

**Recommendation:** Add a counter
`pgtrickle_ivm_lock_mode{mode="exclusive_due_to_parse_error"}` and emit
it via the metrics_summary aggregation.

### 4.4 No bounded-memory protection for the parser itself

**Where:** [`src/dvm/parser/mod.rs:120-230`](../src/dvm/parser/mod.rs).

`pg_trickle.max_parse_depth` (G13-SD) bounds recursion *depth* but not
total *memory*. A large `IN (1, 2, 3, …, 1_000_000)` list still allocates
proportionally. PostgreSQL itself bounds these via memory contexts, but
the Rust-side parse caches (`PARSE_ADVISORY_WARNINGS`,
`cte_ctx.registry`) live in the backend heap, not in a memory context,
and never shrink within a long-running session.

**Recommendation:** Track aggregate node count per parse, reject above
`pg_trickle.max_parse_nodes` (new GUC, default 100k), and clear
thread-locals at end-of-statement via a `XactCallback`.

### 4.5 Catalog reads remain hot-path

The scheduler reads `pgtrickle.pgt_stream_tables` on every tick (already
known and partly mitigated by v0.25's cached snapshot). The new
`metrics_summary()` and `cluster_worker_summary()` re-query the catalog
on every Prometheus scrape. At 200+ stream tables and a 5-second scrape
interval, this is one full table scan per database every 5 s on top of
the scheduler's own reads. A shared snapshot that both surfaces
write would eliminate redundant reads.

### 4.6 `ivm.rs` still re-uses statement-level transition tables via temp tables

**Where:** [`src/ivm.rs:30-37`](../src/ivm.rs) (comments) and the trigger
function builders below.

The comments admit “Uses temp tables for transition table access (ENR-based
access is a future optimization).” PostgreSQL 18 supports referencing
ephemeral named relations directly inside trigger bodies, so the
intermediate temp table is unnecessary overhead.

### 4.7 No multi-database singleton for the scheduler

The launcher spawns one scheduler per database. If two databases share a
high-cost source table (e.g., a foreign-data-wrapper view), each schedules
its own refresh, doubling the work. There is no notion of a “refresh
broker” at the cluster level.

### 4.8 No back-pressure between change capture and refresh

If `pgtrickle.refresh_stream_table` falls behind under a write spike, the
change buffer (`pgtrickle_changes.changes_<oid>`) grows monotonically.
`pg_trickle.buffer_alert_threshold` warns but does not throttle the source
DML. A real back-pressure design (RAISE NOTICE → application slow-down) is
out of scope here, but the absence is worth flagging.

### 4.9 ARCH-1B left dead `#[allow(unused_imports)]` shims

**Where:** [`src/refresh/orchestrator.rs:6-26`](../src/refresh/orchestrator.rs).

Every import is `#[allow(unused_imports)]`. This is a maintenance hazard:
removing a dependency from a function will not produce a clippy warning,
and the imports become silently misleading. Recommend converting to
specific `pub use` clauses now that the module split has stabilised.

### 4.10 No `Drop` guards on `Spi::run_with_args` failure inside ivm.rs

If an IVM trigger function fails mid-statement, the `__pgt_newtable_<oid>`
/ `__pgt_oldtable_<oid>` temp tables are normally cleaned up by PG's
sub-transaction abort, but the thread-local `IVM_DELTA_CACHE` is **not**
cleared. A stale entry can survive a failed apply and be reused in the
next statement. Mitigation: clear the thread-local in a `XactCallback`
on subxact abort.

---

## 5. Performance

### 5.1 Existing benchmarks

Three Criterion benches:

- [`benches/diff_operators.rs`](../benches/diff_operators.rs) — covers
  `Scan`, `Filter`, `Project`, `Aggregate`, `Join`, `Distinct`, `Window`
  via `DiffContext::new_standalone`.
- [`benches/refresh_bench.rs`](../benches/refresh_bench.rs) — covers
  `quote_ident`, `col_list`, `Expr::to_sql`, `OpTree::output_columns`,
  `OpTree::source_oids`, `Frontier::lsn_gt`,
  `select_canonical_period_secs`.
- [`benches/scheduler_bench.rs`](../benches/scheduler_bench.rs) — covers
  500-stream-table dispatch (PERF-5).

### 5.2 Performance gaps

| Area | Bench? | Notes |
|---|---|---|
| Snapshot/restore round-trip | ❌ | New SNAP API has zero benchmark coverage. |
| Predictive scheduler `recommend_schedule` | ❌ | PLAN-1 reads refresh history via SPI; cost grows with retention window. |
| Cluster-wide observability | ❌ | `cluster_worker_summary()` and `metrics_summary()` re-query the catalog per scrape. |
| L2 template-cache lookup hit-rate at scale | ❌ | No microbench measuring 1ms vs 45ms cold cost. |
| IVM apply path | ❌ | `pgt_ivm_apply_delta` has no bench. Repo memory says IVM is on the hot path for some users. |
| WAL decoder throughput | ❌ | No bench for the polling loop. |
| Multi-database fairness | ❌ | Quota formula is documented but unmeasured under contention. |

### 5.3 Known wins shipped since v0.23

- 50-80% write-path overhead reduction via statement-level CDC triggers
  (v0.27.0).
- Cold-backend ~45 ms → ~1 ms via L2 template cache (v0.25.0).
- Differential refresh adaptive threshold (`differential_max_change_ratio`)
  prevents pathological FULL fallback cycles.
- Frontier two-phase commit removes the “silent stale frontier” class of
  no-op refreshes.

### 5.4 Recommendation

Add one benchmark per missing row in §5.2 before v1.0. Priority: SNAP and
predictive planner, which are the newest code paths.

---

## 6. Security (OWASP Mapped)

| OWASP Top 10 (2021) | Status | Notes |
|---|---|---|
| A01 — Broken Access Control | **Hardened.** v0.13 SECURITY DEFINER triggers + `SET search_path`; `PgTrickleError::PermissionDenied` separates SEC-1 from SPI permission errors. | [`src/error.rs:65-67`](../src/error.rs) (`PermissionDenied`); [`src/error.rs:88-93`](../src/error.rs) (`SpiPermissionError`). |
| A02 — Cryptographic Failures | N/A — extension does not handle credentials. |
| A03 — Injection | **Mostly hardened.** All user-table identifiers go through `quote_ident`; the few `Spi::run(&dynamic_format)` sites carry `nosemgrep` justifications and pass only catalog-resolved double-quoted identifiers ([`src/api/snapshot.rs:240, 252`](../src/api/snapshot.rs)). **Risk:** the L2 template cache stores SQL strings; if an attacker can control `defining_query`, they control what's later replayed at refresh time — but creating a stream table requires elevated privileges. |
| A04 — Insecure Design | **Watch.** Snapshot/restore (§3.2) is not transaction-safe by design. |
| A05 — Security Misconfiguration | **Watch.** New v0.27.0 GUCs (`schedule_recommendation_min_samples`, `schedule_alert_cooldown_seconds`, `metrics_request_timeout_ms`) are not yet documented in CONFIGURATION.md (see §8 / Appendix C). Operators may not know what they default to. |
| A06 — Vulnerable Components | **Watch.** pgrx 0.18 is current; `cargo deny` is configured ([`deny.toml`](../deny.toml)). Recommend a scheduled `cargo audit` job. |
| A07 — Identification & Auth Failures | N/A. |
| A08 — Software & Data Integrity | **Risk.** Snapshot files include `__pgt_snapshot_version` for compatibility, but no checksum or signature; a tampered snapshot can be restored without detection. |
| A09 — Logging & Monitoring | **Hardened.** OpenMetrics + per-DB labels (CLUS-2). |
| A10 — SSRF | N/A. |

**Additional security observations:**

- Trigger functions are `SECURITY DEFINER` (v0.13). Source-table DDL is
  blocked by `pg_trickle.block_source_ddl` (configurable). The audit table
  `pgtrickle.pgt_refresh_history` is INSERT-only by enforcement at
  [`src/cdc.rs:71-83`](../src/cdc.rs).
- The relay binary ([pgtrickle-relay/](../pgtrickle-relay/)) connects via
  ordinary libpq; it does not embed credentials. Good.
- Fuzz coverage exists for `cdc_fuzz`, `cron_fuzz`, `guc_fuzz`,
  `parser_fuzz` ([`fuzz/fuzz_targets/`](../fuzz/fuzz_targets/)). **Missing:**
  WAL-decoder fuzz target, MERGE-template fuzz, snapshot/restore fuzz.

---

## 7. Test Coverage Gaps

The repository ships six test tiers
([AGENTS.md](../AGENTS.md) §Testing). The table below maps the tier to
the gap.

| Area | Tier | Gap |
|---|---|---|
| Snapshot / restore atomicity | E2E | No test asserts that a mid-snapshot crash leaves no orphan tables (cf. §3.2). |
| Snapshot version mismatch | E2E | No test feeds a version-mismatched snapshot to `restore_from_snapshot`. |
| Predictive planner under sparse history | Integration | No test asserts `recommend_schedule` returns confidence=0 when N < `schedule_recommendation_min_samples`. |
| IVM cache memory growth | Soak | No long-running test asserts `IVM_DELTA_CACHE.len()` is bounded over many ALTER QUERY cycles. |
| L2 template cache eviction | Integration | No test asserts the L2 catalog table size stays bounded across 1000 stream-table churn cycles. |
| Multi-database worker fairness | E2E | No test asserts the per-DB quota under contention from N parallel writer connections. |
| WAL decoder failure injection | E2E | No test asserts behaviour when the replication slot is missing or has wrong plugin. |
| Statement-level CDC with mixed INSERT/UPDATE/DELETE in one transaction | E2E | The new triggers split into 3 functions; assert ordering invariants. |
| `classify_spi_error_retryable` localised messages | Unit | No test runs with `lc_messages=fr_FR.UTF-8`; the heuristic silently regresses. |
| Snapshot under concurrent refresh | E2E | No test runs `snapshot_stream_table` while a refresh is in flight. |
| `restore_from_snapshot` rollback on partial INSERT failure | E2E | No test injects a constraint violation mid-restore. |
| SubLink-in-CASE silently downgraded to FULL | Integration | No test surfaces this as a NOTICE. |
| EC-01 cross-cycle phantom convergence | E2E + soak | The existing `test_tpch_q07_*` is flaky per repo memory; needs a deterministic reproducer. |
| Snapshot interaction with `pg_dump` / `pg_restore` | E2E | No test pipes a snapshot through `pg_dump` and asserts round-trip. |
| Fuzz: WAL decoder | Fuzz | Not present. |
| Fuzz: MERGE template generator | Fuzz | Not present. |
| Fuzz: snapshot SQL builder | Fuzz | Not present. |
| Fuzz: DAG SCC graphs | Fuzz | Not present. |
| Property test: cluster_worker_summary consistency under crash | Property | None. |
| Property test: predictive planner monotone in history length | Property | None. |
| dbt 1.11 compatibility | Integration | dbt-pgtrickle pins `dbt-core ~=1.10`; no test runs against 1.11+ (CI gate?). |
| CNPG 1.29 manifest | Integration | `cnpg/cluster-example.yaml` declares 1.28+; no test runs against 1.29. |

**Recommendation:** Open a `tests/e2e_snap_atomicity_tests.rs` to cover §3.2,
a `tests/e2e_ivm_cache_growth_tests.rs` to cover §3.3, and add four new
fuzz targets.

---

## 8. Documentation Gaps

| Doc | Gap | Severity |
|---|---|---|
| [`docs/UPGRADING.md`](../docs/UPGRADING.md) | The TOC and “Supported Upgrade Paths” table stop at 0.13.0 → 0.14.0. v0.15.0–v0.27.0 upgrade notes are entirely missing. The table at line 502 ends at `0.13.0 | 0.14.0`. | **High** |
| [`docs/CONFIGURATION.md`](../docs/CONFIGURATION.md) | New v0.27.0 GUCs (`schedule_recommendation_min_samples`, `schedule_alert_cooldown_seconds`, `metrics_request_timeout_ms`) are not in the TOC and have no individual sections. The deprecated `merge_planner_hints`/`merge_work_mem_mb` are listed but the deprecation warning text is not documented. | **High** |
| [`docs/SQL_REFERENCE.md`](../docs/SQL_REFERENCE.md) | New v0.27.0 functions (`snapshot_stream_table`, `restore_from_snapshot`, `list_snapshots`, `drop_snapshot`, `recommend_schedule`, `schedule_recommendations`, `cluster_worker_summary`, `metrics_summary`) — verify each has its own section. Spot check shows `cluster_worker_summary` and `metrics_summary` are referenced from SCALING.md but not necessarily fully documented in SQL_REFERENCE.md. | **High** |
| [`docs/BACKUP_AND_RESTORE.md`](../docs/BACKUP_AND_RESTORE.md) | Snapshot section present; **does not warn** about §3.2 atomicity gap or about concurrent restore. | **Medium** |
| [`docs/SCALING.md`](../docs/SCALING.md) | Has the new v0.27.0 “Cluster-wide Worker Fairness” section; the per-DB Prometheus example is correct. **Missing:** numerical guidance for `schedule_recommendation_min_samples` and `metrics_request_timeout_ms`. | **Medium** |
| [`docs/integrations/multi-tenant.md`](../docs/integrations/multi-tenant.md) | Excellent. **Add:** worked example of a noisy-neighbour scenario and the corresponding alert. | **Low** |
| [`docs/GETTING_STARTED.md`](../docs/GETTING_STARTED.md) | Comprehensive (1,491 LOC). **Missing:** snapshot/PITR walkthrough; predictive planner advice. | **Medium** |
| [`docs/PERFORMANCE_COOKBOOK.md`](../docs/PERFORMANCE_COOKBOOK.md) | Should add a recipe: “use `recommend_schedule` to right-size a 100-table dbt project.” | **Low** |
| [`docs/PRE_DEPLOYMENT.md`](../docs/PRE_DEPLOYMENT.md) | Does not mention `change_buffer_durability` or `frontier_holdback_*` GUCs. | **Medium** |
| [`docs/FAQ.md`](../docs/FAQ.md) | No entry for “How do I take a snapshot?” or “How do I tune `schedule_recommendation_min_samples`?” | **Low** |
| [ROADMAP.md](../ROADMAP.md) | The v0.29.0 row in the milestone table at line 89 contains a copy-paste typo: it repeats v0.27.0's description (“Operability, observability & DR”) instead of describing the Relay CLI. | **Low** |
| [`docs/ERRORS.md`](../docs/ERRORS.md) | New error variants (SnapshotAlreadyExists, SnapshotSourceNotFound, SnapshotSchemaVersionMismatch, DiagnosticError, PublicationAlreadyExists, PublicationNotFound, PublicationRebuildFailed, SlaTooSmall, ChangedColsBitmaskFailed) need documented HINT/DETAIL guidance. | **High** |
| [`docs/RELEASE.md`](../docs/RELEASE.md) | Should now include the SNAP/PLAN/CLUS/METR migration script as a checklist item. | **Low** |
| [`docs/PLAYGROUND.md`](../docs/PLAYGROUND.md) | Should add a snapshot demo. | **Low** |
| [`dbt-pgtrickle/README.md`](../dbt-pgtrickle/README.md) | Macro library is unchanged in v0.27 — but the README does not say which extension version it is tested against. | **Low** |

---

## 9. Operational / Ecosystem

1. **CNPG manifest version bump.** [`cnpg/cluster-example.yaml`](../cnpg/cluster-example.yaml)
   targets CNPG 1.28+. CNPG 1.29 ships in 2026-Q2; the example should be
   refreshed and re-tested in CI before v1.0.
2. **dbt-pgtrickle dbt-core compatibility.**
   [`dbt-pgtrickle/AGENTS.md`](../dbt-pgtrickle/AGENTS.md) pins `~=1.10`
   and explicitly notes Python 3.13 only because dbt 1.10's mashumaro
   dep cannot build on 3.14. dbt-core 1.11 is in beta. Open an issue
   tracking the upgrade path.
3. **Helm chart.** No first-party Helm chart yet; the CNPG path is the
   only documented K8s deployment.
4. **SQL API parity.** New SNAP/PLAN/CLUS/METR functions
   (`snapshot_stream_table`, `restore_from_snapshot`, `list_snapshots`,
   `recommend_schedule`, `schedule_recommendations`,
   `cluster_worker_summary`, `metrics_summary`) are only accessible via SQL.
5. **Grafana dashboards.** Per-DB `db_oid` / `db_name` labels (CLUS-2)
   are documented but no first-party Grafana dashboard JSON ships in
   `monitoring/grafana/`. The dashboard snippets in
   [docs/integrations/multi-tenant.md](../docs/integrations/multi-tenant.md)
   are the only artefact.
6. **OpenTelemetry.** No tracing spans yet — every pg_trickle operation
   is opaque to OTel collectors. v1.0 should at minimum emit per-refresh
   spans.
7. **Backup tooling.** The bin target [`src/bin/pg_trickle_dump.rs`](../src/bin/pg_trickle_dump.rs)
   (458 LOC) is the only out-of-database backup tool; it is not yet
   documented in `docs/BACKUP_AND_RESTORE.md`.
8. **Multi-version test matrix.** The CI matrix runs PostgreSQL 18.x; PG
   17 support is on the v1.1 roadmap but not in CI today.
9. **Connection pooler matrix.** PgBouncer transaction-mode is supported
   via `pg_trickle.connection_pooler_mode = 'transaction'`. Pgcat / Odyssey
   are untested.
10. **Multi-database soak test.** The G17-MDB workflow exists; it should
    be promoted from `stability-tests.yml` to the main `ci.yml` for v1.0
    GA.
11. **Release artefact signing.** GHCR images are not yet
    cosign-signed.
12. **Marketplace / extension catalog.** PG Extension Network listing
    not yet pursued.

---

## 10. Recommended New Features

The following ten ideas are ranked roughly by user value × effort. Each
maps to a v0.28+ release window.

### 10.1 Reactive subscription API

**Idea:** A `pgtrickle.subscribe(stream_table, channel)` SQL function
that emits `NOTIFY` payloads on every commit affecting a stream table.
Clients use ordinary `LISTEN`. v0.27.0's
[`src/api/publication.rs`](../src/api/publication.rs) already builds the
plumbing for downstream publication; the reactive API is the next step.

**Cost:** ~1 sprint. **Value:** unlocks browser-side reactive UIs without
needing Apollo, Hasura, or Postgraphile.

**Risk:** Need to coalesce notifies to avoid storms.

### 10.2 Temporal IVM (time-travel materialisation)

**Idea:** Allow a stream-table query to reference `AS OF TIMESTAMP $1`
(or LSN). Materialise rolling history. Useful for slowly-changing
dimensions and audit reporting.

**Cost:** ~3 sprints. Requires extending the frontier model to a
two-dimensional (frontier, ts).

**Value:** First-class SCD-Type-2 semantics out of the box.

### 10.3 Adaptive batching

**Idea:** The scheduler currently runs each ready stream table
independently. An adaptive batcher would notice that two STs share
a source table and a refresh window and run them together, reusing
the change-buffer scan.

**Cost:** ~2 sprints in `scheduler.rs`.

**Value:** 10-30% throughput win for multi-tenant deployments with
shared sources.

### 10.4 Plan-aware delta routing

**Idea:** Inspect `EXPLAIN ANALYZE` output for the delta MERGE and
choose between `merge_strategy=merge` and `merge_strategy=delete_insert`
adaptively per refresh. Today the strategy is per-stream-table.

**Cost:** ~1 sprint. Heuristics already exist in
[`src/refresh/codegen.rs`](../src/refresh/codegen.rs).

**Value:** Removes the “tune `merge_strategy` by hand” chore.

### 10.5 GraphQL / PostGraphile integration

**Idea:** A Postgraphile plugin that exposes stream tables as
subscription-capable types. v0.27's publication primitive is the wire
format.

**Cost:** ~2 sprints, mostly in JavaScript.

**Value:** “real-time GraphQL backed by PostgreSQL with no extra
infrastructure” is a unique selling point.

### 10.6 Kafka outbox pattern

**Idea:** A built-in outbox stream table type that writes deltas to a
Kafka topic instead of (or in addition to) materialising. The v0.28
roadmap entry covers this.

**Cost:** ~3 sprints (Kafka client, retry semantics, partition strategy).

**Value:** Closes the “how do I get pg_trickle results into my event
bus?” question.

### 10.7 Columnar materialisation backend

**Idea:** Allow a stream table to materialise into Citus columnar
storage or pg_mooncake. Massively reduces storage cost for analytic
workloads.

**Cost:** ~3 sprints, mostly catalog plumbing.

**Value:** Bridges OLTP and OLAP in one extension.

### 10.8 Zero-downtime view evolution

**Idea:** Today, `ALTER QUERY` triggers a full refresh. A “shadow ST”
mode would build the new query side-by-side, switch atomically, and
keep the old version readable for the duration.

**Cost:** ~2 sprints.

**Value:** Removes the operational fear of `ALTER QUERY` on large
tables.

### 10.9 LLM-assisted advisor

**Idea:** A `pgtrickle.advise()` function that takes a query and
returns recommendations: refresh mode, schedule, source-table indexes,
flagged anti-patterns. Backed by a small open model (e.g. Phi-3) called
via PL/Python.

**Cost:** ~2 sprints (mostly prompt engineering).

**Value:** Onboarding accelerator for new users.

### 10.10 Edge / embedded WASM (PGlite)

**Idea:** Compile the DVM core to WebAssembly and run inside PGlite.
Aligns with the v1.4 roadmap entry. v0.27.0's clean module split
(`refresh/`, `dvm/`, `cdc/`) is finally a tractable starting point.

**Cost:** ~4 sprints (the WASM toolchain interaction with pgrx is the
hard part).

**Value:** Reactive client-side databases with full IVM — no JavaScript
ORM needed.

---

## 11. Prioritised Action Plan

The table below lists 30 actionable items. **Items marked P0 must land
before v0.30.0.** The order within a priority band is the order I would
ship them.

| # | Item | Section | Priority | Target |
|---|---|---|---|---|
| 1 | Eliminate EC-01 cross-cycle phantoms (route every cycle through PH-D1 cleanup or fix Part 2 row-id derivation) | §3.1 | P0 | v0.28.0 |
| 2 | Wrap snapshot/restore in subxact + take exclusive lock on storage during restore | §3.2 | P0 | v0.28.0 |
| 3 | Replace `classify_spi_error_retryable` text matching with SQLSTATE-based classifier | §3.4 | P0 | v0.28.0 |
| 4 | Bound `IVM_DELTA_CACHE` via `pg_trickle.template_cache_max_entries` | §3.3 | P0 | v0.28.0 |
| 5 | Document v0.27.0 GUCs (`schedule_recommendation_min_samples`, `schedule_alert_cooldown_seconds`, `metrics_request_timeout_ms`) | §8, App C | P0 | v0.28.0 |
| 6 | Document v0.15.0–v0.27.0 upgrade notes in [docs/UPGRADING.md](../docs/UPGRADING.md) | §8 | P0 | v0.28.0 |
| 7 | Add E2E test for snapshot atomicity under crash | §7 | P0 | v0.28.0 |
| 8 | Bound L2 template-cache catalog table via age-based purge | §3.5 | P0 | v0.28.0 |
| 9 | Surface `snapshot_stream_table` catalog INSERT failure as WARNING | §3.8 | P0 | v0.28.0 |
| 10 | Replace `restore_from_snapshot`'s `SELECT * EXCEPT` with explicit column list | §3.7 | P0 | v0.28.0 |
| 11 | Implement real shared-shmem L0 dshash template cache | §4.1 | P1 | v0.29.0 |
| 12 | Bound parser memory via `pg_trickle.max_parse_nodes` GUC + `XactCallback` | §4.4 | P1 | v0.29.0 |
| 13 | Replace `refresh::*` blanket re-exports with explicit `pub use` lists | §4.2 | P1 | v0.29.0 |
| 14 | Add IVM lock-mode counter to metrics | §4.3 | P1 | v0.29.0 |
| 15 | Switch IVM transition-table access to ENR (drop temp tables) | §4.6 | P1 | v0.29.0 |
| 16 | Eliminate `wal_decoder.rs:307` `.expect()` via `c"test_decoding"` | §3.10 | P1 | v0.29.0 |
| 17 | Generalise `node_tree_contains_sublink` to recurse into CASE/COALESCE/FuncCall | §3.6 | P1 | v0.29.0 |
| 18 | Add benchmarks: SNAP, PLAN, CLUS, IVM apply, WAL decoder | §5 | P1 | v0.29.0 |
| 19 | Document new error variants (HINT/DETAIL) in [docs/ERRORS.md](../docs/ERRORS.md) | §8 | P1 | v0.29.0 |
| 20 | Update SQL_REFERENCE.md to fully cover SNAP/PLAN/CLUS/METR functions | §8 | P1 | v0.29.0 |
| 21 | Add fuzz targets: WAL decoder, MERGE template, snapshot builder, DAG SCC | §7 | P1 | v0.29.0 |
| 22 | Add E2E tests: predictive planner sparse history, multi-DB worker fairness | §7 | P1 | v0.29.0 |
| 23 | Document `change_buffer_durability` in [docs/PRE_DEPLOYMENT.md](../docs/PRE_DEPLOYMENT.md) | §8 | P1 | v0.29.0 |
| 24 | Surface CNPG 1.29 compatibility in [cnpg/](../cnpg/) and CI | §9 | P1 | v0.29.0 |
| 25 | Add SQL reference docs for SNAP/PLAN/CLUS/METR functions | §9 | P2 | v0.30.0 |
| 26 | Ship first-party Grafana dashboard JSON in `monitoring/grafana/` | §9 | P2 | v0.30.0 |
| 27 | Add OpenTelemetry tracing spans for refresh/CDC/snapshot | §9 | P2 | v1.0 |
| 28 | Adaptive batching of refreshes that share a source table | §10.3 | P2 | v0.30.0 |
| 29 | Plan-aware delta routing (auto-pick `merge_strategy`) | §10.4 | P2 | v0.30.0 |
| 30 | Cosign-sign GHCR release images | §9 | P2 | v1.0 |

---

## 12. Risk Register

| ID | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R-01 | EC-01 phantom drift causes silent data corruption in production | Medium | Critical | Action items #1, #7 |
| R-02 | Snapshot crash leaves orphan tables, fills disk | Medium | High | Action items #2, #7 |
| R-03 | Localised PG build silently retries non-retryable SPI errors | High (if deployed in fr/de/ja) | Medium | Action item #3 |
| R-04 | Long-lived backend memory growth via IVM cache | Medium | Medium | Action item #4 |
| R-05 | L2 template-cache catalog table grows unbounded | Low | Medium | Action item #8 |
| R-06 | Operator does not know snapshot was un-tracked | Medium | Low | Action item #9 |
| R-07 | Restore fails on non-mainstream PG 18 build | Low | Medium | Action item #10 |
| R-08 | New v0.27.0 GUCs are misconfigured because undocumented | Medium | Medium | Action items #5, #19, #20 |
| R-09 | dbt-pgtrickle blocks dbt 1.11 adoption | Medium | Low | Track upstream dbt 1.11 release |
| R-10 | EC-01 fix re-introduces another regression in TPC-H Q15 | Medium | Medium | Property test + soak test (item #1) |
| R-11 | pgrx 0.18 → 0.19 rev (when 0.19 ships) breaks pg_trickle | Medium | Medium | Pin strict, plan dedicated upgrade window |
| R-12 | Per-DB worker quota starves a noisy-neighbour database | Medium | Medium | `cluster_worker_summary` Grafana alert |

---

## Appendix A — Unsafe Block Inventory

282 `unsafe { … }` blocks total across [`src/`](../src/). A heuristic
audit (preceding line contains `// SAFETY:`) finds **40** unsafe blocks
**without an immediately preceding SAFETY comment**. The vast majority
of these are inside the `cast_node!` and `pg_deref!` macros in
[`src/dvm/parser/mod.rs:80-160`](../src/dvm/parser/mod.rs), which carry
the SAFETY comment **inside** the macro body — these are sound. The
remaining sites worth a closer audit are:

- [`src/dvm/parser/mod.rs:60-90`](../src/dvm/parser/mod.rs) — `cast_node!`,
  `is_node_type!`, `pg_deref!` macros. SAFETY comments are inside the
  macros, but the call sites do not repeat them. The G13-PRF refactor
  that introduced the macros explicitly accepts this trade-off.
- [`src/dvm/parser/sublinks.rs`](../src/dvm/parser/sublinks.rs) —
  numerous unsafe-block call sites with sub-comments referring back
  to “Parse-tree pointer from PostgreSQL's raw_parser; valid within
  current memory context.” Sound.
- [`src/wal_decoder.rs:300-390`](../src/wal_decoder.rs) — All blocks
  carry SAFETY justifications referencing the PG replication-slot API
  contract. Sound.
- [`src/scheduler.rs:250-308`](../src/scheduler.rs) (`SubTransaction`
  RAII) — Every block carries a SAFETY comment. Sound.
- [`src/api/helpers.rs:560-1010`](../src/api/helpers.rs) (parse-tree
  walker) — All blocks carry SAFETY comments. Sound.

**No** unsafe block was found that lacks justification entirely.

The `#![cfg_attr(not(test), deny(clippy::unwrap_used))]` lint at
[`src/lib.rs:23`](../src/lib.rs) keeps `unwrap()` out of production
code. The 31 `unreachable!()` calls in
[`src/api/mod.rs`](../src/api/mod.rs) are all unreachable-after-`report()`
and are sound.

---

## Appendix B — Undocumented SQL Functions

93 `#[pg_extern]` functions in production source (per file):

| File | Count |
|---|---|
| [`src/api/diagnostics.rs`](../src/api/diagnostics.rs) | 30 |
| [`src/monitor.rs`](../src/monitor.rs) | 19 |
| [`src/api/mod.rs`](../src/api/mod.rs) | 9 |
| [`src/diagnostics.rs`](../src/diagnostics.rs) | 5 |
| [`src/api/self_monitoring.rs`](../src/api/self_monitoring.rs) | 5 |
| [`src/api/helpers.rs`](../src/api/helpers.rs) | 5 |
| [`src/api/snapshot.rs`](../src/api/snapshot.rs) | 4 |
| [`src/api/publication.rs`](../src/api/publication.rs) | 3 |
| [`src/lib.rs`](../src/lib.rs) | 2 |
| [`src/ivm.rs`](../src/ivm.rs) | 2 |
| [`src/hooks.rs`](../src/hooks.rs) | 2 |
| [`src/hash.rs`](../src/hash.rs) | 2 |
| [`src/api/planner.rs`](../src/api/planner.rs) | 2 |
| [`src/error.rs`](../src/error.rs) | 1 |
| [`src/api/metrics_ext.rs`](../src/api/metrics_ext.rs) | 1 |
| [`src/api/cluster.rs`](../src/api/cluster.rs) | 1 |

The functions confirmed shipped by [the v0.27.0 migration script](../sql/pg_trickle--0.26.0--0.27.0.sql)
are all under `pgtrickle.*`:

- `snapshot_stream_table(p_name text, p_target text DEFAULT NULL) → text`
- `restore_from_snapshot(p_name text, p_source text) → void`
- `list_snapshots(p_name text) → TABLE(snapshot_table text, created_at timestamptz, row_count bigint, frontier jsonb, size_bytes bigint)`
- `drop_snapshot(p_snapshot_table text) → void`
- `recommend_schedule(p_name text) → jsonb`
- `schedule_recommendations() → TABLE(name text, current_interval_seconds float8, recommended_interval_seconds float8, delta_pct float8, confidence float8, reasoning text)`
- `cluster_worker_summary() → TABLE(db_oid bigint, db_name text, active_workers int, scheduler_pid int, scheduler_running bool, total_active_workers int)`
- `metrics_summary() → TABLE(db_name text, total_stream_tables bigint, active_stream_tables bigint, suspended_stream_tables bigint, total_refreshes bigint, successful_refreshes bigint, failed_refreshes bigint, total_rows_processed bigint, active_workers int)`

A spot check of [`docs/SQL_REFERENCE.md`](../docs/SQL_REFERENCE.md) shows
`fuse_status`, `trigger_inventory`, and `parallel_job_status` are
documented; the new SNAP/PLAN/CLUS/METR functions need verification that
they all have full sections. This is action item #20.

---

## Appendix C — Undocumented GUCs

The new v0.27.0 GUCs registered in [`src/config.rs`](../src/config.rs)
that **do not appear** in the [docs/CONFIGURATION.md](../docs/CONFIGURATION.md)
TOC:

- `pg_trickle.schedule_recommendation_min_samples` (default 20) — gates
  PLAN-1 confidence
- `pg_trickle.schedule_alert_cooldown_seconds` (default 300) — throttles
  PLAN-3 alerts
- `pg_trickle.metrics_request_timeout_ms` (default 5000) — bounds
  metrics endpoint scrape
- `pg_trickle.change_buffer_durability` (`unlogged|logged|sync`,
  default `unlogged`) — referenced but not in TOC
- `pg_trickle.frontier_holdback_lsn_bytes` (gauge) — internal, but
  noted in [`src/shmem.rs`](../src/shmem.rs)
- `pg_trickle.template_cache_max_entries` — referenced by L1, not by L2
- `pg_trickle.worker_pool_size` — referenced indirectly via
  `max_dynamic_refresh_workers`
- `pg_trickle.unlogged_buffers` (default `false`) — documented in
  upgrade notes for v0.14, but not in CONFIGURATION.md
- `pg_trickle.agg_diff_cardinality_threshold` (default 1000) — in TOC
  but no individual section verified
- `pg_trickle.cost_model_safety_margin` — in TOC but no individual
  section verified

These need full sections in [docs/CONFIGURATION.md](../docs/CONFIGURATION.md)
following the existing template (Property/Default/Range/Context table +
Tuning Guidance + example).

---

*End of assessment.*
