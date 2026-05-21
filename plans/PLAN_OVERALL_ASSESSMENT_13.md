# pg_trickle — Overall Assessment 13

> Status: Exhaustive multi-pass engineering audit
> Date: 2026-05-21
> Scope: Full repository, current `main`, pg_trickle v0.67.0
> Method: Static source review, SQL archive review, CI/workflow review, test-map review, and targeted verification against Report 12 findings

---

## Executive Summary

pg_trickle has advanced materially since Overall Assessment 12. Several of the
previous high-risk findings are now fixed: ownership checks exist in the outbox
and publication APIs, recursive CTE depth is guarded in differential execution,
row-id schema verification is wired into delta generation, compaction contention
is observable, `refresh_efficiency()` is tested, backup/restore guidance exists,
and the CI matrix is much broader than it was in the v0.57 era. The project is
not drifting; it is actively burning down assessment debt.

This audit therefore avoids re-reporting stale issues. The current risk profile
has shifted toward newer v0.62-v0.67 surfaces: fused refresh, DuckLake sink
integration, durability GUC evolution, scheduler pool leftovers, generated API
documentation, and test-harness drift.

The most serious findings are concrete and actionable:

1. Fused refresh records use `initiated_by = 'SCHEDULER_FUSED'`, but the
   `pgt_refresh_history` CHECK constraint allows only `SCHEDULER`, `MANUAL`,
   `INITIAL`, and `SELF_MONITOR`. The insert fails after the fused SQL has run,
   so fused refresh can complete without an audit row.
2. LATERAL raw SQL is skipped by the volatility and support validators. A query
   can hide `random()`, `clock_timestamp()`, or unsupported constructs inside a
   LATERAL SRF/subquery and still pass the `volatile_function_policy = reject`
   gate for DIFFERENTIAL mode.
3. The new `pg_trickle.change_buffer_durability` GUC is effectively unwired.
   Buffer creation still consults the older `pg_trickle.unlogged_buffers` bool,
   while the new GUC advertises `unlogged` as default and `sync` as supported.
4. DuckLake timestamp columns are silently serialized as NULL because storage
   rows are fetched via `::text` and the Parquet writer parses timestamps as
   integer microseconds.
5. The DuckLake sink is a warning-only post-refresh side effect with no retry,
   status, metric, or failure accounting. For a feature that markets data-lake
   delivery, this is not operationally durable enough.
6. The persistent scheduler pool path references non-existent catalog columns
   (`db_name`, `completed_at`) and invalid statuses (`COMPLETED`, `FAILED`). It
   is currently stale/dead code, but if enabled it fails immediately.
7. The monitor's CDC buffer health check still performs one SPI `COUNT(*)` per
   tracked source every scheduler loop, which is exactly the kind of fan-out the
   scheduler-throughput releases were meant to eliminate.
8. The generated SQL API catalog is formally checked for drift but still emits
   truncated return signatures such as `TableIterator<` and `Result<`, so the
   generator is preserving broken output as the canonical file.

Overall judgment: pg_trickle is a sophisticated and unusually well-tested
PostgreSQL extension, but it is not yet at a world-class v1.0 release bar. The
core DVM engine is stronger than the newest integration edges. The next hardening
cycle should be narrow: fix fused refresh auditability, make durability settings
truthful and wired, close LATERAL validation bypasses, add DuckLake sink delivery
semantics, and delete or repair stale scheduler pool code.

---

## Severity Issue Lists

### Critical

No confirmed CRITICAL findings in this pass. I did not find a current, proven
data-loss path in the already-committed PostgreSQL storage layer. The closest
risks are HIGH because they are either silent audit loss, silent external-sink
data corruption, or policy bypass rather than direct primary data loss.

### High

| ID | Area | Title |
|----|------|-------|
| ARCH-001 | Architecture | Durability configuration is split between an old bool and a new enum |
| ARCH-002 | Architecture | DuckLake sink is modeled as a best-effort post-refresh hook |
| COR-001 | Correctness | Fused refresh writes an invalid `initiated_by` value |
| COR-002 | Correctness | LATERAL raw SQL bypasses volatility and support validation |
| COR-003 | Correctness | `change_buffer_durability` is advertised but not wired |
| COR-004 | Correctness | DuckLake timestamp columns serialize as NULL |
| REL-001 | Reliability | DuckLake sink failures are warning-only side effects |
| SCAL-001 | Scalability | Persistent scheduler pool path is schema-incompatible stale code |
| PERF-001 | Performance | Monitor health performs per-source SPI `COUNT(*)` fan-out |
| DOC-001 | Documentation | Generated SQL API catalog truncates return signatures |

### Medium

| ID | Area | Title |
|----|------|-------|
| COR-005 | Correctness | DuckLake view registration is stale after query-only ALTER |
| COR-006 | Correctness | DuckLake snapshot ID allocation is not serialized |
| PERF-002 | Performance | Fused refresh eligibility performs per-node/per-source SPI probes |
| PERF-003 | Performance | History-prune interval GUC is unused and pruning lacks a start-time index |
| PERF-004 | Performance | `delta_work_mem_cap_mb = 0` leaves no cluster-wide memory guard |
| SCAL-002 | Scalability | Launcher polls every database and `pg_stat_activity` every 10 seconds |
| SEC-001 | Security | Publication API uses a weaker name parser than the common API helper |
| SEC-002 | Security | DuckLake catalog writes use unqualified `ducklake_view` resolution |
| TEST-001 | Test Coverage | No LATERAL volatile-function rejection coverage |
| TEST-002 | Test Coverage | `cache_stats()` lacks direct SQL/E2E coverage |
| TEST-003 | Test Coverage | Fused-refresh E2E test does not exercise scheduler fused audit path |
| TEST-004 | Test Coverage | `change_buffer_durability` has no tests |
| TEST-005 | Test Coverage | Test harness catalog schema is stale |
| OBS-001 | Observability | DuckLake sink has no health/status/lag metrics |
| OBS-002 | Observability | Failed history pruning is invisible |
| CI-001 | CI/CD | Fuzz smoke omits three committed fuzz targets |
| CI-002 | CI/CD | `just fuzz-all` masks fuzz crashes with `|| true` |
| CI-003 | CI/CD | E2E coverage workflow is manual-only despite weekly comments |
| DEP-001 | Dependencies | Advisory ignores lack expiry/review metadata |

### Low

| ID | Area | Title |
|----|------|-------|
| ARCH-003 | Architecture | The master implementation plan is no longer a reliable source of truth |
| CODE-001 | Code Quality | SQL API generator relies on regexes that cannot parse nested returns |
| CODE-002 | Code Quality | Tarjan SCC code suppresses SQL-path unwraps instead of returning errors |
| DOC-002 | Documentation | `plans/PLAN.md` is obsolete and duplicated around v0.9 status |
| DOC-003 | Documentation | `plans/INDEX.md` is stale and misses current planning/report files |
| CI-004 | CI/CD | `docs-lint` is not part of local `just lint` |
| DEP-002 | Dependencies | Arrow/Parquet/ObjectStore policy lacks compatibility rationale |

---

## Pass 1 — Architecture, Product Fit, And Invariants

### [ARCH-001] Durability configuration is split between an old bool and a new enum

File/Location: [src/config.rs](../src/config.rs), [src/cdc/mod.rs](../src/cdc/mod.rs)

Severity: HIGH

Description: The architecture now exposes two durability controls for change
buffers: legacy `pg_trickle.unlogged_buffers` and new
`pg_trickle.change_buffer_durability`. The new GUC promises `unlogged`, `logged`,
and `sync`, but the CDC table creation path still checks only
`pg_trickle_unlogged_buffers()`. The architectural contract is therefore split:
operators can set the new GUC and receive no behavior change.

Evidence: `PGS_CHANGE_BUFFER_DURABILITY` defaults to `Some(c"unlogged")` and is
registered as `pg_trickle.change_buffer_durability`, but `create_change_buffer_table`
and `create_st_change_buffer_table` select `CREATE UNLOGGED TABLE` only when
`pg_trickle_unlogged_buffers()` is true. The only references to
`pg_trickle_change_buffer_durability()` are in `config.rs`; the CDC path never
uses it.

Remediation: Make `ChangeBufferDurability` the single source of truth. Map the
legacy bool into the enum at read time, wire buffer creation and trigger-time
`synchronous_commit` handling to the enum, document the final default, and add
E2E tests for all three modes.

### [ARCH-002] DuckLake sink is modeled as a best-effort post-refresh hook

File/Location: [src/ducklake_sink.rs](../src/ducklake_sink.rs)

Severity: HIGH

Description: DuckLake delivery is implemented as `run_ducklake_sink()`, a
post-refresh side effect that logs failures and returns. This is acceptable for
an optional notification hook, but weak for a data-lake sink that users will
read as part of the system's delivery semantics.

Evidence: `run_ducklake_sink()` catches `run_ducklake_sink_inner()` errors and
emits `pgrx::warning!` only. There is no catalog status, retry queue, refresh
failure propagation, Prometheus counter, dead-letter table, or way to query the
last sink failure. The write order uploads Parquet first and then writes catalog
rows, but failed upload/catalog paths are not retained for retry.

Remediation: Introduce a sink delivery state machine: `PENDING`, `WRITING`,
`DELIVERED`, `FAILED_RETRYABLE`, `FAILED_PERMANENT`. Store attempts in a
catalog table keyed by refresh id and stream table id. Add retry/backoff,
operator-visible status, and metrics. Let users choose whether sink failure
should fail the refresh or remain asynchronous.

### [ARCH-003] The master implementation plan is no longer a reliable source of truth

File/Location: [plans/PLAN.md](PLAN.md)

Severity: LOW

Description: `plans/PLAN.md` still opens with duplicated v0.9.0 status blocks
and old milestone framing, while the project is now v0.67.0. This weakens the
planning hierarchy because contributors cannot tell which roadmap file is
authoritative.

Evidence: The file begins with repeated "Implementation Status (v0.9.0 Cycle)"
sections, including long resolved checklists. Current release arcs live in
[ROADMAP.md](../ROADMAP.md), but the master plan still reads like a historical
working document.

Remediation: Archive the old implementation plan as historical design context
or rewrite its front matter as "historical baseline". Make [ROADMAP.md](../ROADMAP.md)
and a current v1.0 readiness document the authoritative planning entry points.

---

## Pass 2 — Correctness And SQL Semantics

### [COR-001] Fused refresh writes an invalid `initiated_by` value

File/Location: [src/scheduler/mod.rs](../src/scheduler/mod.rs), [sql/archive/pg_trickle--0.67.0.sql](../sql/archive/pg_trickle--0.67.0.sql)

Severity: HIGH

Description: The fused refresh path records audit rows with
`initiated_by = 'SCHEDULER_FUSED'`, but the refresh-history table CHECK
constraint does not allow that value. The fused SQL can execute, frontiers can
be stored, and then audit insertion fails and is silently ignored.

Evidence: `try_fused_chain_refresh()` calls `RefreshRecord::insert(...,
Some("SCHEDULER_FUSED"), ...)`. The SQL archive defines
`CHECK (initiated_by IN ('SCHEDULER', 'MANUAL', 'INITIAL', 'SELF_MONITOR'))`.
The insert result is only used inside `if let Ok(rid) = refresh_id`; an error
does not abort or emit a warning.

Remediation: Either use `SCHEDULER` plus a new `merge_strategy_used` or
`refresh_reason` marker, or extend the CHECK constraint and all upgrade scripts
to include `SCHEDULER_FUSED`. Treat audit insert failure as at least a warning,
and add an E2E assertion that fused refresh creates completed history rows.

### [COR-002] LATERAL raw SQL bypasses volatility and support validation

File/Location: [src/dvm/parser/validation.rs](../src/dvm/parser/validation.rs), [src/dvm/parser/types.rs](../src/dvm/parser/types.rs)

Severity: HIGH

Description: LATERAL functions and LATERAL subqueries store their bodies as raw
SQL (`func_sql`, `subquery_sql`), but the validation walkers only recurse into
the outer child. Volatile expressions hidden inside LATERAL therefore bypass
`pg_trickle.volatile_function_policy = 'reject'`, and unsupported subquery
features inside LATERAL bypass the DVM support validator.

Evidence: `OpTree::LateralFunction` contains `func_sql`; `OpTree::LateralSubquery`
contains `subquery_sql`. In `tree_collect_volatility`, both variants are grouped
with `Distinct` and `Subquery` and only call `tree_collect_volatility(child,
worst)`. `check_ivm_support_inner` likewise validates only the child for both
LATERAL variants. Existing LATERAL tests cover ordinary SRFs/subqueries but not
volatile LATERAL bodies.

Remediation: Parse and validate the stored LATERAL SQL body, or conservatively
reject LATERAL raw SQL containing function calls unless it can be classified.
At minimum, scan `func_sql` and `subquery_sql` through the existing parser and
volatility registry. Add E2E tests for `LATERAL (SELECT random())` and
`LATERAL generate_series(...)` under `volatile_function_policy = reject`.

### [COR-003] `change_buffer_durability` is advertised but not wired

File/Location: [src/config.rs](../src/config.rs), [src/cdc/mod.rs](../src/cdc/mod.rs)

Severity: HIGH

Description: The new durability enum GUC is effectively dead from the CDC
creation path. This is correctness-adjacent because it makes operator intent
untruthful: `sync` does not enforce synchronous change-buffer writes, and
`unlogged` does not create unlogged buffers unless the old bool is also set.

Evidence: `PGS_CHANGE_BUFFER_DURABILITY` is defined and normalized, with
`ChangeBufferDurability::{Unlogged, Logged, Sync}`, but the buffer DDL uses:
`let unlogged_kw = if crate::config::pg_trickle_unlogged_buffers() { "UNLOGGED " } else { "" }`.
The test suite only covers `pg_trickle.unlogged_buffers`, not
`pg_trickle.change_buffer_durability`.

Remediation: Replace the bool check with the enum. For `Sync`, set
`synchronous_commit = on` around trigger writes or document a precise mechanism
that guarantees sync behavior. Add upgrade compatibility tests that prove the
legacy bool maps to the enum without conflicting defaults.

### [COR-004] DuckLake timestamp columns serialize as NULL

File/Location: [src/ducklake_sink.rs](../src/ducklake_sink.rs)

Severity: HIGH

Description: DuckLake sink rows are fetched by casting every column to text,
then Parquet timestamp arrays are built by parsing those text values as `i64`
microseconds. PostgreSQL timestamp text is not an integer microsecond epoch, so
timestamp/timestamptz values become NULL in the Parquet output.

Evidence: `fetch_stream_table_rows()` builds `SELECT "col"::text`. The type map
maps `timestamp`/`timestamptz` to `DataType::Timestamp(TimeUnit::Microsecond,
None)`. `write_parquet_bytes()` then does `s.parse().ok()` for timestamp
columns, producing `None` for values like `2026-05-21 12:34:56+00`.

Remediation: Fetch timestamp columns as `EXTRACT(EPOCH FROM col) * 1000000` or
use typed SPI extraction per column. Add unit tests that write a timestamp row
and read the Parquet footer/array back, asserting the timestamp value survives.

### [COR-005] DuckLake view registration is stale after query-only ALTER

File/Location: [src/api/alter.rs](../src/api/alter.rs), [src/ducklake_sink.rs](../src/ducklake_sink.rs)

Severity: MEDIUM

Description: When an existing DuckLake-sink stream table changes only its query,
`alter_stream_table()` updates pg_trickle metadata but does not update the
corresponding `ducklake_view` row. DuckLake clients may keep seeing the old view
definition until the sink configuration is altered again.

Evidence: `alter_stream_table_impl()` applies `alter_stream_table_query()` early
when `query` is present. The later `register_ducklake_view()` call is inside
`if sink.is_some() || ducklake_sink_path.is_some() || ducklake_sink_table_id.is_some()`.
If only `query` is supplied and the existing sink remains active, the
registration block is skipped.

Remediation: After a successful query migration, if `st.ducklake_sink_mode` is
active, update `ducklake_view` with the new query. Add an E2E test for
`ALTER STREAM TABLE ... query => ...` with an existing DuckLake sink.

### [COR-006] DuckLake snapshot ID allocation is not serialized

File/Location: [src/ducklake_sink.rs](../src/ducklake_sink.rs)

Severity: MEDIUM

Description: DuckLake catalog registration computes `MAX(snapshot_id) + 1`
inside insert statements without an advisory lock, table lock, or unique retry
loop. Concurrent sink writers for the same DuckLake table can race and attempt
the same snapshot id.

Evidence: `register_ducklake_data_file()` inserts `ducklake_data_file` with
`begin_snapshot = (SELECT COALESCE(MAX(snapshot_id), 0) ... ) + 1`, then inserts
`ducklake_snapshot` with another `MAX(snapshot_id) + 1`. No lock is taken on
`table_id`, and `ducklake_sink_table_id` is not constrained unique in
`pgt_stream_tables`.

Remediation: Use `pg_advisory_xact_lock(hash(table_id))` before catalog writes,
or delegate snapshot allocation to DuckLake's own transactional writer. Add a
concurrency test with two stream tables targeting the same `ducklake_sink_table_id`.

---

## Pass 3 — Performance And Hot Paths

### [PERF-001] Monitor health performs per-source SPI `COUNT(*)` fan-out

File/Location: [src/monitor/mod.rs](../src/monitor/mod.rs), [src/scheduler/scheduler_loop.rs](../src/scheduler/scheduler_loop.rs)

Severity: HIGH

Description: Every scheduler loop runs the slot/buffer health check in its own
transaction. That health check reads all tracked sources, then issues one
`SELECT count(*)` against each change buffer. At thousands of sources, this is
an avoidable SPI storm.

Evidence: `check_slot_health_and_alert()` selects all rows from
`pgtrickle.pgt_change_tracking`, loops over them, constructs each buffer name,
and calls `Spi::get_one::<i64>(&format!("SELECT count(*)::bigint FROM {buf}"))`.
The scheduler calls this function every loop in a dedicated transaction.

Remediation: Batch counts into one dynamically generated `UNION ALL` query, or
maintain approximate pending counts in catalog/shared memory during CDC writes
and refresh cleanup. Add a benchmark with 1k/10k tracked sources.

### [PERF-002] Fused refresh eligibility performs per-node/per-source SPI probes

File/Location: [src/scheduler/mod.rs](../src/scheduler/mod.rs)

Severity: MEDIUM

Description: The fused-refresh eligibility loop performs several catalog and
buffer probes per candidate node. This can erase the benefit of SQL fusion on
wide DAGs because eligibility costs grow with nodes times source count.

Evidence: `try_fused_chain_refresh()` loads metadata per `pgt_id`, checks gated
sources and watermarks per node, locks each row, calls `get_source_oids_for_st`,
loads dependencies again for ST sources, queries upstream `MAX(lsn)`, calls
`has_table_source_changes()` / `has_stream_table_source_changes()`, and, when a
max-delta cap is configured, counts each dependency buffer separately.

Remediation: Preload dependency rows and source buffer metadata once per DAG
tick. Compute pending counts in a single query. Cache `StreamTableMeta` and
frontier inputs across the fused eligibility pass.

### [PERF-003] History-prune interval GUC is unused and pruning lacks a start-time index

File/Location: [src/config.rs](../src/config.rs), [src/scheduler/scheduler_loop.rs](../src/scheduler/scheduler_loop.rs), [sql/archive/pg_trickle--0.67.0.sql](../sql/archive/pg_trickle--0.67.0.sql)

Severity: MEDIUM

Description: `pg_trickle.history_prune_interval_seconds` is registered with a
default of 60 seconds, but the scheduler uses a hard-coded 24-hour interval.
When pruning finally runs, it filters by `start_time` without a leading
`start_time` index.

Evidence: `PGS_HISTORY_PRUNE_INTERVAL_SECONDS` and
`pg_trickle_history_prune_interval_seconds()` exist, but the only scheduler gate
is `const HISTORY_CLEANUP_INTERVAL_MS: u64 = 24 * 60 * 60 * 1000`. The prune SQL
uses `WHERE start_time < now() - make_interval(days => $1) LIMIT $2`. The SQL
archive has indexes on `(pgt_id, data_timestamp)` and `(pgt_id, start_time)`,
not on `start_time` as the leading key.

Remediation: Use the configured interval. Add `CREATE INDEX ... ON
pgtrickle.pgt_refresh_history (start_time, refresh_id)` or partition history by
time. Add tests that set `history_prune_interval_seconds` and verify cleanup
runs at that cadence.

### [PERF-004] `delta_work_mem_cap_mb = 0` leaves no cluster-wide memory guard

File/Location: [src/config.rs](../src/config.rs), [src/refresh/codegen.rs](../src/refresh/codegen.rs)

Severity: MEDIUM

Description: Planner hints can raise `work_mem` for large deltas and deep joins,
but the cap defaults to disabled. In a parallel-refresh deployment, multiple
workers can each raise work memory without a cluster-level guard.

Evidence: `PGS_DELTA_WORK_MEM_CAP_MB` defaults to `0`, documented as "no limit
enforced". `apply_planner_hints()` raises work_mem to configured values and only
falls back to FULL if the cap is non-zero and exceeded.

Remediation: Choose a conservative non-zero default, or compute a cap from
`max_parallel_workers * work_mem` and available memory. At minimum, surface a
preflight warning when planner-aggressive mode is enabled and the cap is zero.

---

## Pass 4 — Scalability, Scheduling, And Concurrency

### [SCAL-001] Persistent scheduler pool path is schema-incompatible stale code

File/Location: [src/scheduler/pool.rs](../src/scheduler/pool.rs), [sql/archive/pg_trickle--0.67.0.sql](../sql/archive/pg_trickle--0.67.0.sql)

Severity: HIGH

Description: The persistent pool worker implementation is out of sync with the
actual `pgt_scheduler_jobs` schema and status enum. It appears uncalled today,
but if it is enabled or reused, it fails on first job claim/complete.

Evidence: `execute_pool_worker_tick()` filters queued jobs with `AND db_name =
'{db}'`, but `pgt_scheduler_jobs` has no `db_name` column. It marks completion
using `completed_at = now()`, but the table column is `finished_at`. It also
uses statuses `COMPLETED` and `FAILED`, while the CHECK constraint allows
`SUCCEEDED`, `RETRYABLE_FAILED`, `PERMANENT_FAILED`, and `CANCELLED`.

Remediation: Delete the persistent pool path if dynamic workers are the sole
supported mode. Otherwise, add the missing schema fields through migrations,
align statuses with `JobStatus`, and add an E2E test that starts pool workers
and processes a job.

### [SCAL-002] Launcher polls every database and `pg_stat_activity` every 10 seconds

File/Location: [src/scheduler/scheduler_loop.rs](../src/scheduler/scheduler_loop.rs)

Severity: MEDIUM

Description: The launcher scans all connectable databases and then scans
`pg_stat_activity` every 10 seconds. This is simple and robust, but it scales
with database count even when pg_trickle is installed in only a few databases.

Evidence: The launcher loop runs `SELECT datname FROM pg_database WHERE NOT
datistemplate AND datallowconn`, then `SELECT datname FROM pg_stat_activity
WHERE backend_type = 'pg_trickle scheduler'`, then loops over every database,
sleeping only 10 seconds between passes.

Remediation: Add a small launcher catalog or shared-memory cache of databases
where pg_trickle is installed. Increase polling interval dynamically when no DAG
invalidations occur. Consider event-trigger nudges for extension create/drop
instead of steady polling.

---

## Pass 5 — Security, Privileges, And Namespace Safety

### [SEC-001] Publication API uses a weaker name parser than the common API helper

File/Location: [src/api/publication.rs](../src/api/publication.rs), [src/api/helpers.rs](../src/api/helpers.rs)

Severity: MEDIUM

Description: The publication module has a private `parse_qualified_name()` that
defaults unqualified names to `public` and splits on the first dot. The common
helper defaults to `current_schema()` and returns a `Result`. This inconsistency
creates surprising behavior for users in non-public schemas and mishandles
quoted identifiers containing dots.

Evidence: `publication.rs` returns `(public, name)` for unqualified names and
does not return errors. `helpers.rs` uses `SELECT current_schema()::text` for
unqualified names. Both split text names rather than using `regclass`, but the
publication variant is strictly weaker.

Remediation: Remove the local parser and use the shared helper or a
`regclass`-based resolver. Add tests for `SET search_path`, quoted identifiers,
and names containing dots.

### [SEC-002] DuckLake catalog writes use unqualified `ducklake_view` resolution

File/Location: [src/ducklake_sink.rs](../src/ducklake_sink.rs)

Severity: MEDIUM

Description: DuckLake view registration checks for a table named
`ducklake_view` without constraining schema, then inserts into unqualified
`ducklake_view`. In a database with multiple schemas or an unexpected
`search_path`, this can write to the wrong table.

Evidence: `ducklake_view_table_exists()` queries `information_schema.tables`
with only `WHERE table_name = 'ducklake_view'`. `register_ducklake_view_inner()`
then runs `INSERT INTO ducklake_view ...` with no schema qualification.

Remediation: Resolve the DuckLake catalog schema explicitly. If DuckLake's
schema is configurable, store it in a GUC and quote it. Otherwise query
`pg_class`/`pg_namespace` for the exact expected schema and use a qualified
identifier in all writes.

---

## Pass 6 — Test Coverage And Verification Depth

### [TEST-001] No LATERAL volatile-function rejection coverage

File/Location: [tests/e2e_lateral_tests.rs](../tests/e2e_lateral_tests.rs), [tests/e2e_lateral_subquery_tests.rs](../tests/e2e_lateral_subquery_tests.rs), [tests/e2e_error_tests.rs](../tests/e2e_error_tests.rs)

Severity: MEDIUM

Description: The test suite has strong baseline volatility tests and extensive
LATERAL correctness tests, but no test combining the two. This is exactly the
gap that allows COR-002.

Evidence: `e2e_error_tests.rs` verifies top-level `random()` and volatile
operators are rejected. LATERAL tests cover SRFs, subqueries, left join lateral,
aggregation, and mixed DML. Searches found no test for volatile expressions
inside LATERAL SRFs/subqueries.

Remediation: Add tests for `SELECT ... FROM t, LATERAL (SELECT random()) x`,
`LEFT JOIN LATERAL`, and `LATERAL generate_series(1, random()::int)` under
DIFFERENTIAL and IMMEDIATE policy gates.

### [TEST-002] `cache_stats()` lacks direct SQL/E2E coverage

File/Location: [src/monitor/mod.rs](../src/monitor/mod.rs), `tests/`

Severity: MEDIUM

Description: `cache_stats()` exposes shared-memory and per-backend template
cache counters but has no direct test validating shape or monotonic behavior.

Evidence: `cache_stats()` is present as `#[pg_extern(schema = "pgtrickle",
name = "cache_stats")]`. Searches under `tests/**` found no call to
`pgtrickle.cache_stats()` or `cache_stats(`. By contrast, `refresh_efficiency()`
does have E2E coverage.

Remediation: Add a smoke test asserting the function returns one row with all
expected columns, then a cache-warming test that exercises a differential
refresh and verifies misses/hits move in the expected direction.

### [TEST-003] Fused-refresh E2E test does not exercise scheduler fused audit path

File/Location: [tests/e2e_refresh_tests.rs](../tests/e2e_refresh_tests.rs), [src/scheduler/mod.rs](../src/scheduler/mod.rs)

Severity: MEDIUM

Description: The test named `test_fused_refresh_tpch_22` enables fused refresh
but then manually refreshes the two stream tables sequentially. It validates
result correctness, not the scheduler's fused path or audit-history insertion.

Evidence: The test sets `pg_trickle.enable_fused_refresh = true`, creates a
two-node chain, performs DML, then calls `db.refresh_st("ftr_agg")` and
`db.refresh_st("ftr_top")`. It does not wait for scheduler dispatch, verify
fused SQL execution, or inspect `pgt_refresh_history`.

Remediation: Add a scheduler-driven fused refresh test that creates an eligible
chain, waits for automatic refresh, asserts expected data, and verifies
completed history rows with valid `initiated_by`/strategy metadata.

### [TEST-004] `change_buffer_durability` has no tests

File/Location: [tests/e2e_unlogged_buffer_tests.rs](../tests/e2e_unlogged_buffer_tests.rs), [src/config.rs](../src/config.rs)

Severity: MEDIUM

Description: The current tests cover only the legacy `unlogged_buffers` bool,
not the new enum-style durability GUC. This let ARCH-001/COR-003 survive.

Evidence: `e2e_unlogged_buffer_tests.rs` asserts `pg_trickle.unlogged_buffers`
defaults to off, and verifies logged/unlogged behavior through that bool. The
test search found no reference to `change_buffer_durability`.

Remediation: Add E2E tests for `SET pg_trickle.change_buffer_durability =
'unlogged'|'logged'|'sync'`, checking `pg_class.relpersistence` and any sync
behavior promised by the implementation.

### [TEST-005] Test harness catalog schema is stale

File/Location: [tests/common/mod.rs](../tests/common/mod.rs), [sql/archive/pg_trickle--0.67.0.sql](../sql/archive/pg_trickle--0.67.0.sql)

Severity: MEDIUM

Description: The lightweight test harness builds a hand-written subset of the
catalog schema that has drifted from the extension SQL. Tests using that harness
can pass while current extension catalog constraints fail.

Evidence: `tests/common/mod.rs` defines `pgt_refresh_history.initiated_by` with
`CHECK (... 'SCHEDULER', 'MANUAL', 'INITIAL')`, missing `SELF_MONITOR`. It also
defines `pgt_scheduler_jobs.unit_kind` with only `singleton`, `atomic_group`,
and `immediate_closure`, while current SQL includes `cyclic_scc`,
`repeatable_read_group`, and `fused_chain`.

Remediation: Generate test catalog DDL from the archive SQL or a single shared
source. Add a CI check that compares the harness schema against the current
archive for tables it claims to model.

---

## Pass 7 — Observability And Operations

### [OBS-001] DuckLake sink has no health/status/lag metrics

File/Location: [src/ducklake_sink.rs](../src/ducklake_sink.rs), [src/monitor/mod.rs](../src/monitor/mod.rs), [src/shmem.rs](../src/shmem.rs)

Severity: MEDIUM

Description: DuckLake sink integration writes files and catalog/provenance rows,
but operators cannot ask whether the sink is healthy, how far behind it is, or
how many attempts have failed.

Evidence: `insert_ducklake_provenance()` is best-effort and warnings-only.
There are no DuckLake sink counters in the shared-memory metrics area or
Prometheus exposition, and no SQL function like `ducklake_sink_status()`.

Remediation: Add counters for attempts, successes, retryable failures,
permanent failures, bytes written, rows written, upload latency, catalog-write
latency, and last error per stream table. Expose them via SQL and Prometheus.

### [OBS-002] Failed history pruning is invisible

File/Location: [src/scheduler/scheduler_loop.rs](../src/scheduler/scheduler_loop.rs)

Severity: MEDIUM

Description: The history pruner collapses SPI errors to zero deleted rows. If
pruning fails repeatedly, operators see no alert or metric while history keeps
growing.

Evidence: The prune loop uses `Spi::get_one_with_args::<i64>(...).unwrap_or(Some(0)).unwrap_or(0)`.
Only successful deletes greater than zero are logged.

Remediation: Log warning on prune SPI failure and increment a reliability
counter. Expose last prune timestamp, rows deleted, and last error in a status
function.

---

## Pass 8 — Documentation, Generated References, And Roadmap Truth

### [DOC-001] Generated SQL API catalog truncates return signatures

File/Location: [docs/SQL_API_CATALOG.md](../docs/SQL_API_CATALOG.md), [scripts/gen_catalogs.py](../scripts/gen_catalogs.py)

Severity: HIGH

Description: The auto-generated SQL API catalog claims to be checked by CI, but
many return types are visibly truncated. CI therefore guarantees the broken
output is reproducible, not useful.

Evidence: The catalog contains rows such as `pgtrickle.cache_stats()` returning
`TableIterator<`, `pgtrickle.refresh_efficiency()` returning `Result<`, and
multiple void-returning entries with empty return cells. The generator uses a
regex `_FN_SIG_RE` and scans only a small window of lines, which is insufficient
for nested generics and multiline `TableIterator` returns.

Remediation: Generate API signatures from pgrx output SQL or a Rust parser
(`syn`) rather than regexes. Add a catalog quality check that fails on dangling
return values ending in `<`, empty returns for non-void functions, or unbalanced
generic delimiters.

### [DOC-002] `plans/PLAN.md` is obsolete and duplicated around v0.9 status

File/Location: [plans/PLAN.md](PLAN.md)

Severity: LOW

Description: The master plan starts with old v0.9 status content duplicated
twice, which conflicts with the current v0.67 roadmap and assessment workflow.

Evidence: The first hundred lines repeatedly list "Implementation Status
(v0.9.0 Cycle)" and old F15/F40 items. The current project version is 0.67.0.

Remediation: Move the historical plan to an archive path and replace it with a
short current architecture/roadmap index, or mark it explicitly historical.

### [DOC-003] `plans/INDEX.md` is stale and misses current planning/report files

File/Location: [plans/INDEX.md](INDEX.md)

Severity: LOW

Description: The planning index has not kept pace with the number of assessment
and roadmap documents. A manual index that omits current reports undermines
discoverability.

Evidence: The index lists older root plans and many subdirectories, but not the
current Overall Assessment series through 13, despite multiple assessment files
existing in `plans/`.

Remediation: Generate the index or add a CI check that fails when new `plans/*.md`
files are not represented.

---

## Pass 9 — CI/CD, Dependency Policy, And Build Quality

### [CI-001] Fuzz smoke omits three committed fuzz targets

File/Location: [.github/workflows/fuzz-smoke.yml](../.github/workflows/fuzz-smoke.yml), [fuzz/fuzz_targets](../fuzz/fuzz_targets), [justfile](../justfile)

Severity: MEDIUM

Description: The repository has nine fuzz targets, but the CI fuzz-smoke
workflow runs only six. The omitted targets cover SQL builder, merge SQL, and
row-id logic, all of which are high-value parser/string-generation surfaces.

Evidence: `fuzz/fuzz_targets/` contains `parser_fuzz`, `cron_fuzz`, `dag_fuzz`,
`guc_fuzz`, `cdc_fuzz`, `wal_fuzz`, `sql_builder_fuzz`, `merge_sql_fuzz`, and
`row_id_fuzz`. `fuzz-smoke.yml` runs only the first six. `just fuzz-all` lists
all nine.

Remediation: Derive the CI target list from `fuzz/fuzz_targets/*.rs` or update
the workflow to include all nine. Add a check that fails when a fuzz target is
not listed.

### [CI-002] `just fuzz-all` masks fuzz crashes with `|| true`

File/Location: [justfile](../justfile)

Severity: MEDIUM

Description: The local command advertised as running all fuzz targets ignores
target failures. This makes it unsuitable as a release or pre-merge gate.

Evidence: The `fuzz-all` recipe loops over targets and runs `cargo +nightly fuzz
run ... || true`, then always prints `fuzz-all complete`.

Remediation: Remove `|| true`, collect failures, and exit non-zero if any target
fails. If a best-effort mode is useful, add a separate recipe such as
`fuzz-all-best-effort`.

### [CI-003] E2E coverage workflow is manual-only despite weekly comments

File/Location: [.github/workflows/coverage.yml](../.github/workflows/coverage.yml)

Severity: MEDIUM

Description: The coverage workflow comments say E2E coverage is weekly and
manual, but the `e2e-coverage` job is gated to `workflow_dispatch` only.

Evidence: The file-level comment says weekly runs track module-level coverage,
and the E2E coverage section says "weekly + manual". The job condition is
`if: github.event_name == 'workflow_dispatch'`, so scheduled runs skip it.

Remediation: Either update the comments to say manual-only, or allow scheduled
E2E coverage with a lower frequency and longer timeout.

### [CI-004] `docs-lint` is not part of local `just lint`

File/Location: [justfile](../justfile), [.github/workflows/docs-drift.yml](../.github/workflows/docs-drift.yml)

Severity: LOW

Description: CI does run docs drift/lint separately, but the local `just lint`
recipe required by repo guidance does not include `docs-lint`. Contributors can
run the documented local gate and still miss documentation drift.

Evidence: `lint: fmt-check clippy security-definer-check`. The docs linter is a
separate recipe and duplicated in `docs-drift.yml`.

Remediation: Either add `docs-lint` to `just lint`, or add a `just lint-all`
recipe and update contributor guidance to use it for doc-affecting changes.

### [DEP-001] Advisory ignores lack expiry/review metadata

File/Location: [deny.toml](../deny.toml), [.github/workflows/dependency-policy.yml](../.github/workflows/dependency-policy.yml)

Severity: MEDIUM

Description: `cargo-deny` is present and scheduled, which is good, but ignored
advisories have no expiry dates, owner, or review cadence. A temporary ignore
can become permanent by accident.

Evidence: `deny.toml` ignores five RUSTSEC advisories with rationale comments,
including dev-path issues, but no `review_by`, upstream issue links, or target
version notes. The workflow runs weekly but cannot enforce human review.

Remediation: Add structured comments for each ignore: owner, reason, upstream
tracking issue, date added, review date, and removal condition. Consider a small
script that fails when review dates are stale.

### [DEP-002] Arrow/Parquet/ObjectStore policy lacks compatibility rationale

File/Location: [Cargo.toml](../Cargo.toml)

Severity: LOW

Description: The DuckLake sink introduces a high-churn dependency group
(`arrow-array`, `arrow-schema`, `parquet`, `object_store`, `tokio`) without a
documented compatibility policy. That matters for a PostgreSQL extension where
binary size, compile time, and transitive advisories are release concerns.

Evidence: Arrow and Parquet are pinned to major `58`, object_store to `0.10`,
and tokio is included in both dependencies and dev-dependencies with different
feature sets. There is no comment explaining upgrade cadence or compatibility
testing against DuckLake clients.

Remediation: Add a dependency policy note for the DuckLake stack: upgrade
cadence, supported object-store schemes, required features, and CI coverage.

---

## Pass 10 — Code Quality, Maintainability, And Release Readiness

### [CODE-001] SQL API generator relies on regexes that cannot parse nested returns

File/Location: [scripts/gen_catalogs.py](../scripts/gen_catalogs.py)

Severity: LOW

Description: The generator uses regexes over Rust source to infer signatures.
This is fragile for nested generics, pgrx `TableIterator` return types, and
multiline signatures.

Evidence: `_FN_SIG_RE` captures `-> ([^{;]+)` from a joined line window and the
output in [docs/SQL_API_CATALOG.md](../docs/SQL_API_CATALOG.md) demonstrates
truncated signatures. This is the root cause behind DOC-001.

Remediation: Use `syn` or pgrx-generated SQL as the data source. If Python must
remain, parse brace/generic depth rather than using a flat regex.

### [CODE-002] Tarjan SCC code suppresses SQL-path unwraps instead of returning errors

File/Location: [src/dag.rs](../src/dag.rs)

Severity: LOW

Description: The SCC implementation contains unwraps in production code with
`nosemgrep: rust.panic-in-sql-path` comments. The invariants are likely valid,
but SQL-reachable code should not rely on panic for invariant enforcement.

Evidence: `tarjan_strongconnect()` uses `lowlinks.get_mut(&v).unwrap()` and
`stack.pop().unwrap()` with comments asserting the SCC invariants. The repo
guidelines explicitly prefer no `unwrap()` or `panic!()` in SQL-reachable code.

Remediation: Convert invariant failures into `PgTrickleError::InternalError` or
use `debug_assert!` plus safe fallback in release builds. Keep the comments, but
make the runtime path non-panicking.

### [REL-001] DuckLake sink failures are warning-only side effects

File/Location: [src/ducklake_sink.rs](../src/ducklake_sink.rs)

Severity: HIGH

Description: Sink failures do not affect stream table status, refresh history,
or delivery retry. This duplicates ARCH-002 at the release-readiness level: it
is not merely architectural; it is an operator-facing reliability gap.

Evidence: `run_ducklake_sink()` logs `pg_trickle: DuckLake sink failed...` and
returns. No error is propagated to the scheduler and no failure row is recorded.

Remediation: Before v1.0, decide whether sinks are best-effort exports or
delivery guarantees. If they are best-effort, document that clearly and expose
health metrics. If they are delivery guarantees, add durable retry state.

---

## Recommended Immediate Action Plan

### First 48 Hours

1. Fix COR-001 by aligning `SCHEDULER_FUSED` with the refresh-history CHECK
   constraint or by using an already-valid `initiated_by` value plus a separate
   strategy marker.
2. Fix COR-003/ARCH-001 by wiring `change_buffer_durability` into CDC buffer
   creation and adding tests for all modes.
3. Fix COR-004 by changing timestamp extraction for DuckLake Parquet output and
   adding a read-back test.
4. Add TEST-003 to prove the fused scheduler path records valid audit rows.

### Next 1-2 Weeks

1. Close COR-002 with parser/validator coverage for LATERAL raw SQL.
2. Repair or delete the stale scheduler pool path (SCAL-001).
3. Batch monitor buffer-count checks (PERF-001) and fused eligibility counts
   (PERF-002).
4. Make DuckLake sink delivery observable: status SQL, Prometheus counters, and
   last-error persistence.
5. Replace the SQL API catalog generator's regex signature parser.

### Before v1.0

1. Decide and document the durability defaults for change buffers. Performance
   defaults are acceptable only if the data-loss tradeoff is explicit and true.
2. Introduce a generated or validated test-harness catalog schema.
3. Bring fuzz-smoke, coverage comments, and local `just` gates into alignment.
4. Archive or rewrite stale planning documents so the roadmap and implementation
   plan do not disagree.

---

## Metrics Snapshot

| Metric | Value | Notes |
|--------|-------|-------|
| Project version | 0.67.0 | From [Cargo.toml](../Cargo.toml) |
| PostgreSQL target | 18 | pgrx `=0.18.0`, feature `pg18` |
| Relevant source/docs/config files audited | 1,343 | Excludes `.git` and `target` |
| Rust source files under `src/` | 103 | `find src -type f -name '*.rs'` |
| Rust test files under `tests/` | 153 | `find tests -type f -name '*.rs'` |
| SQL-callable functions in source | 124 `#[pg_extern]` occurrences | Grep count; generated catalog reports 121 discovered functions |
| Fuzz targets present | 9 | CI smoke currently runs 6 |
| Prior Overall Assessment 12 high findings rechecked | 4 | Ownership, recursive CTE depth, row-id verification, compaction metrics now fixed |
| Findings in this report | 36 | 0 Critical, 10 High, 19 Medium, 7 Low |

---

## Closing Assessment

pg_trickle's core has matured faster than its newest integration surfaces. The
best parts of the project are now excellent: the DVM operator test depth, the
upgrade completeness checks, the light/full E2E split, and the PostgreSQL-aware
catalog conventions. The weak spots are newer and more operational: truth in
configuration, sink delivery semantics, scheduler path consistency, generated
docs quality, and making observability match what the system now promises.

The recommendation is not to broaden the roadmap. It is to narrow it for one
hardening release and make the promises already in the codebase exact.