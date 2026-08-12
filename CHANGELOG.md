# Changelog

What's new in pg_trickle — written for everyone, not just developers.

For future plans and upcoming features, see [ROADMAP.md](ROADMAP.md).

---

## Upgrade E2E Cutoff Policy

> **U-2 (v0.80.0)**: This section documents the automated upgrade test coverage
> policy.  Always read the [Rollback Runbook](docs/ROLLBACK_RUNBOOK.md) before
> upgrading.

The upgrade E2E test suite validates pg_extension migration SQL for the
**last two released versions** to the current version.  Specifically:

| Current version | Tested upgrade paths |
|-----------------|----------------------|
| v0.79.0 | v0.77.0 → v0.79.0, v0.78.0 → v0.79.0 |
| v0.80.0 | v0.78.0 → v0.80.0, v0.79.0 → v0.80.0 |

Upgrades from older versions (e.g. v0.77.0 → v0.80.0) are not covered by
automated testing but may work.  Always back up your database before any
upgrade — see [docs/ROLLBACK_RUNBOOK.md](docs/ROLLBACK_RUNBOOK.md).

The cutoff exists because:
1. Testing every historical version pair grows quadratically in CI time.
2. Multi-version jumps (skipping more than two releases) carry higher risk
   and should be done as a series of single-version upgrades anyway.
3. The migration SQL is cumulative — each file is idempotent and builds on
   the previous version's schema.

---

## Table of Contents

<!-- TOC start -->
- [0.81.1 — Non-superuser stream-table creation](#0811--non-superuser-stream-table-creation)
- [0.81.0 — Observability, Self-Tuning & Quick Wins](#0810--observability-self-tuning--quick-wins)
- [0.80.0 — Operational Excellence, Documentation Completeness & Final v1.0 Gate](#0800--operational-excellence-documentation-completeness--final-v10-gate)
- [0.79.0 — Code Quality, API Ergonomics & Security](#0790--code-quality-api-ergonomics--security)
- [0.78.0 — DVM Engine Root-Cause Fixes + Scheduler Intelligence](#0780--dvm-engine-root-cause-fixes--scheduler-intelligence)
- [0.77.0 — Correctness Stop-the-Line & DVM Proof Infrastructure](#0770--correctness-stop-the-line--dvm-proof-infrastructure)
- [0.75.0 — API Polish, Documentation Excellence & Developer Experience](#0750--api-polish-documentation-excellence--developer-experience)
- [0.74.0 — Test Coverage, CI Integrity & Security Hardening](#0740--test-coverage-ci-integrity--security-hardening)
- [0.73.0 — Monitoring Scalability, Operational Resilience & HOT-Friendly Storage](#0730--monitoring-scalability-operational-resilience--hot-friendly-storage)
- [0.72.0 — Frontier Durability & Catalog Correctness](#0720--frontier-durability--catalog-correctness)
- [0.71.0 — CI Truthfulness, Test Harness & Documentation Cleanup](#0710--ci-truthfulness-test-harness--documentation-cleanup)
- [0.70.0 — Scheduler, Validator & Security Hardening](#0700--scheduler-validator--security-hardening)
- [0.69.0 — DuckLake Sink Reliability & Security](#0690--ducklake-sink-reliability--security)
- [0.68.0 — Assessment-13 Correctness & Durability Sprint](#0680--assessment-13-correctness--durability-sprint)
- [0.67.0 — DuckLake Phase 3b: View Registration, Provenance & Ecosystem](#0670--ducklake-phase-3b-view-registration-provenance--ecosystem)
- [0.66.0 — DuckLake Phase 3a: Parquet Sink Infrastructure](#0660--ducklake-phase-3a-parquet-sink-infrastructure)
- [0.65.0 — DuckLake Phase 2: Change-Feed Adapter](#0650--ducklake-phase-2-change-feed-adapter)
- [0.64.0 — DuckLake Ecosystem Phase 1](#0640--ducklake-ecosystem-phase-1)
- [0.63.0 — Fused Multi-Node Refresh](#0630--fused-multi-node-refresh)
- [0.62.0 — Scheduler Throughput & pg_aqueduct Prerequisites](#0620--scheduler-throughput--pg_aqueduct-prerequisites)
- [0.61.0 — DX, Documentation & Final Pre-1.0 Polish](#0610--dx-documentation--final-pre-10-polish)
- [0.60.0 — Code Quality, Test Coverage & CI](#0600--code-quality-test-coverage--ci)
- [0.59.0 — Performance & Observability](#0590--performance--observability)
- [0.58.0 — Security & Correctness Hardening](#0580--security--correctness-hardening)
- [0.57.0 — Documentation Excellence](#0570--documentation-excellence)
- [0.56.0 — Documentation Foundation](#0560--documentation-foundation)
- [0.55.0 — Final Pre-1.0 Polish](#0550--final-pre-10-polish)
- [0.54.0 — DVM Engine Hardening](#0540--dvm-engine-hardening)
- [0.53.0 — Unit Test Depth Sweep](#0530--unit-test-depth-sweep)
- [0.52.0 — DVM Hot-Path Performance](#0520--dvm-hot-path-performance)
- [0.51.0 — Citus Chaos Resilience & Documentation Truth](#0510--citus-chaos-resilience--documentation-truth)
- [0.50.0 — Performance, Security & Operational Hardening](#0500--performance-security--operational-hardening)
- [0.49.1 — Repository Migration to trickle-labs/pg-trickle](#0491--repository-migration-to-trickle-labspg-trickle)
- [0.49.0 — Test Infrastructure Hardening & Scheduler Decomposition](#0490--test-infrastructure-hardening--scheduler-decomposition)
- [0.48.0 — Complete Embedding Programme: Hybrid Search, Sparse Vectors & Ergonomic API](#0480--complete-embedding-programme-hybrid-search-sparse-vectors--ergonomic-api)
- [0.47.0 — Embedding Pipeline Infrastructure & ANN Maintenance](#0470--embedding-pipeline-infrastructure--ann-maintenance)
- [0.46.0 — Extract `pg_tide`: Standalone Outbox, Inbox & Relay](#0460--extract-pg_tide-standalone-outbox-inbox--relay)
- [0.45.0 — Operational Readiness, Scalability & CI Completeness](#0450--operational-readiness-scalability--ci-completeness)
- [0.44.0 — Security Hardening & Code Quality](#0440--security-hardening--code-quality)
- [0.43.0 — D+I Change-Buffer Schema, GUC Tuning & WAL Diagnostics](#0430--di-change-buffer-schema-guc-tuning--wal-diagnostics)
- [0.42.0 — Repair API, Docs Overhaul & Test Infrastructure](#0420--repair-api-docs-overhaul--test-infrastructure)
- [0.41.0 — DVM Correctness: Structural Cache Keys, Placeholder Safety & WAL Transition Guards](#0410--dvm-correctness-structural-cache-keys-placeholder-safety--wal-transition-guards)
- [0.40.0 — Operator Trust, Maintainability & Release Confidence](#0400--operator-trust-maintainability--release-confidence)
- [0.39.0 — Operational Truthfulness & Distributed Hardening](#0390--operational-truthfulness--distributed-hardening)
- [0.38.0 — EC-01 Join Correctness Sprint](#0380--ec-01-join-correctness-sprint)
- [0.37.0 — pgVector Incremental Aggregates & Distributed Trace Propagation](#0370--pgvector-incremental-aggregates--distributed-trace-propagation)
- [0.36.0 — Structural Hardening, Performance & Temporal IVM](#0360--structural-hardening-performance--temporal-ivm)
- [0.35.0 — Hardening, Reactive Subscriptions & Relay Resilience](#0350--hardening-reactive-subscriptions--relay-resilience)
- [0.34.0 — Citus: Automated Distributed CDC Scheduler & Shard Recovery](#0340--citus-automated-distributed-cdc-scheduler--shard-recovery)
- [0.33.0 — Citus: Distributed Source CDC & Stream Tables](#0330--citus-distributed-source-cdc--stream-tables)
- [0.32.0 — Citus: Stable Naming & Per-Source Frontier Foundation](#0320--citus-stable-naming--per-source-frontier-foundation)
- [0.31.0 — Performance & Scheduler Intelligence](#0310--performance--scheduler-intelligence)
- [0.30.0 — Pre-GA Correctness & Stability Sprint](#0300--pre-ga-correctness--stability-sprint)
- [0.29.0 — Relay CLI (pgtrickle-relay)](#0290--relay-cli-pgtrickle-relay)
- [0.28.0 — Transactional Inbox & Outbox Patterns](#0280--transactional-inbox--outbox-patterns)
- [0.27.0 — Operability, Observability & DR](#0270--operability-observability--dr)
- [0.26.0 — Test & Concurrency Hardening](#0260--test--concurrency-hardening)
- [0.25.0 — Scheduler Scalability & Pooler Performance](#0250--scheduler-scalability--pooler-performance)
- [0.24.0 — Join Correctness & Durability Hardening](#0240--join-correctness--durability-hardening)
- [0.23.0 — Performance Tuning & Diagnostics](#0230--performance-tuning--diagnostics)
- [0.22.0 — Downstream CDC, Parallel Refresh & Predictive Cost Model](#0220--downstream-cdc-parallel-refresh--predictive-cost-model)
- [0.21.0 — Reliability, Safety & Operational Tools](#0210--reliability-safety--operational-tools)
- [0.20.0 — Self Monitoring](#0200--self-monitoring)
- [0.19.0 — Security, Scheduler Performance & Operator Convenience](#0190--security-scheduler-performance--operator-convenience)
- [0.18.0 — Hardening & Delta Performance](#0180--hardening--delta-performance)
- [0.17.0 — Query Intelligence & Stability](#0170--query-intelligence--stability)
- [0.16.0 — Performance & Refresh Optimization](#0160--performance--refresh-optimization)
- [0.15.0 — Interactive TUI, Bulk Create & Runaway-Refresh Protection](#0150--interactive-tui-bulk-create--runaway-refresh-protection)
- [0.14.0 — Tiered Scheduling, Diagnostics & TUI](#0140--tiered-scheduling-diagnostics--tui)
- [0.13.0 — Scalability Foundations & Full TPC-H Coverage](#0130--scalability-foundations--full-tpc-h-coverage)
- [0.12.0 — Join Correctness, Diagnostics & Reliability](#0120--join-correctness-diagnostics--reliability)
- [0.11.0 — Event-Driven Latency, Chain IVM & Observability Stack](#0110--event-driven-latency-chain-ivm--observability-stack)
- [0.10.0 — Cloud Deployment, PgBouncer & Query Engine Correctness](#0100--cloud-deployment-pgbouncer--query-engine-correctness)
- [0.9.0 — Incremental Aggregates & Smarter Scheduling](#090--incremental-aggregates--smarter-scheduling)
- [0.8.0 — Backup, Pooler Compatibility & Reliability](#080--backup-pooler-compatibility--reliability)
- [0.7.0 — Watermark Gating, Circular Pipelines & SQL Broadening](#070--watermark-gating-circular-pipelines--sql-broadening)
- [0.6.0 — Idempotent DDL, Partitioned Sources & dbt Integration](#060--idempotent-ddl-partitioned-sources--dbt-integration)
- [0.5.0 — Row-Level Security, Source Gating & Append-Only Fast Path](#050--row-level-security-source-gating--append-only-fast-path)
- [0.4.0 — Parallel Refresh & Statement-Level CDC Triggers](#040--parallel-refresh--statement-level-cdc-triggers)
- [0.3.0 — Incremental Correctness & Security Tooling](#030--incremental-correctness--security-tooling)
- [0.2.3 — Per-Table CDC Mode & WAL Lag Monitoring](#023--per-table-cdc-mode--wal-lag-monitoring)
- [0.2.2 — AUTO Refresh Mode & Query Alteration](#022--auto-refresh-mode--query-alteration)
- [0.2.1 — Safe Upgrades & Scheduling Improvements](#021--safe-upgrades--scheduling-improvements)
- [0.2.0 — Monitoring, IMMEDIATE Mode & Diamond Consistency](#020--monitoring-immediate-mode--diamond-consistency)
- [0.1.3 — TPC-H Correctness, Window Functions & Aggregate Fixes](#013--tpc-h-correctness-window-functions--aggregate-fixes)
- [0.1.2 — Incremental Correctness Fixes & Project Rename](#012--incremental-correctness-fixes--project-rename)
- [0.1.1 — CloudNativePG Image & Test Hardening](#011--cloudnativepg-image--test-hardening)
- [0.1.0 — Initial Release](#010--initial-release)
<!-- TOC end -->

---

## [0.81.1] — Non-superuser stream-table creation

- Fix `create_stream_table()` for documented non-superuser roles without
  granting access to internal catalog or change-buffer objects (#903).
- Keep storage-table DDL and the defining query under the caller's PostgreSQL
  privileges and ownership.

## [0.81.0] — Observability, Self-Tuning & Quick Wins

### What's New

v0.81.0 delivers 10 quality-of-life improvements (Assessment-16 Quick Wins
QW-1 through QW-10) spanning observability, self-tuning, API ergonomics, and
engine performance.  No schema migrations are required; all new GUCs default to
safe backward-compatible values.

#### QW-1 — Commit latency statistics (`pgtrickle.commit_latency_stats()`)

A new SQL function returns per-stream-table refresh latency statistics (min, p50,
p95, max in milliseconds) computed from `pgt_refresh_history`.  When the new
`pg_trickle.commit_timestamp_tracking` GUC is enabled together with
PostgreSQL's `track_commit_timestamp = on`, this function reports the full
commit-to-visible wall-clock latency; otherwise it reports refresh duration
as a conservative proxy.

#### QW-2 — GUC tuning recommendations (`pgtrickle.tune_recommendations()`)

A new read-only SQL function analyses recent refresh history and current GUC
settings to produce a ranked list of tuning recommendations.  It detects:
- Large p99 delta sizes that would benefit from chunked MERGE (QW-9)
- High `delta_work_mem_cap_mb` values correlated with high p95 latency
- OOM errors in the past 7 days suggesting `self_heal_oom = on`
- `max_concurrent_refreshes` set higher than `worker_pool_size`

#### QW-3 — Query preview without creating (`pgtrickle.preview_stream_table()`)

`pgtrickle.preview_stream_table(query text)` analyses a defining query using the
DVM parser and returns a key/value result set showing: DVM support, planned
refresh strategy (DIFFERENTIAL or FULL), source tables, operator tree root type,
parser warnings, and a complexity estimate — all without creating any objects.

#### QW-4 — Additional OpenTelemetry span names

Five new span name constants are exported from `src/otel.rs`:
`SPAN_SCHEDULER_TICK`, `SPAN_REFRESH_CYCLE`, `SPAN_DELTA_EXECUTE`,
`SPAN_FRONTIER_ADVANCE`, and `SPAN_CLEANUP`.  The existing
`emit_trace_span_if_enabled` function now uses `SPAN_DELTA_EXECUTE` for
DIFFERENTIAL refreshes and `SPAN_REFRESH_CYCLE` for other modes, replacing the
previous hard-coded `SPAN_MERGE_APPLY` for all cases.

#### QW-5 — Bounded LRU cache for DVM delta templates

The in-process DVM template caches (L0 delta templates, L1 placeholder
resolvers) now enforce a maximum size.  When the cache is full, the
least-recently-used entry is evicted.  The new
`pg_trickle.l1_cache_max_entries` GUC (default: 256) controls the cap.
Previously the caches grew without bound over the lifetime of a background
worker session.

#### QW-6 — `DeltaOperator` trait for the DVM engine

All 20+ operator types now implement the `DeltaOperator` trait defined in
`src/dvm/operators/mod.rs`.  The trait provides a unified interface
(`generate_delta`, `supports_immediate_mode`, `is_monotone`) for future
plugin-style extension and cross-operator tooling.  Existing diff functions
are unchanged; the trait implementations delegate to them via a zero-cost
`impl_delta_operator!` macro.

#### QW-7 — `src/config.rs` split into sub-modules

The 4 748-line monolithic `src/config.rs` has been decomposed into:
`src/config/mod.rs`, `src/config/scheduler.rs`, `src/config/cdc.rs`,
`src/config/dvm.rs`, and `src/config/monitoring.rs`.  All existing public
symbols are re-exported; no call sites changed.

#### QW-8 — Self-healing circuit breaker

Two new GUCs control automatic error-counter reset on transient failures:
- `pg_trickle.self_heal_oom` (default: `on`) — when a DIFFERENTIAL refresh
  fails with "out of memory", the consecutive-error counter is reset instead of
  counting toward auto-suspension, and a hint is emitted to lower
  `merge_work_mem_mb` or `delta_work_mem_cap_mb`.
- `pg_trickle.self_heal_lock_timeout` (default: `on`) — same behaviour for
  lock-timeout errors.  The stream table retries on the next scheduler tick
  instead of progressing toward suspension.

#### QW-9 — Chunked MERGE batching

A new `pg_trickle.merge_batch_size` GUC (default: 0 = disabled, i32) routes
large deltas through the existing PH-D1 DELETE+INSERT path when the estimated
delta row count exceeds the configured threshold.  This avoids peak-memory
MERGE join cost for unexpectedly large refresh cycles.

#### QW-10 — Stream table presets

Three convenience wrapper functions are now available:
- `pgtrickle.create_stream_table_realtime(name, query, …)` — schedule `1s`,
  mode `DIFFERENTIAL`
- `pgtrickle.create_stream_table_batch(name, query, …)` — schedule `5m`,
  mode `AUTO`
- `pgtrickle.create_stream_table_cost_optimized(name, query, …)` — schedule
  `15m`, mode `AUTO`

Each wrapper accepts the same CDC/partition/join-limit optional parameters as
`create_stream_table`.

### Configuration

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.commit_timestamp_tracking` | `off` | Enable commit-to-visible latency tracking |
| `pg_trickle.l1_cache_max_entries` | `256` | Maximum DVM L1 cache entries |
| `pg_trickle.merge_batch_size` | `0` | Route deltas > N rows through DELETE+INSERT (0 = off) |
| `pg_trickle.self_heal_oom` | `on` | Reset error counter on OOM instead of suspending |
| `pg_trickle.self_heal_lock_timeout` | `on` | Reset error counter on lock-timeout instead of suspending |

### Upgrade

No schema changes.  Standard `ALTER EXTENSION pg_trickle UPDATE` is sufficient.

---

## [0.80.0] — Operational Excellence, Documentation Completeness & Final v1.0 Gate

### What's New

v0.80.0 is the final item in the Assessment-15-Driven Hardening Arc
(v0.77.x–v0.80.x) and completes every remaining observability, documentation,
and build-confidence goal required before the v1.0 milestone.

#### O-1 — DVM fallback reason codes in refresh history and health output

Three machine-readable reason codes are now recorded in
`pgt_refresh_history.refresh_reason` whenever a stream table's differential
refresh falls back to FULL due to a known DVM-incompatible query pattern:

| Code | Condition |
|------|-----------|
| `CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK` | SUM/COUNT(CASE…) aggregate with IN-list predicate on a mutable source |
| `CORRELATED_SUBQUERY_DELTA_QUADRATIC` | Correlated aggregate scalar subquery in WHERE (q20-like) |
| `REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN` | CASE aggregate with EXISTS/subquery inside — string classifier uncertain |

`pgtrickle.health_check()` now includes a `dvm_fallbacks` check that emits
`WARN` if any of these codes appeared in the last hour.  See
[docs/DVM_SUPPORT_MATRIX.md](docs/DVM_SUPPORT_MATRIX.md) for pattern details
and remediation guidance.

#### O-2 — health_check() ring overflow alert

`health_check()` now includes a `ring_overflow_trend` check.  If the
invalidation ring has overflowed since startup (meaning a DDL burst forced
a full DAG rebuild), the check emits `WARN` with the overflow count and a
suggestion to raise `pg_trickle.invalidation_ring_capacity`.

#### O-3 — Cleanup backlog trend in metrics_summary()

`pgtrickle.metrics_summary()` gains two new columns:

| Column | Description |
|--------|-------------|
| `cleanup_backlog_count` | Total entries in `pgt_cleanup_status` |
| `cleanup_blocked_count` | Entries with `blocked = true` (stalled cleanup) |

These allow Grafana dashboards to trend cleanup-worker health over time.

#### DOC-1 — Docs lint: pg_extern exports vs SQL_REFERENCE.md

A new script `scripts/check_pg_extern_docs.py` extracts every
`#[pg_extern(schema = "pgtrickle")]` export from Rust source files and checks
that each one appears in either `docs/SQL_REFERENCE.md` or
`docs/SQL_API_CATALOG.md`.  This check is now part of `just docs-lint` (and
therefore `just lint`), so undocumented API exports are caught in CI on every PR.

#### DOC-2 — DVM Support Matrix

[docs/DVM_SUPPORT_MATRIX.md](docs/DVM_SUPPORT_MATRIX.md) is a new comprehensive
reference covering every supported SQL query pattern, fallback behaviour, IMMEDIATE
restrictions, and known-unsupported forms including q12 (CASE/IN-list) and q20
(correlated aggregate subquery).

#### U-1 — Operational rollback runbook

[docs/ROLLBACK_RUNBOOK.md](docs/ROLLBACK_RUNBOOK.md) documents:
- Why downgrades are unsafe (schema migrations, WAL decoder format, shmem layout)
- Pre-upgrade backup requirements (pg_dump and filesystem snapshot)
- The recommended snapshot workflow before every upgrade
- The restore/rollback procedure step-by-step

#### U-2 — Upgrade E2E cutoff policy

The [Upgrade E2E Cutoff Policy](#upgrade-e2e-cutoff-policy) section at the top
of this file documents which version-to-version upgrade paths are covered by
automated testing (the last two released versions) and why the cutoff exists.

#### B-1 — CI gate documentation expanded

The CI gate table in `CONTRIBUTING.md` now lists every workflow with its
trigger conditions (PR / push to main / daily schedule / manual dispatch),
so contributors can see exactly which jobs must pass before a PR can merge.

#### B-2 — cargo-deny in PR gates (confirmed)

`cargo-deny` runs on every PR that touches `Cargo.toml`, `Cargo.lock`, or
`deny.toml` via `.github/workflows/dependency-policy.yml`.  All advisory
suppressions in `deny.toml` carry `# Review-By: YYYY-MM-DD` expiry dates.

#### P-5 — Fuzz test for DVM snapshot fingerprint cache

A new fuzz target `fuzz/fuzz_targets/snapshot_fingerprint_fuzz.rs` exercises
the `DiffContext::snapshot_fingerprint_cache` under arbitrary OpTree pointer
mutations, checking that address reuse does not produce stale SQL strings
(cache-key safety under GC pressure).

#### A-3 — Event trigger function documentation

`src/hooks.rs` now has comprehensive doc comments on the DDL event trigger
callback functions (`_on_ddl_end`, `_on_sql_drop`) explaining their role in
the invalidation ring pipeline.  `docs/ARCHITECTURE.md` gains a new section
describing the event trigger subsystem.

### Migration

No schema changes.  The `sql/pg_trickle--0.79.0--0.80.0.sql` migration is a
no-op placeholder — v0.80.0 changes are entirely in Rust code, scripts, and
documentation.

---

## [0.79.0] — Code Quality, API Ergonomics & Security

### What's New

v0.79.0 is a maintainability and security polish release that reduces technical
debt accumulated between hardening arcs.  No schema migrations are required —
the changes are primarily code quality improvements, new convenience API
functions, and security testing coverage.

#### Q-1 — Unused-import suppressions removed

Per-line `#[allow(unused_imports)]` attributes in `src/refresh/codegen.rs` and
`src/refresh/merge/mod.rs` are removed.  The redundant `use super::*` glob in
`codegen.rs` (which caused the imports to appear unused) is also removed.
`merge/mod.rs` retains `use super::*` because it relies on functions defined in
the parent module.

#### Q-2 — Typed parameter structs for internal API

`alter_stream_table_impl` is refactored to accept a single
`AlterStreamTableOptions<'_>` struct, matching the existing
`CreateStreamTableOptions` pattern.  Callers that only need a subset of
options can use `..Default::default()`.  The public SQL-callable wrappers
are unchanged (pgrx requires individual parameters on `#[pg_extern]` functions).

#### Q-3 — Per-module dead_code annotations

The global `#![allow(dead_code)]` crate attribute is replaced with narrower
`#[allow(dead_code)]` attributes on specific module declarations where items
are intentionally visible to PostgreSQL (via `#[pg_extern]` or pgrx macros)
but not to Rust's static call-graph analysis.  Pure computation modules gain
no annotation.

#### Q-4 — `consume_slot_changes()` deprecated

`consume_slot_changes()` in `src/cdc/mod.rs` is formally marked
`#[deprecated]`.  The function has been a no-op since the extension switched
from WAL-based CDC to trigger-based CDC.  Prefer `pending_change_count()` or
inspect the change buffer table directly.

#### A-1 — SQL convenience helpers

Three new SQL functions simplify common operations:

- **`pgtrickle.create_stream_table_fast_append_only(name, query)`** — creates a
  stream table with `append_only = true` and `refresh_mode = 'DIFFERENTIAL'`.
  Ideal for event or audit tables that only ever receive INSERT.

- **`pgtrickle.set_stream_table_refresh_policy(name, refresh_mode)`** — changes
  only the refresh mode of an existing stream table.

- **`pgtrickle.set_stream_table_storage_policy(name, append_only, tier)`** —
  sets the append-only flag and/or scheduling tier in a single call.

#### A-2 — `pause_stream_table()`

`pgtrickle.pause_stream_table(name)` is a first-class SQL function that sets
the stream table status to SUSPENDED.  It is the mirror of the existing
`resume_stream_table()`.

#### S-1 — Semgrep coverage for parameterised SPI helpers

Three new Semgrep rules are added to `.semgrep/pg_trickle.yml`:
`rust.spi.run_with_args.dynamic-format`,
`rust.spi.get_one_with_args.dynamic-format`, and
`rust.spi.connect_mut.dynamic-format`.  These close a gap where dynamic SQL
could bypass the parameterisation that `run_with_args` and `get_one_with_args`
are intended to provide.

#### S-2 — RLS warning confirmed

The runtime `WARNING` emitted when a source table has Row Level Security
enabled (A45-3) is confirmed present and tested in v0.79.0.

#### S-3 — Global SECURITY DEFINER trigger function test

A new test `test_all_security_definer_trigger_fns_have_search_path` asserts
that **every** SECURITY DEFINER trigger function installed by the extension has
a locked `search_path` in its `proconfig` — regardless of naming convention.
This complements the per-pattern tests already in `e2e_rls_tests.rs`.

#### D-3 — Cleanup chaos test

`tests/e2e_cleanup_chaos_tests.rs` tests the auto-suspend circuit under
consecutive change-buffer cleanup failures.  A `BEFORE DELETE` chaos trigger
on the CDC buffer table blocks cleanup for three consecutive refreshes; the
test asserts the stream table enters SUSPENDED, then verifies full recovery
after the trigger is removed and the stream table is resumed.

#### T-5 — dbt adapter compatibility matrix

The dbt integration test suite is extended with a new `order_totals_compat`
model and three assertion SQL files that cover the CREATE, ALTER, DROP, and
REBUILD flows.  The test script exercises the ALTER path explicitly by
temporarily changing the model schedule from `1m` to `3m` mid-run.

---

## [0.78.0] — DVM Engine Root-Cause Fixes + Scheduler Intelligence

### What's New

v0.78.0 completes the Assessment-15 Hardening Arc by fixing root-cause DVM
engine bugs and adding scheduler intelligence that eliminates per-stream-table
subquery overhead.

#### DVM-1 — CASE/IN-list aggregate drift: append-only bypass

The CASE/IN-list aggregate fallback (introduced in v0.77.0) was overly
conservative: it forced FULL refresh even for append-only sources where the
bug cannot manifest (INSERT-only workloads are handled correctly by
GROUP_RESCAN). v0.78.0 restricts the fallback to mutable sources while
allowing append-only sources to take the differential path. Correctness
is preserved; append-only performance is restored.

#### DVM-2 — Correlated aggregate subquery rewrite (q20-like)

Queries with a correlated aggregate scalar subquery in the WHERE clause
(e.g., `WHERE col > (SELECT 0.5 * SUM(...) FROM t2 WHERE t2.key = t1.key)`)
previously always fell back to FULL refresh. v0.78.0 attempts a CTE
pre-aggregation rewrite first. Queries that can be safely decorrelated now
run differentially; remaining unsafe patterns fall back with the new
`CORRELATED_SUBQUERY_DELTA_QUADRATIC` reason code for auditability.

#### P-1 — Auditable fallback reason code

All correlated aggregate scalar subquery FULL fallbacks now record
`CORRELATED_SUBQUERY_DELTA_QUADRATIC` in `pgt_refresh_history.fallback_reason`,
making it easy to identify affected stream tables via monitoring.

#### P-2 — Query complexity class in catalog

A new `query_complexity_class` column on `pgtrickle.pgt_stream_tables` stores
the OpTree-derived complexity label (`Scan`, `Filter`, `Aggregate`, `Join`,
`JoinAggregate`) computed at CREATE/ALTER time. For stream tables created
before v0.78.0, the class is back-filled lazily on the first refresh.

#### P-3 — Batch cost model summary table

A new `pgtrickle.pgt_cost_model_summary` table caches per-stream-table
refresh performance aggregates. The scheduler updates it in a single batch
query per tick, replacing N per-stream-table history subqueries with one
grouped upsert. This reduces scheduler overhead significantly for
deployments with many stream tables.

#### P-4 — Placeholder resolver cache collision guard

The Aho-Corasick placeholder resolver cache now stores the canonical key
string alongside its hash. On hash collision the old entry is evicted and
rebuilt with a `WARNING` log, preventing stale automata from causing silent
incorrect delta SQL.

#### T-2 — TPC-H differential latency regression gate

A new `test_t2_latency_regression` test in the TPC-H test suite runs a
rotating subset of TPC-H queries and asserts end-to-end differential refresh
latency stays below per-query thresholds. Catches FULL fallback regressions
and O(N²) delta path regressions before they reach production.

#### T-3 — Nightly 300 s fuzz workflow

A new `fuzz-nightly.yml` workflow runs each fuzz target for 300 seconds
(10× the smoke run) on a nightly schedule. The grown corpus is archived as
a CI artifact, enabling trend analysis of corpus growth over time.

### Schema Changes

```sql
-- P-2: new column on pgt_stream_tables
ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS query_complexity_class TEXT;

-- P-3: new cost model summary table
CREATE TABLE pgtrickle.pgt_cost_model_summary (
    pgt_id       BIGINT      PRIMARY KEY
                             REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                             ON DELETE CASCADE,
    avg_full_ms  DOUBLE PRECISION,
    avg_diff_ms  DOUBLE PRECISION,
    sample_count INTEGER     NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Upgrade Notes

The upgrade migration (`pg_trickle--0.77.0--0.78.0.sql`) adds the two schema
changes above. Existing stream tables will have `query_complexity_class = NULL`
until their first refresh after upgrade. The cost model summary table starts
empty and is populated on the first scheduler tick.

---

## [0.77.0] — Correctness Stop-the-Line & DVM Proof Infrastructure

### What's New

v0.77.0 is the first release of the Assessment-15 Hardening Arc.
It delivers a correctness hard gate: no new features, no schema changes —
only fixes to known DVM bugs, safety assertions that fail loudly on
correctness violations, and tests that prove the fixes are permanent.

#### C-1 — TRUNCATE LSN fix

The TRUNCATE CDC trigger was using `pg_current_wal_lsn()` (the WAL write
position) instead of `pg_current_wal_insert_lsn()` (the insert position).
Inside a transaction, the insert position is always at or ahead of the write
position. This meant that when a TRUNCATE and subsequent INSERTs were committed
in the same transaction, the refresh engine could silently lose the
post-TRUNCATE rows. Fixed.

Two regression tests (`test_t4_truncate_lsn_insert_ordering_regression` and
`test_t4b_truncate_then_insert_separate_transactions`) verify the fix.

#### C-2 — Source-placeholder coverage assertion

The delta template resolver now verifies that every source OID has both
`__PGS_PREV_LSN_{oid}__` and `__PGS_NEW_LSN_{oid}__` placeholder tokens in
the generated delta SQL. If a token is missing, it means a source table was
silently dropped from the delta — changes to that table would be missed and
the stream table would drift. The resolver now fails fast and triggers a
full reinitialisation.

#### C-3/DVM-1 — TPC-H q12 CASE/IN-list drift → forced FULL

Queries with `SUM(CASE…)` or `COUNT(CASE…)` combined with an `IN (…)` predicate
in WHERE can produce non-deterministic incremental results (the delta rule
double-counts rows under certain orderings). v0.77.0 detects this pattern
and forces FULL refresh with reason code `CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK`.
The root-cause delta rule fix is planned for v0.78.0.

#### DVM-2/P-1 — TPC-H q20 correlated aggregate subquery → forced FULL

Queries with a comparison against a correlated aggregate scalar subquery in
WHERE (e.g., `qty > (SELECT SUM(…) FROM lineitem WHERE l_partkey = ps_partkey)`)
produce O(delta × table) DVM delta SQL. At SF=10 this takes 45+ minutes per
refresh cycle. v0.77.0 detects this pattern and forces FULL refresh with
reason code `CORRELATED_SUBQUERY_DELTA_QUADRATIC`. The pre-aggregation CTE
rewrite is planned for v0.78.0.

#### D-1 — Multi-consumer cleanup advisory lock

When two stream tables share a source table, concurrent cleanup workers could
race on the min-frontier computation + DELETE. v0.77.0 adds a non-blocking
`pg_try_advisory_xact_lock(oid::bigint)` call — one worker owns the cleanup
window per source OID per transaction; the other skips and retries next tick.

#### D-2 — IMMEDIATE mode SAVEPOINT/rollback tests

Three new E2E tests verify that IMMEDIATE mode stream tables correctly handle
transaction rollbacks: full rollback, partial SAVEPOINT rollback (committed
rows visible, rolled-back rows absent), and nested SAVEPOINT rollback.

#### DVM-3 — `pg_trickle.validate_delta_invariants` GUC

New boolean GUC (default: `false`). When enabled, after every DIFFERENTIAL
refresh the extension compares the stream table row count against a full
recomputation of the defining query and emits a WARNING on any discrepancy.
Enable only for debugging or CI validation — significant performance impact.

#### T-1 — DVM algebra property generators

12 new property-based unit tests cover the C-3/DVM-1 and DVM-2/P-1 detection
functions: canonical true-positive patterns (q12, q20), case-insensitive
variants, and false-positive guards (benign aggregates, simple scans).

#### C-4 — Semgrep CI rule formalised

The existing `rust.panic-in-sql-path` semgrep rule already blocks
`.unwrap()`, `.expect()`, and `panic!()` in non-test code on every PR.
v0.77.0 formally annotates it as the Assessment-15 C-4 hard gate.

### Upgrade notes

No schema changes. The upgrade is a no-op:
```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.77.0';
```

---

## [0.75.0] — API Polish, Documentation Excellence & Developer Experience

### What's New

v0.75.0 completes the Assessment-14-driven hardening arc with a focused release
on API documentation quality, developer experience, and Rust type safety.
No schema changes were made in this release.

#### API-002/DOC-003 — Full SQL reference for `pgtrickle.metrics_summary()`

`pgtrickle.metrics_summary()` is one of the most useful monitoring functions
in the extension, but was only documented by its signature. A complete reference
section is now included in `docs/SQL_REFERENCE.md` with per-column descriptions,
a Grafana integration example, and cost caveats explaining why `estimated_cost`
may differ from `EXPLAIN` costs.

#### API-003 — Parameter naming convention documented

The parameter naming convention used throughout the SQL API (snake_case names
that match the underlying GUC keys) is now stated explicitly in
`docs/SQL_REFERENCE.md`, making it easier for users to predict parameter names
without consulting source code.

#### API-004 — Generated SQL API catalog uses SQL-facing return types

`scripts/gen_catalogs.py` now maps all Rust return types to their SQL-facing
equivalents before writing `docs/SQL_API_CATALOG.md`. Previously, the generated
catalog exposed internal Rust type names (e.g., `Result<(), PgTrickleError>`,
`pgrx::JsonB`). The catalog now shows SQL types (`void`, `jsonb`, `text`,
`bigint`, `boolean`, `integer`, `double precision`, `SetOf row`) that match
what PostgreSQL actually exposes to users.

#### API-005 — Schedule-mode comparison table

A side-by-side comparison table of all four schedule modes (Duration, Cron,
CALCULATED, IMMEDIATE) is now in `docs/SQL_REFERENCE.md`. The table covers
trigger type, typical latency, change-accumulation behaviour, and when to
use each mode — replacing the previous prose-only description.

#### CODE-003 — Typed domain wrappers: `PgtId` and `StreamTableOid`

Two newtype wrappers were added to `src/catalog.rs`:
- `PgtId(i64)` — wraps catalog row primary keys so they cannot be confused
  with raw integers or other `i64` values at compile time.
- `StreamTableOid(pg_sys::Oid)` — wraps PostgreSQL OIDs for stream tables so
  they cannot be passed where a generic OID is expected.

Both types implement `From`/`Into` and are covered by unit tests.

#### DOC-001 — Repaired `plans/PLAN.md` architecture-doc table

The key architecture documentation table in `plans/PLAN.md` had several rows
corrupted by a merge conflict. The table is restored with clean rows for
COST_MODEL, GUC_CATALOG, LIMITATIONS, COMPARISONS, and PLAN_OVERALL_ASSESSMENT_14.
A docs-lint rule now catches fragment-level table corruption in CI.

#### DOC-002 — README GUC count updated to generated phrase

The README previously hard-coded the GUC count ("115 configuration parameters").
That count was stale (the actual count is 132 and growing). The reference is
replaced with the phrase "All generated configuration parameters" so it stays
accurate without manual maintenance.

#### DOC-004 — Stale-version scanner wired to `just lint-ci`

`scripts/check_stale_versions.sh` scans `Dockerfile.hub` and `Dockerfile.ghcr`
for hard-coded pg_trickle image version tags in comments that have not been
updated to `<version>`. When a release bumps the version, stale comment tags
fail `just lint-ci` immediately rather than silently persisting in the repo.

#### ARCH-004 — Expanded `docs/COMPARISONS.md` with Feldera and IVM matrix

`docs/COMPARISONS.md` now includes:
- A dedicated **vs. Feldera** section comparing dataflow-engine approaches.
- A dedicated **vs. DuckDB / DuckLake** section with combination architecture
  guidance.
- A **Comprehensive IVM comparison matrix** with five sub-tables covering SQL
  coverage, consistency model, CDC, performance profile, and operational model
  for pg_ivm, Materialize, Feldera, DuckDB, and pg_trickle.

---

## [0.74.0] — Test Coverage, CI Integrity & Security Hardening

### What's New

v0.74.0 is the first release in the final pre-1.0 hardening arc.
It focuses on test quality, CI truthfulness, and security best practices.
No schema changes were made in this release.

#### TEST-002 — Condition-based polling replaces fixed stabilization sleeps

Eight `tokio::time::sleep` calls in WAL CDC, safety, and background-worker
tests were replaced with deterministic condition-polling loops. Tests now
complete as soon as the expected system state is reached, reducing flakiness
and cutting average CI wall time for affected test files.

#### TEST-004 — Security-gate CI job for risky path changes

A new `e2e-security-gate` CI job runs targeted E2E tests
(`e2e_safety_tests`, `e2e_ivm_tests`, `e2e_failure_recovery_tests`,
`e2e_wal_cdc_tests`) automatically whenever a PR touches security-sensitive
paths (`src/ivm.rs`, `src/cdc/**`, `src/refresh/**`, `sql/*.sql`).
Uses `dorny/paths-filter` to avoid running the gate on unrelated PRs.

#### TEST-005 — `just coverage-summary` recipe

New `just coverage-summary` recipe runs `cargo llvm-cov` to produce a
per-module coverage summary. Ignores test files themselves so the output
reflects production code coverage.

#### CODE-002 — Unit tests for pure logic in core modules

Added `#[cfg(test)]` unit-test suites to three core modules that previously
had no DB-free tests:
- `src/refresh/codegen.rs`: 15 tests for `build_content_hash_expr`,
  `parameterize_lsn_template`, `build_prepare_type_list`, `build_execute_params`.
- `src/refresh/merge/mod.rs`: 7 tests for `delta_fraction_exceeds_threshold`
  (extracted from `execute_differential_refresh`).
- `src/api/metrics_ext.rs`: 6 tests for `compute_probe_avg_ms`
  (extracted from `metrics_summary_impl`).

#### SEC-001 — Centralized advisory ignores and `just security` recipe

Advisory ignores previously scattered across CI environment variables are now
centralized in `deny.toml` with full metadata comments (CVE, rationale, review
date). Added `cargo-deny` check step to `security.yml`. New `just security`
recipe lets developers reproduce the exact advisory check that CI runs.

#### SEC-002 — IVM trigger shadowing test

Added `test_sec002_ivm_trigger_not_shadowed_by_public_pgtrickle` E2E test that
creates a `pgtrickle.pg_trickle_capture_change()` function in a shadowing
schema and verifies it cannot intercept the IVM trigger.

#### SEC-003 — SQL builder injection audit and lint

New `scripts/check_sql_builder.sh` script scans `src/` for `format!()` patterns
that interpolate values into SQL strings (potential injection vectors). Integrated
as `just check-sql-builder`. Zero vectors found in the current codebase.

#### DEVEX-001 — Re-enabled push-to-main benchmark baselines

The `benchmarks.yml` CI workflow push trigger (accidentally disabled) is
restored. Criterion baselines are now recorded on every push to `main`,
allowing the PR regression gate to compare against a valid baseline.

#### DEVEX-002 — `just lint-ci` recipe

New `just lint-ci` recipe chains `lint`, `check-version-sync`, and
`check-meta-version` into a single command that mirrors what CI enforces.

#### DEVEX-003 — Version tag updates

Version strings in `Dockerfile.hub`, `Dockerfile.ghcr`, and `justfile` updated
from `0.73.0` to `0.74.0`. Build targets and upgrade image defaults aligned.

#### DEP-001 — sqlx 0.8.6 → 0.9.0

Full upgrade of `sqlx` to 0.9.0 across all test files. The 0.9.0 release
requires dynamic SQL strings to be wrapped in `sqlx::AssertSqlSafe(T)`.
Audited and fixed 25+ test files. No production code changes required.

#### DEP-002 — lru 0.16.4 → 0.18.0

Minor version bump. API compatibility verified; no code changes required.

#### DEP-003 — object_store 0.10.2 → 0.13.2

Three-version jump for the DuckLake Parquet sink dependency. Full E2E test
suite passed on the upgraded version.

---

## [0.73.0] — Monitoring Scalability, Operational Resilience & HOT-Friendly Storage

### What's New

v0.73.0 scales scheduler and monitoring hot paths under high stream counts,
improves operator visibility into cleanup retries and launcher health, and
adds a `fillfactor` option for stream table storage heaps to enable HOT
(Heap-Only Tuple) updates on update-heavy differential workloads.

#### PERF-001 — Incremental refresh summary table

Introduced `pgtrickle.pgt_refresh_summary` and moved refresh stats aggregation
to summary-backed queries. This removes repeated per-stream scans over
`pgt_refresh_history` for high-frequency monitoring calls.

#### PERF-002 / ARCH-002 / REL-002 — Cleanup pipeline scalability and durability

Frontier cleanup metadata lookups now batch OID discovery, reducing SPI query
churn. Added durable `pgtrickle.pgt_cleanup_status` state with retry/backoff
metadata so deferred cleanup progress and failures survive worker restarts.

#### PERF-003 / ARCH-003 — Holdback and launcher observability

Added short-lived holdback probe-result caching and exported probe telemetry
(calls, cache hits, last/average latency). Launcher scans now use a combined
database/activity query and publish scan duration and coverage metrics in shared
memory for `metrics_text` and SQL APIs.

#### PERF-005 / PERF-006 / PERF-007 — Hot-path cache improvements

Delta template placeholder replacement now reuses cached Aho-Corasick resolver
automata per template shape. Template merge cache entries are now bounded by a
configurable byte cap with current memory usage exported in `cache_stats()`.
Scheduler-state handling was consolidated around a single metrics/state path to
reduce duplicated per-stream bookkeeping overhead.

#### HOT-1 — fillfactor option for stream table storage heaps

Stream tables now accept a `fillfactor` parameter that controls the PostgreSQL
heap storage fillfactor. pg_trickle's differential refresh path applies changes
via `MERGE` (one `UPDATE` per changed row). With the default `fillfactor = 100`
(pages packed full), every update allocates a new heap tuple on a new page and
a new index entry. Setting `fillfactor` below 100 leaves free space so that
in-place HOT updates fire — eliminating index tuple churn and reducing WAL
volume by 30–50 % on update-heavy workloads.

```sql
SELECT pgtrickle.create_stream_table(
    'my_summary',
    'SELECT region, SUM(revenue) FROM orders GROUP BY region',
    fillfactor => 80
);
```

Accepted range: `10`–`100`. `NULL` (default) = PostgreSQL's built-in default
(100). The setting is stored in `pgtrickle.pgt_stream_tables.storage_fillfactor`
and preserved across `ALTER STREAM TABLE` rebuilds. `bulk_create()` accepts it
as a JSON key `"fillfactor"`.

### Breaking Changes

No user-facing SQL API removals. Monitoring and metrics outputs include new
columns/series for cache bytes, holdback probe metrics, and launcher scan
health.

---

## [0.72.0] — Frontier Durability & Catalog Correctness

### What's New

v0.72.0 closes four correctness-critical findings from Assessment 14. These
were wiring gaps where a safety mechanism appeared to exist in the code but
was either never connected, contained invalid SQL, or silenced its own error
path. No new features are introduced; all changes are correctness fixes.

#### COR-001 / REL-001 / ARCH-001 — Dead frontier code removed; store failure is now fatal

The catalog layer contained a "DUR-1" two-phase tentative-frontier design with
three functions that had no production call sites. The recovery query in one of
those functions contained invalid SQL that would always fail at runtime.
Meanwhile, the real scheduler hot-path silenced frontier-store errors.

All three dead DUR-1 functions are removed. The scheduler now propagates
frontier-store failures so that a store failure aborts the entire refresh
transaction, preventing partial commits. See ADR-004 for the full rationale.

#### COR-002 / API-001 — Outbox `stream_table_oid` now stores the correct OID

`pgtrickle.pgt_outbox_config.stream_table_oid` was storing `pgt_id` (an
internal sequential counter) instead of `pgt_relid` (the actual `pg_class`
OID). All joins against `pg_class` from the outbox were silently wrong.
All five write and read paths are corrected. The migration SQL fixes existing
rows.

#### COR-003 — WAL transition handoff is now atomic

`complete_wal_transition` dropped the CDC trigger before updating the catalog
mode to WAL, creating a race window where writes could be missed. The steps are
reversed and wrapped in `pg_advisory_lock` so the catalog is updated before
the trigger is removed.

#### COR-004 — Replication slot creation guarded against XID-assigned transactions

`create_replication_slot_pristine` now checks for an XID-assigned transaction
before calling the slot-creation primitive and returns an actionable error
instead of crashing with a PostgreSQL internal error.

### Breaking Changes

None for users. Operators who inspect `pgt_outbox_config.stream_table_oid`
directly should note that the migration SQL corrects existing rows.

---

## [0.71.0] — CI Truthfulness, Test Harness & Documentation Cleanup

### What's New

v0.71.0 is an infrastructure quality sprint with no user-visible schema changes.
It tightens CI coverage of fuzz targets and linting, prevents silent drift
between the test harness schema and the extension catalog, and cleans up
long-standing technical debt in dependency advisory policy and documentation.

**CI-001: Dynamic Fuzz Target Matrix**

`fuzz-smoke.yml` now runs `scripts/check_fuzz_targets.py` before any fuzz
execution. The script scans `fuzz/fuzz_targets/*.rs` and fails CI if any target
is absent from the targets list. Three previously missing fuzz targets
(`sql_builder_fuzz`, `merge_sql_fuzz`, `row_id_fuzz`) are now included.

**CI-002: `fuzz-all` Hard Failures**

The `fuzz-all` justfile recipe previously swallowed per-target errors with
`|| true`. It now uses a failure accumulator and exits non-zero if any target
fails. A `fuzz-all-best-effort` alias restores the old lenient behaviour for
developer convenience.

**CI-003: E2E Coverage on Schedule**

The `e2e-coverage` job in `coverage.yml` now also runs on a weekly cron
(`0 2 * * 0`, Sunday 02:00 UTC). The job timeout was trimmed from 120 to 90
minutes to enforce an upper bound.

**CI-004: `docs-lint` Wired into `just lint`**

`just lint` now runs `clippy`, `fmt-check`, `security-definer-check`, and
`docs-lint` in a single invocation. A `just lint-all` alias is provided for
back-compat. The `AGENTS.md` and `CONTRIBUTING.md` contributor guides are
updated to document the new gate.

**DEP-001: Advisory Expiry Metadata**

All five RUSTSEC ignore entries in `deny.toml` now carry structured
`# Review-By:` metadata. `scripts/check_deny_expiry.py` fails CI if any
advisory is past its review date, preventing "silent forever-ignores".
The check runs as a new step in `dependency-policy.yml`.

**CODE-001 / DOC-001: SQL API Catalog Quality Gate**

`scripts/gen_catalogs.py` was rewritten to parse the pgrx-generated SQL output
as the primary source of function signatures (regex fallback for offline builds).
`validate_catalog()` now rejects truncated return types. The catalog was
regenerated; `refresh_efficiency()` now correctly shows `SetOf row (failable)`
instead of the truncated `Result<`.

**CODE-002: Tarjan SCC Error Propagation**

Three `unwrap()` calls inside `tarjan_strongconnect()` are replaced with
`ok_or_else(...)` + `?`. `compute_sccs()` and `condensation_order()` now return
`Result<Vec<Scc>, PgTrickleError>`. Call sites in the scheduler loop and SQL API
helpers use `unwrap_or_else(|e| { pgrx::warning!(...); Vec::new() })` to avoid
cascading signature changes into the public API.

**TEST-005: Generated Test Harness Schema**

`tests/common/mod.rs` used to hand-maintain `CATALOG_DDL`, causing `initiated_by`
and `unit_kind` CHECK constraints to lag behind the extension catalog (missing
`SELF_MONITOR`, `SCHEDULER_FUSED`, `cyclic_scc`, `repeatable_read_group`, and
`fused_chain`). These enum values are now included via a generated file:

- `scripts/gen_test_schema.py` extracts table DDL from the latest archive SQL
  and emits a Rust raw-string literal to `tests/generated/schema.rs`.
- `build.rs` regenerates the file on every `cargo build`.
- `ci.yml` verifies the committed file matches the script output with
  `gen_test_schema.py --check`.

**ARCH-003 / DOC-002: PLAN.md Archival**

The 2,233-line v0.9.0 implementation plan in `plans/PLAN.md` has been moved to
`plans/archive/PLAN_HISTORICAL.md`. `plans/PLAN.md` is now a short Architecture
& Roadmap Index linking to active planning documents.

**DOC-003: plans/INDEX.md Generation**

`scripts/gen_plans_index.py` now generates `plans/INDEX.md` by scanning all 156
files in `plans/**/*.md`, categorising by prefix, and sorting by modification
time. `docs-drift.yml` checks the index is up to date on every push.

### Schema Changes

None. This is a version-bump-only release; `sql/pg_trickle--0.70.0--0.71.0.sql`
contains no DDL.

### Upgrade Notes

Run `ALTER EXTENSION pg_trickle UPDATE TO '0.71.0';`. No table or function
changes — the upgrade is instantaneous.

---

## [0.70.0] — Scheduler, Validator & Security Hardening

### What's New

v0.70.0 is a quality sprint targeting correctness in the LATERAL validator,
scheduler throughput, observability of the history prune loop, scalability of
the launcher rescan path, and security hardening of the publication API.

**COR-002: LATERAL Validator Body Scanning**

Stream tables with LATERAL SRFs or LATERAL subqueries that contain volatile
expressions (e.g. `random()`, `clock_timestamp()`) now correctly trigger the
`volatile_function_policy` check. Before this fix, only the left-hand child of
the LATERAL node was scanned; volatile expressions inside the LATERAL body SQL
were silently skipped, allowing non-deterministic queries to poison differential
maintenance.

**PERF-001: Batched Buffer Health Checks**

`check_slot_health_and_alert()` previously issued one SPI call per monitored
change buffer. It now builds a single `UNION ALL` query for all buffers,
reducing the per-alert overhead from O(n) to O(1) SPI round-trips.

**PERF-002: Batched Fused-Chain Eligibility Lookups**

The fused-chain refresh eligibility loop now fetches all dependency rows for the
candidate set in a single `StDependency::get_for_sts()` query instead of one
per candidate.

**PERF-003: History Prune Interval Now GUC-Controlled**

The background history cleanup previously ran on a hard-coded 24-hour cycle.
`pg_trickle.history_prune_interval_seconds` (default `60`) now controls the
interval. Setting it to `0` disables automatic pruning entirely.

**PERF-004: `delta_work_mem_cap_mb` Default Raised to 256 MiB**

The previous default of `0` (disabled) caused unbounded memory growth during
large differential refreshes. The new default of `256` caps per-refresh memory
use at a safe value for typical deployments.

**SCAL-002: Launcher Install-Epoch Fast-Rescan**

A shared-memory counter (`LAUNCHER_INSTALL_EPOCH`) is now bumped every time
`CREATE EXTENSION pg_trickle` or `DROP EXTENSION pg_trickle` is executed. The
launcher detects the change on the next loop and briefly switches to a 10-second
poll interval before returning to the steady-state 60-second interval. This
eliminates the 60-second dead zone after a fresh extension install.

**SEC-001: Publication Name Parser Unified**

`create_publication()` and `alter_publication()` now share the single
`helpers::parse_qualified_name_pub()` implementation. The redundant local copy
has been removed, closing a potential divergence between the two parse paths.

**OBS-002: Prune Error Visibility**

History prune failures now: (1) increment a shared-memory counter
`HISTORY_PRUNE_ERRORS`, (2) log a `WARNING`, and (3) are exposed via the new
SQL function `pgtrickle.history_prune_status()`. A non-zero
`prune_error_count` from this function indicates the cleanup loop is failing and
`pgt_refresh_history` may be growing unbounded.

### New SQL Functions

| Function | Returns | Description |
|---|---|---|
| `pgtrickle.history_prune_status()` | `(prune_error_count bigint, last_prune_at timestamptz, last_rows_deleted bigint)` | OBS-002: prune error counter and last timing |

### Configuration Changes

| GUC | Old Default | New Default | Notes |
|---|---|---|---|
| `pg_trickle.delta_work_mem_cap_mb` | `0` (disabled) | `256` | PERF-004: caps per-refresh memory |
| `pg_trickle.history_prune_interval_seconds` | hard-coded 24 h | `60` s | PERF-003: set `0` to disable |

---

## [0.69.0] — DuckLake Sink Reliability & Security

### What's New

v0.69.0 focuses exclusively on the DuckLake integration surface — making sink
delivery observable, resilient to transient failures, and safe against
concurrent writes and `search_path` manipulation.

**ARCH-002 / REL-001: Sink Delivery State Machine**

`run_ducklake_sink()` is now a state-machine driver. Every delivery attempt is
tracked in a new catalog table `pgtrickle.pgt_ducklake_sink_delivery`:

```
PENDING → WRITING → DELIVERED
                 ↘ FAILED_RETRYABLE (up to max_retries attempts)
                 ↘ FAILED_PERMANENT
```

Two new GUCs control behaviour:

- `pg_trickle.ducklake_sink_max_retries` (int, default `3`): maximum
  `FAILED_RETRYABLE` transitions before `FAILED_PERMANENT`.
- `pg_trickle.ducklake_sink_failure_mode` (`warn` | `error`, default `warn`):
  whether a `FAILED_PERMANENT` delivery propagates as a PostgreSQL error or is
  silently warned.

**COR-005: View Registration on Query-Only ALTER**

When `pgtrickle.alter_stream_table(name, query => '...')` changes a stream
table's defining query, the matching `ducklake_view` entry is now updated with
the new view definition. Before this fix DuckLake clients continued to see the
old query.

**COR-006: Snapshot ID Advisory Lock**

`register_ducklake_data_file()` now acquires a transaction-scoped advisory lock
(`pg_advisory_xact_lock(table_id)`) before computing `MAX(snapshot_id)`. This
prevents two concurrent sink writes for the same DuckLake table from receiving
the same snapshot ID.

**SEC-002: Qualified Schema Resolution**

All DuckLake catalog writes now use a fully-qualified schema prefix derived
from the new `pg_trickle.ducklake_catalog_schema` GUC (default `"main"`).
Previously, `INSERT INTO ducklake_view ...` and friends were resolved via
`search_path`, which could be manipulated to redirect catalog writes to a
different schema. The `ducklake_view_table_exists()` check has also been
upgraded from `information_schema.tables` to `pg_class JOIN pg_namespace` for
the same reason.

**OBS-001: Sink Health Metrics**

New SQL function `pgtrickle.ducklake_sink_status()`:

```sql
SELECT *
FROM pgtrickle.ducklake_sink_status();
-- stream_table_name | last_delivery_status | last_delivery_at | last_bytes_written | last_rows_written | failed_attempts | last_error
-- revenue_by_region | DELIVERED            | 2026-05-21 ...   |            124 208 |               150 |               0 | NULL
```

**DEP-002: Dependency Policy Documentation**

`docs/DEPENDENCIES.md` documents the criteria for adding, updating, and
removing Rust crate dependencies in pg_trickle, including the DuckLake sink
crate inventory and CI enforcement via `cargo deny`.

### Upgrade Notes

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.69.0';
```

The migration creates `pgtrickle.pgt_ducklake_sink_delivery` with an index.
No existing data is modified. Existing DuckLake deployments continue to work
without configuration changes; the new GUCs default to backward-compatible
values.

If your DuckLake installation uses a schema other than `main`, set:

```sql
ALTER SYSTEM SET pg_trickle.ducklake_catalog_schema = 'your_schema';
SELECT pg_reload_conf();
```

---

## [0.68.0] — Assessment-13 Correctness & Durability Sprint

### What's New

v0.68.0 targets the highest-severity correctness findings from the v0.67.0
overall assessment (Assessment 13). The release resolves four code defects that
produced silent data loss, audit-integrity loss, or operator-visible
misbehaviour — fixing real bugs before adding more features.

**COR-001: Fused Refresh Audit Trail**

Scheduler-driven fused refreshes are now fully recorded in
`pgt_refresh_history`. Before this fix the `initiated_by` CHECK constraint
did not include `'SCHEDULER_FUSED'`, so every fused-refresh audit record was
silently rejected by the constraint and the audit log showed a gap. The
constraint now accepts all five valid values:
`SCHEDULER`, `MANUAL`, `INITIAL`, `SELF_MONITOR`, `SCHEDULER_FUSED`.

**COR-003 / ARCH-001: `change_buffer_durability` GUC is now wired**

The `pg_trickle.change_buffer_durability` GUC (`'unlogged'` / `'logged'` /
`'sync'`) is now honoured when `create_stream_table()` creates a new change
buffer. Previously the GUC was registered but never read; only the legacy
`pg_trickle.unlogged_buffers` bool had any effect. The legacy GUC is preserved
as a backward-compat alias and now emits a `WARNING` advising migration to the
new GUC.

**COR-004: DuckLake Timestamp NULL Fix**

`timestamptz` and `timestamp` columns exported to the DuckLake Parquet sink
now round-trip correctly. Before this fix they were serialised as text (e.g.
`"2023-01-01 00:00:00+00"`) which the Arrow writer could not parse as an
integer, silently producing `NULL` in every exported Parquet file. They are
now serialised as microsecond-epoch integers via
`EXTRACT(EPOCH FROM ...) * 1000000`.

**SCAL-001: Pool Path Deleted**

The persistent background-worker pool introduced in v0.25.0 (SCAL-5) was dead
code: `pg_trickle.worker_pool_size` defaults to 0 and no production deployment
had enabled it. The pool executor pre-dated fused chains, SCC cycles, and
immediate closures and would have broken on activation. The 297-line `pool.rs`
module has been removed. Dynamic per-tick background workers remain the sole
scheduling path (see ADR-024). The GUC is retained as a documented no-op for
backward compatibility.

### Upgrade Notes

Run the upgrade migration:

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.68.0';
```

The migration adds `'SCHEDULER_FUSED'` to the `initiated_by` CHECK constraint
on `pgt_refresh_history`. It is idempotent and safe to run on a live database.

The `pg_trickle.unlogged_buffers` GUC continues to work; the only visible
change is a `WARNING` in the PostgreSQL log advising migration to
`pg_trickle.change_buffer_durability`. No action is required before v1.0.

---

## [0.67.0] — DuckLake Phase 3b: View Registration, Provenance & Ecosystem

### What's New

v0.67.0 completes the DuckLake Phase 3 arc with discoverability and ecosystem
polish: stream tables with a DuckLake sink are now automatically visible to
every DuckLake client as native catalog objects, and every Parquet delta is
traceable back to the exact refresh cycle that produced it.

**F-6: DuckLake View Registration**

When a stream table is created or altered with `sink => 'ducklake'`, pg_trickle
automatically upserts a matching row in `ducklake_view` so the result set is
immediately visible to every DuckDB, Spark, and Trino client that queries the
DuckLake catalog — no manual catalog surgery required:

```sql
SELECT pgtrickle.create_stream_table(
    'revenue_by_region',
    query             => 'SELECT region, SUM(amount) FROM orders GROUP BY region',
    schedule          => '5s',
    sink              => 'ducklake',
    ducklake_sink_path => 's3://my-lake/revenue_by_region/'
);
-- DuckDB now sees:  SELECT * FROM my_lake.revenue_by_region;
```

When the stream table is dropped, the `ducklake_view` entry is removed in the
same transaction. If DuckLake is not installed (i.e. the `ducklake_view` table
does not exist), registration is silently skipped — no error.

**INT-11: Snapshot Provenance & Audit Trails**

Every successful DuckLake sink run now:

1. Writes the `created_by` field in `ducklake_snapshot` with a structured
   identifier: `pg_trickle/<version>/stream_table/<oid>/<name>`.
2. Inserts a row into the new `pgtrickle.pgt_ducklake_provenance` catalog table:

```sql
SELECT *
FROM pgtrickle.pgt_ducklake_provenance
ORDER BY written_at DESC LIMIT 10;
--  provenance_id | stream_table_oid | stream_table_name | ducklake_snapshot_id | delta_row_count | written_at
--            1   |        42        | revenue_by_region |          7           |       150       | 2026-05-20 ...
```

This table enables end-to-end lineage queries: from the raw PostgreSQL event,
through the differential computation, to the Parquet file on object storage.

**New catalog table:** `pgtrickle.pgt_ducklake_provenance`

| Column | Description |
|--------|-------------|
| `stream_table_oid` | OID (pgt_id) of the producing stream table |
| `stream_table_name` | Human-readable name |
| `ducklake_snapshot_id` | The DuckLake snapshot ID |
| `refresh_id` | pg_trickle internal refresh sequence number |
| `delta_row_count` | Rows in the Parquet delta |
| `written_at` | Timestamp |

**Three new tutorials:**

- [Tutorial 3: The Modern Data Stack in One Box](docs/tutorial-modern-data-stack-one-box.md) —
  PostgreSQL + pg_trickle + DuckLake + DuckDB in a single `docker compose up`.
- [Tutorial 4: Streaming PostgreSQL to a Data Lake without Kafka](docs/tutorial-streaming-postgres-to-data-lake.md) —
  replicating a PostgreSQL table into DuckLake using the sink output mode.
- [INT-10: pg-tide DuckLake Pipeline Tutorial](docs/tutorial-pg-tide-ducklake-pipeline.md) —
  transactional relay from pg_trickle stream tables to DuckLake via pg-tide.

**Two new containerised demos:**

- [Demo C: Multi-Engine Leaderboard](demos/ducklake-leaderboard/) —
  a game leaderboard maintained in both PostgreSQL and DuckLake simultaneously.
- [Demo E: OLTP-to-Lake Loop](demos/ducklake-oltp-lake/) —
  end-to-end from PostgreSQL order inserts to DuckLake snapshots on MinIO.

### Breaking Changes

None. All new columns default to NULL. Existing stream tables are unaffected
until explicitly configured with a sink.

### Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.67.0';
```

---

## [0.66.0] — DuckLake Phase 3a: Parquet Sink Infrastructure

### What's New

v0.66.0 adds the foundational write path that lets pg_trickle stream tables
export their computed results directly into a DuckLake-compatible Parquet data
lake, closing the loop from PostgreSQL OLTP → incremental view maintenance →
object-storage lake.

**F-1 / F-2: Parquet Sink Mode & API**

Two new parameters on `create_stream_table()` and `alter_stream_table()`:

- `sink => 'ducklake'` — enable the DuckLake sink for this stream table.
  Accepted values: `'ducklake'`/`'append'` (write new files per refresh),
  `'replace'` (overwrite the previous file), `'none'` (disable).
- `ducklake_sink_path => 's3://bucket/prefix/'` — object-store path where
  Parquet files are written. Supports `file://` (local) and `s3://` (AWS S3).

Three new catalog columns in `pgtrickle.pgt_stream_tables`:
- `ducklake_sink_mode TEXT` — `'append'` or `'replace'` (NULL = disabled).
- `ducklake_sink_path TEXT` — fully-qualified object-store path.
- `ducklake_sink_table_id BIGINT` — DuckLake `table_id` for catalog writes.

**F-3: Parquet Serialisation** (Rust `parquet` + `arrow-array` crates)

Stream table output is serialised to Apache Parquet with per-column Arrow
type mapping (INT64, FLOAT64, BOOL, TIMESTAMP, UTF8). Compression defaults
to Snappy; ZSTD is also supported via the
`pg_trickle.ducklake_sink_compression` GUC.

**F-4: Object-Store Upload**

- `file://` scheme: direct filesystem write (no network, suitable for development,
  NFS, and EFS mounts).
- `s3://` scheme: synchronous upload via the `object_store` crate with a
  per-call single-threaded Tokio runtime. Credentials configured via GUCs:
  `pg_trickle.ducklake_sink_s3_region`, `pg_trickle.ducklake_sink_s3_endpoint`,
  `pg_trickle.ducklake_sink_s3_access_key`, `pg_trickle.ducklake_sink_s3_secret_key`.
  Omit the key GUCs to use the AWS credential chain (environment variables, IAM role).

**F-5 / F-6: DuckLake Catalog Transaction Writer**

When `ducklake_sink_table_id` is set, each sink run inserts into:
- `ducklake_data_file` — registers the new Parquet file with row count and
  file size.
- `ducklake_table_stats` — updates cumulative row and file counts.
- `ducklake_snapshot` — creates a new snapshot entry so DuckDB readers see
  the new data immediately on the next query.

**F-9: Encryption Key Pass-Through**

When `pg_trickle.ducklake_sink_encryption_key_prefix` is set, the sink
generates a per-file key ID (`<prefix>/<table_id>/<epoch_ms>`) and records it
in `ducklake_data_file.encryption_key_id`. Key management is handled externally;
pg_trickle only records the reference.

**Sink Scheduling Integration**

The DuckLake sink runs after every successful scheduler refresh. Failures are
logged as warnings and never block the next scheduled refresh — best-effort
semantics match the DuckLake writer model (orphaned Parquet files are
garbage-collected by DuckLake VACUUM).

### Breaking Changes

None. All new columns default to NULL and all new API parameters default to NULL.
Existing stream tables are unaffected until explicitly configured.

### Upgrade

Run the migration:
```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.66.0';
```

---

## [0.65.0] — DuckLake Phase 2: Change-Feed Adapter

### What's New

v0.65.0 wires DuckLake's `table_changes()` API directly into pg_trickle's CDC
pipeline so that refreshes do O(Δ) work — proportional to the change, not the
table size — instead of the previous O(N) full `EXCEPT ALL` scan.

**F-1: DuckLake Change-Feed Adapter (`CdcMode::DuckLakeChangeFeed`)**

Replaces generic polling for DuckLake foreign tables with a snapshot-aware
adapter that calls `table_changes(from_snapshot, to_snapshot)`. The adapter
tracks `last_consumed_snapshot_id` rather than LSN and is detected automatically
when the source table's foreign data wrapper is identified as DuckLake.

**F-3: Snapshot-Based Frontier**

Extends the frontier model to carry DuckLake snapshot IDs alongside WAL LSNs
and clock-based markers. A frontier row for a mixed-source stream table looks
like:

```json
{
  "ducklake:lake.events":  { "snapshot_id": 42 },
  "ducklake:lake.users":   { "snapshot_id": 38 },
  "wal:postgres":          { "lsn": "0/16A4F08" }
}
```

This lets a single stream table join a PostgreSQL OLTP table with a DuckLake
analytics table and still have a coherent, single-transaction consistency
guarantee.

**F-5: Inlined-Data Trigger Adapter**

Adds a specialised trigger function for DuckLake tables stored as
`ducklake_inlined_data_table_<id>_<version>` (native PostgreSQL tables with
virtual columns `row_id`, `begin_snapshot`, `end_snapshot`, `is_deleted`). The
adapter translates those columns into standard INSERT/DELETE change-buffer rows
and a DDL watcher recreates the trigger whenever DuckLake rotates the inlined
table to a new schema version. Enables sub-millisecond CDC for small,
high-frequency DuckLake event streams.

**F-7: Row-ID Plumbing**

Extends the row-identity interface to accept a caller-supplied stable
identifier. For DuckLake sources, the `rowid` virtual column is used directly,
eliminating the hash computation and enabling exact O(1) delta application.

**F-8: Snapshot-Window Compaction Safety**

Detects when `last_consumed_snapshot_id` falls before DuckLake's compaction
horizon (old snapshots expired) and applies the configured policy:

- `fallback` (default) — automatically falls back to a full refresh and logs a
  warning.
- `error` — raises a clear, actionable error rather than silently re-scanning.

Configure via:
```sql
ALTER STREAM TABLE … SET (ducklake_compaction_policy = 'error');
```

**New GUC:** `pg_trickle.ducklake_compaction_policy` — cluster-wide default
for the compaction safety policy (`'fallback'` or `'error'`).

### Tutorials

- **Tutorial 2: "IVM for DuckLake before v2.0"** — walks through creating a
  stream table on a DuckLake foreign table and benchmarks the change-feed
  adapter (v0.65.0) against the generic polling path (pre-v0.65.0): same
  result, 100× less work on large tables.
- **Tutorial 6: "Sub-millisecond inlined-data CDC"** — demonstrates the
  inlined-data fast path for small DuckLake tables kept in PostgreSQL.

### Demo

**Demo B: Time-travel debugging** — a stream table over a DuckLake change-feed
queried at a specific past snapshot ID. Shows the snapshot-based frontier:
roll back `last_consumed_snapshot_id` and the stream table rewinds
deterministically. Ships as a self-contained `docker-compose up` demo.

### Breaking Changes

None.

### Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.65.0';
```

---

## [0.64.0] — DuckLake Ecosystem Phase 1

### What's New

v0.64.0 is a pure documentation, demo, and community release — no changes to
the extension binary, SQL schema, or GUCs. It launches pg_trickle as a
recognised first-class participant in the DuckLake ecosystem and demonstrates
that pg_trickle is the incremental view maintenance engine that every
PostgreSQL-backed DuckLake deployment has been waiting for.

DuckLake v1.0 (released April 2026) stores all its bookkeeping in a standard
SQL database — most commonly PostgreSQL. pg_trickle lives in that same database.
DuckLake's own public roadmap lists "Materialized views and incremental
maintenance" as a future item tagged *"looking for funding."* This release
plants the flag: pg_trickle already does this, it works today, and here is how.

**Tutorial T-1: Real-Time Dashboards on Your Data Lake**
(`blog/ducklake-real-time-dashboards.md`)

DuckDB writes synthetic events into a DuckLake table. pg_trickle stream tables
compute per-minute revenue and funnel aggregations incrementally. Grafana
displays the results live with a five-second auto-refresh. Step-by-step setup
from DuckLake PostgreSQL catalog to live dashboard.

**Tutorial T-2: The Modern Data Stack in One Box**
(`blog/ducklake-modern-data-stack.md`)

Replaces the classic seven-system Debezium-Kafka-Flink-Iceberg pipeline with
a single PostgreSQL instance plus an S3 bucket. OLTP in PostgreSQL, pg_trickle
stream tables for real-time aggregations, DuckLake for historical Parquet
storage, DuckDB for ad-hoc queries. Includes a system comparison table and an
honest discussion of the limitations.

**Tutorial T-3: Monitoring Your DuckLake with pg_trickle**
(`blog/ducklake-monitoring.md`)

DuckLake's ~28 metadata tables live in PostgreSQL and are rich with operational
signals. This tutorial builds five monitoring stream tables:
- `ducklake_small_file_counts` — compaction alerts
- `ducklake_snapshot_rate` — commit rate spikes
- `ducklake_storage_growth` — capacity planning
- `ducklake_tenant_activity` — per-tenant billing / quota
- `ducklake_compaction_events` — compaction audit trail

Includes Grafana dashboard definitions and alert thresholds.

**Blog B-1: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM**
(`blog/ducklake-ivm-missing-piece.md`)

Thought-leadership post for Hacker News / r/dataengineering. Frames the
IVM gap in existing lakehouse formats, explains why DuckLake's SQL-catalog
architecture changes the equation, and shows how pg_trickle fills the gap
with zero external infrastructure. References DuckLake's own roadmap item.

**Blog B-2: DuckLake's `table_changes()` Meets pg_trickle's DVM Engine**
(`blog/ducklake-table-changes-dvm.md`)

Technical deep-dive for the systems-programming audience. Maps DuckLake's
change-feed output format row-by-row to pg_trickle's internal change-buffer
schema (signed-multiset weights, `pgt_weight` column). Covers the `rowid`
advantage, the inlined-data sub-millisecond path, snapshot IDs as frontier
values, and the Phase 2 adapter roadmap.

**Documentation D-1: DuckLake section in foreign-table-sources.md**
(`blog/foreign-table-sources.md` — updated)

Added a dedicated "DuckLake Sources" section with three integration paths:
bridge-table (works today, trigger CDC), metadata-table monitoring, and
foreign-table polling. Preview of the Phase 2 `table_changes()` adapter.

**Demo A: The Five-Second Funnel**
(`demos/ducklake-funnel/`)

Self-contained `docker compose up` demo with PostgreSQL + pg_trickle, a Python
event generator (50 events/second), Grafana (live funnel and revenue panels),
and MinIO (S3-compatible storage). Configurable via `.env`. Runs on any
developer laptop.

**Demo D: DuckLake Observability in a Box**
(`demos/ducklake-observability/`)

Attaches to an **existing** DuckLake PostgreSQL catalog. Run `init_monitoring.sql`
to install five stream tables, then `docker compose up grafana` for a
production-quality observability dashboard. Five minutes from `git clone` to
live operational visibility. Includes `teardown_monitoring.sql` for clean removal.

**Community Outreach Plan**

Documented outreach plan for six named DuckLake production users (PostHog,
Windmill, locals.com, Ascend.io, Sliplane, Media Cluster Norway). Complete CFP
submission texts for DuckCon, PGConf EU, and PGCon. GitHub Discussion draft for
`duckdb/ducklake`.

### Upgrading

No migration required. The upgrade script `pg_trickle--0.63.0--0.64.0.sql`
contains only documentation comments. The extension binary and SQL schema are
unchanged.

---

## [0.63.0] — Fused Multi-Node Refresh

### What's New

v0.63.0 introduces CTE-fused multi-node refresh: the scheduler now composes the
delta SQL for an entire topological batch of stream-table nodes into a **single**
`WITH … MERGE; MERGE; …` statement, reducing per-node SPI round-trips and giving
the PostgreSQL planner visibility across the entire batch.

**PERF-2: CTE-Fused Multi-Node Refresh**

In v0.62.0 and earlier the scheduler issued one SPI call per node per tick.
For a DAG with N DIFFERENTIAL nodes sharing source tables, this meant N
sequential `Spi::run` calls.  v0.63.0 introduces `fuse_diff_batch`, which:

- Parses each node's delta SQL into its constituent CTEs.
- Renumbers CTE names across nodes (node *k* gets counter offset *k × 100*)
  to prevent collisions.
- Deduplicates source-scan CTEs whose SQL bodies are byte-identical (same
  source OID, same LSN bounds), so each change-buffer range is scanned at
  most once across the entire batch.
- Wraps each node's final MERGE in a named `_apply_<pgt_id>` CTE so all
  intermediate results are available to subsequent nodes in the chain.
- Emits a single SQL string and executes it in **one** `Spi::run` call.

The fused path is active by default.  Two new GUCs give operators fine-grained
control:

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.enable_fused_refresh` | `true` | Enable/disable CTE fusion globally. Set to `false` to revert to v0.62.0 sequential-per-node behaviour. |
| `pg_trickle.fused_refresh_max_delta_rows` | `500000` | Nodes whose estimated pending-row count exceeds this threshold are excluded from the fused batch and refreshed sequentially. Set to `0` to disable the size gate. |

Nodes in `FULL` refresh mode are always excluded from fusion (they use
a different code path with no delta SQL).

### Upgrading

No schema migration is required.  The `pg_trickle--0.62.0--0.63.0.sql` upgrade
script is included and contains no SQL changes; the new GUCs are registered
automatically by the shared library.

---

## [0.62.0] — Scheduler Throughput & pg_aqueduct Prerequisites

### What's New

v0.62.0 delivers a change-buffer fan-out optimisation that eliminates redundant
scans in multi-consumer DAGs, and three SQL API additions required by the
planned `pg_aqueduct` migration tool.

**PERF-1: Change-Buffer Fan-Out**

The scheduler now scans each source's change buffer **once per tick** and routes
the resulting delta to every dependent stream table, rather than each dependent
node re-scanning the buffer independently.  For a DAG with N consumers of the
same source table this reduces change-buffer I/O from O(N) to O(1).  Controlled
by a new GUC `pg_trickle.enable_change_buffer_fanout` (default: `true`).

**API-1 & API-2: `pgtrickle.pause_scheduler` / `pgtrickle.resume_scheduler`**

New SQL functions `pgtrickle.pause_scheduler(nodes text[])` and
`pgtrickle.resume_scheduler(nodes text[])` allow operators and migration tools
to pause and resume the differential refresh scheduler for specific stream tables.
In-flight refreshes are drained before `pause_scheduler` returns, with a
configurable timeout (`pg_trickle.scheduler_drain_timeout`, default 30 s).

**API-3: `pgtrickle.stream_table_spec(relid oid | qualified_name text)`**

Returns a stable JSON projection of a single stream table's specification,
including query, refresh mode, schedule, CDC mode, and outbox attachment.
The canonical representation needed by `pg_aqueduct` for import, drift detection,
and spec-hash computation.  Also available by qualified name.

### Upgrade Notes

No breaking changes.  The fan-out optimisation is transparent; set
`pg_trickle.enable_change_buffer_fanout = false` to revert to per-node scan
behaviour.  New functions are additive.

---

## [0.61.0] — DX, Documentation & Final Pre-1.0 Polish

### What's New

This release focuses on developer experience improvements, documentation
completeness, and final correctness hardening before the 1.0 milestone.
There are no schema changes — all improvements are pure Rust or documentation.

**Developer experience (DX-1, DX-2)**

- **Foreign-owner attachment detection (DX-1):** `pgtrickle.health_check()`
  now includes a ninth check — `attachment_owner_check` — that detects when
  `pgtrickle.pgt_outbox_config` or `pgtrickle.pgt_publication_config` rows are
  owned by a role different from the current session's role.  Returns a WARNING
  with actionable detail text.

- **SQL reference completeness check (DX-2):** New `scripts/gen_sql_reference.py`
  script compares `#[pg_extern]` symbols in source against `docs/SQL_REFERENCE.md`
  and fails with a diff when a new public function is added without documentation.
  Wired into the `upgrade-check` CI job so every PR is validated.

**Correctness hardening (COR-7, COR-8, COR-9)**

- **ctid invariant comment (COR-7):** Added explicit `// INVARIANT:` comment
  before the `ctid`-based deletion CTE in `src/refresh/phd1.rs` documenting
  the snapshot-stability guarantee that makes `ctid` deletion safe.

- **Snapshot cache secondary equality check (COR-8):** `get_or_register_snapshot_cte()`
  now performs a secondary canonical-string comparison when a hash hit occurs.
  On collision, the cached entry is evicted and `pg_trickle_snapshot_cache_collisions_total`
  (a new shared-memory counter) is incremented.

- **DiffContext cte_counter reset (COR-9):** `differentiate()` now resets
  `self.cte_counter = 0` at the start of each invocation, preventing stale CTE
  name reuse across separate calls on the same `DiffContext` instance.

**Security (SEC-5)**

- **Outbox table name collision prevention (SEC-5):** `outbox_table_name_for()`
  now appends an 8-hex-character xxh64 hash suffix when the stream table's
  identifier exceeds 63 bytes.  This prevents silent name truncation from
  causing two distinct stream tables to map to the same outbox table.

**Code quality (QUAL-4, QUAL-5)**

- **`sublinks.rs` decomposition (QUAL-4):** The 7 000-line `sublinks.rs`
  monolith has been split into a `sublinks/` directory with focused sub-modules:
  `having.rs` (HAVING rewrites), `exists.rs`, `in_list.rs`, `scalar.rs`.

- **Brittle `split().nth()` test fix (QUAL-5):** Two test patterns in
  `lateral_subquery.rs` that used `sql.split("marker").nth(1).unwrap_or("")`
  were rewritten to `splitn(2, …).collect()` with an explicit assertion, so
  test failures emit a diff rather than silently passing on an empty string.

**Features (FEAT-1, FEAT-2)**

- **SEARCH/CYCLE clause clear error (FEAT-1):** `extract_cte_map_with_recursive()`
  now detects `SEARCH BREADTH FIRST BY` and `CYCLE … SET … USING` clauses and
  returns `UnsupportedOperator` with a clear hint message instead of silently
  producing incorrect output.

- **LATERAL + DIFFERENTIAL documentation (FEAT-2):** New "LATERAL Joins and
  DIFFERENTIAL Mode" table in `docs/DVM_OPERATORS.md` and `docs/LIMITATIONS.md`
  documenting which LATERAL patterns are fully supported, which have caveats, and
  which fall back to FULL refresh with guidance for each.

**Documentation (DOC-1, DOC-2)**

- **Three foundational ADRs (DOC-1):** Created `plans/adrs/ADR-001.md`
  (Trigger-Based CDC), `ADR-002.md` (Z-Set Formalism), and `ADR-003.md`
  (EC-01 Join-Correctness Invariant) capturing the architectural rationale
  for core design decisions.  `PLAN_ADRS.md` updated to ACTIVE.

- **Multi-column NOT IN + NULL documentation (DOC-2):** New subsection in
  `docs/LIMITATIONS.md` explaining when `(col1, col2) NOT IN (SELECT …)` falls
  back to subquery-based computation (nullable columns) and how to restore
  anti-join performance with `IS NOT NULL` guards or a `NOT EXISTS` rewrite.

### Upgrade Notes

No SQL schema changes.  `ALTER EXTENSION pg_trickle UPDATE` is sufficient.

---

## [0.60.0] — Code Quality, Test Coverage & CI

### What's New

This release focuses on engineering quality across three areas: code
maintainability, test depth, and CI reliability.  There are no schema
changes — all improvements are pure Rust.

**Correctness fixes (COR-5, COR-6)**

- **WAL decoder OID-based table filter (COR-5):** `poll_wal_changes` now
  resolves canonical table names via `pg_class` and `pg_inherits` once per
  poll cycle instead of string-matching against a user-supplied name.  This
  correctly handles quoted identifiers, search-path-sensitive names, and
  partition routing where a child table appears in `test_decoding` output.

- **Publication rebuild detects table-becomes-partition (COR-6):** The
  `needs_publication_rebuild` check now also detects when a plain source
  table has been attached to a parent as a partition (`pg_inherits.inhrelid`).
  Previously this condition was silently missed, causing CDC to freeze.

**Code quality (QUAL-1–3)**

- Scheduler log levels standardised: routine operational messages use
  `info!()`, leaving `warning!()` and `error!()` for actionable conditions.
- `refresh/codegen.rs` decomposed: pure SQL-fragment helpers moved into
  `refresh/sql_fragments.rs` for independent testability.
- `src/cdc.rs` decomposed into a proper module tree (`cdc/triggers.rs`,
  `cdc/buffer.rs`, `cdc/compact.rs`, `cdc/partition.rs`).

**Test coverage (TEST-1–5)**

- Refresh orchestrator: 8 adaptive-threshold and cost-model unit tests.
- CDC pure-logic: 13 unit tests across the new cdc/ submodules.
- DDL hook classification: 14-case table-driven test.
- Fixed brittle `sleep()` in 4 E2E tests — replaced with
  `wait_for_condition` / `wait_for_auto_refresh` polling loops.
- DVM differential engine: 4 property tests via `proptest`.

**CI improvements (CI-1–3)**

- Path-filtered full E2E job on PRs: the `full-e2e-on-dvm-change` job
  automatically triggers the full E2E suite when a PR touches
  `src/dvm/`, `src/refresh/`, or `src/cdc/`.
- `tests/Dockerfile.e2e` now sets `USER postgres` — containers run as
  the unprivileged OS user instead of root.
- Codecov per-module thresholds added for `src/cdc/`, `src/hooks.rs`,
  and `src/wal_decoder.rs` (50%, 40%, 40% patch coverage gates).

---

## [0.59.0] — Performance & Observability

### What's New

v0.59.0 delivers seven hot-path performance improvements and six new
observability features. No behaviour-visible SQL API changes are made; the only
schema change is a new `defining_query_hash` catalog column used internally.

#### PERF-1: Batched CDC Buffer-Growth Monitoring

`check_change_buffer_sizes()` previously issued one `SELECT count(*)` SPI call
per source table, proportional to the number of CDC-enabled stream tables.
It now builds a single `UNION ALL` query and executes it in one SPI round-trip,
reducing latency and lock overhead for deployments with many stream tables.

#### PERF-2: Defining-Query Hash Cached in Catalog

A new `defining_query_hash BIGINT` column on `pgtrickle.pgt_stream_tables` caches
the Rust `DefaultHasher` digest of each stream table's defining query. Refresh
cycles skip recomputing the hash; any ALTER that changes the query updates it
atomically in the same SPI transaction.

#### PERF-3: Arc<str> Shared Templates

All eight SQL template fields inside `CachedMergeTemplate` were changed from
`String` to `Arc<str>`. Cache reads now clone a reference-counted pointer
instead of copying the string data, reducing heap allocations on every cache hit.

#### PERF-4: Single MERGE_TEMPLATE_CACHE Borrow

The two consecutive `MERGE_TEMPLATE_CACHE.with()` calls that were needed to
check both the `non_monotonic` flag and the `is_deduplicated` flag have been
merged into a single `peek()` call that returns both values in one borrow, halving
the thread-local lock traffic on the hot cache-hit path.

#### PERF-5: WAL Decoder UPDATE Vec Pre-Allocation

The five `Vec` accumulators in the WAL decoder's UPDATE-row handler now call
`Vec::with_capacity(num_columns)` up front, eliminating the incremental
reallocations that previously occurred for each column.

#### PERF-6: Frontier Borrow Instead of Clone

`has_stream_table_source_changes()` cloned the entire `Frontier` (a
`HashMap<Oid, Lsn>`) when no frontier was stored yet. It now borrows a static
empty `Frontier` via `Frontier::empty_ref()`, avoiding the allocation on every
scheduler tick for stream tables with no CDC sources.

#### PERF-7: Diamond Detection Short-Circuit

`detect_diamonds()` in the DAG module now performs a lazy `.next().is_some()`
intersection check before collecting the full shared-ancestor list. Branches that
share no ancestors — the common case — exit immediately without allocating the
result `Vec`.

#### OBS-1: CDC Lag Percentile Metrics

A ring-buffer sampler (`CdcLagSampler`, 256 slots, protected by `PgLwLock`)
records CDC-to-refresh lag in milliseconds. Three new Prometheus gauges expose
rolling percentiles: `pg_trickle_cdc_lag_p50_seconds`,
`pg_trickle_cdc_lag_p95_seconds`, and `pg_trickle_cdc_lag_p99_seconds`.

#### OBS-2: Parallel Worker Utilisation Metrics

Two new counters make pool-worker pressure visible:
- `pg_trickle_parallel_queue_depth` — jobs currently waiting for a free worker
- `pg_trickle_worker_idle_time_seconds_total` — cumulative idle time across all workers

#### OBS-3: WAL Decoder Pending-Record Gauge

`pg_trickle_wal_decoder_pending_records` reports the number of logical-replication
records buffered in the last WAL poll that have not yet been written to the CDC
change buffer, useful for detecting WAL consumer backpressure.

#### OBS-4: Refresh Mode Ratio Counters

`pg_trickle_refresh_mode_total{mode="differential"}` and
`pg_trickle_refresh_mode_total{mode="full"}` count every refresh cycle by mode.
The ratio surfaces differential-to-full degradation before it impacts latency.

#### OBS-5: pg_stat_activity Application Names

Every background-worker connection now sets `application_name` immediately after
connecting to SPI, making pg_trickle workers trivially identifiable in
`pg_stat_activity`:

| Connection | `application_name` |
|---|---|
| Database-discovery launcher | `pg_trickle_launcher` |
| Per-database scheduler | `pg_trickle_scheduler` |
| Parallel refresh pool worker (N) | `pg_trickle_pool_N` |
| Parallel refresh dispatcher | `pg_trickle_dispatcher` |

#### OBS-6: Backup & Restore Documentation

`INSTALL.md` now includes a dedicated **Backup & Restore** section explaining
which schemas to include in `pg_dump`, how to validate catalog integrity after
restore with `pgtrickle.health_check()`, and how to handle OID re-assignment
with `repair_stream_table()`.

### Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.59.0';
```

The upgrade script adds the `defining_query_hash` column with `DEFAULT 0`.
Existing stream tables will recompute their hash on the next refresh and write
it back via `ALTER STREAM TABLE` — no manual intervention is needed.

---

## [0.58.0] — Security & Correctness Hardening

### What's New

v0.58.0 closes all HIGH-severity findings from the v0.57.0 overall assessment
(Report 12).  No new SQL API surface is added — every change is a targeted
security fix or correctness fix.

#### SEC-1/2: Ownership Checks for Outbox and Publication APIs

`attach_outbox()`, `detach_outbox()`, `attach_embedding_outbox()`,
`stream_table_to_publication()`, and `drop_stream_table_publication()` now call
`check_stream_table_ownership()` immediately after resolving stream table metadata.
Previously, any role with `EXECUTE` on the `pgtrickle` schema could attach an
outbox or create a publication for a stream table owned by a different role.
Non-owner callers now receive `ERROR: must be owner of stream table`.

#### COR-1: Multi-Column NOT IN + NULL Row Handling

The v0.55.0 multi-column `IN` rewrite now detects `NULL` constants on either
side of the row constructor in `NOT IN` expressions. When detected, the AntiJoin
rewrite is skipped and the original subquery-based execution path is used,
emitting a diagnostic `NOTICE`. See [LIMITATIONS.md](docs/LIMITATIONS.md) for
details.

#### COR-2: Recursive-CTE Depth Guard in DIFFERENTIAL Mode

`pg_trickle.ivm_recursive_max_depth` GUC now applies consistently to both
DIFFERENTIAL and IMMEDIATE modes. Previously only IMMEDIATE mode enforced the
depth limit.

#### COR-3: WAL Decoder TOCTOU Advisory Lock

`poll_source_changes()` now acquires a `pg_advisory_xact_lock` keyed on the
source OID before calling `poll_wal_changes()`, serialising the eligibility
check and WAL consumption into an atomic unit.

#### COR-4: Compact-Buffer Lock Contention Is Observable

`compact_change_buffer()` now returns `CompactionResult::Contended` instead of
`Ok(0)` when it cannot acquire the advisory lock, increments the new shared-memory
counter `pg_trickle_cdc_compact_contended_total`, and exposes it via the
Prometheus `/metrics` endpoint.

#### SEC-3: DDL Hook Escalates on SPI Failure

`handle_alter_table()` now retries `find_downstream_pgt_ids()` once on SPI error
and, if the retry also fails, raises `pgrx::error!()` to block the originating
ALTER TABLE rather than silently returning.

#### SEC-4: Schema Identifier Quoted in CDC Buffer Names

`buffer_qualified_name_for_oid()` now uses `sql_builder::qualified()` to properly
quote the schema identifier in the change-buffer table path.

### Upgrade Notes

No SQL schema changes. No `ALTER EXTENSION` migration is required.

---

## [0.57.0] — Documentation Excellence

### What's New

v0.57.0 completes the Documentation Excellence Arc. It delivers four new
end-to-end tutorials, resolves all P2/P3 quality gaps from the Round 2
documentation audit, and applies a full consistency pass across all 83
documentation files.

**New tutorials (P1):**
- `docs/tutorials/FIRST_DASHBOARD.md` — Build a real-time analytics dashboard
  backend over an e-commerce dataset: revenue by region, hourly order counts,
  top-10 products chain, and optional Grafana integration.
- `docs/tutorials/EVENT_SOURCING.md` — Stream tables as CQRS read-model
  projections over an event-sourced write model: current order state, customer
  lifetime value, and inventory levels maintained incrementally.
- `docs/tutorials/BACKFILL_AND_MIGRATION.md` — Zero-downtime migration from
  `REFRESH MATERIALIZED VIEW` to a stream table: pre-migration assessment,
  `validate_query()` check, parallel running, verification, cutover, and
  rollback.
- `docs/tutorials/SECURITY_HARDENING.md` — Role separation, CDC trigger
  ownership, change-buffer protection, and audit logging; copy-paste SQL
  templates for all GRANT statements and a verification checklist.

**Quality improvements (P2):**
- `docs/SECURITY_GUIDE.md`: Added "Copy-Paste Templates" section with
  `CREATE ROLE` and `GRANT` statements for `pgtrickle_admin`,
  `pgtrickle_user`, and `pgtrickle_readonly`.
- `docs/WHATS_NEW.md`: Backfilled user-impact summaries for v0.1 through
  v0.7.
- `docs/tutorials/HYBRID_SEARCH_PATTERNS.md`: Expanded patterns 2
  (RLS-scoped) and 3 (tiered storage) to match quality of pattern 1;
  documented `pg_trickle.enable_vector_agg` GUC.
- `docs/tutorials/PER_TENANT_ANN_PATTERNS.md`: Documented
  `partition_key => 'HASH:<col>:<buckets>'` syntax with a partition-count
  guide; expanded patterns 2–3 with full step-by-step examples.
- `docs/QUICKSTART_5MIN.md`: Fixed display-text inconsistency on
  Installation link.
- `docs/PERFORMANCE_COOKBOOK.md`: Added three worked examples to §13:
  (a) `max_diff_ctes` hit and recovery, (b) detecting when FULL beats
  DIFFERENTIAL via `recommend_refresh_mode()`, (c) deep-join chain and
  `max_differential_joins`.
- `docs/SECURITY_MODEL.md`: Resolved supply-chain TODO items — filled
  current status or marked "Planned for v1.0" with implementation notes.

**Polish (P3):**
- `docs/FAQ.md`: Converted plain-text GUC cross-references to markdown
  links pointing to `CONFIGURATION.md` anchors; added link to
  `SQL_REFERENCE.md`.
- `docs/DVM_OPERATORS.md`: Added quick-reference table at the top
  (operator name, mode support, section anchor).
- `docs/tutorials/VECTOR_RAG_STARTER.md`: Added full parameter breakdown
  for `pgtrickle.embedding_stream_table()` with a parameter table and
  examples.
- `docs/tutorials/tuning-refresh-mode.md`: Added prose explanation of
  composite score thresholds (+0.15/−0.15) and dead-zone tuning.
- `docs/research/multi_db_refresh_broker.md`: Added implementation status
  banner.

**Consistency pass (DOC-CONS-28..31):**
- Terminology sweep: enforced `stream table`, `differential refresh`,
  `change buffer`, `refresh frontier`, `CDC`, `DVM`, `DAG` across all
  83 docs files.
- Capitalisation sweep: enforced `pg_trickle` lowercase, `PostgreSQL`
  (not `Postgres`), `pgtrickle` schema, `pgrx` lowercase.
- Code style sweep: SQL keywords uppercase; `pgtrickle.` prefix on all
  function calls; language hints added to unlabelled code blocks.
- Cross-link audit: verified all internal `[text](path.md)` links;
  fixed 7 broken links (`USE_CASES.md`, `integrations/multi-tenant.md`,
  and added `docs/ESSENCE.md` mdbook include).

---

## [0.56.0] — Documentation Foundation

### What's New

v0.56.0 is the first release of the Documentation Excellence Arc, resolving all
findings from the Round 2 documentation audit (2026-05-11). It fixes three P0
blockers, completes two reference documents, and adds three new conceptual
guides that bring the documentation to world-class standard before v1.0.

**P0 fixes (breaking inaccuracies):**
- Fixed `scripts/gen_catalogs.py`: GUC names now correctly resolve to
  `pg_trickle.*` names instead of `(registration pending — PGS_*)`. Rust types
  are converted to PostgreSQL type names (`int4`, `float8`, `text`). Stale
  garbage rows at the end of `GUC_CATALOG.md` are eliminated. The catalog now
  shows all 115 GUCs with correct names and types.
- Fixed `docs/CONFIGURATION.md`: `pg_trickle.parallel_refresh_mode` now
  correctly documents its default as `'on'` (changed from the stale `'off'`
  which was the pre-v0.11.0 default).
- Completed `docs/ERRORS.md`: Added documentation for 18 previously missing
  error variants across 6 new categories (Publication, SLA, CDC, Diagnostic,
  Snapshot, Outbox/pg_tide, Placeholder, DVM engine). All 39 `PgTrickleError`
  variants are now documented with SQLSTATE codes, descriptions, causes, and
  fixes.

**Reference completeness:**
- `docs/SQL_REFERENCE.md`: Added working code examples for all 10 outbox/inbox
  consumer API functions (`poll_outbox`, `commit_offset`, `extend_lease`,
  `seek_offset`, `consumer_heartbeat`, `consumer_lag`, `drop_consumer_group`,
  `outbox_rows_consumed`, `replay_inbox_messages`, `inbox_ordering_gaps`).
- `docs/SQL_REFERENCE.md`: Added full column-schema tables for all 7 previously
  undocumented catalog tables (`pgt_outbox_config`, `pgt_consumer_groups`,
  `pgt_consumer_offsets`, `pgt_consumer_leases`, `pgt_inbox_config`,
  `pgt_inbox_ordering_config`, `pgt_inbox_priority_config`).
- `docs/research/`: Added standalone 3-paragraph abstracts to the three
  previously stub-only research documents (`CUSTOM_SQL_SYNTAX.md`,
  `PG_IVM_COMPARISON.md`, `TRIGGERS_VS_REPLICATION.md`).
- `docs/DVM_REWRITE_RULES.md`: Added concrete before/after SQL examples for all
  5 rewrite passes (view inlining, grouping sets expansion, EXISTS→anti/semi-join,
  scalar sublink hoisting, delta key restriction).
- `docs/introduction.md`: Added 3 paragraphs explaining how pg_trickle works
  conceptually (CDC → delta SQL → MERGE cycle), plus a link to INSTALL.md.

**New documents:**
- [`docs/MENTAL_MODEL.md`](docs/MENTAL_MODEL.md): 8-section conceptual guide
  for developers who know SQL but not IVM. Covers the problem of full
  recomputation, delta semantics, change capture, delta SQL generation, algebraic
  operator classification, row identity, the refresh cycle, and DAG chaining.
- [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md): Comprehensive reference of
  unsupported SQL constructs, DIFFERENTIAL mode constraints, source table
  restrictions, operational anti-patterns, and a "Will this work?" decision tree.
- [`docs/PERFORMANCE_CHEATSHEET.md`](docs/PERFORMANCE_CHEATSHEET.md): Single-page
  quick reference with the three golden rules, top-10 GUC quick wins, 5
  FULL-fallback patterns with rewrites, and refresh latency diagnostics.

### Upgrade Notes

No SQL migration is required. Run `ALTER EXTENSION pg_trickle UPDATE TO '0.56.0'`
or reinstall from packages. All changes are documentation and tooling only.

After upgrading, regenerate `docs/GUC_CATALOG.md` with:
```bash
python3 scripts/gen_catalogs.py
```

---

## [0.55.0] — Final Pre-1.0 Polish

### What's New

v0.55.0 is a focused polish release that lowers technical debt and improves
observability ahead of the 1.0 stable label. All nine milestones deliver
better diagnostics, cleaner code structure, and more operator-friendly
documentation — without any SQL schema changes.

### Changes

- **M-1 — Wider invalidation ring** (`shmem.rs`, `config.rs`): Maximum ring
  capacity raised from 1 024 to 4 096; the GUC default is now 1 024 so
  deployments with many concurrent stream tables no longer drop events.

- **M-2 — API module decomposition** (`src/api/`): `api/mod.rs` split into
  `create.rs`, `alter.rs`, and `refresh_ops.rs`. Each sub-module is now
  independently readable and testable.

- **M-3 — Monitor module decomposition** (`src/monitor/`): `monitor.rs` split
  into `alert.rs`, `health.rs`, and `tree.rs`. Alert emission, health checks,
  and DAG tree rendering are now in separate, focused units.

- **M-4 — Structured NOTIFY payloads**: All `pg_notify` calls now emit
  structured `serde_json` values instead of hand-built strings, making it
  easier to parse alert events in downstream consumers.

- **M-5 — Multi-column `IN` rewrite** (`src/dvm/parser/sublinks.rs`): Row
  expressions and multi-target sub-selects in `IN` / `NOT IN` predicates are
  now automatically rewritten to AND-chained equality rather than returning an
  unsupported-syntax error.

- **M-6 — DVM parse metrics** (`src/shmem.rs`, `src/dvm/mod.rs`): Two new
  shared-memory counters track cumulative DVM parse time
  (`pg_trickle_dvm_parse_ms`) and total delta SQL template size
  (`pg_trickle_delta_query_size_bytes`). Both are exposed via the Prometheus
  `/metrics` endpoint.

- **M-7 — Reserved column-name prefix docs** (`docs/SQL_REFERENCE.md`): New
  "Reserved Column-Name Prefixes" section documents `__pgt_*` and `__pgs_*`
  internal prefixes and explains the consequences of naming conflicts.

- **M-8 — GUC rationale comments** (`src/config.rs`): Every magic-number GUC
  default now has an inline comment explaining why that value was chosen and
  when operators should raise or lower it.

- **M-9 — Codecov upload in PR gate** (`.github/workflows/ci.yml`): The
  Linux unit-test job now uploads coverage data to Codecov after each run.
  `fail_ci_if_error: false` ensures that a Codecov outage never blocks merges.

### Upgrade

No SQL migration is required. Run `ALTER EXTENSION pg_trickle UPDATE TO '0.55.0'`
or reinstall to pick up the new extension version string.

---

## [0.54.0] — DVM Engine Hardening

### What's New

v0.54.0 hardens the DVM (Differential View Maintenance) engine across seven
dimensions: depth-limit enforcement, CTE-count cap, snapshot fingerprint
caching, expression visitor pattern, view-inlining relkind cache, upstream
frontier validation, and O(V+E) diamond detection.  Every change is targeted
at correctness and performance; no user-visible API surface changes.

### Changed

#### C-7: diff_node() Recursion Depth Guard

`diff_node()` in `src/dvm/diff.rs` now enforces a hard depth limit drawn from
the `pg_trickle.max_parse_depth` GUC (default 64).  Exceeding the limit returns
a new `PgTrickleError::DiffDepthExceeded(limit)` error with a user-actionable
hint instead of overflowing the call stack.

#### R-7: DiffContext CTE Count Cap (OOM Guard)

`DiffContext` tracks the number of CTEs emitted during a single differentiation
pass.  When the count reaches the new `pg_trickle.max_diff_ctes` GUC (default
1000, range 10–100 000), `diff_node()` returns
`PgTrickleError::DiffCteCountExceeded(limit)` before allocating further
memory.  This prevents pathological queries from exhausting server memory.

#### P-4: Snapshot Fingerprint Two-Level Cache

`get_or_register_snapshot_cte()` now uses a two-level cache: a fast pointer
identity check (same `OpTree` node, O(1)) and a structural fingerprint check
(equal subtrees, O(k)).  Identical subtrees share a single CTE, eliminating
redundant snapshot SQL generation for diamond-shaped query plans.

#### P-5: Expr::to_sql() Visitor Pattern

`Expr::to_sql()` now delegates to a new `to_sql_into(&self, buf: &mut String)`
method that writes SQL directly into a pre-allocated buffer using `push`/
`push_str`.  Intermediate heap allocations for nested expressions are
eliminated, reducing allocation pressure on large queries.

#### P-6: View-Inlining Relkind Cache

`rewrite_views_inline_once()` now passes a mutable `HashMap<(schema,name),
Option<relkind>>` through the call chain.  Each relkind lookup is cached for
the duration of the rewrite pass, preventing repeated SPI catalog queries for
the same relation within a single inlining iteration.

#### C-4: Upstream Stream-Table Frontier Validation

`generate_delta_query()` now validates that every upstream stream-table source
referenced in a query has a corresponding entry in the provided refresh
frontier.  Missing entries return `PgTrickleError::StSourceFrontierMissing`
with a clear message and the affected `pgt_id`, allowing the scheduler to
reinitialize rather than silently producing incorrect delta results.

#### S-1: O(V+E) Diamond Detection

`detect_diamonds()` in `src/dag.rs` previously called `collect_ancestors()`
per fan-in branch (O(V) per branch, O(V²) total for dense graphs).  It now
calls the new `compute_all_ancestors()` which traverses the DAG once in forward
topological order, building all ancestor sets in O(V+E) total work.  Per-branch
ancestor lookup is then O(1) via the precomputed map.

---

## [0.53.0] — Unit Test Depth Sweep

### What's New

v0.53.0 fills the unit-test coverage gaps identified in the v0.51.0 overall
assessment (Report 11, findings T-2 through T-9). Six scheduler and parser
submodules that previously had zero inline `#[test]` coverage now each have a
`#[cfg(test)]` block covering their pure logic. Property-based testing is
extended to the DAG cycle detection and topological sort invariants. Two fixed
sleeps in the buffer-growth E2E tests are replaced with adaptive polling.

### Changed

#### Scheduler Module Unit Tests

Five scheduler submodules previously had zero inline unit tests. New
`#[cfg(test)]` blocks have been added to:

- **`dispatch.rs`** — `parse_worker_extra` (format validation, edge cases,
  rejected zero/negative job IDs) and `compute_adaptive_poll_ms` (exponential
  backoff, completion reset, no-inflight fast path).
- **`pool.rs`** — `pool_size_from_config_value` (negative GUC values clamped
  to zero, positive values preserved).
- **`watermark.rs`** — `should_emit_holdback_warning` pure rate-limit helper:
  disabled threshold, age threshold, 60-second cooldown, saturating subtraction
  on clock skew.
- **`citus.rs`** — `record_worker_failure` / `reset_worker_failure` thread-local
  failure counter: increment, per-key isolation, reset-to-zero, no-op on missing
  key.
- **`scheduler_loop.rs`** — Structural compile-check test (module contains only
  BGW entry points; E2E coverage in `tests/e2e_bgworker_tests.rs`).

#### DVM Parser Unit Tests

`dvm/parser/sublinks.rs` had zero inline unit tests. New tests cover:

- `extract_bare_scalar_subquery_sql` — parenthesised SELECT, missing parens,
  whitespace trimming, case-insensitive SELECT detection.
- `is_known_aggregate` — known built-ins, statistical, ordered-set, and range
  aggregates; unknown function names.
- `is_star_only` — bare `*`, qualified `t.*`, empty slice, multi-expression.
- `rewrite_having_expr` — COUNT(*) and SUM rewrites, non-matching functions,
  recursive rewrite inside BinaryOp, literal pass-through.
- `split_exists_correlation` — simple equality extraction, non-correlation
  remaining predicates, AND conjunction splitting.
- `collect_tree_source_aliases` — single Scan, InnerJoin, Filter, Subquery.

#### Proptest Extension (T-2)

Two new `proptest!` blocks in `src/dag.rs`:

- **Acyclic invariant** — randomly generated chain DAGs of length 1–20 always
  pass `detect_cycles()`.
- **Cyclic invariant** — adding a single back-edge to any chain of length 2–20
  is always detected as a cycle by `detect_cycles()`.
- **Topological order invariant** — for any acyclic chain, `topological_order()`
  places every upstream node before its downstream successor.
- **Back-edge invariant** — any single back-edge added to an acyclic DAG creates
  a cycle (parameterised over both chain length and back-edge position).

#### Buffer-Growth Sleep Removal (T-8, T-9)

`tests/e2e_buffer_growth_tests.rs` contained two long fixed sleeps in the
sustained-write test:

- **7-second sleep** replaced with `db.wait_for_auto_refresh("sustained_st", 30s)`.
- **20-second sleep** replaced with `db.wait_for_condition(...)` polling until
  the stream table count matches the source count, with a 60-second cap.

---

## [0.52.0] — DVM Hot-Path Performance

### What's New

v0.52.0 eliminates four measurable hot-path costs in the DVM differential
refresh pipeline, all identified in the v0.51.0 overall assessment (Report 11).

#### P-1: O(1) Placeholder Resolution (aho-corasick)

`resolve_delta_template()` previously called `.replace()` twice per source
table OID, scanning the full SQL string for each placeholder. For a 10-table
join (~50 KB SQL), this was 20 full-string scans per refresh cycle. v0.52.0
replaces the loop with a single-pass [Aho-Corasick](https://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_algorithm)
multi-pattern replacer that resolves all `__PGS_PREV_LSN_*__` and
`__PGS_NEW_LSN_*__` tokens in one traversal — O(template_length) regardless
of the number of source tables.

#### P-2: Thread-Local Volatility Cache

`lookup_function_volatility()` and `lookup_operator_volatility()` previously
issued one SPI round-trip to `pg_proc` / `pg_operator` for every function or
operator name encountered during DVM parsing. A query referencing 50 functions
triggered 50 round-trips (~50 ms overhead). v0.52.0 adds thread-local
`HashMap<String, char>` caches so each name is resolved via SPI at most once
per backend session. The caches are flushed by `pgtrickle.clear_caches()`.

#### P-3: Lazy DiffContext Allocations

`DiffContext::new()` previously initialized all maps unconditionally.
`agg_sum_coalesce_defaults` — only needed for queries with COALESCE-wrapped
aggregates — is now `Option<HashMap<String, String>>` and allocated lazily on
first use. Simple scan/filter/project queries never allocate it.

#### P-8: O(1) MERGE Template Cache LRU Eviction

The MERGE template cache previously stored entries in a plain `HashMap` and
found the least-recently-used entry by scanning all entries for the minimum
`last_used` counter — O(N) per eviction. v0.52.0 replaces this with
`lru::LruCache`, which provides O(1) eviction automatically on `put()`.

#### C-1: Safety Fix in filter.rs HAVING Path

Replaced a bare `.expect("BUG: …")` in the HAVING-filter delta path with a
proper `PgTrickleError::InternalError` return. An invariant violation now
returns a clean error rather than crashing the backend.

### Upgrade Notes

No SQL schema changes. No configuration changes required.

---

## [0.51.0] — Citus Chaos Resilience & Documentation Truth

### Breaking Changes

- **`pg_trickle.event_driven_wake` has been removed.** This GUC had no effect
  since v0.39.0 because PostgreSQL's `LISTEN` command is not permitted inside
  background worker processes. Remove it from `postgresql.conf` and any
  `ALTER SYSTEM` settings to avoid an "unrecognized configuration parameter"
  warning on upgrade. No behavioral change — the scheduler always used
  efficient latch-based polling regardless of this setting.

- **`pg_trickle.wake_debounce_ms` has been removed.** This GUC was only
  meaningful when `event_driven_wake` was functional (it never was). Remove it
  from `postgresql.conf` as well.

### What's New

#### FEAT-10-01: Citus Chaos Test Rig

Three new chaos resilience scenarios for the Citus distributed integration,
proving correctness under real production failure modes:

- **CHAOS-5 — Coordinator restart during active refresh**: Creates a distributed
  stream table, starts a refresh, restarts the coordinator mid-flight, and
  verifies that 5 subsequent cycles produce the correct result with no phantom
  or missing rows.

- **CHAOS-6 — Worker kill with shard redistribution**: Kills a worker node,
  triggers `rebalance_table_shards()`, inserts new rows on the remaining workers,
  and verifies that DIFFERENTIAL refresh produces a consistent result post-recovery.
  Asserts that CDC change buffers contain no orphaned records.

- **CHAOS-7 — Network partition and recovery**: Uses `docker network disconnect`
  to isolate one worker, inserts rows on the remaining workers, reconnects the
  isolated worker, and verifies that the stream table converges to the correct
  state within 3 refresh cycles with no data loss.

All three tests are marked `#[ignore]` and run nightly in the `stability-tests.yml`
workflow alongside the existing G17-SOAK and G17-MDB tests. Use
`just citus-chaos-up && just test-citus-chaos` to run them locally.

#### CQ-10-02: Remove Deprecated event_driven_wake GUC

Removed the non-functional `event_driven_wake` and `wake_debounce_ms` GUCs
and all associated dead code paths from the scheduler loop. The code that
emitted a WARNING when `event_driven_wake = on` is gone. The scheduler log
message at startup no longer includes the GUC value.

#### DOC-10-01: ARCHITECTURE.md — pg_tide Integration Boundary

Added a new **§ pg_tide Integration** section to `docs/ARCHITECTURE.md` that
clearly describes the v0.46.0 extraction boundary: what remains in pg_trickle
(`attach_outbox()` hook, change buffer subscription interface) vs what lives in
the standalone `pg_tide` extension (outbox, inbox, consumer groups, relay binary).
Updated the module layout diagram to reflect the extraction.

#### DOC-10-03: ARCHITECTURE.md — Recursive CTE Strategy Selection

Added a new **§ Recursive CTE Strategy Selection** subsection to the DVM Engine
section documenting the five-tier strategy selection logic (Tier 1 inline
expansion → Tier 2 shared delta → Tier 3a semi-naive → Tier 3b DRed → Tier 3c
recomputation), a selection criteria table, observability via
`explain_stream_table()`, and a concrete Tier 3a example for hierarchical
closure queries.

#### DOC-10-02 + COR-10-02: Configuration Documentation Truth

- **CONFIGURATION.md**: `event_driven_wake` and `wake_debounce_ms` sections
  replaced with clear removal notices. All tuning profiles, interaction matrix
  entries, and example configs updated to remove these GUCs.
- **CONFIGURATION.md**: Added deprecation `⚠️` callouts for
  `merge_planner_hints` (accepted, no effect) and `user_triggers = 'on'`
  (deprecated alias for `'auto'`).
- **CONFIGURATION.md**: Added a **Note on CDC triggers** to the
  `pg_trickle.enabled` section explaining that CDC triggers continue to fire
  when the scheduler is disabled, why this is intentional, and how to fully
  quiesce CDC overhead during extended maintenance.

---

## [0.50.0] — Performance, Security & Operational Hardening

### What's New

#### PERF-10-01: Batch preflight source-table existence check
- Replaced the N-query per-OID loop in `execute_differential_refresh` with a
  single batch `SELECT ... FROM unnest(ARRAY[oid1, oid2, ...])` that returns
  all source-table existence checks in one SPI round-trip.
- Reduces preflight overhead from O(N) queries to O(1) for stream tables with
  multiple sources.

#### PERF-10-02: CDC trigger SQL string-building micro-optimisation
- `build_stmt_trigger_fn_sql` now uses `String::with_capacity` + direct
  `push_str` loops instead of `Vec<String>::join`, eliminating intermediate
  allocations in the column list builders (`cn`, `ncr`, `ocr`).
- Noticeable on high-throughput workloads that re-register triggers frequently.

#### PERF-10-03: Single-query watermark computation (already present; documented)
- Confirmed that `compute_safe_upper_bound()` in `src/cdc.rs` already
  consolidates `pg_current_wal_lsn()`, `pg_stat_activity` xmin probe, and
  `pg_prepared_xacts` into one compound CTE `SELECT`. Added an explanatory
  comment referencing PERF-10-03.

#### SEC-10-01: Replace manual SQL string escaping with `pg_catalog.quote_literal`
- All `dblink(...)` call sites in `src/citus.rs` now escape connection strings
  and remote query strings via a new `pg_quote_literal()` helper that delegates
  to PostgreSQL's built-in `pg_catalog.quote_literal($1)` function.
- Eliminates the risk of SQL injection through attacker-controlled hostnames or
  slot names in Citus distributed setups.
- The manual `.replace('\'', "''")` pattern has been removed from
  `worker_conn_string()` and all four `dblink` call sites.

#### OPS-10-01: Kubernetes rolling-upgrade drain hook (CNPG)
- Added `lifecycle.preStop` hook to `cnpg/cluster-production.yaml` that runs
  `pgtrickle.drain(timeout_s => 120)` before CloudNativePG shuts down a
  primary pod during rolling upgrades.
- New `docs/RUNBOOK_DRAIN.md` section documents the Kubernetes rolling-upgrade
  procedure and post-upgrade verification steps.

#### OPS-10-02: Prometheus reliability counters
- Three new shared-memory atomics in `src/shmem.rs`:
  - `TEMPLATE_CACHE_STALE_EVICTIONS` — incremented when a delta template cache
    entry is evicted because its `defining_query_hash` no longer matches.
  - `DAG_CYCLES_DETECTED` — incremented each time `detect_cycles()` returns
    `Err(CycleDetected)`.
- `src/dvm/mod.rs`: Hash-mismatch stale entries are now detected and counted
  before being evicted from `DELTA_TEMPLATE_CACHE`.
- New `pgtrickle.reliability_counters()` SQL function (in `src/monitor.rs`)
  exposes all three reliability counters as a single-row table.
- New `pg_trickle_reliability` query block in
  `monitoring/prometheus/pg_trickle_queries.yml` for postgres_exporter.

#### OPS-10-03: Docker base-image digest pinning
- All three Dockerfiles (`Dockerfile.demo`, `Dockerfile.ghcr`,
  `tests/Dockerfile.e2e`) now pin `postgres:18.3-bookworm` to an exact
  SHA256 digest, providing supply-chain security and reproducible builds.
- New `scripts/update_base_image_digests.sh` automates quarterly digest
  refreshes.
- `CONTRIBUTING.md` documents the update process.

#### SCAL-10-01: Invalidation ring capacity documentation
- New `docs/CONFIGURATION.md` section documents `pg_trickle.invalidation_ring_capacity`
  (default 128, hard ceiling 1024), overflow behaviour, the overflow counter,
  and capacity guidance for deployments with 1,000+ stream tables.

#### COR-10-01: Deep join chain threshold documentation
- New `docs/CONFIGURATION.md` section documents `pg_trickle.part3_max_scan_count`
  (default 5), the Part 3 threshold trade-off between SQL complexity and delta
  correctness at depth, and recommendations for ≤6 vs. >6 table join chains.

### SQL Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.50.0';
```

---

## [0.49.1] — Repository Migration to trickle-labs/pg-trickle

### What's New

#### Repository Migration
- pg_trickle has moved to its permanent home at **[trickle-labs/pg-trickle](https://github.com/trickle-labs/pg-trickle)**.
- All CI/CD pipelines, Docker image publishing, and release artifacts now originate from the new repository.
- GitHub Container Registry images are published under `ghcr.io/trickle-labs/pg-trickle`.
- Docker Hub images are published under `tricklehq/pg_trickle`.
- The PGXN distribution, dbt Hub package, and CloudNativePG plugin listings are updated to reflect the new repository URL.
- No code changes — this is a pure packaging and infrastructure release.

### SQL Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.49.1';
```

---

## [0.49.0] — Test Infrastructure Hardening & Scheduler Decomposition

### What's New

#### TEST-10-01: Concurrency Test Synchronization Overhaul
- Replaced all `tokio::time::sleep` busy-waits in `tests/e2e_concurrent_tests.rs`
  with `pg_stat_activity`-polling loops that wait until the target query is
  actually visible before proceeding.
- Added `wait_for_active_query` helper with configurable timeout and a clear
  failure message so flakiness surfaces as a named error rather than a silent pass.
- Affected tests: `test_pb1_concurrent_refresh_skip_locked_no_corruption`,
  `test_concurrent_refresh_and_drop`, `test_conc1_alter_while_refresh`,
  `test_conc2_drop_while_refresh`.

#### TEST-10-02: Unit Test Coverage Sweep
- Added `#[cfg(test)]` modules to `src/template_cache.rs`, `src/cdc/polling.rs`,
  and `src/cdc/rebuild.rs` — modules that previously had zero unit test coverage.
- New tests cover hash key derivation and round-trip correctness, CDC trigger
  naming conventions, CDC mode classification, replica identity sufficiency checks,
  and cache guard condition logic.

#### TEST-10-03: Fuzz Targets for Merge Codegen and Row Identity
- Added `fuzz/fuzz_targets/merge_sql_fuzz.rs` — fuzzes the merge SQL construction
  pipeline (`pg_quote_literal`, `parse_hash_bound_spec`, `extract_keyword_int`,
  amplification ratio, `build_content_hash_expr`). Validates no panics, UTF-8
  output, and deterministic results.
- Added `fuzz/fuzz_targets/row_id_fuzz.rs` — fuzzes the row identity schema
  classifier (`is_compatible_with`, `verify_pipeline`). Validates reflexivity
  and that no byte sequence causes a panic.
- Both targets registered in `fuzz/Cargo.toml` and the `just fuzz-all` recipe.

#### TEST-10-04: DDL During Concurrent Refresh E2E Test
- Added `test_ddl_during_concurrent_refresh` to `tests/e2e_concurrent_tests.rs`.
  Fires `ALTER STREAM TABLE` concurrently with a running refresh and asserts
  either graceful completion or correct blocking — no torn state.

#### CI-10-02: Expanded e2e-Smoke Filter
- The PR smoke test now also matches `test_.*join.*`, `test_.*aggregate.*`,
  `test_.*window.*`, and `test_.*subquery.*` patterns, catching operator-level
  regressions earlier.

#### CI-10-03: Consolidated Fuzz Recipe
- Added `just fuzz-all` to the `justfile` — runs every fuzz target for a
  configurable duration (default 60 s each).
- Documented all fuzz targets and corpus paths in `CONTRIBUTING.md`.

#### CQ-10-01: Scheduler Module Decomposition
- `src/scheduler/mod.rs` was 6,700+ lines. Extracted into three focused
  submodules:
  - `src/scheduler/dispatch.rs` — parallel dispatch state, dynamic worker spawn,
    worker claiming, orphan reaping, adaptive poll-interval logic.
  - `src/scheduler/scheduler_loop.rs` — BGW registration, launcher main loop,
    per-database scheduler main loop.
  - `src/scheduler/watermark.rs` — tick watermark computation, xmin holdback,
    frontier advance helpers.
- `mod.rs` is now a thin re-export façade. All existing public API is preserved
  with no behaviour change.

### SQL Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.49.0';
```

---

## [0.48.0] — Complete Embedding Programme: Hybrid Search, Sparse Vectors & Ergonomic API

### What's New

#### VH-1: Sparse and Half-Precision Vector Aggregates
- `avg(halfvec_col)` and `avg(sparsevec_col)` stream tables now produce output
  columns typed `halfvec(N)` and `sparsevec(N)` respectively — no silent coercion
  to `vector` anymore.
- The DVM engine correctly propagates vector type names through `extract_vector_agg_output_dims`.

#### VH-2: Reactive Distance Subscriptions
- New functions: `pgtrickle.subscribe_distance(stream_table, channel, vector_column, query_vector, op, threshold)`,
  `pgtrickle.unsubscribe_distance(stream_table, channel)`, and
  `pgtrickle.list_distance_subscriptions(stream_table)`.
- After each refresh, the scheduler fires NOTIFY on registered channels when
  rows in the storage table satisfy the distance predicate.

#### VH-3: Hybrid-Search Cookbook
- New doc: [docs/tutorials/HYBRID_SEARCH_PATTERNS.md](docs/tutorials/HYBRID_SEARCH_PATTERNS.md) —
  three hybrid search patterns with worked SQL examples.

#### VH-4: Vector Benchmark Suite
- New benchmark: `benches/pgvector_bench.rs` — measures OpTree construction,
  AggFunc dispatch, vector string encoding, and drift-detection overhead.

#### VA-1: `embedding_stream_table()` Ergonomic API
- New function: `pgtrickle.embedding_stream_table(name, source_table, vector_column, extra_columns, refresh_interval, index_type, dry_run)`.
- Automatically generates a stream table, creates an HNSW or IVFFlat index,
  and configures post-refresh drift monitoring.
- `dry_run => true` returns the generated SQL without executing it.

#### VA-2: Materialised k-NN Graph Research
- New doc: [docs/research/KNN_GRAPH_TRADEOFFS.md](docs/research/KNN_GRAPH_TRADEOFFS.md) —
  storage/latency/maintenance analysis for materialised k-NN graphs.

#### VA-3: Multi-Tenant ANN Patterns
- New doc: [docs/tutorials/PER_TENANT_ANN_PATTERNS.md](docs/tutorials/PER_TENANT_ANN_PATTERNS.md) —
  per-tenant ANN stream tables with RLS, tenant isolation, and security checklist.

#### VA-4: Embedding Outbox
- New function: `pgtrickle.attach_embedding_outbox(stream_table, vector_column, retention_hours, inline_threshold_rows)`.
- Extends outbox events with `event_type: "embedding_change"` and the
  `vector_column` name in event headers.

#### VA-5: Vector RAG Starter Guide
- New doc: [docs/tutorials/VECTOR_RAG_STARTER.md](docs/tutorials/VECTOR_RAG_STARTER.md) —
  quick-start guide for building a RAG pipeline with pg_trickle and pgvector.

### SQL Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.48.0';
```

Direct upgrade scripts are provided from **v0.40.0** onward.

---

## [0.47.0] — Embedding Pipeline Infrastructure & ANN Maintenance

> **⚠ Upgrade support policy change (v0.47.0+)**
>
> Starting from v0.47.0, pg_trickle provides direct upgrade scripts only for
> **v0.40.0 and later**. If you are running v0.39.0 or older, you must first
> upgrade to v0.40.0 before upgrading to v0.47.0 or later:
>
> ```sql
> -- Users on v0.39.x or older: upgrade to v0.40.0 first
> ALTER EXTENSION pg_trickle UPDATE TO '0.40.0';
> -- Then upgrade to the latest version
> ALTER EXTENSION pg_trickle UPDATE;
> ```
>
> Both steps can be issued in the same session. PostgreSQL handles the
> intermediate chain automatically. Users already on v0.40.0 or later are
> unaffected — a single `ALTER EXTENSION pg_trickle UPDATE` is all that is
> needed.

v0.47.0 resumes the deferred embedding programme with post-refresh action
hooks, drift-based HNSW reindex scheduling, vector-aware monitoring, and the
pgvector RAG cookbook.

### Post-Refresh Actions (VP-1)

Stream tables can now specify what happens after a successful refresh that
produces changed rows:

```sql
-- Run ANALYZE after each refresh (keep statistics fresh)
SELECT pgtrickle.alter_stream_table(
    'embedding_store',
    post_refresh_action => 'analyze'
);

-- Always REINDEX the storage table after each refresh
SELECT pgtrickle.alter_stream_table(
    'embedding_store',
    post_refresh_action => 'reindex'
);

-- REINDEX only when the drift threshold is exceeded
SELECT pgtrickle.alter_stream_table(
    'embedding_store',
    post_refresh_action     => 'reindex_if_drift',
    reindex_drift_threshold => 0.20   -- 20% of rows changed
);
```

The action runs **outside** the refresh transaction so it does not add latency
to the critical refresh window. The four supported values are `none` (default),
`analyze`, `reindex`, and `reindex_if_drift`.

### Drift Detection (VP-2)

Two new catalog columns track ANN index freshness:

- `rows_changed_since_last_reindex` — running count of rows changed since the
  last REINDEX, reset to 0 after each successful REINDEX.
- `last_reindex_at` — timestamp of the last pg_trickle-triggered REINDEX.

A new GUC `pg_trickle.reindex_drift_threshold` (default 0.20) sets the global
default fraction; per-table overrides via `reindex_drift_threshold` take
precedence.

### Vector Status View (VP-3)

```sql
SELECT * FROM pgtrickle.vector_status();
```

Returns one row per stream table with a non-`none` `post_refresh_action`:

| Column | Description |
|--------|-------------|
| `name` | Schema-qualified stream table name |
| `post_refresh_action` | Configured action |
| `reindex_drift_threshold` | Per-table threshold (NULL = global GUC) |
| `rows_changed_since_last_reindex` | Rows changed since last REINDEX |
| `last_reindex_at` | When the last REINDEX completed |
| `data_timestamp` | When the stream table data was last updated |
| `embedding_lag` | Interval since last refresh |
| `estimated_rows` | PostgreSQL reltuples estimate |
| `drift_pct` | Percentage of rows changed (NULL if no estimate available) |

### pgvector RAG Cookbook (VP-4)

`docs/tutorials/PGVECTOR_RAG_COOKBOOK.md` — copy-paste patterns for:
- Pre-computed embeddings with always-fresh search corpus
- Tenant-isolated embedding corpus with RLS
- Drift-aware HNSW reindexing
- Centroid maintenance for cluster-aware search
- Operational sizing guidance and monitoring queries

### New SQL Functions

- `pgtrickle.vector_status()` — embedding lag, ANN age, drift percentage

### New Catalog Columns

`pgtrickle.pgt_stream_tables`:
- `post_refresh_action TEXT NOT NULL DEFAULT 'none'`
- `reindex_drift_threshold DOUBLE PRECISION`
- `rows_changed_since_last_reindex BIGINT NOT NULL DEFAULT 0`
- `last_reindex_at TIMESTAMPTZ`

### New GUCs

- `pg_trickle.reindex_drift_threshold` (default: `0.20`) — global default
  drift fraction for drift-triggered REINDEX

### Upgrade Notes

Existing stream tables keep `post_refresh_action = 'none'` after upgrade —
no behaviour change unless explicitly configured.

---

## [0.46.0] — Extract `pg_tide`: Standalone Outbox, Inbox & Relay

v0.46.0 is a focused extraction release. The full transactional outbox, inbox,
and relay subsystem (~6,150 Rust LOC + ~2,500 SQL LOC) has been moved to the
new standalone `pg_tide` extension (`trickle-labs/pg-tide`). `pg_trickle` now
ships exactly one thing: incremental view maintenance.

The only remaining integration point is `attach_outbox()`, which registers a
`pg_tide` outbox for a stream table. After attachment, every non-empty refresh
calls `tide.outbox_publish()` inside the same transaction — preserving the
ADR-001/ADR-002 single-transaction atomicity guarantee.

### New SQL Functions

- **TIDE-7**: `pgtrickle.attach_outbox(stream_table, retention_hours=>24, inline_threshold_rows=>10000)` —
  requires `pg_tide` to be installed; calls `tide.outbox_create()` and
  registers the mapping in `pgtrickle.pgt_outbox_config`. Every subsequent
  non-empty refresh writes a delta-summary row to the `pg_tide` outbox inside
  the same transaction.

- **TIDE-7**: `pgtrickle.detach_outbox(stream_table, if_exists=>false)` —
  removes the `pgt_outbox_config` entry. The `pg_tide` outbox table itself is
  NOT dropped; use `tide.outbox_drop()` in `pg_tide` after detaching to also
  remove the outbox data.

### Removed SQL Functions

The following functions were moved to `pg_tide` (`trickle-labs/pg-tide`):

**Outbox & Consumer Groups:**
`enable_outbox`, `disable_outbox`, `outbox_status`, `outbox_rows_consumed`,
`create_consumer_group`, `drop_consumer_group`, `poll_outbox`, `commit_offset`,
`extend_lease`, `seek_offset`, `consumer_heartbeat`, `consumer_lag`

**Inbox:**
`create_inbox`, `drop_inbox`, `enable_inbox_tracking`, `inbox_health`,
`inbox_status`, `replay_inbox_messages`, `enable_inbox_ordering`,
`disable_inbox_ordering`, `enable_inbox_priority`, `disable_inbox_priority`,
`inbox_ordering_gaps`, `inbox_is_my_partition`

**Relay:**
`set_relay_outbox`, `set_relay_inbox`, `enable_relay`, `disable_relay`,
`delete_relay`, `get_relay_config`, `list_relay_configs`

### Removed Catalog Tables

Dropped as part of the extraction: `relay_outbox_config`, `relay_inbox_config`,
`relay_consumer_offsets`, `pgt_inbox_config`, `pgt_inbox_ordering_config`,
`pgt_inbox_priority_config`, `pgt_consumer_groups`, `pgt_consumer_offsets`,
`pgt_consumer_leases`. The `pgtrickle_relay` role is also dropped.
`pgtrickle.pgt_outbox_config` is replaced with a slim integration schema.

### GUC Changes

The following GUCs are removed (all moved to `pg_tide`):
`pg_trickle.outbox_enabled`, `pg_trickle.outbox_retention_hours`,
`pg_trickle.outbox_drain_batch_size`, `pg_trickle.outbox_inline_threshold_rows`,
`pg_trickle.outbox_drain_interval_seconds`, `pg_trickle.outbox_storage_critical_mb`,
`pg_trickle.outbox_skip_empty_delta`, `pg_trickle.outbox_force_retention`,
`pg_trickle.inbox_enabled`, `pg_trickle.inbox_processed_retention_hours`,
`pg_trickle.inbox_dlq_retention_hours`, `pg_trickle.inbox_drain_batch_size`,
`pg_trickle.inbox_drain_interval_seconds`, `pg_trickle.inbox_dlq_alert_max_per_refresh`,
`pg_trickle.consumer_dead_threshold_hours`

### Upgrade Notes

Run `pg_trickle--0.45.0--0.46.0.sql` to drop all removed objects and migrate
`pgt_outbox_config` to the new schema. Base outbox payload tables
(`pgtrickle.outbox_<st>`) are **not** dropped — they remain for manual data
migration to `pg_tide`. See the `pg_tide` repository for migration guidance.

### New: `pg_tide` Extension

The extracted functionality is now available as `pg_tide`, a standalone
PostgreSQL extension at `https://github.com/trickle-labs/pg-tide`. It includes:
- Transactional outbox with claim-check mode
- Idempotent inbox with DLQ, priority, and ordering
- The `pg-tide` relay binary (NATS, Kafka, SQS, webhooks, stdout)
- Consumer group API (poll, commit, heartbeat, lag)

---

## [0.45.0] — Operational Readiness, Scalability & CI Completeness

v0.45.0 is an operational and CI maturity release. It adds a first-class
`preflight()` health-check function, enhances the worker pool status view,
makes the invalidation ring capacity configurable, adds lag-aware scheduling,
introduces incremental DAG rebuild for faster event propagation, completes
dbt macro option parity, and substantially tightens CI coverage.

### New SQL Functions

- **A46-4**: `pgtrickle.preflight()` — returns a JSON health report with 7
  system checks: `shared_preload_libraries` presence, scheduler running,
  `max_worker_processes` sufficiency, `wal_level` for WAL-CDC, replication
  slots availability, invalidation ring overflow count, and Citus worker
  failure total. Run this after install or after configuration changes to
  verify the environment is ready.

### Enhanced SQL Functions

- **A46-5**: `pgtrickle.worker_pool_status()` gains four new columns:
  `idle_workers` (free slots), `last_scheduler_tick_unix` (Unix timestamp
  of last scheduler wake), `ring_overflow_count` (invalidation ring overflows
  since startup), and `citus_failure_total` (Citus worker failures logged).

### Configuration (GUCs)

- **A46-7**: New GUC `pg_trickle.invalidation_ring_capacity` (integer, default
  128, max 1024, postmaster scope). Configures the in-memory invalidation ring
  used for cross-backend event propagation. Requires a PostgreSQL restart when
  changed.
- **A46-10**: New GUC `pg_trickle.lag_aware_scheduling` (boolean, default
  false, superuser scope). When enabled, the per-database refresh quota is
  boosted proportionally to refresh lag (up to 2×), accelerating catch-up
  without starving other databases.

### Performance

- **A46-9**: Incremental DAG schedule re-resolution — when upstream CDC events
  affect a subset of stream tables, the scheduler now recomputes only the
  affected CALCULATED-schedule nodes (O(affected)) instead of the full DAG
  (O(V)). Falls back to full resolution if more than 25% of the DAG is
  affected. The new `resolve_calculated_schedule_incremental()` method is
  benchmarked in `benches/scheduler_bench.rs`.
- **A46-11**: Citus worker failure counter persisted in shared memory
  (`pg_trickle_citus_fail_total`), visible via `worker_pool_status()`. The
  counter increments when a Citus worker crosses the failure threshold,
  enabling operational dashboards to track distribution health over time.

### Observability & Deployment

- **A46-1/A46-2**: `Dockerfile.hub`, `Dockerfile.ghcr`, and `Dockerfile.demo`
  now carry the correct default `ARG VERSION=0.45.0` and a `HEALTHCHECK`
  directive (`pg_isready`) for Docker Compose and Kubernetes readiness probes.
- **A46-3**: `cnpg/cluster-dev.yaml` (single-instance) and
  `cnpg/cluster-production.yaml` (3-node HA) added as ready-to-use
  CloudNativePG cluster manifests, including the worker budget formula
  `max_worker_processes = 8 + (2 × num_databases) + worker_pool_size`.
- **A46-6**: `monitoring/production/README.md` documents least-privilege role
  setup, TLS Prometheus scrape config, Kubernetes ServiceMonitor, and
  recommended alert thresholds for production deployments.
- **A46-16**: `docs/STORAGE_BACKENDS.md` — reference page covering Heap,
  Unlogged, Citus columnar, and pg_mooncake backends with migration guidance.

### CI & Developer Experience

- **A46-13**: Windows compile failures are now **blocking** on scheduled CI
  runs (removed `continue-on-error: true`). A lightweight
  `windows-compile-gate` job also runs on every PR to catch Windows-specific
  compile errors early.
- **A46-14**: New `e2e-smoke` CI job runs on every PR and push to main. It
  builds the full E2E Docker image and runs a representative subset of tests
  (DVM, CDC, scheduler), catching packaging/install regressions faster than
  the full E2E run (schedule/manual only).
- **A46-15**: The Coverage workflow now runs on a weekly Monday schedule in
  addition to push-to-main and manual dispatch, providing consistent
  module-level coverage trend data.
- **A46-17**: dbt macros fully synced with `CreateStreamTableOptions` —
  `storage_backend`, `temporal`, `append_only`, `diamond_consistency`,
  `diamond_schedule_policy`, `pooler_compatibility_mode`,
  `max_differential_joins`, `max_delta_fraction`, and
  `output_distribution_column` are now configurable from dbt model configs and
  are correctly passed to the underlying SQL functions.

### Schema Changes

```sql
-- New function
pgtrickle.preflight() RETURNS text

-- worker_pool_status() return type extended (4 new columns):
--   idle_workers             integer
--   last_scheduler_tick_unix bigint
--   ring_overflow_count      bigint
--   citus_failure_total      bigint

-- New GUCs (set in postgresql.conf):
--   pg_trickle.invalidation_ring_capacity = 128  -- postmaster scope
--   pg_trickle.lag_aware_scheduling = false       -- superuser scope
```

> **Upgrade note:** `ALTER EXTENSION pg_trickle UPDATE` will DROP and
> re-create `worker_pool_status()` automatically (return type changed).
> The migration script `pg_trickle--0.44.0--0.45.0.sql` handles this.

---

## [0.44.0] — Security Hardening & Code Quality

v0.44.0 is a security and code-quality sprint. It hardens SECURITY DEFINER
paths, centralizes dynamic SQL construction, adds RLS bypass warnings,
decomposes large modules, consolidates API options, and strengthens the
parser's unsafe FFI façade.

### Security

- **A45-1**: IVM trigger function `SET search_path` hardened. BEFORE trigger
  functions (advisory lock only) now use a restricted path with no `public`,
  preventing search_path shadowing of extension internals. AFTER trigger
  functions retain `public` so that user delta SQL can resolve unqualified
  source-table references; their PLPGSQL bodies call only schema-qualified
  `pgtrickle.*` functions, so the security boundary is maintained.
- **A45-3**: A `WARNING` is now emitted when a stream table is created over a
  source table that has Row-Level Security (RLS) enabled, clarifying that
  source-table RLS does not protect stream-table contents.
- **A45-4**: Monitoring `docker-compose.yml` credentials are now driven by
  environment variables with a `monitoring/.env.example` template. PostgreSQL
  and Grafana services bind to `127.0.0.1` by default.
- **A45-5**: New `scripts/check_security_definer.sh` CI check validates that
  every `SECURITY DEFINER` occurrence in Rust and SQL files has a corresponding
  `SET search_path` and does not include `public` without justification. Added
  to `just lint` pipeline.
- **A45-6**: `docs/SECURITY_MODEL.md` now documents why `superuser = true` and
  `trusted = false` are required, with a privilege table and guidance for
  managed environments (RDS, AlloyDB, CNPG).

### Code Quality

- **A45-2**: New `src/sql_builder.rs` module provides safe helpers for all
  dynamic SQL construction: `ident`, `qualified`, `literal`, `regclass`,
  `spi_param`, `list_idents`. Includes unit tests and a new fuzz target
  (`FUZZ-6`).
- **A45-7**: `src/cdc.rs` split into three files — trigger-rebuild logic
  extracted to `src/cdc/rebuild.rs` and polling CDC extracted to
  `src/cdc/polling.rs`, reducing the main file from 4,259 to 3,386 lines.
- **A45-8**: `CreateStreamTableOptions` struct introduced in `src/api/mod.rs`
  to centralize all `create_stream_table` parameters. All four create paths
  (`create_stream_table`, `create_stream_table_if_not_exists`, `bulk_create`,
  `create_or_replace_stream_table`) now construct this struct before calling
  the implementation.
- **A45-9**: Extended the SAF-2 typed unsafe façade in `src/dvm/parser/mod.rs`
  with six additional safe wrapper functions (`safe_deparse_sort_clause`,
  `safe_deparse_target_list`, `safe_node_contains_window_func`,
  `safe_collect_all_window_func_nodes`, `safe_extract_func_name`,
  `safe_extract_operator_name`). Added `FUZZ-6` fuzz target for sql_builder
  and parser volatility helpers.
- **A45-10**: Scheduler background worker now emits structured `pgrx::warning!()`
  calls instead of silently discarding errors from `pg_backend_pid()`,
  `SchedulerJob::claim()`, and `pg_current_wal_lsn()` SPI calls.
- **A45-11**: All milestone-ID comments audited; each ID is now accompanied by
  a human-readable invariant description and links to a live design document
  in `plans/`.

---

## [0.43.0] — D+I Change-Buffer Schema, GUC Tuning & WAL Diagnostics

v0.43.0 delivers a fundamental change to how CDC change buffers are stored
(D+I schema: flat column names, UPDATE decomposed into a D-row + I-row at
write time), five new operator-tuning GUCs, a new `wal_source_status()`
diagnostic view for per-source WAL CDC state, extended `explain_stream_table()`
output, and a comprehensive microbenchmark suite for all new code paths.

### A44-1 — Deep-Join Threshold GUCs

Two new GUCs let operators tune when the DVM planner switches from the fast
L0-scan path to the full recursive join decomposition:

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.part3_max_scan_count` | `10000` | Maximum number of source rows before the planner escalates from P3 (direct scan) to a deeper join strategy. |
| `pg_trickle.deep_join_l0_scan_threshold` | `256` | Row count at which multi-level join decomposition uses an L0 pre-scan instead of a full plan. |

```sql
-- Lower threshold to force deep-join path for testing
SET pg_trickle.deep_join_l0_scan_threshold = 1;
```

### A44-2 — GROUP_RESCAN: Correct Incremental SUM(CASE …) Aggregates

The P5 aggregate differentiation path now produces correct incremental results
for non-invertible expressions such as `SUM(CASE WHEN status = 'active' THEN
amount ELSE 0 END)`.  The previous LATERAL VALUES decomposition has been
replaced with direct `c.action = 'I'` / `c.action = 'D'` filtering against
the D+I change buffer, eliminating the extra join overhead and fixing a
correctness gap for UPDATE rows that cross a CASE boundary.

### A44-3 — WAL Poll GUCs

Two new GUCs for tuning the WAL logical replication decoder polling loop:

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.wal_max_changes_per_poll` | `10000` | Maximum number of change messages to consume from a WAL slot in a single poll pass. |
| `pg_trickle.wal_max_lag_bytes` | `104857600` (100 MiB) | WAL slot lag threshold (bytes) above which the decoder pauses to avoid slot saturation. |

### A44-4 — Cost-Cache Capacity GUC

`pg_trickle.cost_cache_capacity` (default `4096`) controls the maximum number
of entries in the shared refresh-cost estimate cache. On deployments with
thousands of stream tables, increasing this value avoids cold-cache fallback to
full-plan estimation.

### A44-5 through A44-7 — Mandatory Microbenchmarks

Three new Criterion benchmark groups:

- **`bench_a44_5_pool_vs_spawn`** — measures EU-DAG pool reuse vs. per-tick
  rebuild at `n_sts ∈ {50, 200, 500, 1000}`.
- **`bench_a44_6_write_amplification`** — compares single-hash (pre-D+I wide
  schema) vs. double-hash (D+I) write overhead at `cols ∈ {4, 10, 20, 50}`.
- **`bench_a44_7_join_codegen_by_depth`** and
  **`bench_a44_7_scan_agg_delta_sql`** — join chain depth 2–16 and P5
  aggregate SQL generation at 1–5 group columns.

### A44-8 — `explain_stream_table()` GUC Threshold Section

`pgtrickle.explain_stream_table(name)` now includes a **GUC thresholds**
section in its output, showing the effective values of all tuning GUCs
(deep-join threshold, WAL poll limits, cost-cache capacity) alongside the
existing plan and mode information.

### A44-9 — `pgtrickle.wal_source_status()` — Per-Source WAL Diagnostics

New SQL function returning one row per registered source table with WAL CDC
diagnostics:

```sql
SELECT * FROM pgtrickle.wal_source_status();
```

| Column | Description |
|--------|-------------|
| `source_relid` | Source table OID |
| `source_name` | Fully-qualified source table name |
| `cdc_mode` | `trigger`, `wal`, or `transitioning` |
| `slot_name` | Logical replication slot name (NULL if trigger-based) |
| `slot_lag_bytes` | Current WAL slot lag in bytes |
| `publication_name` | Publication name (NULL if trigger-based) |
| `blocked_reason` | Human-readable reason why WAL CDC is unavailable (NULL if active) |
| `transition_started_at` | Timestamp when WAL transition began (NULL if not transitioning) |
| `decoder_confirmed_lsn` | Last LSN confirmed by the decoder (NULL if trigger-based) |

### A44-10 — D+I Change-Buffer Schema

**Breaking internal change** — the CDC change buffer table schema has been
redesigned for correctness and performance.

**Before (wide schema):** Each source column was stored as two columns
(`new_<col>` and `old_<col>`). UPDATE was stored as a single `action = 'U'`
row; the DVM scan operator decomposed it at read time using a 5-CTE UNION ALL
pipeline.

**After (D+I schema):** Source columns are stored with their original names
(`"col"`). `UPDATE` is decomposed at write time into:
- A **D-row** (`action = 'D'`) carrying the **old** values.
- An **I-row** (`action = 'I'`) carrying the **new** values.

Both rows carry the same `changed_cols` VARBIT bitmask; genuine
INSERT/DELETE rows have `changed_cols = NULL`.

Benefits:
- Scan SQL is significantly simpler (no UNION ALL decomposition at read time).
- Aggregate differentiation eliminates the LATERAL VALUES join.
- Write amplification is constant (2 rows per UPDATE regardless of column count).
- Change buffer tables are compatible with standard SQL tooling.

The `sync_change_buffer_columns()` migration guard detects existing wide-schema
buffers (any `new_*`/`old_*` columns) and performs a no-op, logging a warning.
To migrate an existing deployment, use `pgtrickle.repair_stream_table(name)`.

### A44-11 — D+I Benchmark Suite

`bench_a44_11_di_delta_scan` exercises the full D+I Scan→Aggregate pipeline
at `cols ∈ {4, 10, 20, 50}` to track differential scan performance as the
column count grows.

---

## [0.42.0] — Repair API, Docs Overhaul & Test Infrastructure

v0.42.0 delivers a new `repair_stream_table` SQL function for disaster recovery
and self-healing after PITR restores, a comprehensive documentation overhaul
(deprecated GUC appendix, RLS bypass warnings, updated architecture diagrams),
security hardening of the WAL decoder via SQL parameterization, and a major
test infrastructure uplift with state-polling helpers, new correctness property
tests, and two new CI gates.

### A42-1 — `pgtrickle.repair_stream_table(name text) → text`

New SQL-callable function for stream table repair and self-healing. Use after
point-in-time recovery (`pg_basebackup` / PITR) or any operation that may have
left CDC triggers, change buffer tables, or catalog state inconsistent.

**Actions performed:**
1. Acquires an advisory lock on the stream table to prevent concurrent mutations.
2. Verifies the stream table exists in `pgtrickle.pgt_stream_tables`.
3. Resets the refresh frontier to `NULL` and sets `needs_reinit = true`, forcing a full refresh on the next scheduler cycle.
4. Rebuilds any missing CDC triggers on all source tables.
5. Recreates any missing change buffer tables in `pgtrickle_changes`.
6. Resets error fuse state and stream table status to `ACTIVE`.
7. Returns a text summary of all actions taken.

```sql
-- After a PITR restore, reinstall all CDC infrastructure
SELECT pgtrickle.repair_stream_table('order_totals');
-- → "repair_stream_table(order_totals): frontier reset; triggers OK; buffers rebuilt (1 recreated); status reset to ACTIVE"
```

### A42-2 — Catalog Generator Accuracy Improvement

`scripts/gen_catalogs.py` regex now correctly captures non-`pub` `#[pg_extern]`
functions (pgrx does not require `pub`). The SQL API catalog grew from 24 to 98
entries, including `repair_stream_table`. CI fails on catalog drift.

### A42-3 — SQL Reference: `repair_stream_table` Signature

`docs/SQL_REFERENCE.md` now correctly documents `→ text` (not `→ void`) return
type with full examples and parameter table.

### A42-4 — Stale-Term Docs Linter (`just docs-lint`)

New `just docs-lint` recipe greps all `docs/**/*.md` for retired GUC names
(`pg_trickle.max_workers`, `pg_trickle.max_parallel_refresh_workers`) and fails
if any are found outside deprecated/compatibility sections. Also integrated into
`.github/workflows/docs-drift.yml` as a CI gate.

### A42-5 — Deprecated GUC Compatibility Appendix

`docs/CONFIGURATION.md` now has an **Appendix: Deprecated / Compatibility
GUCs** section documenting `event_driven_wake` and `wake_debounce_ms` with
migration guidance. Existing active references to the two retired GUCs were
updated to their current replacements across `PATTERNS.md`, `SCALING.md`,
`PRE_DEPLOYMENT.md`, and `docs/integrations/multi-tenant.md`.

### A42-6 — ARCHITECTURE.md Module Diagram Updated

`docs/ARCHITECTURE.md` module layout now correctly reflects the `src/dvm/parser/`
subdirectory structure introduced in v0.39.0 (G13-PRF), with all five sub-modules
(`mod.rs`, `types.rs`, `validation.rs`, `rewrites.rs`, `sublinks.rs`) listed.

### A42-7 — RLS Bypass Prominence

`docs/GETTING_STARTED.md` and `docs/PRE_DEPLOYMENT.md` now include prominent
security notices explaining that pg_trickle background workers execute with
`SET LOCAL row_security = off` (matching PostgreSQL's own `REFRESH MATERIALIZED
VIEW` semantics), and providing mitigation guidance.

### A42-8 — Generated Docs Freshness CI Gate

`.github/workflows/docs-drift.yml` now runs both the catalog check
(`python3 scripts/gen_catalogs.py --check`) and the stale-term linter on every
PR targeting `main`, on every push to `main`, and on a weekly schedule.

### A42-9 — State-Polling Test Helpers

`tests/common/mod.rs` now exports seven polling helpers:
- `wait_for_first_refresh` / `wait_for_refresh_history` / `wait_for_refresh_after`
- `wait_for_cdc_mode`
- `wait_for_stream_table_status`
- `wait_for_scheduler_tick`
- `wait_for_query_count`

All new E2E test files created in this release use these helpers exclusively
(zero `tokio::time::sleep` calls). Existing tests had their most egregious
blind waits replaced.

### A42-10 — Differential SUM(CASE) E2E Tests

New test file `tests/e2e_sum_case_differential_tests.rs` (5 tests) validating
that `SUM(CASE WHEN ... END)` expressions correctly trigger full refresh mode
instead of attempting algebraically incorrect incremental updates.

### A42-11 — SUM(CASE) AST-Level Detection

`src/dvm/operators/aggregate.rs`: `is_algebraically_invertible` now calls the
new `expr_contains_case` helper which recursively inspects the `Expr` AST for
CASE expressions at any nesting depth, catching wrapped forms like
`SUM(CAST(CASE ... END AS numeric))`.

### A42-12 — FULL JOIN Aggregate Property Tests

New test file `tests/e2e_full_join_aggregate_tests.rs` (4 tests) including a
`test_full_join_diff_vs_full_property_10_cycles` property test that runs 10
insert/delete cycles and asserts DIFFERENTIAL refresh produces identical output
to FULL refresh after each cycle.

### A42-13 — WAL Decoder SQL Parameterization

`src/wal_decoder.rs`: `write_decoded_change` now builds fully parameterized
SPI queries using `$N` placeholders and `Spi::run_with_args`, eliminating
all direct string interpolation of WAL values into SQL. This closes a class of
SQL injection risks in the WAL CDC path.

### A42-14 — Stale EC-06 Comment Cleanup

`src/dvm/operators/scan.rs`: Updated design comments from the outdated EC-06
reference to accurately describe the current net-counting strategy and point to
the `test_keyless_multiset_property` test.

### A42-15 — Keyless Multiset Property Tests

New test file `tests/e2e_keyless_tests.rs` (4 tests) validating that keyless
(no primary key) tables maintain correct multiset semantics through 10 cycles
of insert/delete/update operations.

### A42-16 — Fuzz Smoke CI Job

New `.github/workflows/fuzz-smoke.yml` runs daily and on PRs that touch fuzz
targets. On PRs: replays the corpus for each target (zero new crashes allowed).
On schedule/dispatch: runs each target for 60 s and uploads crash artifacts.
Targets: `parser_fuzz`, `cron_fuzz`, `dag_fuzz`, `guc_fuzz`, `cdc_fuzz`,
`wal_fuzz`.

---

## [0.41.0] — DVM Correctness: Structural Cache Keys, Placeholder Safety & WAL Transition Guards

v0.41.0 targets internal correctness of the Differential View Maintenance (DVM)
engine: eliminating snapshot-CTE cache collisions on structurally different
subtrees, making unresolved SQL placeholders hard errors, guarding WAL CDC
transitions against concurrent DDL, and ensuring the pool worker obeys the
global `pg_trickle.enabled` switch.

### A41-1 — Structural Snapshot CTE Cache Key Fingerprint

The old `snapshot_cache_key()` concatenated leaf-table aliases, meaning two
OpTrees with identical source tables but different join conditions, join types,
predicates, projections, or grouping expressions mapped to the *same* key and
could silently share a snapshot CTE.

The function now computes a 64-bit structural fingerprint via `DefaultHasher`,
recursively encoding every operator type, join condition, predicate, projection,
group-by expression, and child fingerprints before formatting the key as a
16-character hex string.  Collision probability is now astronomically low for
any realistic OpTree and is independent of alias names.

### A41-2 — Placeholder Resolution Full-Validation Assertion

`resolve_delta_template()` and `resolve_lsn_placeholders()` now return
`Result<String, PgTrickleError>` instead of `String`. After all substitutions
a `check_no_remaining_placeholders()` call scans for any leftover `__PGS_*__`
or `__PGT_*__` tokens. If any are found, `PgTrickleError::UnresolvedPlaceholder`
is returned and propagated all the way to the SQL surface as a clear
`ERRCODE_INTERNAL_ERROR` with a detail message naming the offending token and
the calling context.

This converts a class of silent wrong-query bugs (where an unresolved
placeholder was executed as literal SQL text) into an immediate, actionable
server error.

### A41-3 — WAL Transition Eligibility Recheck at Commit Point

Before committing the `TRANSITIONING → WAL` state change, the background
worker now calls `recheck_source_eligible_for_wal()` to verify that:

- `pg_class.relkind = 'r'` (table not dropped)
- primary-key columns are still present
- `REPLICA IDENTITY = FULL` is still set

If any check fails, the replication slot is immediately dropped, the catalog is
reset to `Trigger` mode, and a `WalTransitionError` is returned.  This closes a
race window in which a concurrent `DROP CONSTRAINT` or `ALTER TABLE … REPLICA
IDENTITY DEFAULT` could leave the CDC pipeline in an inconsistent WAL mode with
stale slot resources.

### A41-4 — Pool Worker `pg_trickle.enabled` Check

The persistent pool-worker main loop now checks `config::pg_trickle_enabled()`
at the top of each iteration. When `pg_trickle.enabled = off` the worker sleeps
500 ms and skips all job claiming, ensuring that a live-reload of the GUC
immediately quiesces all workers without requiring a process restart.

### A41-5 — Document Isolation Invariants (All Execution Modes)

`// A41-5 — Isolation invariant:` doc comments have been added to all five
execution-mode functions in `src/scheduler/mod.rs`:

| Mode | Invariant |
|------|-----------|
| `execute_worker_singleton` | `READ COMMITTED` per-refresh; no cross-session writes visible |
| `execute_worker_atomic_group` | `READ COMMITTED` with sub-transactions; repeatable-read group shares a snapshot |
| `execute_worker_immediate_closure` | Single `READ COMMITTED` transaction; trigger-propagated and atomic |
| `execute_worker_cyclic_scc` | Per-iteration `READ COMMITTED`; external observers see partial states between iterations |
| `execute_worker_fused_chain` | Single `READ COMMITTED` transaction; bypass tables `ON COMMIT DROP`; externally atomic |

---

## [0.40.0] — Operator Trust, Maintainability & Release Confidence

v0.40.0 focuses on building confidence for operators, maintainers, and adopters:
auto-generated API/GUC catalogs to eliminate drift, a formal security model,
drain-mode runbook with E2E proof, expanded alert rules, dbt/relay parity,
strict unsafe-block gate, L0-cache documentation truthfulness, formal deprecation
of `event_driven_wake`, and secret scanning in CI.

### O40-1 — Auto-Generated GUC & SQL API Catalogs

`scripts/gen_catalogs.py` parses `src/config.rs` and `src/**/*.rs` to produce
`docs/GUC_CATALOG.md` (125 GUCs) and `docs/SQL_API_CATALOG.md` (24 SQL-callable
functions). Both are checked by a new `.github/workflows/docs-drift.yml` CI gate
that fails if the catalogs fall out of sync with the source.

Run `just gen-catalogs` to regenerate; `just check-docs-drift` (or the CI gate)
detects drift.

### O40-2 — Security Model & Secret-Handling Guide

New `docs/SECURITY_MODEL.md` covers: `SECURITY DEFINER` scope, `search_path`
hardening, RLS boundary semantics, CDC buffer access controls, TRUNCATE
semantics, relay credential storage guide, background worker privilege model,
incident response checklist, and v1.0 supply-chain preparation checklist.

### O40-3 — Drain-Mode Runbook & E2E Proof

New `docs/RUNBOOK_DRAIN.md` provides a step-by-step operator runbook for
controlled shutdown, rolling upgrade, and load-testing drain scenarios with
observability guidance and troubleshooting steps.

Six new E2E tests in `tests/e2e_drain_mode_tests.rs` validate: idle drain
returns `true`, `is_drained()` state reflection, post-resume catch-up,
drain under active workload, timeout parameter semantics, and change-buffer
accumulation during drain.

### O40-4 — Expanded Alert Rules

`monitoring/prometheus/alerts.yml` gains eight new production-grade alert
rules:

| Alert | Threshold | Severity |
|-------|-----------|----------|
| `PgTrickleFreshnessLagHigh` | staleness > 600 s for 10 min | warning |
| `PgTrickleRefreshP99High` | avg_duration > 60 000 ms for 5 min | warning |
| `PgTrickleCdcBufferDepthHigh` | pending_rows > 500 000 for 5 min | warning |
| `PgTrickleWalSlotLagHigh` | retained_wal_mb > 200 for 5 min | warning |
| `PgTrickleWalSlotLagCritical` | retained_wal_mb > 1 024 for 2 min | critical |
| `PgTrickleWorkerPoolSaturated` | active ≥ 90 % pool_size for 5 min | warning |
| `PgTrickleCitusLeaseUnhealthy` | lease_held == 0 for 5 min | critical |
| `PgTrickleOtelExportErrors` | export_errors_total > 0 for 5 min | warning |

### O40-5 — dbt & Relay Parity

New dbt macro `pgtrickle_operational_status()` returns scheduler health,
drain state, CDC pause state, force-full mode, and back-pressure status.
New `pgtrickle_drain()` macro for drain from dbt. `stream_table_status()`
updated with `cdc_paused`, `force_full`, and `is_drained` fields.

### O40-6 — Unsafe-Inventory Gate (Strict Mode)

`.github/workflows/unsafe-inventory.yml` changed from `--report-only` to
strict mode: the workflow now exits 1 on unsafe-block regressions, making it
a hard PR gate. Unsafe blocks that need to be added must update the baseline
via an explicit PR that reviewers can audit.

### O40-7 — L0-Cache Truthfulness

`pg_trickle.template_cache` GUC documentation updated to explain the full
L0/L1/L2 cache architecture:
- **L0** (process-local `RwLock<HashMap>`) — fast, not shared across pooler
  connections; hit rate is low in PgBouncer transaction-pooling deployments.
- **L1** (thread-local delta template) — fastest, reset on each SPI connect.
- **L2** (UNLOGGED catalog table) — shared across all backends; the correct
  layer to rely on for cross-connection performance.

Operators using transaction-pooling should rely on L2 warm-up, not L0.

### O40-8 — `event_driven_wake` Formal Deprecation

`pg_trickle.event_driven_wake` and `pg_trickle.wake_debounce_ms` are
formally deprecated with full rationale in the GUC doc comments:
`LISTEN` is not allowed in PostgreSQL background workers; the scheduler
always uses latch-based polling. Both GUCs are preserved in v0.40.0 for
upgrade compatibility and will be removed in v1.0. Setting them now
emits a WARNING but does not break existing configurations.

### O40-9 — Secret Scanning CI

New `.github/workflows/secret-scan.yml` runs `gitleaks` on all pull
requests to `main`, on pushes to `main`, and weekly. `.gitleaks.toml`
provides an allowlist for known example credentials in documentation
and test fixtures.

---

## [0.39.0] — Operational Truthfulness & Distributed Hardening

v0.39.0 focuses on making pg_trickle's operational behavior more honest and
robust: CDC hold mode, enhanced diagnostics, SQLSTATE-aware retry, OpenTelemetry
documentation, Citus chaos hardening, and a broader testing pyramid.

### O39-1/O39-8 — CDC Hold Mode (`cdc_capture_mode`)

New GUC `pg_trickle.cdc_capture_mode` (default `discard`). When set to `hold`,
captured change rows are buffered in the change table while CDC is paused rather
than being silently discarded. The existing `discard` behavior is unchanged and
remains the default to preserve backward compatibility.

New SQL function `pgtrickle.cdc_pause_status()` returns per-stream-table CDC
pause state including `paused`, `capture_mode`, and an operator-guidance `note`.

### O39-2 — Wake Truthfulness

The scheduler no longer attempts `LISTEN/NOTIFY` in background worker contexts
(PostgreSQL does not support this). Wake truthfulness is documented in the
header of `e2e_wake_tests.rs`; tests now verify that the scheduler falls back
to polling correctly rather than asserting sub-polling-interval wake latency.

### O39-3 — Configuration Documentation

`docs/CONFIGURATION.md` gains three new sections covering GUCs introduced in
v0.36.0 (WAL Backpressure), v0.37.0 (pgVectorMV & OpenTelemetry), and v0.39.0
(Operational Truthfulness: `cdc_capture_mode`). Each section includes an
operator checklist and configuration examples.

### O39-4 — Upgrade Guide

`docs/UPGRADING.md` gains upgrade sections for every version from 0.34.0 to
0.39.0, including schema change details, new GUCs, new functions, and known
limitations per release.

### O39-5 — OpenTelemetry Operator Guide

New `docs/OPENTELEMETRY.md` provides an end-to-end operator guide for the W3C
Trace Context integration introduced in v0.37.0. Covers Jaeger/Tempo/OTEL
Collector configuration, span attributes, failure behavior (best-effort; never
blocks refresh), and verification steps.

Three new E2E tests (`tests/e2e_otel_tests.rs`) verify trace context capture,
unreachable-endpoint graceful degradation, and disabled-tracing NULL context.

### O39-6 — SQLSTATE-First SPI Retry

New GUC `pg_trickle.use_sqlstate_classification` (default `off`). When enabled,
the scheduler uses a SQLSTATE integer class (40xxx = retryable, 23xxx = not
retryable, etc.) before falling back to text pattern matching. The new unified
`classify_error_for_retry()` function is used at both retry decision points in
the scheduler.

### O39-7 — Citus Chaos Test Harness

New `tests/e2e_citus_chaos_tests.rs` containing four `#[ignore]` chaos tests:
- CHAOS-1: Worker death mid-refresh (graceful failure + recovery)
- CHAOS-2: Coordinator restart during lease (lock invalidation + re-acquire)
- CHAOS-3: Shard rebalance during active CDC (no row gaps)
- CHAOS-4: Stale worker slot cleanup (topology change detection)

Tests require `CITUS_COORDINATOR_URL` and `CITUS_*_CONTAINER` env vars; they
are skipped automatically when not set.

### O39-9 — Enhanced `explain_stream_table()`

`pgtrickle.explain_stream_table()` now shows: Status, Populated, Refresh mode
(with `force_full_refresh` GUC note), CDC status (paused/active + capture mode),
Backpressure state, and the Defining query. This makes the function a one-stop
diagnostic tool for operators.

### O39-10 — TPC-H EXPLAIN Artifacts CI

New workflow `.github/workflows/tpch-explain-artifacts.yml` captures EXPLAIN
ANALYZE BUFFERS output and p50/p99 timing for TPC-H queries Q04, Q05, Q07, Q08,
Q09, Q20, Q22. Runs weekly (Sunday 06:00 UTC) and on manual dispatch. Artifacts
are uploaded and retained for 90 days.

New `test_tpch_explain_artifacts` test function (`#[ignore]`) in
`tests/e2e_tpch_tests.rs` performs the collection.

### O39-11 — SQLancer Light PR Mode

Two new non-`#[ignore]` tests in `tests/e2e_sqlancer_tests.rs`:
- `test_sqlancer_crash_oracle_light`: 50 random queries, crash oracle.
- `test_sqlancer_equivalence_oracle_light`: 50 random queries, equivalence oracle.

Both use a fixed seed (`SQLANCER_LIGHT_SEED`) and bounded case count
(`SQLANCER_LIGHT_CASES`, default 50) for fast, deterministic PR CI gates.

### O39-12 — Fuzz Target Expansion

Two new libFuzzer targets:
- `fuzz/fuzz_targets/wal_fuzz.rs`: SQLSTATE classifier + `sqlstate_to_string` invariants.
- `fuzz/fuzz_targets/dag_fuzz.rs`: schedule parsing, cron validation, SELECT * detection.

Both verify no-panic and determinism properties for adversarial inputs.

### O39-13 — Inbox/Outbox Reliability Property Tests

Unit-level property tests in `src/api/inbox.rs` (`#[cfg(test)]`) covering:
- Partition exhaustiveness: every aggregate ID maps to exactly one worker.
- Hash determinism: same inputs always produce the same assignment.
- Negative `total_workers` degenerate case.
- Known hash anchors for regression protection.

SQLSTATE classifier property tests in `src/error.rs` covering:
- Retryable class detection.
- Bracket-code extraction with malformed inputs.
- `sqlstate_to_string` totality and determinism.

### O39-14 — PR-Scoped Upgrade E2E Slice

New CI job `upgrade-e2e-pr-slice` in `.github/workflows/ci.yml`. Triggered on
PRs that modify `sql/`, `src/config.rs`, `src/cdc.rs`, or `src/api/`. Runs the
most recent N-1→N upgrade pair using a stock `postgres:18.3` container (no
custom Docker build). Tests filtered to `smoke | basic | catalog` labels for
speed.

**Upgrade:** Run `ALTER EXTENSION pg_trickle UPDATE TO '0.39.0'`. The
`0.38.0→0.39.0` migration creates `pgtrickle.cdc_pause_status()` and registers
the `cdc_capture_mode` GUC comment. No existing tables or functions are removed.

---

## [0.38.0] — EC-01 Join Correctness Sprint

v0.38.0 is a focused correctness release for EC-01, the join phantom-row class
where non-deduplicated join deltas could leave stale row IDs behind across
refresh cycles.

### EC-01 — Unconditional PH-D1 Cleanup

Non-deduplicated keyed join deltas now run PH-D1 cross-cycle cleanup after every
differential apply. The cleanup computes the current FULL-refresh row-id set and
deletes stream-table row IDs that no longer exist in the correct result. This
removes historical phantoms that are not present in the current delta and keeps
DIFFERENTIAL output convergent with FULL output.

### RowIdSchema Planner Guard

The dormant `RowIdSchema` model is now exercised during DVM planning. The
planner infers row-id schemas for scans, transparent operators, joins,
aggregates, set operations, CTEs, recursive plans, lateral plans, and scalar
subqueries before generating delta SQL. If a row-id pipeline is internally
inconsistent, planning fails with a clear `RowIdSchema verification failed`
message rather than allowing silent refresh drift.

### EC-01 Property Release Gate

Added `e2e_ec01_property_tests`, a DIFF-vs-FULL property test that runs a
deterministic three-table join aggregate through 100 mixed-DML cycles by
default. Each cycle includes inserts, updates, deletes on both sides of joins,
and co-delete cases, then compares DIFFERENTIAL and FULL stream tables with
multiset equality and row-id diagnostics.

Q07 and Q15 are no longer allowed in `IMMEDIATE_SKIP_ALLOWLIST`, so CI must
prove those query shapes instead of accepting silent skips.

### Removed

- **`pgtrickle-tui`** — The terminal dashboard binary has been removed from
  this repository. All SQL-level monitoring functions (`pgtrickle.health_check()`,
  `pgtrickle.list_stream_tables()`, etc.) remain fully available in the extension.

**Upgrade:** The `0.37.0 -> 0.38.0` migration has no SQL-object changes; the
release changes Rust DVM/refresh behavior and test coverage only.

---

## [0.37.0] — pgVector Incremental Aggregates & Distributed Trace Propagation

v0.37.0 adds two independent capability pillars: incremental vector aggregates
for pgvector workloads, and W3C Trace Context propagation through the CDC →
DVM → MERGE pipeline.

### F4 — pgVector Incremental Aggregates

Stream tables can now maintain `avg(embedding)` and `sum(embedding)` over
`vector`, `halfvec`, and `sparsevec` columns incrementally. The DVM planner
detects vector-typed aggregate arguments at plan time and reclassifies them to
use pgvector-native differential operators (`VectorAvg`, `VectorSum`) that
maintain a running `(count, sum_vector)` auxiliary state instead of a full
table scan on every change.

**SQL usage:**

```sql
CREATE EXTENSION pgvector;

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    category TEXT,
    embedding vector(3)
);

-- This stream table is maintained incrementally — no full scan on INSERT.
SELECT pgtrickle.create_stream_table(
    'category_centroids',
    'SELECT category, avg(embedding)::vector AS centroid
     FROM products GROUP BY category',
    schedule => '5s'
);
```

**GUC:** `SET pg_trickle.enable_vector_agg = on;` (session-level opt-in).

**Distance operator fallback:** `<=>`, `<->`, `<#>` operators in WHERE clauses
trigger automatic full-refresh fallback because they are non-monotone. The
planner emits a `WARNING` so operators know the mode downgrade occurred.

**Criterion benchmarks** are provided for vector_avg, vector_sum, and mixed
workloads in `benches/diff_operators.rs`.

**Documentation:** `docs/tutorials/PGVECTOR_EMBEDDING_PIPELINES.md`.

### F10 — W3C Trace Context Propagation

Every CDC change buffer table now contains a `__pgt_trace_context TEXT` column.
When an application sets the `pg_trickle.trace_id` GUC before executing DML,
the row-level and statement-level CDC triggers capture the W3C `traceparent`
string into that column.

After each differential refresh, if `pg_trickle.enable_trace_propagation = on`,
the extension reads the trace context from the change buffer and either:

- exports an OTLP/JSON span to `pg_trickle.otel_endpoint` (Jaeger, Zipkin,
  OTEL Collector), or
- logs the span at `INFO` level when no endpoint is configured.

The span covers the full CDC-drain → DVM-plan → merge-apply cycle, linking
PostgreSQL refresh latency directly to application request traces.

**GUCs added:**

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pg_trickle.enable_trace_propagation` | BOOL | `false` | Enable W3C trace propagation |
| `pg_trickle.otel_endpoint` | STRING | `''` | OTLP HTTP endpoint (e.g. `http://localhost:4318`) |
| `pg_trickle.trace_id` | STRING | `''` | W3C traceparent set by the application session |
| `pg_trickle.enable_vector_agg` | BOOL | `false` | Enable incremental pgvector aggregates |

**Upgrade:** The `0.36.0 → 0.37.0` migration script adds `__pgt_trace_context`
to all existing change buffer tables automatically.

### Internal improvements

- **A15/A16:** `src/scheduler` and `src/refresh/merge` each split into focused
  sub-modules (completed in v0.37.0 development cycle).

---

## [0.36.0] — Structural Hardening, Performance & Temporal IVM

v0.36.0 closes structural and performance gaps accumulated since the Citus arc.
The L0 process-local template cache is now constructed (was wired-but-empty
since v0.31.0). WAL slot backpressure enforcement is available via the new
`pg_trickle.enforce_backpressure` GUC. Structured JSON logging arrives for
OpenTelemetry/Loki integration. The `RowIdSchema` type formalises cross-operator
row-id compatibility, addressing the architectural root cause of EC-01 class bugs.
Temporal IVM (SCD Type 2, `AS OF TIMESTAMP` ready) and columnar storage backend
support are introduced. A drain mode API enables graceful quiesce before
maintenance windows.

### New features

- **A09 — L0 process-local template cache**: Process-local `RwLock<HashMap>`
  keyed by `(pgt_id, cache_generation)` avoids ~45 ms cold-start penalty per
  backend for connection-pooler workloads. Invalidated automatically on generation
  bump. New API: `shmem::l0_cache_lookup()`, `shmem::l0_cache_store()`,
  `shmem::invalidate_l0_cache()`.

- **A12 — WAL backpressure enforcement**: When
  `pg_trickle.enforce_backpressure = on`, CDC trigger writes are suppressed
  once the WAL replication slot lag reaches `slot_lag_critical_threshold_mb`.
  Writes resume when lag drops below 50% of the threshold (hysteresis).
  Default: `off`.

- **A17 — Typed DDL event payload**: Replaced string-tag matching in
  `hooks.rs` with a `DdlCommandKind` enum. `CREATE OR REPLACE FUNCTION` is
  now correctly classified as `FunctionChange`.

- **A18 — `RowIdSchema` type**: Every DVM operator can now declare its
  row-id hash schema. A `verify_pipeline()` function asserts cross-operator
  compatibility at plan time, making EC-01-class bugs detectable before execution.

- **A20 — Structured JSON logging**: New `src/logging.rs` module with
  `PgtLogEvent` struct and `pgt_info!` macro. When
  `pg_trickle.log_format = json`, events are emitted as structured JSON
  with fields `event`, `pgt_id`, `cycle_id`, `duration_ms`, `refresh_reason`,
  `error_code`, `msg`. Default: `text`.

- **A25 — Bulk alter / drop APIs**: New SQL functions
  `pgtrickle.bulk_alter_stream_tables(names TEXT[], params JSONB)` and
  `pgtrickle.bulk_drop_stream_tables(names TEXT[])` for dbt deployments
  managing many stream tables.

- **A35 — Drain mode**: `pgtrickle.drain(timeout_s INT DEFAULT 60)` signals
  the scheduler to stop accepting new cycles and waits for all in-flight
  refreshes to complete. `pgtrickle.is_drained()` checks drain status.
  Useful before `pg_upgrade`, rolling restarts, and backup windows.

- **CORR-1 / UX-1 — Temporal IVM**: `create_stream_table()` and
  `create_stream_table_if_not_exists()` now accept `temporal := true`.
  When enabled, `__pgt_valid_from TIMESTAMPTZ` and `__pgt_valid_to TIMESTAMPTZ`
  columns are automatically added to the storage table. A `temporal_mode` column
  is recorded in `pgtrickle.pgt_stream_tables`.

- **CORR-2 / UX-3 — Columnar storage backend**: `create_stream_table()` now
  accepts `storage_backend := 'heap'|'citus'|'pg_mooncake'` (default: `'heap'`).
  The backend is recorded in `pgtrickle.pgt_stream_tables.storage_backend` and
  can be overridden globally via the `pg_trickle.columnar_backend` GUC.

- **F5 — Online schema evolution**: When
  `pg_trickle.online_schema_evolution = on`, `ALTER QUERY` with only
  column additions (no removals) preserves the existing frontier and
  `is_populated` flag, enabling continuous differential refresh without
  a full reinit. Default: `off`.

- **F11 — `CREATE STREAM TABLE` SQL syntax**: New function
  `pgtrickle.exec_stream_ddl(TEXT)` parses custom DDL strings such as
  `CREATE STREAM TABLE name AS SELECT ...` and
  `CREATE OR REPLACE STREAM TABLE name AS SELECT ...` and
  `DROP STREAM TABLE name`.

- **F12 — Column lineage**: New function
  `pgtrickle.stream_table_lineage(name TEXT)` returns
  `TABLE(output_col, source_table, source_col)` from the `column_lineage`
  JSONB recorded in the catalog at creation time.

### New GUCs

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.enforce_backpressure` | `off` | Pause CDC writes when slot lag exceeds critical threshold |
| `pg_trickle.log_format` | `text` | Log format: `text` or `json` |
| `pg_trickle.drain_timeout` | `60` | Default drain timeout (seconds) |
| `pg_trickle.online_schema_evolution` | `off` | Preserve frontier on compatible ALTER QUERY |
| `pg_trickle.columnar_backend` | `none` | Default columnar backend: `none`, `citus`, `pg_mooncake` |
| `pg_trickle.temporal_stream_tables` | `off` | Global temporal IVM flag |

### Schema changes

- `pgtrickle.pgt_stream_tables` gains three new columns:
  - `temporal_mode BOOLEAN NOT NULL DEFAULT FALSE`
  - `storage_backend TEXT NOT NULL DEFAULT 'heap'`
  - `column_lineage JSONB`

### Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.36.0';
```

The migration script (`sql/pg_trickle--0.35.0--0.36.0.sql`) is fully
idempotent and adds the new columns with `IF NOT EXISTS`.

---

## [0.35.0] — Hardening, Reactive Subscriptions & Relay Resilience

v0.35.0 is a focused correctness, operability, and resilience sprint. It adds reactive NOTIFY subscriptions, an SLA summary API, CDC kill-switch GUCs, and several operator-facing improvements. The relay gains exponential reconnect backoff and `${ENV:VAR_NAME}` secret interpolation.

### New features

- **Reactive subscriptions** — `pgtrickle.subscribe(stream_table, channel)` / `pgtrickle.unsubscribe()` / `pgtrickle.list_subscriptions()`: NOTIFY-based reactive delivery after every non-empty refresh cycle (UX-SUB).
- **SLA summary API** — `pgtrickle.sla_summary()` returns p50/p99 latency, freshness lag, and error-budget remaining over a configurable window (`pg_trickle.sla_window_hours`, default 24 h) (F17).
- **Explain stream table** — `pgtrickle.explain_stream_table(name)` returns the defining query and cached refresh metadata for a stream table (A23).
- **Shadow-build evolution status** — `pgtrickle.view_evolution_status()` lists which stream tables are in a zero-downtime shadow build (UX-STATUS).
- **CDC kill-switch** — new `pg_trickle.cdc_paused` GUC (boolean, default `off`) pauses all CDC capture at the trigger level without dropping triggers (A07).
- **Force-full-refresh GUC** — `pg_trickle.force_full_refresh` (boolean, default `off`) forces all stream tables to use FULL refresh mode for a debugging/recovery window (A08).
- **FULL-fallback NOTICE** — a `NOTICE` is emitted every time differential refresh falls back to FULL refresh, including the reason string (A22).
- **Shadow-ST catalog columns** — `in_shadow_build` and `shadow_table_name` columns added to `pgtrickle.pgt_stream_tables` (UX-SHADOW).
- **History start_time index** — `pgt_refresh_history_start_time_idx (start_time DESC)` for faster SLA queries and retention pruning (A11).
- **Relay ENV-var interpolation** — connection strings support `${ENV:VAR_NAME}` placeholders that are expanded from the process environment at startup (A30).
- **Relay reconnect backoff** — the relay now retries failed PostgreSQL connections with exponential backoff (initial 100 ms, max 30 s, ±20 % jitter) (A38).
- **Relay backpressure** — new `sink_max_inflight` config field (default 1 000 messages) that can be used to pause upstream polling (A39).
- **Notify coalesce GUC** — `pg_trickle.notify_coalesce_ms` (integer, default 250 ms) reserved for future NOTIFY debounce (UX-GUC).

### Correctness fixes

- **A01 / EC-01**: cross-cycle phantom-row cleanup now runs unconditionally after every join differential refresh cycle (batch 1 024 rows) instead of being deferred. This eliminates phantom residual rows that accumulated over multi-cycle windows (A01).
- **A05**: `join_and_predicates()` no longer panics on an empty predicate list — now returns `Result<Expr, PgTrickleError>` (A05).

### Performance

- **History prune batch size** raised from 1 000 → 10 000 rows per transaction to reduce pruning lag on busy clusters (A10).
- **Citus lease jitter**: `try_acquire_st_lock()` adds 50–500 ms random backoff on INSERT conflict to prevent coordinator thundering herd (A13).

### Developer experience

- Unit tests added for `inbox_is_my_partition` (A06) and `outbox_table_name_for` (A06).
- Relay config tests added for `${ENV:VAR_NAME}` expansion.

### Upgrade notes

Run the upgrade migration:
```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.35.0';
```
The migration adds `pgt_refresh_history_start_time_idx`, creates `pgtrickle.pgt_subscriptions`, and adds `in_shadow_build` / `shadow_table_name` columns to `pgtrickle.pgt_stream_tables`. All DDL is idempotent.

---

## [0.34.0] — Citus: Automated Distributed CDC Scheduler & Shard Recovery

v0.33.0 shipped all the Citus distributed CDC infrastructure — per-worker WAL
slots, `pgt_st_locks` coordination, `poll_worker_slot_changes`, and
`handle_vp_promoted`. v0.34.0 closes the final gap: the scheduler is now fully
aware of distributed sources and drives the per-worker slot lifecycle
automatically, making distributed stream tables completely hands-off.

### What's new

- **Automated scheduler integration** (COORD-10, COORD-11, COORD-12):
  When a stream table source has `source_placement = 'distributed'`, the
  scheduler now calls `ensure_worker_slot()` on the first tick (and after
  rebalances), calls `poll_worker_slot_changes()` to drain per-worker WAL
  changes into the local buffer, and acquires/extends/releases a
  `pgt_st_locks` lease around the entire operation.

- **Shard rebalance auto-recovery** (COORD-13):
  The scheduler detects `pg_dist_node` topology changes by comparing active
  primaries against `pgt_worker_slots`. When a change is detected, stale
  slot entries are dropped, new worker slots are inserted, and the stream
  table is marked for a full refresh — no operator intervention needed.

- **Worker failure handling** (COORD-14):
  If `poll_worker_slot_changes()` fails for a worker, the error is logged
  and the worker is skipped for that tick. After
  `pg_trickle.citus_worker_retry_ticks` consecutive failures, a WARNING
  is emitted in the PostgreSQL log for operator attention. Refreshes
  against healthy workers continue uninterrupted.

- **New GUC** (COORD-15):
  `pg_trickle.citus_worker_retry_ticks` (default 5) — consecutive
  worker-poll failures before flagging in `citus_status`. Set to 0 to
  disable the alert.

- **Extended `citus_status` view** (COORD-16):
  The view now includes `last_polled_at` (timestamp of the last successful
  poll for each worker slot), `lease_holder`, `lease_acquired_at`,
  `lease_expires_at`, and `lease_health` (`'unlocked'` / `'locked'` /
  `'expired'`) columns for full operational visibility.

### Migration

No application-level changes required. The new scheduler behaviour activates
automatically for stream tables with `source_placement = 'distributed'`.
Operators using manual `LISTEN + handle_vp_promoted()` wiring can remove that
code — it is now redundant (though harmless to leave in place).

Run `ALTER EXTENSION pg_trickle UPDATE TO '0.34.0'` to pick up the new
`last_polled_at` column and extended `citus_status` view.

---

## [0.33.0] — Citus: Distributed Source CDC & Stream Tables

This release delivers world-class incremental view maintenance over Citus
distributed tables, and aligns with pg_ripple v0.58.0 Citus sharding support.
pg_trickle can now track changes on distributed source tables and write results
to distributed output tables, while leaving all non-Citus code paths completely
unchanged.

### pg_ripple Citus Co-location Helper

#### New: `pgtrickle.handle_vp_promoted(payload TEXT) → BOOLEAN`

Processes a `pg_ripple.vp_promoted` NOTIFY payload emitted by pg_ripple
v0.58.0 when a VP table is distributed via Citus.  Call this from any
regular backend session that is LISTENing to `pg_ripple.vp_promoted`:

```sql
LISTEN "pg_ripple.vp_promoted";
-- … receive notification …
SELECT pgtrickle.handle_vp_promoted(:'NOTIFY_PAYLOAD');
```

The function:
- Parses the payload JSON (`table`, `shard_count`, `shard_table_prefix`,
  `predicate_id`).
- Logs the promotion details.
- When the promoted table matches an active distributed CDC source in
  `pgt_change_tracking`, signals the scheduler to probe worker slots on the
  next tick without a full catalog scan.
- Returns `true` if a matching source was found, `false` otherwise.

`docs/integrations/citus.md` gains a new **pg_ripple Integration** section
covering co-location DDL, the `vp_promoted` notification contract, and
guidance on aligning `pgt_st_locks` lease expiry with
`pg_ripple.merge_fence_timeout_ms`.

### Distributed stream table output

`create_stream_table()` gains a new optional parameter
`output_distribution_column`. When provided, and Citus is installed, the
output storage table is converted to a Citus distributed table on that column
immediately after creation. Existing call sites without the parameter are
unaffected.

```sql
-- Co-locate the stream table with the source shards
CALL pgtrickle.create_stream_table(
    name                       => 'orders_summary',
    query                      => 'SELECT customer_id, count(*) FROM orders GROUP BY 1',
    output_distribution_column => 'customer_id'
);
```

### Per-worker WAL slot tracking (`pgt_worker_slots`)

A new catalog table `pgtrickle.pgt_worker_slots` records the logical
replication slot name and last-consumed frontier for each Citus worker node
per source table. This enables per-worker CDC polling and accurate lag
monitoring across all nodes in the cluster.

### Cross-node refresh coordination (`pgt_st_locks`)

A new catalog table `pgtrickle.pgt_st_locks` provides lightweight distributed
mutex semantics using `INSERT … ON CONFLICT DO NOTHING`. This replaces
advisory locks for distributed stream table refreshes, ensuring that only one
coordinator node applies changes at a time across a multi-coordinator Citus
setup.

### Citus observability view (`citus_status`)

`SELECT * FROM pgtrickle.citus_status` returns one row per
(stream table, source, worker) combination, showing the coordinator slot,
worker slot name, last consumed LSN, and source placement type. Use this view
to monitor replication lag and detect unreachable workers.

```sql
SELECT pgt_name, worker_name, worker_port, worker_slot, worker_frontier
FROM pgtrickle.citus_status;
```

### Correct apply path for distributed stream tables

Citus blocks cross-shard `MERGE` statements. pg_trickle now automatically
detects distributed output stream tables and switches to a
`DELETE + INSERT … ON CONFLICT DO UPDATE` apply path, which Citus supports
natively. Single-node and reference-table stream tables continue to use the
existing `MERGE` path.

### Pre-flight checks for Citus clusters

Two new pre-flight check functions are available via the Rust API:

- `check_citus_version_compat()` — verifies that all worker nodes are running
  the same pg_trickle version as the coordinator. Returns an error listing any
  mismatched workers.
- `check_worker_wal_levels()` — verifies that `wal_level = logical` is
  configured on all worker nodes. Returns an error if any worker has a lower
  WAL level, preventing silent slot-creation failures.

### Per-worker CDC helpers

The `poll_worker_slot_changes()` function drains a logical replication slot on
a remote Citus worker via `dblink` and writes the decoded changes into the
coordinator's local change buffer. The `ensure_worker_slot()` function creates
the slot if it does not already exist, making the setup idempotent on every
scheduler tick.

### Citus integration guide

A new documentation page at `docs/integrations/citus.md` covers prerequisites,
installation, placement options, the observability view, known failure modes
(unreachable workers, recycled WAL slots, shard rebalancing), and performance
considerations.

### Upgrade

Run the standard extension upgrade. The migration script adds the three new
catalog objects (`pgt_st_locks`, `pgt_worker_slots`, `citus_status`) and
replaces the three `create_stream_table` function signatures with versions that
include the new `output_distribution_column` parameter. Existing call sites
without the new parameter continue to work without change.

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.33.0';
```

---

## [0.32.0] — Citus: Stable Naming & Per-Source Frontier Foundation

This release lays the foundation for world-class Citus support by replacing
OID-based internal object names with stable hash-derived names and adding
Citus cluster detection helpers.

### Stable internal object naming

pg_trickle now names every internal object (change buffer tables, trigger
functions, WAL replication slots, publication names) using a short 16-character
hex string derived from the schema-qualified source table name:

```
changes_a3f7b2c1d0e5f9a8       -- was: changes_12345
pgt_cdc_fn_a3f7b2c1d0e5f9a8    -- was: pgt_cdc_fn_12345
pgtrickle_a3f7b2c1d0e5f9a8     -- was: pgtrickle_12345
```

This name is identical on every Citus node, survives `pg_dump`/restore cycles,
and survives OID reassignment after a major-version upgrade. Existing
installations are upgraded automatically by the migration script — all existing
objects are renamed in a single transaction with no downtime.

The change is invisible to end users: no SQL API changes, no configuration
changes, no behaviour changes on single-node PostgreSQL.

### Citus cluster detection

A new internal module (`src/citus.rs`) provides helpers to detect whether Citus
is loaded and how a given source table is distributed (local, reference, or
distributed). This information is stored in the catalog and will drive per-node
CDC and apply strategies in v0.35.0.

### New catalog columns

Three catalog tables gain new columns:

- `pgtrickle.pgt_stream_tables`: `st_placement TEXT DEFAULT 'local'`
- `pgtrickle.pgt_dependencies`: `source_stable_name TEXT`, `source_placement TEXT DEFAULT 'local'`
- `pgtrickle.pgt_change_tracking`: `source_stable_name TEXT`, `source_placement TEXT DEFAULT 'local'`, `frontier_per_node JSONB`

### New SQL function

`pgtrickle.source_stable_name(oid) → TEXT` — returns the 16-character stable
hash for any source relation by OID. Useful for diagnostics.

### Upgrade notes

The `0.31.0 → 0.32.0` migration script handles all object renames
automatically. Replication slots are renamed if the PostgreSQL version is 15+;
on older versions a manual rename step is logged as a NOTICE. Existing change
buffer data is preserved — only the table and function names change.

---

## [0.31.0] — Performance & Scheduler Intelligence

This release delivers measurable performance improvements for deployments with
many stream tables, along with new tools for monitoring scheduler behaviour
and reacting to processing backlogs before they become a problem.

### Faster immediate-mode updates

Stream tables configured in immediate mode — which update on every data change
rather than on a schedule — now handle those changes more efficiently.
Previously, every single data change caused PostgreSQL to create and destroy a
temporary table in the background, a fixed cost that adds up at high write
rates. That overhead has been eliminated.

This improvement is opt-in. Enable it with `pg_trickle.ivm_use_enr = true`
(requires PostgreSQL 18+).

### Fewer database round-trips for shared sources

When multiple stream tables all read from the same source table, pg_trickle
now scans their pending changes in a single database pass instead of once per
stream table. If you have ten stream tables all watching the same `orders`
table, pg_trickle makes one read instead of ten. The benefit scales with the
number of stream tables. This is on by default.

### Smarter update-strategy hints

Every refresh, pg_trickle chooses between two strategies for applying changes:
a merge approach (efficient for small change sets) and a delete-then-reinsert
approach (faster when large portions of the data have changed). Enabling
`pg_trickle.adaptive_merge_strategy` now logs a suggestion after each refresh
indicating whether the current strategy is optimal, based on the ratio of
changes to total rows. This makes performance tuning straightforward — no
restarts or code changes required.

### Silent fallbacks are now visible

When pg_trickle encounters a problem analysing certain query types, it falls
back to a slower, more conservative update mode. Previously this was invisible.
The count of such fallbacks is now tracked and surfaced in
`pgtrickle.metrics_summary()` under `ivm_lock_parse_error_count`, so you can
spot and address the underlying cause.

### Back-pressure alerts for overloaded pipelines

If data is arriving faster than pg_trickle can process it, the change buffer
grows. pg_trickle now watches this and, after 3 consecutive cycles above the
alert threshold (configurable via `pg_trickle.backpressure_consecutive_limit`),
raises a `change_buffer_backpressure` alert. Applications or monitoring systems
can listen for this event and respond — for example by slowing producers or
adding consumers.

### Coming soon: cross-database refresh coordination

A detailed design for a future cross-database refresh coordinator has been
published in `docs/research/multi_db_refresh_broker.md`. Implementation is
planned for v0.32.0.

### What changed

- Error messages are now categorised by standard SQL error code by default,
  making them easier to parse in automated monitoring. The previous behaviour
  can be restored with `pg_trickle.use_sqlstate_classification = false`.

### New settings

| Setting | Default | What it does |
|---------|---------|--------------|
| `pg_trickle.ivm_use_enr` | off | Eliminate temporary-table overhead in immediate mode (PostgreSQL 18+ only) |
| `pg_trickle.adaptive_batch_coalescing` | on | Scan change buffers for shared sources in a single pass |
| `pg_trickle.adaptive_merge_strategy` | off | Log update-strategy suggestions after each refresh |
| `pg_trickle.backpressure_consecutive_limit` | 3 | Consecutive over-threshold cycles before raising a back-pressure alert |

### Upgrade

Run `ALTER EXTENSION pg_trickle UPDATE TO '0.31.0';` — no manual changes
required. The faster immediate-mode path is opt-in; set
`pg_trickle.ivm_use_enr = true` to enable it.

---

## [0.30.0] — Pre-GA Correctness & Stability Sprint

This release is focused entirely on correctness and stability in preparation
for the 1.0 release. There are no new user-facing features — every change is
a fix, a safety guard, or a memory efficiency improvement.

### Fixed: phantom rows in join-based stream tables

Stream tables that join multiple source tables could silently accumulate stale
rows over time when a refresh was interrupted part-way through. Those rows are
now cleaned up automatically after every refresh, ensuring the result always
converges to the correct answer.

### Fixed: incorrect results for complex query patterns

Subqueries nested inside `CASE` expressions, `COALESCE` calls, and function
arguments are now correctly detected and handled. Previously, stream tables
using these patterns could produce wrong incremental refresh results.

### Safer snapshots

Snapshot creation and restore are now fully atomic. If anything goes wrong
mid-operation — a disk error, a timeout, a lost connection — the operation is
cleanly rolled back and no partial tables are left behind.

Restoring from a snapshot no longer relies on PostgreSQL's internal column
ordering, making restores safe across different PostgreSQL minor versions.

### Bounded memory for in-flight update data

The internal cache that stores update data between steps was previously
unbounded. On deployments with many stream tables, it could grow large over
time. The cache now enforces a configurable maximum and evicts the oldest
entries when full, keeping memory usage predictable.

Additionally, cached query templates now expire after a configurable age
(default: 7 days). Old plans are automatically removed during background
maintenance, preventing stale query plans from accumulating.

### Complexity cap for queries

A new `pg_trickle.max_parse_nodes` setting lets you cap query complexity.
Queries that exceed the limit are rejected immediately with a clear error
instead of consuming unexpected memory.

### New settings

| Setting | Default | What it does |
|---------|---------|--------------|
| `pg_trickle.use_sqlstate_classification` | off | Categorise errors by SQL error code (useful for automated retry logic) |
| `pg_trickle.template_cache_max_age_hours` | 168 (7 days) | Evict cached query plans older than this |
| `pg_trickle.max_parse_nodes` | 0 (disabled) | Reject queries that exceed this complexity limit |

### Upgrade

No schema changes. Upgrade from v0.29.0 with:

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.30.0';
```

---

## [0.29.0] — Relay CLI (pgtrickle-relay)

This release introduces `pgtrickle-relay` — a standalone companion tool that
connects pg_trickle to the outside world.

### What is pgtrickle-relay?

The relay bridges pg_trickle's inbox and outbox tables with external messaging
systems, handling the reliable "last mile" of getting data in and out of your
database.

- **Forward (outbox → external):** Watches your pg_trickle outbox tables and
  forwards new records to external systems as they arrive. Supported
  destinations include Kafka, NATS, HTTP webhooks, Redis Streams, AWS SQS,
  RabbitMQ, and plain text output.
- **Reverse (external → inbox):** Reads messages from external systems and
  writes them into your pg_trickle inbox tables, enabling fully bidirectional
  event-driven pipelines.

### Configured entirely through SQL

There are no YAML files or config files to manage. You set up and manage relay
pipelines with SQL:

| Function | What it does |
|----------|-------------|
| `pgtrickle.set_relay_outbox(...)` | Configure an outbox-to-external pipeline |
| `pgtrickle.set_relay_inbox(...)` | Configure an external-to-inbox pipeline |
| `pgtrickle.enable_relay(name)` | Start a relay pipeline |
| `pgtrickle.disable_relay(name)` | Pause a relay pipeline |
| `pgtrickle.delete_relay(name)` | Remove a relay pipeline |
| `pgtrickle.list_relay_configs()` | List all configured pipelines |

### Built for reliability

- **No duplicate messages:** Every destination uses a deduplication key to
  prevent the same message from being delivered more than once, even if the
  relay restarts mid-send.
- **High availability:** Multiple relay instances can run simultaneously and
  coordinate automatically using database-level locks — no external
  coordination service such as ZooKeeper or Redis is needed.
- **Live config updates:** Change relay configuration in SQL and it takes
  effect within seconds, with no restart.
- **Built-in monitoring:** Health check at `/health` and Prometheus metrics
  at `/metrics` (port 9090 by default).

### Upgrade notes

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.29.0';
```

The relay binary is distributed separately (see `Dockerfile.relay`). Existing
stream tables, views, and outbox/inbox APIs are unchanged.

---

## [0.28.0] — Transactional Inbox & Outbox Patterns

This release adds two complementary patterns for reliably integrating
pg_trickle with external systems.

### The problem these patterns solve

When you update a database and need to notify an external system — a message
queue, an API, a downstream service — you face a reliability challenge: what
happens if the database update succeeds but the notification fails? You can end
up with data in your database that the external system never heard about, or a
notification sent for a change that was rolled back.

The **outbox pattern** solves this: the notification is written in the same
database transaction as the data change, so they either both succeed or both
fail. pg_trickle then delivers the notification reliably once the transaction
has committed.

The **inbox pattern** is the reverse: external messages arrive into a managed
queue inside PostgreSQL, where they can be processed reliably, retried on
failure, and replayed if needed.

### Outbox

Enable the outbox on any stream table with `pgtrickle.enable_outbox()`. After
each refresh, pg_trickle writes a record to a dedicated outbox table. Your
application or the relay tool picks it up from there and forwards it to
external consumers.

Consumers can work in named **consumer groups** — similar to Kafka consumer
groups. Each consumer tracks its own position in the stream independently and
can be replayed, paused, or have its lease extended without affecting others.

| Function | What it does |
|----------|-------------|
| `pgtrickle.enable_outbox(name, retention_hours)` | Start capturing refresh output for external delivery |
| `pgtrickle.disable_outbox(name)` | Stop capturing |
| `pgtrickle.outbox_status(name)` | See the current outbox state |
| `pgtrickle.outbox_rows_consumed(stream_table, outbox_id)` | Acknowledge that records have been delivered |
| `pgtrickle.create_consumer_group(name, outbox, ...)` | Create a named group of consumers |
| `pgtrickle.drop_consumer_group(name)` | Remove a consumer group |
| `pgtrickle.poll_outbox(group, consumer, batch_size, ...)` | Claim the next batch of records |
| `pgtrickle.commit_offset(group, consumer, last_offset)` | Acknowledge processed records |
| `pgtrickle.extend_lease(group, consumer, ...)` | Hold onto a batch longer before it times out |
| `pgtrickle.seek_offset(group, consumer, new_offset)` | Jump to a specific position (for replay) |
| `pgtrickle.consumer_heartbeat(group, consumer)` | Signal that a consumer is still alive |
| `pgtrickle.consumer_lag(group)` | See how far behind each consumer is |

### Inbox

Create a named inbox with `pgtrickle.create_inbox()`. pg_trickle automatically
sets up a pending queue, a dead-letter queue (for messages that could not be
processed), and a stats table.

| Function | What it does |
|----------|-------------|
| `pgtrickle.create_inbox(name, ...)` | Create a managed inbox with pending queue and dead-letter queue |
| `pgtrickle.drop_inbox(name, ...)` | Remove an inbox |
| `pgtrickle.enable_inbox_tracking(name, ...)` | Attach inbox tracking to an existing table |
| `pgtrickle.inbox_health(name)` | Get a health summary for an inbox |
| `pgtrickle.inbox_status(name)` | Show queue depths and processing stats |
| `pgtrickle.replay_inbox_messages(name, event_ids)` | Reset specific messages for re-processing |

**Additional inbox capabilities:**

- **Ordered processing:** `pgtrickle.enable_inbox_ordering()` ensures messages
  for the same entity (e.g. the same customer or order ID) are processed in
  sequence, eliminating race conditions without any extra coordination in your
  application.
- **Priority tiers:** `pgtrickle.enable_inbox_priority()` marks messages as
  high or low priority so the scheduler processes urgent messages first.
- **Horizontal scaling:** `pgtrickle.inbox_is_my_partition()` provides
  consistent hash-based partition assignment for multi-worker inbox processing.
  Multiple workers can safely share an inbox without an external coordinator.
- **Gap detection:** `pgtrickle.inbox_ordering_gaps()` surfaces any sequence
  gaps per entity so you can detect and recover from missing messages.

### New settings

| Setting | Default | What it does |
|---------|---------|--------------|
| `pg_trickle.outbox_enabled` | on | Enable the outbox subsystem |
| `pg_trickle.outbox_retention_hours` | 24 | How long to keep delivered outbox records |
| `pg_trickle.outbox_drain_batch_size` | 1000 | Records to process per drain pass |
| `pg_trickle.outbox_skip_empty_delta` | on | Skip writing an outbox record when there are no changes |
| `pg_trickle.consumer_dead_threshold_hours` | 24 | Hours before a silent consumer is considered dead |
| `pg_trickle.inbox_enabled` | on | Enable the inbox subsystem |
| `pg_trickle.inbox_processed_retention_hours` | 72 | How long to keep processed inbox records |
| `pg_trickle.inbox_dlq_alert_max_per_refresh` | 10 | Alert when this many messages land in the dead-letter queue in one cycle |

### Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.28.0';
```

---

## [0.27.0] — Operability, Observability & DR

This release focuses on three areas: disaster recovery tooling, better
visibility into multi-database deployments, and a more reliable built-in
metrics server.

### Snapshot and restore

You can now export a stream table's current data to an archive table and
restore it later. This is useful for bootstrapping a new read replica without
a full database dump, taking a point-in-time snapshot before a risky migration,
or recovering a stream table to a known-good state.

| Function | What it does |
|----------|-------------|
| `pgtrickle.snapshot_stream_table(name, target)` | Export a stream table to an archive table |
| `pgtrickle.restore_from_snapshot(name, source)` | Restore from an archive table |
| `pgtrickle.list_snapshots(name)` | List available snapshots with size and age |
| `pgtrickle.drop_snapshot(snapshot_table)` | Delete a snapshot |

Restore aligns the stream table's internal progress marker with the snapshot,
so incremental refresh resumes correctly without any manual steps.

### Predictive schedule recommendations

pg_trickle now analyses its own refresh history and recommends optimal refresh
intervals for each stream table.

- `pgtrickle.recommend_schedule(name)` returns a suggested interval and a
  confidence score. Confidence is low on new deployments and rises as history
  accumulates (at least 20 samples are needed before the score is meaningful).
- `pgtrickle.schedule_recommendations()` returns recommendations for all stream
  tables in one call.
- A **`predicted_sla_breach`** alert fires when the model predicts the next
  refresh is likely to miss your freshness target by more than 20%. The alert
  fires at most once every 5 minutes by default, to avoid flooding.

### Cluster-wide worker visibility

In deployments that run pg_trickle across multiple databases,
`pgtrickle.cluster_worker_summary()` shows which databases are consuming
background workers. This makes it easy to diagnose situations where one
database is crowding out others.

All Prometheus metrics now include database-level labels, so you can split a
single Grafana panel by database.

### Metrics server improvements

- A new `pgtrickle.metrics_summary()` SQL function returns cluster-wide refresh
  and error counts — useful for monitoring without a Prometheus scraper.
- Port conflicts now produce a clear error message instead of failing silently.
- Malformed HTTP requests now return a proper `400 Bad Request` response.

### New settings

| Setting | Default | What it does |
|---------|---------|--------------|
| `pg_trickle.schedule_recommendation_min_samples` | 20 | Minimum history samples before schedule confidence is meaningful |
| `pg_trickle.schedule_alert_cooldown_seconds` | 300 | Minimum seconds between consecutive `predicted_sla_breach` alerts |
| `pg_trickle.metrics_request_timeout_ms` | 5000 | Maximum time the metrics server waits for a request (ms) |

### Upgrade

This release upgrades the internal pgrx library to 0.18.0. This is transparent
to users. Run:

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.27.0';
```

---

## [0.26.0] — Test & Concurrency Hardening

This release is all about making pg_trickle more reliable and battle-tested.
There are no new SQL commands or user-facing features — every change is
internal: more tests, safer concurrent operations, cleaner code structure, and
better error messages.

### Safer under concurrent load

Running multiple operations at the same time — such as modifying a stream table
while it's actively refreshing, or dropping a table while its workers are still
running — is now explicitly tested and guaranteed to be safe. These scenarios
were handled before, but lacked the tests to prove it. That proof is now part
of every build.

- **Simultaneous alter + refresh** no longer risks a deadlock. The catalog
  stays consistent throughout.
- **Drop during refresh** aborts cleanly — no orphaned change buffers or
  dangling catalog rows left behind.
- **Parallel scheduler workers** are prevented from picking the same stream
  table for refresh at the same time — a hard guarantee, not just a convention.
- **Simultaneous buffer promotion** — when two workers race to promote a change
  buffer, exactly one succeeds and the metadata stays consistent.

### More stable SLA-based scheduling

The scheduler uses a predictive model to decide when to refresh stream tables,
balancing your freshness targets against system load. That model now holds its
ground under difficult workloads.

- **Bursty, sawtooth, and spike workloads** are all validated in a new
  dedicated test suite.
- **No more tier flapping** — the priority tier of a stream table (which
  controls how aggressively it is refreshed) now requires 3 consecutive
  breaches before downgrading and 3 consecutive successes before upgrading.
  This prevents the system from oscillating at the boundary, which caused
  unnecessary refresh churn in earlier releases.
- A **10,000-iteration randomised stress test** confirms the tier stays stable
  even under adversarial latency patterns.

### Fuzz testing and extreme-scale validation

The extension is now tested against malformed, random, and adversarial inputs
in three new fuzz test areas, preventing certain classes of unexpected input
from crashing the extension:

- Invalid cron schedule expressions
- Unrecognised or malformed configuration values
- Unexpected row shapes in change-capture triggers

Two new scale tests verify behaviour at extremes:

- A source table with **1,000 partitions** installs change-capture triggers and
  completes its first refresh within 60 seconds.
- A flooded worker pool does **not** starve high-priority stream tables in a
  second database — multi-database fairness is enforced under load.

### Cleaner internals: refresh module reorganised

The refresh orchestrator had grown into a single very large file. It has been
split into three focused modules with **no behaviour change**:

| Module | What it handles |
|--------|----------------|
| `orchestrator` | Deciding when and how to refresh — timing, cost model, recovery |
| `codegen` | Building the SQL queries and managing the query cache |
| `merge` | Executing the actual refresh — incremental, full, or TopK |

### Better error messages

Error messages throughout the extension now include more context — table names,
operation types, and hints such as "check system clock" on timestamp failures.
This makes it easier to diagnose problems from logs alone.

A new crash-recovery test verifies that a publication subscriber that was
active when the database was killed catches up with **zero data loss** after
restart.

---

## [0.25.0] — Scheduler Scalability & Pooler Performance

pg_trickle now comfortably manages **thousands of stream tables** on commodity
hardware — a significant jump from the practical ceiling of a few hundred in
earlier releases. The scheduler avoids reloading the full catalog on every
tick, change detection is batched into far fewer database round-trips, and a
new cache-sharing mechanism means connecting backends can skip expensive
query re-parsing entirely. If you use a connection pooler such as PgBouncer,
RDS Proxy, or Supabase Pooler, this release delivers the largest latency
improvement to date.

### Scales to thousands of stream tables

Previously, the scheduler queried the catalog on every tick — a process that
grew slower as the stream table count increased. Metadata is now cached per
backend and only reloaded when the dependency graph actually changes. Checking
whether source tables have new rows is batched across an entire refresh group
into a single query, down from one query per source per tick. Dependency-graph
rebuilds now happen in the background without blocking ongoing refreshes, so
you never get a stall when a stream table is created or dropped.

**New GUC: `pg_trickle.worker_pool_size`** (default `0` = spawn-per-task).
Set this to a positive number to keep that many background workers running
permanently, eliminating roughly 2 ms of spawn overhead per worker on
high-throughput deployments.

### Faster connections through poolers

A new shared-memory signal lets each connecting backend check whether the
query-template cache is already warm. If it is, the backend skips query
parsing entirely and jumps straight to the cached result. This matters most in
pooled environments — PgBouncer, RDS Proxy, Supabase — where backends connect
and disconnect frequently and re-parsing on every connection was a hidden cost.

The per-backend template cache is now bounded by
**`pg_trickle.template_cache_max_entries`** (default `0` = unbounded). When
the limit is reached, the least-recently-used entry is evicted automatically,
keeping memory usage predictable on servers with many concurrent backends.

A new SQL function, **`pgtrickle.clear_caches()`**, flushes all cache levels
in one call — useful after schema changes or when debugging unexpected
behaviour.

### Lower overhead on high-write workloads

Change fingerprinting — the hashing that identifies which rows changed —
now streams values directly into the hash function instead of building a
temporary string per row, eliminating one heap allocation per incoming change.
SQL buffers in the query-projection step are pre-sized rather than repeatedly
concatenated. Refresh timing data (how long full and incremental refreshes
take) is stored in shared memory so parallel workers can read it without a
catalog round-trip.

### More conservative refresh-mode predictions

The predictive model that decides when to fall back from incremental to full
refresh is now more stable. It waits for at least 60 seconds of history before
making any prediction — preventing erratic switches on fresh deployments —
removes statistical outliers before fitting, and keeps its output within a
reasonable band around recent observed timings.

### Subscriber lag tracking for downstream publications

If you use `stream_table_to_publication()` to feed a downstream system,
pg_trickle now monitors how far behind each subscriber's replication slot has
fallen. When a subscriber exceeds **`pg_trickle.publication_lag_warn_bytes`**,
a warning is logged and change-buffer cleanup is paused for that slot until it
catches up — preventing data loss for slow consumers.

A new SQL function, **`pgtrickle.worker_allocation_status()`**, returns
per-database worker usage, quotas, and queue depth across the cluster. Useful
for diagnosing scheduler starvation in multi-tenant deployments.

### Upgrade notes

- **Row ID change:** The internal hash function changed from xxh64 to xxh3.
  If your application relies on stable pg_trickle row ID values across
  versions, run `SELECT pgtrickle.reinitialize('<schema>.<table>')` on each
  affected stream table after upgrading.
- **No schema changes** beyond two new SQL functions (`clear_caches` and
  `worker_allocation_status`). No data migration required.

---

## [0.24.0] — Join Correctness & Durability Hardening

This release focuses on two themes: **correctness** — ensuring stream tables
that join multiple source tables always give you the right answer — and
**durability** — ensuring your data is never lost or skipped, even when the
server crashes or long-running transactions are in flight.

### More accurate results from multi-table joins

When a stream table combines rows from two or more source tables, pg_trickle
now guarantees that an incremental refresh produces exactly the same result as
a full recompute from scratch. A subtle bug in how rows were tracked across
refresh cycles could previously cause phantom rows to accumulate silently over
time. Those phantom rows are now detected automatically after every incremental
refresh and cleaned up.

### No data loss across crashes or restarts

pg_trickle now records its progress in a crash-safe sequence: it saves its
intent before writing data, then marks completion afterwards. If the server
goes down between those two steps, pg_trickle reconciles its position on
restart — no changes are processed twice and none are silently dropped. The
scheduler also persists its last known-safe position across restarts, closing
a narrow gap that existed in earlier versions.

### Long-running transactions no longer cause missed changes

If a database transaction stays open while pg_trickle is running a refresh,
the changes it is writing could previously be overlooked — they were captured
before the refresh started but not yet visible to it. pg_trickle now checks
for open transactions before advancing its read position and waits for them to
commit first.

- **`pg_trickle.frontier_holdback_mode`** — controls the holdback behaviour.
- **`pg_trickle.frontier_holdback_warn_seconds`** (default `60`) — logs a
  warning when a transaction has been blocking progress longer than this
  threshold.

### Works correctly on managed cloud databases

AWS RDS, Cloud SQL, and Azure Database for PostgreSQL restrict access to
certain monitoring views. pg_trickle now detects this automatically and tells
you exactly what to do:

```sql
GRANT pg_monitor TO <your_pg_trickle_role>;
```

Without this grant, pg_trickle previously behaved as if no transactions were
open — the same unsafe condition the holdback feature was built to prevent.
See `docs/TROUBLESHOOTING.md` section 14 for full diagnosis steps.

### Choose your durability level

The new **`pg_trickle.change_buffer_durability`** setting controls how
carefully incoming changes are stored before processing:

- **`unlogged`** (default) — fastest; change buffers do not survive a server
  crash.
- **`logged`** — survives crashes and replicates to standby servers.
- **`sync`** — maximum safety; every write is confirmed to disk before
  continuing.

### Automatic history clean-up

Old refresh history rows are now pruned automatically in small background
batches during idle time. Previously the history table grew without bound,
which could become noticeable on busy deployments.

### Alerts for frozen stream tables

The new **`pgtrickle.df_frozen_stream_tables`** view flags any stream table
that has not refreshed within 5× its expected interval, and sends a
notification on the `pgtrickle_alert` channel. Useful for catching a stuck or
disabled stream table before users notice stale data.

### New monitoring metrics

Two new Prometheus metrics expose holdback state:

- **`pg_trickle_frontier_holdback_lsn_bytes`** — how far behind the read
  position is being held, in bytes of WAL.
- **`pg_trickle_frontier_holdback_seconds`** — how long the oldest blocking
  transaction has been running.

> **Note:** All metrics now use the `pg_trickle_` prefix consistently. If
> your dashboards or alerting rules use the old `pgtrickle_` prefix, update
> them before upgrading.

---

## [0.23.0] — Performance Tuning & Diagnostics

This release gives you better tools to understand and control how pg_trickle
performs, with new settings for memory tuning and new functions for
inspecting what the extension is doing under the hood.

### See exactly what SQL is running

Turn on `pg_trickle.log_delta_sql` and pg_trickle will log the SQL it
generates for each incremental refresh. You can paste that SQL directly
into `EXPLAIN ANALYZE` to understand why a particular refresh is taking
longer than expected — no code changes required.

### Tune memory for refreshes without restarting

`pg_trickle.delta_work_mem` lets you give incremental refresh queries more
(or less) working memory without touching PostgreSQL's global settings or
restarting the server. Apply it instantly with:
```sql
ALTER SYSTEM SET pg_trickle.delta_work_mem = 256;
```

### Automatic statistics before each refresh

pg_trickle now runs a quick statistics pass on change buffers before
executing an incremental refresh. This gives PostgreSQL's query planner
accurate row counts and generally produces faster, more predictable query
plans with no manual intervention. Controlled by `pg_trickle.analyze_before_delta`
(on by default).

### Warning when incremental is unexpectedly slower than full

If an incremental refresh takes longer than the last full refresh,
pg_trickle now logs a warning that includes both timings. This surfaces
scenarios where incremental refresh has become counterproductive so you
can investigate and adjust thresholds.

### Alert when too many changes pile up

Set `pg_trickle.max_change_buffer_alert_rows` to a row count and pg_trickle
will warn you whenever any source table's pending change buffer exceeds
that threshold. This is useful for catching unexpected write bursts before
they slow down your refreshes.

### Refresh timing statistics at a glance

The new `pgtrickle.pgtrickle_refresh_stats()` function returns per-stream-table
refresh durations — average, 95th percentile, and 99th percentile — in a
single query. No need to manually aggregate the history table.

### Inspect generated SQL without running it

Call `pgtrickle.explain_diff_sql(name)` on any stream table to see the SQL
pg_trickle would use for an incremental refresh — without actually executing
it. Useful for understanding query structure and diagnosing performance issues.

---

## [0.22.0] — Downstream CDC, Parallel Refresh & Predictive Cost Model

This release makes it easier to feed stream table changes to other systems,
gives you a knob to control how many refreshes run at once, and adds
automatic intelligence for choosing between incremental and full refresh.

### Stream table changes can flow to other systems

`stream_table_to_publication(name)` creates a PostgreSQL logical replication
publication for a stream table. Any downstream tool that understands
PostgreSQL replication — Debezium, Kafka Connect, a read replica, or a
custom consumer — can then subscribe and receive changes as they happen.
Publications are removed automatically when the stream table is dropped.
Use `drop_stream_table_publication(name)` to remove one manually.

### Control how many tables refresh at once

`pg_trickle.max_parallel_workers` caps the number of stream tables that
can refresh simultaneously. The scheduler already runs independent refreshes
in parallel; this setting gives you an explicit limit if you want to reserve
database resources for your application.

### Automatic mode switching based on predicted cost

pg_trickle now learns from your refresh history. Before each incremental
refresh it predicts how long it will take based on recent timings. If that
prediction exceeds 1.5× the cost of a full refresh, it switches to full
refresh for that cycle automatically — no manual intervention needed. The
lookback window, threshold, and minimum sample count are all configurable:
- `pg_trickle.prediction_window` — how many recent refreshes to consider (default 60).
- `pg_trickle.prediction_ratio` — how much more expensive incremental must
  be before switching to full (default 1.5).
- `pg_trickle.prediction_min_samples` — minimum history before the model
  activates (default 5).

### Set a freshness target and let pg_trickle handle the rest

Call `set_stream_table_sla(name, interval)` with your target maximum data
age — for example `'5 seconds'` or `'1 minute'` — and pg_trickle assigns
the most appropriate refresh tier automatically. It re-evaluates the
assignment over time as real-world refresh performance changes.

---

## [0.21.0] — Reliability, Safety & Operational Tools

This release focuses on making pg_trickle safer and easier to operate day-to-day.
It eliminates hidden crash risks in the query analysis engine, adds new
operational commands for maintenance windows, and introduces a built-in
monitoring endpoint so you don't need extra software to observe pg_trickle.

### The extension can no longer crash your database

When pg_trickle analyses a query internally, it previously had hidden error
paths that could — in rare edge cases — abort a PostgreSQL backend process.
All of those paths now return a structured error instead of crashing.
Additionally, a compile-time rule now prevents production code from ever
calling the Rust equivalent of an unchecked assertion, so this class of
bug cannot be reintroduced silently.

### Warning for queries that shouldn't use incremental refresh

If you create a stream table with a query that calls time-sensitive or
non-deterministic functions such as `now()`, `random()`, or
`gen_random_uuid()`, pg_trickle now warns you at creation time. Those
functions produce a different result every time they run, which means
incremental refresh would produce wrong answers — the warning lets you
catch this before it becomes a data problem.

### Pause and resume everything at once

Two new functions let you halt and restart all active stream tables with
a single SQL call:

```sql
SELECT pgtrickle.pause_all();   -- stop all refreshes (e.g. before maintenance)
SELECT pgtrickle.resume_all();  -- restart them when you're done
```

### Refresh only when the data is actually stale

`pgtrickle.refresh_if_stale(name, max_age)` triggers a refresh only if the
stream table is older than your specified threshold. Returns `TRUE` when a
refresh ran, `FALSE` when the data was already fresh enough. Useful for
scripts and scheduled jobs that shouldn't over-refresh.

### Export a stream table's definition

`pgtrickle.stream_table_definition(name)` returns the complete
`CREATE STREAM TABLE` statement for any stream table. Handy for
documentation, disaster recovery playbooks, and migrations.

### Test query changes safely before going live

A three-step canary workflow lets you try a new query on a shadow copy of
your stream table and compare the results before committing to the change:

1. `canary_begin(name, new_query)` — creates a shadow stream table running
   the new query in parallel with the original.
2. `canary_diff(name)` — shows exactly which rows differ between the old
   and new queries.
3. `canary_promote(name)` — atomically switches the live stream table to
   the new query once you are satisfied with the results.

### Built-in monitoring endpoint

Set `pg_trickle.metrics_port = 9188` and pg_trickle serves a Prometheus-
compatible metrics endpoint directly — no extra exporter software needed.
Metrics include total refreshes, failures, rows changed per refresh, and
the number of active stream tables.

### Visibility into recursive query fallbacks

When a query containing a recursive clause cannot be refreshed incrementally
and falls back to a full refresh, pg_trickle now logs a notice and records
the reason in refresh history. Previously this happened silently.

### Upgrade

```sql
ALTER EXTENSION pg_trickle UPDATE TO '0.21.0';
```

---

## [0.20.0] — Self Monitoring

**pg_trickle now monitors itself.** Instead of you having to check on
pg_trickle's health manually, this release lets pg_trickle watch its own
performance, spot problems early, and even fix some of them on its own. Five
new stream tables sit in the `pgtrickle` schema and continuously analyse
refresh history — the same technology you use for your own data, pointed
inward. One SQL call sets everything up; one call tears it down.

We call this *self monitoring* — pg_trickle uses its own stream-table technology
to keep an eye on itself, just like it keeps your data views up to date.

### What's new

- **One-click self-monitoring** — run `SELECT pgtrickle.setup_self_monitoring()`
  and pg_trickle creates five monitoring stream tables that continuously track
  how well it is performing. Run `teardown_self_monitoring()` to remove them.
  Both are idempotent — safe to call as many times as you like, even during
  rolling upgrades.

- **Health at a glance** — the new `self_monitoring_status()` function shows the
  status of all five monitoring views in one query: whether each one exists,
  its refresh mode, and the last time it refreshed. Quick to run from a
  monitoring script or dashboard.

- **Threshold recommendations** — after enough refresh cycles accumulate
  (typically 10–20 minutes of activity), `df_threshold_advice` starts
  producing suggestions for each stream table. Each recommendation includes
  a confidence level (HIGH / MEDIUM / LOW) and a reason — for example,
  "DIFF is 73% faster — raise threshold to allow more DIFF". A
  `sla_headroom_pct` column shows exactly how much faster incremental refresh
  is versus full refresh for that table.

- **Automatic tuning** — set `pg_trickle.self_monitoring_auto_apply = 'threshold_only'`
  and pg_trickle will apply HIGH-confidence threshold recommendations
  automatically. Changes are rate-limited to once per 10 minutes per stream
  table, and every adjustment is logged to `pgt_refresh_history` with
  `initiated_by = 'SELF_MONITOR'` so you have a full audit trail.

- **Real-time alerts** — when pg_trickle detects an anomaly (duration spike
  exceeding 3× the baseline, or two or more recent failures), it sends a
  `NOTIFY` on the `pgtrickle_alert` channel with a JSON payload. Your
  application, Alertmanager webhook, or `LISTEN` client can act immediately
  without polling.

- **Scheduling interference detection** — `df_scheduling_interference` tracks
  pairs of stream tables that consistently overlap during refresh. When
  overlap is heavy, the scheduler automatically backs off its poll interval
  (up to 2× the configured base) to reduce contention.

- **Visual dependency graph** — the new `explain_dag()` function renders
  your full refresh pipeline as a Mermaid or Graphviz DOT diagram. User
  stream tables appear in blue, self-monitoring tables in green, suspended tables
  in red. Paste the output into any Mermaid renderer or `dot` to see exactly
  how your tables depend on each other.

- **Scheduler overhead report** — `scheduler_overhead()` returns metrics
  for the last hour: total refreshes, how many were self-monitoring, the
  fraction they represent, and average durations. Useful for confirming that
  self-monitoring adds negligible cost.

### What pg_trickle watches

| Monitoring view | What it tracks |
|-----------------|----------------|
| `df_efficiency_rolling` | Rolling-window refresh speed, change ratio, DIFF vs FULL counts |
| `df_anomaly_signals` | Duration spikes (> 3× baseline), error bursts, mode oscillation |
| `df_threshold_advice` | Per-table threshold recommendations with confidence level and reasoning |
| `df_cdc_buffer_trends` | Change-capture buffer growth rate per source table; alerts on burst spikes |
| `df_scheduling_interference` | Refresh overlap patterns; pairs with 3+ concurrent refreshes in the last hour |

### Faster and more reliable

- A new index on `pgt_refresh_history(pgt_id, start_time)` speeds up all
  self-monitoring queries and general history lookups. Applied automatically
  during the 0.19.0 → 0.20.0 upgrade.
- Old history records are now pruned in batches of 1,000 rows per transaction
  (previously one large DELETE), which avoids long lock holds on
  `pgt_refresh_history` during the nightly cleanup.
- `check_cdc_health()` is enriched with spill-risk alerts: if a source
  table's max burst delta exceeds 10× its average, you get an early warning
  before the buffer fills.
- `explain_st()` now shows two new properties: `self_monitoring_coverage`
  (none / partial / full) and `recommended_refresh_mode`, so diagnostics
  automatically surface self-monitoring data when it is available.

### New documentation and tooling

- **SQL Reference** — a new "Self Monitoring — Self-Monitoring" section covers
  all five stream tables, `setup_self_monitoring()`, `teardown_self_monitoring()`,
  confidence levels, and the `sla_headroom_pct` column.
- **Getting Started** — a new "Day 2 Operations" section walks through
  enabling self-monitoring, reading recommendations, enabling auto-apply, and
  visualising the DAG.
- **Configuration** — `pg_trickle.self_monitoring_auto_apply` is fully
  documented with values, rate-limiting behaviour, and the audit trail.
- A ready-made **Grafana dashboard** (`pg_trickle_self_monitoring.json`) with
  five panels covers refresh throughput, anomaly heatmap, threshold
  calibration, CDC buffer growth, and the scheduling interference matrix.
- A **dbt macro** (`pgtrickle_enable_monitoring`) enables monitoring as a
  post-hook with one line in `dbt_project.yml`.
- A **quick-start SQL script** at `sql/self_monitoring_setup.sql` walks through
  setup, auto-apply, alert listening, and status verification in six steps.

---

## [0.19.0] — Security, Scheduler Performance & Operator Convenience

**Safer, faster, easier to operate.** This release closes several security
and correctness gaps, adds new conveniences for operators and developers, and
significantly improves performance for deployments with many stream tables.
The background scheduler finds the next table to refresh 10–15× faster.
Four breaking changes are included — all easy to adapt to, each one
correcting behaviour that was a source of subtle bugs in production.

### Breaking changes

- **Only owners can modify their own stream tables** — other database users
  can no longer drop or alter a stream table they did not create. If shared
  access is intentional, grant superuser or explicitly add the user as owner.
  Superusers are unaffected.

- **Dropping a stream table no longer cascades** — `drop_stream_table()` now
  behaves like PostgreSQL's own `DROP TABLE`: it refuses to drop if dependent
  objects exist, unless you pass `cascade => true` explicitly. Previously it
  silently removed all dependents, which surprised operators after restructuring.

- **The refresh notification channel was renamed** — change `LISTEN pgtrickle_refresh`
  to `LISTEN pg_trickle_refresh` (note the added underscore). The old name
  was inconsistent with every other channel in the extension.

- **The `delete_insert` refresh strategy was removed** — this strategy could
  produce wrong results for queries containing aggregates or `DISTINCT`. If
  you had it configured, pg_trickle logs a warning and automatically switches
  to the safe `auto` strategy. No data is lost; the next refresh corrects
  any affected rows.

### New features

- **Installation health check** — `version_check()` returns the installed
  extension version, the loaded library version, and the PostgreSQL server
  version in one row. If the extension was upgraded but the server has not
  been restarted, you get an explicit warning. Useful in deploy scripts and
  smoke tests.

- **Write and refresh in one step** — `write_and_refresh(sql, st_name)`
  executes an arbitrary SQL statement and immediately refreshes the named
  stream table in the same transaction. Downstream readers see consistent
  results as soon as the transaction commits — no polling loop needed.

- **Better connection-pooler support** — the new
  `pg_trickle.connection_pooler_mode` GUC configures pg_trickle for
  PgBouncer, pgcat, or Supavisor at the cluster level. Previously each
  stream table had to be configured individually, which was error-prone on
  large deployments.

- **Automatic refresh history cleanup** — `pgt_refresh_history` is now
  trimmed automatically after 90 days (configurable with
  `pg_trickle.history_retention_days`; set to `0` to disable). Without
  this, the history table could grow by thousands of rows per day on
  busy deployments.

- **Schema migration tracking** — pg_trickle now records which upgrade
  scripts have been applied in `pgtrickle.pgt_schema_version`. This makes
  it straightforward to verify that a deployment is fully up to date and
  simplifies the rollback story.

- **Clearer skip messages** — when a refresh is skipped because another
  refresh of the same stream table is already running, you now see a
  `NOTICE: skipping refresh of <name> — already running` message instead
  of silence. Reduces confusion when debugging slow or stuck schedulers.

- **Deeper diagnostics** — `explain_st()` gains a `with_analyze` parameter.
  When set to `true`, it runs `EXPLAIN (ANALYZE, BUFFERS)` on the refresh
  query and returns actual row counts, timing, and buffer hit/miss ratios —
  the same information PostgreSQL's query planner provides for any query,
  but surfaced inside the stream-table diagnostic tool.

- **New deployment guides** — step-by-step documentation for PgBouncer,
  pgcat, Supavisor, CNPG, and Kubernetes deployments, plus an operational
  runbook for common Kubernetes failure modes.

### Bug fixes

- Fixed a constraint-validation inconsistency in databases upgraded from
  0.11.0 or earlier where `pgt_refresh_history` had a duplicate check entry
  in the catalog. Affected databases could see spurious constraint errors
  on busy write paths.

- Error messages throughout the extension now show human-readable table
  names (e.g. `public.orders`) instead of raw PostgreSQL OIDs. This affects
  "source table was dropped", "schema changed", and several other error
  paths that were previously unreadable without a catalog lookup.

### Performance

- **10–15× faster scheduler dispatch** — the scheduler now finds the next
  stream table to process with a direct lookup instead of scanning the full
  list on every poll cycle. On a deployment with 500 stream tables this
  drops from ~650 µs to ~45 µs per poll, reducing background CPU overhead
  significantly at scale.

- **Single-query change detection** — when the scheduler checks whether any
  source tables have changed, it now issues one query covering all sources
  at once instead of one query per source table. On deployments with 50+
  source tables this meaningfully reduces the overhead of each scheduler
  cycle, especially under PgBouncer transaction pooling.

---

## [0.18.0] — Hardening & Delta Performance

**Hardening & Delta Performance.** This release focuses on correctness,
reliability, and giving operators better visibility into what pg_trickle is
doing. Stream tables that group by columns containing NULL values now refresh
correctly in all cases. A new memory safety net prevents runaway refreshes
from consuming too much RAM. Error messages across the board now explain what
went wrong and suggest how to fix it. Two new SQL functions —
`health_summary()` and `cache_stats()` — give you a single-query overview of
the entire system, and updated Grafana dashboards make monitoring plug-and-play.
The TPC-H industry benchmark now runs as a nightly regression guard, and
property-based tests mathematically verify the core delta engine's arithmetic.

### Highlights

- **NULL values in GROUP BY now handled correctly** — previous versions could
  produce wrong results when a stream table grouped by a column that contained
  NULL values and rows were deleted. The root cause was that NULL group keys
  broke the internal row-matching logic. This is now fixed: NULL keys are
  matched correctly during both inserts and deletes, so aggregate stream
  tables always return the right answer regardless of NULLs in the data.

- **Memory safety net for large deltas** — if an unexpectedly large batch of
  changes arrives (for example, a bulk import into a source table), the
  incremental refresh could previously consume unbounded memory. A new
  configuration option (`pg_trickle.delta_work_mem_cap_mb`) lets you set a
  ceiling. When a refresh would exceed it, pg_trickle automatically falls
  back to a full refresh instead of risking an out-of-memory crash.

- **Early warning when refreshes spill to disk** — when the incremental
  refresh engine runs low on memory, PostgreSQL may spill intermediate data
  to temporary files on disk, which is much slower. pg_trickle now detects
  this and sends a notification so you can investigate before performance
  degrades. If spilling happens repeatedly, the scheduler automatically
  switches the affected stream table to full refresh.

- **One-query system health check** — the new `pgtrickle.health_summary()`
  function returns a single row with everything you need at a glance: how
  many stream tables are active, how many are in error or suspended state,
  the worst staleness across all tables, whether the scheduler is running,
  and the overall cache hit rate. Perfect for dashboards, alerting rules, or
  a quick manual check.

- **Cache performance visibility** — the new `pgtrickle.cache_stats()`
  function shows how effectively pg_trickle is reusing its internal query
  templates. You can see cache hit rates, eviction counts, and current cache
  size — useful for tuning `pg_trickle.template_cache_size` on busy systems.

- **Better error messages** — every error pg_trickle can raise now includes a
  standard PostgreSQL error code (SQLSTATE), a DETAIL line explaining the
  context, and a HINT suggesting what to do. Instead of a cryptic internal
  error, you get actionable guidance like "Table 'orders' was dropped while
  stream table 'order_summary' depends on it — recreate the source table or
  drop the stream table."

### Monitoring & dashboards

- **Updated Grafana dashboards** — the bundled `pg_trickle_overview.json`
  dashboard now includes panels for template cache hit rate, P99 and average
  refresh latency, hourly refresh success/failure counts, and cache eviction
  trends. Import it into Grafana and point it at your Prometheus instance for
  instant visibility.

- **Prometheus metric documentation** — all 8 new metrics exposed by
  `cache_stats()` and `health_summary()` are now fully documented in the
  monitoring guide, with ready-to-use PromQL queries.

### Correctness & testing

- **TPC-H regression guard** — all 22 queries from the TPC-H industry
  benchmark now run nightly against known-good expected output. If a code
  change causes any query to return different results, CI fails immediately.
  This catches subtle correctness regressions that targeted tests might miss.

- **Mathematical proof of delta arithmetic** — 6 property-based tests
  (2,000 random cases each) verify that the core engine's insert/delete
  accounting is correct: operations compose in the right order, groups cancel
  out properly, and no phantom rows appear after mixed workloads. An
  additional 4 end-to-end property tests exercise the full pipeline from
  change capture through to the final merged result.

- **CDC edge case coverage** — new tests cover composite primary keys,
  generated (computed) columns, NULL values in non-key columns, and domain
  types — real-world schema patterns that were previously untested.

- **dbt integration tests** — the dbt adapter now has regression tests for
  AUTO refresh mode, stream table health checks, and refresh history
  lifecycle — ensuring the dbt workflow stays reliable across releases.

### Scalability

- **Scaling guide** — a new `docs/SCALING.md` document covers how to
  configure pg_trickle for large deployments (200+ stream tables), including
  worker pool sizing, tiered scheduling, per-database quotas, and tuning
  profiles for different workload types.

- **Buffer growth stress tests** — new tests verify that the
  `max_buffer_rows` safety limit works correctly under sustained high write
  rates, including automatic recovery back to incremental refresh after a
  burst subsides.

### Testing infrastructure

- **Faster CI on pull requests** — 19 additional test files (~197 tests)
  were moved to the lightweight test runner that does not require building a
  custom Docker image. Pull request CI is now faster without sacrificing
  coverage.

- **Upgrade path tested** — the full upgrade chain from version 0.1.3
  through every release up to 0.18.0 is verified automatically in CI,
  including function availability, schema integrity, and data survival.

### Fixed

- **Upgrade script completeness** — the 0.17.0→0.18.0 upgrade migration now
  includes all new and changed functions (`pg_trickle_hash`, `cache_stats()`,
  `health_summary()`), so `ALTER EXTENSION pg_trickle UPDATE` works correctly.

---

## [0.17.0] — Query Intelligence & Stability

**Query Intelligence & Stability.** This release teaches pg_trickle to make
smarter decisions about how to refresh each stream table, reduces unnecessary
work when only a handful of columns actually changed, and proves correctness
through 10,000 automated random mutations every night. Large deployments
with hundreds of stream tables now handle schema changes much faster.
Alongside these improvements, three new documentation resources make it
easier to get started, troubleshoot problems, and migrate from pg_ivm.

### Highlights

- **Query-aware refresh decisions** — pg_trickle previously used a fixed
  threshold to decide between incremental and full refresh: if more than 50%
  of rows changed, switch to full. That works for simple queries but is
  poorly calibrated for joins or aggregates. The engine now classifies each
  query by its complexity (simple scan, filter, aggregate, join, or
  join+aggregate) and weights the cost estimate accordingly. Simple queries
  stay incremental even at high change rates; expensive join-heavy queries
  switch to full refresh sooner when the data is largely different. You can
  also pin a table to always use one strategy with the new
  `pg_trickle.refresh_strategy` setting (`'auto'` / `'differential'` /
  `'full'`), or tune the aggressiveness with `pg_trickle.cost_model_safety_margin`.

- **Skip columns that did not change** — when a row is updated in a wide
  source table (say, 50 columns) but only 2 columns that the stream table
  actually uses are modified, pg_trickle previously processed the full change
  anyway. It now tracks exactly which columns were modified and skips updates
  that touch none of the relevant columns. For aggregate stream tables the
  savings go further: a value-only update that does not affect group
  membership is applied as a single lightweight correction instead of a
  delete-then-insert pair. On write-heavy workloads with wide tables, this
  reduces the volume of data flowing through the refresh pipeline by 50–90%.

- **Faster schema changes on large deployments** — every time you create,
  alter, or drop a stream table, pg_trickle previously rebuilt the entire
  internal dependency graph from scratch. With 100 stream tables that takes
  only a few milliseconds, but at 1,000 it becomes noticeable. The graph is
  now updated incrementally — only the affected edges are touched, leaving
  everything else in place. At 1,000 stream tables the rebuild time drops
  from ~600 µs to ~116 µs and no longer scales with the total number of
  tables in the database.

- **Nightly correctness oracle** — a new automated test runs 10,000 random
  data mutations every night against a broad set of query shapes. For each
  mutation it compares the result of incremental refresh against a full
  recompute and fails if they ever disagree. This catches subtle correctness
  bugs that only surface after unusual sequences of inserts, updates, and
  deletes — the kind that hand-written tests rarely reach.

- **`ROWS FROM()` fully supported** — queries that use `ROWS FROM()` to call
  multiple set-returning functions side-by-side are now fully supported in
  incremental mode, including updates and deletes. This was previously
  restricted to insert-only workloads.

### New documentation

- **Try it in 60 seconds** — a new `playground/` directory contains a
  `docker compose up` environment with PostgreSQL 18 + pg_trickle pre-wired,
  sample data loaded, and five stream tables ready to query. No installation
  required beyond Docker.

- **Troubleshooting runbook** — `docs/TROUBLESHOOTING.md` covers 13
  real-world failure scenarios: scheduler not running, stream table stuck in
  SUSPENDED state, CDC triggers missing, WAL slot problems, out-of-memory,
  disk full, circular dependency convergence issues, unexpected schema
  changes, worker pool exhaustion, and blown fuses. Each scenario lists
  symptoms, diagnostic queries, and step-by-step resolution.

- **Migrating from pg_ivm** — `docs/tutorials/MIGRATING_FROM_PG_IVM.md`
  is a step-by-step guide for teams moving from the pg_ivm extension. It
  maps every pg_ivm API to its pg_trickle equivalent, explains behavioral
  differences, and includes ready-to-run SQL examples and a post-migration
  verification checklist.

- **New user FAQ** — the top 15 common questions are now answered at the
  top of `docs/FAQ.md` so new users find answers before scrolling through
  the full document.

- **Post-install verification script** — `scripts/verify_install.sql` walks
  through the complete setup: checks that pg_trickle is loaded, creates a
  test stream table, runs a refresh, verifies the result, and cleans up.
  Useful for confirming a fresh installation or diagnosing environment issues.

### Stability & code quality

- **Safer internal code** — the number of `unsafe` Rust blocks in the query
  parser was reduced from 690 to 441 (a 36% drop) by introducing two
  helper macros that wrap the most common unsafe patterns. No behavior change;
  this makes the codebase easier to audit and maintain.

- **Cleaner internal structure** — the largest source file (`api.rs`, ~9,400
  lines) was split into three focused modules. This has no user-visible
  effect but makes the codebase significantly easier to work with and
  reduces the risk of regressions from unrelated code being in the same file.

- **Refresh logic extracted and tested** — seven functions responsible for
  building the SQL used during refresh were extracted into standalone
  testable units and covered with 29 new unit tests. This catches
  regressions in generated SQL templates before they reach production.

---

## [0.16.0] — Performance & Refresh Optimization

**Performance & Refresh Optimization.** This release makes stream table
refreshes significantly faster across the board. Small changes to large
tables are now applied without expensive full-table scans. Tables that only
receive new rows (no updates or deletes) use a streamlined path that skips
unnecessary work. Aggregate queries like `SUM` and `COUNT` are refreshed
with pinpoint updates instead of recalculating entire groups. A new template
cache eliminates repeated startup work when database connections are recycled.
An automated benchmark system now prevents future changes from accidentally
slowing things down.

### Highlights

- **Smarter refresh for small changes** — when only a handful of rows change
  in a large stream table (less than 1% of total rows), pg_trickle now uses
  a faster strategy that skips the full-table comparison. This can reduce
  refresh time by up to 40% for common workloads where most data stays the
  same between refreshes. The system picks the best strategy automatically,
  but you can override it via the `merge_strategy` setting.

- **Insert-only fast path** — stream tables backed by append-only data sources
  (like event logs or audit trails that never update or delete rows) are now
  detected automatically and refreshed using a much simpler, faster path.
  No configuration is needed — pg_trickle observes your data patterns and
  switches to the fast path on its own. If an update or delete is later
  detected, it safely falls back to the standard approach with a warning.

- **Faster aggregate refreshes** — stream tables that use `SUM`, `COUNT`,
  `AVG`, or `STDDEV` aggregates now update individual groups directly instead
  of re-joining against the entire table. For queries with many distinct
  groups, this can be 5–20× faster. Non-invertible aggregates like `MIN`,
  `MAX`, and `STRING_AGG` continue using the standard path.

- **Template cache for faster cold starts** — the first time a database
  connection refreshes a stream table, pg_trickle normally spends ~45 ms
  preparing the refresh query. A new cross-connection cache stores these
  prepared queries so that subsequent connections (including those from
  connection poolers like PgBouncer) start refreshing in about 1 ms instead.

- **Automated performance regression checks** — every code change to
  pg_trickle is now automatically benchmarked before it can be merged. If any
  operation slows down by more than 10%, the change is blocked until the
  regression is fixed. This protects users from accidental performance
  degradation in future releases.

### New features

- **Error reference guide** — a new [error reference](docs/ERRORS.md) page
  documents every error message pg_trickle can produce, explains what caused
  it, and suggests how to fix it. Useful when troubleshooting unexpected
  behavior in production.

- **Change buffer growth protection** — if a stream table's refresh keeps
  failing, the backlog of unprocessed changes could previously grow without
  limit, consuming disk space. A new `max_buffer_rows` setting (default:
  1,000,000 rows) caps this growth. When the limit is reached, pg_trickle
  performs a full refresh to clear the backlog and warns you about the
  situation.

- **Automatic index creation control** — pg_trickle has always created helpful
  indexes on stream tables automatically. A new `auto_index` setting lets you
  disable this behavior when you want full control over indexing. Stream tables
  using `SELECT DISTINCT` now also get an automatic index on their distinct
  columns.

- **Compaction and predicate pushdown stats** — the `explain_st()` diagnostics
  function now shows additional information about change buffer compaction
  thresholds, merge strategy selection, append-only mode, aggregate fast-path
  status, and template cache hit rates.

### Improved

- **Configuration guidance** — the documentation now includes detailed tuning
  advice for the `planner_aggressive` and `cleanup_use_truncate` settings,
  especially for environments using connection poolers like PgBouncer or
  running under memory pressure.

- **Terminal dashboard improvements** — the `pgtrickle` TUI dashboard now shows
  the effective refresh mode for each stream table (e.g., when a table is
  temporarily downgraded from differential to full refresh). The Alerts tab
  has been restructured with a clearer table layout and better distinction
  between "stale data" and "no upstream changes" conditions.

### Fixed

- **Append-only detection with chained stream tables** — stream tables that
  feed into other stream tables (cascading dependencies) now correctly skip
  the append-only fast path to avoid data inconsistencies. Previously, a
  chained stream table could incorrectly use the insert-only path even when
  downstream tables needed the full change set.

- **Append-only heuristic accuracy** — the automatic detection of insert-only
  data sources now also checks the stream table's own change buffer for
  non-insert operations, avoiding false positives.

- **Full refresh fallback for mixed changes** — when both a stream table and
  its source table have pending changes in the same refresh cycle, pg_trickle
  now correctly falls back to a full refresh to avoid inconsistencies.

- **`resume_stream_table()` confirmed working** — the function referenced in
  error messages when a stream table enters `SUSPENDED` state was verified to
  exist and work correctly (present since v0.2.0).

### Testing & quality

- 13 new end-to-end tests covering JOIN correctness across update/delete
  cycles, window function differential behavior, differential-vs-full
  equivalence validation, and source table schema evolution resilience.
- 5 new benchmark scenarios covering semi-joins, anti-joins, multi-table join
  chains, and aggregate queries at varying group counts. Total: 22 benchmark
  functions.
- 1,700 unit tests pass (up from 1,630 in v0.15.0).

---

## [0.15.0] — Interactive TUI, Bulk Create & Runaway-Refresh Protection

0.15.0 brings the terminal dashboard to full operational capability, adds
safety features that protect against runaway refreshes, and broadens the
ecosystem with guides for popular migration and ORM frameworks. It also
includes a major internal refactoring of the query parser and a new streaming
benchmark suite.

### Highlights

- **Interactive terminal dashboard** — the `pgtrickle` TUI is no longer
  read-only. Refresh, pause, resume, and repair stream tables directly from
  the dashboard. A command palette (`:`) with fuzzy search makes common
  operations fast. The poller reconnects automatically after network
  interruptions.

- **Bulk creation** — `pgtrickle.bulk_create()` creates many stream tables in
  a single atomic transaction, ideal for CI/CD and dbt pipelines.

- **Runaway-refresh protection** — two new safety nets prevent expensive
  merges from spiralling: a pre-flight row-count estimate that downgrades to
  FULL refresh when deltas are too large (`max_delta_estimate_rows`), and a
  spill detector that forces FULL refresh after repeated temp-file writes
  (`spill_threshold_blocks`).

- **Stuck-watermark alerting** — if an upstream ETL pipeline stops advancing
  its watermark, pg_trickle now pauses affected stream tables and sends a
  `watermark_stuck` notification so the issue is surfaced immediately rather
  than silently producing stale data.

- **Integration guides** — new documentation for Flyway, Liquibase,
  SQLAlchemy, Django, and dbt Hub helps teams adopt pg_trickle alongside
  their existing tooling.

### New Features

- **Volatile function policy** — a new `volatile_function_policy` setting
  lets you choose whether volatile functions (like `random()` or
  `clock_timestamp()`) should be rejected (the default), allowed with a
  warning, or allowed silently when creating stream tables.

- **Bulk create API** — `pgtrickle.bulk_create(definitions)` accepts a JSON
  array of stream table definitions and creates them all in one transaction.
  If any definition fails, the entire batch is rolled back.

- **Enhanced diagnostics** — `pgtrickle.explain_st()` now shows refresh
  timing statistics (min/max/average duration), partition info for
  partitioned source tables, and a dependency graph you can render with
  Graphviz.

- **Join strategy override** — the `merge_join_strategy` setting lets you
  force a specific join method (`hash_join`, `nested_loop`, or `merge_join`)
  during delta merges, which can help when the automatic heuristic doesn't
  suit your workload.

- **Pre-flight delta estimation** — when `max_delta_estimate_rows` is set,
  pg_trickle counts the delta rows before merging. If the count exceeds the
  limit, it falls back to a FULL refresh and logs a notice, preventing
  out-of-memory conditions on unexpectedly large change sets.

- **Spill-aware refresh** — if differential merges spill to disk repeatedly
  (controlled by `spill_threshold_blocks` and `spill_consecutive_limit`),
  the scheduler switches to FULL refresh automatically.

- **Stuck watermark hold-back** — the `watermark_holdback_timeout` setting
  detects watermarks that have not advanced within a configurable window.
  Downstream stream tables are paused and a `watermark_stuck` notification
  is emitted until the watermark advances again.

- **Cascade drop** — `drop_stream_table()` now accepts an optional `cascade`
  parameter (default `true`). Setting it to `false` raises an error if
  dependent stream tables exist, matching PostgreSQL's RESTRICT behavior.

- **Nexmark benchmark suite** — a 10-query streaming benchmark (modelled on
  an online auction system) validates correctness under sustained
  high-frequency inserts, updates, and deletes.

- **17 new end-to-end tests** — 7 tests for multi-level stream-table chains
  (3- and 4-level cascades with mixed refresh modes) and 10 tests for
  diamond/fan-in topologies with IMMEDIATE mode. No deadlocks were found.

### Terminal Dashboard (TUI)

- **Write actions** — refresh, pause, resume, repair, reset fuse, and
  gate/ungate operations can now be performed without leaving the dashboard.
- **Command palette** — press `:` for fuzzy-matched command entry with
  tab-completion.
- **Automatic reconnection** — the dashboard reconnects with exponential
  back-off (up to 15 s) after a connection loss, with a visual indicator.
- **Richer views** — all 14 views now show additional live data (diagnostics,
  CDC health, refresh history with row-delta counts, error remediation hints,
  dependency-graph annotations, worker queue status, and watermark alignment).
- **Cross-view filtering** — the `/` search filter now persists across all
  10 list views.
- **Navigation re-fetch** — moving between rows in the Detail view
  immediately fetches fresh data for the selected table.
- **Toast messages** — write actions show confirmation and error toasts.
- **Sort cycling** — press `s` / `S` on the Dashboard to cycle through 6
  sort modes.
- **Mouse support** — `--mouse` enables scroll-wheel navigation.
- **Theme toggle** — `t` or `--theme dark|light` switches colour themes.
- **JSON export** — `Ctrl+E` or `:export` writes the current view to a file.
- **TLS support** — `--sslmode` and `--sslrootcert` flags.

### Documentation & Ecosystem

- **Flyway / Liquibase guide** — migration patterns for versioned and
  repeatable migrations, rollback blocks, and CI environments.
- **SQLAlchemy / Django guide** — read-only model patterns, write-blocking
  safeguards, DRF viewsets, and freshness checking.
- **dbt Hub readiness** — the `dbt-pgtrickle` package is version-synced and
  ready for dbt Hub submission.
- **Kubernetes / CNPG** — updated probe configuration and a new deployment
  section in the Getting Started guide.
- **Full documentation review** — configuration reference expanded from 23
  to 40+ settings, missing SQL reference entries filled in, outdated FAQ
  answers corrected.

### Internal Improvements

- **Parser modularisation** — the 21 000-line query parser has been split
  into 5 focused sub-modules (`types`, `validation`, `rewrites`, `sublinks`,
  and the main entry point). No behavior change — all 1 687 unit tests pass.
- **Unsafe audit** — every `unsafe` block in the codebase (~750 total) now
  has a `// SAFETY:` comment explaining why it is sound.
- **Shared-memory cache RFC** — an RFC for a DSM-based MERGE template cache
  has been written, informing the v0.16.0 implementation plan.
- **TRUNCATE handling verified** — TRUNCATE on source tables in trigger CDC
  mode already triggers a FULL refresh; this is now documented.
- **JOIN key-change fix verified** — the v0.14.0 correctness fix for
  simultaneous JOIN key updates and DELETEs has been verified working and
  the former known-limitation note replaced with a description of the fix.

### Bug Fixes

- Fixed a panic in the TUI when deserializing health-check data that
  returned 64-bit integers where 32-bit was expected.
- Fixed spurious "Error: db error" toasts in the TUI Detail view —
  background queries now degrade silently instead of surfacing transient
  errors.
- Fixed incorrect integer type annotations in two E2E tests for IMMEDIATE
  mode diamond topologies.

---

## [0.14.0] — Tiered Scheduling, Diagnostics & TUI

0.14.0 is the **Tiered Scheduling, Diagnostics & TUI** release. It gives you
fine-grained control over how often each stream table refreshes, adds tools
that recommend the best refresh strategy for your workload, introduces a
full-screen terminal dashboard for managing stream tables without SQL, and
includes important security and reliability fixes.

### Terminal Dashboard (TUI)

A new `pgtrickle` command-line tool lets you monitor and manage stream tables
from a terminal — no SQL required. Run it with no arguments to launch a
live-updating full-screen dashboard (think `htop` for stream tables), or use
one-shot subcommands like `pgtrickle list`, `pgtrickle status`, or
`pgtrickle refresh` for scripting and CI.

The interactive dashboard includes:

- **Live overview** — stream table statuses, refresh timing, and issue counts
  update every 2 seconds, with color-coded health indicators.
- **Dependency graph** — see how stream tables relate to each other in an
  ASCII tree view.
- **Diagnostics** — view refresh mode recommendations with confidence levels.
- **CDC health** — monitor change buffer sizes with warnings when they grow
  too large.
- **Alert feed** — real-time notification display with severity levels.
- **Issue detection** — automatically spots broken dependency chains, growing
  buffers, blown fuses, and stale data, with a persistent badge showing the
  issue count from any view.
- **Watch mode** — `pgtrickle watch` provides continuous non-interactive
  output suitable for log aggregation.
- **Output formats** — all CLI subcommands support `--format json`,
  `--format csv`, and human-readable table output.

See [docs/TUI.md](docs/TUI.md) for the full user guide.

### Tiered Refresh Scheduling

Stream tables can now be assigned to refresh tiers — **hot**, **warm**,
**cold**, or **frozen** — to control how frequently they refresh:

- **Hot** (default) — refreshes at the configured interval.
- **Warm** — refreshes at 2× the interval.
- **Cold** — refreshes at 10× the interval, ideal for infrequently accessed
  reports.
- **Frozen** — pauses automatic refresh entirely until promoted back.

Assign a tier with
`ALTER STREAM TABLE ... SET (tier = 'cold')`. A NOTICE is emitted when
demoting from Hot to Cold or Frozen so operators are aware of the change in
refresh frequency.

### Smarter Refresh Recommendations

Two new diagnostic functions help you choose the most efficient refresh
strategy for each stream table:

- **`pgtrickle.recommend_refresh_mode(name)`** — analyzes seven workload
  signals (change frequency, timing history, query complexity, table size,
  index coverage, and latency patterns) and recommends FULL or DIFFERENTIAL
  mode with a confidence level and plain-language explanation. Useful when
  you're unsure which mode will be faster for a particular table.

- **`pgtrickle.refresh_efficiency(name)`** — shows per-table refresh
  performance: how many FULL vs. DIFFERENTIAL refreshes have run, average
  timing for each, and the speedup factor. Good for monitoring dashboards
  and alerting.

A new tutorial — [Tuning Refresh Mode](docs/tutorials/tuning-refresh-mode.md)
— walks through the process step by step.

### Reduced Write Overhead with UNLOGGED Buffers

Enable `pg_trickle.unlogged_buffers = true` and newly created change buffer
tables will skip write-ahead logging, reducing WAL volume by roughly 30%.
This is ideal for workloads where you can tolerate a full re-sync after a
crash (the extension detects the crash and re-syncs automatically).

A utility function — `pgtrickle.convert_buffers_to_unlogged()` — converts
existing buffers in one call. Run it during a maintenance window since it
briefly locks each buffer table.

### Instant Error Detection

Previously, when a stream table's refresh hit a permanent error (for example,
a function that doesn't exist for the column type), the extension would retry
several times before giving up. Now it recognizes permanent errors immediately,
sets the stream table status to **ERROR** with a clear error message, and
stops retrying. You can see the error at a glance in the `stream_tables_info`
view or the TUI dashboard, and fix it by altering the stream table's query.

### Security Hardening

- **CDC trigger functions now use `SECURITY DEFINER`** — change-data-capture
  trigger functions run with the privileges of the extension owner rather
  than the current user, preventing privilege escalation through modified
  search paths.
- **Explicit `SET search_path`** — all CDC trigger functions now set
  `search_path` to `pgtrickle_changes, pg_catalog` to prevent search-path
  manipulation attacks.

### Other Improvements

- **Export definitions** — `pgtrickle.export_definition(name)` exports a
  stream table's full configuration as reproducible SQL (`DROP` + `CREATE` +
  `ALTER` statements), making it easy to version-control or migrate stream
  table definitions between environments.

- **Creation-time warnings** — when creating a stream table with aggregates
  like `MIN`, `MAX`, or `STRING_AGG` in DIFFERENTIAL mode, a warning now
  suggests that FULL or AUTO mode may be more efficient. For algebraic
  aggregates (`SUM`/`COUNT`/`AVG`), the warning only appears when the
  estimated number of groups is below a configurable threshold.

- **Simplified settings** — the `merge_planner_hints` and `merge_work_mem_mb`
  settings have been consolidated into a single `planner_aggressive` switch.
  The old setting names still work but are ignored in favor of the new one.

- **GHCR Docker image** — a multi-architecture Docker image
  (`ghcr.io/trickle-labs/pg_trickle`) with PostgreSQL 18.3 and pg_trickle
  pre-installed is now published automatically on each release.

- **Pre-deployment checklist** — new [PRE_DEPLOYMENT.md](docs/PRE_DEPLOYMENT.md)
  with a 10-point checklist for production deployments.

- **Best-practice patterns guide** — new [PATTERNS.md](docs/PATTERNS.md) with
  6 common patterns: Bronze/Silver/Gold materialization, event sourcing,
  slowly-changing dimensions, high-fan-out topology, real-time dashboards,
  and tiered refresh strategies.

- **Keyless dedup fix** — replaced `MAX(col)` with `array_agg(col)[1]` for
  deduplicating keyless scan results, which is more correct for non-orderable
  types.

### Bug Fixes

- **ST-on-ST differential refresh** — manually refreshing a stream table that
  reads from another stream table now uses true incremental (DIFFERENTIAL)
  refresh instead of falling back to a full re-scan. This matches the behavior
  of the automatic scheduler and is significantly faster for large tables.

- **Staleness tracking** — the staleness indicator now uses the actual last
  refresh time instead of an internal data timestamp, making the
  `pg_stat_stream_tables` view more accurate.

### Testing & Reliability

- **Soak test** — a new long-running stability test validates zero worker
  crashes, zero ERROR states, and stable memory usage under sustained mixed
  workload (configurable duration, default 10 minutes).

- **Multi-database isolation test** — verifies that two databases in the same
  PostgreSQL cluster run pg_trickle independently without interference.

- **140 TUI tests** — comprehensive unit, snapshot, and interaction tests for
  the terminal dashboard.

- **23 mixed-object E2E tests** — validates stream tables alongside regular
  PostgreSQL views, materialized views, and other objects.

- **Scheduler race fixes** — eliminated flaky test failures caused by
  scheduler timing races and GUC leak between tests.

### New SQL Functions

| Function | Purpose |
|----------|---------|
| `pgtrickle.recommend_refresh_mode(name)` | Workload-based refresh mode recommendation |
| `pgtrickle.refresh_efficiency(name)` | Per-table refresh performance metrics |
| `pgtrickle.export_definition(name)` | Export stream table as reproducible DDL |
| `pgtrickle.convert_buffers_to_unlogged()` | Convert logged change buffers to UNLOGGED |

### New Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `pg_trickle.planner_aggressive` | `true` | Consolidated switch for MERGE planner hints |
| `pg_trickle.unlogged_buffers` | `false` | Create new change buffers as UNLOGGED |
| `pg_trickle.agg_diff_cardinality_threshold` | `1000` | Warn about DIFFERENTIAL mode below this group count |

### Deprecated

- **`pg_trickle.merge_planner_hints`** — Use `pg_trickle.planner_aggressive`
  instead. Still accepted but ignored at runtime.
- **`pg_trickle.merge_work_mem_mb`** — Same; use `planner_aggressive` instead.

### Upgrading

Run `ALTER EXTENSION pg_trickle UPDATE;` after installing the new binaries.
The upgrade adds new catalog columns, functions, and the TUI workspace member.
No breaking changes — everything from v0.13.0 continues to work. See
[UPGRADING.md](docs/UPGRADING.md) for details.

---

## [0.13.0] — Scalability Foundations & Full TPC-H Coverage

0.13.0 is the **Scalability Foundations** release. It makes pg_trickle handle
large tables, complex queries, and multi-tenant deployments much more
efficiently — and it achieves a major milestone: **all 22 TPC-H benchmark
queries now run in incremental (DIFFERENTIAL) mode**, meaning the engine no
longer needs to fall back to slow full-refresh for any standard analytical
query pattern.

### Smarter Change Detection for Wide Tables

When you UPDATE a few columns in a large table — say, changing a `status`
column in a 60-column table — pg_trickle used to treat every column as
potentially changed, doing extra work to keep all downstream views up to date.

Now it knows the difference. Columns used in GROUP BY, JOIN, or WHERE clauses
are "key columns"; everything else is a "value column." When only value columns
change, the engine takes a shortcut: it sends a single correction row instead
of a full delete-and-reinsert pair. For wide-table workloads, this can cut the
volume of data processed by 50% or more.

### Shared Change Buffers

If you have several stream tables watching the same source table, each one used
to maintain its own private copy of the change log. That's wasteful. Now they
share a single change buffer per source, and each consumer simply tracks how
far it has read. The slowest reader protects the buffer for everyone.

You can see how this is working with the new `pgtrickle.shared_buffer_stats()`
function — it shows each buffer, who's reading from it, how many rows are
queued, and whether it's been automatically partitioned for performance.

### Automatic Buffer Partitioning

Set `pg_trickle.buffer_partitioning = 'auto'` and pg_trickle will start with
simple, unpartitioned change buffers. If a buffer starts accumulating a lot of
rows (high-throughput sources), it automatically converts to a partitioned
layout where old data can be removed almost instantly instead of deleting rows
one by one.

### More Partitioning Options for Stream Tables

Building on the RANGE partitioning added in v0.11.0, you can now partition
stream tables in three additional ways:

- **Multi-column keys** — partition by a combination of columns
  (`partition_by='region,year'`)
- **LIST partitioning** — for low-cardinality columns like `status` or `type`
  (`partition_by='LIST:status'`)
- **HASH partitioning** — for even distribution across a fixed number of
  partitions (`partition_by='HASH:customer_id:8'`)

You can also change the partition key of an existing stream table at runtime
with `alter_stream_table(partition_by => ...)` — data is preserved
automatically. If rows land in the default (catch-all) partition, a WARNING
is emitted to prompt you to add explicit partitions.

### All 22 TPC-H Queries Now Run Incrementally

The DVM (differential view maintenance) engine received its most significant
set of improvements yet, targeting the complex multi-table join patterns found
in standard analytical benchmarks:

- **Smarter pre-image lookups** — instead of reconstructing what the data
  looked like before a change by subtracting deltas (expensive for large
  tables), the engine now uses targeted index lookups that only touch the rows
  that actually changed.
- **Predicate pushdown** — WHERE conditions from the original query are now
  pushed into the delta computation, preventing unnecessary cross-products
  in multi-table joins.
- **Deep-join optimizations** — queries joining 5+ tables get automatic planner
  hints (more memory, smarter join strategies) to avoid spilling to disk.
- **Scan-count-aware strategy selector** — queries that exceed configurable
  join complexity or delta volume thresholds automatically fall back to full
  refresh on a per-query basis rather than failing.

The result: all 22 TPC-H queries pass at SF=0.01 in DIFFERENTIAL mode
with zero drift across 3 refresh cycles. The `DIFFERENTIAL_SKIP_ALLOWLIST`
(queries that previously required full refresh) is now empty.

### Refresh Performance Inspection Tools

Two new functions help you understand what pg_trickle is doing under the hood:

- **`pgtrickle.explain_delta(name, format)`** — shows you the query plan for
  the auto-generated delta SQL, the same way `EXPLAIN` works for regular
  queries. Available in text, JSON, XML, or YAML format.
- **`pgtrickle.dedup_stats()`** — reports how often concurrent writes produce
  duplicate entries that need pre-processing before the MERGE step.

### Multi-Tenant Worker Quotas

New setting: **`pg_trickle.per_database_worker_quota`** — if you run many
databases on one PostgreSQL cluster, this prevents a busy database from
monopolizing all the refresh workers. Workers are assigned by priority
(immediate-mode tables first, then hot, warm, and cold), with burst capacity
up to 150% when other databases are idle.

### TPC-H Benchmark Harness

You can now measure refresh performance across all 22 TPC-H queries in a
structured way. Run `just bench-tpch` to get per-query timing, FULL vs.
DIFFERENTIAL comparison, and P95 latency numbers. Five synthetic benchmarks
(`q01`, `q05`, `q08`, `q18`, `q21`) also measure the pure Rust delta-SQL
generation time without needing a database.

### Broader SQL Support

- **`IS JSON` predicates** (PG 16+) — expressions like
  `expr IS JSON OBJECT` now work in incremental mode.
- **SQL/JSON constructors** (PG 16+) — `JSON_OBJECT(...)`, `JSON_ARRAY(...)`,
  `JSON_OBJECTAGG(...)`, and `JSON_ARRAYAGG(...)` are now accepted.
- **Recursive CTEs** — recursive queries with non-monotone operators (like
  `EXCEPT`) correctly fall back to full refresh instead of producing
  wrong results.

### dbt Integration Updates

If you use dbt-pgtrickle, you can now set partitioning and fuse options
directly from dbt model config:

- `{{ config(partition_by='customer_id') }}` for partitioned stream tables
- `{{ config(fuse='auto', fuse_ceiling=100000, fuse_sensitivity=3) }}` for
  circuit-breaker protection

### Bug Fixes

- **Scheduler cascade fix** — stream tables downstream of FULL-mode upstream
  tables now detect changes correctly via a `last_refresh_at` fallback,
  preventing stale data in chains where the upstream uses full refresh.
- **SUM(CASE WHEN ...) drift fix** — aggregate expressions using CASE were
  occasionally producing slightly wrong incremental results; these are now
  correctly detected and processed via a group rescan.
- **Duplicate column DDL fix** — removed a duplicate column definition in the
  `pgt_stream_tables` DDL that could cause issues on fresh installs.

### Testing Improvements

- New regression test suite targeting 9 structural weaknesses: join multi-cycle
  correctness (7 tests), differential-equals-full equivalence (11 tests), DVM
  operator execution, failure recovery, and MERGE template unit tests.
- E2E test infrastructure now uses template databases, cutting per-test setup
  time significantly.

### New SQL Functions

| Function | Purpose |
|----------|---------|
| `pgtrickle.explain_delta(name, format)` | Show the query plan for the delta SQL |
| `pgtrickle.dedup_stats()` | MERGE deduplication frequency counters |
| `pgtrickle.shared_buffer_stats()` | Per-source change buffer status |
| `pgtrickle.explain_refresh_mode(name)` | Why a stream table uses its current refresh mode |
| `pgtrickle.reset_fuse(name)` | Reset a blown circuit-breaker fuse |
| `pgtrickle.fuse_status()` | Fuse state across all stream tables |

### New Catalog Columns

Ten new columns on `pgtrickle.pgt_stream_tables`:

| Column | Purpose |
|--------|---------|
| `effective_refresh_mode` | The actual refresh mode after AUTO resolution |
| `fuse_mode` | Circuit-breaker configuration (off / auto / manual) |
| `fuse_state` | Current fuse state (armed / blown) |
| `fuse_ceiling` | Maximum change count before fuse blows |
| `fuse_sensitivity` | Consecutive cycles above ceiling before triggering |
| `blown_at` | When the fuse last blew |
| `blow_reason` | Why the fuse blew |
| `st_partition_key` | Partition key specification |
| `max_differential_joins` | Maximum join count for differential mode |
| `max_delta_fraction` | Maximum delta-to-table ratio for differential mode |

### Upgrading

Run `ALTER EXTENSION pg_trickle UPDATE;` after installing the new binaries.
All new columns and functions are added automatically. No breaking changes —
everything from v0.12.0 continues to work as before. See
[UPGRADING.md](docs/UPGRADING.md) for details.

---

## [0.12.0] — Join Correctness, Diagnostics & Reliability

0.12.0 is a correctness, reliability, and developer-experience release built on
top of 0.11.0's major new features. It closes the last known wrong-answer bugs
for complex join queries, adds tools to help you understand and debug stream
table behavior, hardens the scheduler against several edge cases that could
cause stale data or crashes, and backs it all with thousands of new
automatically generated tests.

### Stale Rows Fixed in Stream-Table Chains

**What was the problem?** When a stream table (B) reads from another stream
table (A), each change in A is recorded as a small "what changed" entry — a
row added or removed. But the identity key used for those entries was computed
differently inside the change buffer than it was inside B's own storage. As a
result, when A changed via an upstream UPDATE, B's refresh could silently fail
to delete the old version of a row, leaving a stale duplicate.

**What changed?** The change buffer now computes row identity the same way B
does — using a hash of all the data columns rather than the upstream source's
primary key. Stale rows after UPDATE no longer appear in stream-table chains.
This bug was found and confirmed by the new property-based test suite (see
below).

### Phantom Rows Fixed for Complex Joins (TPC-H Q7 / Q8 / Q9)

**What was the problem?** When a stream table's query joins three or more
tables together and rows are deleted from more than one join side at the same
time, the incremental engine could silently drop the correction — leaving rows
in the stream table that should have been removed.

This affected TPC-H queries Q7, Q8, and Q9 (which all involve deep join
trees), and any user query with a similar multi-table join structure. A
temporary workaround (falling back to full refresh for wide joins) was in
place since v0.11.0 and has now been lifted.

**What changed?** The incremental engine now takes an individual "before
snapshot" for each leaf table in the join tree — each one cheaply computed
from a single-table comparison — and re-joins them after the delete. This
avoids writing multi-gigabyte temp files to disk (the root cause of the
original workaround) and eliminates the phantom-row bug entirely. Q7, Q8, and
Q9 now run in differential mode without any workarounds.

### Type Errors Fixed in Parallel Refresh Chains

**What was the problem?** When a chain of stream tables is fused into a single
execution unit for efficiency (the "bypass" optimisation added in v0.11.0),
the internal bypass table used `text` for every column regardless of the
actual column type. This caused an `operator does not exist: text > integer`
error whenever a downstream stream table had a type-sensitive WHERE clause
(e.g. `WHERE amount > 100`), making the parallel worker tests fail silently
across all topologies that included a fused chain.

**What changed?** Bypass tables now use the real column types. The six
parallel-worker benchmark tests now complete in 9–26 seconds rather than
timing out after 120 seconds.

### Scheduler Fixes for Diamond and ST-on-ST Topologies

Two scheduler bugs that caused incorrect refresh behavior with complex
dependency graphs were fixed:

- **Diamond timeout.** In a diamond topology (A → B, A → C, B+C → D), the L1
  arm stream tables (B and C) were created with a 1-minute fixed interval
  rather than a calculated schedule. This meant D never received updates within
  the test window. The scheduler also had a bug loading stream table records
  by ID that caused silent failures in parallel worker paths. Both are fixed.

- **ST-on-ST parallel workers.** When an upstream stream table changed, the
  parallel worker paths (singleton, atomic group, immediate closure, fused
  chain) were not forcing a full refresh on downstream stream tables the way
  the main scheduler loop did. This could leave downstream tables stale. The
  fix ensures all parallel paths treat upstream stream-table changes the same
  way.

### Four New Diagnostic Functions

When stream table behavior is unexpected — wrong refresh mode, a query being
rewritten in a surprising way, persistent errors — it previously required
reading server logs or source code to understand why. Four new SQL functions
expose that internal state directly in queries:

- **`pgtrickle.explain_query_rewrite(query TEXT)`** — shows exactly how
  pg_trickle rewrites your query for incremental refresh: which operators were
  applied, how delta keys are injected, and how aggregates are classified.
  Useful for understanding why a query got a particular refresh mode.

- **`pgtrickle.diagnose_errors(name TEXT)`** — shows the last 5 errors for a
  stream table, each classified by type (correctness, performance,
  configuration, infrastructure) with a suggested fix.

- **`pgtrickle.list_auxiliary_columns(name TEXT)`** — lists the internal
  `__pgt_*` columns that pg_trickle injects into a stream table's query plan,
  with an explanation of each one's purpose. Helpful when `SELECT *` returns
  unexpected extra columns.

- **`pgtrickle.validate_query(query TEXT)`** — analyses a SQL query and
  reports which refresh mode it would get, which SQL constructs were detected,
  and any warnings — all without creating a stream table.

### Multi-Column `IN (subquery)` Now Gives a Clear Error

**What was the problem?** A query like `WHERE (col_a, col_b) IN (SELECT x, y
FROM …)` passed validation but produced silently wrong results — the engine
was only matching on the first column and ignoring the second.

**What changed?** This construct is now detected at stream table creation time
and rejected with a clear error message that recommends rewriting it as
`EXISTS (SELECT 1 FROM … WHERE col_a = x AND col_b = y)`.

### IMMEDIATE Mode Proven Correct Under High Concurrency

IMMEDIATE mode (where the stream table updates inside the same transaction as
the source table change) now has a dedicated concurrency stress test: 100–120
concurrent transactions firing simultaneously against the same source table,
across five scenarios (all inserts, all updates to distinct rows, all updates
to the same row, all deletes, and a mixed workload). Zero lost updates, zero
phantom rows, and no deadlocks were observed in any run.

### Protection Against Pathological Queries

A new guard prevents a particularly deep or convoluted query from consuming
all available stack space and crashing the database backend. When the query
analyser recurses more than 64 levels deep (configurable via
`pg_trickle.max_parse_depth`), it now returns a clear `QueryTooComplex` error
instead of crashing.

### Tiered Scheduling Now On By Default

The tiered scheduling feature — which automatically slows down cold
(infrequently-read) stream tables and speeds up hot ones — is now enabled by
default. In large deployments this reduces the scheduler's CPU usage
significantly. Stream tables you query often continue refreshing at full speed.
Stream tables that nobody has read recently back off gracefully.

If you rely on all stream tables refreshing at the same rate regardless of
read frequency, set `pg_trickle.tiered_scheduling = off`.

### Thousands of Automatically Generated Tests

Two new automated testing systems were added to complement the hand-written
test suite:

- **Property-based tests** — the test framework automatically generates
  thousands of random DAG shapes, schedule combinations, and edge cases and
  checks that the scheduler's ordering guarantees hold for all of them. If any
  configuration would cause a table to refresh in the wrong order or get
  spuriously suspended, these tests catch it.

- **SQLancer fuzzing** — SQLancer generates random SQL queries and checks
  that pg_trickle's incremental result matches the result of running the same
  query directly in PostgreSQL. Any mismatch is automatically saved as a
  permanent regression test. A weekly CI job runs this continuously. At time
  of release, zero mismatches have been found.

### CDC Write-Side Benchmark Published

A new benchmark suite measures the overhead that pg_trickle's change capture
triggers add to your write workload. Results across five scenarios (single-row
INSERT, bulk INSERT, bulk UPDATE, bulk DELETE, concurrent writers) are
published in [docs/BENCHMARK.md](docs/BENCHMARK.md). Use these numbers to
estimate the impact before deploying pg_trickle on a write-heavy table.

### MERGE Template Validation at Test Startup

The SQL templates that pg_trickle generates for applying incremental changes
(the MERGE statements) are now validated with an `EXPLAIN` dry-run at every
test startup. If a code change accidentally produces a malformed MERGE
template, the tests catch it before any data is processed — rather than
manifesting as a cryptic runtime error.

---

## [0.11.0] — Event-Driven Latency, Chain IVM & Observability Stack

This is the biggest release since the initial launch. The headline features are
**34× lower latency** for real-time workloads, **stream-table chains that now
refresh incrementally** (no more forced full recomputation when one stream table
feeds another), **declarative partitioning** to cut I/O on large tables by up to
100×, a **ready-to-use Prometheus and Grafana monitoring stack**, and a **circuit
breaker** to protect production databases from runaway change bursts.

### 34× Lower Latency — Changes Arrive Instantly

**Previously**, the background worker woke up on a fixed timer every ~500 ms to
check for new data, even when nothing had changed. Every change had to wait up to
half a second in the change buffer before being processed.

**Now**, when a source table is modified, the change capture trigger immediately
wakes the background worker via a PostgreSQL notification channel. The worker
starts processing within ~15 ms of the write committing — a 34× improvement for
low-volume workloads. Under heavy DML, a 10 ms debounce window coalesces rapid
notifications so the worker isn't flooded.

Event-driven wake is on by default. You can turn it off
(`pg_trickle.event_driven_wake = off`) to revert to poll-based wake, and you can
tune the debounce window with `pg_trickle.wake_debounce_ms` (default `10`).

### Stream-Table-to-Stream-Table Chains Now Refresh Incrementally

**Previously**, when stream table B's query read from stream table A, pg_trickle
had to do a full recomputation of B every time A changed — even if only a few
rows in A actually changed. For long chains (A → B → C → D), every hop was a
full re-scan.

**Now**, stream tables can read from other stream tables incrementally. When A
refreshes, the rows it added and removed are recorded in a change buffer just like
a base table. B wakes up, reads only the changed rows from A, and applies a delta
— not a full recomputation. Even when A does a full refresh (e.g. because its
query does not support differential mode), a before/after snapshot diff is
captured automatically so downstream tables still receive a small insert/delete
delta rather than cascading full refreshes through the chain.

### Declaratively Partitioned Stream Tables

Stream tables can now be declared with a partition key:

```sql
SELECT create_stream_table(
  'monthly_sales',
  $$ SELECT month, region, SUM(amount) FROM orders GROUP BY 1, 2 $$,
  partition_by => 'month'
);
```

pg_trickle creates a range-partitioned storage table and, when refreshing,
automatically restricts the MERGE operation to only the partitions that contain
changed rows. For large tables where changes touch only 2–3 out of 100 monthly
partitions, this can reduce the MERGE I/O from 10 million rows to ~100,000 — a
100× improvement.

### Ready-to-Use Prometheus and Grafana Monitoring

A complete observability stack is now included in the `monitoring/` directory:

- **`monitoring/prometheus/pg_trickle_queries.yml`** — drop-in configuration for
  `postgres_exporter` that exports 14 metrics covering refresh performance,
  CDC buffer sizes, staleness, error rates, and per-table status.
- **`monitoring/prometheus/alerts.yml`** — 8 alerting rules that page you when a
  stream table goes stale (> 5 min), starts error-looping (≥ 3 consecutive
  failures), is suspended, or when the CDC buffer exceeds 1 GB.
- **`monitoring/grafana/dashboards/pg_trickle_overview.json`** — a pre-built
  Grafana dashboard with six sections: cluster overview, refresh latency
  time-series, staleness heatmap, CDC lag, per-table drill-down, and scheduler
  health.
- **`monitoring/docker-compose.yml`** — brings up PostgreSQL + pg_trickle +
  postgres_exporter + Prometheus + Grafana with one command
  (`docker compose up`). Grafana opens at http://localhost:3000; the dashboard
  shows live metrics generated by a seed workload of stream tables continuously
  refreshing synthetic order and product data (see `monitoring/init/01_demo.sql`).

No code changes are needed to use this stack with an existing pg_trickle
installation.

### Circuit Breaker (Fuse) — Protection Against Runaway Change Bursts

A new circuit breaker mechanism halts refresh for a stream table when its pending
change count exceeds a configurable threshold. This protects your database from
accidental mass-delete scripts, runaway migrations, or data imports that would
otherwise trigger an unexpectedly large and expensive refresh operation.

When the fuse blows, pg_trickle sends a `pgtrickle_alert` PostgreSQL notification
that you can subscribe to, and suspends the affected stream table. You then choose
how to recover using `reset_fuse()`:

- `reset_fuse(name, action => 'apply')` — process the backlog normally (default).
- `reset_fuse(name, action => 'reinitialize')` — clear the change buffer and
  repopulate the stream table from scratch.
- `reset_fuse(name, action => 'skip_changes')` — discard the pending changes and
  resume without reprocessing them.

Configure per-table with `alter_stream_table(fuse => 'on', fuse_ceiling => 10000)`
or set a global default with `pg_trickle.fuse_default_ceiling`. Use
`fuse_status()` to inspect the blown/active state of all stream tables at once.

### Wider Column Bitmask — No More 63-Column Limit

pg_trickle's change capture tracks which columns were actually modified in each
row so that stream tables that reference only a subset of columns can ignore
irrelevant updates. Previously, this optimization silently stopped working for
source tables with more than 63 columns — all updates were treated as touching
every column.

The bitmask has been extended from a 64-bit integer to an arbitrary-width
PostgreSQL `VARBIT` value, removing the column count cap entirely. Existing
deployments are migrated automatically (the old column value becomes `NULL`,
which the filter treats conservatively — no rows are silently dropped). Tables
with fewer than 64 columns are unaffected at the data level.

### Per-Database Worker Quotas

In multi-tenant environments where multiple databases share a single PostgreSQL
instance, all stream-table refresh workers previously competed for the same
concurrency pool. A single busy database could crowd out others.

A new GUC `pg_trickle.per_database_worker_quota` sets a soft concurrency limit
per database. When the rest of the cluster is lightly loaded (< 80% of available
capacity in use), a database can burst to 150% of its quota. When the cluster is
busy, each database is held to its base quota.

Refresh work is also now dispatched in priority order:
IMMEDIATE mode tables → atomic diamond groups → singleton tables.

### DAG Scheduling Performance

For deployments with chains of stream tables (A → B → C), several improvements
reduce end-to-end propagation latency:

- **Fused single-consumer chains.** When a stream table chain has exactly one
  downstream consumer at each hop, the scheduler fuses the chain into a single
  execution unit in one background worker. Intermediate deltas are stored in
  temporary in-memory tables instead of persistent change buffers, eliminating
  the WAL writes, index maintenance, and cleanup that would normally occur at
  each hop.
- **Batch coalescing.** Before a downstream table reads from an upstream change
  buffer, redundant insert/delete pairs for the same row are cancelled out. This
  prevents rapid-fire upstream refreshes from accumulating duplicate work for
  downstream tables.
- **Adaptive dispatch polling.** The parallel dispatch loop now backs off
  exponentially (20 ms → 200 ms) instead of using a fixed 200 ms poll, and
  resets to 20 ms as soon as any worker finishes. Cheap refreshes no longer
  wait a full 200 ms for the next tick.
- **Delta amplification warnings.** When a differential refresh produces many
  more output rows than input rows (default threshold: 100×), a `WARNING` is
  emitted with the table name, input and output counts, and a tuning hint.
  `explain_st()` now exposes `amplification_stats` from the last 20 refreshes.

### Smarter Diagnostics and Warnings

Several improvements to make problems visible earlier and easier to diagnose:

- **Know which refresh mode is actually running.** When a stream table is set to
  `AUTO`, pg_trickle now records which mode it actually chose at each refresh
  (`DIFFERENTIAL`, `FULL`, etc.) in a new `effective_refresh_mode` column on
  `pgt_stream_tables`. A new `explain_refresh_mode(name)` function reports the
  configured mode, the actual mode used, and the reason for any downgrade — all
  in one query.
- **Clearer warning when a stream table falls back to full refresh.** If a stream
  table cannot use differential mode, pg_trickle now emits a `WARNING` message
  naming the affected table and the reason. Previously this happened silently.
- **Warning when using aggregates that require full group rescans.** Aggregate
  functions like `STRING_AGG`, `ARRAY_AGG`, and `JSON_AGG` require re-aggregating
  the entire group whenever any member changes. pg_trickle now warns at stream
  table creation time when such aggregates are used in `DIFFERENTIAL` mode, and
  `explain_st()` classifies each aggregate's maintenance strategy
  (incremental, auxiliary-state, or group-rescan) so you can understand the cost.
- **Better error messages.** Errors for unsupported query patterns, cycle
  detection, upstream schema changes, and query parse failures now include a
  `DETAIL` field explaining what went wrong and a `HINT` field suggesting how to
  fix it.
- **Invalid parameter combinations are rejected at creation time.** For example,
  using `diamond_schedule_policy='slowest'` without `diamond_consistency='atomic'`
  now produces a clear error at `create_stream_table` / `alter_stream_table` time
  rather than silently doing the wrong thing at refresh time.
- **TopK queries validate their metadata on every refresh.** Stream tables defined
  with `ORDER BY ... LIMIT N` now recheck that the stored LIMIT/OFFSET metadata
  still matches the actual query on each refresh. On mismatch, they fall back to
  a full refresh with a `WARNING` rather than silently producing wrong results.

### Safety and Reliability Improvements

- **No more crashes from schema changes.** If a source table's schema changes
  while a refresh is running (e.g. a column is dropped), pg_trickle now catches
  the error, emits a structured `WARNING` with the table name and error details,
  and continues refreshing all other stream tables. The scheduler never crashes
  due to an individual table's error.
- **Failure injection tests.** New end-to-end tests deliberately drop columns and
  tables mid-refresh to verify that the scheduler stays alive and other stream
  tables continue processing correctly.
- **Safer defaults.** Three default settings have been updated to reflect
  production-safe behavior:
  - `parallel_refresh_mode` now defaults to `'on'` (was `'off'`). Parallel
    refresh has been stable for several releases; serial mode is now opt-in.
  - `block_source_ddl` now defaults to `true`. Accidental `ALTER TABLE` on a
    source table while a stream table depends on it is now blocked by default,
    with clear instructions on how to temporarily disable the guard if needed.
  - The invalidation ring capacity has been doubled from 32 to 128 slots,
    reducing the risk of invalidation events being silently discarded under
    rapid DDL.

### Getting Started Guide Restructured

`docs/GETTING_STARTED.md` has been reorganised into five progressive chapters:

1. **Hello World** — create your first stream table and watch it update.
2. **Joins, Aggregates & Chains** — multi-table dependencies and DAG patterns.
3. **Scheduling & Backpressure** — controlling refresh frequency and auto-backoff.
4. **Monitoring In Depth** — using the five key diagnostic functions and the
   Prometheus/Grafana stack.
5. **Advanced Topics** — FUSE circuit breaker, partitioned stream tables,
   IMMEDIATE (in-transaction) IVM, and multi-tenant worker quotas.

### TPC-H Correctness Gate Added to CI

Five queries derived from the TPC-H benchmark — covering single-table
GROUP BY, filter-aggregate, CASE WHEN inside SUM, a three-way join, and LEFT
OUTER JOIN with GROUP BY — now run in DIFFERENTIAL mode on every push to `main`
and daily. Any correctness mismatch between pg_trickle's incremental output and
plain PostgreSQL execution fails the CI build automatically.

### Docker Hub Image Improvements

The `Dockerfile.hub` image that is published to Docker Hub has been expanded
with a comprehensive set of GUC defaults fine-tuned for production use. A new
`just build-hub-image` recipe builds the image locally for testing.

### Bug Fixes

- **Scheduler crash after event-driven wake was enabled.** The background worker
  crashed immediately after startup when `event_driven_wake = on` (the default)
  because the `LISTEN` command was being issued outside of a transaction. Fixed
  by issuing `LISTEN` inside a short-lived SPI transaction at startup.
  (#296)
- **Spurious full refresh for non-recursive CTEs.** Stream tables containing
  `WITH` clauses that were not recursive (`WITH foo AS (SELECT ...)`) were being
  incorrectly forced to FULL refresh mode. Only truly recursive CTEs
  (`WITH RECURSIVE`) require this. Non-recursive CTEs now correctly use
  differential mode. (#298)
- **`DISTINCT ON` inside a CTE body caused a parse error.** When a stream table's
  defining query contained a `WITH` clause whose body used `DISTINCT ON (...)`,
  the DVM query analyser failed with a parse error. The `DISTINCT ON` clause is
  now rewritten before analysis so it no longer interferes. (#300)
- **Full-refresh fallback warning now names the affected table.** When pg_trickle
  falls back from differential to full refresh, the emitted `WARNING` now
  includes the stream table name and the reason, making it straightforward to
  identify which table you need to investigate. (#301)

---

## [0.10.0] — Cloud Deployment, PgBouncer & Query Engine Correctness

The headline features of 0.10.0 are **cloud deployment compatibility**, **query
engine correctness**, **refresh performance**, and **improved developer
experience for `auto_backoff`**. pg_trickle now works reliably
behind PgBouncer — the connection pooler used by default on Supabase, Railway,
Neon, and other managed PostgreSQL platforms. A broad set of correctness issues
in the incremental query engine are fixed. And several performance optimizations
cut refresh time for large tables and busy deployments.

### `auto_backoff` Is Now Much Friendlier on Developer Machines

When `pg_trickle.auto_backoff = true` is enabled, the scheduler automatically
slows down stream tables whose refresh cost exceeds their schedule budget — a
good safeguard in production. This release makes the feature safe to use
alongside short schedules (e.g. `'1s'`) in developer and CI environments:

- **Trigger threshold raised from 80 % → 95 %.** Backoff now only activates
  when a refresh consumes more than 95 % of the schedule window. A 900 ms
  refresh on a 1-second schedule (90 %) used to trigger backoff; it no longer
  does. EC-11 operator alerting continues to fire at 80 % (unchanged)
  so you still get an early warning before the scheduler is actually stuck.

- **Maximum slowdown reduced from 64× → 8×.** In the worst case, a stream
  table's effective refresh interval is now capped at 8× its configured
  schedule (e.g. 8 seconds for a `'1s'` table) instead of 64 seconds. The
  cap self-heals immediately: a single on-time refresh resets the factor to 1×.

- **Backoff events now emit `WARNING` instead of `INFO`.** When the scheduler
  stretches or resets a stream table's effective interval, you will see a
  `WARNING` message in your PostgreSQL client, including the new effective
  interval — rather than a silent slowdown with no explanation.

- **`auto_backoff` now defaults to `on`.** With the above improvements in place,
  the feature is safe in all environments. New installations get CPU runaway
  protection out of the box. To restore the old opt-in behaviour, set
  `pg_trickle.auto_backoff = off`.

### Works Behind PgBouncer

PgBouncer is the most popular PostgreSQL connection pooler. In "transaction
mode" — the default setting on most cloud PostgreSQL platforms — it hands a
fresh database connection to every transaction, which breaks anything that
assumes the same connection stays open between calls (session locks, prepared
statements). pg_trickle previously relied on both. This release makes pg_trickle
work correctly in such deployments.

- **Session locks replaced with row-level locking.** The background scheduler
  now acquires a short-lived row-level lock on each stream table's catalog entry
  instead of a session-level advisory lock. Row-level locks are released
  automatically at transaction end — exactly what PgBouncer transaction mode
  requires. If a concurrent refresh is already running for a given stream table,
  the scheduler skips that cycle and retries, rather than blocking.

- **New `pooler_compatibility_mode` option per stream table.** Setting
  `pooler_compatibility_mode => true` when creating or altering a stream table
  disables prepared statements and NOTIFY emissions for that table. Leave it off
  (the default) if you're not behind a pooler — behaviour is unchanged from
  v0.9.0.

- **PgBouncer tested end-to-end.** A new automated test suite boots PgBouncer in
  transaction-pool mode alongside pg_trickle and exercises the full lifecycle:
  create, refresh, alter, drop — all through the pooler. Run with
  `just test-pgbouncer`.

### Query Engine Correctness Fixes

Several SQL patterns that appeared to work correctly could produce wrong results
silently under the incremental query engine. All of the following are now fixed:

- **Recursive queries (WITH RECURSIVE) update correctly when rows are deleted.**
  Recursive queries are used for organisation hierarchies, bill-of-materials
  roll-ups, graph traversals, and similar structures. In DIFFERENTIAL mode,
  deleting a row from the source previously caused a full recomputation
  (correct, but expensive — O(n)). Now pg_trickle uses the Delete-and-Rederive
  algorithm, updating only affected rows at O(delta) cost. Computed expressions
  like `ancestor.path || ' > ' || node.name` update correctly when any ancestor
  is renamed or moved.

- **SUM over a FULL OUTER JOIN no longer returns 0 instead of NULL.** When
  matched rows on both join sides transition to matched on one side only (creating
  null-padded rows), the incremental SUM formula previously returned 0 instead of
  NULL. pg_trickle now tracks how many non-null values exist in each group and
  produces the correct answer without any full-group rescan.

- **Multi-source delta merging is now correct for diamond-shaped queries.** A
  "diamond" topology is when two separate paths through the dependency graph both
  feed into the same stream table (e.g. table A → both B and C → D). Simultaneous
  changes on both paths could previously cause some corrections to be silently
  discarded, leaving D with wrong values. Now uses proper weight aggregation
  (Z-set algebra) so every correction is applied. Six property-based tests verify
  this for different diamond shapes.

- **Statistical aggregates (CORR, COVAR, REGR_*) now update in constant time.**
  All twelve SQL correlation and regression functions — `CORR`, `COVAR_POP`,
  `COVAR_SAMP`, and the ten `REGR_*` variants — now update incrementally using
  running totals (Welford-style accumulation) instead of rescanning the whole
  group. Each changed row is processed once regardless of group size.

- **LATERAL subqueries only re-examine correlated rows.** When data changes in
  the inner part of a LATERAL JOIN, pg_trickle previously re-ran the subquery for
  every row in the outer table. Now it re-runs it only for outer rows that
  actually correlate with the changed inner data, reducing work from
  proportional-to-table-size to proportional-to-changes.

- **Materialized view sources now work in DIFFERENTIAL mode.** Stream tables
  can use a PostgreSQL materialized view as their data source when
  `pg_trickle.matview_polling = on` is set. Changes are detected by comparing
  snapshots, the same mechanism used for foreign table sources.

- **Six correctness bugs in the query rewriting engine fixed.** These all
  involved edge cases in how the incremental engine translates SQL:
  - SQL comment fragments such as `/* unsupported ... */` that were being
    injected into generated SQL and causing runtime syntax errors are now
    replaced with clear extension-level errors.
  - When a column-rename step (e.g. `EXTRACT(year FROM orderdate) AS o_year`)
    sits between an aggregate and its source, GROUP BY and aggregate expressions
    now resolve correctly.
  - `EXCEPT` queries wrapped in a projection no longer silently lose their row
    multiplicity tracking.
  - A placeholder row identifier value of zero could collide with real row
    hashes; changed to a sentinel value (`i64::MIN`) outside the normal hash
    range.
  - Empty scalar subqueries now raise a clear error instead of silently
    emitting NULL.

- **Change capture (CDC) fixes.** The UPDATE trigger now correctly handles rows
  with NULL values in their primary key columns (previously those rows were
  silently dropped from the change buffer). WAL logical replication publications
  are automatically rebuilt when a source table is converted to partitioned after
  the publication was set up — previously this caused the stream table to silently
  stop updating. TRUNCATE followed by INSERT is handled atomically so
  post-TRUNCATE inserts are never lost.

### Faster Refreshes

- **Automatic covering index on stream table row IDs.** Stream tables with eight
  or fewer output columns now automatically get a covering index with `INCLUDE
  (col1, col2, ...)` on the internal `__pgt_row_id` column. This lets the MERGE
  step use index-only scans — no heap lookups for matched rows — reducing refresh
  time by roughly 20–50% in small-delta / large-table scenarios.

- **Change buffer compaction.** When the pending change buffer grows beyond
  `pg_trickle.compact_threshold` (default 100,000 rows), pg_trickle compacts it
  before the next refresh cycle. INSERT→DELETE pairs that cancel each other out
  are eliminated; multiple sequential changes to the same row are collapsed to a
  single net change. Reduces delta scan overhead by 50–90% for high-churn tables.
  Uses `change_id` (not `ctid`) for safe operation under concurrent VACUUM.

- **Tiered refresh scheduling.** Large deployments can assign stream tables to
  one of four tiers: Hot (refresh at the configured interval), Warm (2× interval),
  Cold (10× interval), or Frozen (skip until manually promoted). Gate the feature
  with `pg_trickle.tiered_scheduling = on` (default off). Set per stream table
  via `ALTER STREAM TABLE ... SET (tier => 'warm')`. Frozen stream tables are
  entirely skipped by the scheduler until you promote them.

- **Incremental dependency-graph updates.** When a stream table is created,
  altered, or dropped, the internal dependency graph now updates only the affected
  entries instead of rebuilding the entire graph from scratch. Reduces the latency
  impact of DDL operations from roughly 50 ms to roughly 1 ms in deployments with
  1,000+ stream tables.

- **Smarter topo-sort caching inside a scheduler tick.** The ordering in which
  stream tables are refreshed (topological order through the dependency graph) is
  now computed once per scheduler tick and reused across all internal callers,
  eliminating redundant work.

### Better Visibility Into What pg_trickle Is Doing

Several behaviours that previously happened silently now produce a short,
actionable message at the moment they occur:

- **`ORDER BY` without `LIMIT` warns you at creation time.** Adding `ORDER BY`
  to a stream table's defining query without also adding `LIMIT` has no effect:
  stream table storage has no guaranteed row order. pg_trickle now emits a
  `WARNING` pointing you toward the TopK pattern or suggesting you remove the
  `ORDER BY`.

- **`append_only` mode reversions are visible.** When pg_trickle automatically
  exits append-only mode (because deletions or updates were detected in the
  source), the notice is now emitted at `WARNING` level (was `INFO`, normally
  suppressed) and also dispatched as a `pgtrickle_alert` notification.

- **Cleanup failures escalate after 3 consecutive attempts.** If the background
  worker fails to clean up a source table 3 times in a row, the message is
  promoted from `DEBUG1` (normally invisible) to `WARNING` so it appears in the
  server log.

- **Diamond dependency with `diamond_consistency='none'` now advises you.** When
  you create a stream table that forms a diamond in the dependency graph and
  explicitly set `diamond_consistency='none'`, a `NOTICE` advises you to consider
  `diamond_consistency='atomic'` for consistent cross-branch reads.

- **`diamond_consistency` now defaults to `'atomic'`.** New stream tables get
  atomic group semantics by default, meaning all branches of a diamond are
  refreshed together in a single savepoint before the convergence node is
  updated. This prevents a read from the convergence node seeing one branch
  partially updated and the other stale. To restore the old independent behavior,
  pass `diamond_consistency => 'none'` explicitly.

- **Adaptive fallback is visible at the default log level.** When a differential
  refresh falls back to a full refresh because the delta is too large, the
  message is now emitted at `NOTICE` level (the default `client_min_messages`
  threshold) instead of `INFO` (usually suppressed in the client session).

- **`CALCULATED` schedule without downstream dependents warns you.** When a
  stream table is created with `schedule='calculated'` but no existing stream
  table references it as a downstream dependent, a `NOTICE` explains that the
  schedule will fall back to `pg_trickle.default_schedule_seconds`.

- **Internal `__pgt_*` auxiliary columns are now documented.** The hidden
  columns that the refresh engine may add to stream table physical storage are
  described in a new section of [SQL_REFERENCE.md](docs/SQL_REFERENCE.md). This
  covers all variants from the always-present `__pgt_row_id` primary key through
  the aggregate-specific auxiliary columns for AVG, STDDEV, CORR, COVAR, REGR_*,
  window functions, and recursive CTE depth.

### Bug Fixes

- **Scheduler no longer permanently misses stream tables created under a
  stale snapshot.** `signal_dag_invalidation` is called inside the creating
  transaction before it commits. If the background scheduler happened to
  start a new tick and capture a catalog snapshot at that exact instant, the
  DAG rebuild query would not see the new stream table — yet the version
  counter was already advanced, so the scheduler would never rebuild again.
  The affected stream table would then never be scheduled for refresh.
  Fixed by verifying that every invalidated `pgt_id` is present in the
  rebuilt DAG after each rebuild. If any are missing the scheduler signals
  a full-rebuild for the next tick (which starts a fresh transaction that
  includes all committed data) rather than accepting the stale version.
  Fixes CI test `test_autorefresh_diamond_cascade`.

### Upgrade Notes

- **New catalog columns.** The `0.9.0 → 0.10.0` upgrade migration adds
  `pooler_compatibility_mode BOOLEAN` and `refresh_tier TEXT` to
  `pgt_stream_tables`. Run `ALTER EXTENSION pg_trickle UPDATE TO '0.10.0'`
  after replacing the extension files. Verification script:
  `scripts/check_upgrade_completeness.sh`.

- **Hidden auxiliary columns for statistical aggregates.** Stream tables using
  `CORR`, `COVAR_POP`, `COVAR_SAMP`, or any `REGR_*` aggregate will get hidden
  `__pgt_aux_*` columns when created or altered under 0.10.0. These are
  invisible to normal queries (excluded by the `NOT LIKE '__pgt_%'` convention)
  and managed automatically.

- **`pooler_compatibility_mode` is off by default.** Existing stream tables are
  unaffected. Enable it only for stream tables accessed through PgBouncer
  transaction-mode pooling.

### Additional Bug Fixes (2026-03-24)

**Scheduler stability:**

- **Scheduler no longer crashes when concurrent refreshes compete.** The
  internal function that decides whether to skip a refresh cycle was running a
  locking query outside a transaction boundary — a strict PostgreSQL requirement.
  It now runs inside a proper subtransaction, eliminating the crash.

- **Auto-backoff no longer causes a transaction conflict in the background
  worker.** When the auto-backoff feature stretches a stream table's refresh
  interval, it previously tried to open a new transaction inside the background
  worker's already-open transaction. PostgreSQL does not allow this nesting; the
  code path is now restructured to avoid it.

**Query engine correctness:**

- **Queries that filter on hidden columns now produce correct results.** For
  example, `SELECT name FROM users WHERE internal_id > 5` — where `internal_id`
  is not part of the output — could return wrong rows during incremental updates.
  Fixed.

- **JOIN results are correct when both joined tables change at the same time.**
  Simultaneous changes to two stream tables connected by a JOIN could leave the
  output with stale or duplicated rows. Fixed.

- **`NULLIF(a, b)` expressions now work in incremental queries.** `NULLIF`
  returns NULL when its two arguments are equal. It was not recognised by the
  incremental parser, causing a fallback error. Fixed.

- **`LIKE` and `ILIKE` pattern matching now work in filter conditions.** Filter
  expressions such as `WHERE name LIKE 'A%'` or `WHERE description ILIKE
  '%widget%'` were not handled by the incremental engine. Fixed.

- **Subqueries with `ORDER BY`, `LIMIT`, or `OFFSET` are now preserved
  correctly.** When the incremental engine reconstructed a subquery, those
  clauses were silently dropped. The incremental result no longer differs from a
  full refresh for such queries.

- **Scalar subqueries using `LIMIT` or `OFFSET` are now handled gracefully.**
  Rather than producing a runtime error, the engine falls back to a full refresh
  for those cases and continues.

**SQL parser:**

- **Wildcard column references (`table.*`) now work for qualified names.** A
  two- or three-part column reference such as `schema.table.*` or `alias.*`
  caused a parser crash. Fixed.

**Change capture and WAL:**

- **State transitions no longer stall when the WAL replication slot is behind.**
  When a stream table moves through the TRANSITIONING state, pg_trickle now
  advances the WAL replication slot up-front. This eliminates a lag-check stall
  that could cause the transition to hang indefinitely under write-heavy
  workloads.

**Security:**

- Several low-severity code quality and security scanner alerts from Semgrep and
  CodeQL are resolved. No user-visible behaviour changes.

---

## [0.9.0] — Incremental Aggregates & Smarter Scheduling

The headline feature of 0.9.0 is **incremental aggregate maintenance**: when a
single row changes inside a group of 100,000 rows, pg_trickle no longer has to
re-scan all 100,000 rows to update COUNT, SUM, AVG, STDDEV, or VAR results.
Instead it keeps running totals and adjusts them in constant time. Only MIN/MAX
still needs a rescan — and only when the deleted value happens to be the current
extreme.

Beyond aggregates, this release contains a broad set of performance
optimizations that reduce wasted I/O during every refresh cycle, two new
configuration knobs, a refresh-group management API, and several bug fixes.

### Faster Aggregates

- **Constant-time COUNT, SUM, AVG**: Changed rows are now applied
  algebraically (`new_sum = old_sum + inserted − deleted`) instead of
  re-aggregating the whole group.  AVG uses hidden auxiliary SUM and COUNT
  columns maintained automatically on the stream table.
- **Constant-time STDDEV and VAR**: Standard-deviation and variance
  aggregates (`STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`) now
  use a sum-of-squares decomposition with a hidden auxiliary column,
  achieving the same constant-time update as COUNT/SUM/AVG.
- **MIN/MAX safety guard**: Deleting the row that currently holds the
  minimum (or maximum) value correctly triggers a rescan of that group.
  Property-based tests verify this boundary.
- **Floating-point drift reset**: A new setting
  (`pg_trickle.algebraic_drift_reset_cycles`) periodically forces a full
  recomputation to correct any floating-point rounding drift that
  accumulates over many incremental cycles.

### Smarter Refresh Scheduling

- **Automatic backoff for overloaded streams**: The `pg_trickle.auto_backoff`
  GUC was introduced here (default off at the time). See the v0.10.0 entry
  for the improved thresholds, reduced cap, and the flip to `on` by default.
- **Index-aware MERGE**: A new threshold setting
  (`pg_trickle.merge_seqscan_threshold`, default 0.001) tells PostgreSQL
  to use an index lookup instead of a full table scan when only a tiny
  fraction of the stream table's rows are changing.

### Less Wasted I/O

- **Skip unchanged columns**: The scan operator now checks the CDC
  trigger's per-row bitmask to skip UPDATE rows where none of the columns
  your query actually uses were modified.  For wide tables where you only
  reference a few columns, most UPDATE processing is eliminated.
- **Skip unchanged sources in joins**: When a multi-source join query has
  three source tables but only one of them changed, the delta branches for
  the two unchanged sources are now replaced with `FALSE` at plan time.
  PostgreSQL's planner recognises those branches as empty and skips them
  entirely.
- **Push WHERE filters into the change scan**: If your stream table's
  defining query has a WHERE clause (e.g. `WHERE status = 'shipped'`),
  that filter is now applied immediately after reading the change buffer
  — before rows enter the join or aggregate pipeline.  Rows that don't
  match the filter are discarded right away.
- **Faster DISTINCT counting**: The per-row multiplicity lookup for
  `SELECT DISTINCT` queries now uses an index-driven scalar subquery
  instead of a LEFT JOIN, guaranteeing I/O proportional to the number of
  changed rows regardless of stream table size.
- **Scalar subquery short-circuit**: When a scalar subquery's inner source
  has no changes in the current cycle, the expensive outer-table snapshot
  reconstruction is skipped entirely.

### Refresh Group Management

- **New SQL functions** for grouping stream tables that should always be
  refreshed together (cross-source snapshot consistency):
  - `pgtrickle.create_refresh_group(name, members, isolation)`
  - `pgtrickle.drop_refresh_group(name)`
  - `pgtrickle.refresh_groups()` — lists all declared groups.

### Bug Fixes

- **Fixed a crash when internal status queries failed**: The
  `source_gates()` and `watermarks()` SQL functions previously crashed the
  entire PostgreSQL backend process on any internal error.  They now report
  a normal SQL error instead.
- **Clearer handling of window functions in expressions**: Queries like
  `CASE WHEN ROW_NUMBER() OVER (...) > 5 THEN ...` were silently accepted
  but failed at refresh time with a confusing error.  pg_trickle now
  automatically falls back to full refresh mode (in AUTO mode) or warns
  you at creation time (in explicit DIFFERENTIAL mode).

### Documentation

- Documented the known limitation that recursive CTE stream tables in
  DIFFERENTIAL mode fall back to full recomputation when rows are deleted
  or updated.  Workaround: use `refresh_mode = 'IMMEDIATE'`.
- Documented the `pgt_refresh_groups` catalog table schema and usage.
- Documented the O(partition_size) cost of window function maintenance with
  mitigation strategies.

### Deferred to v0.10.0

The following performance optimizations were evaluated and explicitly deferred.
In every case the current behaviour is **correct** — these items would make
certain workloads faster but carry enough implementation risk that they need
more design work first:
- Recursive CTE incremental delete/update in DIFFERENTIAL mode (P2-1)
- SUM NULL-transition shortcut for FULL OUTER JOIN aggregates (P2-2)
- Materialized view sources in IMMEDIATE mode (P2-4)
- LATERAL subquery scoped re-execution (P2-6)
- Welford auxiliary columns for CORR/COVAR/REGR_* aggregates (P3-2)
- Merged-delta weight aggregation for multi-source deduplication (B3-2/B3-3)

### Upgrade Notes

- **New SQL objects**: The `0.8.0 → 0.9.0` upgrade migration adds the
  `pgt_refresh_groups` table and the `restore_stream_tables` function.
  Run `ALTER EXTENSION pg_trickle UPDATE TO '0.9.0'` after replacing the
  extension files.
- **Hidden auxiliary columns**: Stream tables using AVG, STDDEV, or VAR
  aggregates will automatically get hidden `__pgt_aux_*` columns when
  created or altered.  These columns are invisible to normal queries
  (filtered by the existing `NOT LIKE '__pgt_%'` convention) and are
  managed automatically.
- **PGXN publishing**: Release artifacts are now automatically uploaded to
  PGXN via GitHub Actions.

---

## [0.8.0] — Backup, Pooler Compatibility & Reliability

This release focuses on making your streams easier to back up, far more reliable under complex scenarios, and solidifying the underlying core engine through massive testing improvements.

### Added
- **Backup and Restore Support**: You can now safely backup your database using standard `pg_dump` and `pg_restore` commands. The system will automatically reconnect all streams and data queues to eliminate downtime during disaster recovery.
- **Connection Pooler Opt-In**: Replaced the global PgBouncer pooler compatibility setting with a per-stream option. You can now enable connection pooling optimizations selectively on a stream-by-stream basis.

### Fixed
- **Cyclic Stream Reliability**: Fixed internal bugs that occasionally caused streams referencing each other in a loop to get stuck refreshing forever. Streams now accurately detect when row changes stop and naturally settle.
- **Large Dependency Chains**: Fixed a crash (stack overflow) that could happen if you attempted to drop an extremely large or heavily recursive chain of stream tables sequentially.
- **Special Character Support in SQL**: Handled an edge case causing errors when multi-byte characters or special non-ASCII symbols were parsed inside certain SQL commands.
- **Mac Support for Developer Tooling**: Addressed a minor internal tool error stopping test components from automatically building on Apple Silicon machines.

### Under the Hood Code and Testing Enhancements
- **Massive Testing Hardening**: We have fundamentally overhauled and upgraded how we test the system. Our internal test suite has been completely enhanced with tens of thousands of continuous automated checks ensuring query answers are perfect, no matter how complex the data joins or updates get.
- **Performance Migrations**: Began adopting new tools (`cargo nextest`) to speed up how fast we can iterate and develop the software in the background.

## [0.7.0] — Watermark Gating, Circular Pipelines & SQL Broadening

0.7.0 makes pg_trickle easier to trust in real-world data pipelines. The big
theme of this release is fewer surprises: the scheduler can now wait for late
arriving source data, some circular pipelines can run safely instead of being
blocked, more queries stay on incremental refresh, and the system does a better
job of deciding when incremental work is no longer worth it.

### Added

#### Multi-source data can wait until it is actually ready

pg_trickle can now delay a refresh until related source tables have all caught
up to roughly the same point in time. This is useful for ETL jobs where, for
example, `orders` arrives before `order_lines` and refreshing too early would
produce a half-finished report.

- New watermark APIs: `advance_watermark(source, watermark)`,
  `create_watermark_group(name, sources[], tolerance_secs)`, and
  `drop_watermark_group(name)`.
- New status helpers: `watermarks()`, `watermark_groups()`, and
  `watermark_status()`.
- The scheduler now skips gated refreshes when grouped sources are too far
  apart and records the reason in refresh history.
- New catalog tables store per-source watermarks and watermark group
  definitions.
- 28 end-to-end tests cover normal operation, bad input, tolerance windows,
  and scheduler behavior.

#### Some circular pipelines can now run safely

Stream tables that depend on each other in a loop are no longer always blocked.
If the cycle is monotone and uses DIFFERENTIAL mode, pg_trickle can now keep
refreshing the group until it stops changing.

- Circular refreshes run to a fixed point, with `pg_trickle.max_fixpoint_iterations`
  as a safety limit.
- Cycle creation and ALTER validation now check that every member is safe for
  convergence before allowing the loop.
- `pgtrickle.pgt_status()` now reports `scc_id`, and
  `pgtrickle.pgt_scc_status()` shows per-cycle-group status.
- `pgtrickle.pgt_stream_tables` now tracks `last_fixpoint_iterations` so it is
  easier to spot slow or unstable cycles.
- 6 end-to-end tests cover convergence, rejection of unsafe cycles,
  non-convergence handling, and cleanup.

#### More queries stay on incremental refresh

Several query shapes that used to fall back to FULL refresh, or fail outright,
now keep working in DIFFERENTIAL and AUTO mode.

- User-defined aggregates created with `CREATE AGGREGATE` now work through the
  existing group-rescan strategy, including common extension-provided
  aggregates.
- More complex `OR` plus subquery patterns are now rewritten correctly,
  including cases that need De Morgan normalization and multiple rewrite passes.
- The rewrite pipeline has a guardrail to stop runaway branch explosion.
- A dedicated 14-test end-to-end suite covers these previously missing cases.

#### Easier packaging ahead of 1.0

The release also adds infrastructure that makes evaluation and future
distribution simpler.

- `Dockerfile.hub` and a dedicated CI workflow can build and smoke-test a
  ready-to-run PostgreSQL 18 image with pg_trickle preinstalled.
- `META.json` adds PGXN package metadata with `release_status: "testing"`.
- CNPG smoke testing is now part of the documented pre-1.0 packaging story.

### Improved

#### Refresh strategy and performance decisions are smarter

The scheduler and refresh engine now make better choices when incremental work
is likely to help and back off sooner when it is not.

- Wide tables now use xxh64-based change detection instead of slower MD5-based
  comparisons.
- Aggregate stream tables can skip expensive incremental work and jump straight
  to FULL refresh when the pending change set is obviously too large.
- Strategy selection now combines a change-ratio signal with recent refresh
  history, which helps on workloads with uneven batch sizes.
- DAG levels are extracted explicitly, enabling level-parallel refresh
  scheduling.
- Small internal hot paths such as column-list building and LSN comparison were
  tightened to remove avoidable allocations.

#### Benchmarking is much easier to use and compare

The performance toolchain was expanded so regressions are easier to spot and
large-scale behavior is easier to study.

- Benchmarks now support per-cycle output, optional `EXPLAIN ANALYZE` capture,
  larger 1M-row runs, and more stable Criterion settings.
- New tooling covers cross-run comparison, concurrent writers, and extra query
  shapes such as window, lateral, CTE, and `UNION ALL` workloads.
- `just bench-docker` makes it easier to run Criterion inside the builder image
  when local linking is awkward.

### Changed
#### Internal Code Quality: Integration Test Suite Hardening

Completed a full hardening pass of the integration test suite, bringing all items in `PLAN_TEST_EVALS_INTEGRATION.md` to done:
- **Multiset validation** — Extracted `assert_sets_equal()` helper relying on EXCEPT/UNION ALL SQL logic and applied it to workflow tests to ensure storage table state correctly matches the defining query post-refresh.
- **Round-trip notifications** — `pg_trickle_alert` notifications now verify receipt end-to-end via `sqlx::PgListener`.
- **DVM operators** — Added unit coverage for complex semi/anti-join behaviors (multi-column, filtered, complementary), multi-table join chains for inner and full joins, and `proptest!` fuzz tests enforcing generated SQL invariants across INNER, SEMI, and ANTI joins.
- **Resilience and edge cases** — Test coverage for ST drop cascades verifying dependent object removal, exact error escalation thresholds, and scheduler job lifecycles across queued mock states.
- **Cleanups** — Standardized naming practices (`test_workflow_*`, `test_infra_*`) and eliminated clock-bound flakes by widening staleness assertions.


#### Internal low-level code is much safer to audit

This release cuts the amount of low-level `unsafe` Rust in half without
changing behavior.

- Unsafe blocks were reduced by 51%, from 1,309 to 641.
- Repeated patterns were consolidated into a small set of documented helper
  functions.
- 37 internal functions no longer need to be marked `unsafe`.
- Existing unit tests continued to pass unchanged after the refactor.

---

## [0.6.0] — Idempotent DDL, Partitioned Sources & dbt Integration

### Added

#### Idempotent DDL (`create_or_replace`)

New one-call function for deploying stream tables without worrying about
whether they already exist. Replaces the old "check if it exists, then drop
and recreate" pattern.

- **`create_or_replace_stream_table()`** — a single function that does the
  right thing automatically:
  - **Creates** the stream table if it doesn't exist yet.
  - **Does nothing** if the stream table already exists with the same query
    and settings (logs an INFO so you know it was a no-op).
  - **Updates settings** (schedule, refresh mode, etc.) if only config changed.
  - **Replaces the query** if the defining query changed — including
    automatic schema migration and a full refresh.
- **dbt uses it automatically.** The `stream_table` materialization now calls
  `create_or_replace_stream_table()` when running against pg_trickle 0.6.0+,
  with automatic fallback for older versions.
- **Whitespace-insensitive.** Cosmetic SQL differences (extra spaces, tabs,
  newlines) are correctly treated as no-ops — won't trigger unnecessary
  rebuilds.

#### dbt Integration Enhancements

- **Check stream table health from dbt.** New `pgtrickle_stream_table_status()`
  macro returns whether a stream table is healthy, stale, erroring, or paused.
  Pair it with the new built-in `stream_table_healthy` test in your
  `schema.yml` to fail CI when a stream table is behind or broken.
- **Refresh everything in the right order.** New `refresh_all_stream_tables`
  run-operation refreshes all dbt-managed stream tables in dependency order.
  Run it after `dbt run` and before `dbt test` in your CI pipeline.

#### Partitioned Source Tables

Stream tables now work with PostgreSQL's declarative table partitioning —
RANGE, LIST, and HASH partitioned tables all work as sources out of the box.

- **Changes in any partition are captured automatically.** CDC triggers fire
  on the parent table so inserts, updates, and deletes in any child partition
  are picked up.
- **ATTACH PARTITION triggers automatic rebuild.** When you attach a new
  partition, pg_trickle detects the structural change and rebuilds affected
  stream tables to include the new partition's pre-existing data.
- **WAL mode works with partitions.** Publications are configured with
  `publish_via_partition_root = true`, so all partitions report changes under
  the parent table's identity.
- **New tutorial** covering partitioned source tables, ATTACH/DETACH behavior,
  and known caveats (`docs/tutorials/PARTITIONED_TABLES.md`).

#### Circular Dependency Foundation

Lays the groundwork for stream tables that reference each other in a cycle
(A → B → A). The actual cyclic refresh execution is planned for v0.7.0 —
this release adds the detection, validation, and safety infrastructure.

- **Cycle detection.** pg_trickle can now identify groups of stream tables
  that form circular dependencies.
- **Safety checks at creation time.** Queries that can't safely participate
  in a cycle (those using aggregates, EXCEPT, window functions, or NOT EXISTS)
  are rejected with a clear error explaining why.
- **New settings:**
  - `pg_trickle.allow_circular` (default: off) — master switch for circular
    dependencies.
  - `pg_trickle.max_fixpoint_iterations` (default: 100) — prevents runaway
    loops.

#### Source Gating Improvements

- **`bootstrap_gate_status()` function.** Shows which sources are currently
  gated, when they were gated, how long the gate has been active, and which
  stream tables are waiting. Useful for debugging "why isn't my stream table
  refreshing?"
- **ETL coordination cookbook.** SQL Reference now includes five step-by-step
  recipes for common bulk-load patterns.

#### More SQL Patterns Supported

Two query patterns that previously required workarounds now just work:

- **Window functions inside expressions.** Queries like
  `CASE WHEN ROW_NUMBER() OVER (...) = 1 THEN 'top' ELSE 'other' END` or
  `COALESCE(SUM() OVER (...), 0)` are now accepted and produce correct
  results. Use **FULL** refresh mode for these queries — incremental
  (DIFFERENTIAL) refresh of window-in-expression patterns is not yet
  supported. Previously, the query was rejected entirely at creation time.

- **`ALL (subquery)` comparisons.** Queries like
  `WHERE price < ALL (SELECT price FROM competitors)` are now accepted in
  both FULL and DIFFERENTIAL modes. Supports all comparison operators
  (`>`, `>=`, `<`, `<=`, `=`, `<>`) and correctly handles NULL values per
  the SQL standard.

#### Operational Safety Improvements

- **Function changes detected automatically.** If a stream table's query
  calls a user-defined function and you update that function with
  `CREATE OR REPLACE FUNCTION`, pg_trickle detects the change and
  automatically rebuilds the stream table on the next cycle. No manual
  intervention needed.

- **WAL mode explains why it isn't activating.** When `cdc_mode = 'auto'`
  and the system stays on trigger-based tracking, the scheduler now
  periodically logs the exact reason (e.g., "`wal_level` is not `logical`")
  and `check_cdc_health()` reports the current mode so you can diagnose the
  issue.

- **WAL + keyless tables rejected early.** Creating a stream table with
  `cdc_mode = 'wal'` on a table that has no primary key and no
  `REPLICA IDENTITY FULL` is now rejected at creation time with a clear
  error — instead of silently producing incomplete results later.

- **Automatic recovery after backup/restore.** When a PostgreSQL server is
  restored from `pg_basebackup`, WAL replication slots are lost. pg_trickle
  now detects the missing slot, automatically falls back to trigger-based
  tracking, and logs a WARNING so you know what happened.

#### Documentation

- **ALL (subquery) worked example** in the SQL Reference with sample data
  and expected results.
- **Window-in-expression documentation** showing before/after examples of
  the automatic rewrite.
- **Foreign table sources tutorial** — step-by-step guide for using
  `postgres_fdw` foreign tables as stream table sources.

### Fixed

- **`create_or_replace` whitespace handling.** Extra spaces, tabs, and
  newlines in queries no longer trigger unnecessary rebuilds.
- **`create_or_replace` schema incompatibility detection.** Incompatible
  column type changes (e.g., text → integer) are now properly detected
  and handled.

---

## [0.5.0] — Row-Level Security, Source Gating & Append-Only Fast Path

### Added

#### Row-Level Security (RLS) Support

Stream tables now work correctly with PostgreSQL's Row-Level Security feature,
which lets you control which rows different users can see.

- **Refreshes always see all data.** When a stream table is refreshed, it
  computes the full result regardless of RLS policies on the source tables.
  This matches how PostgreSQL's built-in materialized views work. You then
  add RLS policies directly on the stream table to control who can read what.
- **Internal tables are protected.** The internal change-tracking tables used
  by pg_trickle are shielded from RLS interference, so refreshes won't
  silently fail if you turn on RLS at the schema level.
- **Real-time (IMMEDIATE) mode secured.** Triggers that keep stream tables
  updated in real time now run with elevated privileges and a locked-down
  search path, preventing data corruption or security bypasses.
- **RLS changes are detected automatically.** If you enable, disable, or force
  RLS on a source table, pg_trickle detects the change and marks affected
  stream tables for a full rebuild.
- **New tutorial.** Step-by-step guide for setting up per-tenant RLS policies
  on stream tables (see `docs/tutorials/ROW_LEVEL_SECURITY.md`).

#### Source Gating for Bulk Loads

New pause/resume mechanism for large data imports. When you're loading a big
batch of data into a source table, you can temporarily "gate" it to prevent
the background scheduler from triggering refreshes mid-load. Once the load is
done, ungate it and everything catches up in a single refresh.

- **`gate_source('my_table')`** — pauses automatic refreshes for any stream
  table that depends on `my_table`.
- **`ungate_source('my_table')`** — resumes automatic refreshes. All changes
  made during the gate are picked up in the next refresh cycle.
- **`source_gates()`** — shows which source tables are currently gated, when
  they were gated, and by whom.
- **Manual refresh still works.** Even while a source is gated, you can
  explicitly call `refresh_stream_table()` if needed.
- Gating is idempotent — calling `gate_source()` twice is safe, and gating a
  source that's already gated is a no-op.

#### Append-Only Fast Path

Significant performance improvement for tables that only receive INSERTs
(event logs, audit trails, time-series data, etc.). When you mark a stream
table as `append_only`, refreshes skip the expensive merge logic (checking
for deletes, updates, and row comparisons) and use a simple, fast insert.

- **How to use:** Pass `append_only => true` when creating or altering a
  stream table.
- **Safe fallback.** If a DELETE or UPDATE is detected on a source table, the
  extension automatically falls back to the standard refresh path and logs a
  warning. It won't silently produce wrong results.
- **Restrictions.** Append-only mode requires DIFFERENTIAL refresh mode and
  source tables with primary keys.

#### Usability Improvements

- **Manual refresh history.** When you manually call `refresh_stream_table()`,
  the result (success or failure, timing, rows affected) is now recorded in
  the refresh history, just like scheduled refreshes.
- **`quick_health` view.** A single-row health summary showing how many stream
  tables you have, how many are in error or stale, whether the scheduler is
  running, and an overall status (`OK`, `WARNING`, `CRITICAL`). Easy to plug
  into monitoring dashboards.
- **`create_stream_table_if_not_exists()`.** A convenience function that does
  nothing if the stream table already exists, instead of raising an error.
  Makes migration scripts and deployment automation simpler.

#### Smooth Upgrade from 0.4.0

- Existing installations can upgrade with
  `ALTER EXTENSION pg_trickle UPDATE TO '0.5.0'`. All new features (source
  gating, append-only mode, quick health view, and the new convenience
  functions) are included in the upgrade script.
- The upgrade has been verified with automated tests that confirm all 40 SQL
  objects survive the upgrade intact.

---

## [0.4.0] — Parallel Refresh & Statement-Level CDC Triggers

### Added

#### Parallel Refresh (opt-in)

Stream tables can now be refreshed in parallel, using multiple background
workers instead of processing them one at a time. This can dramatically reduce
end-to-end refresh latency when you have many independent stream tables.

- **Off by default.** Set `pg_trickle.parallel_refresh_mode = 'on'` to enable.
  Use `'dry_run'` to preview what the scheduler would do without changing
  behavior.
- **Automatic dependency awareness.** The scheduler figures out which stream
  tables can safely refresh at the same time and which must wait for others.
  Stream tables connected by real-time (IMMEDIATE) triggers are always
  refreshed together to prevent race conditions.
- **Atomic groups.** When a group of stream tables must succeed or fail
  together (e.g. diamond dependencies), all members are wrapped in a single
  transaction — if one fails, the whole group rolls back cleanly.
- **Worker pool controls:**
  - `pg_trickle.max_dynamic_refresh_workers` (default 4) — cluster-wide cap on
    concurrent refresh workers.
  - `pg_trickle.max_concurrent_refreshes` — per-database dispatch cap.
- **Monitoring:**
  - `worker_pool_status()` — shows how many workers are active and the current
    limits.
  - `parallel_job_status(max_age_seconds)` — lists recent and active refresh
    jobs with timing and status.
  - `health_check()` now warns when the worker pool is saturated or the job
    queue is backing up.
- **Self-healing.** On startup, the scheduler automatically cleans up orphaned
  jobs and reclaims leaked worker slots from previous crashes.

#### Statement-Level CDC Triggers

Change tracking triggers have been upgraded from row-level to statement-level,
reducing write-side overhead for bulk INSERT and UPDATE operations. This is
now the default for all new and existing stream tables. A benchmark harness is
included so you can measure the difference on your own hardware.

#### dbt Getting Started Example

New `examples/dbt_getting_started/` project with a complete, runnable dbt
example showing org-chart seed data, staging views, and three stream table
models. Includes an automated test script.

### Fixed

#### Refresh Lock Not Released After Errors

Fixed a bug where `refresh_stream_table()` could get permanently stuck after
a PostgreSQL error (e.g. running out of temp file space). The internal lock
was session-level and survived transaction rollback, causing all future
refreshes for that stream table to report "another refresh is already in
progress". Refresh locks are now transaction-level, so they are automatically
released when the transaction ends — whether it succeeds or fails.

#### dbt Integration Fixes

- Fixed query quoting in dbt macros that broke when queries contained single
  quotes.
- Fixed `schedule = none` in dbt being incorrectly mapped to SQL NULL.
- Fixed view inlining when the same view was referenced with different aliases.

### Changed
#### Internal Code Quality: Integration Test Suite Hardening

Completed a full hardening pass of the integration test suite, bringing all items in `PLAN_TEST_EVALS_INTEGRATION.md` to done:
- **Multiset validation** — Extracted `assert_sets_equal()` helper relying on EXCEPT/UNION ALL SQL logic and applied it to workflow tests to ensure storage table state correctly matches the defining query post-refresh.
- **Round-trip notifications** — `pg_trickle_alert` notifications now verify receipt end-to-end via `sqlx::PgListener`.
- **DVM operators** — Added unit coverage for complex semi/anti-join behaviors (multi-column, filtered, complementary), multi-table join chains for inner and full joins, and `proptest!` fuzz tests enforcing generated SQL invariants across INNER, SEMI, and ANTI joins.
- **Resilience and edge cases** — Test coverage for ST drop cascades verifying dependent object removal, exact error escalation thresholds, and scheduler job lifecycles across queued mock states.
- **Cleanups** — Standardized naming practices (`test_workflow_*`, `test_infra_*`) and eliminated clock-bound flakes by widening staleness assertions.


- Updated to PostgreSQL 18.3 across CI and test infrastructure.
- Dependency updates: `tokio` 1.49 → 1.50 and several GitHub Actions bumps.

### Breaking Changes

These behavioural changes shipped in v0.4.0. They improve usability but may
require action from users upgrading from v0.3.0.

- **Schedule default changed from `'1m'` to `'calculated'`.**
  `create_stream_table` now defaults to `schedule => 'calculated'`, which
  auto-computes the refresh interval from downstream dependents instead of
  refreshing every 1 minute. If you relied on the implicit 1-minute default,
  explicitly pass `schedule => '1m'` to preserve the old behaviour.

- **`NULL` schedule input rejected.** Passing `schedule => NULL` to
  `create_stream_table` now returns an error. Use `schedule => 'calculated'`
  instead — it's explicit and self-documenting.

- **Diamond GUCs removed.** The cluster-wide GUCs
  `pg_trickle.diamond_consistency` and `pg_trickle.diamond_schedule_policy`
  have been removed. Diamond behaviour is now controlled per-table via
  parameters on `create_stream_table()` / `alter_stream_table()`:
  `diamond_consistency => 'atomic'`, `diamond_schedule_policy => 'slowest'`.

---

## [0.3.0] — Incremental Correctness & Security Tooling

This is a correctness and hardening release. No new SQL functions, tables, or
views were added — all changes are in the compiled extension code.
`ALTER EXTENSION pg_trickle UPDATE` is safe and a no-op for schema objects.

### Fixed

#### Incremental Correctness Fixes

All 18 previously-disabled correctness tests have been re-enabled (0
remaining). The following query patterns now produce correct results during
incremental (non-full) refreshes:

- **HAVING clause threshold crossing.** Queries with `HAVING` filters (e.g.
  `HAVING SUM(amount) > 100`) now produce correct totals when groups cross
  the threshold. Previously, a group gaining enough rows to meet the condition
  would show only the newly added values instead of the correct total.

- **FULL OUTER JOIN.** Five bugs affecting incremental updates for
  `FULL OUTER JOIN` queries are fixed: mismatched row identifiers, incorrect
  handling of compound GROUP BY expressions like
  `COALESCE(left.col, right.col)`, and wrong NULL handling for SUM aggregates.

- **EXISTS with HAVING subqueries.** Queries using
  `WHERE EXISTS(... GROUP BY ... HAVING ...)` now work correctly — the inner
  GROUP BY and HAVING were previously being silently discarded.

- **Correlated scalar subqueries.** Correlated subqueries in SELECT like
  `(SELECT MAX(e.salary) FROM emp e WHERE e.dept_id = d.id)` are now
  automatically rewritten into LEFT JOINs so the incremental engine can
  handle them correctly.

#### Background Worker Detection on PostgreSQL 18

Fixed a bug where `health_check()` and the scheduler reported zero active
workers on PostgreSQL 18 due to a column name change in system views.

#### Scheduler Stability

Fixed a loop where the scheduler launcher could get stuck retrying failed
database probes indefinitely instead of backing off properly.

### Added

#### Security Tooling

Added static security analysis to the CI pipeline:

- **GitHub CodeQL** — automated security scanning across all Rust source files.
  First scan: zero findings.
- **`cargo deny`** — enforces a license allow-list and flags unmaintained or
  yanked dependencies.
- **Semgrep** — custom rules that flag potentially dangerous patterns such as
  dynamic SQL construction and privilege escalation. Advisory-only (does not
  block merges).
- **Unsafe block inventory** — CI tracks the count of unsafe code blocks per
  file and fails if any file exceeds its baseline, preventing unreviewed
  growth of low-level code.

## [0.2.3] — Per-Table CDC Mode & WAL Lag Monitoring

### Added

- **Unsafe function detection.** Queries using non-deterministic functions like
  `random()` or `clock_timestamp()` are now rejected when creating incremental
  stream tables, because they can't produce reliable results. Functions like
  `now()` that return the same value within a transaction are allowed with a
  warning.

- **Per-table change tracking mode.** You can now choose how each stream table
  tracks changes (`'auto'`, `'trigger'`, or `'wal'`) via the `cdc_mode`
  parameter on `create_stream_table()` and `alter_stream_table()`, instead of
  relying only on the global setting.

- **CDC status view.** New `pgtrickle.pgt_cdc_status` view shows the change
  tracking mode, replication slot, and transition status for every source
  table in one place.

- **Configurable WAL lag thresholds.** The warning and critical thresholds for
  replication slot lag are now configurable via
  `pg_trickle.slot_lag_warning_threshold_mb` (default 100 MB) and
  `pg_trickle.slot_lag_critical_threshold_mb` (default 1024 MB), instead of
  being hard-coded.

- **`pg_trickle_dump` backup tool.** New standalone CLI that exports all your
  stream table definitions as replayable SQL, ordered by dependency. Useful
  for backups before upgrades or migrations.

- **Upgrade path.** `ALTER EXTENSION pg_trickle UPDATE` picks up all new
  features from this release.

### Changed
#### Internal Code Quality: Integration Test Suite Hardening

Completed a full hardening pass of the integration test suite, bringing all items in `PLAN_TEST_EVALS_INTEGRATION.md` to done:
- **Multiset validation** — Extracted `assert_sets_equal()` helper relying on EXCEPT/UNION ALL SQL logic and applied it to workflow tests to ensure storage table state correctly matches the defining query post-refresh.
- **Round-trip notifications** — `pg_trickle_alert` notifications now verify receipt end-to-end via `sqlx::PgListener`.
- **DVM operators** — Added unit coverage for complex semi/anti-join behaviors (multi-column, filtered, complementary), multi-table join chains for inner and full joins, and `proptest!` fuzz tests enforcing generated SQL invariants across INNER, SEMI, and ANTI joins.
- **Resilience and edge cases** — Test coverage for ST drop cascades verifying dependent object removal, exact error escalation thresholds, and scheduler job lifecycles across queued mock states.
- **Cleanups** — Standardized naming practices (`test_workflow_*`, `test_infra_*`) and eliminated clock-bound flakes by widening staleness assertions.


- After a full refresh, WAL replication slots are now advanced to the current
  position, preventing unnecessary WAL accumulation and false lag alarms.
- Change buffers are now flushed after a full refresh, fixing a cycle where
  the scheduler would alternate endlessly between incremental and full
  refreshes on bulk-loaded tables.
- IMMEDIATE mode now correctly rejects explicit WAL CDC requests with a clear
  error, since real-time mode uses its own trigger mechanism.
- The `pg_trickle.user_triggers` setting is simplified to `auto` and `off`.
  The old `on` value still works as an alias for `auto`.
- CI pipelines are faster on PRs — only essential tests run; the full suite
  runs on merge and daily schedule.

---

## [0.2.2] — AUTO Refresh Mode & Query Alteration

### Added

- **Change a stream table's query.** `alter_stream_table` now accepts a
  `query` parameter, so you can change what a stream table computes without
  dropping and recreating it. If the new query's columns are compatible, the
  underlying storage table is preserved — existing views, policies, and
  publications continue to work.

- **AUTO refresh mode (new default).** Stream tables now default to `AUTO`
  mode, which uses fast incremental updates when the query supports it and
  automatically falls back to a full recompute when it doesn't. You no longer
  need to think about whether your query is "incremental-compatible" — just
  create the stream table and it picks the best strategy.

- **Version mismatch warning.** The background scheduler now warns if the
  installed extension version doesn't match the compiled library, making it
  easier to spot a half-finished upgrade.

- **ORDER BY + LIMIT + OFFSET.** You can now page through top-N results, e.g.
  `ORDER BY revenue DESC LIMIT 10 OFFSET 20` to get the third page of
  top earners.

- **Real-time mode: recursive queries.** `WITH RECURSIVE` queries (e.g.
  org-chart hierarchies) now work in IMMEDIATE mode. A depth limit (default
  100) prevents infinite loops.

- **Real-time mode: top-N queries.** `ORDER BY ... LIMIT N` queries now work
  in IMMEDIATE mode — the top-N rows are recomputed on every data change.
  Maximum N is controlled by `pg_trickle.ivm_topk_max_limit` (default 1000).

- **Foreign table support.** Stream tables can now use foreign tables as
  sources. Changes are detected by comparing snapshots since foreign tables
  don't support triggers. Enable with `pg_trickle.foreign_table_polling = on`.

- **Documentation reorganization.** Configuration and SQL reference docs are
  reorganized around practical workflows. New sections cover DDL-during-refresh
  behavior, standby/replica limitations, and PgBouncer constraints.

### Changed
#### Internal Code Quality: Integration Test Suite Hardening

Completed a full hardening pass of the integration test suite, bringing all items in `PLAN_TEST_EVALS_INTEGRATION.md` to done:
- **Multiset validation** — Extracted `assert_sets_equal()` helper relying on EXCEPT/UNION ALL SQL logic and applied it to workflow tests to ensure storage table state correctly matches the defining query post-refresh.
- **Round-trip notifications** — `pg_trickle_alert` notifications now verify receipt end-to-end via `sqlx::PgListener`.
- **DVM operators** — Added unit coverage for complex semi/anti-join behaviors (multi-column, filtered, complementary), multi-table join chains for inner and full joins, and `proptest!` fuzz tests enforcing generated SQL invariants across INNER, SEMI, and ANTI joins.
- **Resilience and edge cases** — Test coverage for ST drop cascades verifying dependent object removal, exact error escalation thresholds, and scheduler job lifecycles across queued mock states.
- **Cleanups** — Standardized naming practices (`test_workflow_*`, `test_infra_*`) and eliminated clock-bound flakes by widening staleness assertions.


- Default refresh mode changed from `'DIFFERENTIAL'` to `'AUTO'`.
- Default schedule changed from `'1m'` to `'calculated'` (automatic).
- Default change tracking mode changed from `'trigger'` to `'auto'` — WAL-based
  tracking starts automatically when available, with trigger-based as fallback.

---

## [0.2.1] — Safe Upgrades & Scheduling Improvements

### Added

- **Safe upgrades.** New upgrade infrastructure ensures that
  `ALTER EXTENSION pg_trickle UPDATE` works correctly. A CI check detects
  missing functions or views in upgrade scripts, and automated tests verify
  that stream tables survive version-to-version upgrades intact. See
  [docs/UPGRADING.md](docs/UPGRADING.md) for the upgrade guide.

- **ORDER BY + LIMIT + OFFSET.** You can now create stream tables over paged
  results, like "the second page of the top-100 products by revenue"
  (`ORDER BY revenue DESC LIMIT 100 OFFSET 100`).

- **`'calculated'` schedule.** Instead of passing SQL `NULL` to request
  automatic scheduling, you can now write `schedule => 'calculated'`. Passing
  `NULL` now gives a helpful error message.

- **Documentation expansion.** Six new pages in the online book covering dbt
  integration, contributing guidelines, security policy, release process, and
  research comparisons with other projects.

- **Better warnings and safety checks:**
  - Warning when a source table lacks a primary key (duplicate rows are
    handled safely but less efficiently).
  - Warning when using `SELECT *` (new columns added later can break
    incremental updates).
  - Alert when the refresh queue is falling behind (> 80% capacity).
  - Guard triggers prevent accidental direct writes to stream table storage.
  - Automatic fallback from WAL to trigger-based change tracking when the
    replication slot disappears.
  - Nested window functions and complex `WHERE` clauses with `EXISTS` are now
    handled automatically.

- **Change buffer partitioning.** For high-throughput tables, change buffers
  can now be partitioned so that processed data is dropped efficiently.

- **Column pruning.** The incremental engine now skips source columns not used
  in the query, reducing I/O for wide tables.

### Changed
#### Internal Code Quality: Integration Test Suite Hardening

Completed a full hardening pass of the integration test suite, bringing all items in `PLAN_TEST_EVALS_INTEGRATION.md` to done:
- **Multiset validation** — Extracted `assert_sets_equal()` helper relying on EXCEPT/UNION ALL SQL logic and applied it to workflow tests to ensure storage table state correctly matches the defining query post-refresh.
- **Round-trip notifications** — `pg_trickle_alert` notifications now verify receipt end-to-end via `sqlx::PgListener`.
- **DVM operators** — Added unit coverage for complex semi/anti-join behaviors (multi-column, filtered, complementary), multi-table join chains for inner and full joins, and `proptest!` fuzz tests enforcing generated SQL invariants across INNER, SEMI, and ANTI joins.
- **Resilience and edge cases** — Test coverage for ST drop cascades verifying dependent object removal, exact error escalation thresholds, and scheduler job lifecycles across queued mock states.
- **Cleanups** — Standardized naming practices (`test_workflow_*`, `test_infra_*`) and eliminated clock-bound flakes by widening staleness assertions.


- Default `schedule` changed from `'1m'` to `'calculated'` (automatic).
- Minimum schedule interval lowered from 60 s to 1 s.
- Cluster-wide diamond consistency settings removed; per-table settings remain
  and now default to `'atomic'` / `'fastest'`.

### Fixed

- The 0.1.3 → 0.2.0 upgrade script was accidentally a no-op, silently
  skipping 11 new functions. Fixed.
- Queries combining `WITH` (CTEs) and `UNION ALL` now parse correctly.

---

## [0.2.0] — Monitoring, IMMEDIATE Mode & Diamond Consistency

### Added

- **Monitoring & health checks.** Six new functions for inspecting your stream
  tables at runtime (no superuser required):
  - `change_buffer_sizes()` — shows how much pending change data each stream
    table has queued up.
  - `list_sources(name)` — lists all base tables that feed a given stream
    table, with row counts and size estimates.
  - `dependency_tree()` — displays an ASCII tree of how your stream tables
    depend on each other.
  - `health_check()` — quick system triage that checks whether the scheduler
    is running, flags tables in error or stale, and warns about large change
    buffers or WAL lag.
  - `refresh_timeline()` — recent refresh history across all stream tables,
    showing timing, row counts, and any errors.
  - `trigger_inventory()` — verifies that all required change-tracking
    triggers are in place and enabled.

- **IMMEDIATE refresh mode (real-time updates).** New `'IMMEDIATE'` mode keeps
  stream tables updated within the same transaction as your data changes.
  There's no delay — the stream table reflects changes the instant they happen.
  Supports window functions, LATERAL joins, scalar subqueries, and aggregate
  queries. You can switch between IMMEDIATE and other modes at any time using
  `alter_stream_table`.

- **Top-N queries (ORDER BY + LIMIT).** Queries like
  `SELECT ... ORDER BY score DESC LIMIT 10` are now supported. The stream
  table stores only the top N rows and updates efficiently.

- **Diamond dependency consistency.** When multiple stream tables share common
  sources and feed into the same downstream table (a "diamond" pattern), they
  can now be refreshed as an atomic group — either all succeed or all roll
  back. This prevents inconsistent reads at convergence points. Controlled via
  the `diamond_consistency` parameter (default: `'atomic'`).

- **Multi-database auto-discovery.** The background scheduler now automatically
  finds and services all databases on the server where pg_trickle is installed.
  No manual `pg_trickle.database` configuration required — just install the
  extension and the scheduler discovers it.

### Fixed

- Fixed IMMEDIATE mode incorrectly trying to read from change buffer tables
  (which don't exist in that mode) for certain aggregate queries.
- Fixed type mismatches when join queries had unchanged source tables producing
  empty change sets.
- Fixed join condition column order being swapped when the right-side table was
  written first in the `ON` clause (e.g. `ON r.id = l.id`).
- Fixed dbt macros silently rolling back stream table creation because dbt
  wraps statements in a `ROLLBACK` by default.
- Fixed `LIMIT ALL` being incorrectly rejected as an unsupported LIMIT clause.
- Fixed false "query may produce incorrect incremental results" warnings on
  simple arithmetic like `depth + 1` or `path || name`.
- Fixed auto-created indexes using the wrong column name when the query had a
  column alias (e.g. `SELECT id AS department_id`).

---

## [0.1.3] — TPC-H Correctness, Window Functions & Aggregate Fixes

Major hardening release with 50 improvements across correctness, robustness,
operational safety, and test coverage.

### Added

- **DDL change tracking expanded.** `ALTER TYPE`, `ALTER POLICY`, and
  `ALTER DOMAIN` on source tables are now detected and trigger a rebuild of
  affected stream tables. Previously only column changes were tracked.
- **Recursive query safety guard.** Recursive CTEs (`WITH RECURSIVE`) are now
  checked for non-monotonic terms that could produce incorrect incremental
  results.
- **Read replica awareness.** The background scheduler detects when it's
  running on a read replica and skips refresh work, preventing errors.
- **Range aggregates rejected.** `RANGE_AGG` and `RANGE_INTERSECT_AGG` are
  now properly rejected in incremental mode with a clear error.
- **Refresh history: row counts.** Refresh history now records how many rows
  were inserted, updated, and deleted in each refresh cycle.
- **Change buffer alerts.** New `pg_trickle.buffer_alert_threshold` setting
  lets you configure when to be warned about growing change buffers.
- **`st_auto_threshold()` function.** Shows the current adaptive threshold
  that decides when to switch between incremental and full refresh.
- **Wide table optimization.** Tables with more than 50 columns use a hash
  shortcut during refresh merges, improving performance.
- **Change buffer security.** Internal change buffer tables are no longer
  accessible to `PUBLIC`.
- **Documentation.** PgBouncer compatibility, keyless table limitations, delta
  memory bounds, sequential processing rationale, and connection overhead are
  all now documented in the FAQ.

#### TPC-H Correctness Suite: 22/22 Queries Passing

The TPC-H-derived correctness test suite (22 industry-standard analytical
queries) now passes completely across multiple rounds of data changes. This
validates that incremental refreshes produce identical results to full
recomputation for complex real-world query patterns.

### Fixed

#### Window Function Correctness

Fixed incremental maintenance of window functions (ROW_NUMBER, RANK,
DENSE_RANK, NTILE, LAG/LEAD, SUM OVER, etc.) to correctly handle:
- Non-RANGE frame types
- Ranking functions over tied values
- Window functions wrapping aggregates (e.g. `RANK() OVER (ORDER BY SUM(x))`)
- Multiple window functions with different PARTITION BY clauses

#### INTERSECT / EXCEPT Correctness

Fixed incremental maintenance of `INTERSECT` and `EXCEPT` queries that
produced wrong results due to invalid SQL generation.

#### EXISTS / IN with OR Correctness

Fixed `EXISTS` and `IN` subqueries combined with `OR` in WHERE clauses that
produced wrong results.

#### Aggregate Correctness

- `MIN` / `MAX` now correctly rescan the source table when the current
  minimum or maximum value is deleted.
- `STRING_AGG(... ORDER BY ...)` and `ARRAY_AGG(... ORDER BY ...)` no longer
  silently drop the ORDER BY clause.

---

## [0.1.2] — Incremental Correctness Fixes & Project Rename

### Changed
#### Internal Code Quality: Integration Test Suite Hardening

Completed a full hardening pass of the integration test suite, bringing all items in `PLAN_TEST_EVALS_INTEGRATION.md` to done:
- **Multiset validation** — Extracted `assert_sets_equal()` helper relying on EXCEPT/UNION ALL SQL logic and applied it to workflow tests to ensure storage table state correctly matches the defining query post-refresh.
- **Round-trip notifications** — `pg_trickle_alert` notifications now verify receipt end-to-end via `sqlx::PgListener`.
- **DVM operators** — Added unit coverage for complex semi/anti-join behaviors (multi-column, filtered, complementary), multi-table join chains for inner and full joins, and `proptest!` fuzz tests enforcing generated SQL invariants across INNER, SEMI, and ANTI joins.
- **Resilience and edge cases** — Test coverage for ST drop cascades verifying dependent object removal, exact error escalation thresholds, and scheduler job lifecycles across queued mock states.
- **Cleanups** — Standardized naming practices (`test_workflow_*`, `test_infra_*`) and eliminated clock-bound flakes by widening staleness assertions.


#### Project Renamed from pg_stream to pg_trickle

Renamed the entire project from **pg_stream** to **pg_trickle** to avoid a
naming collision with an unrelated project. If you were using the old name,
all configuration prefixes changed from `pg_stream.*` to `pg_trickle.*`, and
the SQL schemas changed from `pgstream` to `pgtrickle`. The "stream tables"
terminology is unchanged.

### Fixed

Fixed numerous incremental computation bugs discovered while building a
comprehensive correctness test suite based on all 22 TPC-H analytical queries:

- **Inner join double-counting.** When both sides of a join had changes in
  the same refresh cycle, some rows were counted twice.
- **Shared source cleanup.** Cleaning up processed changes for one stream
  table could accidentally delete entries still needed by another stream
  table sharing the same source.
- **Scalar aggregate identity mismatch.** Queries like `SELECT SUM(amount)
  FROM orders` could produce mismatched row identifiers between the
  incremental and merge phases. AVG also failed to recompute correctly
  after partial group changes.
- **EXISTS / NOT EXISTS snapshots.** Incremental maintenance of `EXISTS` and
  `NOT EXISTS` subqueries missed pre-change state, producing wrong results.
- **Column resolution in complex joins.** Several fixes for column name
  resolution in multi-table joins and nested subqueries.
- **COUNT(*) rendering.** `COUNT(*)` was sometimes rendered as `COUNT()`
  (missing the star), causing SQL errors.
- **Subquery rewriting.** Several subquery patterns (correlated vs
  non-correlated scalar subqueries, derived tables in FROM) were incorrectly
  rewritten, blocking certain queries from being created.
- **Cleanup worker crash.** The background cleanup worker no longer crashes
  when it encounters entries for stream tables that were dropped mid-cycle.

### Added

#### TPC-H Correctness Test Suite

Added a comprehensive correctness test suite based on all 22 TPC-H analytical
queries. These tests verify that incremental refreshes produce identical
results to a full recompute after INSERT, DELETE, and UPDATE mutations.
20 of 22 queries can be created as stream tables; 15 pass full correctness
checks at this point (improved to 22/22 in v0.1.3).

---

## [0.1.1] — CloudNativePG Image & Test Hardening

### Changed
#### Internal Code Quality: Integration Test Suite Hardening

Completed a full hardening pass of the integration test suite, bringing all items in `PLAN_TEST_EVALS_INTEGRATION.md` to done:
- **Multiset validation** — Extracted `assert_sets_equal()` helper relying on EXCEPT/UNION ALL SQL logic and applied it to workflow tests to ensure storage table state correctly matches the defining query post-refresh.
- **Round-trip notifications** — `pg_trickle_alert` notifications now verify receipt end-to-end via `sqlx::PgListener`.
- **DVM operators** — Added unit coverage for complex semi/anti-join behaviors (multi-column, filtered, complementary), multi-table join chains for inner and full joins, and `proptest!` fuzz tests enforcing generated SQL invariants across INNER, SEMI, and ANTI joins.
- **Resilience and edge cases** — Test coverage for ST drop cascades verifying dependent object removal, exact error escalation thresholds, and scheduler job lifecycles across queued mock states.
- **Cleanups** — Standardized naming practices (`test_workflow_*`, `test_infra_*`) and eliminated clock-bound flakes by widening staleness assertions.


#### CloudNativePG Extension Image

Replaced the full PostgreSQL Docker image (~400 MB) with a minimal
extension-only image (< 10 MB) following the CloudNativePG Image Volume
Extensions specification. This means faster pulls and less disk usage in
Kubernetes deployments. The image contains just the extension files —
no full PostgreSQL server.

---

## [0.1.0] — Initial Release

Initial release of pg_trickle — a PostgreSQL extension that keeps query results
automatically up to date as your data changes.

### Core Concept

Define a SQL query and a schedule. pg_trickle creates a **stream table** that
stores the query's results and keeps them fresh — either on a schedule
(every N seconds) or in real time. When data in your source tables changes,
only the affected rows are recomputed instead of re-running the entire query.

### What You Can Do

- **Create stream tables** from `SELECT` queries — joins, aggregates,
  subqueries, CTEs, window functions, set operations, and more.
- **Automatic refresh** — a background scheduler refreshes stream tables in
  dependency order. You can also trigger refreshes manually.
- **Incremental updates** — the engine automatically figures out how to update
  only the rows that changed, instead of recomputing everything. This works
  for most query patterns including multi-table joins and aggregates.
- **Views as sources** — views referenced in your query are automatically
  expanded so change tracking works on the underlying tables.
- **Tables without primary keys** — supported via content hashing. Tables with
  primary keys get better performance.
- **Hybrid change tracking** — starts with lightweight triggers (no special
  PostgreSQL configuration needed). Can automatically switch to WAL-based
  tracking for lower overhead when `wal_level = logical` is available.
- **Multi-database support** — the scheduler automatically discovers all
  databases on the server where the extension is installed.
- **User triggers on stream tables** — your own `AFTER` triggers on stream
  tables fire correctly during incremental refreshes.
- **DDL awareness** — `ALTER TABLE`, `DROP TABLE`, `CREATE OR REPLACE
  FUNCTION`, and other DDL on source tables or functions used in your query
  are detected and handled automatically.

### SQL Support

Broad coverage of SQL features:

- **Joins:** INNER, LEFT, RIGHT, FULL OUTER, NATURAL, LATERAL subqueries,
  LATERAL set-returning functions (`unnest`, `jsonb_array_elements`, etc.)
- **Aggregates:** 39 functions including COUNT, SUM, AVG, MIN, MAX,
  STRING_AGG, ARRAY_AGG, JSON_ARRAYAGG, JSON_OBJECTAGG, statistical
  regression functions (CORR, COVAR_*, REGR_*), and ordered-set aggregates
  (MODE, PERCENTILE_CONT, PERCENTILE_DISC)
- **Window functions:** ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD,
  SUM OVER, etc. with full frame clause support
- **Set operations:** UNION, UNION ALL, INTERSECT, EXCEPT
- **Subqueries:** in FROM, EXISTS/NOT EXISTS, IN/NOT IN, scalar subqueries
- **CTEs:** `WITH` and `WITH RECURSIVE`
- **Special syntax:** DISTINCT, DISTINCT ON, GROUPING SETS / CUBE / ROLLUP,
  CASE WHEN, COALESCE, JSON_TABLE (PostgreSQL 17+)
- **Unsafe function detection:** queries using non-deterministic functions
  like `random()` are rejected with a clear error

### Monitoring

- `explain_st()` — shows the incremental computation plan
- `st_refresh_stats()`, `get_refresh_history()`, `get_staleness()` — refresh
  performance and status
- `slot_health()` — WAL replication slot health
- `check_cdc_health()` — change tracking health per source table
- `stream_tables_info` and `pg_stat_stream_tables` views
- NOTIFY alerts for stale data, errors, and refresh events

### Documentation

- Architecture guide, SQL reference, configuration reference, FAQ,
  getting-started tutorial, and deep-dive tutorials.

### Known Limitations

- `TABLESAMPLE`, `LIMIT` / `OFFSET`, `FOR UPDATE` / `FOR SHARE` — not yet
  supported (clear error messages).
- Window functions inside expressions (e.g. `CASE WHEN ROW_NUMBER() ...`) —
  not yet supported.
- Circular stream table dependencies — not yet supported.
