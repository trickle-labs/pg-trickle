> **Archive notice (added v0.71.0):** This file reflects the v0.9.0 implementation
> cycle and is preserved verbatim for historical reference.  Active planning now
> lives in [ROADMAP.md](../../ROADMAP.md) and [plans/INDEX.md](../INDEX.md).

# pg_trickle — Implementation Plan

> **Implementation Status (v0.9.0 Cycle)**
> Most core architectural phases (Phases 1-12) are now implemented. The remaining critical features necessary to fully close out the v0.9.0 milestone are:
> - **F15**: Selective CDC Column Capture (Phase 6 Integration) — ✅ **Complete**
> - **F40**: Extension Upgrade Migrations & DB Schema Stability — *Code complete, awaiting final package*
> - **EC-03**: Window-in-expression DIFFERENTIAL fallback warning — ✅ **Complete**
> - **A8**: `pgt_refresh_groups` SQL API — ✅ **Complete**
> - **P3-4**: Index-aware MERGE planning (seqscan disable for small deltas) — ✅ **Complete**
> - **P3-5**: `auto_backoff` GUC for falling-behind stream tables — ✅ **Complete**
> - **B3-1**: Delta-branch pruning for zero-change sources — ✅ **Complete**
> - **P3-3**: Scalar subquery C₀ gating behind inner-delta existence check — ✅ **Complete**
> - **P3-1**: Window partition O(partition_size) cost documented — ✅ **Complete**
> - **P2-5**: `changed_cols` bitmask filter in delta scan CTE — ✅ **Complete**
> - **P2-3**: DISTINCT index-driven `__pgt_count` lookup — ✅ **Complete**
> - **P2-7**: Delta predicate pushdown into scan CTE — ✅ **Complete**
> - **P2-1**: Recursive CTE DRed for DIFFERENTIAL mode — ⏭️ **Deferred to v0.10.0** (high risk; fallback to recomputation is correct)
> - **P2-2**: SUM NULL-transition rescan optimization — ✅ **Done** (`__pgt_aux_nonnull_*` auxiliary column; algebraic NULL-transition correction; full-group rescan eliminated)
> - **P2-4**: Materialized view sources in IMMEDIATE mode — ✅ **Done** (polling-based CDC via snapshot-comparison, same as foreign table support)
> - **P2-6**: LATERAL subquery inner-source scoping — ✅ **Done** (correlation predicate extraction + scoped EXISTS join against change buffer)
> - **P3-2**: Welford auxiliary columns for CORR/COVAR/REGR_* — ✅ **Done** (5 cross-product aux columns per agg; O(1) algebraic merge formulas for all 12 functions)
> - **B3-2**: Merged-delta weight aggregation — ✅ **Done** (replaces `DISTINCT ON` with `GROUP BY __pgt_row_id, SUM(weight)` + `HAVING <> 0`; correctness proven by B3-3 property tests)
> - **B3-3**: Property-based tests for B3-2 — ✅ **Done** (6 diamond-flow E2E property tests: inner join, left join, full join, aggregate, multi-root, deep diamond)
> - **SF-1/SF-2/SF-3**: SQL comment injection in `build_snapshot_sql` and `deparse_from_item` — ✅ **Done** (replaced with `panic!` / `PgTrickleError::UnsupportedOperator`)
> - **SF-7**: Empty scalar subquery column guard — ✅ **Done** (panic instead of silent NULL emission)
> - **SF-9**: NULL-safe PK join in UPDATE trigger — ✅ **Done** (`IS NOT DISTINCT FROM` instead of `=`)
> - **SF-10**: TRUNCATE + INSERT ordering — ✅ **Done** (verified safe: full refresh reads source directly)
> - **SF-12**: DiamondSchedulePolicy CPU cost documentation — ✅ **Done** (added to CONFIGURATION.md)
> - **SF-13**: B-2 roadmap inconsistency — ✅ **Done** (marked as completed by G-4/P2-7 in v0.9.0)
> - **SF-4**: Project node in aggregate rescan FROM clause — ✅ **Done** (`child_to_from_sql` wraps Project in subquery with projected expressions)
> - **SF-6**: EXCEPT/INTERSECT count columns through Project — ✅ **Done** (`diff_project` forwards `__pgt_count_l`/`__pgt_count_r`)
> - **SF-8**: Lateral inner-change branch sentinel — ✅ **Done** (changed from `0` to `i64::MIN`)
> - **SF-5**: EC-01 pre-change snapshot boundary — ✅ **Done** (≤2-scan threshold documented with 5 boundary tests + DVM_OPERATORS.md limitation note)
> - **SF-11**: WAL publication post-creation partitioning — ✅ **Done** (`check_publication_health()` detects and rebuilds stale publications)
> - **A-4**: Index-Aware MERGE Planning — ✅ **Done** (covering index with INCLUDE for ≤8-column schemas + P3-4 planner hints)
> - **C-4**: Change Buffer Compaction — ✅ **Done** (`compact_change_buffer()` with net-zero elimination, advisory lock, `change_id` PK)
> - **B-4**: Cost-Based Refresh Strategy — ✅ **Done** (cost model + adaptive threshold already active since v0.9.0; verified and documented)

### F40 Status Update

Implemented for v0.9.0:

- `sql/pg_trickle--0.8.0--0.9.0.sql` covers all new SQL objects (new table `pgt_refresh_groups`, new function `restore_stream_tables`).
- `sql/archive/pg_trickle--0.9.0.sql` committed as full-install baseline for diff-based checks.
- Upgrade completeness gate (`scripts/check_upgrade_completeness.sh`) validates functions, views, event triggers, tables, indexes, **and column drift in existing tables** (CHECK 6).
- `test_upgrade_v090_catalog_additions` (L15) verifies `pgt_refresh_groups` columns, unique constraint, and empty-state after fresh install.

One item remains before F40 is fully closed:

- **Release finalization:** run `cargo pgrx package`, diff the output against `sql/archive/pg_trickle--0.9.0.sql`, resolve any discrepancies, then tag the v0.9.0 release. This is a pre-release checklist step, not a code change.

> **Implementation Status (v0.9.0 Cycle)**
> Most core architectural phases (Phases 1-12) are now implemented. The remaining critical features necessary to fully close out the v0.9.0 milestone are:
> - **F15**: Selective CDC Column Capture (Phase 6 Integration) — ✅ **Complete**
> - **F40**: Extension Upgrade Migrations & DB Schema Stability - *Code complete, awaiting final package*
> - **EC-03**: Window-in-expression DIFFERENTIAL fallback warning — ✅ **Complete**
> - **A8**: `pgt_refresh_groups` SQL API — ✅ **Complete**
> - **P3-4**: Index-aware MERGE planning (seqscan disable for small deltas) — ✅ **Complete**
> - **P3-5**: `auto_backoff` GUC for falling-behind stream tables — ✅ **Complete**
> - **B3-1**: Delta-branch pruning for zero-change sources — ✅ **Complete**
> - **P3-3**: Scalar subquery C₀ gating behind inner-delta existence check — ✅ **Complete**
> - **P3-1**: Window partition O(partition_size) cost documented — ✅ **Complete**
> - **P2-5**: `changed_cols` bitmask filter in delta scan CTE — ✅ **Complete**
> - **P2-3**: DISTINCT index-driven `__pgt_count` lookup — ✅ **Complete**
> - **P2-7**: Delta predicate pushdown into scan CTE — ✅ **Complete**
> - **P2-1**: Recursive CTE DRed for DIFFERENTIAL mode — ⏭️ **Deferred to v0.10.0** (high risk; fallback to recomputation is correct)
> - **P2-2**: SUM NULL-transition rescan optimization — ✅ **Done** (`__pgt_aux_nonnull_*` auxiliary column; algebraic NULL-transition correction; full-group rescan eliminated)
> - **P2-4**: Materialized view sources in IMMEDIATE mode — ✅ **Done** (polling-based CDC via snapshot-comparison, same as foreign table support)
> - **P2-6**: LATERAL subquery inner-source scoping — ✅ **Done** (correlation predicate extraction + scoped EXISTS join against change buffer)
> - **P3-2**: Welford auxiliary columns for CORR/COVAR/REGR_* — ✅ **Done** (5 cross-product aux columns per agg; O(1) algebraic merge formulas for all 12 functions)
> - **B3-2**: Merged-delta weight aggregation — ✅ **Done** (replaces `DISTINCT ON` with `GROUP BY __pgt_row_id, SUM(weight)` + `HAVING <> 0`; correctness proven by B3-3 property tests)
> - **B3-3**: Property-based tests for B3-2 — ✅ **Done** (6 diamond-flow E2E property tests: inner join, left join, full join, aggregate, multi-root, deep diamond)
> - **SF-1/SF-2/SF-3**: SQL comment injection in `build_snapshot_sql` and `deparse_from_item` — ✅ **Done** (replaced with `panic!` / `PgTrickleError::UnsupportedOperator`)
> - **SF-7**: Empty scalar subquery column guard — ✅ **Done** (panic instead of silent NULL emission)
> - **SF-9**: NULL-safe PK join in UPDATE trigger — ✅ **Done** (`IS NOT DISTINCT FROM` instead of `=`)
> - **SF-10**: TRUNCATE + INSERT ordering — ✅ **Done** (verified safe: full refresh reads source directly)
> - **SF-12**: DiamondSchedulePolicy CPU cost documentation — ✅ **Done** (added to CONFIGURATION.md)
> - **SF-13**: B-2 roadmap inconsistency — ✅ **Done** (marked as completed by G-4/P2-7 in v0.9.0)
> - **SF-4**: Project node in aggregate rescan FROM clause — ✅ **Done** (`child_to_from_sql` wraps Project in subquery with projected expressions)
> - **SF-6**: EXCEPT/INTERSECT count columns through Project — ✅ **Done** (`diff_project` forwards `__pgt_count_l`/`__pgt_count_r`)
> - **SF-8**: Lateral inner-change branch sentinel — ✅ **Done** (changed from `0` to `i64::MIN`)
> - **SF-5**: EC-01 pre-change snapshot boundary — ✅ **Done** (≤2-scan threshold documented with 5 boundary tests + DVM_OPERATORS.md limitation note)
> - **SF-11**: WAL publication post-creation partitioning — ✅ **Done** (`check_publication_health()` detects and rebuilds stale publications)
> - **A-4**: Index-Aware MERGE Planning — ✅ **Done** (covering index with INCLUDE for ≤8-column schemas + P3-4 planner hints)
> - **C-4**: Change Buffer Compaction — ✅ **Done** (`compact_change_buffer()` with net-zero elimination, advisory lock, `change_id` PK)
> - **B-4**: Cost-Based Refresh Strategy — ✅ **Done** (cost model + adaptive threshold already active since v0.9.0; verified and documented)

### F15 Status Update (Selective CDC Column Capture)

**Goal:** Optimize I/O by only tracking columns in the CDC layer which are actually referenced in the query lineage of dependent Stream Tables.

**✅ Fully implemented for v0.9.0.**

All components are in place:

1. **Catalog helper** — `StDependency::union_referenced_columns_for_source(source_oid)` in `src/catalog.rs`:
   - Queries `pgt_dependencies.columns_used` for all STs that share the given base-table source.
   - Returns the union as a sorted `Vec<String>`, or `None` when any ST has `columns_used = NULL` (meaning "all columns needed").

2. **CDC resolver** — `cdc::resolve_referenced_column_defs(source_oid)` in `src/cdc.rs`:
   - Calls the catalog helper to get the union of required columns.
   - Always adds PK columns (required for `pk_hash` correctness).
   - Filters `resolve_source_column_defs` to the keep-set, preserving ordinal order.
   - Falls back to full capture on `None`, empty union, or if the filter would drop all columns.

3. **Monitoring helper** — `cdc::is_selective_capture_active(source_oid)` in `src/cdc.rs`, exposed via `pgtrickle.check_cdc_health()` as the `selective_capture` column:
   - Returns `true` when the referenced column set is a strict subset of the full column list.

4. **Wired into creation path** — `setup_cdc_for_source` in `src/api.rs`:
   - Replaced `resolve_source_column_defs` with `resolve_referenced_column_defs`.
   - At ST creation time, the dependency rows are already written, so the union query finds them.

5. **Wired into rebuild path** — `rebuild_cdc_trigger_function` and `rebuild_cdc_trigger` in `src/cdc.rs`:
   - Both now use `resolve_referenced_column_defs` so schema-change rebuilds also respect column pruning.

6. **Unit tests** — 6 pure-Rust tests for `filter_cdc_columns` covering:
   - `None`/empty referenced → full fallback
   - Filtering to referenced + PK only
   - Ordinal order preservation
   - Case-insensitive column name matching
   - Empty-result safety fallback

7. **E2E integration tests** — 3 tests in `tests/e2e_cdc_tests.rs`:
   - `test_f15_selective_cdc_buffer_has_only_referenced_columns` — verifies buffer only contains PK + referenced columns; unreferenced columns are pruned; `check_cdc_health()` reports `selective_capture = true`.
   - `test_f15_selective_cdc_buffer_expands_for_second_stream_table` — verifies that adding a second ST requiring an additional column triggers buffer expansion (union semantics).
   - `test_f15_select_star_falls_back_to_full_capture` — verifies `SELECT *` sources fall back to full capture; `check_cdc_health()` reports `selective_capture = false`.

**Note on `SELECT *` sources:** When a ST's defining query contains `SELECT *`, the parser emits `columns_used = NULL` (no column list) for that source. The catalog stores `NULL` in `pgt_dependencies.columns_used`, and `union_referenced_columns_for_source` returns `None`, triggering full capture for that source. This is correct and safe — selective capture only activates when every downstream ST has an explicit column list.

### F40 Status Update

Implemented for v0.9.0:

- `sql/pg_trickle--0.8.0--0.9.0.sql` covers all new SQL objects (new table `pgt_refresh_groups`, new function `restore_stream_tables`).
- `sql/archive/pg_trickle--0.9.0.sql` committed as full-install baseline for diff-based checks.
- Upgrade completeness gate (`scripts/check_upgrade_completeness.sh`) validates functions, views, event triggers, tables, indexes, **and column drift in existing tables** (CHECK 6).
- `test_upgrade_v090_catalog_additions` (L15) verifies `pgt_refresh_groups` columns, unique constraint, and empty-state after fresh install.

One item remains before F40 is fully closed:

- **Release finalization:** run `cargo pgrx package`, diff the output against `sql/archive/pg_trickle--0.9.0.sql`, resolve any discrepancies, then tag the v0.9.0 release. This is a pre-release checklist step, not a code change.

## Streaming tables for PostgreSQL 18 as a Rust Extension

A loadable PostgreSQL 18 extension, written in Rust (pgrx), that provides declarative Stream Tables with automated lag-based scheduling and incremental view maintenance (IVM), implementing Delayed View Semantics (DVS). Users create Stream Tables via SQL functions specifying a defining query, target lag, and refresh mode. A background worker scheduler maintains the DAG of STs, triggers refreshes, and applies incremental deltas computed via a query differentiation engine.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Phase 0 — Project Scaffolding](#phase-0--project-scaffolding)
- [Phase 1 — Catalog & Metadata Layer](#phase-1--catalog--metadata-layer)
- [Phase 2 — User-Facing SQL API](#phase-2--user-facing-sql-api)
- [Phase 3 — Change Data Capture via Row-Level Triggers](#phase-3--change-data-capture-via-row-level-triggers)
- [Phase 4 — Dependency DAG & Scheduling](#phase-4--dependency-dag--scheduling)
- [Phase 5 — Refresh Executor](#phase-5--refresh-executor)
- [Phase 6 — Incremental View Maintenance Engine](#phase-6--incremental-view-maintenance-engine-the-core)
- [Phase 7 — DDL Tracking & Schema Evolution](#phase-7--ddl-tracking--schema-evolution)
- [Phase 8 — Version Management & DVS](#phase-8--version-management--dvs)
- [Phase 9 — Monitoring & Observability](#phase-9--monitoring--observability)
- [Phase 10 — Error Handling & Resilience](#phase-10--error-handling--resilience)
- [Phase 11 — Testing Strategy](#phase-11--testing-strategy)
- [Phase 12 — Documentation & Packaging](#phase-12--documentation--packaging)
- [Verification Criteria](#verification-criteria)
- [Key Design Decisions](#key-design-decisions)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        User SQL Session                         │
│  pg_trickle.create_stream_table('enriched_orders',                   │
│    'SELECT o.*, c.name FROM orders o JOIN customers c ...',     │
│    '5m', 'INCREMENTAL')                        │
└───────────────────────────┬──────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│                     SQL API Layer (api.rs)                       │
│  create / alter / drop / refresh / status functions              │
│  Query validation, dependency extraction, cycle detection        │
└───────────────────────────┬──────────────────────────────────────┘
                            │
         ┌──────────────────┼──────────────────┐
         ▼                  ▼                  ▼
┌────────────────┐ ┌────────────────┐ ┌────────────────────────┐
│  Catalog Layer │ │  DAG Manager   │ │  DDL Event Triggers    │
│  (catalog.rs)  │ │  (dag.rs)      │ │  (hooks.rs)            │
│                │ │                │ │                        │
│  pg_trickle.stream_ │ │  Topological   │ │  Track ALTER/DROP on   │
│  tables        │ │  sort, cycle   │ │  upstream tables →     │
│  pg_trickle.st_deps  │ │  detection,    │ │  mark REINITIALIZE     │
│  pg_trickle.st_hist  │ │  DOWNSTREAM    │ │                        │
│  pg_trickle.st_cdc   │ │  resolution    │ │                        │
└────────────────┘ └───────┬────────┘ └────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│               Scheduler Background Worker (scheduler.rs)         │
│                                                                  │
│  Main loop (wakes every pg_trickle.scheduler_interval_ms):             │
│  1. Consume CDC changes → change buffer tables                   │
│  2. Compute data timestamps (canonical periods 48·2ⁿ s)         │
│  3. Topological refresh ordering                                 │
│  4. Execute refreshes (sequential or parallel)                   │
│  5. Health monitoring, skip detection, error handling             │
└──────────┬───────────────────────────────────┬───────────────────┘
           │                                   │
           ▼                                   ▼
┌─────────────────────────┐     ┌──────────────────────────────────┐
│  CDC Engine (cdc.rs)    │     │  Refresh Executor (refresh.rs)   │
│                         │     │                                  │
│  Row-level triggers     │     │  NO_DATA → just advance ts       │
│  per tracked table      │     │  FULL → INSERT OVERWRITE         │
│  Writes changes via     │     │  INCREMENTAL → delta query +     │
│  PL/pgSQL to buffer     │     │    MERGE (DELETE+INSERT)         │
│  Change buffer tables   │     │  REINITIALIZE → full recompute   │
│  in pg_trickle_changes schema │     │    + rebuild row IDs             │
└─────────────────────────┘     └──────────────┬───────────────────┘
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────────────┐
│              IVM Engine (dvm/)                                    │
│                                                                  │
│  parser.rs   — Parse defining query → operator tree              │
│  diff.rs     — Query differentiation: Q → ΔQ                    │
│  row_id.rs   — Row ID generation (xxHash-based)                  │
│  operators/  — Per-operator differentiation rules:               │
│    scan.rs, project.rs, filter.rs, join.rs,                      │
│    aggregate.rs, distinct.rs, window.rs, union_all.rs,           │
│    outer_join.rs                                                 │
│                                                                  │
│  Output: SQL delta query with __pgt_row_id + __pgt_action      │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Base Tables ──TRIGGER──▶ Change Buffer Tables
                     (pg_trickle_changes.changes_<oid>)
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
        ΔScan(T1)     ΔScan(T2)    ΔScan(T3)
            │             │             │
            └──────┬──────┘             │
                   ▼                    │
              ΔJoin(T1,T2)              │
                   │                    │
                   └────────┬───────────┘
                            ▼
                      ΔAggregate(...)
                            │
                            ▼
                   Delta Result Set
                   (__pgt_row_id, __pgt_action, cols...)
                            │
                            ▼
                   MERGE into ST storage table
                   (DELETE old + INSERT new)
```

---

## Phase 0 — Project Scaffolding

### Step 0.1 — Initialize pgrx project

```bash
cargo pgrx init --pg18 /path/to/pg18/bin/pg_config
cargo pgrx new pg_trickle
```

Pin pgrx to v0.17.x in `Cargo.toml`. Set `edition = "2024"`, target PostgreSQL 18.x.

### Step 0.2 — Define crate module structure

```
src/
├── lib.rs              # Extension entry, _PG_init, pg_module_magic!
├── config.rs           # GUC variables
├── error.rs            # Error types
├── shmem.rs            # Shared memory structures
├── catalog.rs          # Metadata tables, CRUD
├── api.rs              # User-facing SQL functions
├── dag.rs              # DAG construction, topological sort, cycle detection
├── scheduler.rs        # Background worker, refresh scheduling
├── cdc.rs              # Trigger-based CDC, change buffer management
├── refresh.rs          # Refresh executor (full/incremental/reinit/no_data)
├── hooks.rs            # Process utility hook, object access hook, event triggers
├── version.rs          # Frontier management, data timestamp tracking
├── monitor.rs          # Custom cumulative statistics, monitoring views
└── dvm/
    ├── mod.rs          # IVM engine entry point
    ├── parser.rs       # Defining query analysis → operator tree
    ├── diff.rs         # Query differentiation framework
    ├── row_id.rs       # Row ID generation (xxHash)
    └── operators/
        ├── mod.rs
        ├── scan.rs     # Base table scan differentiation
        ├── project.rs  # Projection differentiation
        ├── filter.rs   # Filter/WHERE differentiation
        ├── join.rs     # Inner join differentiation
        ├── aggregate.rs # GROUP BY aggregate differentiation
        ├── distinct.rs # DISTINCT differentiation
        ├── window.rs   # Window function differentiation (Phase 2)
        ├── union_all.rs # UNION ALL differentiation (Phase 2)
        └── outer_join.rs # Outer join differentiation (Phase 2)
```

### Step 0.3 — GUC variables (`config.rs`)

Register via pgrx's GUC API in `_PG_init()`:

| Variable | Type | Default | Description |
|---|---|---|---|
| `pg_trickle.enabled` | bool | `true` | Master enable/disable switch |
| `pg_trickle.scheduler_interval_ms` | int | `1000` | Scheduler wake interval (ms) |
| `pg_trickle.min_target_lag_seconds` | int | `60` | Minimum allowed target lag |
| `pg_trickle.max_consecutive_errors` | int | `3` | Errors before auto-suspend |
| `pg_trickle.change_buffer_schema` | string | `pg_trickle_changes` | Schema for change buffer tables |
| `pg_trickle.log_level` | enum | `info` | Extension log verbosity |
| `pg_trickle.max_concurrent_refreshes` | int | `4` | Max parallel refresh workers |

### Step 0.4 — Shared preload enforcement

In `_PG_init()`, check `pg_sys::process_shared_preload_libraries_in_progress`. If false, emit `ereport(ERROR)` instructing the user to add `pg_trickle` to `shared_preload_libraries` in `postgresql.conf`. This is required for background workers and shared memory.

### Step 0.5 — Shared memory initialization (`shmem.rs`)

Define shared memory structures for scheduler↔backend coordination:

```rust
#[derive(Copy, Clone)]
struct PgTrickleSharedState {
    dag_version: u64,             // Incremented on DAG changes
    scheduler_pid: i32,           // PID of scheduler worker
    scheduler_running: bool,      // Is scheduler alive?
    last_scheduler_wake: i64,     // Unix timestamp of last wake
}

static PGS_STATE: PgLwLock<PgTrickleSharedState> = PgLwLock::new();
static DAG_REBUILD_SIGNAL: PgAtomic<u64> = PgAtomic::new();
```

Initialize via `pg_shmem_init!()` in `_PG_init()`.

When a user creates/alters/drops a ST, they increment `DAG_REBUILD_SIGNAL` atomically. The scheduler reads this on each wake cycle and rebuilds the DAG if it changed.

---

## Phase 1 — Catalog & Metadata Layer

### Step 1.1 — Extension schema and catalog tables

Create via `extension_sql!()` in the migration script (`pg_trickle--0.1.0.sql`):

```sql
CREATE SCHEMA IF NOT EXISTS pgtrickle;
CREATE SCHEMA IF NOT EXISTS pg_trickle_changes;

-- Core ST metadata
CREATE TABLE pg_trickle.pgt_stream_tables (
    pgt_id       BIGSERIAL PRIMARY KEY,
    pgt_relid    OID NOT NULL UNIQUE,        -- OID of underlying storage table
    pgt_name     TEXT NOT NULL,
    pgt_schema   TEXT NOT NULL,
    defining_query TEXT NOT NULL,
    target_lag  INTERVAL,                   -- NULL = DOWNSTREAM
    refresh_mode TEXT NOT NULL DEFAULT 'INCREMENTAL'
                 CHECK (refresh_mode IN ('FULL', 'INCREMENTAL')),
    status      TEXT NOT NULL DEFAULT 'INITIALIZING'
                 CHECK (status IN ('INITIALIZING', 'ACTIVE', 'SUSPENDED', 'ERROR')),
    is_populated BOOLEAN NOT NULL DEFAULT FALSE,
    data_timestamp TIMESTAMPTZ,             -- NULL until initialized
    frontier    JSONB,                      -- Per-source version map
    last_refresh_at TIMESTAMPTZ,
    consecutive_errors INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON pg_trickle.pgt_stream_tables (status);

-- DAG edges
CREATE TABLE pg_trickle.pgt_dependencies (
    pgt_id        BIGINT NOT NULL REFERENCES pg_trickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    source_relid OID NOT NULL,
    source_type  TEXT NOT NULL CHECK (source_type IN ('TABLE', 'STREAM_TABLE', 'VIEW')),
    columns_used TEXT[],
    PRIMARY KEY (pgt_id, source_relid)
);

CREATE INDEX ON pg_trickle.pgt_dependencies (source_relid);

-- Refresh history / audit log
CREATE TABLE pg_trickle.pgt_refresh_history (
    refresh_id    BIGSERIAL PRIMARY KEY,
    pgt_id         BIGINT NOT NULL,
    data_timestamp TIMESTAMPTZ NOT NULL,
    start_time    TIMESTAMPTZ NOT NULL,
    end_time      TIMESTAMPTZ,
    action        TEXT NOT NULL
                   CHECK (action IN ('NO_DATA', 'FULL', 'INCREMENTAL', 'REINITIALIZE', 'SKIP')),
    rows_inserted BIGINT DEFAULT 0,
    rows_deleted  BIGINT DEFAULT 0,
    error_message TEXT,
    status        TEXT NOT NULL
                   CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED'))
);

CREATE INDEX ON pg_trickle.pgt_refresh_history (pgt_id, data_timestamp);

-- Per-source CDC trigger tracking (slot_name stores trigger name, e.g., 'pg_trickle_cdc_16384')
CREATE TABLE pg_trickle.pgt_change_tracking (
    source_relid      OID PRIMARY KEY,
    slot_name         TEXT NOT NULL,
    last_consumed_lsn PG_LSN,
    tracked_by_pgt_ids BIGINT[]
);
```

### Step 1.2 — Catalog CRUD operations (`catalog.rs`)

Implement Rust functions wrapping SPI calls:

```rust
pub struct StreamTableMeta {
    pub pgt_id: i64,
    pub pgt_relid: pg_sys::Oid,
    pub pgt_name: String,
    pub pgt_schema: String,
    pub defining_query: String,
    pub target_lag: Option<Interval>,
    pub refresh_mode: RefreshMode,
    pub status: StStatus,
    pub is_populated: bool,
    pub data_timestamp: Option<TimestampWithTimeZone>,
    pub frontier: Option<JsonB>,
    pub consecutive_errors: i32,
}

pub enum RefreshMode { Full, Incremental }
pub enum StStatus { Initializing, Active, Suspended, Error }

impl StreamTableMeta {
    pub fn insert(meta: &StreamTableMeta) -> Result<i64, PgTrickleError> { ... }
    pub fn get_by_name(schema: &str, name: &str) -> Result<Self, PgTrickleError> { ... }
    pub fn get_by_relid(relid: Oid) -> Result<Self, PgTrickleError> { ... }
    pub fn get_all_active() -> Result<Vec<Self>, PgTrickleError> { ... }
    pub fn update_status(pgt_id: i64, status: StStatus) -> Result<(), PgTrickleError> { ... }
    pub fn update_after_refresh(pgt_id: i64, data_ts: TimestampWithTimeZone,
                                frontier: JsonB) -> Result<(), PgTrickleError> { ... }
    pub fn increment_errors(pgt_id: i64) -> Result<i32, PgTrickleError> { ... }
    pub fn delete(pgt_id: i64) -> Result<(), PgTrickleError> { ... }
}

pub struct StDependency { ... }
pub struct RefreshRecord { ... }
// Similar CRUD implementations for dependencies and refresh history.
```

All catalog access goes through `Spi::connect()`. Error handling wraps PostgreSQL errors into `PgTrickleError` type.

---

## Phase 2 — User-Facing SQL API

### Step 2.1 — `pg_trickle.create_stream_table()`

**Signature:**

```sql
pg_trickle.create_stream_table(
    name         TEXT,              -- 'schema.table_name' or 'table_name'
    query        TEXT,              -- Defining SELECT query
    target_lag   INTERVAL DEFAULT '1 minute',  -- NULL for DOWNSTREAM
    refresh_mode TEXT DEFAULT 'INCREMENTAL',
    initialize   BOOLEAN DEFAULT TRUE
) RETURNS VOID
```

**Implementation (`api.rs` — `#[pg_extern(schema = "pgtrickle")]`):**

1. **Parse name** into `(schema, table_name)`. Default schema = `current_schema()`.

2. **Validate the defining query:**
   - Execute `SELECT * FROM (<query>) sub LIMIT 0` via SPI to verify syntax and get output column types. If it fails, propagate the error.
   - Parse the query via `pg_sys::raw_parser()` to obtain the raw parse tree.
   - Walk the `RangeTblEntry` list from the parse tree to extract all referenced relation OIDs. Classify each as `TABLE`, `VIEW`, or `STREAM_TABLE` (check `pg_trickle.pgt_stream_tables`).

3. **Cycle detection:**
   - Load existing DAG from `pg_trickle.pgt_dependencies`.
   - Add proposed edges (new ST → its sources).
   - Run Kahn's algorithm. If any nodes remain unprocessed, a cycle exists → error.

4. **Validate IVM feasibility** (if `refresh_mode = 'INCREMENTAL'`):
   - Analyze the parse tree structure for supported operators (see Phase 6 operator matrix).
   - If unsupported operators found, raise an error: `"INCREMENTAL mode unsupported for this query: <operator> is not yet differentiable. Use refresh_mode => 'FULL' or simplify the query."`

5. **Create the underlying storage table:**
   ```sql
   CREATE TABLE <schema>.<name> (
       __pgt_row_id BIGINT,
       -- ... all columns from the defining query ...
   )
   ```
   Derive the column list from the `LIMIT 0` result metadata.
   
   For aggregate queries (INCREMENTAL mode), also add auxiliary columns:
   - `__pgt_count BIGINT` — for COUNT/DISTINCT tracking
   - `__pgt_sum_<col> NUMERIC` — for each SUM/AVG aggregate
   
   Create a unique index on `__pgt_row_id`.

6. **Initialize** (if `initialize = true`):
   ```sql
   INSERT INTO <schema>.<name> (__pgt_row_id, <user_cols>, [__pgt_count, ...])
   SELECT <row_id_expr>, sub.*, [1, <col>, ...] FROM (<query>) sub
   ```
   Set `is_populated = true`, `data_timestamp = now()`.

7. **Insert catalog entries** into `pg_trickle.pgt_stream_tables` and `pg_trickle.pgt_dependencies`.

8. **Create CDC triggers** for any new base table sources not already tracked:
   - Create a PL/pgSQL trigger function `pg_trickle.pg_trickle_cdc_fn_<source_oid>()`
   - Create an `AFTER INSERT OR UPDATE OR DELETE` trigger `pg_trickle_cdc_<source_oid>` on the source table
   - Trigger writes change data (LSN, XID, action, row data as JSONB) to `pg_trickle_changes.changes_<source_oid>`
   - Insert tracking record into `pg_trickle.pgt_change_tracking` with trigger name

9. **Signal the scheduler** by incrementing `DAG_REBUILD_SIGNAL` in shared memory (no-op if shared memory not initialized).

### Step 2.2 — `pg_trickle.alter_stream_table()`

```sql
pg_trickle.alter_stream_table(
    name         TEXT,
    target_lag   INTERVAL DEFAULT NULL,
    refresh_mode TEXT DEFAULT NULL,
    status       TEXT DEFAULT NULL   -- 'ACTIVE' or 'SUSPENDED'
) RETURNS VOID
```

- Update catalog fields where non-NULL parameters are provided.
- Validate new `refresh_mode` is feasible (re-run IVM feasibility check).
- If resuming from SUSPENDED: reset `consecutive_errors = 0`.
- Signal scheduler to rebuild DAG.

### Step 2.3 — `pg_trickle.drop_stream_table()`

```sql
pg_trickle.drop_stream_table(name TEXT) RETURNS VOID
```

- Look up the ST in `pg_trickle.pgt_stream_tables` by name/schema.
- Drop the underlying storage table: `DROP TABLE <schema>.<name>`.
- Delete entries from `pg_trickle.pgt_stream_tables`, `pg_trickle.pgt_dependencies`, `pg_trickle.pgt_refresh_history`.
- Clean up CDC triggers if no other STs reference the source:
  - `DROP TRIGGER pg_trickle_cdc_<source_oid> ON <source_table>`
  - `DROP FUNCTION pg_trickle.pg_trickle_cdc_fn_<source_oid>() CASCADE`
- Remove orphaned change buffer tables in `pg_trickle_changes`.
- Signal scheduler.

Also register an **object access hook** (`hooks.rs`) so that `DROP TABLE <pgt_name>` done directly (not via `pg_trickle.drop_stream_table()`) also cleans up catalog entries. Check if the dropped OID exists in `pg_trickle.pgt_stream_tables.pgt_relid`. If yes, cascade the cleanup.

### Step 2.4 — `pg_trickle.refresh_stream_table()`

```sql
pg_trickle.refresh_stream_table(name TEXT) RETURNS VOID
```

- Sets a data timestamp of `now()`.
- Refreshes all upstream STs first (walk DAG in topological order).
- Then refreshes the target ST.
- Returns after refresh completes (synchronous).

### Step 2.5 — Monitoring views and functions

```sql
-- Computed view with current lag
CREATE VIEW pg_trickle.stream_tables_info AS
SELECT st.*,
       now() - st.data_timestamp AS current_lag,
       CASE WHEN st.target_lag IS NOT NULL
            THEN (now() - st.data_timestamp) > st.target_lag
            ELSE FALSE
       END AS lag_exceeded
FROM pg_trickle.pgt_stream_tables st;

-- Recent refresh history
CREATE FUNCTION pg_trickle.refresh_history(
    pgt_name TEXT, max_rows INT DEFAULT 20
) RETURNS SETOF pg_trickle.pgt_refresh_history ...;

-- DAG visualization
CREATE FUNCTION pg_trickle.st_graph()
RETURNS TABLE(pgt_name TEXT, source_name TEXT, source_type TEXT) ...;

-- Compact status overview
CREATE FUNCTION pg_trickle.pgt_status()
RETURNS TABLE(
    name TEXT, status TEXT, refresh_mode TEXT,
    target_lag INTERVAL, current_lag INTERVAL,
    last_refresh TIMESTAMPTZ, errors INT
) ...;
```

---

## Phase 3 — Change Data Capture via Row-Level Triggers

### Step 3.1 — CDC trigger management

For each base table tracked by at least one ST, maintain a row-level trigger that captures changes:

- **Trigger name:** `pg_trickle_cdc_<source_oid>` (e.g., `pg_trickle_cdc_16384`)
- **Trigger function:** `pg_trickle.pg_trickle_cdc_fn_<source_oid>()` (PL/pgSQL)
- **Timing:** `AFTER INSERT OR UPDATE OR DELETE FOR EACH ROW`
- **Action:** Writes change metadata and row data (as JSONB) directly to the change buffer table
- **Creation:** via `CREATE TRIGGER` during `pg_trickle.create_stream_table()`
- **Destruction:** via `DROP TRIGGER` during `pg_trickle.drop_stream_table()` when no STs reference the source

**Why triggers instead of logical replication?**
- **Transaction safety:** Triggers can be created in the same transaction as DDL/DML, enabling a single-function `create_stream_table()` API
- **No slot restrictions:** `pg_create_logical_replication_slot()` cannot execute in a transaction that has already performed writes
- **Simplicity:** Simpler lifecycle (CREATE/DROP) and immediate visibility of changes
- **No special configuration:** Works without `wal_level = logical`

**Migration path to logical replication (future):** For high-throughput workloads (>5000 writes/sec), a two-phase API (`create_stream_table_prepare()` + `finalize()`) can be implemented to use logical replication slots. The infrastructure exists in `src/cdc.rs` but is currently unused.

### Step 3.2 — Change buffer tables

Schema: `pg_trickle_changes`. One table per tracked source:

```sql
CREATE TABLE pg_trickle_changes.changes_<source_oid> (
    change_id   BIGSERIAL PRIMARY KEY,
    lsn         PG_LSN NOT NULL,
    xid         BIGINT NOT NULL,
    action      CHAR(1) NOT NULL,   -- 'I' (insert), 'U' (update), 'D' (delete)
    row_data    JSONB,              -- New row values (for I and U)
    old_row_data JSONB,             -- Old row values (for U and D)
    captured_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON pg_trickle_changes.changes_<source_oid> (lsn);
```

Tables are created by `pg_trickle.create_stream_table()` when a new source is first tracked. They are append-only; consumed changes are deleted after each refresh cycle.

### Step 3.3 — CDC trigger function implementation

The PL/pgSQL trigger function writes changes directly to the buffer table:

```sql
CREATE OR REPLACE FUNCTION pg_trickle.pg_trickle_cdc_fn_<oid>()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO pg_trickle_changes.changes_<oid>
            (lsn, xid, action, row_data)
        VALUES (pg_current_wal_lsn(), pg_current_xact_id()::text::bigint, 'I',
                row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO pg_trickle_changes.changes_<oid>
            (lsn, xid, action, row_data, old_row_data)
        VALUES (pg_current_wal_lsn(), pg_current_xact_id()::text::bigint, 'U',
                row_to_json(NEW)::jsonb, row_to_json(OLD)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO pg_trickle_changes.changes_<oid>
            (lsn, xid, action, old_row_data)
        VALUES (pg_current_wal_lsn(), pg_current_xact_id()::text::bigint, 'D',
                row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$;
```

**Key characteristics:**
- Changes are written immediately (visible after commit)
- No separate "consumption" step needed
- LSN and XID captured for frontier tracking
- Row data stored as JSONB for flexible schema handling

### Step 3.4 — Change processing in refresh

During incremental refresh, the scheduler reads pending changes from buffer tables:

```rust
fn get_pending_changes(source_oid: Oid, from_lsn: PgLsn, to_lsn: PgLsn) -> Result<Vec<Change>, PgTrickleError> {
    Spi::connect(|client| {
        let changes = client.select(
            &format!(
                "SELECT lsn, xid, action, row_data, old_row_data 
                 FROM pg_trickle_changes.changes_{} 
                 WHERE lsn > $1 AND lsn <= $2 
                 ORDER BY lsn, change_id",
                source_oid
            ),
            Some(&[(from_lsn, PgLsn), (to_lsn, PgLsn)]),
            None
        )?;
        
        // Parse JSONB and construct Change objects
        // ... implementation ...
    })
}
```

After applying changes to the ST, consumed changes are deleted:

```sql
DELETE FROM pg_trickle_changes.changes_<oid> WHERE lsn <= $1
```

---

## Phase 4 — Dependency DAG & Scheduling

### Step 4.1 — DAG construction (`dag.rs`)

```rust
pub struct StDag {
    /// Adjacency list: node_id -> list of downstream node_ids
    edges: HashMap<NodeId, Vec<NodeId>>,
    /// Reverse adjacency: node_id -> list of upstream node_ids
    reverse_edges: HashMap<NodeId, Vec<NodeId>>,
    /// Node metadata
    nodes: HashMap<NodeId, DagNode>,
}

pub enum NodeId {
    BaseTable(Oid),
    StreamTable(i64),  // pgt_id
}

pub struct DagNode {
    pub id: NodeId,
    pub target_lag: Option<Duration>,  // None = DOWNSTREAM
    pub effective_lag: Duration,       // Resolved (including DOWNSTREAM)
    pub data_timestamp: Option<SystemTime>,
    pub status: StStatus,
}
```

**Operations:**

- `StDag::build_from_catalog()` — load `pg_trickle.pgt_stream_tables` and `pg_trickle.pgt_dependencies` via SPI, construct in-memory graph.
- `StDag::add_st(pgt_id, sources)` — add a ST node with edges.
- `StDag::detect_cycles() -> Result<(), CycleError>` — Kahn's algorithm (BFS topological sort). If any nodes remain after processing all zero-indegree nodes, a cycle exists.
- `StDag::topological_order() -> Vec<NodeId>` — return STs in refresh order (upstream first).
- `StDag::resolve_downstream_lags()` — for STs with `target_lag = NULL`, compute effective lag as `MIN(target_lag)` across all immediate downstream dependents. Repeat until convergence. If no downstream, use `pg_trickle.min_target_lag_seconds`.
- `StDag::get_upstream(pgt_id) -> Vec<NodeId>` — all transitive upstream nodes.
- `StDag::get_downstream(pgt_id) -> Vec<NodeId>` — all transitive downstream nodes.

**Cycle detection algorithm (Kahn's):**

```
1. Compute in-degree for each node.
2. Enqueue all nodes with in-degree = 0.
3. While queue is not empty:
   a. Dequeue node n.
   b. For each downstream neighbor m of n:
      - Decrement in-degree of m.
      - If in-degree of m = 0, enqueue m.
   c. Add n to topological order.
4. If topological order length < total nodes → CYCLE EXISTS.
```

### Step 4.2 — Canonical data timestamp selection

Following heuristic, define canonical periods as $48 \cdot 2^n$ seconds:

| n | Period | Human-readable |
|---|--------|----------------|
| 0 | 48s | ~1 minute |
| 1 | 96s | ~1.5 minutes |
| 2 | 192s | ~3 minutes |
| 3 | 384s | ~6 minutes |
| 4 | 768s | ~13 minutes |
| 5 | 1536s | ~26 minutes |
| 6 | 3072s | ~51 minutes |
| 7 | 6144s | ~1.7 hours |
| ... | ... | ... |

**Period selection for a ST with effective target lag $t$:**

Choose the largest canonical period $p$ such that $p \leq t / 2$. This ensures that even with a full period of lag accumulation plus a refresh duration $d$, the target lag can be met: $p + w + d < t$ (where $w$ is the wait time, bounded by upstream refresh durations).

**Data timestamp alignment:**

```rust
fn canonical_data_timestamp(period_secs: u64) -> SystemTime {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let aligned = (now / period_secs) * period_secs;
    UNIX_EPOCH + Duration::from_secs(aligned)
}
```

Because all periods are powers of 2 × 48, they align: a 96s period timestamp is always also a 192s period timestamp. This guarantees that STs with different target lags in the same DAG can share data timestamps.

### Step 4.3 — Scheduler background worker (`scheduler.rs`)

Register in `_PG_init()`:

```rust
BackgroundWorkerBuilder::new("pg_trickle scheduler")
    .set_function("pg_trickle_scheduler_main")
    .set_library("pg_trickle")
    .enable_spi_access()
    .set_start_time(BgWorkerStartTime::RecoveryFinished)
    .set_restart_time(Some(Duration::from_secs(5)))
    .load();
```

**Main loop:**

```rust
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_trickle_scheduler_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM
    );
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    let mut dag = StDag::new();
    let mut dag_version: u64 = 0;

    loop {
        // Wait for scheduler interval or signal
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(
            pg_trickle_scheduler_interval_ms()
        ))) {
            break; // SIGTERM received
        }

        if BackgroundWorker::sighup_received() {
            // Reload GUC configuration
        }

        BackgroundWorker::transaction(|| {
            // Step A: Check if DAG needs rebuild
            let current_version = DAG_REBUILD_SIGNAL.load(Ordering::Relaxed);
            if current_version != dag_version {
                dag = StDag::build_from_catalog();
                dag.resolve_downstream_lags();
                dag_version = current_version;
            }

            // Step B: Consume CDC changes for all tracked sources
            consume_all_pending_changes(&dag);

            // Step C: Determine which STs need refresh
            let now = SystemTime::now();
            let mut needs_refresh: Vec<StToRefresh> = Vec::new();

            for st in dag.get_all_stream_tables() {
                if st.status != StStatus::Active { continue; }
                let current_lag = now.duration_since(st.data_timestamp.unwrap_or(UNIX_EPOCH));
                if current_lag > st.effective_lag {
                    let period = select_canonical_period(st.effective_lag);
                    let target_ts = canonical_data_timestamp(period);
                    needs_refresh.push(StToRefresh { pgt_id: st.id, target_ts });
                }
            }

            // Step D: Order refreshes topologically
            let ordered = dag.order_refreshes(&needs_refresh);

            // Step E: Execute refreshes sequentially (parallel in future)
            for refresh_task in &ordered {
                execute_single_refresh(refresh_task, &dag);
            }

            Ok::<(), PgTrickleError>(())
        }).unwrap_or_else(|e| {
            log!("Scheduler error: {}", e);
        });
    }
}
```

**Cross-ST coordination constraint:**

Before refreshing ST `B` at data timestamp `T`, verify all upstream STs have `data_timestamp >= T`. If not, refresh them first. The topological ordering guarantees this naturally — upstream STs appear earlier in the ordered list.

**Wait time formula:**

For ST $i$ with upstream STs $j$:

$$w_i \geq \max(w_j + d_j) \quad \forall j \in \text{upstream}(i)$$

The scheduler accounts for this by processing each ST only after all its upstream refreshes complete.

---

## Phase 5 — Refresh Executor

### Step 5.1 — Refresh action determination (`refresh.rs`)

```rust
pub enum RefreshAction {
    NoData,
    Full,
    Incremental,
    Reinitialize,
}

pub fn determine_refresh_action(
    st: &StreamTableMeta,
    has_upstream_changes: bool,
    needs_reinit: bool,
) -> RefreshAction {
    if needs_reinit {
        return RefreshAction::Reinitialize;
    }
    if !has_upstream_changes {
        return RefreshAction::NoData;
    }
    match st.refresh_mode {
        RefreshMode::Full => RefreshAction::Full,
        RefreshMode::Incremental => RefreshAction::Incremental,
    }
}
```

**How to detect `has_upstream_changes`:**

```sql
SELECT EXISTS(
    SELECT 1 FROM pg_trickle_changes.changes_<source_oid>
    WHERE lsn > <last_consumed_lsn>
    LIMIT 1
)
```

Check for each source table in the ST's dependency list. If any have changes, `has_upstream_changes = true`.

**How to detect `needs_reinit`:**

Maintained by the DDL event trigger (Phase 7). If any upstream table has had a schema change since the last refresh, mark the ST for reinitialize. Stored as a flag in `pg_trickle.pgt_stream_tables` or a separate reinit queue.

### Step 5.2 — NO_DATA refresh

```rust
fn execute_no_data_refresh(st: &StreamTableMeta, target_ts: SystemTime) {
    Spi::run(&format!(
        "UPDATE pg_trickle.pgt_stream_tables SET data_timestamp = $1, last_refresh_at = now(),
         updated_at = now() WHERE pgt_id = $2"
    ), /* target_ts, st.pgt_id */);

    insert_refresh_history(st.pgt_id, target_ts, "NO_DATA", "COMPLETED", 0, 0);
}
```

Zero-cost: no warehouse/compute used, only metadata update.

### Step 5.3 — FULL refresh

```sql
BEGIN;
SELECT pg_replication_origin_session_setup('pg_trickle_refresh');

-- Lock the ST to prevent concurrent access during refresh
SELECT pg_advisory_xact_lock(pgt_relid::bigint);

-- Truncate and repopulate
TRUNCATE <schema>.<name>;
INSERT INTO <schema>.<name> (__pgt_row_id, <user_cols>)
SELECT <row_id_expr>, sub.* FROM (<defining_query>) sub;

-- Update catalog
UPDATE pg_trickle.pgt_stream_tables
SET data_timestamp = <target_ts>, is_populated = true,
    frontier = <new_frontier>, last_refresh_at = now(),
    consecutive_errors = 0, status = 'ACTIVE', updated_at = now()
WHERE pgt_id = <pgt_id>;

-- Record history
INSERT INTO pg_trickle.pgt_refresh_history (...) VALUES (...);

-- Clean up consumed changes
DELETE FROM pg_trickle_changes.changes_<source_oid> WHERE lsn <= <consumed_lsn>;

SELECT pg_replication_origin_session_reset();
COMMIT;
```

### Step 5.4 — INCREMENTAL refresh

```sql
BEGIN;
SELECT pg_replication_origin_session_setup('pg_trickle_refresh');
SELECT pg_advisory_xact_lock(pgt_relid::bigint);

-- Step 1: Compute the delta (SQL generated by IVM engine — see Phase 6)
CREATE TEMP TABLE __pgt_delta ON COMMIT DROP AS
<generated_delta_query>;
-- Result has columns: __pgt_row_id, __pgt_action ('I' or 'D'), <user_cols>,
--                     [__pgt_count, __pgt_sum_*, ...] (auxiliary cols for aggregates)

-- Step 2: Delete removed/updated rows
DELETE FROM <schema>.<name> st
USING __pgt_delta d
WHERE st.__pgt_row_id = d.__pgt_row_id
  AND d.__pgt_action = 'D';

-- Step 3: Insert new/updated rows
INSERT INTO <schema>.<name> (__pgt_row_id, <user_cols>, [__pgt_count, ...])
SELECT d.__pgt_row_id, <user_cols>, [d.__pgt_count, ...]
FROM __pgt_delta d
WHERE d.__pgt_action = 'I';

-- Step 4: Update catalog, record history, clean up change buffers
-- (same as FULL refresh)

SELECT pg_replication_origin_session_reset();
COMMIT;
```

For updates (a value changes within the same row ID), the delta contains both a `'D'` row (old) and an `'I'` row (new) for the same `__pgt_row_id`. The DELETE runs first, followed by the INSERT.

### Step 5.5 — REINITIALIZE refresh

Same as FULL refresh, but also:
- Recompute auxiliary columns (`__pgt_count`, `__pgt_sum_*`) for aggregate STs
- Rebuild any internal metadata about the query structure (in case the defining query's semantics changed due to upstream DDL)
- Reset the frontier to reflect the fresh computation

### Step 5.6 — Advisory locking

Each ST is locked during refresh using `pg_advisory_xact_lock(pgt_relid::bigint)`. This:
- Prevents concurrent refreshes of the same ST
- Is transaction-scoped (automatically released on COMMIT/ROLLBACK)
- Does not block readers of the ST (regular SELECT still works)
- Allows the skip mechanism to detect an in-progress refresh

---

## Phase 6 — Incremental View Maintenance Engine (THE CORE)

This is the most complex component. It implements query differentiation — transforming the defining query $Q$ into a delta query $\Delta_I Q$ that computes only the changes over an interval $I = [\text{prev\_frontier}, \text{new\_frontier}]$.

### Step 6.1 — Operator tree (`dvm/parser.rs`)

Parse the defining query and build an intermediate representation:

```rust
pub enum OpTree {
    Scan {
        table_oid: Oid,
        table_name: String,
        schema: String,
        columns: Vec<Column>,
        alias: String,
    },
    Project {
        expressions: Vec<Expr>,
        child: Box<OpTree>,
    },
    Filter {
        predicate: Expr,
        child: Box<OpTree>,
    },
    InnerJoin {
        condition: Expr,
        left: Box<OpTree>,
        right: Box<OpTree>,
    },
    LeftJoin {   // Phase 2
        condition: Expr,
        left: Box<OpTree>,
        right: Box<OpTree>,
    },
    Aggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<AggExpr>,
        child: Box<OpTree>,
    },
    Distinct {
        child: Box<OpTree>,
    },
    UnionAll {   // Phase 2
        children: Vec<OpTree>,
    },
    Window {     // Phase 2
        partition_by: Vec<Expr>,
        order_by: Vec<SortExpr>,
        frame: WindowFrame,
        function: WindowFunc,
        child: Box<OpTree>,
    },
}

pub struct AggExpr {
    pub function: AggFunc, // Count, Sum, Avg, Min, Max
    pub argument: Option<Expr>,
    pub alias: String,
}

pub struct Column {
    pub name: String,
    pub type_oid: Oid,
    pub is_nullable: bool,
}
```

**Query parsing approach:**

1. Call `pg_sys::raw_parser(defining_query)` to get the raw parse tree.
2. Walk the `SelectStmt` node:
   - `fromClause` → `Scan` and `Join` nodes (including `JoinExpr` for explicit joins)
   - `whereClause` → `Filter` node
   - `targetList` → `Project` node
   - `groupClause` + aggregate functions in targetList → `Aggregate` node
   - `distinctClause` → `Distinct` node
   - Window specs → `Window` node
   - `UNION ALL` → `UnionAll` node
3. For joins using WHERE-clause conditions (implicit joins): treat as `Filter(Join(Cross(A, B)))` then optimize to `InnerJoin(condition, A, B)`.
4. Expand views inline by looking up `pg_rewrite` and substituting.

**Alternative simpler approach:** Instead of parsing the raw tree into a full relational algebra and then generating SQL from it, use **EXPLAIN (FORMAT JSON)** to get the plan tree, which already has the operator decomposition. Map the plan nodes to `OpTree` variants. This avoids reimplementing query analysis from scratch.

### Step 6.2 — Row ID generation (`dvm/row_id.rs`)

Every row in an incrementally-maintained ST has a unique `__pgt_row_id BIGINT`. Row IDs must be deterministic (same input → same ID) for the MERGE to work correctly.

**Strategy per operator:**

| Operator | Row ID computation |
|---|---|
| Scan(table) | `pg_trickle_hash(pk_col1, pk_col2, ...)` using primary key, or `pg_trickle_hash(all_cols)` if no PK (with warning) |
| Project | Pass through child's row ID |
| Filter | Pass through child's row ID |
| InnerJoin | `pg_trickle_hash(left.__pgt_row_id, right.__pgt_row_id)` |
| Aggregate | `pg_trickle_hash(group_by_col1, group_by_col2, ...)` |
| Distinct | `pg_trickle_hash(all_output_cols)` |

**Hash function:** Use xxHash64 via a SQL function:

```sql
-- Exposed as a pg_extern
CREATE FUNCTION pg_trickle.pg_trickle_hash(VARIADIC args ANYARRAY) RETURNS BIGINT ...;
```

Implement in Rust using the `xxhash-rust` crate. Accept variadic input, serialize all arguments, compute xxHash64, return as BIGINT.

### Step 6.3 — Query differentiation framework (`dvm/diff.rs`)

The differentiator traverses the `OpTree` bottom-up and generates SQL for each node's delta. The output is composed as CTEs (Common Table Expressions) in a single `WITH` query.

```rust
pub struct DiffContext {
    pub prev_frontier: Frontier,
    pub new_frontier: Frontier,
    pub cte_counter: usize,
    pub ctes: Vec<(String, String)>,  // (cte_name, cte_sql)
}

impl DiffContext {
    /// Generate the delta query for the entire operator tree.
    /// Returns the final SQL query string.
    pub fn differentiate(&mut self, op: &OpTree) -> Result<String, PgTrickleError> {
        let final_cte = self.diff_node(op)?;

        // Build the WITH query
        let cte_defs = self.ctes.iter()
            .map(|(name, sql)| format!("{} AS ({})", name, sql))
            .collect::<Vec<_>>()
            .join(",\n");

        Ok(format!(
            "WITH {}\nSELECT * FROM {}",
            cte_defs, final_cte
        ))
    }

    fn diff_node(&mut self, op: &OpTree) -> Result<String, PgTrickleError> {
        match op {
            OpTree::Scan { .. } => self.diff_scan(op),
            OpTree::Project { .. } => self.diff_project(op),
            OpTree::Filter { .. } => self.diff_filter(op),
            OpTree::InnerJoin { .. } => self.diff_inner_join(op),
            OpTree::Aggregate { .. } => self.diff_aggregate(op),
            OpTree::Distinct { .. } => self.diff_distinct(op),
            // Phase 2 operators:
            OpTree::LeftJoin { .. } => self.diff_left_join(op),
            OpTree::UnionAll { .. } => self.diff_union_all(op),
            OpTree::Window { .. } => self.diff_window(op),
        }
    }

    fn next_cte_name(&mut self) -> String {
        self.cte_counter += 1;
        format!("__pgt_cte_{}", self.cte_counter)
    }
}
```

### Step 6.4 — Scan differentiation (`dvm/operators/scan.rs`)

$\Delta_I(\text{Scan}(T))$ reads from the change buffer table:

```rust
fn diff_scan(&mut self, scan: &OpTree) -> Result<String, PgTrickleError> {
    let OpTree::Scan { table_oid, columns, alias, .. } = scan else { unreachable!() };

    let cte_name = self.next_cte_name();
    let col_list = columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ");

    // For INSERT changes: extract new row from row_data JSONB
    // For DELETE changes: extract old row from old_row_data JSONB
    // For UPDATE changes: emit BOTH a delete (old) and insert (new)
    let sql = format!(
        "SELECT
            CASE WHEN action = 'D' THEN
                pg_trickle.pg_trickle_hash({pk_extraction_from_old})
            ELSE
                pg_trickle.pg_trickle_hash({pk_extraction_from_new})
            END AS __pgt_row_id,
            CASE
                WHEN action = 'I' THEN 'I'
                WHEN action = 'D' THEN 'D'
                WHEN action = 'U' AND __pgt_update_part = 'old' THEN 'D'
                WHEN action = 'U' AND __pgt_update_part = 'new' THEN 'I'
            END AS __pgt_action,
            {col_extractions}
        FROM (
            -- Direct inserts and deletes
            SELECT action, row_data, old_row_data, NULL AS __pgt_update_part
            FROM pg_trickle_changes.changes_{table_oid}
            WHERE lsn > '{prev_lsn}' AND lsn <= '{new_lsn}'
              AND action IN ('I', 'D')
            UNION ALL
            -- Updates expanded to old+new pairs
            SELECT action, row_data, old_row_data, 'old' AS __pgt_update_part
            FROM pg_trickle_changes.changes_{table_oid}
            WHERE lsn > '{prev_lsn}' AND lsn <= '{new_lsn}'
              AND action = 'U'
            UNION ALL
            SELECT action, row_data, old_row_data, 'new' AS __pgt_update_part
            FROM pg_trickle_changes.changes_{table_oid}
            WHERE lsn > '{prev_lsn}' AND lsn <= '{new_lsn}'
              AND action = 'U'
        ) expanded",
        table_oid = table_oid,
        prev_lsn = self.prev_frontier.get_lsn(*table_oid),
        new_lsn = self.new_frontier.get_lsn(*table_oid),
    );

    self.ctes.push((cte_name.clone(), sql));
    Ok(cte_name)
}
```

**JSONB column extraction:** Each column is extracted from the JSONB:
```sql
(row_data->>'column_name')::<type> AS column_name
```

### Step 6.5 — Project differentiation (`dvm/operators/project.rs`)

$$\Delta_I(\pi_E(Q)) = \pi_E(\Delta_I(Q))$$

Simply apply the same projection expressions to the child's delta, passing through `__pgt_row_id` and `__pgt_action`:

```rust
fn diff_project(&mut self, project: &OpTree) -> Result<String, PgTrickleError> {
    let OpTree::Project { expressions, child } = project else { unreachable!() };
    let child_cte = self.diff_node(child)?;
    let cte_name = self.next_cte_name();

    let expr_list = expressions.iter()
        .map(|e| e.to_sql())
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT __pgt_row_id, __pgt_action, {} FROM {}",
        expr_list, child_cte
    );

    self.ctes.push((cte_name.clone(), sql));
    Ok(cte_name)
}
```

### Step 6.6 — Filter differentiation (`dvm/operators/filter.rs`)

$$\Delta_I(\sigma_P(Q))$$

For inserts: keep those satisfying $P$. For deletes: keep those that *were* satisfying $P$ (they must have been, since they were in the ST).

Simplified approach — apply the predicate to both inserts and deletes:

```rust
fn diff_filter(&mut self, filter: &OpTree) -> Result<String, PgTrickleError> {
    let OpTree::Filter { predicate, child } = filter else { unreachable!() };
    let child_cte = self.diff_node(child)?;
    let cte_name = self.next_cte_name();

    let sql = format!(
        "SELECT __pgt_row_id, __pgt_action, * FROM {} WHERE {}",
        child_cte, predicate.to_sql()
    );

    self.ctes.push((cte_name.clone(), sql));
    Ok(cte_name)
}
```

**Subtlety:** For deletes, the predicate is evaluated on the *old* row values (which were in the ST, so they must satisfy the predicate). For inserts, the predicate is evaluated on the *new* values. Since the scan differentiation already splits UPDATEs into old-row-DELETE + new-row-INSERT with the appropriate values, the predicate naturally applies to the correct row version.

However, there's an edge case: a row that didn't satisfy the predicate before UPDATE but does after. The scan emits a DELETE (old row, which doesn't pass filter → dropped) and an INSERT (new row, which passes → kept). Net result: INSERT into the ST. Correct.

Conversely, a row that satisfied the predicate before UPDATE but doesn't after: DELETE (old row, passes filter → kept) and INSERT (new row, doesn't pass → dropped). Net result: DELETE from the ST. Correct.

### Step 6.7 — Inner join differentiation (`dvm/operators/join.rs`)

Using the standard delta join identity:

$$\Delta_I(Q \bowtie_C R) = (\Delta_I Q \bowtie_C R_1) \cup (Q_0 \bowtie_C \Delta_I R)$$

Where:
- $R_1$ = $R$ at the new timestamp (current table state)
- $Q_0$ = $Q$ at the previous timestamp (ST's stored state or base table state before changes)
- To avoid double-counting, use $R_1$ (not $R_0$) in the first term

**For base tables:**
- $R_1$ = `SELECT * FROM <table>` (current snapshot)
- $Q_0$ = For a base table, this is conceptually the table at the previous snapshot. Since we have the current state and the delta, we can compute $Q_0 = Q_1 - \Delta_I^{+}Q + \Delta_I^{-}Q$. But this is expensive. **Practical approach:** Just use the current table and accept the slight technical issue for the double-counting case. For most workloads (insert-only or low-conflict), this works. For correctness, use the `NOT EXISTS` anti-join approach:

```sql
-- Part 1: delta_left JOIN current_right
SELECT pg_trickle.pg_trickle_hash(dl.__pgt_row_id, r.<pk>) AS __pgt_row_id,
       dl.__pgt_action,
       <output_cols>
FROM <delta_left_cte> dl
JOIN <right_table> r ON <join_condition>

UNION ALL

-- Part 2: previous_left JOIN delta_right
-- previous_left = current ST state for upstream STs,
--                 or current table for base tables
SELECT pg_trickle.pg_trickle_hash(l.<pk>, dr.__pgt_row_id) AS __pgt_row_id,
       dr.__pgt_action,
       <output_cols>
FROM <left_table_current> l
JOIN <delta_right_cte> dr ON <join_condition>
-- Exclude rows already counted in Part 1 (anti-join on left keys)
WHERE NOT EXISTS (
    SELECT 1 FROM <delta_left_cte> dl2
    WHERE dl2.__pgt_row_id = pg_trickle.pg_trickle_hash(l.<pk>)
      AND dl2.__pgt_action = 'I'
)
```

**Note:** The anti-join avoidance of double-counting is crucial for correctness when both sides change simultaneously. For insert-only workloads where only one side changes at a time, this simplifies significantly.

### Step 6.8 — Aggregate differentiation (`dvm/operators/aggregate.rs`)

$$\Delta_I(\gamma_{G,F}(Q))$$

This is the most intricate operator. Uses auxiliary counters stored in the ST alongside user data.

**Auxiliary columns per aggregate type:**

| User aggregate | Auxiliary columns in ST | Maintenance logic |
|---|---|---|
| `COUNT(*)` | `__pgt_count` | `+= count_of_inserts - count_of_deletes` |
| `COUNT(col)` | `__pgt_count`, `__pgt_count_nonnull` | Track total and non-null counts |
| `SUM(col)` | `__pgt_count`, `__pgt_sum_col` | `sum += sum_of_inserts - sum_of_deletes` |
| `AVG(col)` | `__pgt_count`, `__pgt_sum_col` | `avg = sum / count` |
| `MIN(col)` | `__pgt_count`, `__pgt_min_col`, `__pgt_min_count` | If current min deleted & min_count reaches 0: RESCAN group |
| `MAX(col)` | `__pgt_count`, `__pgt_max_col`, `__pgt_max_count` | If current max deleted & max_count reaches 0: RESCAN group |

**Generated delta SQL:**

```sql
-- Compute per-group changes from the child delta
__pgt_cte_agg_delta AS (
    SELECT
        <group_by_cols>,
        SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE 0 END) AS __ins_count,
        SUM(CASE WHEN __pgt_action = 'D' THEN 1 ELSE 0 END) AS __del_count,
        SUM(CASE WHEN __pgt_action = 'I' THEN <agg_col> ELSE 0 END) AS __ins_sum,
        SUM(CASE WHEN __pgt_action = 'D' THEN <agg_col> ELSE 0 END) AS __del_sum
    FROM <child_delta_cte>
    GROUP BY <group_by_cols>
),

-- Merge with existing ST state to classify actions
__pgt_cte_agg_merge AS (
    SELECT
        pg_trickle.pg_trickle_hash(<group_by_cols>) AS __pgt_row_id,
        <group_by_cols>,

        -- New auxiliary values
        COALESCE(st.__pgt_count, 0) + d.__ins_count - d.__del_count
            AS new_count,
        COALESCE(st.__pgt_sum_col, 0) + d.__ins_sum - d.__del_sum
            AS new_sum,

        -- Determine action
        CASE
            WHEN st.__pgt_count IS NULL AND (d.__ins_count - d.__del_count) > 0
                THEN 'I'   -- New group appears
            WHEN COALESCE(st.__pgt_count, 0) + d.__ins_count - d.__del_count <= 0
                THEN 'D'   -- Group vanishes
            ELSE 'U'       -- Group value changes (emit D+I pair)
        END AS __pgt_meta_action,

        st.__pgt_count AS old_count

    FROM __pgt_cte_agg_delta d
    LEFT JOIN <st_table> st
        ON st.<group_key_1> = d.<group_key_1>
       AND st.<group_key_2> = d.<group_key_2>
       -- (for each group-by column)
),

-- Expand 'U' (update) into D+I pairs, emit final delta
__pgt_cte_agg_final AS (
    -- Inserts (new groups)
    SELECT __pgt_row_id, 'I' AS __pgt_action,
           <group_by_cols>,
           new_count AS __pgt_count,
           new_sum AS __pgt_sum_col,
           CASE WHEN new_count > 0 THEN new_sum::numeric / new_count ELSE NULL END AS avg_col
    FROM __pgt_cte_agg_merge
    WHERE __pgt_meta_action = 'I'

    UNION ALL

    -- Deletes (vanished groups)
    SELECT __pgt_row_id, 'D' AS __pgt_action,
           <group_by_cols>,
           0 AS __pgt_count, 0 AS __pgt_sum_col, NULL AS avg_col
    FROM __pgt_cte_agg_merge
    WHERE __pgt_meta_action = 'D'

    UNION ALL

    -- Updates: emit delete of old row
    SELECT __pgt_row_id, 'D' AS __pgt_action,
           <group_by_cols>,
           old_count AS __pgt_count,
           NULL AS __pgt_sum_col, NULL AS avg_col
    FROM __pgt_cte_agg_merge
    WHERE __pgt_meta_action = 'U'

    UNION ALL

    -- Updates: emit insert of new row
    SELECT __pgt_row_id, 'I' AS __pgt_action,
           <group_by_cols>,
           new_count AS __pgt_count,
           new_sum AS __pgt_sum_col,
           CASE WHEN new_count > 0 THEN new_sum::numeric / new_count ELSE NULL END AS avg_col
    FROM __pgt_cte_agg_merge
    WHERE __pgt_meta_action = 'U'
)
```

**MIN/MAX special handling:**

When a DELETE removes the current minimum (or maximum) value and its count drops to 0:
1. Mark the group as needing a RESCAN.
2. Execute a subquery: `SELECT MIN(col) FROM (<defining_query_for_group>) sub` to find the new minimum.
3. Emit a D+I pair with the corrected value.

This is the same approach used by pg_ivm. It degrades to a per-group full recompute in the worst case, but only for affected groups.

### Step 6.9 — DISTINCT differentiation (`dvm/operators/distinct.rs`)

DISTINCT is modeled as `GROUP BY ALL` with `COUNT(*)`:

```rust
fn diff_distinct(&mut self, distinct: &OpTree) -> Result<String, PgTrickleError> {
    let OpTree::Distinct { child } = distinct else { unreachable!() };

    // Rewrite DISTINCT as: Aggregate(group_by=all_cols, agg=[COUNT(*)], child)
    // Then use aggregate differentiation
    // Filter output to only emit when count transitions 0→N (INSERT) or N→0 (DELETE)
    // ...
}
```

The ST storage table includes a `__pgt_count` column tracking multiplicity. Rows only appear/disappear when count crosses the 0 boundary.

### Step 6.10 — Change consolidation

After the full delta is computed, ensure at most one row per `(__pgt_row_id, __pgt_action)` pair:

```sql
-- Final consolidation (can be skipped for insert-only optimized paths)
SELECT __pgt_row_id, __pgt_action, <cols>
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY __pgt_row_id, __pgt_action
        ORDER BY __pgt_row_id -- deterministic tie-breaking
    ) AS __rn
    FROM <final_delta_cte>
) sub
WHERE __rn = 1
```

**Optimization:** For insert-only workloads (no DELETEs or UPDATEs in the change buffer), and queries that structurally cannot produce duplicate row IDs (e.g., simple project-filter on a single table), skip consolidation entirely.

### Step 6.11 — Phase 2 operator support

#### Outer Join differentiation (`dvm/operators/outer_join.rs`)

$$\Delta_I(Q \text{⟕} R)$$

A LEFT JOIN = INNER JOIN + anti-join for non-matching left rows:

$$Q \text{⟕} R = (Q \bowtie R) \cup (\pi_{R=\text{NULL}}(Q \text{ anti } R))$$

Differentiate each component separately:

```sql
-- Delta of inner join part (reuse inner join differentiation)
<delta_inner_join>

UNION ALL

-- Delta of anti-join part
-- Rows in Q that lost their last match in R → INSERT with NULL right columns
-- Rows in Q that gained their first match in R → DELETE the NULL-padded row
```

This requires tracking whether each left row has any match in R. Use a count: `__pgt_match_count`. When it transitions 0→1 (first match appears), delete the NULL-padded row. When 1→0 (last match disappears), insert the NULL-padded row.

#### UNION ALL differentiation (`dvm/operators/union_all.rs`)

$$\Delta_I(Q_1 \cup Q_2) = \Delta_I(Q_1) \cup \Delta_I(Q_2)$$

Straightforward — just UNION ALL the deltas of each child, prefixing row IDs with a child index to prevent collisions:

```sql
SELECT pg_trickle.pg_trickle_hash(1, __pgt_row_id) AS __pgt_row_id,
       __pgt_action, <cols>
FROM <delta_child_1>
UNION ALL
SELECT pg_trickle.pg_trickle_hash(2, __pgt_row_id) AS __pgt_row_id,
       __pgt_action, <cols>
FROM <delta_child_2>
```

#### Partitioned window function differentiation (`dvm/operators/window.rs`)

$$\Delta_I(\xi_k(Q)) = \pi^-(\xi_k(Q|_{I_0} \ltimes_k \Delta_I Q)) + \pi^+(\xi_k(Q|_{I_1} \ltimes_k \Delta_I Q))$$

Approach: recompute the window function for all partitions that have *any* changed rows:

```sql
-- Step 1: Find changed partition keys
__pgt_changed_partitions AS (
    SELECT DISTINCT <partition_by_cols>
    FROM <child_delta_cte>
),

-- Step 2: Delete old window results for changed partitions
-- (emit as 'D' actions)
__pgt_window_deletes AS (
    SELECT st.__pgt_row_id, 'D' AS __pgt_action, <cols>
    FROM <st_table> st
    SEMI JOIN __pgt_changed_partitions cp
        ON st.<pk1> = cp.<pk1> AND ...
),

-- Step 3: Recompute window function for changed partitions
-- using current (post-refresh) source data
__pgt_window_inserts AS (
    SELECT pg_trickle.pg_trickle_hash(<pk_cols>) AS __pgt_row_id,
           'I' AS __pgt_action,
           <cols>,
           <window_function> OVER (PARTITION BY <pk> ORDER BY <ok>) AS <wf_col>
    FROM <source_table_current> src
    SEMI JOIN __pgt_changed_partitions cp
        ON src.<pk1> = cp.<pk1> AND ...
),

-- Step 4: Combine
__pgt_window_delta AS (
    SELECT * FROM __pgt_window_deletes
    UNION ALL
    SELECT * FROM __pgt_window_inserts
)
```

This doesn't reuse previous window function state — it recomputes entire changed partitions. This is the pragmatic approach that works for all `PARTITION BY` window functions with repeatable `ORDER BY` tie-breaking.

---

## Phase 7 — DDL Tracking & Schema Evolution

### Step 7.1 — Event triggers (`hooks.rs`)

Create event triggers via `extension_sql!()`:

```sql
-- Track DDL changes that may affect STs
CREATE OR REPLACE FUNCTION pg_trickle._on_ddl_end()
RETURNS event_trigger
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_trickle_on_ddl_end_wrapper';

CREATE EVENT TRIGGER pg_trickle_ddl_tracker
ON ddl_command_end
EXECUTE FUNCTION pg_trickle._on_ddl_end();
```

The Rust implementation (`#[pg_guard]` function called via a thin C wrapper):

```rust
fn handle_ddl_end() {
    // Get affected objects
    let commands = Spi::connect(|client| {
        client.select(
            "SELECT objid, object_type, command_tag
             FROM pg_event_trigger_ddl_commands()",
            None, None
        )
    });

    for cmd in commands {
        let objid: Oid = cmd.get("objid");
        let command_tag: String = cmd.get("command_tag");

        // Check if this object is an upstream dependency of any ST
        let affected_sts = Spi::connect(|client| {
            client.select(
                "SELECT pgt_id FROM pg_trickle.pgt_dependencies WHERE source_relid = $1",
                None, Some(vec![(PgBuiltInOids::OIDOID.oid(), objid.into_datum())])
            )
        });

        if affected_sts.is_empty() { continue; }

        match command_tag.as_str() {
            "ALTER TABLE" => {
                // Mark affected STs for REINITIALIZE
                for st in affected_sts {
                    mark_for_reinitialize(st.pgt_id);
                    log!(INFO, "ST {} marked for reinitialize due to ALTER TABLE on {}",
                         st.pgt_id, objid);
                }
            }
            "DROP TABLE" => {
                // Mark affected STs as ERROR
                for st in affected_sts {
                    set_pgt_status(st.pgt_id, StStatus::Error);
                    log!(WARNING, "ST {} error: upstream table {} dropped", st.pgt_id, objid);
                }
            }
            _ => {}
        }
    }
}
```

### Step 7.2 — Object access hook

Register via pgrx's hook mechanism to intercept `DROP TABLE` on ST storage tables:

```rust
static mut PREV_OBJECT_ACCESS_HOOK: Option<pg_sys::object_access_hook_type> = None;

pub fn register_object_access_hook() {
    unsafe {
        PREV_OBJECT_ACCESS_HOOK = pg_sys::object_access_hook;
        pg_sys::object_access_hook = Some(pg_trickle_object_access_hook);
    }
}

#[pg_guard]
extern "C-unwind" fn pg_trickle_object_access_hook(
    access: pg_sys::ObjectAccessType,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
    arg: *mut std::ffi::c_void,
) {
    // Chain to previous hook
    unsafe {
        if let Some(prev) = PREV_OBJECT_ACCESS_HOOK {
            prev(access, class_id, object_id, sub_id, arg);
        }
    }

    if access == pg_sys::ObjectAccessType_OAT_DROP
       && class_id == pg_sys::RelationRelationId
    {
        // Check if this OID is a ST storage table
        let is_st = Spi::connect(|client| {
            client.select(
                "SELECT 1 FROM pg_trickle.pgt_stream_tables WHERE pgt_relid = $1",
                None, Some(vec![(PgBuiltInOids::OIDOID.oid(), object_id.into_datum())])
            ).len() > 0
        });

        if is_st {
            // Clean up catalog entries
            Spi::run_with_args(
                "DELETE FROM pg_trickle.pgt_stream_tables WHERE pgt_relid = $1",
                Some(vec![(PgBuiltInOids::OIDOID.oid(), object_id.into_datum())])
            );
            // Cascade deletes will clean up pgt_dependencies
            // Signal scheduler to rebuild DAG
            DAG_REBUILD_SIGNAL.fetch_add(1, Ordering::Relaxed);
        }
    }
}
```

### Step 7.3 — `mark_for_reinitialize` mechanism

Add a column `needs_reinit BOOLEAN DEFAULT FALSE` to `pg_trickle.pgt_stream_tables` (or use the separate `status` field). When the event trigger detects an upstream schema change:

1. Set `needs_reinit = true` on affected STs.
2. On next scheduler cycle, the refresh executor checks this flag.
3. If true, use `REINITIALIZE` action instead of `INCREMENTAL`.
4. After successful reinitialize, clear the flag.

---

## Phase 8 — Version Management & DVS

### Step 8.1 — Frontier structure (`version.rs`)

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Frontier {
    pub sources: HashMap<Oid, SourceVersion>,
    pub data_timestamp: SystemTime,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SourceVersion {
    pub lsn: PgLsn,
    pub snapshot_ts: SystemTime,
}
```

Stored as JSONB in `pg_trickle.pgt_stream_tables.frontier`:

```json
{
    "sources": {
        "16384": {"lsn": "0/1A2B3C4", "snapshot_ts": "2026-02-17T10:00:00Z"},
        "16392": {"lsn": "0/1A2B400", "snapshot_ts": "2026-02-17T10:00:00Z"}
    },
    "data_timestamp": "2026-02-17T10:00:00Z"
}
```

### Step 8.2 — Frontier advancement

On each refresh:
1. Read the current frontier from catalog (`old_frontier`).
2. Compute the new frontier:
   - For each base table source: new LSN = `pg_current_wal_lsn()` at the start of refresh (changes up to this point are in the buffer table).
   - For each upstream ST source: new LSN = the upstream ST's LSN at its `data_timestamp`.
   - `data_timestamp` = the target data timestamp for this refresh.
3. The change interval $I = [\text{old\_frontier}, \text{new\_frontier}]$ tells the IVM engine which changes to read from each change buffer.
4. After successful refresh, store the new frontier.

### Step 8.3 — DVS enforcement

**Guarantee:** The contents of a ST are logically equivalent to its defining query evaluated at some past time (the `data_timestamp`).

**Implementation:**
- The scheduler refreshes STs in topological order.
- For a given data timestamp $T$:
  1. Refresh all root STs (those depending only on base tables) to $T$.
  2. Refresh intermediate STs (those depending on other STs) to $T$. These read the upstream STs' stored contents, which now reflect $T$.
  3. Continue until leaf STs are refreshed.
- This guarantees that when ST `B` references upstream ST `A`, it reads `A`'s state at data timestamp $T$, not some arbitrary past state.

**Isolation levels:**
- **Single ST query:** Snapshot Isolation (PL-SI). The ST's contents are a consistent snapshot at `data_timestamp`.
- **Multi-ST query:** Read Committed (PL-2). Different STs may have different `data_timestamp` values. The user sees each ST's latest committed state.
- Future work: atomic refresh across multiple STs to provide cross-ST snapshot isolation.

### Step 8.4 — Data timestamp selection during initialization

When a new ST is created with upstream STs:
1. Find the most recent `data_timestamp` among upstream STs that is within the new ST's `target_lag`.
2. If found, use that timestamp (avoids unnecessary re-refresh of upstream STs).
3. If not found (upstream STs are too stale), use `now()` and trigger upstream refreshes.

This is an optimization to minimize wasted computation during initialization.

---

## Phase 9 — Monitoring & Observability

### Step 9.1 — Custom cumulative statistics (PG 18)

Register custom statistics via `pg_sys` FFI:

```rust
// In _PG_init(), register custom stats kind
unsafe {
    let stats_kind = pg_sys::pgstat_register_kind(
        PGS_STATS_KIND_ID,  // custom kind ID
        &PGS_STATS_KIND_INFO,
    );
}
```

Track per-ST:
- `refresh_count: u64`
- `total_refresh_duration_ms: u64`
- `rows_inserted_total: u64`
- `rows_deleted_total: u64`
- `skip_count: u64`
- `error_count: u64`
- `last_lag_ms: u64`
- `max_lag_ms: u64`

Expose via a view:

```sql
CREATE VIEW pg_trickle.pg_stat_stream_tables AS
SELECT st.pgt_name, st.pgt_schema,
       -- stats from custom cumulative statistics
       s.refresh_count, s.total_refresh_duration_ms,
       s.rows_inserted_total, s.rows_deleted_total,
       s.skip_count, s.error_count,
       s.last_lag_ms, s.max_lag_ms
FROM pg_trickle.pgt_stream_tables st
JOIN pg_trickle._get_stats() s ON s.pgt_id = st.pgt_id;
```

### Step 9.2 — Custom EXPLAIN option (PG 18)

Register a custom EXPLAIN option `STREAM_TABLE`:

```sql
EXPLAIN (STREAM_TABLE) SELECT * FROM my_stream_table;
```

Output includes:
- Whether the query would use FULL or INCREMENTAL refresh
- The generated delta query (for INCREMENTAL)
- Estimated cost: delta query cost vs. full query cost
- IVM operator support matrix for the query
- Current lag and last refresh info

### Step 9.3 — NOTIFY-based alerting

Emit PostgreSQL NOTIFY events for operational alerts:

```rust
fn emit_alert(channel: &str, payload: &str) {
    Spi::run(&format!("NOTIFY {}, '{}'", channel, payload));
}

// Usage:
emit_alert("pg_trickle_alert", &format!(
    "{{\"event\": \"lag_exceeded\", \"st\": \"{}\", \"lag_seconds\": {}}}",
    st.pgt_name, current_lag_secs
));
```

Alert events:
- `lag_exceeded` — ST lag exceeds `2 × target_lag`
- `auto_suspended` — ST suspended due to consecutive errors
- `reinitialize_needed` — Upstream DDL change detected
- `buffer_lag_warning` — Change buffer table row count growing large (>1M pending changes)

Users can `LISTEN pg_trickle_alert;` for integration with monitoring/alerting systems.

---

## Phase 10 — Error Handling & Resilience

### Step 10.1 — Error classification

```rust
pub enum PgTrickleError {
    // User errors — fail, don't retry
    QueryParseError(String),
    DivisionByZero(String),
    TypeMismatch(String),
    UnsupportedOperator(String),
    CycleDetected(Vec<String>),

    // Schema errors — may require reinitialize
    UpstreamTableDropped(Oid),
    UpstreamSchemaChanged(Oid),

    // System errors — retry with backoff
    OutOfMemory,
    DiskFull,
    LockTimeout,
    CdcTriggerError(String),

    // Internal errors — should not happen
    InternalError(String),
}
```

### Step 10.2 — Auto-suspend

```rust
fn handle_refresh_failure(st: &StreamTableMeta, error: &PgTrickleError) {
    let new_error_count = StreamTableMeta::increment_errors(st.pgt_id).unwrap();

    insert_refresh_history(st.pgt_id, target_ts, action, "FAILED",
                          0, 0, Some(error.to_string()));

    if new_error_count >= pg_trickle_max_consecutive_errors() {
        StreamTableMeta::update_status(st.pgt_id, StStatus::Suspended);
        emit_alert("pg_trickle_alert", &format!(
            "{{\"event\": \"auto_suspended\", \"st\": \"{}\", \"errors\": {}, \"last_error\": \"{}\"}}",
            st.pgt_name, new_error_count, error
        ));
        log!(WARNING, "ST {} auto-suspended after {} consecutive errors: {}",
             st.pgt_name, new_error_count, error);
    }
}
```

### Step 10.3 — Skip mechanism

```rust
fn check_skip_needed(st: &StreamTableMeta) -> bool {
    // Try to acquire advisory lock (non-blocking)
    let locked = Spi::get_one::<bool>(
        &format!("SELECT pg_try_advisory_lock({})", st.pgt_relid as i64)
    ).unwrap_or(false);

    if locked {
        // We got the lock — release it, no skip needed
        Spi::run(&format!("SELECT pg_advisory_unlock({})", st.pgt_relid as i64));
        false
    } else {
        // Another refresh is in progress — skip this one
        insert_refresh_history(st.pgt_id, target_ts, "SKIP", "SKIPPED", 0, 0, None);
        log!(INFO, "ST {} refresh skipped — previous refresh still in progress", st.pgt_name);
        true
    }
}
```

The next refresh will cover the skipped interval (larger delta, but saves fixed costs). This means graceful degradation under resource pressure.

### Step 10.4 — Crash recovery

On scheduler restart (after crash):

```rust
fn recover_from_crash() {
    // Any RUNNING refresh records indicate interrupted transactions
    // PostgreSQL would have rolled them back automatically
    Spi::run(
        "UPDATE pg_trickle.pgt_refresh_history SET status = 'FAILED',
         error_message = 'Interrupted by scheduler restart',
         end_time = now()
         WHERE status = 'RUNNING'"
    );

    // Rebuild DAG from catalog
    // Resume scheduling normally — next refresh will pick up from
    // the last committed frontier
}
```

---

## Phase 11 — Testing Strategy

### Step 11.1 — Unit tests (`#[cfg(test)]`)

Test in isolation (no PostgreSQL):

- **DAG operations:** cycle detection, topological sort, DOWNSTREAM resolution
- **Canonical period selection:** verify period choices for various target lags
- **Frontier merge:** JSON serialization/deserialization, LSN comparison
- **Row ID computation:** hash determinism, collision behavior
- **Operator tree construction:** mock parse trees → correct OpTree variants
- **CDC trigger functions:** trigger name generation, JSONB row data formatting

### Step 11.2 — Integration tests (`#[pg_test]`)

Run against a live PostgreSQL 18 instance via pgrx:

```rust
#[pg_test]
fn test_create_and_query_simple_st() {
    Spi::run("CREATE TABLE orders (id INT PRIMARY KEY, amount NUMERIC)");
    Spi::run("INSERT INTO orders VALUES (1, 100), (2, 200)");
    Spi::run("SELECT pg_trickle.create_stream_table(
        'order_totals',
        'SELECT id, amount FROM orders',
        '1m',
        'INCREMENTAL'
    )");

    // Verify ST is populated
    let count = Spi::get_one::<i64>("SELECT count(*) FROM order_totals").unwrap();
    assert_eq!(count, 2);

    // Verify contents match defining query
    let matches = Spi::get_one::<bool>(
        "SELECT NOT EXISTS (
            (SELECT id, amount FROM order_totals EXCEPT SELECT id, amount FROM orders)
            UNION ALL
            (SELECT id, amount FROM orders EXCEPT SELECT id, amount FROM order_totals)
        )"
    ).unwrap();
    assert!(matches);
}

#[pg_test]
fn test_incremental_refresh_insert() {
    // Create base table and ST
    // Insert new rows into base table
    // Trigger manual refresh
    // Verify ST contains new rows
    // Verify ST contents = defining query result
}

#[pg_test]
fn test_incremental_refresh_update() { /* ... */ }
#[pg_test]
fn test_incremental_refresh_delete() { /* ... */ }
#[pg_test]
fn test_join_st_incremental() { /* ... */ }
#[pg_test]
fn test_aggregate_st_incremental() { /* ... */ }
#[pg_test]
fn test_dag_cycle_rejection() { /* ... */ }
#[pg_test]
fn test_cascade_refresh_ordering() { /* ... */ }
#[pg_test]
fn test_upstream_ddl_triggers_reinit() { /* ... */ }
#[pg_test]
fn test_auto_suspend_on_errors() { /* ... */ }
#[pg_test]
fn test_skip_mechanism() { /* ... */ }
#[pg_test]
fn test_no_data_refresh() { /* ... */ }
```

### Step 11.3 — Property-based correctness tests

**THE KEY INVARIANT**:

> For every ST, at every data timestamp:
> `SELECT * FROM st_table ORDER BY 1 = SELECT * FROM (defining_query) sub ORDER BY 1`

Test procedure:

1. Generate random schemas (3–5 tables, 2–8 columns each, various types).
2. Generate random ST defining queries using supported operators.
3. Apply random DML sequences (INSERT, UPDATE, DELETE batches).
4. Trigger refresh.
5. Assert: ST contents == defining query result (set equality).
6. Repeat steps 3–5 for many iterations.

Use the `proptest` or `quickcheck` Rust crates for randomized inputs.

```rust
#[pg_test]
fn property_test_st_correctness() {
    for _ in 0..100 {
        let schema = generate_random_schema();
        create_tables(&schema);
        let query = generate_random_query(&schema);
        create_st("test_st", &query);

        for _ in 0..20 {
            apply_random_dml(&schema);
            manual_refresh("test_st");
            assert_st_equals_query("test_st", &query);
        }

        cleanup();
    }
}
```

### Step 11.4 — Benchmark tests

Measure performance across dimensions:

| Dimension | Values |
|---|---|
| Base table size | 10K, 100K, 1M rows |
| Change rate | 1%, 10%, 50% of rows |
| Query complexity | scan-only, filter, join, aggregate, multi-join+agg |
| Refresh mode | FULL vs. INCREMENTAL |

Metrics to capture:
- Refresh duration (wall clock)
- Delta query execution time
- Change buffer consumption time
- Rows processed vs. total table size
- Peak memory usage

**Expected results:**
- Incremental refresh with 1% change rate should be 10–50x faster than FULL
- Delta query cost should scale with change volume, not total table size
- NO_DATA refresh should be < 10ms (metadata-only)

---

## Phase 12 — Documentation & Packaging

### Step 12.1 — Files to create

| File | Contents |
|---|---|
| `README.md` | Project overview, features, quick start guide |
| `INSTALL.md` | Prerequisites (PG 18.x, Rust 1.82+, pgrx 0.17.x), build steps, postgresql.conf settings |
| `docs/SQL_REFERENCE.md` | All `pg_trickle.*` functions with signatures, parameters, return types, examples |
| `docs/ARCHITECTURE.md` | Component diagram, data flow, DVS explanation, scheduling algorithm |
| `docs/DVM_OPERATORS.md` | Supported operators, differentiation rules, limitations, Phase 1 vs Phase 2 |
| `docs/CONFIGURATION.md` | All GUC variables with defaults, descriptions, tuning guidance |
| `LICENSE` | Apache 2.0 or PostgreSQL License |
| `Cargo.toml` | pgrx 0.17.x, xxhash-rust, serde/serde_json (for Frontier JSONB), proptest (dev) |
| `pg_trickle.control` | Extension control file for PostgreSQL |

### Step 12.2 — Build and install

```bash
cargo pgrx install --release --pg-config /path/to/pg18/bin/pg_config
```

### Step 12.3 — Required postgresql.conf

```ini
shared_preload_libraries = 'pg_trickle'
max_worker_processes = 8     # Must include scheduler + refresh workers

# Note: wal_level = logical is NOT required for trigger-based CDC (current implementation).
# Only set this if migrating to logical replication for high-throughput workloads.
```

---

## Implementation Progress

**Last updated:** 2026-03-04

All phases (0–12) of the core plan are **IMPLEMENTED**. Current work
focuses on edge-case hardening and SQL coverage expansion, tracked in
[PLAN_EDGE_CASES.md](PLAN_EDGE_CASES.md) and
[PLAN_EDGE_CASES_TIVM_IMPL_ORDER.md](PLAN_EDGE_CASES_TIVM_IMPL_ORDER.md).

**Completed sprints:**

- **Stage 1 (P0 Correctness):** EC-19 (WAL+keyless rejection), EC-06
  (full keyless table support with net-counting delta, counted DELETE,
  non-unique index + 2 post-implementation bug fixes), EC-01 (R₀ via
  EXCEPT ALL for inner/left/full joins). Validated with TPC-H regression.
- **Stage 2 (P1 Operational Safety):** EC-25/EC-26 (guard triggers
  blocking direct DML/TRUNCATE on stream tables), EC-15 (SELECT * warning),
  EC-11 (scheduler falling-behind alert), EC-13 (atomic diamond consistency
  default), EC-18 (auto CDC stuck-in-TRIGGER logging), EC-34 (auto-detect
  missing WAL slot).

**Test coverage:** 1032+ unit tests, 7 keyless E2E tests, 5 guard trigger
E2E tests, 9+ diamond E2E tests, plus integration and TPC-H regression
suites.

**What remains (prioritised):**

1. **Stage 3 — SQL Coverage Expansion:** Mixed UNION, multi-PARTITION BY,
   nested window expressions, ALL(subquery), deeply nested SubLinks,
   ROWS FROM(), LATERAL RIGHT/FULL JOIN error message.
2. **Stage 4 — P1 Remainder + Trigger Optimisations:** EC-16 (ALTER
   FUNCTION detection), column-level change detection, incremental
   TRUNCATE, buffer partitioning, skip-unchanged scanning, online ADD
   COLUMN.
3. **Stage 5 — Aggregate Coverage:** CORR/COVAR/REGR, hypothetical-set
   aggregates, XMLAGG.
4. **Stage 6 — IMMEDIATE Mode Parity:** Recursive CTEs + TopK in
   IMMEDIATE mode.
5. **Stage 7 — Usability + Docs:** Foreign tables, GROUPING SET GUC,
   post-restart health check, PgBouncer docs, DDL-during-refresh docs,
   replication docs.

---

## Verification Criteria

1. **Correctness invariant:** For all STs at all data timestamps, `ST contents == defining query result` (set equality). Verified by property-based tests.

2. **DVS guarantee:** ST contents are logically equivalent to the defining query at the recorded `data_timestamp`. No future state is visible.

3. **Lag compliance:** For an ACTIVE ST with sufficient resources, `current_lag <= target_lag` at least 95% of the time. Measured by monitoring.

4. **Incremental efficiency:** Incremental refresh of a 1M-row ST with 1% change rate completes in < 2× the time of bulk-inserting just the delta rows.

5. **Zero DML overhead:** Base table INSERT/UPDATE/DELETE performance is unaffected by the extension (no triggers on base tables). Verified by benchmarking with and without the extension loaded.

6. **Crash safety:** After scheduler crash and restart, all STs resume normal operation with no data corruption. Verified by kill-and-recover tests.

7. **Cycle rejection:** `pg_trickle.create_stream_table()` rejects queries that would create cycles in the dependency graph.

8. **Schema evolution:** Upstream DDL changes (ALTER TABLE, DROP TABLE) are detected and handled gracefully (reinitialize or error with diagnostic).

---

## Key Design Decisions

| Decision | Choice | Rationale | Trade-off |
|---|---|---|---|
| **CDC mechanism** | Row-level triggers (PL/pgSQL) | Single-transaction create API, no `wal_level=logical` requirement, simple lifecycle, immediate change visibility | Per-row trigger overhead (mitigated for <1K writes/sec); future migration path to logical replication for high-throughput |
| **DDL syntax** | SQL functions (`pg_trickle.create_stream_table(...)`) | Works without parser modifications, clean extension boundary, idiomatic PostgreSQL extension pattern | Less "native" feel than `CREATE STREAM TABLE` syntax |
| **Target PostgreSQL** | PG 18 only | Leverages custom cumulative statistics, custom EXPLAIN options, DSM improvements, improved logical replication | Narrows user base to PG 18+ |
| **IVM scope** | Phased — Phase 1 (project, filter, inner join, GROUP BY aggregates, DISTINCT) then Phase 2 (outer joins, UNION ALL, window functions) | Phase 1 covers ~80% of real queries; Phase 2 operators are significantly more complex | Phase 1-only users miss outer joins and window functions |
| **Row IDs** | 64-bit xxHash stored as `BIGINT` | Fast computation, compact storage (8 bytes), sufficient collision resistance for practical datasets | Theoretical collision risk (1 in 2⁶⁴); trade vs. UUID (16 bytes, zero collision) |
| **Scheduling heuristic** | Canonical periods of 48·2ⁿ seconds | Guarantees timestamp alignment across STs with different target lags | Refresh period may be much smaller than target lag |
| **Change storage** | Buffer tables (not in-memory) | Durable across crashes, queryable for debugging, supports arbitrary-size change sets | Extra I/O; mitigated by aggressive cleanup and auto-vacuum |
| **Aggregate maintenance** | Auxiliary counter columns (like pg_ivm) | Well-understood approach, correct for COUNT/SUM/AVG; MIN/MAX degrade gracefully | Hidden columns increase storage; MIN/MAX may require rescan |
| **Replication origin** | `pg_trickle_refresh` origin to prevent feedback loops | Standard PostgreSQL mechanism, reliable filtering | Requires origin tracking overhead |
