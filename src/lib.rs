//! pg_trickle — Stream Tables for PostgreSQL 18.
//!
//! This extension provides declarative Stream Tables with automated
//! schedule-driven refresh and differential view maintenance (DVM).
//!
//! # Theoretical Basis
//!
//! - **DBSP**: Budiu et al., "DBSP: Automatic Differential View Maintenance
//!   for Rich Query Languages", PVLDB 2023. <https://arxiv.org/abs/2203.16684>
//! - **Gupta & Mumick (1995)**: "Maintenance of Materialized Views: Problems,
//!   Techniques, and Applications", IEEE Data Engineering Bulletin.
//! - **PostgreSQL REFRESH MATERIALIZED VIEW CONCURRENTLY** (since 9.4, Dec 2014).
//!
//! # Safety
//! This extension uses `unsafe` code for PostgreSQL FFI calls via pgrx.
//! All unsafe blocks are documented with `// SAFETY:` comments.

#![deny(unsafe_op_in_unsafe_fn)]
// Q-3 (v0.79.0): Global #![allow(dead_code)] replaced with per-module
// allowances on pgrx/export boundaries — modules where items are
// intentionally visible to SQL (via pg_extern) or pgrx infrastructure
// (pg_shmem_init!, BackgroundWorkerBuilder) but appear dead to Rust's
// static call-graph analysis.
// SAF-3: Deny .unwrap() in non-test production code.
// Tests are explicitly exempt (cfg(test) blocks allow free use of unwrap).
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

use pgrx::prelude::*;

// ── pgrx/export-boundary modules (may have intentional dead items) ────────
// Items in these modules are exposed to PostgreSQL via pg_extern, pgrx macros,
// or trigger/hook registration — not via Rust call-graph paths.
#[allow(dead_code)]
pub mod api;
#[allow(dead_code)]
mod catalog;
#[allow(dead_code)]
mod cdc;
#[allow(dead_code)]
pub mod citus;
#[allow(dead_code)]
pub mod config;
// dag and dvm contain utility/helper functions that may not be called from
// the main production path but are retained for future use or test support.
#[allow(dead_code)]
pub mod dag;
pub(crate) mod diagnostics;
#[allow(dead_code)]
pub mod dvm;
pub mod dvm_trace;
pub mod error;
#[cfg(feature = "pg18")]
#[allow(dead_code)]
mod explain_hook;
pub mod fuzz_pub;
mod hash;
#[allow(dead_code)]
mod hooks;
#[allow(dead_code)]
mod ivm;
pub mod logging;
#[allow(dead_code)]
pub(crate) mod metrics_server;
#[allow(dead_code)]
mod monitor;
pub mod otel;
#[allow(dead_code)]
mod refresh;
#[allow(dead_code)]
pub mod scheduler;
#[allow(dead_code)]
mod shmem;
pub mod sql_builder;
#[allow(dead_code)]
mod template_cache;
pub mod version;
#[allow(dead_code)]
pub mod wal_decoder;

::pgrx::pg_module_magic!();

// Declare the `pgtrickle` schema so pgrx's SQL entity graph recognises it
// for `#[pg_extern(schema = "pgtrickle")]` annotations.
#[pg_schema]
mod pgtrickle {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitWarningKind {
    MissingSharedPreload,
    AutoCdcWithoutLogicalWal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InitDecision {
    should_init_runtime: bool,
    warning: Option<InitWarningKind>,
}

fn build_init_decision(in_shared_preload: bool, cdc_mode: &str, wal_level: i32) -> InitDecision {
    if !in_shared_preload {
        return InitDecision {
            should_init_runtime: false,
            warning: Some(InitWarningKind::MissingSharedPreload),
        };
    }

    let warning = if cdc_mode.eq_ignore_ascii_case("auto")
        && wal_level != pg_sys::WalLevel::WAL_LEVEL_LOGICAL as i32
    {
        Some(InitWarningKind::AutoCdcWithoutLogicalWal)
    } else {
        None
    };

    InitDecision {
        should_init_runtime: true,
        warning,
    }
}

/// Extension initialization — called when the shared library is loaded.
///
/// Registers GUC variables, shared memory, and background workers.
/// Must be loaded via `shared_preload_libraries` for full functionality.
#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Register GUC variables first (always available)
    config::register_gucs();
    #[cfg(feature = "pg18")]
    explain_hook::register();

    // Check if loaded via shared_preload_libraries
    // SAFETY: Reading a global boolean set by PostgreSQL during startup.
    // This is safe because the value is set before any extension code runs.
    let in_shared_preload = unsafe { pg_sys::process_shared_preload_libraries_in_progress };
    let cdc_mode = config::pg_trickle_cdc_mode();
    let init_decision = if in_shared_preload {
        // SAFETY: `pg_sys::wal_level` is a PostgreSQL global written from
        // postgresql.conf before shared_preload_libraries are processed.
        let current_wal_level = unsafe { pg_sys::wal_level };
        build_init_decision(in_shared_preload, &cdc_mode, current_wal_level)
    } else {
        build_init_decision(in_shared_preload, &cdc_mode, 0)
    };

    if init_decision.should_init_runtime {
        // Register shared memory allocations
        shmem::init_shared_memory();

        // Register the launcher background worker.
        // The launcher auto-discovers all databases on this server and
        // spawns a per-database scheduler for each one with pg_trickle installed.
        scheduler::register_launcher_worker();

        // ERG-B: Warn if cdc_mode='auto' but wal_level is not 'logical'.
        // In this state the extension silently stays in TRIGGER-only CDC mode,
        // which is correct but may surprise users who expect WAL-based CDC.
        // SAFETY: `pg_sys::wal_level` is a PostgreSQL global written from
        // postgresql.conf before shared_preload_libraries are processed.
        if init_decision.warning == Some(InitWarningKind::AutoCdcWithoutLogicalWal) {
            warning!(
                "pg_trickle: cdc_mode='auto' but wal_level is not 'logical'. \
                 WAL-based CDC will not activate until wal_level = logical is \
                 set in postgresql.conf and PostgreSQL is restarted. \
                 The extension will use trigger-based CDC in the meantime."
            );
        }

        log!("pg_trickle: initialized (shared_preload_libraries)");
    } else {
        warning!(
            "pg_trickle: loaded without shared_preload_libraries. \
             Background scheduler and shared memory are disabled. \
             Add 'pg_trickle' to shared_preload_libraries in \
             postgresql.conf for full functionality."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{InitDecision, InitWarningKind, build_init_decision};
    use pgrx::pg_sys;

    #[test]
    fn test_build_init_decision_requires_shared_preload() {
        assert_eq!(
            build_init_decision(false, "auto", pg_sys::WalLevel::WAL_LEVEL_LOGICAL as i32),
            InitDecision {
                should_init_runtime: false,
                warning: Some(InitWarningKind::MissingSharedPreload),
            }
        );
    }

    #[test]
    fn test_build_init_decision_warns_for_auto_cdc_without_logical_wal() {
        assert_eq!(
            build_init_decision(true, "auto", pg_sys::WalLevel::WAL_LEVEL_REPLICA as i32),
            InitDecision {
                should_init_runtime: true,
                warning: Some(InitWarningKind::AutoCdcWithoutLogicalWal),
            }
        );
    }

    #[test]
    fn test_build_init_decision_accepts_logical_wal_for_auto_cdc() {
        assert_eq!(
            build_init_decision(true, "AUTO", pg_sys::WalLevel::WAL_LEVEL_LOGICAL as i32),
            InitDecision {
                should_init_runtime: true,
                warning: None,
            }
        );
    }

    #[test]
    fn test_build_init_decision_does_not_warn_for_explicit_cdc_modes() {
        assert_eq!(
            build_init_decision(true, "trigger", pg_sys::WalLevel::WAL_LEVEL_REPLICA as i32),
            InitDecision {
                should_init_runtime: true,
                warning: None,
            }
        );
        assert_eq!(
            build_init_decision(true, "wal", pg_sys::WalLevel::WAL_LEVEL_REPLICA as i32),
            InitDecision {
                should_init_runtime: true,
                warning: None,
            }
        );
    }
}

#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        Vec::new()
    }
}

// ── SQL migration for catalog tables ──────────────────────────────────

extension_sql!(
    r#"
-- Extension schemas
CREATE SCHEMA IF NOT EXISTS pgtrickle;
CREATE SCHEMA IF NOT EXISTS pgtrickle_changes;

-- F51: Restrict change buffer schema access to prevent unauthorized
-- injection of bogus changes that would be applied on next refresh.
REVOKE ALL ON SCHEMA pgtrickle_changes FROM PUBLIC;

-- User-declared refresh groups for snapshot consistency
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_refresh_groups (
    group_id    SERIAL PRIMARY KEY,
    group_name  TEXT NOT NULL UNIQUE,
    member_oids OID[] NOT NULL,
    isolation   TEXT NOT NULL DEFAULT 'read_committed'
                CHECK (isolation IN ('read_committed', 'repeatable_read')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Core ST metadata
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_stream_tables (
    pgt_id           BIGSERIAL PRIMARY KEY,
    pgt_relid        OID NOT NULL UNIQUE,
    pgt_name         TEXT NOT NULL,
    pgt_schema       TEXT NOT NULL,
    defining_query  TEXT NOT NULL,
    original_query  TEXT,
    schedule      TEXT,
    refresh_mode    TEXT NOT NULL DEFAULT 'DIFFERENTIAL'
                     CHECK (refresh_mode IN ('FULL', 'DIFFERENTIAL', 'IMMEDIATE')),
    requested_refresh_mode TEXT NOT NULL DEFAULT 'DIFFERENTIAL'
                     CHECK (requested_refresh_mode IN ('AUTO', 'FULL', 'DIFFERENTIAL', 'IMMEDIATE')),
    status          TEXT NOT NULL DEFAULT 'INITIALIZING'
                     CHECK (status IN ('INITIALIZING', 'ACTIVE', 'SUSPENDED', 'ERROR')),
    is_populated    BOOLEAN NOT NULL DEFAULT FALSE,
    data_timestamp  TIMESTAMPTZ,
    frontier        JSONB,
    tentative_frontier JSONB,
    last_refresh_at TIMESTAMPTZ,
    consecutive_errors INT NOT NULL DEFAULT 0,
    needs_reinit    BOOLEAN NOT NULL DEFAULT FALSE,
    auto_threshold  DOUBLE PRECISION,
    last_full_ms    DOUBLE PRECISION,
    functions_used  TEXT[],
    topk_limit      INT,
    topk_order_by   TEXT,
    topk_offset     INT,
    diamond_consistency TEXT NOT NULL DEFAULT 'atomic'
                     CHECK (diamond_consistency IN ('none', 'atomic')),
    diamond_schedule_policy TEXT NOT NULL DEFAULT 'fastest'
                     CHECK (diamond_schedule_policy IN ('fastest', 'slowest')),
    has_keyless_source BOOLEAN NOT NULL DEFAULT FALSE,
    function_hashes TEXT,
    requested_cdc_mode TEXT
                     CHECK (requested_cdc_mode IN ('auto', 'trigger', 'wal')),
    is_append_only  BOOLEAN NOT NULL DEFAULT FALSE,
    scc_id          INT,
    last_fixpoint_iterations INT,
    max_differential_joins   INT
        CHECK (max_differential_joins IS NULL OR max_differential_joins >= 0),
    max_delta_fraction       DOUBLE PRECISION
        CHECK (max_delta_fraction IS NULL OR
               (max_delta_fraction >= 0.0 AND max_delta_fraction <= 1.0)),
    pooler_compatibility_mode BOOLEAN NOT NULL DEFAULT FALSE,
    refresh_tier    TEXT NOT NULL DEFAULT 'hot'
                     CHECK (refresh_tier IN ('hot', 'warm', 'cold', 'frozen')),
    effective_refresh_mode TEXT,
    fuse_mode       TEXT NOT NULL DEFAULT 'off'
                     CHECK (fuse_mode IN ('off', 'on', 'auto')),
    fuse_state      TEXT NOT NULL DEFAULT 'armed'
                     CHECK (fuse_state IN ('armed', 'blown', 'disabled')),
    fuse_ceiling    BIGINT,
    fuse_sensitivity INT,
    blown_at        TIMESTAMPTZ,
    blow_reason     TEXT,
    last_error_message TEXT,
    last_error_at   TIMESTAMPTZ,
    self_heal_work_mem_percent SMALLINT NOT NULL DEFAULT 100
        CHECK (self_heal_work_mem_percent BETWEEN 25 AND 100),
    self_heal_lock_backoff_exponent SMALLINT NOT NULL DEFAULT 0
        CHECK (self_heal_lock_backoff_exponent BETWEEN 0 AND 6),
    self_heal_success_streak SMALLINT NOT NULL DEFAULT 0
        CHECK (self_heal_success_streak BETWEEN 0 AND 3),
    last_error_code TEXT CHECK (last_error_code IS NULL OR last_error_code IN
                     ('LOCK_TIMEOUT', 'STATEMENT_TIMEOUT', 'DEADLOCK',
                      'SERIALIZATION', 'OUT_OF_MEMORY', 'CANCELLED',
                      'PERMANENT', 'UNKNOWN_RETRYABLE')),
    last_error_retryable BOOLEAN,
    downstream_publication_name TEXT,
    freshness_deadline_ms BIGINT,
    target_freshness_mode TEXT
        CHECK (target_freshness_mode IS NULL OR target_freshness_mode IN ('INTERVAL', 'ON_COMMIT', 'MANUAL')),
    refresh_reason TEXT,
    refresh_reason_detail TEXT,
    st_partition_key TEXT,
    in_shadow_build BOOLEAN NOT NULL DEFAULT FALSE,
    shadow_table_name TEXT,
    -- CITUS-3: Placement of this stream table's storage in a Citus cluster.
    st_placement    TEXT NOT NULL DEFAULT 'local',
    -- v0.36.0: temporal IVM flag (CORR-1/UX-1)
    temporal_mode   BOOLEAN NOT NULL DEFAULT FALSE,
    -- v0.36.0: columnar storage backend (CORR-2/UX-3)
    storage_backend TEXT NOT NULL DEFAULT 'heap',
    -- v0.47.0: post-refresh action hooks (VP-1/VP-2)
    post_refresh_action TEXT NOT NULL DEFAULT 'none'
                     CHECK (post_refresh_action IN ('none', 'analyze', 'reindex', 'reindex_if_drift')),
    reindex_drift_threshold DOUBLE PRECISION
                     CHECK (reindex_drift_threshold IS NULL OR (reindex_drift_threshold > 0 AND reindex_drift_threshold <= 1.0)),
    rows_changed_since_last_reindex BIGINT NOT NULL DEFAULT 0,
    last_reindex_at TIMESTAMPTZ,
    -- v0.36.0: column lineage metadata (F12)
    column_lineage  JSONB,
    -- v0.59.0 PERF-2: hash of defining_query to skip recomputation on every refresh
    defining_query_hash BIGINT NOT NULL DEFAULT 0,
    -- v0.73.0 HOT-1: fillfactor for storage heap (NULL = PG default 100). Range 10-100.
    storage_fillfactor INT DEFAULT NULL CHECK (storage_fillfactor IS NULL OR (storage_fillfactor >= 10 AND storage_fillfactor <= 100)),
    -- v0.78.0 P-2: OpTree-derived complexity label, back-filled lazily on first refresh.
    query_complexity_class TEXT,
    -- v0.83.0: Composite row-identity encoding version. NULL is an
    -- unclassified pre-upgrade row; fresh objects are written explicitly.
    row_identity_version SMALLINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- v0.87.7 LSEC-3: exact search_path defining_query was resolved under
    -- (bare $user already expanded). Set at CREATE and on any ALTER that
    -- changes the query; preserved by configuration-only ALTERs and
    -- ownership transfer.
    defining_search_path TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pgt_status ON pgtrickle.pgt_stream_tables (status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pgt_name ON pgtrickle.pgt_stream_tables (pgt_schema, pgt_name);
-- PERF-4: Scheduler hot‐path lookup by relation OID.
CREATE INDEX IF NOT EXISTS idx_pgt_relid ON pgtrickle.pgt_stream_tables (pgt_relid);

-- v0.87.12: Immutable provenance for downstream publications.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_publication_bindings (
    pgt_id                  BIGINT PRIMARY KEY
                            REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                            ON DELETE CASCADE,
    stream_relid            OID NOT NULL,
    publication_oid         OID NOT NULL UNIQUE,
    publication_name        TEXT NOT NULL UNIQUE,
    publication_owner_oid   OID NOT NULL,
    expected_relation_oids  OID[] NOT NULL,
    CONSTRAINT pgt_publication_binding_relations_check
        CHECK (expected_relation_oids = ARRAY[stream_relid])
);
REVOKE ALL ON TABLE pgtrickle.pgt_publication_bindings FROM PUBLIC;

-- v0.83.0: Durable private state registry for set-operation state.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_set_operation_states (
    pgt_id         BIGINT NOT NULL
                   REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                   ON DELETE CASCADE,
    node_ordinal   INTEGER NOT NULL,
    operation      TEXT NOT NULL CHECK (operation IN ('INTERSECT', 'EXCEPT')),
    is_all         BOOLEAN NOT NULL,
    state_relid    OID NOT NULL,
    schema_version SMALLINT NOT NULL,
    PRIMARY KEY (pgt_id, node_ordinal)
);

SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_set_operation_states', '');

-- Snapshot metadata catalog
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_snapshots (
    snapshot_id      BIGSERIAL PRIMARY KEY,
    pgt_id           BIGINT NOT NULL
                     REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    snapshot_schema  TEXT NOT NULL,
    snapshot_table   TEXT NOT NULL,
    snapshot_version TEXT NOT NULL,
    frontier         JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    snapshot_relid   OID,
    snapshot_provenance_token TEXT,
    created_by_role_oid OID,
    CONSTRAINT uq_snapshot_table UNIQUE (snapshot_schema, snapshot_table)
);

CREATE INDEX IF NOT EXISTS idx_pgt_snapshots_pgt_id
    ON pgtrickle.pgt_snapshots (pgt_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pgt_snapshots_snapshot_relid
    ON pgtrickle.pgt_snapshots (snapshot_relid);

SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_snapshots', '');

-- Durable NOTIFY subscriptions
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_subscriptions (
    stream_table TEXT NOT NULL,
    channel      TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_table, channel)
);

SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_subscriptions', '');

-- DAG edges
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_dependencies (
    pgt_id        BIGINT NOT NULL REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    source_relid OID NOT NULL,
    source_type  TEXT NOT NULL CHECK (source_type IN ('TABLE', 'STREAM_TABLE', 'VIEW', 'MATVIEW', 'FOREIGN_TABLE')),
    columns_used TEXT[],
    column_snapshot JSONB,
    schema_fingerprint TEXT,
    cdc_mode     TEXT NOT NULL DEFAULT 'TRIGGER'
                  CHECK (cdc_mode IN ('TRIGGER', 'TRANSITIONING', 'WAL')),
    slot_name    TEXT,
    decoder_confirmed_lsn PG_LSN,
    transition_started_at TIMESTAMPTZ,
    cutover_target TEXT CHECK (cutover_target IN ('TRIGGER', 'WAL')),
    cutover_lsn PG_LSN,
    -- CITUS-3: Stable name for the source table (v0.32.0+). NULL = pre-upgrade row.
    source_stable_name   TEXT,
    -- CITUS-3: Source placement in a Citus cluster: 'local', 'reference', 'distributed'.
    source_placement     TEXT NOT NULL DEFAULT 'local',
    PRIMARY KEY (pgt_id, source_relid)
);

CREATE INDEX IF NOT EXISTS idx_deps_source ON pgtrickle.pgt_dependencies (source_relid);
-- PERF-4: Fast lookup by pgt_id (non‐PK prefix for multi‐column PK).
CREATE INDEX IF NOT EXISTS idx_deps_pgt_id ON pgtrickle.pgt_dependencies (pgt_id);

-- Refresh history / audit log
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_refresh_history (
    refresh_id      BIGSERIAL PRIMARY KEY,
    pgt_id           BIGINT NOT NULL
                     REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    data_timestamp  TIMESTAMPTZ NOT NULL,
    start_time      TIMESTAMPTZ NOT NULL,
    end_time        TIMESTAMPTZ,
    action          TEXT NOT NULL
                     CHECK (action IN ('NO_DATA', 'FULL', 'DIFFERENTIAL', 'REINITIALIZE', 'SKIP')),
    rows_inserted   BIGINT DEFAULT 0,
    rows_updated    BIGINT NOT NULL DEFAULT 0,
    rows_deleted    BIGINT DEFAULT 0,
    delta_row_count BIGINT DEFAULT 0,
    merge_strategy_used TEXT,
    was_full_fallback BOOLEAN NOT NULL DEFAULT FALSE,
    refresh_reason TEXT,
    refresh_reason_detail TEXT,
    error_message   TEXT,
    status          TEXT NOT NULL
                     CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED')),
    initiated_by    TEXT
                     CHECK (initiated_by IN ('SCHEDULER', 'MANUAL', 'INITIAL', 'SELF_MONITOR', 'SCHEDULER_FUSED')),
    freshness_deadline TIMESTAMPTZ,
    tick_watermark_lsn PG_LSN,
    fixpoint_iteration INT
    ,
    error_code      TEXT CHECK (error_code IS NULL OR error_code IN
                     ('LOCK_TIMEOUT', 'STATEMENT_TIMEOUT', 'DEADLOCK',
                      'SERIALIZATION', 'OUT_OF_MEMORY', 'CANCELLED',
                      'PERMANENT', 'UNKNOWN_RETRYABLE')),
    error_sqlstate  TEXT,
    retryable       BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_hist_pgt_ts ON pgtrickle.pgt_refresh_history (pgt_id, data_timestamp);
-- PERF-1: Fast lookup by (pgt_id, start_time) for self-monitoring and scheduler_overhead queries.
CREATE INDEX IF NOT EXISTS idx_hist_pgt_start ON pgtrickle.pgt_refresh_history (pgt_id, start_time);
CREATE INDEX IF NOT EXISTS idx_hist_start_time
    ON pgtrickle.pgt_refresh_history (start_time, refresh_id);
CREATE INDEX IF NOT EXISTS idx_hist_pgt_stats_window
    ON pgtrickle.pgt_refresh_history (pgt_id, start_time, status, action);

-- v0.73.0 PERF-001: Incremental summary table for refresh-history metrics.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_refresh_summary (
    pgt_id BIGINT PRIMARY KEY
          REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    total_refreshes BIGINT NOT NULL DEFAULT 0,
    successful_refreshes BIGINT NOT NULL DEFAULT 0,
    failed_refreshes BIGINT NOT NULL DEFAULT 0,
    total_rows_inserted BIGINT NOT NULL DEFAULT 0,
    total_rows_updated BIGINT NOT NULL DEFAULT 0,
    total_rows_deleted BIGINT NOT NULL DEFAULT 0,
    total_duration_ms BIGINT NOT NULL DEFAULT 0,
    last_refresh_action TEXT,
    last_refresh_status TEXT,
    last_refresh_at TIMESTAMPTZ,
    stats_reset_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    total_full_refreshes BIGINT NOT NULL DEFAULT 0,
    total_diff_refreshes BIGINT NOT NULL DEFAULT 0,
    total_delta_rows_processed BIGINT NOT NULL DEFAULT 0,
    last_full_reason TEXT,
    last_full_reason_detail TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- v0.78.0 P-3: Per-stream-table cost model summary.
-- Populated by batch_update_cost_model_summary() on each scheduler tick.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_cost_model_summary (
    pgt_id       BIGINT      NOT NULL
                             REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                             ON DELETE CASCADE,
    avg_full_ms  DOUBLE PRECISION,
    avg_diff_ms  DOUBLE PRECISION,
    sample_count INTEGER     NOT NULL DEFAULT 0,
    p95_ms       DOUBLE PRECISION,
    p99_ms       DOUBLE PRECISION,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pgt_cost_model_summary_pk PRIMARY KEY (pgt_id)
);

-- v0.73.0 ARCH-002 / REL-002: Persistent cleanup retry/backpressure status.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_cleanup_status (
    source_relid OID PRIMARY KEY,
    buffer_table TEXT NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    blocked BOOLEAN NOT NULL DEFAULT false,
    last_error TEXT,
    last_operation TEXT,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    backlog_rows BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cleanup_status_next_retry
    ON pgtrickle.pgt_cleanup_status (blocked, next_retry_at);

-- Per-source CDC slot tracking
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_change_tracking (
    source_relid        OID PRIMARY KEY,
    slot_name           TEXT NOT NULL,
    last_consumed_lsn   PG_LSN,
    tracked_by_pgt_ids   BIGINT[],
    -- CITUS-3: Stable hash name used for all pg_trickle-managed objects (v0.32.0+).
    -- NULL = pre-upgrade row using OID-based object names (STAB-1 fallback).
    source_stable_name  TEXT,
    -- CITUS-3: How the source is placed in a Citus cluster.
    source_placement    TEXT NOT NULL DEFAULT 'local',
    -- CITUS-7: Per-node WAL frontier for distributed sources. NULL for local sources.
    frontier_per_node   JSONB
);

-- v0.82.0: Registry for validated base-table and stream-table change buffers.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_change_buffers (
    buffer_key       TEXT PRIMARY KEY,
    source_kind      TEXT NOT NULL CHECK (source_kind IN ('BASE', 'STREAM_TABLE')),
    source_id        BIGINT NOT NULL,
    durability       TEXT NOT NULL CHECK (durability IN ('logged', 'unlogged', 'sync')),
    sentinel_token   BIGINT NOT NULL,
    -- v0.83.0: Composite row-identity encoding used by buffer writers.
    row_identity_version SMALLINT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_kind, source_id)
);

-- Scheduler job table for parallel refresh dispatch
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_scheduler_jobs (
    job_id          BIGSERIAL PRIMARY KEY,
    dag_version     BIGINT NOT NULL,
    unit_key        TEXT NOT NULL,
    unit_kind       TEXT NOT NULL
                     CHECK (unit_kind IN ('singleton', 'atomic_group', 'immediate_closure', 'cyclic_scc', 'repeatable_read_group', 'fused_chain')),
    member_pgt_ids  BIGINT[] NOT NULL,
    root_pgt_id     BIGINT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'QUEUED'
                     CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED',
                                       'RETRYABLE_FAILED', 'PERMANENT_FAILED', 'CANCELLED')),
    scheduler_pid   INT NOT NULL,
    worker_pid      INT,
    attempt_no      INT NOT NULL DEFAULT 1,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    outcome_detail  TEXT,
    retryable       BOOLEAN,
    dispatch_tick_id BIGINT,
    tick_watermark_lsn PG_LSN
    ,
    outcome_code    TEXT CHECK (outcome_code IS NULL OR outcome_code IN
                     ('LOCK_TIMEOUT', 'STATEMENT_TIMEOUT', 'DEADLOCK',
                      'SERIALIZATION', 'OUT_OF_MEMORY', 'CANCELLED',
                      'PERMANENT', 'UNKNOWN_RETRYABLE')),
    outcome_sqlstate TEXT,
    worker_slot_generation BIGINT
);

CREATE INDEX IF NOT EXISTS idx_sched_jobs_status_enqueued
    ON pgtrickle.pgt_scheduler_jobs (status, enqueued_at);
CREATE INDEX IF NOT EXISTS idx_sched_jobs_unit_status
    ON pgtrickle.pgt_scheduler_jobs (unit_key, status);
CREATE INDEX IF NOT EXISTS idx_sched_jobs_finished
    ON pgtrickle.pgt_scheduler_jobs (finished_at)
    WHERE finished_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sched_jobs_terminal_finished
    ON pgtrickle.pgt_scheduler_jobs (finished_at, job_id)
    WHERE status IN ('SUCCEEDED', 'RETRYABLE_FAILED', 'PERMANENT_FAILED', 'CANCELLED');

-- Bootstrap source gates (v0.5.0, Phase 3)
-- Records which source tables are currently "gated" (bootstrapping in progress).
-- When a source is gated, all stream tables that depend on it are skipped by
-- the scheduler until pgtrickle.ungate_source() is called.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_source_gates (
    source_relid    OID PRIMARY KEY,
    gated           BOOLEAN NOT NULL DEFAULT true,
    gated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ungated_at      TIMESTAMPTZ,
    gated_by        TEXT
);

-- Per-source watermark state: tracks how far each external source has been loaded.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_watermarks (
    source_relid       OID PRIMARY KEY,
    watermark          TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    advanced_by        TEXT,
    wal_lsn_at_advance TEXT
);

-- Watermark groups: declare that a set of sources must be temporally aligned.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_watermark_groups (
    group_id           SERIAL PRIMARY KEY,
    group_name         TEXT UNIQUE NOT NULL,
    source_relids      OID[] NOT NULL,
    tolerance_secs     DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- DB-3: Schema version tracking table.
-- Records which schema migration versions have been applied to this database.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_schema_version (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    description TEXT
);
INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES ('0.19.0', 'Initial schema version tracking')
ON CONFLICT (version) DO NOTHING;
INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.84.0',
    'Bootstrap catalog parity repair and manifest tooling baseline'
)
ON CONFLICT (version) DO NOTHING;
INSERT INTO pgtrickle.pgt_schema_version (version, description)
VALUES (
    '0.85.0',
    'Scheduler and resource resilience gate'
)
ON CONFLICT (version) DO NOTHING;

SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_stream_tables', '');
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_dependencies', '');
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_change_buffers', '');
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_source_gates', '');
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_watermarks', '');
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_watermark_groups', '');

-- CIT-2: Cross-node advisory lock table for distributed stream table refresh.
-- Uses INSERT … ON CONFLICT DO NOTHING for atomic acquisition; timestamp-based
-- lease expiry handles crashed holders without requiring heartbeats.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_st_locks (
    lock_key    TEXT        NOT NULL,
    holder      TEXT        NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    CONSTRAINT pgt_st_locks_pkey PRIMARY KEY (lock_key)
);
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_st_locks', '');

-- CIT-3: Per-worker logical replication slot tracking for distributed sources.
-- Each row represents a WAL slot on one Citus worker node that feeds changes
-- for a given (stream_table, source) pair into the coordinator's change buffer.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_worker_slots (
    pgt_id       BIGINT      NOT NULL
                 REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    source_relid OID         NOT NULL,
    worker_name  TEXT        NOT NULL,
    worker_port  INT         NOT NULL DEFAULT 5432,
    slot_name    TEXT        NOT NULL,
    last_frontier TEXT,
    CONSTRAINT pgt_worker_slots_pkey
        PRIMARY KEY (pgt_id, source_relid, worker_name, worker_port)
);
SELECT pg_catalog.pg_extension_config_dump('pgtrickle.pgt_worker_slots', '');

-- G14-SHC: Shared template cache (catalog-backed, UNLOGGED)
CREATE UNLOGGED TABLE IF NOT EXISTS pgtrickle.pgt_template_cache (
    pgt_id       BIGINT PRIMARY KEY
                 REFERENCES pgtrickle.pgt_stream_tables(pgt_id) ON DELETE CASCADE,
    query_hash   BIGINT NOT NULL,
    delta_sql    TEXT NOT NULL,
    columns      TEXT[] NOT NULL,
    source_oids  INTEGER[] NOT NULL,
    is_dedup     BOOLEAN NOT NULL DEFAULT FALSE,
    key_changed  BOOLEAN NOT NULL DEFAULT FALSE,
    all_algebraic BOOLEAN NOT NULL DEFAULT FALSE,
    cached_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);


"#,
    name = "pg_trickle_catalog",
    bootstrap,
);

// ── Status overview view (requires parse_duration_seconds) ────────────

extension_sql!(
    r#"
-- Status overview view (ERR-1d: last_error_message and last_error_at are
-- included via st.* from pgt_stream_tables)
CREATE OR REPLACE VIEW pgtrickle.stream_tables_info AS
SELECT st.*,
       now() - st.last_refresh_at AS staleness,
       CASE WHEN st.schedule IS NOT NULL
                 AND st.schedule !~ '[\s@]'
            THEN EXTRACT(EPOCH FROM (now() - st.last_refresh_at)) >
                 pgtrickle.parse_duration_seconds(st.schedule)
            ELSE NULL::boolean
       END AS stale,
       CASE WHEN st.topk_limit IS NOT NULL THEN TRUE ELSE FALSE END AS is_topk
FROM pgtrickle.pgt_stream_tables st;
"#,
    name = "pg_trickle_info_view",
    requires = [parse_duration_seconds],
);

// ── v0.87.12: publication API security attributes ─────────────────────

extension_sql!(
    r#"
ALTER FUNCTION pgtrickle.stream_table_to_publication(text)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned immediately below.
ALTER FUNCTION pgtrickle.stream_table_to_publication(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
ALTER FUNCTION pgtrickle.drop_stream_table_publication(text)
    SECURITY DEFINER; -- nosemgrep: sql.security-definer.present — search_path is pinned immediately below.
ALTER FUNCTION pgtrickle.drop_stream_table_publication(text)
    SET search_path = pgtrickle, pg_catalog, pg_temp;
"#,
    name = "pg_trickle_publication_security",
    requires = ["pg_trickle_acl_policy"],
);

// ── Citus observability view ───────────────────────────────────────────

extension_sql!(
    r#"
-- CIT-4: Per-worker replication slot tracking view.
-- Safe to query on non-Citus deployments (pgt_change_tracking and
-- pgt_worker_slots are local catalog tables; no pg_dist_node reference).
-- Returns one row per (stream_table, source, worker) combination.
CREATE OR REPLACE VIEW pgtrickle.citus_status AS
SELECT
    st.pgt_id,
    st.pgt_schema,
    st.pgt_name,
    ct.source_relid,
    ct.source_stable_name,
    ct.slot_name           AS coordinator_slot,
    ct.source_placement,
    ct.frontier_per_node,
    ws.worker_name,
    ws.worker_port,
    ws.slot_name           AS worker_slot,
    ws.last_frontier       AS worker_frontier
FROM pgtrickle.pgt_change_tracking ct
JOIN pgtrickle.pgt_stream_tables   st ON st.pgt_id = ANY(ct.tracked_by_pgt_ids)
LEFT JOIN pgtrickle.pgt_worker_slots ws
       ON ws.pgt_id       = st.pgt_id
      AND ws.source_relid = ct.source_relid
WHERE ct.source_placement = 'distributed';
"#,
    name = "pg_trickle_citus_status_view",
    requires = ["pg_trickle_catalog"],
);

// ── DDL event triggers (Phase 7) ──────────────────────────────────────

extension_sql!(
    r#"
-- Create event trigger functions with correct RETURNS event_trigger type.
-- pgrx's #[pg_extern] generates RETURNS void, which PostgreSQL rejects for
-- event triggers. We create them manually here with the correct return type.
CREATE FUNCTION pgtrickle."_on_ddl_end"()
    RETURNS event_trigger
    SECURITY DEFINER -- nosemgrep: sql.security-definer.present — event trigger runs with pinned pgtrickle catalog search_path below.
    SET search_path TO pgtrickle, pg_catalog, pg_temp
    LANGUAGE c
    AS 'MODULE_PATHNAME', 'pg_trickle_on_ddl_end_wrapper';

CREATE FUNCTION pgtrickle."_on_sql_drop"()
    RETURNS event_trigger
    SECURITY DEFINER -- nosemgrep: sql.security-definer.present — event trigger runs with pinned pgtrickle catalog search_path below.
    SET search_path TO pgtrickle, pg_catalog, pg_temp
    LANGUAGE c
    AS 'MODULE_PATHNAME', 'pg_trickle_on_sql_drop_wrapper';

-- Event trigger: track ALTER TABLE on upstream sources
CREATE EVENT TRIGGER pg_trickle_ddl_tracker
    ON ddl_command_end
    EXECUTE FUNCTION pgtrickle._on_ddl_end();

-- Event trigger: track DROP TABLE on upstream sources / ST storage tables
CREATE EVENT TRIGGER pg_trickle_drop_tracker
    ON sql_drop
    EXECUTE FUNCTION pgtrickle._on_sql_drop();
"#,
    name = "pg_trickle_event_triggers",
);

// ── Monitoring views (Phase 9) ────────────────────────────────────────

extension_sql!(
    r#"
-- Convenience view: pg_stat_stream_tables
-- Combines catalog metadata with aggregate refresh statistics.
CREATE OR REPLACE VIEW pgtrickle.pg_stat_stream_tables AS
SELECT
    st.pgt_id,
    st.pgt_schema,
    st.pgt_name,
    st.status,
    st.refresh_mode,
    st.is_populated,
    st.data_timestamp,
    st.schedule,
    now() - st.last_refresh_at AS staleness,
    CASE WHEN st.schedule IS NOT NULL AND st.last_refresh_at IS NOT NULL
              AND st.schedule !~ '[\s@]'
         THEN EXTRACT(EPOCH FROM (now() - st.last_refresh_at)) >
              pgtrickle.parse_duration_seconds(st.schedule)
         ELSE NULL::boolean
    END AS stale,
    st.consecutive_errors,
    st.needs_reinit,
    st.last_refresh_at,
    COALESCE(stats.total_refreshes, 0) AS total_refreshes,
    COALESCE(stats.successful_refreshes, 0) AS successful_refreshes,
    COALESCE(stats.failed_refreshes, 0) AS failed_refreshes,
    COALESCE(stats.total_rows_inserted, 0) AS total_rows_inserted,
    COALESCE(stats.total_rows_updated, 0) AS total_rows_updated,
    COALESCE(stats.total_rows_deleted, 0) AS total_rows_deleted,
    stats.avg_duration_ms,
    stats.last_action,
    stats.last_status,
    (SELECT array_agg(DISTINCT d.cdc_mode ORDER BY d.cdc_mode)
     FROM pgtrickle.pgt_dependencies d
     WHERE d.pgt_id = st.pgt_id AND d.source_type = 'TABLE') AS cdc_modes,
    st.scc_id,
    st.last_fixpoint_iterations,
    st.refresh_tier
FROM pgtrickle.pgt_stream_tables st
LEFT JOIN LATERAL (
    SELECT
        count(*)::bigint AS total_refreshes,
        count(*) FILTER (WHERE h.status = 'COMPLETED')::bigint AS successful_refreshes,
        count(*) FILTER (WHERE h.status = 'FAILED')::bigint AS failed_refreshes,
        COALESCE(sum(h.rows_inserted), 0)::bigint AS total_rows_inserted,
        COALESCE(sum(h.rows_updated), 0)::bigint AS total_rows_updated,
        COALESCE(sum(h.rows_deleted), 0)::bigint AS total_rows_deleted,
        CASE WHEN count(*) FILTER (WHERE h.end_time IS NOT NULL) > 0
             THEN avg(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)
                  FILTER (WHERE h.end_time IS NOT NULL)
             ELSE NULL
        END::float8 AS avg_duration_ms,
        (SELECT h2.action FROM pgtrickle.pgt_refresh_history h2
         WHERE h2.pgt_id = st.pgt_id ORDER BY h2.refresh_id DESC LIMIT 1) AS last_action,
        (SELECT h2.status FROM pgtrickle.pgt_refresh_history h2
         WHERE h2.pgt_id = st.pgt_id ORDER BY h2.refresh_id DESC LIMIT 1) AS last_status,
        (SELECT h2.initiated_by FROM pgtrickle.pgt_refresh_history h2
         WHERE h2.pgt_id = st.pgt_id ORDER BY h2.refresh_id DESC LIMIT 1) AS last_initiated_by,
        (SELECT h2.freshness_deadline FROM pgtrickle.pgt_refresh_history h2
         WHERE h2.pgt_id = st.pgt_id ORDER BY h2.refresh_id DESC LIMIT 1) AS freshness_deadline
    FROM pgtrickle.pgt_refresh_history h
    WHERE h.pgt_id = st.pgt_id
) stats ON true;

-- v0.86.0: Bounded per-stream-table diagnostic statistics.
-- All cumulative values come from summary tables; scraping this view never
-- scans refresh history.
CREATE OR REPLACE VIEW pgtrickle.pg_stat_pgtrickle AS
SELECT
    st.pgt_id,
    st.pgt_schema AS schema_name,
    st.pgt_name AS table_name,
    COALESCE(s.total_refreshes, 0)::bigint AS total_refreshes,
    COALESCE(s.total_full_refreshes, 0)::bigint AS total_full_refreshes,
    COALESCE(s.total_diff_refreshes, 0)::bigint AS total_diff_refreshes,
    COALESCE(s.total_delta_rows_processed, 0)::bigint AS total_delta_rows_processed,
    CASE WHEN COALESCE(s.total_refreshes, 0) > 0
         THEN s.total_duration_ms::double precision / s.total_refreshes
    END AS avg_refresh_duration_ms,
    c.p95_ms AS p95_refresh_duration_ms,
    c.p99_ms AS p99_refresh_duration_ms,
    s.last_refresh_at,
    CASE WHEN COALESCE(s.last_refresh_at, st.created_at) IS NOT NULL
         THEN EXTRACT(EPOCH FROM (now() - COALESCE(s.last_refresh_at, st.created_at))) * 1000
    END AS current_lag_ms,
    COALESCE(st.requested_refresh_mode, st.refresh_mode) AS requested_refresh_mode,
    st.target_freshness_mode,
    st.freshness_deadline_ms AS target_freshness_ms,
    s.last_full_reason,
    s.last_full_reason_detail,
    st.last_error_message AS last_error,
    st.last_error_at,
    s.stats_reset_at
FROM pgtrickle.pgt_stream_tables st
LEFT JOIN pgtrickle.pgt_refresh_summary s ON s.pgt_id = st.pgt_id
LEFT JOIN pgtrickle.pgt_cost_model_summary c ON c.pgt_id = st.pgt_id;

-- Per-source CDC status view (G5): exposes cdc_mode, slot names, and
-- transition timestamps for every TABLE dependency of a stream table.
CREATE OR REPLACE VIEW pgtrickle.pgt_cdc_status AS
SELECT
    st.pgt_schema,
    st.pgt_name,
    d.source_relid,
    c.relname        AS source_name,
    n.nspname        AS source_schema,
    d.cdc_mode,
    d.slot_name,
    d.decoder_confirmed_lsn,
    d.transition_started_at
FROM pgtrickle.pgt_dependencies d
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
JOIN pg_class c ON c.oid = d.source_relid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE d.source_type = 'TABLE';
"#,
    name = "pg_trickle_monitoring_views",
    requires = [parse_duration_seconds],
);

// ── Quick health overview (ERG-E) ─────────────────────────────────────

extension_sql!(
    r#"
-- ERG-E: One-row health summary for dashboards and alerting.
CREATE OR REPLACE VIEW pgtrickle.quick_health AS
SELECT
    (SELECT count(*) FROM pgtrickle.pgt_stream_tables)::bigint
        AS total_stream_tables,
    (SELECT count(*) FROM pgtrickle.pgt_stream_tables
     WHERE status = 'ERROR' OR consecutive_errors > 0)::bigint
        AS error_tables,
    (SELECT count(*) FROM pgtrickle.pgt_stream_tables
     WHERE schedule IS NOT NULL
       AND schedule !~ '[\s@]'
       AND last_refresh_at IS NOT NULL
       AND EXTRACT(EPOCH FROM (now() - last_refresh_at)) >
           pgtrickle.parse_duration_seconds(schedule))::bigint
        AS stale_tables,
    (SELECT count(*) > 0 FROM pg_stat_activity
     WHERE backend_type = 'pg_trickle scheduler')
        AS scheduler_running,
    CASE
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables) = 0 THEN 'EMPTY'
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE status = 'SUSPENDED') > 0 THEN 'CRITICAL'
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE status = 'ERROR' OR consecutive_errors > 0) > 0 THEN 'WARNING'
        WHEN (SELECT count(*) FROM pgtrickle.pgt_stream_tables
              WHERE schedule IS NOT NULL
                AND schedule !~ '[\s@]'
                AND last_refresh_at IS NOT NULL
                AND EXTRACT(EPOCH FROM (now() - last_refresh_at)) >
                    pgtrickle.parse_duration_seconds(schedule)) > 0 THEN 'WARNING'
        ELSE 'OK'
    END AS status;
"#,
    name = "pg_trickle_quick_health_view",
    requires = [parse_duration_seconds],
);

// ── OP-3: pause_all / resume_all ─────────────────────────────────────

extension_sql!(
    r#"
CREATE OR REPLACE FUNCTION pgtrickle."pause_all"()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE pgtrickle.pgt_stream_tables
       SET status = 'PAUSED'
     WHERE status = 'ACTIVE';
    RAISE NOTICE 'pg_trickle: all stream tables paused.';
END;
$$;

COMMENT ON FUNCTION pgtrickle."pause_all"() IS
    'Pause automatic refreshes for every ACTIVE stream table. '
    'Use pgtrickle.resume_all() to re-activate them.';

CREATE OR REPLACE FUNCTION pgtrickle."resume_all"()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE pgtrickle.pgt_stream_tables
       SET status = 'ACTIVE'
     WHERE status = 'PAUSED';
    RAISE NOTICE 'pg_trickle: all paused stream tables resumed.';
END;
$$;

COMMENT ON FUNCTION pgtrickle."resume_all"() IS
    'Re-activate all stream tables that were paused with pgtrickle.pause_all().';
"#,
    name = "pg_trickle_pause_resume",
);

// ── OP-4: refresh_if_stale ────────────────────────────────────────────

extension_sql!(
    r#"
CREATE OR REPLACE FUNCTION pgtrickle."refresh_if_stale"(
    p_name   text,
    p_max_age interval DEFAULT '5 minutes'::interval
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_end timestamp with time zone;
    v_refreshed boolean := false;
BEGIN
    SELECT MAX(end_time)
      INTO v_last_end
      FROM pgtrickle.pgt_refresh_history h
      JOIN pgtrickle.pgt_stream_tables   s USING (pgt_id)
     WHERE s.pgt_name = p_name
       AND h.status = 'COMPLETED';

    IF v_last_end IS NULL OR (now() - v_last_end) > p_max_age THEN
        PERFORM pgtrickle.refresh_stream_table(p_name);
        v_refreshed := true;
    END IF;

    RETURN v_refreshed;
END;
$$;

COMMENT ON FUNCTION pgtrickle."refresh_if_stale"(text, interval) IS
    'Refresh the named stream table only when the most recent completed '
    'refresh is older than max_age.  Returns TRUE when a refresh was '
    'triggered, FALSE when the table was fresh enough.';
"#,
    name = "pg_trickle_refresh_if_stale",
    requires = [refresh_stream_table],
);

// ── OP-5: stream_table_definition ────────────────────────────────────

extension_sql!(
    r#"
CREATE OR REPLACE FUNCTION pgtrickle."stream_table_definition"(
    p_name text
)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT pgtrickle.export_definition(p_name);
$$;

COMMENT ON FUNCTION pgtrickle."stream_table_definition"(text) IS
    'Return the CREATE STREAM TABLE DDL for the named stream table. '
    'Equivalent to pgtrickle.export_definition(name) — provided as a '
    'more discoverable alias.';
"#,
    name = "pg_trickle_stream_table_definition",
    requires = [export_definition],
);

// ── OPS-1: Canary / shadow-mode helpers ──────────────────────────────

extension_sql!(
    r#"
CREATE OR REPLACE FUNCTION pgtrickle."canary_begin"(
    p_name      text,
    p_new_query text
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema  text;
    v_table   text;
    v_canary  text;
    v_dot     int;
BEGIN
    v_dot    := strpos(p_name, '.');
    IF v_dot > 0 THEN
        v_schema := substr(p_name, 1, v_dot - 1);
        v_table  := substr(p_name, v_dot + 1);
    ELSE
        v_schema := current_schema();
        v_table  := p_name;
    END IF;

    v_canary := '__pgt_canary_' || v_table;

    -- Drop any stale canary table from a previous run.
    BEGIN
        PERFORM pgtrickle.drop_stream_table(v_schema || '.' || v_canary);
    EXCEPTION WHEN OTHERS THEN
        NULL;  -- ignore if it does not exist
    END;

    -- Create the canary stream table with the new query.
    PERFORM pgtrickle.create_stream_table(
        v_schema || '.' || v_canary,
        p_new_query
    );

    RETURN format(
        'Canary stream table %I.%I created. Run pgtrickle.canary_diff(%L) to compare.',
        v_schema, v_canary, p_name
    );
END;
$$;

COMMENT ON FUNCTION pgtrickle."canary_begin"(text, text) IS
    'Start a shadow/canary test for the named stream table. '
    'Creates __pgt_canary_<name> with p_new_query and starts refreshing it. '
    'Use canary_diff(name) to inspect differences and canary_promote(name) to '
    'swap canary into production.';

CREATE OR REPLACE FUNCTION pgtrickle."canary_diff"(
    p_name text
)
RETURNS TABLE(
    row_source text,
    diff_row   text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema  text;
    v_table   text;
    v_canary  text;
    v_dot     int;
    v_sql     text;
BEGIN
    v_dot    := strpos(p_name, '.');
    IF v_dot > 0 THEN
        v_schema := substr(p_name, 1, v_dot - 1);
        v_table  := substr(p_name, v_dot + 1);
    ELSE
        v_schema := current_schema();
        v_table  := p_name;
    END IF;

    v_canary := '__pgt_canary_' || v_table;

    -- Return rows in live-only vs canary-only using EXCEPT (symmetric difference).
    v_sql := format(
        '(SELECT %L AS row_source, t::text AS diff_row FROM %I.%I t EXCEPT
          SELECT %L, c::text FROM %I.%I c)
         UNION ALL
         (SELECT %L, c::text FROM %I.%I c EXCEPT
          SELECT %L, t::text FROM %I.%I t)',
        'live_only',   v_schema, v_table,
        'canary_only', v_schema, v_canary,
        'canary_only', v_schema, v_canary,
        'live_only',   v_schema, v_table
    );
    RETURN QUERY EXECUTE v_sql;
END;
$$;

COMMENT ON FUNCTION pgtrickle."canary_diff"(text) IS
    'Compare the live stream table with its canary counterpart. '
    'Returns rows that exist in only one of the two tables. '
    'An empty result set indicates the new query produces the same output.';

CREATE OR REPLACE FUNCTION pgtrickle."canary_promote"(
    p_name text
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema    text;
    v_table     text;
    v_canary    text;
    v_dot       int;
    v_new_query text;
BEGIN
    v_dot    := strpos(p_name, '.');
    IF v_dot > 0 THEN
        v_schema := substr(p_name, 1, v_dot - 1);
        v_table  := substr(p_name, v_dot + 1);
    ELSE
        v_schema := current_schema();
        v_table  := p_name;
    END IF;

    v_canary := '__pgt_canary_' || v_table;

    -- Read the defining query from the canary table.
    SELECT defining_query
      INTO v_new_query
      FROM pgtrickle.pgt_stream_tables
     WHERE pgt_schema = v_schema
       AND pgt_name   = v_canary;

    IF v_new_query IS NULL THEN
        RAISE EXCEPTION 'No canary found for %. Run pgtrickle.canary_begin() first.', p_name;
    END IF;

    -- Promote: alter the live table to use the new query, then drop the canary.
    PERFORM pgtrickle.alter_stream_table(v_schema || '.' || v_table, query => v_new_query);

    BEGIN
        PERFORM pgtrickle.drop_stream_table(v_schema || '.' || v_canary);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    RETURN format(
        'Canary promoted: %I.%I now uses the canary query. Canary table dropped.',
        v_schema, v_table
    );
END;
$$;

COMMENT ON FUNCTION pgtrickle."canary_promote"(text) IS
    'Promote the canary stream table to production. '
    'Calls ALTER STREAM TABLE with the canary query, then drops the canary table. '
    'Run pgtrickle.canary_diff(name) first to confirm the result set matches.';
"#,
    name = "pg_trickle_canary",
    requires = [create_stream_table, drop_stream_table, alter_stream_table],
);

// ── v0.46.0: Outbox attachment catalog (pg_tide integration) ───────────

extension_sql!(
    r#"
-- v0.46.0: Slim outbox attachment catalog.
-- Maps stream tables to their pg_tide outbox names.
-- The full outbox/inbox/relay stack lives in pg_tide (trickle-labs/pg-tide).
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_outbox_config (
    stream_table_oid         OID         NOT NULL PRIMARY KEY,
    stream_table_name        TEXT        NOT NULL,
    tide_outbox_name         TEXT        NOT NULL,
    -- VA-4 (v0.48.0): optional vector column name for embedding outbox events.
    embedding_vector_column  TEXT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pgt_outbox_config_name
    ON pgtrickle.pgt_outbox_config (stream_table_name);

COMMENT ON TABLE pgtrickle.pgt_outbox_config IS
    'v0.46.0: Catalog of stream tables with a pg_tide outbox attached via attach_outbox(). '
    'v0.48.0: embedding_vector_column set when attached via attach_embedding_outbox().';
"#,
    name = "pg_trickle_outbox_catalog",
    requires = [],
);

// ── VH-2 (v0.48.0): Distance subscription catalog ─────────────────────────
extension_sql!(
    r#"
-- VH-2 (v0.48.0): Distance-predicate subscription catalog.
-- Stores per-(stream_table, channel) vector distance subscriptions.
-- After each non-empty refresh the background worker evaluates the predicate
-- and emits pg_notify(channel, payload) when matched_rows > 0.
CREATE TABLE IF NOT EXISTS pgtrickle.pgt_distance_subscriptions (
    stream_table    TEXT NOT NULL,
    channel         TEXT NOT NULL,
    vector_column   TEXT NOT NULL,
    query_vector    TEXT NOT NULL,
    op              TEXT NOT NULL
        CHECK (op IN ('<->', '<=>', '<#>', '<+>', '<<->>', '<<%>>')),
    threshold       DOUBLE PRECISION NOT NULL CHECK (threshold > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_table, channel)
);

COMMENT ON TABLE pgtrickle.pgt_distance_subscriptions IS
    'VH-2 (v0.48.0): Distance-predicate NOTIFY subscriptions per stream table. '
    'Populated via pgtrickle.subscribe_distance() / pgtrickle.unsubscribe_distance().';
"#,
    name = "pg_trickle_distance_subscriptions_catalog",
    requires = [],
);

// ── v0.84.0: explicit SQL ACL policy ─────────────────────────────────

extension_sql!(
    r#"
-- Generated by scripts/check_sql_api_policy.py emit-acl-sql
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle FROM PUBLIC;

-- Explicit overload policy follows.

-- admin_global
REVOKE EXECUTE ON FUNCTION pgtrickle.advance_watermark(text, timestamp with time zone) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.clear_caches() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.convert_buffers_to_unlogged() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_refresh_group(text, text[], text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_watermark_group(text, text[], double precision) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.drain(integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.drop_refresh_group(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.drop_watermark_group(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.gate_source(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.lifecycle_preflight() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.migrate() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.pause_all() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.pause_scheduler(text[]) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.rebuild_cdc_triggers() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.restore_stream_tables() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.resume_all() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.resume_scheduler(text[]) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.resume_after_drain() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.setup_self_monitoring() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.teardown_self_monitoring() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.ungate_source(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.stat_reset_all() FROM PUBLIC;

-- arbitrary_sql
REVOKE EXECUTE ON FUNCTION pgtrickle.write_and_refresh(text, text) FROM PUBLIC;

-- internal
REVOKE EXECUTE ON FUNCTION pgtrickle._signal_launcher_rescan() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.handle_vp_promoted(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.pgt_ivm_apply_delta(bigint, integer, boolean, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.pgt_ivm_apply_delta_enr(bigint, integer, boolean, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.pgt_ivm_handle_truncate(bigint) FROM PUBLIC;

-- owner_lifecycle
REVOKE EXECUTE ON FUNCTION pgtrickle.alter_stream_table(text, text, text, text, text, text, text, text, boolean, boolean, text, text, bigint, integer, text, integer, double precision, text, double precision, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.attach_embedding_outbox(text, text, integer, integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.attach_outbox(text, integer, integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.bulk_alter_stream_tables(text[], json) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.bulk_create(jsonb) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.bulk_drop_stream_tables(text[]) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.canary_begin(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.canary_diff(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.canary_promote(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_or_replace_stream_table(text, text, text, text, boolean, text, text, text, boolean, boolean, text, integer, double precision, text, boolean, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table(text, text, text, text, boolean, text, text, text, boolean, boolean, text, integer, double precision, text, boolean, text, integer, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table_batch(text, text, text, boolean, text, integer, double precision) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table_cost_optimized(text, text, text, boolean, text, integer, double precision) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table_fast_append_only(text, text, text, text, text, integer, double precision) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table_if_not_exists(text, text, text, text, boolean, text, text, text, boolean, boolean, text, integer, double precision, text, boolean, text, integer, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.create_stream_table_realtime(text, text, text, boolean, text, integer, double precision) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.detach_outbox(text, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.drop_snapshot(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.drop_stream_table(text, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.drop_stream_table_publication(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.embedding_stream_table(text, text, text, text, text, text, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.exec_stream_ddl(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.pause_stream_table(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.refresh_efficiency() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.refresh_groups() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.refresh_if_stale(text, interval) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.refresh_stream_table(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.refresh_timeline(integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.repair_stream_table(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.reset_fuse(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.restore_from_snapshot(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.resume_stream_table(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.set_stream_table_refresh_policy(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.set_stream_table_sla(text, interval) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.set_stream_table_storage_policy(text, boolean, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.snapshot_stream_table(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.stream_table_to_publication(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.subscribe(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.subscribe_distance(text, text, text, text, text, double precision) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.unsubscribe(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.unsubscribe_distance(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle.stat_reset(bigint) FROM PUBLIC;

-- public_read
GRANT EXECUTE ON FUNCTION pgtrickle.bootstrap_gate_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.cache_stats() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.cdc_pause_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.change_buffer_sizes() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.check_cdc_health() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.cluster_worker_summary() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.commit_latency_stats() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.dedup_stats() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.dependency_tree() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.diagnose_errors(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.diamond_groups() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_dag(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_delta(text, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_diff_sql(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_json(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_query_rewrite(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_refresh_mode(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_st(text, boolean) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.explain_stream_table(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.export_definition(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.fuse_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.get_refresh_history(text, integer) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.get_staleness(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.health_check() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.health_summary() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.history_prune_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.is_drained() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.list_auxiliary_columns(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.list_distance_subscriptions(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.list_snapshots(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.list_sources(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.list_subscriptions() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.metrics_summary() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.parallel_job_status(integer) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.parse_duration_seconds(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.pg_trickle_hash(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.pg_trickle_hash_multi(text[]) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.pgt_scc_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.pgt_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.pgtrickle_refresh_stats() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.preflight() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.preview_stream_table(text, text, text, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.recommend_refresh_mode(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.recommend_schedule(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.reliability_counters() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.schedule_recommendations() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.scheduler_overhead() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.self_monitoring_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.shared_buffer_stats() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.sla_summary() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.slot_health() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.source_gates() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.source_stable_name(oid) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.st_auto_threshold(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.st_refresh_stats() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.stream_table_definition(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.stream_table_lineage(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.stream_table_spec(oid) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.stream_table_spec(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.trigger_inventory() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.tune_recommendations() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.validate_query(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.vector_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.version() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.version_check() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.view_evolution_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.wal_source_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.watermark_groups() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.watermark_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.watermarks() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.worker_allocation_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION pgtrickle.worker_pool_status() TO PUBLIC;

-- trigger_entry
REVOKE EXECUTE ON FUNCTION pgtrickle._on_ddl_end() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pgtrickle._on_sql_drop() FROM PUBLIC;

SELECT pgtrickle._signal_launcher_rescan();

"#,
    name = "pg_trickle_acl_policy",
    requires = [
        "pg_trickle_event_triggers",
        "pg_trickle_pause_resume",
        "pg_trickle_refresh_if_stale",
        "pg_trickle_stream_table_definition",
        "pg_trickle_canary",
        "pg_trickle_distance_subscriptions_catalog",
        _signal_launcher_rescan,
        preview_stream_table,
        create_stream_table_realtime,
        create_stream_table_batch,
        create_stream_table_cost_optimized,
    ],
    finalize,
);
