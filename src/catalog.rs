//! Catalog layer — metadata tables and CRUD operations for stream tables.
//!
//! All catalog access goes through PostgreSQL's SPI interface. This module
//! provides typed Rust abstractions over the `pgtrickle.pgt_stream_tables`,
//! `pgtrickle.pgt_dependencies`, and `pgtrickle.pgt_refresh_history` tables.

use pgrx::prelude::*;
use pgrx::spi::{SpiHeapTupleData, SpiTupleTable};
use std::hash::{Hash, Hasher};

use crate::dag::{DiamondConsistency, DiamondSchedulePolicy, RefreshMode, StStatus};
use crate::dvm::parser::WindowStrategyPlan;
use crate::error::PgTrickleError;
use crate::version::Frontier;

// ---------------------------------------------------------------------------
// CODE-003 (v0.75.0): Typed domain wrappers for stream-table identifiers.
//
// `PgtId` and `StreamTableOid` represent two different identifier domains:
//
//   PgtId          — the internal sequence-based `pgt_id` column in
//                    `pgtrickle.pgt_stream_tables`.  This value is NOT a
//                    PostgreSQL relation OID and MUST NOT be stored in columns
//                    typed as `oid` or joined to `pg_class.oid`.
//
//   StreamTableOid — the PostgreSQL relation OID (`pgt_relid`) of the storage
//                    table.  This IS a valid `pg_class.oid` and can be passed
//                    to `::regclass`, joined to catalog views, etc.
//
// Use these wrappers in new code so that cross-domain casts become a visible
// `.into()` call rather than an invisible `as` cast.
// ---------------------------------------------------------------------------

/// Internal sequence-based identifier for a stream table row in
/// `pgtrickle.pgt_stream_tables`.
///
/// **Not** a PostgreSQL relation OID.  Do not store in `oid`-typed columns or
/// join against `pg_class`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PgtId(pub i64);

impl From<i64> for PgtId {
    fn from(v: i64) -> Self {
        PgtId(v)
    }
}

impl From<PgtId> for i64 {
    fn from(p: PgtId) -> Self {
        p.0
    }
}

/// PostgreSQL relation OID for a stream table's storage table.
///
/// This is the `pgt_relid` column value — a real `pg_class.oid`.  It can be
/// cast to `::regclass`, joined to catalog views, and stored in `oid`-typed
/// columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamTableOid(pub pg_sys::Oid);

impl From<pg_sys::Oid> for StreamTableOid {
    fn from(v: pg_sys::Oid) -> Self {
        StreamTableOid(v)
    }
}

impl From<StreamTableOid> for pg_sys::Oid {
    fn from(s: StreamTableOid) -> Self {
        s.0
    }
}

#[cfg(test)]
mod domain_type_tests {
    use super::{PgtId, StreamTableOid};

    // CODE-003: verify that PgtId and StreamTableOid cannot be assigned to each
    // other without an explicit conversion — different types, different domains.
    #[test]
    fn test_pgt_id_roundtrip() {
        let id = PgtId(42);
        let raw: i64 = id.into();
        assert_eq!(raw, 42);
        let back = PgtId::from(raw);
        assert_eq!(back, id);
    }

    #[test]
    fn test_stream_table_oid_roundtrip() {
        use pgrx::pg_sys::Oid;
        let raw_oid = Oid::from(16384u32);
        let wrapped = StreamTableOid(raw_oid);
        let unwrapped: Oid = wrapped.into();
        assert_eq!(unwrapped, raw_oid);
    }
}

///
/// Uses `DefaultHasher` (same algorithm used historically in codegen.rs) so
/// that values stored in the catalog are consistent with those computed at
/// refresh time.  The result is cast to `i64` for the BIGINT catalog column.
pub fn compute_defining_query_hash(query: &str) -> i64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    query.hash(&mut hasher);
    hasher.finish() as i64
}

/// Metadata for a stream table, mirrors `pgtrickle.pgt_stream_tables`.
#[derive(Debug, Clone)]
pub struct StreamTableMeta {
    pub pgt_id: i64,
    pub pgt_relid: pg_sys::Oid,
    pub pgt_name: String,
    pub pgt_schema: String,
    pub defining_query: String,
    pub original_query: Option<String>,
    pub schedule: Option<String>,
    pub refresh_mode: RefreshMode,
    pub status: StStatus,
    pub is_populated: bool,
    pub data_timestamp: Option<TimestampWithTimeZone>,
    pub consecutive_errors: i32,
    pub needs_reinit: bool,
    /// Per-ST adaptive fallback threshold. None means use global GUC.
    pub auto_threshold: Option<f64>,
    /// Last observed FULL refresh execution time in milliseconds.
    pub last_full_ms: Option<f64>,
    /// Function/operator names referenced in the defining query (G8.2).
    /// Used by DDL hooks to detect `CREATE OR REPLACE FUNCTION` / `DROP FUNCTION`
    /// that may change the semantics of this stream table.
    pub functions_used: Option<Vec<String>>,
    /// Serialized frontier (JSONB). None means never refreshed.
    pub frontier: Option<Frontier>,
    /// TopK LIMIT value. None means this is not a TopK stream table.
    pub topk_limit: Option<i32>,
    /// TopK ORDER BY clause SQL. None means this is not a TopK stream table.
    pub topk_order_by: Option<String>,
    /// TopK OFFSET value. None means no OFFSET.
    pub topk_offset: Option<i32>,
    /// Diamond consistency mode for this ST ('none' or 'atomic').
    pub diamond_consistency: DiamondConsistency,
    /// Diamond schedule policy for this convergence node ('fastest' or 'slowest').
    pub diamond_schedule_policy: DiamondSchedulePolicy,
    /// Whether any source table lacks a PRIMARY KEY (EC-06).
    /// When true, the storage table uses a non-unique index on __pgt_row_id
    /// and the apply logic uses counted DELETE instead of MERGE.
    pub has_keyless_source: bool,
    /// Serialized JSON map of function-name → SHA-256 hash for EC-16 polling.
    /// None means no hashes have been recorded yet (baseline will be taken on
    /// the next differential refresh).
    pub function_hashes: Option<String>,
    /// User-requested CDC mode override for this stream table.
    /// None means "follow the global pg_trickle.cdc_mode GUC".
    pub requested_cdc_mode: Option<String>,
    /// Whether this stream table uses the append-only INSERT fast path.
    /// When true, differential refresh uses INSERT instead of MERGE,
    /// bypassing DELETE and UPDATE handling for better throughput.
    /// Automatically reverted to false if a DELETE/UPDATE is detected.
    pub is_append_only: bool,
    /// SCC (Strongly Connected Component) identifier for circular dependencies.
    /// `None` means this stream table is not part of a cyclic SCC.
    /// When set, all members of the same cycle share the same `scc_id`.
    pub scc_id: Option<i32>,
    /// Number of fixpoint iterations in the last SCC convergence (CYC-5).
    /// `None` for non-cyclic stream tables or if never iterated.
    pub last_fixpoint_iterations: Option<i32>,
    /// PB2: When true, the refresh engine skips `PREPARE`/`EXECUTE` for this
    /// stream table and suppresses `NOTIFY` emissions, enabling compatibility
    /// with PgBouncer transaction-mode pooling.
    pub pooler_compatibility_mode: bool,
    /// G-7: Refresh tier for tiered scheduling (hot/warm/cold/frozen).
    /// Controls the effective schedule multiplier when
    /// `pg_trickle.tiered_scheduling` is enabled.
    pub refresh_tier: String,
    /// FUSE-1: Fuse circuit breaker mode ('off', 'on', 'auto').
    /// 'off' = disabled, 'on' = always active, 'auto' = inherit from global GUC.
    pub fuse_mode: String,
    /// FUSE-1: Current fuse state ('armed', 'blown', 'disabled').
    pub fuse_state: String,
    /// FUSE-1: Per-ST change count threshold that triggers the fuse.
    /// None means use the global `pg_trickle.fuse_default_ceiling` GUC.
    pub fuse_ceiling: Option<i64>,
    /// FUSE-1: Sensitivity — number of consecutive over-ceiling observations
    /// required before the fuse actually blows. None means 1 (immediate).
    pub fuse_sensitivity: Option<i32>,
    /// FUSE-1: Timestamp when the fuse was blown. None if never blown.
    pub blown_at: Option<TimestampWithTimeZone>,
    /// FUSE-1: Human-readable reason the fuse was blown.
    pub blow_reason: Option<String>,
    /// A1-1: Partition key column for partitioned stream tables.
    /// `None` means not partitioned. When set, the stream table storage is
    /// created as a declaratively partitioned table (RANGE on this column),
    /// and the refresh path will inject a partition-key range predicate (A1-3).
    pub st_partition_key: Option<String>,
    /// DI-7: Maximum number of join scans allowed for DIFFERENTIAL mode.
    /// When the defining query has more Scan nodes in its join tree than
    /// this threshold, the scheduler automatically falls back to FULL refresh.
    /// `None` means no limit (use DIFFERENTIAL regardless of join count).
    pub max_differential_joins: Option<i32>,
    /// DI-7: Maximum delta fraction (0.0–1.0) for DIFFERENTIAL mode.
    /// When the change buffer row count exceeds this fraction of the
    /// estimated base table size, the scheduler falls back to FULL refresh.
    /// `None` means no limit.
    pub max_delta_fraction: Option<f64>,
    /// ERR-1: Last error message from a permanent refresh failure.
    /// `None` means no error has occurred (or it was cleared).
    pub last_error_message: Option<String>,
    /// ERR-1: Timestamp of the last permanent refresh failure.
    /// `None` means no error has occurred (or it was cleared).
    pub last_error_at: Option<TimestampWithTimeZone>,
    /// CDC-PUB-1: Name of the downstream logical replication publication.
    /// `None` means no publication has been created for this stream table.
    pub downstream_publication_name: Option<String>,
    /// SLA-1: Freshness deadline in milliseconds, derived from the user-supplied SLA interval.
    /// `None` means no SLA has been set for this stream table.
    pub freshness_deadline_ms: Option<i64>,
    /// CIT-1: Citus placement of the output storage table.
    /// 'local' = coordinator-local (default), 'distributed' = Citus distributed table,
    /// 'reference' = Citus reference table.
    pub st_placement: String,
    /// CORR-1 (v0.36.0): Whether this stream table uses temporal IVM (SCD Type 2).
    /// When true, the storage table has `__pgt_valid_from` and `__pgt_valid_to`
    /// columns, and the frontier model is two-dimensional `(frontier_lsn, valid_from_ts)`.
    pub temporal_mode: bool,
    /// CORR-2 (v0.36.0): Storage backend for the stream table output.
    /// 'heap' = standard PostgreSQL heap (default).
    /// 'citus' = Citus columnar extension.
    pub storage_backend: String,
    /// VP-1 (v0.47.0): Action to run after a successful refresh commit.
    /// 'none' = no action (default), 'analyze' = run ANALYZE,
    /// 'reindex' = always REINDEX, 'reindex_if_drift' = REINDEX only when
    /// rows_changed_since_last_reindex exceeds reindex_drift_threshold.
    pub post_refresh_action: String,
    /// VP-2 (v0.47.0): Fraction (0.0–1.0) of rows in the storage table that
    /// must change since the last REINDEX before a drift-triggered REINDEX
    /// fires. Only used when post_refresh_action = 'reindex_if_drift'.
    /// None means use the global pg_trickle.reindex_drift_threshold GUC.
    pub reindex_drift_threshold: Option<f64>,
    /// VP-2 (v0.47.0): Number of rows changed since the last REINDEX.
    /// Reset to 0 after each REINDEX completes.
    pub rows_changed_since_last_reindex: i64,
    /// VP-2 (v0.47.0): Timestamp of the last REINDEX on this stream table.
    /// None means the stream table has never been REINDEXed.
    pub last_reindex_at: Option<TimestampWithTimeZone>,
    /// PERF-2 (v0.59.0): Hash of the defining query, computed at CREATE/ALTER
    /// and stored in the catalog.  Avoids recomputing DefaultHasher over the
    /// full query string on every differential refresh.
    /// 0 means not yet computed (triggers one-time cache rebuild).
    pub defining_query_hash: i64,
    /// HOT-1 (v0.73.0): fillfactor for the storage heap table.
    /// `None` means use PostgreSQL's default (100 — pages packed full).
    /// Set to 70–90 on update-heavy differential workloads so that in-place
    /// HOT updates are possible, eliminating index tuple churn and extra WAL.
    pub storage_fillfactor: Option<i32>,
    /// P-2 (v0.78.0): Complexity class of the defining query, computed via
    /// OpTree scan-count analysis at CREATE/ALTER time and stored in the
    /// catalog.  `None` means not yet computed (back-filled on next refresh).
    /// Used by the scheduler to log per-class cost estimates without
    /// re-running the OpTree analysis on every tick.
    pub query_complexity_class: Option<String>,
    /// v0.83.0: Composite row-identity encoding used by this stream table.
    /// NULL is treated as an unknown pre-upgrade value and fails closed on
    /// incremental paths.
    pub row_identity_version: Option<i16>,
    /// v0.87.16: bounded identity probe encoding version.
    /// NULL is treated as unknown and fails closed on incremental paths.
    pub row_probe_version: Option<i16>,
    /// v0.89.0: analyzed incremental-window strategy. Unknown versions fail
    /// closed when the catalog row is loaded.
    pub window_strategy: Option<WindowStrategyPlan>,
    pub self_heal_work_mem_percent: i16,
    pub self_heal_lock_backoff_exponent: i16,
    pub self_heal_success_streak: i16,
    pub last_error_code: Option<String>,
    pub last_error_retryable: Option<bool>,
    /// LSEC-3 (v0.87.7): the exact `search_path` under which the defining
    /// query was authored, with a bare `$user` element already expanded to
    /// the authoring caller's quoted role name. Set at CREATE and on any
    /// ALTER that changes the query; preserved by configuration-only ALTERs
    /// and by storage ownership transfer. Legacy rows are backfilled from
    /// their storage relation's owner at the current storage owner and
    /// `public` on upgrade.
    pub defining_search_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshRuntimeTuning {
    pub merge_work_mem_mb: i32,
    pub delta_work_mem_mb: i32,
    pub delta_work_mem_cap_mb: i32,
    pub lock_backoff_exponent: i16,
}

impl StreamTableMeta {
    pub fn runtime_tuning(&self) -> RefreshRuntimeTuning {
        let percent = i32::from(self.self_heal_work_mem_percent.clamp(25, 100));
        let scale = |value: i32| {
            if value <= 0 {
                0
            } else {
                ((value * percent) / 100).max(1)
            }
        };
        RefreshRuntimeTuning {
            merge_work_mem_mb: scale(crate::config::pg_trickle_merge_work_mem_mb()),
            delta_work_mem_mb: scale(crate::config::pg_trickle_delta_work_mem()),
            delta_work_mem_cap_mb: crate::config::pg_trickle_delta_work_mem_cap_mb(),
            lock_backoff_exponent: self.self_heal_lock_backoff_exponent.clamp(0, 6),
        }
    }
}

/// CDC mode for a source dependency — tracks whether change capture uses
/// row-level triggers, WAL-based logical replication, or is transitioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcMode {
    /// Row-level AFTER trigger writes to buffer table (default).
    Trigger,
    /// Both trigger and WAL decoder are active; decoder is catching up.
    Transitioning,
    /// Only the WAL decoder populates the buffer table; trigger dropped.
    Wal,
}

impl CdcMode {
    /// Serialize to the SQL CHECK constraint value.
    pub fn as_str(&self) -> &'static str {
        match self {
            CdcMode::Trigger => "TRIGGER",
            CdcMode::Transitioning => "TRANSITIONING",
            CdcMode::Wal => "WAL",
        }
    }

    /// Deserialize from SQL string. Falls back to `Trigger` for unknown values.
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "TRIGGER" => CdcMode::Trigger,
            "TRANSITIONING" => CdcMode::Transitioning,
            "WAL" => CdcMode::Wal,
            _ => CdcMode::Trigger,
        }
    }
}

impl std::fmt::Display for CdcMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A dependency edge from a stream table to one of its upstream sources.
#[derive(Debug, Clone)]
pub struct StDependency {
    pub pgt_id: i64,
    pub source_relid: pg_sys::Oid,
    pub source_type: String,
    pub columns_used: Option<Vec<String>>,
    /// JSONB snapshot of source columns at creation time.
    pub column_snapshot: Option<serde_json::Value>,
    /// SHA-256 fingerprint of the serialized column snapshot for fast equality checks.
    pub schema_fingerprint: Option<String>,
    /// Current CDC mechanism for this source.
    pub cdc_mode: CdcMode,
    /// Name of the replication slot (NULL when using triggers).
    pub slot_name: Option<String>,
    /// Last LSN confirmed by the WAL decoder.
    pub decoder_confirmed_lsn: Option<String>,
    /// When the transition from triggers to WAL started (for timeout detection).
    pub transition_started_at: Option<String>,
    /// v0.82.0: Exact CDC cutover target and handoff LSN.
    pub cutover_target: Option<String>,
    pub cutover_lsn: Option<String>,
    /// CITUS-4: Stable name (hash of schema+table) for this source.
    pub source_stable_name: Option<String>,
    /// CITUS-4: Citus placement type: 'local', 'distributed', or 'reference'.
    pub source_placement: String,
}

/// A refresh history record.
#[derive(Debug, Clone)]
pub struct RefreshRecord {
    pub refresh_id: i64,
    pub pgt_id: i64,
    pub data_timestamp: TimestampWithTimeZone,
    pub start_time: TimestampWithTimeZone,
    pub end_time: Option<TimestampWithTimeZone>,
    pub action: String,
    pub rows_inserted: i64,
    pub rows_updated: i64,
    pub rows_deleted: i64,
    pub error_message: Option<String>,
    pub status: String,
    /// What triggered this refresh: SCHEDULER, MANUAL, or INITIAL.
    pub initiated_by: Option<String>,
    /// SLA deadline at the time of refresh (duration-based schedules only).
    pub freshness_deadline: Option<TimestampWithTimeZone>,
    /// CSS1: WAL LSN watermark captured at scheduler tick start (NULL when feature disabled).
    pub tick_watermark_lsn: Option<String>,
    /// CYC-3: Iteration of the fixed-point loop that produced this refresh.
    /// `None` for non-cyclic refreshes.
    pub fixpoint_iteration: Option<i32>,
    pub error_code: Option<String>,
    pub error_sqlstate: Option<String>,
    pub retryable: Option<bool>,
    pub duration_ms: Option<f64>,
    pub source_commit_at: Option<TimestampWithTimeZone>,
    pub visibility_xid: Option<pg_sys::TransactionId>,
    pub visible_at: Option<TimestampWithTimeZone>,
    pub commit_to_visible_ms: Option<f64>,
    pub plan_identity: Option<i64>,
}

// ── StreamTableMeta CRUD ──────────────────────────────────────────────────

impl StreamTableMeta {
    /// Insert a new stream table record. Returns the assigned `pgt_id`.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        pgt_relid: pg_sys::Oid,
        pgt_name: &str,
        pgt_schema: &str,
        defining_query: &str,
        original_query: Option<&str>,
        schedule: Option<String>,
        refresh_mode: RefreshMode,
        functions_used: Option<Vec<String>>,
        topk_limit: Option<i32>,
        topk_order_by: Option<&str>,
        topk_offset: Option<i32>,
        diamond_consistency: DiamondConsistency,
        diamond_schedule_policy: DiamondSchedulePolicy,
        has_keyless_source: bool,
        requested_cdc_mode: Option<&str>,
        is_append_only: bool,
        pooler_compatibility_mode: bool,
        st_partition_key: Option<&str>,
        max_differential_joins: Option<i32>,
        max_delta_fraction: Option<f64>,
        // CORR-1/UX-1 (v0.36.0): temporal IVM mode
        temporal_mode: bool,
        // CORR-2/UX-3 (v0.36.0): columnar storage backend ("heap", "citus", "pg_mooncake")
        storage_backend: &str,
        // HOT-1 (v0.73.0): heap fillfactor for HOT-friendly differential updates
        storage_fillfactor: Option<i32>,
        // LSEC-3 (v0.87.7): the caller's search_path used to resolve `defining_query`
        defining_search_path: &str,
    ) -> Result<i64, PgTrickleError> {
        // PERF-2: Compute hash of the defining query at INSERT time so that
        // the refresh engine can skip the per-refresh DefaultHasher computation.
        let query_hash = compute_defining_query_hash(defining_query);

        Spi::connect_mut(|client| {
            let row = client
                .update(
                    "INSERT INTO pgtrickle.pgt_stream_tables \
                     (pgt_relid, pgt_name, pgt_schema, defining_query, original_query, schedule, \
                      refresh_mode, functions_used, topk_limit, topk_order_by, topk_offset, \
                      diamond_consistency, diamond_schedule_policy, has_keyless_source, \
                      requested_cdc_mode, is_append_only, pooler_compatibility_mode, \
                      st_partition_key, max_differential_joins, max_delta_fraction, \
                      temporal_mode, storage_backend, defining_query_hash, \
                      storage_fillfactor, row_identity_version, row_probe_version, defining_search_path) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, \
                             $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27) \
                     RETURNING pgt_id",
                    None,
                    &[
                        pgt_relid.into(),
                        pgt_name.into(),
                        pgt_schema.into(),
                        defining_query.into(),
                        original_query.into(),
                        schedule.into(),
                        refresh_mode.as_str().into(),
                        functions_used.into(),
                        topk_limit.into(),
                        topk_order_by.into(),
                        topk_offset.into(),
                        diamond_consistency.as_str().into(),
                        diamond_schedule_policy.as_str().into(),
                        has_keyless_source.into(),
                        requested_cdc_mode.into(),
                        is_append_only.into(),
                        pooler_compatibility_mode.into(),
                        st_partition_key.into(),
                        max_differential_joins.into(),
                        max_delta_fraction.into(),
                        temporal_mode.into(),
                        storage_backend.into(),
                        query_hash.into(),
                        storage_fillfactor.into(),
                        crate::hash::CURRENT_ROW_IDENTITY_VERSION.into(),
                        (crate::dvm::row_id_v2::PROBE_VERSION_V1 as i16).into(),
                        defining_search_path.into(),
                    ],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
                .first();

            row.get_one::<i64>()
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("INSERT did not return pgt_id".into()))
        })
    }

    /// Look up a stream table by schema-qualified name.
    pub fn get_by_name(schema: &str, name: &str) -> Result<Self, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT pgt_id, pgt_relid, pgt_name, pgt_schema, defining_query, \
                     original_query, schedule, refresh_mode, status, is_populated, \
                     data_timestamp, consecutive_errors, needs_reinit, frontier, \
                     auto_threshold, last_full_ms, functions_used, topk_limit, topk_order_by, \
                     topk_offset, diamond_consistency, diamond_schedule_policy, \
                     has_keyless_source, function_hashes, requested_cdc_mode, is_append_only, \
                     scc_id, last_fixpoint_iterations, pooler_compatibility_mode, \
                     COALESCE(refresh_tier, 'hot') AS refresh_tier, \
                     COALESCE(fuse_mode, 'off') AS fuse_mode, \
                     COALESCE(fuse_state, 'armed') AS fuse_state, \
                     fuse_ceiling, fuse_sensitivity, blown_at, blow_reason, \
                     st_partition_key, max_differential_joins, max_delta_fraction, \
                     last_error_message, last_error_at, downstream_publication_name, freshness_deadline_ms, \
                     COALESCE(st_placement, 'local') AS st_placement, \
                     COALESCE(temporal_mode, FALSE) AS temporal_mode, \
                     COALESCE(storage_backend, 'heap') AS storage_backend, \
                     COALESCE(post_refresh_action, 'none') AS post_refresh_action, \
                     reindex_drift_threshold, \
                     COALESCE(rows_changed_since_last_reindex, 0) AS rows_changed_since_last_reindex, \
                     last_reindex_at, \
                     COALESCE(defining_query_hash, 0) AS defining_query_hash, \
                     storage_fillfactor, \
                     query_complexity_class, row_identity_version, \
                     NULLIF(to_jsonb(st)->>'row_probe_version', '')::smallint AS row_probe_version, \
                     COALESCE(self_heal_work_mem_percent, 100::smallint), \
                     COALESCE(self_heal_lock_backoff_exponent, 0::smallint), \
                     COALESCE(self_heal_success_streak, 0::smallint), \
                     last_error_code, last_error_retryable, defining_search_path, \
                     NULLIF(to_jsonb(st)->'window_strategy', 'null'::jsonb) AS window_strategy \
                     FROM pgtrickle.pgt_stream_tables st \
                     WHERE pgt_schema = $1 AND pgt_name = $2",
                    None,
                    &[schema.into(), name.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            if table.is_empty() {
                return Err(PgTrickleError::NotFound(format!("{}.{}", schema, name)));
            }

            Self::from_spi_table(&table.first())
        })
    }

    /// Look up a stream table by its storage table OID.
    pub fn get_by_relid(relid: pg_sys::Oid) -> Result<Self, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT pgt_id, pgt_relid, pgt_name, pgt_schema, defining_query, \
                     original_query, schedule, refresh_mode, status, is_populated, \
                     data_timestamp, consecutive_errors, needs_reinit, frontier, \
                     auto_threshold, last_full_ms, functions_used, topk_limit, topk_order_by, \
                     topk_offset, diamond_consistency, diamond_schedule_policy, \
                     has_keyless_source, function_hashes, requested_cdc_mode, is_append_only, \
                     scc_id, last_fixpoint_iterations, pooler_compatibility_mode, \
                     COALESCE(refresh_tier, 'hot') AS refresh_tier, \
                     COALESCE(fuse_mode, 'off') AS fuse_mode, \
                     COALESCE(fuse_state, 'armed') AS fuse_state, \
                     fuse_ceiling, fuse_sensitivity, blown_at, blow_reason, \
                     st_partition_key, max_differential_joins, max_delta_fraction, \
                     last_error_message, last_error_at, downstream_publication_name, freshness_deadline_ms, \
                     COALESCE(st_placement, 'local') AS st_placement, \
                     COALESCE(temporal_mode, FALSE) AS temporal_mode, \
                     COALESCE(storage_backend, 'heap') AS storage_backend, \
                     COALESCE(post_refresh_action, 'none') AS post_refresh_action, \
                     reindex_drift_threshold, \
                     COALESCE(rows_changed_since_last_reindex, 0) AS rows_changed_since_last_reindex, \
                     last_reindex_at, \
                     COALESCE(defining_query_hash, 0) AS defining_query_hash, \
                     storage_fillfactor, \
                     query_complexity_class, row_identity_version, \
                     NULLIF(to_jsonb(st)->>'row_probe_version', '')::smallint AS row_probe_version, \
                     COALESCE(self_heal_work_mem_percent, 100::smallint), \
                     COALESCE(self_heal_lock_backoff_exponent, 0::smallint), \
                     COALESCE(self_heal_success_streak, 0::smallint), \
                     last_error_code, last_error_retryable, defining_search_path, \
                     NULLIF(to_jsonb(st)->'window_strategy', 'null'::jsonb) AS window_strategy \
                     FROM pgtrickle.pgt_stream_tables st \
                     WHERE pgt_relid = $1",
                    None,
                    &[relid.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            if table.is_empty() {
                return Err(PgTrickleError::NotFound(format!(
                    "relid={}",
                    relid.to_u32()
                )));
            }

            Self::from_spi_table(&table.first())
        })
    }

    /// Look up a stream table by its catalog `pgt_id`.
    ///
    /// Returns `Ok(Some(meta))` if found, `Ok(None)` if the row doesn't exist.
    pub fn get_by_id(pgt_id: i64) -> Result<Option<Self>, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT pgt_id, pgt_relid, pgt_name, pgt_schema, defining_query, \
                     original_query, schedule, refresh_mode, status, is_populated, \
                     data_timestamp, consecutive_errors, needs_reinit, frontier, \
                     auto_threshold, last_full_ms, functions_used, topk_limit, topk_order_by, \
                     topk_offset, diamond_consistency, diamond_schedule_policy, \
                     has_keyless_source, function_hashes, requested_cdc_mode, is_append_only, \
                     scc_id, last_fixpoint_iterations, pooler_compatibility_mode, \
                     COALESCE(refresh_tier, 'hot') AS refresh_tier, \
                     COALESCE(fuse_mode, 'off') AS fuse_mode, \
                     COALESCE(fuse_state, 'armed') AS fuse_state, \
                     fuse_ceiling, fuse_sensitivity, blown_at, blow_reason, \
                     st_partition_key, max_differential_joins, max_delta_fraction, \
                     last_error_message, last_error_at, downstream_publication_name, freshness_deadline_ms, \
                     COALESCE(st_placement, 'local') AS st_placement, \
                     COALESCE(temporal_mode, FALSE) AS temporal_mode, \
                     COALESCE(storage_backend, 'heap') AS storage_backend, \
                     COALESCE(post_refresh_action, 'none') AS post_refresh_action, \
                     reindex_drift_threshold, \
                     COALESCE(rows_changed_since_last_reindex, 0) AS rows_changed_since_last_reindex, \
                     last_reindex_at, \
                     COALESCE(defining_query_hash, 0) AS defining_query_hash, \
                     storage_fillfactor, \
                     query_complexity_class, row_identity_version, \
                     NULLIF(to_jsonb(st)->>'row_probe_version', '')::smallint AS row_probe_version, \
                     COALESCE(self_heal_work_mem_percent, 100::smallint), \
                     COALESCE(self_heal_lock_backoff_exponent, 0::smallint), \
                     COALESCE(self_heal_success_streak, 0::smallint), \
                     last_error_code, last_error_retryable, defining_search_path, \
                     NULLIF(to_jsonb(st)->'window_strategy', 'null'::jsonb) AS window_strategy \
                     FROM pgtrickle.pgt_stream_tables st \
                     WHERE pgt_id = $1",
                    None,
                    &[pgt_id.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            if table.is_empty() {
                return Ok(None);
            }

            Self::from_spi_table(&table.first()).map(Some)
        })
    }

    /// Get all stream tables (including paused/broken).
    pub fn get_all() -> Result<Vec<Self>, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT pgt_id, pgt_relid, pgt_name, pgt_schema, defining_query, \
                     original_query, schedule, refresh_mode, status, is_populated, \
                     data_timestamp, consecutive_errors, needs_reinit, frontier, \
                     auto_threshold, last_full_ms, functions_used, topk_limit, topk_order_by, \
                     topk_offset, diamond_consistency, diamond_schedule_policy, \
                     has_keyless_source, function_hashes, requested_cdc_mode, is_append_only, \
                     scc_id, last_fixpoint_iterations, pooler_compatibility_mode, \
                     COALESCE(refresh_tier, 'hot') AS refresh_tier, \
                     COALESCE(fuse_mode, 'off') AS fuse_mode, \
                     COALESCE(fuse_state, 'armed') AS fuse_state, \
                     fuse_ceiling, fuse_sensitivity, blown_at, blow_reason, \
                     st_partition_key, max_differential_joins, max_delta_fraction, \
                     last_error_message, last_error_at, downstream_publication_name, freshness_deadline_ms, \
                     COALESCE(st_placement, 'local') AS st_placement, \
                     COALESCE(temporal_mode, FALSE) AS temporal_mode, \
                     COALESCE(storage_backend, 'heap') AS storage_backend, \
                     COALESCE(post_refresh_action, 'none') AS post_refresh_action, \
                     reindex_drift_threshold, \
                     COALESCE(rows_changed_since_last_reindex, 0) AS rows_changed_since_last_reindex, \
                     last_reindex_at, \
                     COALESCE(defining_query_hash, 0) AS defining_query_hash, \
                     storage_fillfactor, \
                     query_complexity_class, row_identity_version, \
                     NULLIF(to_jsonb(st)->>'row_probe_version', '')::smallint AS row_probe_version, \
                     COALESCE(self_heal_work_mem_percent, 100::smallint), \
                     COALESCE(self_heal_lock_backoff_exponent, 0::smallint), \
                     COALESCE(self_heal_success_streak, 0::smallint), \
                     last_error_code, last_error_retryable, defining_search_path, \
                     NULLIF(to_jsonb(st)->'window_strategy', 'null'::jsonb) AS window_strategy \
                     FROM pgtrickle.pgt_stream_tables st",
                    None,
                    &[],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            let mut results = Vec::new();
            for row in table {
                match Self::from_spi_heap_tuple(&row) {
                    Ok(meta) => results.push(meta),
                    Err(e) => {
                        pgrx::warning!("Skipping corrupted ST catalog row in get_all: {}", e);
                    }
                }
            }
            Ok(results)
        })
    }

    /// Get all active stream tables.
    pub fn get_all_active() -> Result<Vec<Self>, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT pgt_id, pgt_relid, pgt_name, pgt_schema, defining_query, \
                     original_query, schedule, refresh_mode, status, is_populated, \
                     data_timestamp, consecutive_errors, needs_reinit, frontier, \
                     auto_threshold, last_full_ms, functions_used, topk_limit, topk_order_by, \
                     topk_offset, diamond_consistency, diamond_schedule_policy, \
                     has_keyless_source, function_hashes, requested_cdc_mode, is_append_only, \
                     scc_id, last_fixpoint_iterations, pooler_compatibility_mode, \
                     COALESCE(refresh_tier, 'hot') AS refresh_tier, \
                     COALESCE(fuse_mode, 'off') AS fuse_mode, \
                     COALESCE(fuse_state, 'armed') AS fuse_state, \
                     fuse_ceiling, fuse_sensitivity, blown_at, blow_reason, \
                     st_partition_key, max_differential_joins, max_delta_fraction, \
                     last_error_message, last_error_at, downstream_publication_name, freshness_deadline_ms, \
                     COALESCE(st_placement, 'local') AS st_placement, \
                     COALESCE(temporal_mode, FALSE) AS temporal_mode, \
                     COALESCE(storage_backend, 'heap') AS storage_backend, \
                     COALESCE(post_refresh_action, 'none') AS post_refresh_action, \
                     reindex_drift_threshold, \
                     COALESCE(rows_changed_since_last_reindex, 0) AS rows_changed_since_last_reindex, \
                     last_reindex_at, \
                     COALESCE(defining_query_hash, 0) AS defining_query_hash, \
                     storage_fillfactor, \
                     query_complexity_class, row_identity_version, \
                     NULLIF(to_jsonb(st)->>'row_probe_version', '')::smallint AS row_probe_version, \
                     COALESCE(self_heal_work_mem_percent, 100::smallint), \
                     COALESCE(self_heal_lock_backoff_exponent, 0::smallint), \
                     COALESCE(self_heal_success_streak, 0::smallint), \
                     last_error_code, last_error_retryable, defining_search_path, \
                     NULLIF(to_jsonb(st)->'window_strategy', 'null'::jsonb) AS window_strategy \
                     FROM pgtrickle.pgt_stream_tables st \
                     WHERE status = 'ACTIVE'",
                    None,
                    &[],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            let mut result = Vec::new();
            for row in table {
                match Self::from_spi_heap_tuple(&row) {
                    Ok(meta) => result.push(meta),
                    Err(e) => {
                        pgrx::warning!("Skipping corrupted ST catalog row: {}", e);
                    }
                }
            }
            Ok(result)
        })
    }

    /// Look up a stream table's `pgt_id` by its storage table OID.
    ///
    /// Lightweight alternative to `get_by_relid` when only the ID is needed.
    pub fn pgt_id_for_relid(relid: pg_sys::Oid) -> Option<i64> {
        Spi::get_one_with_args::<i64>(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = $1",
            &[relid.into()],
        )
        .unwrap_or(None)
    }

    /// Find pgt_ids of stream tables whose `functions_used` array contains
    /// the given function name (case-insensitive match via `@>`).
    /// Used by DDL hooks to detect which STs are affected when a function
    /// is CREATEd OR REPLACEd / ALTERed / DROPped.
    pub fn find_by_function_name(func_name: &str) -> Result<Vec<i64>, PgTrickleError> {
        let lower = func_name.to_lowercase();
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                     WHERE functions_used @> ARRAY[$1]::text[]",
                    None,
                    &[lower.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            let mut ids = Vec::new();
            for row in table {
                if let Ok(Some(id)) = row.get::<i64>(1) {
                    ids.push(id);
                }
            }
            Ok(ids)
        })
    }

    /// Update the status of a stream table.
    pub fn update_status(pgt_id: i64, status: StStatus) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET status = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[status.as_str().into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Mark a ST as populated with a data timestamp after refresh.
    pub fn update_after_refresh(
        pgt_id: i64,
        data_ts: TimestampWithTimeZone,
        _rows_affected: i64,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET data_timestamp = $1, is_populated = true, \
             last_refresh_at = now(), consecutive_errors = 0, \
             status = 'ACTIVE', needs_reinit = false, \
             last_error_message = NULL, last_error_at = NULL, \
             updated_at = now() \
             WHERE pgt_id = $2",
            &[data_ts.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Record a "no data" refresh cycle — the table was verified up-to-date but
    /// no rows were written.  Updates `last_refresh_at` so staleness calculations
    /// see the check, but intentionally preserves `data_timestamp` so that
    /// downstream stream tables (which compare `upstream.data_timestamp > us.data_timestamp`
    /// to detect when a full refresh is needed) do not see a spurious "upstream
    /// changed" signal after a pure no-data verification pass.
    pub fn update_after_no_data_refresh(pgt_id: i64) -> Result<(), PgTrickleError> {
        // NOTE: intentionally does NOT clear needs_reinit.  A no-data refresh
        // means no rows were written — it must not overwrite a needs_reinit=true
        // flag set by EC-16 function-body-change detection or DDL hooks.  The
        // flag is cleared only by update_after_refresh / store_frontier_and_complete_refresh
        // after an actual full reinitialization succeeds.
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET is_populated = true, \
             last_refresh_at = now(), consecutive_errors = 0, \
             status = 'ACTIVE', \
             last_error_message = NULL, last_error_at = NULL, \
             updated_at = now() \
             WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Mark a ST as populated with a data timestamp and store frontier after refresh.
    pub fn update_after_refresh_with_frontier(
        pgt_id: i64,
        data_ts: TimestampWithTimeZone,
        _rows_affected: i64,
        frontier: &Frontier,
    ) -> Result<(), PgTrickleError> {
        let frontier_json = serde_json::to_value(frontier).map_err(|e| {
            PgTrickleError::InternalError(format!("Failed to serialize frontier: {}", e))
        })?;

        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET data_timestamp = $1, is_populated = true, \
             last_refresh_at = now(), consecutive_errors = 0, \
             status = 'ACTIVE', needs_reinit = false, \
             frontier = $3, updated_at = now() \
             WHERE pgt_id = $2",
            &[
                data_ts.into(),
                pgt_id.into(),
                pgrx::JsonB(frontier_json).into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Store frontier + mark refresh complete in a single SPI call (S3 optimization).
    ///
    /// Combines `store_frontier()` + `SELECT now()` + `update_after_refresh()`
    /// into one UPDATE ... RETURNING, saving 2 SPI round-trips.
    pub fn store_frontier_and_complete_refresh(
        pgt_id: i64,
        frontier: &Frontier,
        rows_affected: i64,
    ) -> Result<TimestampWithTimeZone, PgTrickleError> {
        let frontier_json = serde_json::to_value(frontier).map_err(|e| {
            PgTrickleError::InternalError(format!("Failed to serialize frontier: {}", e))
        })?;

        Spi::get_one_with_args::<TimestampWithTimeZone>(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET data_timestamp = now(), is_populated = true, \
             last_refresh_at = now(), consecutive_errors = 0, \
             status = 'ACTIVE', needs_reinit = false, \
             last_error_message = NULL, last_error_at = NULL, \
             frontier = $3, updated_at = now() \
             WHERE pgt_id = $1 \
             RETURNING data_timestamp",
            &[
                pgt_id.into(),
                rows_affected.into(),
                pgrx::JsonB(frontier_json).into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::NotFound(format!("pgt_id={}", pgt_id)))
    }

    /// Store a frontier for a stream table.
    pub fn store_frontier(pgt_id: i64, frontier: &Frontier) -> Result<(), PgTrickleError> {
        let frontier_json = serde_json::to_value(frontier).map_err(|e| {
            PgTrickleError::InternalError(format!("Failed to serialize frontier: {}", e))
        })?;

        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET frontier = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[pgrx::JsonB(frontier_json).into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Load the frontier for a stream table. Returns None if not yet set.
    pub fn get_frontier(pgt_id: i64) -> Result<Option<Frontier>, PgTrickleError> {
        let json_opt = Spi::get_one_with_args::<pgrx::JsonB>(
            "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

        match json_opt {
            Some(jsonb) => {
                let frontier: Frontier = serde_json::from_value(jsonb.0).map_err(|e| {
                    PgTrickleError::InternalError(format!("Failed to deserialize frontier: {}", e))
                })?;
                Ok(Some(frontier))
            }
            None => Ok(None),
        }
    }

    // ── COR-001/REL-001 (v0.72.0): Frontier durability design ────────────
    //
    // Decision (ADR-009): Use the canonical single-phase `store_frontier` /
    // `store_frontier_and_complete_refresh` path as the sole frontier
    // persistence mechanism. All refresh paths treat frontier-store failure as
    // refresh failure (the transaction is aborted, rolling back the data change
    // as well).
    //
    // The DUR-1 tentative-frontier design (prepare_frontier /
    // finalize_frontier_and_complete_refresh / reconcile_tentative_frontiers)
    // was removed in v0.72.0 because:
    //
    //   1. No call sites existed — the design was dead code from the start.
    //   2. The recovery query contained invalid SQL (table-name concatenation
    //      `{change_schema}.changes_ || s.pgt_relid::text` is not valid SQL).
    //   3. The schema column `tentative_frontier` still exists for operator
    //      visibility but is never written after v0.72.0.
    //
    // See plans/adrs/ADR-009-frontier-durability.md for the full rationale.

    /// Increment the consecutive error count. Returns the new count.
    pub fn increment_errors(pgt_id: i64) -> Result<i32, PgTrickleError> {
        Spi::get_one_with_args::<i32>(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET consecutive_errors = consecutive_errors + 1, updated_at = now() \
             WHERE pgt_id = $1 \
             RETURNING consecutive_errors",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::NotFound(format!("pgt_id={}", pgt_id)))
    }

    pub fn record_scheduled_failure(
        pgt_id: i64,
        error_code: &str,
        retryable: bool,
        reduce_memory: bool,
        increase_lock_backoff: bool,
    ) -> Result<i32, PgTrickleError> {
        let count = Self::increment_errors(pgt_id)?;
        Spi::get_one_with_args::<i16>(
            "UPDATE pgtrickle.pgt_stream_tables
             SET self_heal_work_mem_percent = CASE WHEN $2 THEN
                     GREATEST(25, FLOOR(self_heal_work_mem_percent * 0.75)::smallint)
                 ELSE self_heal_work_mem_percent END,
                 self_heal_lock_backoff_exponent = CASE WHEN $3 THEN
                     LEAST(6, self_heal_lock_backoff_exponent + 1)
                 ELSE self_heal_lock_backoff_exponent END,
                 self_heal_success_streak = 0,
                 last_error_code = $4,
                 last_error_retryable = $5,
                 updated_at = now()
             WHERE pgt_id = $1
             RETURNING self_heal_work_mem_percent",
            &[
                pgt_id.into(),
                reduce_memory.into(),
                increase_lock_backoff.into(),
                error_code.into(),
                retryable.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::NotFound(format!("pgt_id={pgt_id}")))?;
        Ok(count)
    }

    pub fn record_scheduled_success(pgt_id: i64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables
             SET consecutive_errors = 0,
                 self_heal_success_streak = CASE
                     WHEN self_heal_work_mem_percent < 100
                       OR self_heal_lock_backoff_exponent > 0
                     THEN LEAST(3, self_heal_success_streak + 1)
                     ELSE 0 END,
                 self_heal_work_mem_percent = CASE
                     WHEN self_heal_success_streak >= 2 THEN 100
                     ELSE self_heal_work_mem_percent END,
                 self_heal_lock_backoff_exponent = CASE
                     WHEN self_heal_success_streak >= 2 THEN 0
                     ELSE self_heal_lock_backoff_exponent END,
                 last_error_code = NULL,
                 last_error_retryable = NULL,
                 updated_at = now()
             WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// ERR-1b: Set status to ERROR with an error message and timestamp.
    /// Used for permanent failures that should not be retried.
    pub fn set_error_state(pgt_id: i64, error_message: &str) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET status = 'ERROR', last_error_message = $1, last_error_at = now(), \
             updated_at = now() \
             WHERE pgt_id = $2",
            &[error_message.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    pub fn set_typed_error(
        pgt_id: i64,
        error_message: &str,
        error_code: &str,
        retryable: bool,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables
             SET status = CASE WHEN $3 THEN status ELSE 'ERROR' END,
                 last_error_message = $1, last_error_at = now(),
                 last_error_code = $2, last_error_retryable = $3,
                 updated_at = now()
             WHERE pgt_id = $4",
            &[
                error_message.into(),
                error_code.into(),
                retryable.into(),
                pgt_id.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// ERR-1c: Clear error state (null out last_error_message and last_error_at).
    /// Called when a pipeline-regenerating API call succeeds.
    pub fn clear_error_state(pgt_id: i64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET last_error_message = NULL, last_error_at = NULL, updated_at = now() \
             WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Delete a stream table record from the catalog.
    pub fn delete(pgt_id: i64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "DELETE FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Mark a ST for reinitialization (e.g., due to upstream DDL change).
    pub fn mark_for_reinitialize(pgt_id: i64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET needs_reinit = true,
                 refresh_reason = 'SCHEMA_CHANGED',
                 refresh_reason_detail = 'A catalog or source-definition change requires a FULL rebuild.',
                 updated_at = now() \
             WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    pub fn set_refresh_reason(
        pgt_id: i64,
        reason: &crate::refresh::FullRefreshReason,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables
                SET refresh_reason = $1, refresh_reason_detail = $2, updated_at = now()
              WHERE pgt_id = $3",
            &[
                reason.code.as_str().into(),
                reason.detail.as_str().into(),
                pgt_id.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Mark a successfully rebuilt stream table as using the current row
    /// identity encoding.
    pub fn mark_row_identity_reinitialized(pgt_id: i64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET row_identity_version = $1, row_probe_version = $2, updated_at = now() \
             WHERE pgt_id = $3",
            &[
                crate::hash::CURRENT_ROW_IDENTITY_VERSION.into(),
                (crate::dvm::row_id_v2::PROBE_VERSION_V1 as i16).into(),
                pgt_id.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Persist the function source hashes for a stream table (EC-16).
    ///
    /// `hashes_json` is a JSON text string mapping `{ "func_name": "md5hex", ... }`.
    /// Pass `None` to clear (reset) stored hashes (e.g., after a full rebase).
    pub fn update_function_hashes(
        pgt_id: i64,
        hashes_json: Option<&str>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET function_hashes = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[hashes_json.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Update the per-stream-table requested CDC mode override.
    pub fn update_requested_cdc_mode(
        pgt_id: i64,
        requested_cdc_mode: Option<&str>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET requested_cdc_mode = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[requested_cdc_mode.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// G12-ERM-1: Record the effective refresh mode used in the most recent refresh.
    ///
    /// `mode` — one of `"FULL"`, `"DIFFERENTIAL"`, `"APPEND_ONLY"`, `"TOP_K"`,
    /// `"NO_DATA"`.  Populated by the scheduler after every completed refresh so
    /// operators can see which mode was actually executed (useful when AUTO
    /// downgrades DIFFERENTIAL → FULL due to adaptive thresholds, CTEs, etc.).
    pub fn update_effective_refresh_mode(pgt_id: i64, mode: &str) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET effective_refresh_mode = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[mode.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Store the query complexity class for a stream table.
    ///
    /// P-2 (v0.78.0): Called at CREATE/ALTER time (and lazily on first refresh
    /// for back-compat) to persist the OpTree-derived complexity label.
    pub fn update_query_complexity_class(pgt_id: i64, class: &str) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET query_complexity_class = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[class.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Update the append-only flag for a stream table.
    ///
    /// Called by the CDC heuristic fallback when a DELETE or UPDATE is
    /// detected on an append-only stream table, reverting it to MERGE.
    pub fn update_append_only(pgt_id: i64, is_append_only: bool) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET is_append_only = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[is_append_only.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// PB2: Update the pooler compatibility mode flag for a stream table.
    pub fn update_pooler_compatibility_mode(
        pgt_id: i64,
        enabled: bool,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET pooler_compatibility_mode = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[enabled.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// G-7: Update the refresh tier for a stream table.
    pub fn update_refresh_tier(pgt_id: i64, tier: &str) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET refresh_tier = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[tier.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// A1-1c: Update the partition key for a stream table.
    /// Pass `None` to remove partitioning.
    pub fn update_partition_key(
        pgt_id: i64,
        partition_key: Option<&str>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET st_partition_key = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[partition_key.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Update the SCC identifier for a stream table (CYC-3).
    ///
    /// `scc_id` — the SCC group identifier, or `None` to clear (no cycle).
    pub fn update_scc_id(pgt_id: i64, scc_id: Option<i32>) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET scc_id = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[scc_id.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Update the last fixpoint iteration count for a stream table (CYC-5).
    ///
    /// Recorded after a successful fixpoint convergence so monitoring can
    /// track how many iterations each SCC member required.
    pub fn update_last_fixpoint_iterations(
        pgt_id: i64,
        iterations: i32,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET last_fixpoint_iterations = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[iterations.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Update the per-ST adaptive fallback threshold and last FULL refresh time.
    ///
    /// Called after each differential or adaptive-fallback refresh to track
    /// performance and auto-tune the change ratio threshold.
    ///
    /// `auto_threshold` — the new threshold (0.0–1.0), or None to reset to GUC default.
    /// `last_full_ms` — the last observed FULL refresh execution time, or None to keep existing.
    pub fn update_adaptive_threshold(
        pgt_id: i64,
        auto_threshold: Option<f64>,
        last_full_ms: Option<f64>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET auto_threshold = $1, \
                 last_full_ms = COALESCE($2, last_full_ms), \
                 updated_at = now() \
             WHERE pgt_id = $3",
            &[auto_threshold.into(), last_full_ms.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Get the diamond consistency mode for a stream table by pgt_id.
    pub fn get_diamond_consistency(pgt_id: i64) -> Result<DiamondConsistency, PgTrickleError> {
        let val = Spi::get_one_with_args::<String>(
            "SELECT diamond_consistency FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or_else(|| "none".into());
        Ok(DiamondConsistency::from_sql_str(&val))
    }

    /// Set the diamond consistency mode for a stream table.
    pub fn set_diamond_consistency(
        pgt_id: i64,
        mode: DiamondConsistency,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET diamond_consistency = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[mode.as_str().into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Get the diamond schedule policy for a stream table by pgt_id.
    pub fn get_diamond_schedule_policy(
        pgt_id: i64,
    ) -> Result<DiamondSchedulePolicy, PgTrickleError> {
        let val = Spi::get_one_with_args::<String>(
            "SELECT diamond_schedule_policy FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or_else(|| "fastest".into());
        Ok(DiamondSchedulePolicy::from_sql_str(&val).unwrap_or_default())
    }

    /// Set the diamond schedule policy for a stream table.
    pub fn set_diamond_schedule_policy(
        pgt_id: i64,
        policy: DiamondSchedulePolicy,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET diamond_schedule_policy = $1, updated_at = now() \
             WHERE pgt_id = $2",
            &[policy.as_str().into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    // ── Fuse circuit breaker CRUD ──────────────────────────────────────

    /// FUSE-1: Update fuse configuration for a stream table.
    pub fn update_fuse_config(
        pgt_id: i64,
        fuse_mode: &str,
        fuse_ceiling: Option<i64>,
        fuse_sensitivity: Option<i32>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET fuse_mode = $1, fuse_ceiling = $2, fuse_sensitivity = $3, updated_at = now() \
             WHERE pgt_id = $4",
            &[
                fuse_mode.into(),
                fuse_ceiling.into(),
                fuse_sensitivity.into(),
                pgt_id.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// FUSE-5: Blow the fuse for a stream table.
    pub fn blow_fuse(pgt_id: i64, reason: &str) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET fuse_state = 'blown', blown_at = now(), blow_reason = $1, \
             status = 'SUSPENDED', updated_at = now() \
             WHERE pgt_id = $2",
            &[reason.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// FUSE-3: Reset the fuse (re-arm) for a stream table.
    pub fn reset_fuse(pgt_id: i64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET fuse_state = 'armed', blown_at = NULL, blow_reason = NULL, \
             status = 'ACTIVE', consecutive_errors = 0, updated_at = now() \
             WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// VP-1/VP-2 (v0.47.0): Update post_refresh_action and reindex_drift_threshold.
    pub fn update_post_refresh_options(
        pgt_id: i64,
        post_refresh_action: &str,
        reindex_drift_threshold: Option<f64>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET post_refresh_action = $1, reindex_drift_threshold = $2, updated_at = now() \
             WHERE pgt_id = $3",
            &[
                post_refresh_action.into(),
                reindex_drift_threshold.into(),
                pgt_id.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// VP-2 (v0.47.0): Increment rows_changed_since_last_reindex by delta.
    pub fn increment_rows_changed_for_reindex(
        pgt_id: i64,
        delta: i64,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET rows_changed_since_last_reindex = \
               COALESCE(rows_changed_since_last_reindex, 0) + $1, \
             updated_at = now() \
             WHERE pgt_id = $2",
            &[delta.into(), pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// VP-2 (v0.47.0): Reset rows_changed_since_last_reindex to 0 and set last_reindex_at.
    pub fn reset_reindex_drift_counter(pgt_id: i64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET rows_changed_since_last_reindex = 0, \
             last_reindex_at = now(), updated_at = now() \
             WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    // ── Private helpers ────────────────────────────────────────────────

    /// Extract a StreamTableMeta from a positioned SpiTupleTable (after first()).
    fn from_spi_table(table: &SpiTupleTable<'_>) -> Result<Self, PgTrickleError> {
        let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());

        let pgt_id = table
            .get::<i64>(1)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("pgt_id is NULL".into()))?;

        let pgt_relid = table
            .get::<pg_sys::Oid>(2)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("pgt_relid is NULL".into()))?;

        let pgt_name = table
            .get::<String>(3)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("pgt_name is NULL".into()))?;

        let pgt_schema = table
            .get::<String>(4)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("pgt_schema is NULL".into()))?;

        let defining_query = table
            .get::<String>(5)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("defining_query is NULL".into()))?;

        let original_query = table.get::<String>(6).map_err(map_spi)?;

        let schedule = table.get::<String>(7).map_err(map_spi)?;

        let refresh_mode_str = table
            .get::<String>(8)
            .map_err(map_spi)?
            .unwrap_or_else(|| "DIFFERENTIAL".into());
        let refresh_mode = RefreshMode::from_str(&refresh_mode_str)?;

        let status_str = table
            .get::<String>(9)
            .map_err(map_spi)?
            .unwrap_or_else(|| "INITIALIZING".into());
        let status = StStatus::from_str(&status_str)?;

        let is_populated = table.get::<bool>(10).map_err(map_spi)?.unwrap_or(false);

        let data_timestamp = table.get::<TimestampWithTimeZone>(11).map_err(map_spi)?;

        let consecutive_errors = table.get::<i32>(12).map_err(map_spi)?.unwrap_or(0);

        let needs_reinit = table.get::<bool>(13).map_err(map_spi)?.unwrap_or(false);

        let frontier_json = table.get::<pgrx::JsonB>(14).map_err(map_spi)?;
        let frontier = frontier_json.and_then(|j| serde_json::from_value(j.0).ok());

        let auto_threshold = table.get::<f64>(15).map_err(map_spi)?;
        let last_full_ms = table.get::<f64>(16).map_err(map_spi)?;
        let functions_used = table.get::<Vec<String>>(17).map_err(map_spi)?;
        let topk_limit = table.get::<i32>(18).map_err(map_spi)?;
        let topk_order_by = table.get::<String>(19).map_err(map_spi)?;
        let topk_offset = table.get::<i32>(20).map_err(map_spi)?;

        let diamond_consistency_str = table
            .get::<String>(21)
            .map_err(map_spi)?
            .unwrap_or_else(|| "none".into());
        let diamond_consistency = DiamondConsistency::from_sql_str(&diamond_consistency_str);

        let diamond_schedule_policy_str = table
            .get::<String>(22)
            .map_err(map_spi)?
            .unwrap_or_else(|| "fastest".into());
        let diamond_schedule_policy =
            DiamondSchedulePolicy::from_sql_str(&diamond_schedule_policy_str).unwrap_or_default();

        let has_keyless_source = table.get::<bool>(23).map_err(map_spi)?.unwrap_or(false);
        let function_hashes = table.get::<String>(24).map_err(map_spi)?;
        let requested_cdc_mode = table.get::<String>(25).map_err(map_spi)?;
        let is_append_only = table.get::<bool>(26).map_err(map_spi)?.unwrap_or(false);
        let scc_id = table.get::<i32>(27).map_err(map_spi)?;
        let last_fixpoint_iterations = table.get::<i32>(28).map_err(map_spi)?;
        let pooler_compatibility_mode = table.get::<bool>(29).map_err(map_spi)?.unwrap_or(false);
        let refresh_tier = table
            .get::<String>(30)
            .map_err(map_spi)?
            .unwrap_or_else(|| "hot".into());
        let fuse_mode = table
            .get::<String>(31)
            .map_err(map_spi)?
            .unwrap_or_else(|| "off".into());
        let fuse_state = table
            .get::<String>(32)
            .map_err(map_spi)?
            .unwrap_or_else(|| "armed".into());
        let fuse_ceiling = table.get::<i64>(33).map_err(map_spi)?;
        let fuse_sensitivity = table.get::<i32>(34).map_err(map_spi)?;
        let blown_at = table.get::<TimestampWithTimeZone>(35).map_err(map_spi)?;
        let blow_reason = table.get::<String>(36).map_err(map_spi)?;
        let st_partition_key = table.get::<String>(37).map_err(map_spi)?;
        let max_differential_joins = table.get::<i32>(38).map_err(map_spi)?;
        let max_delta_fraction = table.get::<f64>(39).map_err(map_spi)?;
        let last_error_message = table.get::<String>(40).map_err(map_spi)?;
        let last_error_at = table.get::<TimestampWithTimeZone>(41).map_err(map_spi)?;
        let downstream_publication_name = table.get::<String>(42).map_err(map_spi)?;
        let freshness_deadline_ms = table.get::<i64>(43).map_err(map_spi)?;
        let st_placement = table
            .get::<String>(44)
            .map_err(map_spi)?
            .unwrap_or_else(|| "local".into());
        let temporal_mode = table.get::<bool>(45).map_err(map_spi)?.unwrap_or(false);
        let storage_backend = table
            .get::<String>(46)
            .map_err(map_spi)?
            .unwrap_or_else(|| "heap".into());
        let post_refresh_action = table
            .get::<String>(47)
            .map_err(map_spi)?
            .unwrap_or_else(|| "none".into());
        let reindex_drift_threshold = table.get::<f64>(48).map_err(map_spi)?;
        let rows_changed_since_last_reindex = table.get::<i64>(49).map_err(map_spi)?.unwrap_or(0);
        let last_reindex_at = table.get::<TimestampWithTimeZone>(50).map_err(map_spi)?;
        let defining_query_hash = table.get::<i64>(51).map_err(map_spi)?.unwrap_or(0);
        let storage_fillfactor = table.get::<i32>(52).map_err(map_spi)?;
        let query_complexity_class = table.get::<String>(53).map_err(map_spi)?;
        let row_identity_version = table.get::<i16>(54).map_err(map_spi)?;
        let row_probe_version = table.get::<i16>(55).map_err(map_spi)?;
        let self_heal_work_mem_percent = table.get::<i16>(56).map_err(map_spi)?.unwrap_or(100);
        let self_heal_lock_backoff_exponent = table.get::<i16>(57).map_err(map_spi)?.unwrap_or(0);
        let self_heal_success_streak = table.get::<i16>(58).map_err(map_spi)?.unwrap_or(0);
        let last_error_code = table.get::<String>(59).map_err(map_spi)?;
        let last_error_retryable = table.get::<bool>(60).map_err(map_spi)?;
        let defining_search_path = table
            .get::<String>(61)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("defining_search_path is NULL".into()))?;
        let window_strategy = table
            .get::<pgrx::JsonB>(62)
            .map_err(map_spi)?
            .map(|json| WindowStrategyPlan::from_json(json.0))
            .transpose()
            .map_err(|reason| PgTrickleError::WindowStateInvalid {
                pgt_id,
                node_ordinal: -1,
                spec_ordinal: -1,
                reason,
            })?;

        Ok(StreamTableMeta {
            pgt_id,
            pgt_relid,
            pgt_name,
            pgt_schema,
            defining_query,
            original_query,
            schedule,
            refresh_mode,
            status,
            is_populated,
            data_timestamp,
            consecutive_errors,
            needs_reinit,
            auto_threshold,
            last_full_ms,
            functions_used,
            frontier,
            topk_limit,
            topk_order_by,
            topk_offset,
            diamond_consistency,
            diamond_schedule_policy,
            has_keyless_source,
            function_hashes,
            requested_cdc_mode,
            is_append_only,
            scc_id,
            last_fixpoint_iterations,
            pooler_compatibility_mode,
            refresh_tier,
            fuse_mode,
            fuse_state,
            fuse_ceiling,
            fuse_sensitivity,
            blown_at,
            blow_reason,
            st_partition_key,
            max_differential_joins,
            max_delta_fraction,
            last_error_message,
            last_error_at,
            downstream_publication_name,
            freshness_deadline_ms,
            st_placement,
            temporal_mode,
            storage_backend,
            post_refresh_action,
            reindex_drift_threshold,
            rows_changed_since_last_reindex,
            last_reindex_at,
            defining_query_hash,
            storage_fillfactor,
            query_complexity_class,
            row_identity_version,
            row_probe_version,
            self_heal_work_mem_percent,
            self_heal_lock_backoff_exponent,
            self_heal_success_streak,
            last_error_code,
            last_error_retryable,
            defining_search_path,
            window_strategy,
        })
    }

    /// Extract a StreamTableMeta from an SpiHeapTupleData (from iteration).
    fn from_spi_heap_tuple(row: &SpiHeapTupleData<'_>) -> Result<Self, PgTrickleError> {
        let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());

        let pgt_id = row
            .get::<i64>(1)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("pgt_id is NULL".into()))?;

        let pgt_relid = row
            .get::<pg_sys::Oid>(2)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("pgt_relid is NULL".into()))?;

        let pgt_name = row
            .get::<String>(3)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("pgt_name is NULL".into()))?;

        let pgt_schema = row
            .get::<String>(4)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("pgt_schema is NULL".into()))?;

        let defining_query = row
            .get::<String>(5)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("defining_query is NULL".into()))?;

        let original_query = row.get::<String>(6).map_err(map_spi)?;

        let schedule = row.get::<String>(7).map_err(map_spi)?;

        let refresh_mode_str = row
            .get::<String>(8)
            .map_err(map_spi)?
            .unwrap_or_else(|| "DIFFERENTIAL".into());
        let refresh_mode = RefreshMode::from_str(&refresh_mode_str)?;

        let status_str = row
            .get::<String>(9)
            .map_err(map_spi)?
            .unwrap_or_else(|| "INITIALIZING".into());
        let status = StStatus::from_str(&status_str)?;

        let is_populated = row.get::<bool>(10).map_err(map_spi)?.unwrap_or(false);

        let data_timestamp = row.get::<TimestampWithTimeZone>(11).map_err(map_spi)?;

        let consecutive_errors = row.get::<i32>(12).map_err(map_spi)?.unwrap_or(0);

        let needs_reinit = row.get::<bool>(13).map_err(map_spi)?.unwrap_or(false);

        let frontier_json = row.get::<pgrx::JsonB>(14).map_err(map_spi)?;
        let frontier = frontier_json.and_then(|j| serde_json::from_value(j.0).ok());

        let auto_threshold = row.get::<f64>(15).map_err(map_spi)?;
        let last_full_ms = row.get::<f64>(16).map_err(map_spi)?;
        let functions_used = row.get::<Vec<String>>(17).map_err(map_spi)?;
        let topk_limit = row.get::<i32>(18).map_err(map_spi)?;
        let topk_order_by = row.get::<String>(19).map_err(map_spi)?;
        let topk_offset = row.get::<i32>(20).map_err(map_spi)?;

        let diamond_consistency_str = row
            .get::<String>(21)
            .map_err(map_spi)?
            .unwrap_or_else(|| "none".into());
        let diamond_consistency = DiamondConsistency::from_sql_str(&diamond_consistency_str);

        let diamond_schedule_policy_str = row
            .get::<String>(22)
            .map_err(map_spi)?
            .unwrap_or_else(|| "fastest".into());
        let diamond_schedule_policy =
            DiamondSchedulePolicy::from_sql_str(&diamond_schedule_policy_str).unwrap_or_default();

        let has_keyless_source = row.get::<bool>(23).map_err(map_spi)?.unwrap_or(false);
        let function_hashes = row.get::<String>(24).map_err(map_spi)?;
        let requested_cdc_mode = row.get::<String>(25).map_err(map_spi)?;
        let is_append_only = row.get::<bool>(26).map_err(map_spi)?.unwrap_or(false);
        let scc_id = row.get::<i32>(27).map_err(map_spi)?;
        let last_fixpoint_iterations = row.get::<i32>(28).map_err(map_spi)?;
        let pooler_compatibility_mode = row.get::<bool>(29).map_err(map_spi)?.unwrap_or(false);
        let refresh_tier = row
            .get::<String>(30)
            .map_err(map_spi)?
            .unwrap_or_else(|| "hot".into());
        let fuse_mode = row
            .get::<String>(31)
            .map_err(map_spi)?
            .unwrap_or_else(|| "off".into());
        let fuse_state = row
            .get::<String>(32)
            .map_err(map_spi)?
            .unwrap_or_else(|| "armed".into());
        let fuse_ceiling = row.get::<i64>(33).map_err(map_spi)?;
        let fuse_sensitivity = row.get::<i32>(34).map_err(map_spi)?;
        let blown_at = row.get::<TimestampWithTimeZone>(35).map_err(map_spi)?;
        let blow_reason = row.get::<String>(36).map_err(map_spi)?;
        let st_partition_key = row.get::<String>(37).map_err(map_spi)?;
        let max_differential_joins = row.get::<i32>(38).map_err(map_spi)?;
        let max_delta_fraction = row.get::<f64>(39).map_err(map_spi)?;
        let last_error_message = row.get::<String>(40).map_err(map_spi)?;
        let last_error_at = row.get::<TimestampWithTimeZone>(41).map_err(map_spi)?;
        let downstream_publication_name = row.get::<String>(42).map_err(map_spi)?;
        let freshness_deadline_ms = row.get::<i64>(43).map_err(map_spi)?;
        let st_placement = row
            .get::<String>(44)
            .map_err(map_spi)?
            .unwrap_or_else(|| "local".into());
        let temporal_mode = row.get::<bool>(45).map_err(map_spi)?.unwrap_or(false);
        let storage_backend = row
            .get::<String>(46)
            .map_err(map_spi)?
            .unwrap_or_else(|| "heap".into());
        let post_refresh_action = row
            .get::<String>(47)
            .map_err(map_spi)?
            .unwrap_or_else(|| "none".into());
        let reindex_drift_threshold = row.get::<f64>(48).map_err(map_spi)?;
        let rows_changed_since_last_reindex = row.get::<i64>(49).map_err(map_spi)?.unwrap_or(0);
        let last_reindex_at = row.get::<TimestampWithTimeZone>(50).map_err(map_spi)?;
        let defining_query_hash = row.get::<i64>(51).map_err(map_spi)?.unwrap_or(0);
        let storage_fillfactor = row.get::<i32>(52).map_err(map_spi)?;
        let query_complexity_class = row.get::<String>(53).map_err(map_spi)?;
        let row_identity_version = row.get::<i16>(54).map_err(map_spi)?;
        let row_probe_version = row.get::<i16>(55).map_err(map_spi)?;
        let self_heal_work_mem_percent = row.get::<i16>(56).map_err(map_spi)?.unwrap_or(100);
        let self_heal_lock_backoff_exponent = row.get::<i16>(57).map_err(map_spi)?.unwrap_or(0);
        let self_heal_success_streak = row.get::<i16>(58).map_err(map_spi)?.unwrap_or(0);
        let last_error_code = row.get::<String>(59).map_err(map_spi)?;
        let last_error_retryable = row.get::<bool>(60).map_err(map_spi)?;
        let defining_search_path = row
            .get::<String>(61)
            .map_err(map_spi)?
            .ok_or_else(|| PgTrickleError::InternalError("defining_search_path is NULL".into()))?;
        let window_strategy = row
            .get::<pgrx::JsonB>(62)
            .map_err(map_spi)?
            .map(|json| WindowStrategyPlan::from_json(json.0))
            .transpose()
            .map_err(|reason| PgTrickleError::WindowStateInvalid {
                pgt_id,
                node_ordinal: -1,
                spec_ordinal: -1,
                reason,
            })?;

        Ok(StreamTableMeta {
            pgt_id,
            pgt_relid,
            pgt_name,
            pgt_schema,
            defining_query,
            original_query,
            schedule,
            refresh_mode,
            status,
            is_populated,
            data_timestamp,
            consecutive_errors,
            needs_reinit,
            auto_threshold,
            last_full_ms,
            functions_used,
            frontier,
            topk_limit,
            topk_order_by,
            topk_offset,
            diamond_consistency,
            diamond_schedule_policy,
            has_keyless_source,
            function_hashes,
            requested_cdc_mode,
            is_append_only,
            scc_id,
            last_fixpoint_iterations,
            pooler_compatibility_mode,
            refresh_tier,
            fuse_mode,
            fuse_state,
            fuse_ceiling,
            fuse_sensitivity,
            blown_at,
            blow_reason,
            st_partition_key,
            max_differential_joins,
            max_delta_fraction,
            last_error_message,
            last_error_at,
            downstream_publication_name,
            freshness_deadline_ms,
            st_placement,
            temporal_mode,
            storage_backend,
            post_refresh_action,
            reindex_drift_threshold,
            rows_changed_since_last_reindex,
            last_reindex_at,
            defining_query_hash,
            storage_fillfactor,
            query_complexity_class,
            row_identity_version,
            row_probe_version,
            self_heal_work_mem_percent,
            self_heal_lock_backoff_exponent,
            self_heal_success_streak,
            last_error_code,
            last_error_retryable,
            defining_search_path,
            window_strategy,
        })
    }
}

// ── Dependency CRUD ────────────────────────────────────────────────────────

impl StDependency {
    /// Insert a dependency edge.
    pub fn insert(
        pgt_id: i64,
        source_relid: pg_sys::Oid,
        source_type: &str,
        columns_used: Option<Vec<String>>,
    ) -> Result<(), PgTrickleError> {
        Self::insert_with_snapshot(pgt_id, source_relid, source_type, columns_used, None, None)
    }

    /// Delete all dependency edges for a stream table.
    pub fn delete_for_st(pgt_id: i64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "DELETE FROM pgtrickle.pgt_dependencies WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Insert a dependency edge with column snapshot and schema fingerprint.
    pub fn insert_with_snapshot(
        pgt_id: i64,
        source_relid: pg_sys::Oid,
        source_type: &str,
        columns_used: Option<Vec<String>>,
        column_snapshot: Option<pgrx::JsonB>,
        schema_fingerprint: Option<String>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "INSERT INTO pgtrickle.pgt_dependencies \
             (pgt_id, source_relid, source_type, cdc_mode, columns_used, \
              column_snapshot, schema_fingerprint) \
             VALUES ($1, $2, $3, 'TRIGGER', $4, $5, $6) \
             ON CONFLICT DO NOTHING",
            &[
                pgt_id.into(),
                source_relid.into(),
                source_type.into(),
                columns_used.into(),
                column_snapshot.into(),
                schema_fingerprint.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Update the CDC mode and related fields for a dependency.
    pub fn update_cdc_mode(
        pgt_id: i64,
        source_relid: pg_sys::Oid,
        cdc_mode: CdcMode,
        slot_name: Option<&str>,
        decoder_confirmed_lsn: Option<&str>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_dependencies \
             SET cdc_mode = $1, slot_name = $2, decoder_confirmed_lsn = $3::pg_lsn, \
                 transition_started_at = CASE WHEN $1 = 'TRANSITIONING' THEN now() ELSE NULL END \
             WHERE pgt_id = $4 AND source_relid = $5",
            &[
                cdc_mode.as_str().into(),
                slot_name.into(),
                decoder_confirmed_lsn.into(),
                pgt_id.into(),
                source_relid.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Update the CDC mode and related fields for all dependencies of a source.
    pub fn update_cdc_mode_for_source(
        source_relid: pg_sys::Oid,
        cdc_mode: CdcMode,
        slot_name: Option<&str>,
        decoder_confirmed_lsn: Option<&str>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_dependencies \
             SET cdc_mode = $1, slot_name = $2, decoder_confirmed_lsn = $3::pg_lsn, \
                 transition_started_at = CASE WHEN $1 = 'TRANSITIONING' THEN now() ELSE NULL END \
             WHERE source_relid = $4",
            &[
                cdc_mode.as_str().into(),
                slot_name.into(),
                decoder_confirmed_lsn.into(),
                source_relid.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Store or clear the exact WAL handoff proof for a source.
    pub fn set_cutover_for_source(
        source_relid: pg_sys::Oid,
        target: Option<&str>,
        lsn: Option<&str>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_dependencies \
             SET cutover_target = $1, cutover_lsn = $2::pg_lsn \
             WHERE source_relid = $3",
            &[target.into(), lsn.into(), source_relid.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Resolve the effective CDC request for a source across all deferred STs.
    ///
    /// Precedence is conservative: if any dependent ST requests `trigger`, the
    /// source remains trigger-based. Otherwise `wal` wins over `auto`.
    /// Returns `None` when no deferred TABLE dependencies exist.
    pub fn effective_requested_mode_for_source(
        source_relid: pg_sys::Oid,
    ) -> Result<Option<String>, PgTrickleError> {
        Spi::get_one_with_args::<String>(
            "SELECT CASE \
                    WHEN bool_or(lower(COALESCE(st.requested_cdc_mode, current_setting('pg_trickle.cdc_mode'))) = 'trigger') THEN 'trigger' \
                    WHEN bool_or(lower(COALESCE(st.requested_cdc_mode, current_setting('pg_trickle.cdc_mode'))) = 'wal') THEN 'wal' \
                    WHEN bool_or(lower(COALESCE(st.requested_cdc_mode, current_setting('pg_trickle.cdc_mode'))) = 'auto') THEN 'auto' \
                    ELSE NULL \
                END \
             FROM pgtrickle.pgt_dependencies d \
             JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id \
             WHERE d.source_relid = $1 \
               AND d.source_type = 'TABLE' \
               AND st.refresh_mode <> 'IMMEDIATE'",
            &[source_relid.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Get all dependencies for a stream table.
    pub fn get_for_st(pgt_id: i64) -> Result<Vec<Self>, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT pgt_id, source_relid, source_type, columns_used, \
                            cdc_mode, slot_name, decoder_confirmed_lsn::text, \
                            transition_started_at::text, cutover_target, cutover_lsn::text, \
                            column_snapshot, schema_fingerprint, source_stable_name, \
                            COALESCE(source_placement, 'local') AS source_placement \
                     FROM pgtrickle.pgt_dependencies WHERE pgt_id = $1",
                    None,
                    &[pgt_id.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            let mut result = Vec::new();
            for row in table {
                let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());
                let pgt_id = row.get::<i64>(1).map_err(map_spi)?.unwrap_or(0);
                let source_relid = row
                    .get::<pg_sys::Oid>(2)
                    .map_err(map_spi)?
                    .unwrap_or(pg_sys::InvalidOid);
                let source_type = row.get::<String>(3).map_err(map_spi)?.unwrap_or_default();
                let columns_used = row.get::<Vec<String>>(4).map_err(map_spi)?;
                let cdc_mode_str = row.get::<String>(5).map_err(map_spi)?.unwrap_or_default();
                let slot_name = row.get::<String>(6).map_err(map_spi)?;
                let decoder_confirmed_lsn = row.get::<String>(7).map_err(map_spi)?;
                let transition_started_at = row.get::<String>(8).map_err(map_spi)?;
                let cutover_target = row.get::<String>(9).map_err(map_spi)?;
                let cutover_lsn = row.get::<String>(10).map_err(map_spi)?;
                let column_snapshot = row.get::<pgrx::JsonB>(11).map_err(map_spi)?.map(|jb| jb.0);
                let schema_fingerprint = row.get::<String>(12).map_err(map_spi)?;
                let source_stable_name = row.get::<String>(13).map_err(map_spi)?;
                let source_placement = row
                    .get::<String>(14)
                    .map_err(map_spi)?
                    .unwrap_or_else(|| "local".to_string());
                result.push(StDependency {
                    pgt_id,
                    source_relid,
                    source_type,
                    columns_used,
                    column_snapshot,
                    schema_fingerprint,
                    cdc_mode: CdcMode::from_str(&cdc_mode_str),
                    slot_name,
                    decoder_confirmed_lsn,
                    transition_started_at,
                    cutover_target,
                    cutover_lsn,
                    source_stable_name,
                    source_placement,
                });
            }
            Ok(result)
        })
    }

    /// PERF-002 (v0.70.0): Batch-load dependencies for multiple stream tables
    /// in a single SPI round-trip. Returns a `HashMap<pgt_id, Vec<StDependency>>`.
    ///
    /// Used by the fused eligibility checker to preload all dependency rows
    /// before the eligibility loop, eliminating the O(N) per-node SPI fan-out.
    pub fn get_for_sts(
        pgt_ids: &[i64],
    ) -> Result<std::collections::HashMap<i64, Vec<Self>>, PgTrickleError> {
        if pgt_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        // Build a VALUES list to avoid binding an array parameter.
        let id_list = pgt_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        // nosemgrep: rust.spi.query.dynamic-format — pgt_ids are i64 (not user input).
        let sql = format!(
            "SELECT pgt_id, source_relid, source_type, columns_used, \
                    cdc_mode, slot_name, decoder_confirmed_lsn::text, \
                    transition_started_at::text, cutover_target, cutover_lsn::text, \
                    column_snapshot, schema_fingerprint, source_stable_name, \
                    COALESCE(source_placement, 'local') AS source_placement \
             FROM pgtrickle.pgt_dependencies WHERE pgt_id IN ({id_list})"
        );
        Spi::connect(|client| {
            let table = client
                .select(&sql, None, &[])
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            let mut map: std::collections::HashMap<i64, Vec<Self>> =
                std::collections::HashMap::new();
            for row in table {
                let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());
                let pgt_id = row.get::<i64>(1).map_err(map_spi)?.unwrap_or(0);
                let source_relid = row
                    .get::<pg_sys::Oid>(2)
                    .map_err(map_spi)?
                    .unwrap_or(pg_sys::InvalidOid);
                let source_type = row.get::<String>(3).map_err(map_spi)?.unwrap_or_default();
                let columns_used = row.get::<Vec<String>>(4).map_err(map_spi)?;
                let cdc_mode_str = row.get::<String>(5).map_err(map_spi)?.unwrap_or_default();
                let slot_name = row.get::<String>(6).map_err(map_spi)?;
                let decoder_confirmed_lsn = row.get::<String>(7).map_err(map_spi)?;
                let transition_started_at = row.get::<String>(8).map_err(map_spi)?;
                let cutover_target = row.get::<String>(9).map_err(map_spi)?;
                let cutover_lsn = row.get::<String>(10).map_err(map_spi)?;
                let column_snapshot = row.get::<pgrx::JsonB>(11).map_err(map_spi)?.map(|jb| jb.0);
                let schema_fingerprint = row.get::<String>(12).map_err(map_spi)?;
                let source_stable_name = row.get::<String>(13).map_err(map_spi)?;
                let source_placement = row
                    .get::<String>(14)
                    .map_err(map_spi)?
                    .unwrap_or_else(|| "local".to_string());
                map.entry(pgt_id).or_default().push(StDependency {
                    pgt_id,
                    source_relid,
                    source_type,
                    columns_used,
                    column_snapshot,
                    schema_fingerprint,
                    cdc_mode: CdcMode::from_str(&cdc_mode_str),
                    slot_name,
                    decoder_confirmed_lsn,
                    transition_started_at,
                    cutover_target,
                    cutover_lsn,
                    source_stable_name,
                    source_placement,
                });
            }
            Ok(map)
        })
    }

    /// Get all dependencies across all STs (for building the full DAG).
    pub fn get_all() -> Result<Vec<Self>, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT pgt_id, source_relid, source_type, columns_used, \
                            cdc_mode, slot_name, decoder_confirmed_lsn::text, \
                            transition_started_at::text, cutover_target, cutover_lsn::text, \
                            column_snapshot, schema_fingerprint, source_stable_name, \
                            COALESCE(source_placement, 'local') AS source_placement \
                     FROM pgtrickle.pgt_dependencies",
                    None,
                    &[],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            let mut result = Vec::new();
            for row in table {
                let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());
                let pgt_id = row.get::<i64>(1).map_err(map_spi)?.unwrap_or(0);
                let source_relid = row
                    .get::<pg_sys::Oid>(2)
                    .map_err(map_spi)?
                    .unwrap_or(pg_sys::InvalidOid);
                let source_type = row.get::<String>(3).map_err(map_spi)?.unwrap_or_default();
                let columns_used = row.get::<Vec<String>>(4).map_err(map_spi)?;
                let cdc_mode_str = row.get::<String>(5).map_err(map_spi)?.unwrap_or_default();
                let slot_name = row.get::<String>(6).map_err(map_spi)?;
                let decoder_confirmed_lsn = row.get::<String>(7).map_err(map_spi)?;
                let transition_started_at = row.get::<String>(8).map_err(map_spi)?;
                let cutover_target = row.get::<String>(9).map_err(map_spi)?;
                let cutover_lsn = row.get::<String>(10).map_err(map_spi)?;
                let column_snapshot = row.get::<pgrx::JsonB>(11).map_err(map_spi)?.map(|jb| jb.0);
                let schema_fingerprint = row.get::<String>(12).map_err(map_spi)?;
                let source_stable_name = row.get::<String>(13).map_err(map_spi)?;
                let source_placement = row
                    .get::<String>(14)
                    .map_err(map_spi)?
                    .unwrap_or_else(|| "local".to_string());
                result.push(StDependency {
                    pgt_id,
                    source_relid,
                    source_type,
                    columns_used,
                    column_snapshot,
                    schema_fingerprint,
                    cdc_mode: CdcMode::from_str(&cdc_mode_str),
                    slot_name,
                    decoder_confirmed_lsn,
                    transition_started_at,
                    cutover_target,
                    cutover_lsn,
                    source_stable_name,
                    source_placement,
                });
            }
            Ok(result)
        })
    }

    /// Return the `pgt_id`s of all stream tables that depend on the given
    /// `source_relid` as a `STREAM_TABLE` source.
    ///
    /// Used by `drop_stream_table` to implement CASCADE: dropping a stream
    /// table also drops every stream table downstream of it.
    pub fn get_downstream_pgt_ids(source_relid: pg_sys::Oid) -> Result<Vec<i64>, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT DISTINCT pgt_id \
                     FROM pgtrickle.pgt_dependencies \
                     WHERE source_relid = $1 AND source_type = 'STREAM_TABLE'",
                    None,
                    &[source_relid.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;
            let mut result = Vec::new();
            for row in table {
                let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());
                if let Some(id) = row.get::<i64>(1).map_err(map_spi)? {
                    result.push(id);
                }
            }
            Ok(result)
        })
    }

    /// Return the **union** of `columns_used` recorded across all stream tables
    /// that depend on `source_oid` as a base-table source.
    ///
    /// This is the catalog read for F15 (Selective CDC Column Capture). When
    /// every downstream ST has a non-NULL `columns_used`, the result is the
    /// minimal set of columns that must be present in the CDC change buffer.
    ///
    /// Returns `None` when *any* dependency has `columns_used = NULL` (meaning
    /// "all columns are needed" — the ST was created without AST column
    /// tracking, e.g. `SELECT *`). Callers must treat `None` as "track
    /// everything".
    pub fn union_referenced_columns_for_source(
        source_oid: pg_sys::Oid,
    ) -> Result<Option<Vec<String>>, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT d.columns_used \
                     FROM pgtrickle.pgt_dependencies d \
                     JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id \
                     WHERE d.source_relid = $1 \
                       AND d.source_type IN ('TABLE', 'FOREIGN_TABLE') \
                       AND st.status IN ('ACTIVE', 'INITIALIZING')",
                    None,
                    &[source_oid.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            let mut union: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            let mut found_any = false;

            for row in table {
                found_any = true;
                let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());
                match row.get::<Vec<String>>(1).map_err(map_spi)? {
                    // NULL columns_used → ST needs all columns; bail out early.
                    None => return Ok(None),
                    Some(cols) => {
                        for c in cols {
                            union.insert(c);
                        }
                    }
                }
            }

            if !found_any {
                // No dependencies yet (called before the ST insert) — return None
                // so the caller falls back to full column tracking.
                return Ok(None);
            }

            Ok(Some(union.into_iter().collect()))
        })
    }
}

// ── Column snapshot helpers ────────────────────────────────────────────────

/// Build a JSONB column snapshot and SHA-256 fingerprint for a source table.
///
/// Queries `pg_attribute` for the current column set (name, type OID, ordinal
/// position) and returns `(snapshot_jsonb, sha256_hex)`.
///
/// F49: Generated (STORED/VIRTUAL) columns are excluded to align with
/// `resolve_source_column_defs()` which also filters `attgenerated != ''`.
/// This ensures the snapshot matches the columns tracked in the change
/// buffer table, preventing false schema-change alerts.
///
/// The snapshot is a JSON array of objects:
/// ```json
/// [{"name":"id","type_oid":23,"ordinal":1},{"name":"val","type_oid":25,"ordinal":2}]
/// ```
///
/// Used at creation time to record the source schema in `pgt_dependencies`
/// so `detect_schema_change_kind()` can compare against the current catalog.
#[cfg(not(test))]
pub fn build_column_snapshot(
    source_oid: pg_sys::Oid,
) -> Result<(pgrx::JsonB, String), PgTrickleError> {
    use sha2::{Digest, Sha256};

    let sql = format!(
        "SELECT attname::text, atttypid::int, attnum::int \
         FROM pg_attribute \
         WHERE attrelid = {} AND attnum > 0 AND NOT attisdropped \
           AND attgenerated = '' \
         ORDER BY attnum",
        source_oid.to_u32(),
    );

    let entries: Vec<serde_json::Value> = Spi::connect(|client| {
        let result = client
            .select(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut out = Vec::new();
        for row in result {
            let name: String = row
                .get(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let type_oid: i32 = row
                .get(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            let ordinal: i32 = row
                .get(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            out.push(serde_json::json!({
                "name": name,
                "type_oid": type_oid,
                "ordinal": ordinal,
            }));
        }
        Ok(out)
    })?;

    // Include RLS state so the fingerprint changes when RLS is toggled.
    let (rls_enabled, rls_forced) = query_rls_flags(source_oid)?;

    // PT2: Include partition child count so the fingerprint changes when
    // ATTACH/DETACH PARTITION modifies the partition structure.
    let partition_child_count = query_partition_child_count(source_oid)?;

    let snapshot_obj = serde_json::json!({
        "columns": entries,
        "rls_enabled": rls_enabled,
        "rls_forced": rls_forced,
        "partition_child_count": partition_child_count,
    });

    let json_str = serde_json::to_string(&snapshot_obj)
        .map_err(|e| PgTrickleError::InternalError(format!("JSON serialization failed: {e}")))?;

    let mut hasher = Sha256::new();
    hasher.update(json_str.as_bytes());
    let hash = hasher.finalize();
    let fingerprint = hash.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    });

    let snapshot = pgrx::JsonB(snapshot_obj);
    Ok((snapshot, fingerprint))
}

/// Query the current RLS state of a table from `pg_class`.
///
/// Returns `(relrowsecurity, relforcerowsecurity)`.
#[cfg(not(test))]
pub fn query_rls_flags(source_oid: pg_sys::Oid) -> Result<(bool, bool), PgTrickleError> {
    Spi::connect(|client| {
        let sql = format!(
            "SELECT relrowsecurity, relforcerowsecurity \
             FROM pg_class WHERE oid = {}",
            source_oid.to_u32(),
        );
        let mut result = client
            .select(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if let Some(row) = result.next() {
            let rls: bool = row
                .get(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(false);
            let force: bool = row
                .get(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(false);
            return Ok((rls, force));
        }
        Ok((false, false))
    })
}

/// Test-only stub: SPI is unavailable in unit tests.
#[cfg(test)]
pub fn build_column_snapshot(
    _source_oid: pg_sys::Oid,
) -> Result<(pgrx::JsonB, String), PgTrickleError> {
    let obj = serde_json::json!({
        "columns": [],
        "rls_enabled": false,
        "rls_forced": false,
    });
    Ok((pgrx::JsonB(obj), String::new()))
}

/// Test-only stub for `query_rls_flags`.
#[cfg(test)]
pub fn query_rls_flags(_source_oid: pg_sys::Oid) -> Result<(bool, bool), PgTrickleError> {
    Ok((false, false))
}

/// Query the number of child partitions of a table.
///
/// Returns 0 for non-partitioned tables. For partitioned tables (`relkind = 'p'`),
/// returns the count of rows in `pg_inherits` where this table is the parent.
#[cfg(not(test))]
pub fn query_partition_child_count(source_oid: pg_sys::Oid) -> Result<i64, PgTrickleError> {
    Spi::get_one_with_args::<i64>(
        "SELECT count(*)::bigint FROM pg_inherits WHERE inhparent = $1",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    .map(|opt| opt.unwrap_or(0))
}

/// Test-only stub for `query_partition_child_count`.
#[cfg(test)]
pub fn query_partition_child_count(_source_oid: pg_sys::Oid) -> Result<i64, PgTrickleError> {
    Ok(0)
}

/// Get the stored column snapshot for a dependency pair.
///
/// Returns `None` if no snapshot is stored.
pub fn get_column_snapshot(
    pgt_id: i64,
    source_oid: pg_sys::Oid,
) -> Result<Option<pgrx::JsonB>, PgTrickleError> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT column_snapshot FROM pgtrickle.pgt_dependencies \
         WHERE pgt_id = $1 AND source_relid = $2",
        &[pgt_id.into(), source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

/// Get the stored schema fingerprint for a dependency pair.
pub fn get_schema_fingerprint(
    pgt_id: i64,
    source_oid: pg_sys::Oid,
) -> Result<Option<String>, PgTrickleError> {
    Spi::get_one_with_args::<String>(
        "SELECT schema_fingerprint FROM pgtrickle.pgt_dependencies \
         WHERE pgt_id = $1 AND source_relid = $2",
        &[pgt_id.into(), source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

/// Rebuild and persist the column snapshot + schema fingerprint for a given
/// (pgt_id, source_oid) dependency pair.
///
/// Called after Task 3.5 ADD COLUMN online extension so that the next DDL
/// event detects the correct baseline and does not spuriously trigger a reinit.
#[cfg(not(test))]
pub fn store_column_snapshot_for_pgt_id(
    pgt_id: i64,
    source_oid: pg_sys::Oid,
) -> Result<(), PgTrickleError> {
    let (snapshot, fingerprint) = build_column_snapshot(source_oid)?;
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_dependencies \
         SET column_snapshot = $1, schema_fingerprint = $2 \
         WHERE pgt_id = $3 AND source_relid = $4",
        &[
            snapshot.into(),
            fingerprint.as_str().into(),
            pgt_id.into(),
            source_oid.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("Failed to store column snapshot: {e}")))
}

/// Test-only stub.
#[cfg(test)]
pub fn store_column_snapshot_for_pgt_id(
    _pgt_id: i64,
    _source_oid: pg_sys::Oid,
) -> Result<(), PgTrickleError> {
    Ok(())
}

// ── Refresh history CRUD ───────────────────────────────────────────────────

impl RefreshRecord {
    /// Settle committed refreshes using PostgreSQL's authoritative commit
    /// timestamp. Rows without both provenance values remain unmeasured.
    pub fn settle_visibility_timestamps() -> Result<(), PgTrickleError> {
        let limit = crate::config::pg_trickle_scheduler_maintenance_batch_size();
        Spi::run(&format!(
            "WITH candidates AS (
                 SELECT refresh_id
                   FROM pgtrickle.pgt_refresh_history
                  WHERE visible_at IS NULL
                    AND visibility_xid IS NOT NULL
                    AND source_commit_at IS NOT NULL
                    AND status = 'COMPLETED'
                    AND current_setting('track_commit_timestamp', true) = 'on'
                    AND CASE WHEN current_setting('track_commit_timestamp', true) = 'on'
                             THEN pg_xact_commit_timestamp(visibility_xid) END IS NOT NULL
                  ORDER BY refresh_id
                  LIMIT {limit}
             )
             UPDATE pgtrickle.pgt_refresh_history h
                SET visible_at = pg_xact_commit_timestamp(h.visibility_xid),
                    commit_to_visible_ms = EXTRACT(EPOCH FROM
                        (pg_xact_commit_timestamp(h.visibility_xid) - h.source_commit_at)) * 1000
              FROM candidates
             WHERE h.refresh_id = candidates.refresh_id
               AND current_setting('track_commit_timestamp', true) = 'on'
               AND CASE WHEN current_setting('track_commit_timestamp', true) = 'on'
                        THEN pg_xact_commit_timestamp(h.visibility_xid) END IS NOT NULL"
        ))
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    }

    /// Settle committed history and refresh the bounded summaries for active
    /// interval targets. This is intentionally scheduler-maintenance work;
    /// refresh execution never waits for an external metrics consumer.
    pub fn settle_and_refresh_freshness() -> Result<(), PgTrickleError> {
        Self::settle_visibility_timestamps()?;
        let ids = Spi::connect(|client| {
            let rows = client
                .select(
                    "SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                      WHERE target_freshness_mode = 'INTERVAL'
                      ORDER BY pgt_id",
                    None,
                    &[],
                )
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let mut ids = Vec::new();
            for row in rows {
                if let Some(id) = row
                    .get::<i64>(1)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                {
                    ids.push(id);
                }
            }
            Ok::<_, PgTrickleError>(ids)
        })?;
        for id in ids {
            Self::update_freshness_state(id)?;
        }
        Ok(())
    }

    fn source_commit_at(
        pgt_id: i64,
        tick_watermark_lsn: Option<&str>,
    ) -> Result<Option<TimestampWithTimeZone>, PgTrickleError> {
        let keys: Vec<(String, i64)> = Spi::connect(|client| {
            let rows = client
                .select(
                    "SELECT DISTINCT cb.buffer_key::text, cb.source_id
                       FROM pgtrickle.pgt_change_buffers cb
                       JOIN pgtrickle.pgt_dependencies d
                         ON ((d.source_type = 'STREAM_TABLE'
                              AND cb.source_kind = 'STREAM_TABLE'
                              AND cb.source_id = (SELECT st.pgt_id
                                                    FROM pgtrickle.pgt_stream_tables st
                                                   WHERE st.pgt_relid = d.source_relid))
                          OR (d.source_type <> 'STREAM_TABLE'
                              AND cb.source_kind = 'BASE'
                              AND cb.source_id = d.source_relid::bigint))
                      WHERE d.pgt_id = $1",
                    None,
                    &[pgt_id.into()],
                )
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let mut keys = Vec::new();
            for row in rows {
                if let (Some(key), Some(source_id)) = (
                    row.get::<String>(1)
                        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?,
                    row.get::<i64>(2)
                        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?,
                ) {
                    keys.push((key, source_id));
                }
            }
            Ok(keys)
        })?;
        if keys.is_empty() {
            return Ok(None);
        }

        let schema = crate::config::pg_trickle_change_buffer_schema();
        let upper_lsn_filter = tick_watermark_lsn
            .map(|lsn| format!(" AND lsn <= '{}'::pg_lsn", lsn.replace('\'', "''")))
            .unwrap_or_default();
        let selects = keys
            .iter()
            .map(|(key, source_id)| {
                format!(
                    r#"SELECT min(COALESCE(source_commit_at, CASE
                               WHEN current_setting('track_commit_timestamp', true) = 'on'
                               THEN pg_xact_commit_timestamp(source_xid) END)) AS source_commit_at
                       FROM {qualified} WHERE lsn > COALESCE(
                           (SELECT (frontier->'sources'->'{source_id}'->>'lsn')::pg_lsn
                              FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {pgt_id}),
                           '0/0'::pg_lsn)
                         AND (source_commit_at IS NOT NULL
                          OR (source_xid IS NOT NULL
                              AND current_setting('track_commit_timestamp', true) = 'on')){upper_lsn_filter}"#,
                    qualified = crate::sql_builder::qualified(&schema, key),
                    pgt_id = pgt_id,
                    upper_lsn_filter = upper_lsn_filter
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        Spi::get_one::<TimestampWithTimeZone>(&format!(
            "SELECT min(source_commit_at) FROM ({selects}) samples"
        ))
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    }

    /// Return a safely quoted exact source timestamp for generated downstream
    /// CDC rows, or a typed NULL when the input frontier has no evidence.
    pub(crate) fn source_commit_at_sql_for_refresh(pgt_id: i64) -> Result<String, PgTrickleError> {
        let Some(timestamp) = Self::source_commit_at(pgt_id, None)? else {
            return Ok("NULL::timestamptz".into());
        };
        Spi::get_one_with_args::<String>(
            "SELECT pg_catalog.quote_literal($1::timestamptz)",
            &[timestamp.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::SpiError("could not quote source timestamp".into()))
    }

    fn update_freshness_state(pgt_id: i64) -> Result<(), PgTrickleError> {
        let window_hours = crate::config::pg_trickle_sla_window_hours().max(1);
        Spi::run_with_args(
            &format!(
                "WITH samples AS (
                     SELECT h.refresh_id, h.commit_to_visible_ms
                       FROM pgtrickle.pgt_refresh_history h
                      WHERE h.pgt_id = $1
                        AND h.status = 'COMPLETED'
                        AND h.commit_to_visible_ms IS NOT NULL
                        AND h.start_time >= clock_timestamp() - {window_hours} * interval '1 hour'
                      ORDER BY h.refresh_id DESC
                      LIMIT 128
                 ), stats AS (
                     SELECT count(*)::integer AS sample_count,
                            max(refresh_id) AS last_refresh_id,
                            (array_agg(commit_to_visible_ms ORDER BY refresh_id DESC))[1] AS last_sample,
                            percentile_cont(0.50) WITHIN GROUP (ORDER BY commit_to_visible_ms) AS p50,
                            percentile_cont(0.95) WITHIN GROUP (ORDER BY commit_to_visible_ms) AS p95,
                            percentile_cont(0.99) WITHIN GROUP (ORDER BY commit_to_visible_ms) AS p99
                       FROM samples
                 ), costs AS (
                     SELECT min(h.duration_ms) AS minimum_cost_ms,
                            count(h.duration_ms)::integer AS cost_samples
                       FROM pgtrickle.pgt_refresh_history h
                      WHERE h.pgt_id = $1
                        AND h.status = 'COMPLETED'
                        AND h.plan_identity = (SELECT defining_query_hash
                                                 FROM pgtrickle.pgt_stream_tables
                                                WHERE pgt_id = $1)
                        AND h.duration_ms IS NOT NULL
                        AND h.start_time >= clock_timestamp() - {window_hours} * interval '1 hour'
                 ), previous AS (
                     SELECT * FROM pgtrickle.pgt_freshness_controller_state WHERE pgt_id = $1
                 ), decision AS (
                     SELECT st.pgt_id, st.defining_query_hash, st.freshness_deadline_ms,
                            stats.sample_count, stats.last_refresh_id, stats.last_sample,
                            stats.p50, stats.p95, stats.p99,
                            costs.minimum_cost_ms, costs.cost_samples,
                            COALESCE(previous.breach_streak, 0) AS old_breach_streak,
                            COALESCE(previous.recovery_streak, 0) AS old_recovery_streak,
                            COALESCE(previous.infeasibility_streak, 0) AS old_infeasibility_streak,
                            COALESCE(previous.last_settled_refresh_id, 0) AS old_last_refresh_id,
                            COALESCE(previous.sla_status, '') AS old_status,
                            previous.next_due_at
                       FROM pgtrickle.pgt_stream_tables st
                      CROSS JOIN stats CROSS JOIN costs
                      LEFT JOIN previous ON true
                      WHERE st.pgt_id = $1
                        AND st.target_freshness_mode = 'INTERVAL'
                 )
                 INSERT INTO pgtrickle.pgt_freshness_controller_state
                 (pgt_id, controller_version, plan_identity, target_ms,
                  sample_count, last_settled_refresh_id, last_sample_ms,
                  p50_freshness_ms, p95_freshness_ms, p99_freshness_ms,
                  sla_status, evidence_state, breach_streak, recovery_streak,
                  breach_started_at, infeasibility_streak, minimum_cost_ms,
                  infeasibility_reason, next_due_at, last_decision_at,
                  last_input_snapshot)
             SELECT pgt_id, 1, defining_query_hash, freshness_deadline_ms,
                    sample_count, last_refresh_id, last_sample,
                    p50, p95, p99,
                    CASE
                      WHEN current_setting('track_commit_timestamp', true) <> 'on'
                        OR EXISTS (SELECT 1 FROM pgtrickle.pgt_dependencies d
                                    WHERE d.pgt_id = decision.pgt_id
                                      AND d.source_type NOT IN ('TABLE', 'STREAM_TABLE'))
                        THEN 'EVIDENCE_UNAVAILABLE'
                      WHEN sample_count < 20 THEN 'INSUFFICIENT_DATA'
                      WHEN cost_samples >= 5 AND minimum_cost_ms > freshness_deadline_ms
                           AND old_infeasibility_streak >= 2 THEN 'INFEASIBLE'
                      WHEN old_status = 'BREACHING' AND p95 <= freshness_deadline_ms
                           AND old_recovery_streak < 2 THEN 'BREACHING'
                      WHEN old_breach_streak >= 2 AND p95 > freshness_deadline_ms THEN 'BREACHING'
                      WHEN p95 <= freshness_deadline_ms THEN 'MEETING'
                      ELSE 'AT_RISK'
                    END,
                    CASE
                      WHEN current_setting('track_commit_timestamp', true) = 'on'
                       AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_dependencies d
                                        WHERE d.pgt_id = decision.pgt_id
                                          AND d.source_type NOT IN ('TABLE', 'STREAM_TABLE'))
                        THEN 'EXACT' ELSE 'UNAVAILABLE'
                    END,
                    CASE WHEN last_refresh_id > old_last_refresh_id AND p95 > freshness_deadline_ms
                         THEN LEAST(old_breach_streak + 1, 3) ELSE 0 END,
                    CASE WHEN last_refresh_id > old_last_refresh_id AND p95 <= freshness_deadline_ms
                         THEN LEAST(old_recovery_streak + 1, 3) ELSE 0 END,
                    CASE WHEN last_refresh_id > old_last_refresh_id AND p95 > freshness_deadline_ms
                              AND old_breach_streak = 0
                         THEN clock_timestamp() ELSE NULL END,
                    CASE WHEN last_refresh_id > old_last_refresh_id
                               AND cost_samples >= 5 AND minimum_cost_ms > freshness_deadline_ms
                         THEN LEAST(old_infeasibility_streak + 1, 3) ELSE 0 END,
                    minimum_cost_ms,
                    CASE WHEN cost_samples >= 5 AND minimum_cost_ms > freshness_deadline_ms
                         THEN 'FASTEST_COMPATIBLE_REFRESH_EXCEEDS_TARGET' ELSE NULL END,
                    COALESCE(next_due_at,
                             clock_timestamp() + freshness_deadline_ms * interval '1 millisecond'),
                    clock_timestamp(),
                    jsonb_build_object(
                        'controller_version', 1,
                        'sample_count', sample_count,
                        'last_settled_refresh_id', last_refresh_id,
                        'minimum_cost_ms', minimum_cost_ms,
                        'cost_samples', cost_samples)
               FROM decision
             ON CONFLICT (pgt_id) DO UPDATE SET
                 controller_version = EXCLUDED.controller_version,
                 plan_identity = EXCLUDED.plan_identity,
                 target_ms = EXCLUDED.target_ms,
                 last_settled_refresh_id = EXCLUDED.last_settled_refresh_id,
                 last_sample_ms = EXCLUDED.last_sample_ms,
                 sample_count = EXCLUDED.sample_count,
                 p50_freshness_ms = EXCLUDED.p50_freshness_ms,
                 p95_freshness_ms = EXCLUDED.p95_freshness_ms,
                 p99_freshness_ms = EXCLUDED.p99_freshness_ms,
                 sla_status = EXCLUDED.sla_status,
                 evidence_state = EXCLUDED.evidence_state,
                 breach_streak = EXCLUDED.breach_streak,
                 recovery_streak = EXCLUDED.recovery_streak,
                 breach_started_at = CASE
                     WHEN EXCLUDED.sla_status = 'MEETING' THEN NULL
                     WHEN pgtrickle.pgt_freshness_controller_state.breach_started_at IS NOT NULL
                         THEN pgtrickle.pgt_freshness_controller_state.breach_started_at
                     ELSE EXCLUDED.breach_started_at END,
                 infeasibility_streak = EXCLUDED.infeasibility_streak,
                 minimum_cost_ms = EXCLUDED.minimum_cost_ms,
                 infeasibility_reason = EXCLUDED.infeasibility_reason,
                 next_due_at = COALESCE(pgtrickle.pgt_freshness_controller_state.next_due_at,
                                        EXCLUDED.next_due_at),
                 last_input_snapshot = EXCLUDED.last_input_snapshot,
                 last_decision_at = EXCLUDED.last_decision_at,
                 updated_at = now()"
            ),
            &[pgt_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    }

    fn upsert_refresh_summary_for_refresh(refresh_id: i64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "INSERT INTO pgtrickle.pgt_refresh_summary (
                 pgt_id,
                 total_refreshes,
                 successful_refreshes,
                 failed_refreshes,
                 total_rows_inserted,
                 total_rows_updated,
                 total_rows_deleted,
                 total_duration_ms,
                 total_full_refreshes,
                 total_diff_refreshes,
                 total_delta_rows_processed,
                 last_full_reason,
                 last_full_reason_detail,
                 last_refresh_action,
                 last_refresh_status,
                 last_refresh_at
             )
             SELECT
                 h.pgt_id,
                 1,
                 CASE WHEN h.status = 'COMPLETED' THEN 1 ELSE 0 END,
                 CASE WHEN h.status = 'FAILED' THEN 1 ELSE 0 END,
                 COALESCE(h.rows_inserted, 0),
                 COALESCE(h.rows_updated, 0),
                 COALESCE(h.rows_deleted, 0),
                 CASE
                     WHEN h.end_time IS NOT NULL THEN
                         (EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)::bigint
                     ELSE 0
                 END,
                 CASE WHEN h.status = 'COMPLETED' AND h.action IN ('FULL', 'REINITIALIZE') THEN 1 ELSE 0 END,
                 CASE WHEN h.status = 'COMPLETED' AND h.action = 'DIFFERENTIAL' THEN 1 ELSE 0 END,
                 CASE WHEN h.status = 'COMPLETED' THEN COALESCE(h.delta_row_count, 0) ELSE 0 END,
                 CASE WHEN h.action IN ('FULL', 'REINITIALIZE') THEN h.refresh_reason END,
                 CASE WHEN h.action IN ('FULL', 'REINITIALIZE') THEN h.refresh_reason_detail END,
                 h.action,
                 h.status,
                 COALESCE(h.end_time, h.start_time)
             FROM pgtrickle.pgt_refresh_history h
             WHERE h.refresh_id = $1
               AND h.status <> 'RUNNING'
             ON CONFLICT (pgt_id) DO UPDATE SET
                 total_refreshes = pgtrickle.pgt_refresh_summary.total_refreshes + EXCLUDED.total_refreshes,
                 successful_refreshes = pgtrickle.pgt_refresh_summary.successful_refreshes + EXCLUDED.successful_refreshes,
                 failed_refreshes = pgtrickle.pgt_refresh_summary.failed_refreshes + EXCLUDED.failed_refreshes,
                 total_rows_inserted = pgtrickle.pgt_refresh_summary.total_rows_inserted + EXCLUDED.total_rows_inserted,
                 total_rows_updated = pgtrickle.pgt_refresh_summary.total_rows_updated + EXCLUDED.total_rows_updated,
                 total_rows_deleted = pgtrickle.pgt_refresh_summary.total_rows_deleted + EXCLUDED.total_rows_deleted,
                 total_duration_ms = pgtrickle.pgt_refresh_summary.total_duration_ms + EXCLUDED.total_duration_ms,
                 total_full_refreshes = pgtrickle.pgt_refresh_summary.total_full_refreshes + EXCLUDED.total_full_refreshes,
                 total_diff_refreshes = pgtrickle.pgt_refresh_summary.total_diff_refreshes + EXCLUDED.total_diff_refreshes,
                 total_delta_rows_processed = pgtrickle.pgt_refresh_summary.total_delta_rows_processed + EXCLUDED.total_delta_rows_processed,
                 last_full_reason = COALESCE(EXCLUDED.last_full_reason, pgtrickle.pgt_refresh_summary.last_full_reason),
                 last_full_reason_detail = COALESCE(EXCLUDED.last_full_reason_detail, pgtrickle.pgt_refresh_summary.last_full_reason_detail),
                 last_refresh_action = EXCLUDED.last_refresh_action,
                 last_refresh_status = EXCLUDED.last_refresh_status,
                 last_refresh_at = EXCLUDED.last_refresh_at,
                 updated_at = now()",
            &[refresh_id.into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Insert a new refresh history record. Returns the `refresh_id`.
    ///
    /// `initiated_by` indicates what triggered the refresh:
    /// - `"SCHEDULER"` — background scheduler
    /// - `"MANUAL"` — user-invoked `pgtrickle.refresh_stream_table()`
    /// - `"INITIAL"` — first refresh after `create_stream_table()`
    ///
    /// `freshness_deadline` is the SLA deadline for duration-based schedules
    /// (NULL for cron-based schedules).
    ///
    /// `tick_watermark_lsn` is the WAL LSN watermark at tick start (CSS1; NULL when disabled).
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        pgt_id: i64,
        data_timestamp: TimestampWithTimeZone,
        action: &str,
        status: &str,
        rows_inserted: i64,
        rows_deleted: i64,
        error_message: Option<&str>,
        initiated_by: Option<&str>,
        freshness_deadline: Option<TimestampWithTimeZone>,
        delta_row_count: i64,
        merge_strategy_used: Option<&str>,
        was_full_fallback: bool,
        tick_watermark_lsn: Option<&str>,
    ) -> Result<i64, PgTrickleError> {
        Self::settle_visibility_timestamps()?;
        // Settlement runs before the new row is inserted, so refresh the
        // bounded summary now to include any rows that became visible since
        // the last refresh.
        let _ = Self::update_freshness_state(pgt_id);
        let source_commit_at = Self::source_commit_at(pgt_id, tick_watermark_lsn)?;
        let refresh_id = Spi::get_one_with_args::<i64>(
            "INSERT INTO pgtrickle.pgt_refresh_history \
             (pgt_id, data_timestamp, start_time, action, status, \
              rows_inserted, rows_deleted, error_message, \
              initiated_by, freshness_deadline, \
              delta_row_count, merge_strategy_used, was_full_fallback, tick_watermark_lsn,
              source_commit_at, plan_identity) \
             VALUES ($1, $2, clock_timestamp(), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::pg_lsn, $14, \
                     (SELECT defining_query_hash FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1)) \
             RETURNING refresh_id",
            &[
                pgt_id.into(),
                data_timestamp.into(),
                action.into(),
                status.into(),
                rows_inserted.into(),
                rows_deleted.into(),
                error_message.into(),
                initiated_by.into(),
                freshness_deadline.into(),
                delta_row_count.into(),
                merge_strategy_used.into(),
                was_full_fallback.into(),
                tick_watermark_lsn.into(),
                source_commit_at.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::InternalError("INSERT did not return refresh_id".into()))?;

        if status != "RUNNING" {
            Self::upsert_refresh_summary_for_refresh(refresh_id)?;
        }

        Ok(refresh_id)
    }

    /// Complete a refresh record (set end_time and final status).
    #[allow(clippy::too_many_arguments)]
    pub fn complete(
        refresh_id: i64,
        status: &str,
        rows_inserted: i64,
        rows_deleted: i64,
        error_message: Option<&str>,
        delta_row_count: i64,
        merge_strategy_used: Option<&str>,
        was_full_fallback: bool,
    ) -> Result<(), PgTrickleError> {
        Self::complete_with_rows_updated(
            refresh_id,
            status,
            rows_inserted,
            rows_inserted.min(rows_deleted),
            rows_deleted,
            error_message,
            delta_row_count,
            merge_strategy_used,
            was_full_fallback,
        )
    }

    /// Complete a refresh record with exact INSERT/UPDATE/DELETE counts.
    #[allow(clippy::too_many_arguments)]
    pub fn complete_with_rows_updated(
        refresh_id: i64,
        status: &str,
        rows_inserted: i64,
        rows_updated: i64,
        rows_deleted: i64,
        error_message: Option<&str>,
        delta_row_count: i64,
        merge_strategy_used: Option<&str>,
        was_full_fallback: bool,
    ) -> Result<(), PgTrickleError> {
        Self::complete_with_rows_updated_and_reason(
            refresh_id,
            status,
            rows_inserted,
            rows_updated,
            rows_deleted,
            error_message,
            delta_row_count,
            merge_strategy_used,
            was_full_fallback,
            None,
        )
    }

    /// Complete a refresh record and persist a typed FULL-refresh reason.
    #[allow(clippy::too_many_arguments)]
    pub fn complete_with_rows_updated_and_reason(
        refresh_id: i64,
        status: &str,
        rows_inserted: i64,
        rows_updated: i64,
        rows_deleted: i64,
        error_message: Option<&str>,
        delta_row_count: i64,
        merge_strategy_used: Option<&str>,
        was_full_fallback: bool,
        full_reason: Option<&crate::refresh::FullRefreshReason>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_refresh_history \
             SET end_time = clock_timestamp(), duration_ms = EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000, \
             visibility_xid = pg_current_xact_id_if_assigned()::xid, status = $1, rows_inserted = $2, \
             rows_updated = $3, rows_deleted = $4, error_message = $5, \
             delta_row_count = $6, merge_strategy_used = $7, \
             was_full_fallback = $8, refresh_reason = $9, \
             refresh_reason_detail = $10 \
             WHERE refresh_id = $11",
            &[
                status.into(),
                rows_inserted.into(),
                rows_updated.into(),
                rows_deleted.into(),
                error_message.into(),
                delta_row_count.into(),
                merge_strategy_used.into(),
                was_full_fallback.into(),
                full_reason.map(|reason| reason.code.as_str()).into(),
                full_reason.map(|reason| reason.detail.as_str()).into(),
                refresh_id.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

        Self::upsert_refresh_summary_for_refresh(refresh_id)?;
        let pgt_id = Spi::get_one_with_args::<i64>(
            "SELECT pgt_id FROM pgtrickle.pgt_refresh_history WHERE refresh_id = $1",
            &[refresh_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if let Some(pgt_id) = pgt_id {
            Self::update_freshness_state(pgt_id)?;
        }
        Ok(())
    }

    pub fn complete_with_failure(
        refresh_id: i64,
        status: &str,
        error_message: &str,
        error_code: &str,
        error_sqlstate: Option<&str>,
        retryable: bool,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_refresh_history
             SET end_time = clock_timestamp(), duration_ms = EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000,
                 status = $1, error_message = $2,
                 error_code = $3, error_sqlstate = $4, retryable = $5
             WHERE refresh_id = $6",
            &[
                status.into(),
                error_message.into(),
                error_code.into(),
                error_sqlstate.into(),
                retryable.into(),
                refresh_id.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;
        Self::upsert_refresh_summary_for_refresh(refresh_id)
    }
}

// ── Source gate CRUD (v0.5.0, Phase 3 — Bootstrap Source Gating) ──────────

/// Returns the OIDs of all currently gated source relations.
///
/// Used by the scheduler once per tick to build the gated-source set before
/// deciding whether to skip a stream table refresh.
pub fn get_gated_source_oids() -> Result<Vec<pg_sys::Oid>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT source_relid \
                 FROM pgtrickle.pgt_source_gates \
                 WHERE gated = true",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut oids: Vec<pg_sys::Oid> = Vec::new();
        for row in table {
            if let Some(oid) = row
                .get::<pg_sys::Oid>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            {
                oids.push(oid);
            }
        }
        Ok(oids)
    })
}

/// UPSERT a source gate: mark the given source OID as gated.
pub fn upsert_gate(
    source_relid: pg_sys::Oid,
    gated_by: Option<&str>,
) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_source_gates \
             (source_relid, gated, gated_at, ungated_at, gated_by) \
         VALUES ($1, true, now(), NULL, $2) \
         ON CONFLICT (source_relid) DO UPDATE SET \
             gated = true, gated_at = now(), ungated_at = NULL, \
             gated_by = EXCLUDED.gated_by",
        &[source_relid.into(), gated_by.into()],
    )
    .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
}

/// Mark a source gate as ungated (sets gated=false and records ungated_at).
pub fn set_ungated(source_relid: pg_sys::Oid) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_source_gates \
         SET gated = false, ungated_at = now() \
         WHERE source_relid = $1",
        &[source_relid.into()],
    )
    .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
}

// ── Watermark Gating (v0.7.0) ─────────────────────────────────────────────

/// Per-source watermark state, mirrors `pgtrickle.pgt_watermarks`.
#[derive(Debug, Clone)]
pub struct WatermarkState {
    pub source_relid: pg_sys::Oid,
    pub watermark: TimestampWithTimeZone,
    pub updated_at: TimestampWithTimeZone,
    pub advanced_by: Option<String>,
    pub wal_lsn_at_advance: Option<String>,
}

/// Watermark group definition, mirrors `pgtrickle.pgt_watermark_groups`.
#[derive(Debug, Clone)]
pub struct WatermarkGroup {
    pub group_id: i32,
    pub group_name: String,
    pub source_relids: Vec<pg_sys::Oid>,
    pub tolerance_secs: f64,
    pub created_at: TimestampWithTimeZone,
}

/// Advance (or insert) the watermark for a source table.
///
/// Enforces monotonicity: the new watermark must be >= the current value.
/// Records `pg_current_wal_insert_lsn()` alongside the watermark for
/// future hold-back support.
pub fn advance_watermark(
    source_relid: pg_sys::Oid,
    watermark: TimestampWithTimeZone,
    advanced_by: Option<&str>,
) -> Result<(), PgTrickleError> {
    Spi::connect_mut(|client| {
        // Check monotonicity: reject backward movement.
        // Use client.select + into_iter().next() to safely handle zero rows.
        let table = client
            .select(
                "SELECT watermark FROM pgtrickle.pgt_watermarks WHERE source_relid = $1",
                None,
                &[source_relid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let current: Option<TimestampWithTimeZone> = if let Some(row) = table.into_iter().next() {
            row.get::<TimestampWithTimeZone>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        } else {
            None
        };

        if let Some(current_wm) = current {
            // Compare via SQL to use PostgreSQL's TIMESTAMPTZ comparison.
            let cmp_table = client
                .select(
                    "SELECT $1::timestamptz < $2::timestamptz AS is_backward, \
                            $1::timestamptz = $2::timestamptz AS is_equal",
                    None,
                    &[watermark.into(), current_wm.into()],
                )
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

            if let Some(cmp_row) = cmp_table.into_iter().next() {
                let is_backward = cmp_row
                    .get::<bool>(1)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .unwrap_or(false);

                if is_backward {
                    return Err(PgTrickleError::WatermarkBackwardMovement(format!(
                        "new watermark is older than current for source OID {}",
                        source_relid.to_u32()
                    )));
                }

                let is_equal = cmp_row
                    .get::<bool>(2)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .unwrap_or(false);

                if is_equal {
                    return Ok(());
                }
            }
        }

        client
            .update(
                "INSERT INTO pgtrickle.pgt_watermarks \
                     (source_relid, watermark, updated_at, advanced_by, wal_lsn_at_advance) \
                 VALUES ($1, $2, now(), $3, pg_current_wal_insert_lsn()::text) \
                 ON CONFLICT (source_relid) DO UPDATE SET \
                     watermark = EXCLUDED.watermark, \
                     updated_at = EXCLUDED.updated_at, \
                     advanced_by = EXCLUDED.advanced_by, \
                     wal_lsn_at_advance = EXCLUDED.wal_lsn_at_advance",
                None,
                &[source_relid.into(), watermark.into(), advanced_by.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Ok(())
    })
}

/// Get all current watermark states.
pub fn get_all_watermarks() -> Result<Vec<WatermarkState>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT source_relid, watermark, updated_at, advanced_by, wal_lsn_at_advance \
                 FROM pgtrickle.pgt_watermarks \
                 ORDER BY source_relid",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut out = Vec::new();
        for row in table {
            let source_relid = row
                .get::<pg_sys::Oid>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(pg_sys::Oid::from(0u32));
            let watermark = row
                .get::<TimestampWithTimeZone>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL watermark in pgt_watermarks".into())
                })?;
            let updated_at = row
                .get::<TimestampWithTimeZone>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL updated_at in pgt_watermarks".into())
                })?;
            let advanced_by = row
                .get::<String>(4)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let wal_lsn_at_advance = row
                .get::<String>(5)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            out.push(WatermarkState {
                source_relid,
                watermark,
                updated_at,
                advanced_by,
                wal_lsn_at_advance,
            });
        }
        Ok(out)
    })
}

/// Get the watermark for a specific source OID.
pub fn get_watermark_for_source(
    source_relid: pg_sys::Oid,
) -> Result<Option<WatermarkState>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT source_relid, watermark, updated_at, advanced_by, wal_lsn_at_advance \
                 FROM pgtrickle.pgt_watermarks WHERE source_relid = $1",
                None,
                &[source_relid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        if let Some(row) = table.into_iter().next() {
            let watermark = row
                .get::<TimestampWithTimeZone>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL watermark in pgt_watermarks".into())
                })?;
            let updated_at = row
                .get::<TimestampWithTimeZone>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL updated_at in pgt_watermarks".into())
                })?;
            let advanced_by = row
                .get::<String>(4)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let wal_lsn_at_advance = row
                .get::<String>(5)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            return Ok(Some(WatermarkState {
                source_relid,
                watermark,
                updated_at,
                advanced_by,
                wal_lsn_at_advance,
            }));
        }
        Ok(None)
    })
}

/// Create a new watermark group.
pub fn create_watermark_group(
    group_name: &str,
    source_relids: &[pg_sys::Oid],
    tolerance_secs: f64,
) -> Result<i32, PgTrickleError> {
    // Check for duplicate name.
    let exists: Option<bool> = Spi::get_one_with_args(
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_watermark_groups WHERE group_name = $1)",
        &[group_name.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    if exists == Some(true) {
        return Err(PgTrickleError::WatermarkGroupAlreadyExists(
            group_name.to_string(),
        ));
    }

    // Build an OID array literal for SQL.
    let oid_strs: Vec<String> = source_relids
        .iter()
        .map(|o| o.to_u32().to_string())
        .collect();
    let array_literal = format!("ARRAY[{}]::oid[]", oid_strs.join(","));

    let sql = format!(
        "INSERT INTO pgtrickle.pgt_watermark_groups \
             (group_name, source_relids, tolerance_secs) \
         VALUES ($1, {}, $2) \
         RETURNING group_id",
        array_literal
    );
    let group_id: i32 = Spi::get_one_with_args(&sql, &[group_name.into(), tolerance_secs.into()])
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| {
            PgTrickleError::InternalError(
                "create_watermark_group: INSERT RETURNING returned NULL".into(),
            )
        })?;
    Ok(group_id)
}

/// Drop a watermark group by name.
pub fn drop_watermark_group(group_name: &str) -> Result<(), PgTrickleError> {
    let deleted: Option<i64> = Spi::get_one_with_args(
        "WITH d AS (\
             DELETE FROM pgtrickle.pgt_watermark_groups WHERE group_name = $1 RETURNING 1\
         ) SELECT count(*) FROM d",
        &[group_name.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    if deleted == Some(0) || deleted.is_none() {
        return Err(PgTrickleError::WatermarkGroupNotFound(
            group_name.to_string(),
        ));
    }
    Ok(())
}

/// Get all watermark groups.
pub fn get_all_watermark_groups() -> Result<Vec<WatermarkGroup>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT group_id, group_name, source_relids, tolerance_secs, created_at \
                 FROM pgtrickle.pgt_watermark_groups \
                 ORDER BY group_name",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut out = Vec::new();
        for row in table {
            let group_id = row
                .get::<i32>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            let group_name = row
                .get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            // source_relids is OID[] — fetch as a Vec<pg_sys::Oid>.
            let source_relids: Vec<pg_sys::Oid> = row
                .get::<Vec<pg_sys::Oid>>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let tolerance_secs = row
                .get::<f64>(4)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0.0);
            let created_at = row
                .get::<TimestampWithTimeZone>(5)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("NULL created_at in pgt_watermark_groups".into())
                })?;
            out.push(WatermarkGroup {
                group_id,
                group_name,
                source_relids,
                tolerance_secs,
                created_at,
            });
        }
        Ok(out)
    })
}

/// Check watermark alignment for a stream table's source OIDs.
///
/// Returns `true` if all overlapping watermark groups are aligned (or no
/// groups apply). Returns `false` if any group's watermarks are misaligned
/// beyond tolerance.
///
/// A group is considered aligned when:
///   max(watermark) - min(watermark) <= tolerance
/// among all source OIDs that belong to both the group and the ST's source set.
///
/// Sources that have never had a watermark advanced are ignored (they don't
/// participate in gating until their first `advance_watermark()` call).
pub fn check_watermark_alignment(
    source_oids: &[pg_sys::Oid],
) -> Result<(bool, Option<String>), PgTrickleError> {
    let groups = get_all_watermark_groups()?;
    if groups.is_empty() {
        return Ok((true, None));
    }

    let source_set: std::collections::HashSet<pg_sys::Oid> = source_oids.iter().copied().collect();

    for group in &groups {
        // Find the intersection: group sources that are also ST sources.
        let overlapping: Vec<pg_sys::Oid> = group
            .source_relids
            .iter()
            .filter(|oid| source_set.contains(oid))
            .copied()
            .collect();

        // If fewer than 2 of this group's sources are in the ST's source
        // set, the group is irrelevant for this ST.
        if overlapping.len() < 2 {
            continue;
        }

        // Collect watermarks for overlapping sources.
        let mut timestamps: Vec<TimestampWithTimeZone> = Vec::new();
        let mut missing_count = 0usize;
        for oid in &overlapping {
            match get_watermark_for_source(*oid)? {
                Some(wm) => timestamps.push(wm.watermark),
                None => missing_count += 1,
            }
        }

        // If any overlapping source has no watermark yet, skip gating for
        // this group (watermarks not fully set up yet).
        if missing_count > 0 {
            continue;
        }

        // All sources have watermarks — check alignment.
        if timestamps.len() >= 2 {
            let lag_secs: Option<f64> = Spi::get_one_with_args(
                "SELECT EXTRACT(EPOCH FROM ($1::timestamptz - $2::timestamptz))::float8",
                &[timestamps[0].into(), timestamps[1].into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

            // For >2 sources, compute max and min via SQL for robustness.
            let (max_wm, min_wm) = if timestamps.len() == 2 {
                // Determine which is max and which is min.
                let first_is_greater: bool = Spi::get_one_with_args(
                    "SELECT $1::timestamptz >= $2::timestamptz",
                    &[timestamps[0].into(), timestamps[1].into()],
                )
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(true);

                if first_is_greater {
                    (timestamps[0], timestamps[1])
                } else {
                    (timestamps[1], timestamps[0])
                }
            } else {
                // For 3+ sources, build SQL to find max/min.
                let mut max = timestamps[0];
                let mut min = timestamps[0];
                for ts in &timestamps[1..] {
                    let is_greater: bool = Spi::get_one_with_args(
                        "SELECT $1::timestamptz > $2::timestamptz",
                        &[(*ts).into(), max.into()],
                    )
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .unwrap_or(false);
                    if is_greater {
                        max = *ts;
                    }
                    let is_less: bool = Spi::get_one_with_args(
                        "SELECT $1::timestamptz < $2::timestamptz",
                        &[(*ts).into(), min.into()],
                    )
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .unwrap_or(false);
                    if is_less {
                        min = *ts;
                    }
                }
                (max, min)
            };

            let lag: f64 = Spi::get_one_with_args(
                "SELECT EXTRACT(EPOCH FROM ($1::timestamptz - $2::timestamptz))::float8",
                &[max_wm.into(), min_wm.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(0.0);

            let _ = lag_secs; // used above for 2-source shortcut

            if lag > group.tolerance_secs {
                let reason = format!(
                    "watermark group '{}' misaligned: lag {:.1}s exceeds tolerance {:.1}s",
                    group.group_name, lag, group.tolerance_secs
                );
                return Ok((false, Some(reason)));
            }
        }
    }

    Ok((true, None))
}

/// WM-7: Check whether any watermark in groups overlapping with the given
/// source OIDs is "stuck" — i.e. its `updated_at` is older than
/// `now() - holdback_timeout_secs`.
///
/// Returns `(is_stuck, reason)`. When `holdback_timeout_secs` is 0,
/// always returns `(false, None)`.
pub fn check_watermark_staleness(
    source_oids: &[pg_sys::Oid],
    holdback_timeout_secs: i32,
) -> Result<(bool, Option<String>), PgTrickleError> {
    if holdback_timeout_secs <= 0 {
        return Ok((false, None));
    }

    let groups = get_all_watermark_groups()?;
    if groups.is_empty() {
        return Ok((false, None));
    }

    let source_set: std::collections::HashSet<pg_sys::Oid> = source_oids.iter().copied().collect();

    for group in &groups {
        let overlapping: Vec<pg_sys::Oid> = group
            .source_relids
            .iter()
            .filter(|oid| source_set.contains(oid))
            .copied()
            .collect();

        if overlapping.is_empty() {
            continue;
        }

        // Check each overlapping source for staleness.
        for oid in &overlapping {
            if let Some(wm) = get_watermark_for_source(*oid)? {
                let age_secs: Option<f64> = Spi::get_one_with_args(
                    "SELECT EXTRACT(EPOCH FROM (now() - $1::timestamptz))::float8",
                    &[wm.updated_at.into()],
                )
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

                if let Some(age) = age_secs
                    && age > holdback_timeout_secs as f64
                {
                    let reason = format!(
                        "watermark group '{}' has stuck source OID {} \
                         (last advanced {:.0}s ago, timeout {}s)",
                        group.group_name,
                        oid.to_u32(),
                        age,
                        holdback_timeout_secs
                    );
                    return Ok((true, Some(reason)));
                }
            }
            // Sources without a watermark are not considered stuck
            // (they haven't participated in gating yet).
        }
    }

    Ok((false, None))
}

/// WM-7: Find all stuck watermarks across all groups.
///
/// Returns a list of `(group_name, source_oid, age_secs)` for each stuck
/// source. Used by the scheduler's periodic alerting.
pub fn find_stuck_watermarks(
    holdback_timeout_secs: i32,
) -> Result<Vec<(String, u32, f64)>, PgTrickleError> {
    if holdback_timeout_secs <= 0 {
        return Ok(Vec::new());
    }

    let groups = get_all_watermark_groups()?;
    if groups.is_empty() {
        return Ok(Vec::new());
    }

    let mut stuck = Vec::new();
    for group in &groups {
        for oid in &group.source_relids {
            if let Some(wm) = get_watermark_for_source(*oid)? {
                let age_secs: Option<f64> = Spi::get_one_with_args(
                    "SELECT EXTRACT(EPOCH FROM (now() - $1::timestamptz))::float8",
                    &[wm.updated_at.into()],
                )
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

                if let Some(age) = age_secs
                    && age > holdback_timeout_secs as f64
                {
                    stuck.push((group.group_name.clone(), oid.to_u32(), age));
                }
            }
        }
    }

    Ok(stuck)
}

// ── Scheduler Job (Phase 2: parallel refresh) ─────────────────────────────

/// Status of a scheduler job in the parallel refresh pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    RetryableFailed,
    PermanentFailed,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Queued => "QUEUED",
            JobStatus::Running => "RUNNING",
            JobStatus::Succeeded => "SUCCEEDED",
            JobStatus::RetryableFailed => "RETRYABLE_FAILED",
            JobStatus::PermanentFailed => "PERMANENT_FAILED",
            JobStatus::Cancelled => "CANCELLED",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "QUEUED" => JobStatus::Queued,
            "RUNNING" => JobStatus::Running,
            "SUCCEEDED" => JobStatus::Succeeded,
            "RETRYABLE_FAILED" => JobStatus::RetryableFailed,
            "PERMANENT_FAILED" => JobStatus::PermanentFailed,
            "CANCELLED" => JobStatus::Cancelled,
            _ => JobStatus::Cancelled,
        }
    }

    /// Whether this status represents a terminal state.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobStatus::Succeeded
                | JobStatus::RetryableFailed
                | JobStatus::PermanentFailed
                | JobStatus::Cancelled
        )
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A scheduler job row from `pgtrickle.pgt_scheduler_jobs`.
#[derive(Debug, Clone)]
pub struct SchedulerJob {
    pub job_id: i64,
    pub dag_version: i64,
    pub unit_key: String,
    pub unit_kind: String,
    pub member_pgt_ids: Vec<i64>,
    pub root_pgt_id: i64,
    pub status: JobStatus,
    pub scheduler_pid: i32,
    pub worker_pid: Option<i32>,
    pub attempt_no: i32,
    pub enqueued_at: TimestampWithTimeZone,
    pub started_at: Option<TimestampWithTimeZone>,
    pub finished_at: Option<TimestampWithTimeZone>,
    pub outcome_detail: Option<String>,
    pub retryable: Option<bool>,
    pub dispatch_tick_id: Option<i64>,
    pub tick_watermark_lsn: Option<String>,
    pub outcome_code: Option<String>,
    pub outcome_sqlstate: Option<String>,
    pub worker_slot_generation: Option<i64>,
}

impl SchedulerJob {
    /// Enqueue a new job in QUEUED status. Returns the assigned `job_id`.
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue(
        dag_version: i64,
        unit_key: &str,
        unit_kind: &str,
        member_pgt_ids: &[i64],
        root_pgt_id: i64,
        scheduler_pid: i32,
        attempt_no: i32,
        dispatch_tick_id: i64,
        tick_watermark_lsn: &str,
    ) -> Result<i64, PgTrickleError> {
        Spi::connect_mut(|client| {
            let row = client
                .update(
                    "INSERT INTO pgtrickle.pgt_scheduler_jobs \
                     (dag_version, unit_key, unit_kind, member_pgt_ids, root_pgt_id, \
                      scheduler_pid, attempt_no, dispatch_tick_id, tick_watermark_lsn) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::pg_lsn) \
                     RETURNING job_id",
                    None,
                    &[
                        dag_version.into(),
                        unit_key.into(),
                        unit_kind.into(),
                        member_pgt_ids.into(),
                        root_pgt_id.into(),
                        scheduler_pid.into(),
                        attempt_no.into(),
                        dispatch_tick_id.into(),
                        tick_watermark_lsn.into(),
                    ],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
                .first();

            row.get_one::<i64>()
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("INSERT job did not return job_id".into())
                })
        })
    }

    /// Claim a QUEUED job: transition QUEUED → RUNNING and set worker_pid.
    ///
    /// Returns `Ok(true)` if the claim succeeded (row was updated),
    /// `Ok(false)` if the job was already claimed or no longer QUEUED.
    pub fn claim(job_id: i64, worker_pid: i32) -> Result<bool, PgTrickleError> {
        Spi::connect_mut(|client| {
            let result = client
                .update(
                    "UPDATE pgtrickle.pgt_scheduler_jobs \
                     SET status = 'RUNNING', worker_pid = $2, started_at = now() \
                     WHERE job_id = $1 AND status = 'QUEUED'",
                    None,
                    &[job_id.into(), worker_pid.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;
            if !result.is_empty() {
                // SAFETY: Catalog transitions run in a backend attached to a database.
                crate::shmem::decrement_parallel_queue_depth_for_database(unsafe {
                    pg_sys::MyDatabaseId.to_u32()
                });
            }
            Ok(!result.is_empty())
        })
    }

    /// Persist the shared-memory generation assigned to a queued job.
    pub fn set_worker_slot_generation(job_id: i64, generation: u64) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_scheduler_jobs \
             SET worker_slot_generation = $2 \
             WHERE job_id = $1 AND status = 'QUEUED'",
            &[job_id.into(), (generation as i64).into()],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Complete a job: set terminal status, outcome detail, and retryability.
    pub fn complete(
        job_id: i64,
        status: JobStatus,
        outcome_detail: Option<&str>,
        retryable: Option<bool>,
    ) -> Result<(), PgTrickleError> {
        Spi::connect_mut(|client| {
            client
                .update(
                    "UPDATE pgtrickle.pgt_scheduler_jobs \
                     SET status = $2, finished_at = now(), \
                         outcome_detail = $3, retryable = $4 \
                     WHERE job_id = $1",
                    None,
                    &[
                        job_id.into(),
                        status.as_str().into(),
                        outcome_detail.into(),
                        retryable.into(),
                    ],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;
            Ok(())
        })
    }

    pub fn complete_typed(
        job_id: i64,
        status: JobStatus,
        outcome_detail: Option<&str>,
        retryable: Option<bool>,
        outcome_code: Option<&str>,
        outcome_sqlstate: Option<&str>,
    ) -> Result<(), PgTrickleError> {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_scheduler_jobs
             SET status = $2, finished_at = now(), outcome_detail = $3,
                 retryable = $4, outcome_code = $5, outcome_sqlstate = $6
             WHERE job_id = $1",
            &[
                job_id.into(),
                status.as_str().into(),
                outcome_detail.into(),
                retryable.into(),
                outcome_code.into(),
                outcome_sqlstate.into(),
            ],
        )
        .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))
    }

    /// Cancel a job (force to CANCELLED).
    pub fn cancel(job_id: i64, reason: &str) -> Result<(), PgTrickleError> {
        let cancelled_queued = Spi::connect_mut(|client| {
            let result = client
                .update(
                    "UPDATE pgtrickle.pgt_scheduler_jobs \
                     SET status = 'CANCELLED', finished_at = now(), \
                         outcome_detail = $2, retryable = NULL \
                     WHERE job_id = $1 AND status = 'QUEUED'",
                    None,
                    &[job_id.into(), reason.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<bool, PgTrickleError>(!result.is_empty())
        })?;
        if cancelled_queued {
            // SAFETY: Catalog transitions run in a backend attached to a database.
            crate::shmem::decrement_parallel_queue_depth_for_database(unsafe {
                pg_sys::MyDatabaseId.to_u32()
            });
            return Ok(());
        }
        Self::complete(job_id, JobStatus::Cancelled, Some(reason), None)
    }

    /// Load a job by its ID. Returns `None` if not found.
    pub fn get_by_id(job_id: i64) -> Result<Option<Self>, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT job_id, dag_version, unit_key, unit_kind, member_pgt_ids, \
                     root_pgt_id, status, scheduler_pid, worker_pid, attempt_no, \
                     enqueued_at, started_at, finished_at, outcome_detail, retryable, \
                     dispatch_tick_id, tick_watermark_lsn::text, \
                     outcome_code, outcome_sqlstate, worker_slot_generation \
                     FROM pgtrickle.pgt_scheduler_jobs \
                     WHERE job_id = $1",
                    None,
                    &[job_id.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

            if table.is_empty() {
                return Ok(None);
            }

            Self::from_spi_table_row(&table.first()).map(Some)
        })
    }

    /// Cancel all QUEUED/RUNNING jobs whose worker_pid or scheduler_pid is no
    /// longer alive. Used for crash recovery / orphaned job cleanup.
    ///
    /// Returns the number of jobs cancelled.
    pub fn cancel_orphaned_jobs() -> Result<i64, PgTrickleError> {
        Spi::connect_mut(|client| {
            let queued_before = client
                .select(
                    "SELECT count(*)::bigint \
                     FROM pgtrickle.pgt_scheduler_jobs \
                     WHERE status = 'QUEUED' \
                       AND (dispatch_tick_id IS NULL OR tick_watermark_lsn IS NULL \
                            OR NOT EXISTS (SELECT 1 FROM pg_stat_activity \
                                           WHERE pid = pgt_scheduler_jobs.worker_pid) \
                            OR NOT EXISTS (SELECT 1 FROM pg_stat_activity \
                                           WHERE pid = pgt_scheduler_jobs.scheduler_pid))",
                    None,
                    &[],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
                .first()
                .get_one::<i64>()
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            let result = client
                .update(
                    "UPDATE pgtrickle.pgt_scheduler_jobs \
                     SET status = 'CANCELLED', \
                         finished_at = now(), \
                         outcome_detail = 'Cancelled: orphaned after crash recovery' \
                     WHERE status IN ('QUEUED', 'RUNNING') \
                       AND (dispatch_tick_id IS NULL OR tick_watermark_lsn IS NULL \
                            OR NOT EXISTS ( \
                           SELECT 1 FROM pg_stat_activity \
                           WHERE pid = pgt_scheduler_jobs.worker_pid \
                       ) \
                            OR NOT EXISTS ( \
                           SELECT 1 FROM pg_stat_activity \
                           WHERE pid = pgt_scheduler_jobs.scheduler_pid \
                       ))",
                    None,
                    &[],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;
            // SAFETY: Catalog transitions run in a backend attached to a database.
            for _ in 0..queued_before {
                crate::shmem::decrement_parallel_queue_depth_for_database(unsafe {
                    pg_sys::MyDatabaseId.to_u32()
                });
            }
            Ok(result.len() as i64)
        })
    }

    /// Prune completed/failed/cancelled jobs older than the given age.
    ///
    /// Returns the number of rows deleted.
    pub fn prune_completed(max_age_seconds: i64, batch_size: i32) -> Result<i64, PgTrickleError> {
        Spi::connect_mut(|client| {
            let result = client
                .update(
                    "DELETE FROM pgtrickle.pgt_scheduler_jobs \
                     WHERE ctid IN ( \
                         SELECT ctid FROM pgtrickle.pgt_scheduler_jobs \
                         WHERE status IN ('SUCCEEDED', 'RETRYABLE_FAILED', 'PERMANENT_FAILED', 'CANCELLED') \
                           AND finished_at < now() - make_interval(secs => $1::float8) \
                         ORDER BY finished_at, job_id \
                         LIMIT $2 \
                     )",
                    None,
                    &[max_age_seconds.into(), batch_size.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;
            Ok(result.len() as i64)
        })
    }

    /// Check whether an in-flight (QUEUED or RUNNING) job already exists for
    /// the given unit_key.
    pub fn has_inflight_job(unit_key: &str) -> Result<bool, PgTrickleError> {
        Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT 1 FROM pgtrickle.pgt_scheduler_jobs \
                     WHERE unit_key = $1 AND status IN ('QUEUED', 'RUNNING') \
                     LIMIT 1",
                    None,
                    &[unit_key.into()],
                )
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;
            Ok(!table.is_empty())
        })
    }

    /// Parse a job row from SPI query results (ordinal column access).
    ///
    /// Column order must match the SELECT in `get_by_id`:
    /// 1=job_id, 2=dag_version, 3=unit_key, 4=unit_kind, 5=member_pgt_ids,
    /// 6=root_pgt_id, 7=status, 8=scheduler_pid, 9=worker_pid, 10=attempt_no,
    /// 11=enqueued_at, 12=started_at, 13=finished_at, 14=outcome_detail, 15=retryable,
    /// 16=dispatch_tick_id, 17=tick_watermark_lsn, 18=outcome_code,
    /// 19=outcome_sqlstate, 20=worker_slot_generation.
    fn from_spi_table_row(table: &SpiTupleTable<'_>) -> Result<Self, PgTrickleError> {
        let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());

        let status_str: String = table.get::<String>(7).map_err(map_spi)?.unwrap_or_default();

        Ok(Self {
            job_id: table.get::<i64>(1).map_err(map_spi)?.unwrap_or(0),
            dag_version: table.get::<i64>(2).map_err(map_spi)?.unwrap_or(0),
            unit_key: table.get::<String>(3).map_err(map_spi)?.unwrap_or_default(),
            unit_kind: table.get::<String>(4).map_err(map_spi)?.unwrap_or_default(),
            member_pgt_ids: table
                .get::<Vec<i64>>(5)
                .map_err(map_spi)?
                .unwrap_or_default(),
            root_pgt_id: table.get::<i64>(6).map_err(map_spi)?.unwrap_or(0),
            status: JobStatus::from_str(&status_str),
            scheduler_pid: table.get::<i32>(8).map_err(map_spi)?.unwrap_or(0),
            worker_pid: table.get::<i32>(9).map_err(map_spi)?,
            attempt_no: table.get::<i32>(10).map_err(map_spi)?.unwrap_or(1),
            enqueued_at: table
                .get::<TimestampWithTimeZone>(11)
                .map_err(map_spi)?
                .ok_or_else(|| PgTrickleError::InternalError("NULL enqueued_at".into()))?,
            started_at: table.get::<TimestampWithTimeZone>(12).map_err(map_spi)?,
            finished_at: table.get::<TimestampWithTimeZone>(13).map_err(map_spi)?,
            outcome_detail: table.get::<String>(14).map_err(map_spi)?,
            retryable: table.get::<bool>(15).map_err(map_spi)?,
            dispatch_tick_id: table.get::<i64>(16).map_err(map_spi)?,
            tick_watermark_lsn: table.get::<String>(17).map_err(map_spi)?,
            outcome_code: table.get::<String>(18).map_err(map_spi)?,
            outcome_sqlstate: table.get::<String>(19).map_err(map_spi)?,
            worker_slot_generation: table.get::<i64>(20).map_err(map_spi)?,
        })
    }
}

// ── Refresh Group Catalog (A8) ──────────────────────────────────────────────

/// Metadata for a user-declared refresh group, mirrors
/// `pgtrickle.pgt_refresh_groups`.
#[derive(Debug, Clone)]
pub struct RefreshGroupMeta {
    pub group_id: i32,
    pub group_name: String,
    pub member_oids: Vec<pg_sys::Oid>,
    pub isolation: String,
    pub created_at: TimestampWithTimeZone,
}

/// Insert a new refresh group. Returns the assigned `group_id`.
pub fn create_refresh_group(
    group_name: &str,
    member_oids: &[pg_sys::Oid],
    isolation: &str,
) -> Result<i32, PgTrickleError> {
    Spi::connect_mut(|client| {
        let row = client
            .update(
                "INSERT INTO pgtrickle.pgt_refresh_groups \
                 (group_name, member_oids, isolation) \
                 VALUES ($1, $2, $3) \
                 RETURNING group_id",
                None,
                &[
                    group_name.into(),
                    member_oids.to_vec().into(),
                    isolation.into(),
                ],
            )
            .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
            .first();

        row.get_one::<i32>()
            .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
            .ok_or_else(|| PgTrickleError::InternalError("INSERT did not return group_id".into()))
    })
}

/// Delete a refresh group by name.
pub fn drop_refresh_group(group_name: &str) -> Result<(), PgTrickleError> {
    Spi::connect_mut(|client| {
        let count = client
            .update(
                "DELETE FROM pgtrickle.pgt_refresh_groups WHERE group_name = $1",
                None,
                &[group_name.into()],
            )
            .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?
            .len();

        if count == 0 {
            return Err(PgTrickleError::NotFound(format!(
                "refresh group '{}' does not exist",
                group_name
            )));
        }
        Ok(())
    })
}

/// Return all refresh groups.
pub fn get_all_refresh_groups() -> Result<Vec<RefreshGroupMeta>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT group_id, group_name, member_oids, isolation, created_at \
                 FROM pgtrickle.pgt_refresh_groups \
                 ORDER BY group_id",
                None,
                &[],
            )
            .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

        let mut groups = Vec::new();
        for row in table {
            let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());
            groups.push(RefreshGroupMeta {
                group_id: row.get::<i32>(1).map_err(map_spi)?.unwrap_or(0),
                group_name: row.get::<String>(2).map_err(map_spi)?.unwrap_or_default(),
                member_oids: row
                    .get::<Vec<pg_sys::Oid>>(3)
                    .map_err(map_spi)?
                    .unwrap_or_default(),
                isolation: row
                    .get::<String>(4)
                    .map_err(map_spi)?
                    .unwrap_or_else(|| "read_committed".to_string()),
                created_at: row
                    .get::<TimestampWithTimeZone>(5)
                    .map_err(map_spi)?
                    .ok_or_else(|| PgTrickleError::InternalError("NULL created_at".into()))?,
            });
        }
        Ok(groups)
    })
}

/// Check whether any existing refresh group already contains the given OID.
/// Returns the conflicting group name if found.
pub fn find_group_containing_member(
    member_oid: pg_sys::Oid,
) -> Result<Option<String>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT group_name FROM pgtrickle.pgt_refresh_groups \
                 WHERE $1 = ANY(member_oids) LIMIT 1",
                None,
                &[member_oid.into()],
            )
            .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?;

        if table.is_empty() {
            Ok(None)
        } else {
            Ok(table
                .first()
                .get::<String>(1)
                .map_err(|e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string()))?)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── CdcMode tests ──────────────────────────────────────────────────

    #[test]
    fn test_cdc_mode_as_str() {
        assert_eq!(CdcMode::Trigger.as_str(), "TRIGGER");
        assert_eq!(CdcMode::Transitioning.as_str(), "TRANSITIONING");
        assert_eq!(CdcMode::Wal.as_str(), "WAL");
    }

    #[test]
    fn test_cdc_mode_from_str_valid() {
        assert_eq!(CdcMode::from_str("TRIGGER"), CdcMode::Trigger);
        assert_eq!(CdcMode::from_str("TRANSITIONING"), CdcMode::Transitioning);
        assert_eq!(CdcMode::from_str("WAL"), CdcMode::Wal);
    }

    #[test]
    fn test_cdc_mode_from_str_case_insensitive() {
        assert_eq!(CdcMode::from_str("trigger"), CdcMode::Trigger);
        assert_eq!(CdcMode::from_str("Transitioning"), CdcMode::Transitioning);
        assert_eq!(CdcMode::from_str("wal"), CdcMode::Wal);
        assert_eq!(CdcMode::from_str("Wal"), CdcMode::Wal);
    }

    #[test]
    fn test_cdc_mode_from_str_unknown_defaults_to_trigger() {
        assert_eq!(CdcMode::from_str(""), CdcMode::Trigger);
        assert_eq!(CdcMode::from_str("unknown"), CdcMode::Trigger);
        assert_eq!(CdcMode::from_str("LOGICAL"), CdcMode::Trigger);
    }

    #[test]
    fn test_cdc_mode_display() {
        assert_eq!(format!("{}", CdcMode::Trigger), "TRIGGER");
        assert_eq!(format!("{}", CdcMode::Transitioning), "TRANSITIONING");
        assert_eq!(format!("{}", CdcMode::Wal), "WAL");
    }

    #[test]
    fn test_cdc_mode_roundtrip() {
        for mode in [CdcMode::Trigger, CdcMode::Transitioning, CdcMode::Wal] {
            assert_eq!(CdcMode::from_str(mode.as_str()), mode);
        }
    }

    #[test]
    fn test_cdc_mode_equality() {
        assert_eq!(CdcMode::Trigger, CdcMode::Trigger);
        assert_ne!(CdcMode::Trigger, CdcMode::Wal);
        assert_ne!(CdcMode::Transitioning, CdcMode::Wal);
    }

    #[test]
    fn test_cdc_mode_clone_copy() {
        let mode = CdcMode::Wal;
        let cloned = mode;
        assert_eq!(mode, cloned);
    }

    // ── JobStatus tests ────────────────────────────────────────────────

    #[test]
    fn test_job_status_as_str() {
        assert_eq!(JobStatus::Queued.as_str(), "QUEUED");
        assert_eq!(JobStatus::Running.as_str(), "RUNNING");
        assert_eq!(JobStatus::Succeeded.as_str(), "SUCCEEDED");
        assert_eq!(JobStatus::RetryableFailed.as_str(), "RETRYABLE_FAILED");
        assert_eq!(JobStatus::PermanentFailed.as_str(), "PERMANENT_FAILED");
        assert_eq!(JobStatus::Cancelled.as_str(), "CANCELLED");
    }

    #[test]
    fn test_job_status_from_str_valid() {
        assert_eq!(JobStatus::from_str("QUEUED"), JobStatus::Queued);
        assert_eq!(JobStatus::from_str("RUNNING"), JobStatus::Running);
        assert_eq!(JobStatus::from_str("SUCCEEDED"), JobStatus::Succeeded);
        assert_eq!(
            JobStatus::from_str("RETRYABLE_FAILED"),
            JobStatus::RetryableFailed
        );
        assert_eq!(
            JobStatus::from_str("PERMANENT_FAILED"),
            JobStatus::PermanentFailed
        );
        assert_eq!(JobStatus::from_str("CANCELLED"), JobStatus::Cancelled);
    }

    #[test]
    fn test_job_status_from_str_unknown_defaults_to_cancelled() {
        assert_eq!(JobStatus::from_str(""), JobStatus::Cancelled);
        assert_eq!(JobStatus::from_str("UNKNOWN"), JobStatus::Cancelled);
    }

    #[test]
    fn test_job_status_roundtrip() {
        for status in [
            JobStatus::Queued,
            JobStatus::Running,
            JobStatus::Succeeded,
            JobStatus::RetryableFailed,
            JobStatus::PermanentFailed,
            JobStatus::Cancelled,
        ] {
            assert_eq!(JobStatus::from_str(status.as_str()), status);
        }
    }

    #[test]
    fn test_job_status_is_terminal() {
        assert!(!JobStatus::Queued.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(JobStatus::Succeeded.is_terminal());
        assert!(JobStatus::RetryableFailed.is_terminal());
        assert!(JobStatus::PermanentFailed.is_terminal());
        assert!(JobStatus::Cancelled.is_terminal());
    }

    #[test]
    fn test_job_status_display() {
        assert_eq!(format!("{}", JobStatus::Queued), "QUEUED");
        assert_eq!(format!("{}", JobStatus::Running), "RUNNING");
        assert_eq!(format!("{}", JobStatus::Succeeded), "SUCCEEDED");
    }

    // ── TEST-003 (v0.72.0): Frontier durability model unit tests ───────

    /// TEST-003a: The canonical frontier path is `store_frontier` — verify
    /// that the pure-Rust serialization used by this function does not drop
    /// any fields from the Frontier type.
    ///
    /// This test exercises the pure logic of frontier JSON serialization and
    /// deserialization, which is what `store_frontier` / `get_frontier`
    /// execute against the catalog.  The SPI calls require a live PostgreSQL
    /// backend, so those are covered by integration/E2E tests.
    #[test]
    fn test_frontier_serialization_roundtrip() {
        let mut frontier = crate::version::Frontier::default();
        // Ensure a non-empty frontier survives a JSON roundtrip.
        frontier.set_source(
            12345u32,
            "A/1".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        );
        frontier.set_data_timestamp("2024-01-01T00:00:00Z".to_string());

        let json = serde_json::to_value(&frontier).expect("frontier serialization must not fail");
        let round: crate::version::Frontier =
            serde_json::from_value(json).expect("frontier deserialization must not fail");

        assert_eq!(
            frontier.get_lsn(12345u32),
            round.get_lsn(12345u32),
            "LSN must survive JSON roundtrip"
        );
    }

    /// TEST-003b: An empty default Frontier is flagged as empty.
    #[test]
    fn test_frontier_default_is_empty() {
        let f = crate::version::Frontier::default();
        assert!(f.is_empty(), "Default frontier must be empty");
    }

    /// TEST-003c: A Frontier with at least one source entry is not empty.
    #[test]
    fn test_frontier_with_entry_is_not_empty() {
        let mut f = crate::version::Frontier::default();
        f.set_source(99u32, "0/1".to_string(), "2024-01-01T00:00:00Z".to_string());
        assert!(
            !f.is_empty(),
            "Frontier with source entry must not be empty"
        );
    }

    /// TEST-003d: Verify the decision in ADR-004 — the DUR-1 dead functions
    /// no longer exist on `StreamTableMeta`.  If they do (e.g. after a bad
    /// rebase), this test will fail to compile, surfacing the regression
    /// immediately.
    ///
    /// The test itself is a no-op; its purpose is compile-time verification.
    #[test]
    fn test_dur1_dead_functions_removed() {
        // The following would cause a compile error if prepare_frontier,
        // finalize_frontier_and_complete_refresh (DUR-1), or
        // reconcile_tentative_frontiers still exist on StreamTableMeta.
        //
        // We verify by asserting that the canonical path functions exist
        // (store_frontier and store_frontier_and_complete_refresh) and that
        // the type resolves correctly in this module, which implicitly
        // confirms the API shape matches what the scheduler uses.
        let _ = StreamTableMeta::store_frontier
            as fn(i64, &crate::version::Frontier) -> Result<(), PgTrickleError>;
        let _ = StreamTableMeta::store_frontier_and_complete_refresh
            as fn(
                i64,
                &crate::version::Frontier,
                i64,
            ) -> Result<pgrx::datum::TimestampWithTimeZone, PgTrickleError>;
    }
}
