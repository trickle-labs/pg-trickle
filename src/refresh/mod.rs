//! Refresh executor — handles full, differential, and reinitialize refreshes.
//!
//! The executor is called by the scheduler for automated refreshes and by
//! `pgtrickle.refresh_stream_table()` for manual refreshes.
//!
//! ## Delta SQL Caching
//!
//! The differential refresh path caches the delta SQL template and MERGE
//! SQL template per `pgt_id` in thread-local storage. On subsequent
//! refreshes, the cached templates are resolved with actual frontier LSN
//! values — skipping SQL parsing, DVM differentiation, and MERGE SQL
//! string formatting. This eliminates ~45ms of overhead per refresh
//! (29.6ms planning + 15ms generate_delta).
//!
//! ## ARCH-1B: Module structure
//!
//! - [`orchestrator`] — RefreshAction, determine_refresh_action, adaptive cost model,
//!   execute_reinitialize_refresh
//! - [`codegen`]      — SQL template builders, MERGE SQL cache, planner hints,
//!   change-buffer cleanup, ST-to-ST delta capture
//! - [`merge`]        — execute_differential_refresh, execute_full_refresh,
//!   execute_topk_refresh, execute_no_data_refresh,
//!   partition-aware MERGE helpers
//! - [`phd1`]         — PH-D1 phantom-cleanup DELETE+INSERT strategy,
//!   cross-cycle phantom cleanup (EC01-2)

use crate::catalog::{RefreshRecord, StreamTableMeta};
use crate::error::PgTrickleError;
use pgrx::{Spi, prelude::TimestampWithTimeZone};

pub(crate) mod codegen;
pub(crate) mod delta_stage;
pub(crate) mod fused;
pub(crate) mod merge;
pub(crate) mod orchestrator;
pub(crate) mod phd1;
pub(crate) mod pipeline;
pub(crate) mod sql_fragments;

// SCAL-2 (v0.30.0): Explicit re-export lists enforce module boundary discipline.
// Adding a new public symbol to a sub-module no longer silently promotes it;
// each export is intentional and visible at a glance.
// SCAL-2 (v0.30.0): Make the external public API explicit so that callers
// outside this crate see a well-defined surface.  Crate-internal access
// (including tests via `use super::*`) is preserved via `pub(crate) use *`
// globs; the named `pub use` lines promote specific items to fully public.
//
// Precedence: explicit `pub use` overrides the `pub(crate) use *` glob for
// the same name, giving those items public (not just crate) visibility.
pub(crate) use codegen::*;
pub(crate) use fused::{NodeSpec, fuse_diff_batch};
pub(crate) use orchestrator::*;

// ── External public API surface ─────────────────────────────────────────
// Only items that are actually imported via the `crate::refresh::*` path
// (rather than the full `crate::refresh::codegen::*` path) need to appear here.
pub use codegen::{
    capture_delta_to_bypass_table, clear_all_st_bypass, clear_fallback_leaf_oids,
    flush_local_template_cache, flush_pending_cleanups_for_oids, get_fallback_leaf_oids,
    get_st_bypass_tables, get_st_user_columns, get_st_user_columns_typed,
    has_downstream_st_consumers, has_template_cache_entry, invalidate_merge_cache,
    prewarm_merge_cache, set_fallback_leaf_oids,
};
pub(crate) use merge::compute_amplification_ratio;
pub use merge::{
    execute_differential_refresh, execute_differential_refresh_with_tuning, execute_full_refresh,
    execute_no_data_refresh, execute_topk_refresh, poll_foreign_table_sources_for_st,
    post_full_refresh_cleanup,
};
pub use orchestrator::{
    RefreshAction, determine_refresh_action, execute_reinitialize_refresh, validate_topk_metadata,
};

/// Run definition-derived SQL with the stream storage owner's role, stored
/// search path, and RLS policy. Privileged callers resume their original
/// identity before refresh metadata or private CDC state is finalized.
pub(crate) fn with_stream_owner<T>(
    st: &StreamTableMeta,
    f: impl FnOnce() -> Result<T, PgTrickleError>,
) -> Result<T, PgTrickleError> {
    let context = crate::api::security_context::stream_execution_context(st)?;
    crate::api::security_context::with_stream_owner_context(&context, f)
}

pub(crate) fn stream_owner_name(st: &StreamTableMeta) -> Result<String, PgTrickleError> {
    let context = crate::api::security_context::stream_execution_context(st)?;
    Spi::get_one_with_args::<String>(
        "SELECT rolname::text FROM pg_catalog.pg_roles WHERE oid = $1",
        &[context.owner_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InternalError(format!(
            "stream owner OID {} is missing",
            context.owner_oid.to_u32()
        ))
    })
}

pub(crate) fn source_visibility_key(
    source_oid: pgrx::pg_sys::Oid,
) -> Result<(String, Vec<String>), PgTrickleError> {
    let source_table = Spi::get_one_with_args::<String>(
        "SELECT format('%I.%I', n.nspname, c.relname) \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = $1",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::NotFound(format!("source relation with OID {}", source_oid.to_u32()))
    })?;
    let columns = if StreamTableMeta::pgt_id_for_relid(source_oid).is_some() {
        crate::cdc::resolve_st_output_columns(source_oid)?
    } else {
        let pk_columns = crate::cdc::resolve_pk_columns(source_oid)?;
        if pk_columns.is_empty() {
            // Keyless trigger and polling CDC hashes every source column.
            // Visibility checks must use the identical key, even when the
            // stream definition references only a subset of those columns.
            crate::cdc::resolve_source_column_defs(source_oid)?
        } else {
            pk_columns
                .into_iter()
                .map(|column| (column, String::new()))
                .collect()
        }
    }
    .into_iter()
    .map(|(column, _)| column)
    .collect();
    Ok((source_table, columns))
}

/// Create a `pg_temp`-qualified scratch table and return its qualified name
/// for callers to use in later SQL.
///
/// `basename` is a raw, unquoted internal identifier (e.g.
/// `"__pgt_delta_42"`) — this function is the only place that qualifies it
/// with `pg_temp` and quotes it, so an unqualified bare name can never
/// resolve to a same-named permanent relation on the search path (e.g. one
/// planted in `public` ahead of a `SECURITY DEFINER` trigger call).
///
/// Runs under whichever identity the caller is currently running as. When
/// `select_sql` is definition-derived (may reference owner-schema
/// functions, operators, casts, or relations that only resolve correctly
/// under the owner's stored `search_path`), the caller must wrap this call
/// in [`with_stream_owner`] itself — Postgres performs full parse analysis
/// for `WITH NO DATA` even though it skips rewrite/execution, so name
/// resolution during the `CREATE` must match the identity that later reads
/// the same SQL text. When `select_sql` only references the stream table's
/// own fully-qualified storage relation (no ambiguous unqualified names),
/// callers that need the table readable by privileged bookkeeping code
/// afterward (e.g. downstream-diff capture) should call this un-wrapped, as
/// before — the unconditional `GRANT` below still lets owner-context code
/// read/write it too.
pub(crate) fn prepare_owner_temp_table(
    st: &StreamTableMeta,
    basename: &str,
    select_sql: &str,
) -> Result<String, PgTrickleError> {
    let table = format!("pg_temp.{}", crate::sql_builder::ident(basename));
    Spi::run(&format!("DROP TABLE IF EXISTS {table}")) // nosemgrep: rust.spi.run.dynamic-format — table is pg_temp-qualified and quoted by this function.
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    // nosemgrep: rust.spi.run.dynamic-format — table is pg_temp-qualified and quoted by this function; select_sql is generated internally.
    Spi::run(&format!(
        "CREATE TEMP TABLE {table} ON COMMIT DROP AS {select_sql} WITH NO DATA"
    ))
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    let owner = crate::sql_builder::ident(&stream_owner_name(st)?);
    // nosemgrep: rust.spi.run.dynamic-format — table and owner are quoted identifiers.
    Spi::run(&format!(
        "GRANT SELECT, INSERT, TRUNCATE ON TABLE {table} TO {owner}"
    ))
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Ok(table)
}

/// Drop a scratch table previously created by [`prepare_owner_temp_table`].
/// `basename` must be the same raw basename passed to that call — this
/// function re-derives the identical `pg_temp`-qualified name and drops it
/// under the owner's identity, since the owner is what created (and thus
/// owns) it.
pub(crate) fn drop_owner_temp_table(st: &StreamTableMeta, basename: &str) {
    let table = format!("pg_temp.{}", crate::sql_builder::ident(basename));
    let _ = with_stream_owner(st, || {
        Spi::run(&format!("DROP TABLE IF EXISTS {table}")) // nosemgrep: rust.spi.run.dynamic-format — table is pg_temp-qualified and quoted by this function.
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    });
}
// phd1: cross-cycle phantom cleanup (CORR-1, deferred — see merge.rs).

/// Stable machine-readable causes for FULL/recomputation refreshes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullRefreshReasonCode {
    FirstRefresh,
    ConfiguredFull,
    AutoQueryFullOnly,
    DeltaRatioExceeded,
    CostModelPreferredFull,
    CorrelatedSubqueryDeltaQuadratic,
    CaseInListDvmDriftFullFallback,
    RegexComplexityClassifierUncertain,
    SourceTruncated,
    SchemaChanged,
    FunctionChanged,
    RowIdentityUpgrade,
    CdcStateRecovery,
    FrontierMissing,
    ForceFullRefresh,
    BufferRowLimitExceeded,
    JoinLimitExceeded,
    AggregateSaturation,
    DeltaEstimateExceeded,
    WorkMemCapExceeded,
    TempSpillThresholdExceeded,
    RecursiveCteRecompute,
    SccFixpointRecompute,
    TopKRecompute,
    ManualImmediateRebuild,
    StreamTableSourceManualRebuild,
}

impl FullRefreshReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FirstRefresh => "FIRST_REFRESH",
            Self::ConfiguredFull => "CONFIGURED_FULL",
            Self::AutoQueryFullOnly => "AUTO_QUERY_FULL_ONLY",
            Self::DeltaRatioExceeded => "DELTA_RATIO_EXCEEDED",
            Self::CostModelPreferredFull => "COST_MODEL_PREFERRED_FULL",
            Self::CorrelatedSubqueryDeltaQuadratic => "CORRELATED_SUBQUERY_DELTA_QUADRATIC",
            Self::CaseInListDvmDriftFullFallback => "CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK",
            Self::RegexComplexityClassifierUncertain => "REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN",
            Self::SourceTruncated => "SOURCE_TRUNCATED",
            Self::SchemaChanged => "SCHEMA_CHANGED",
            Self::FunctionChanged => "FUNCTION_CHANGED",
            Self::RowIdentityUpgrade => "ROW_IDENTITY_UPGRADE",
            Self::CdcStateRecovery => "CDC_STATE_RECOVERY",
            Self::FrontierMissing => "FRONTIER_MISSING",
            Self::ForceFullRefresh => "FORCE_FULL_REFRESH",
            Self::BufferRowLimitExceeded => "BUFFER_ROW_LIMIT_EXCEEDED",
            Self::JoinLimitExceeded => "JOIN_LIMIT_EXCEEDED",
            Self::AggregateSaturation => "AGGREGATE_SATURATION",
            Self::DeltaEstimateExceeded => "DELTA_ESTIMATE_EXCEEDED",
            Self::WorkMemCapExceeded => "WORK_MEM_CAP_EXCEEDED",
            Self::TempSpillThresholdExceeded => "TEMP_SPILL_THRESHOLD_EXCEEDED",
            Self::RecursiveCteRecompute => "RECURSIVE_CTE_RECOMPUTE",
            Self::SccFixpointRecompute => "SCC_FIXPOINT_RECOMPUTE",
            Self::TopKRecompute => "TOP_K_RECOMPUTE",
            Self::ManualImmediateRebuild => "MANUAL_IMMEDIATE_REBUILD",
            Self::StreamTableSourceManualRebuild => "STREAM_TABLE_SOURCE_MANUAL_REBUILD",
        }
    }

    pub fn from_str(code: &str) -> Option<Self> {
        Some(match code {
            "FIRST_REFRESH" => Self::FirstRefresh,
            "CONFIGURED_FULL" => Self::ConfiguredFull,
            "AUTO_QUERY_FULL_ONLY" => Self::AutoQueryFullOnly,
            "DELTA_RATIO_EXCEEDED" => Self::DeltaRatioExceeded,
            "COST_MODEL_PREFERRED_FULL" => Self::CostModelPreferredFull,
            "CORRELATED_SUBQUERY_DELTA_QUADRATIC" => Self::CorrelatedSubqueryDeltaQuadratic,
            "CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK" => Self::CaseInListDvmDriftFullFallback,
            "REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN" => Self::RegexComplexityClassifierUncertain,
            "SOURCE_TRUNCATED" => Self::SourceTruncated,
            "SCHEMA_CHANGED" => Self::SchemaChanged,
            "FUNCTION_CHANGED" => Self::FunctionChanged,
            "ROW_IDENTITY_UPGRADE" => Self::RowIdentityUpgrade,
            "CDC_STATE_RECOVERY" => Self::CdcStateRecovery,
            "FRONTIER_MISSING" => Self::FrontierMissing,
            "FORCE_FULL_REFRESH" => Self::ForceFullRefresh,
            "BUFFER_ROW_LIMIT_EXCEEDED" => Self::BufferRowLimitExceeded,
            "JOIN_LIMIT_EXCEEDED" => Self::JoinLimitExceeded,
            "AGGREGATE_SATURATION" => Self::AggregateSaturation,
            "DELTA_ESTIMATE_EXCEEDED" => Self::DeltaEstimateExceeded,
            "WORK_MEM_CAP_EXCEEDED" => Self::WorkMemCapExceeded,
            "TEMP_SPILL_THRESHOLD_EXCEEDED" => Self::TempSpillThresholdExceeded,
            "RECURSIVE_CTE_RECOMPUTE" => Self::RecursiveCteRecompute,
            "SCC_FIXPOINT_RECOMPUTE" => Self::SccFixpointRecompute,
            "TOP_K_RECOMPUTE" => Self::TopKRecompute,
            "MANUAL_IMMEDIATE_REBUILD" => Self::ManualImmediateRebuild,
            "STREAM_TABLE_SOURCE_MANUAL_REBUILD" => Self::StreamTableSourceManualRebuild,
            _ => return None,
        })
    }
}

/// A typed, durable FULL-refresh reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullRefreshReason {
    pub code: FullRefreshReasonCode,
    pub detail: String,
}

impl FullRefreshReason {
    pub fn new(code: FullRefreshReasonCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub fn from_catalog(code: Option<String>, detail: Option<String>) -> Option<Self> {
        Some(Self::new(
            FullRefreshReasonCode::from_str(code.as_deref()?)?,
            detail.unwrap_or_default(),
        ))
    }

    pub fn for_action(action: RefreshAction, first_refresh: bool) -> Option<Self> {
        match action {
            RefreshAction::Full | RefreshAction::Reinitialize if first_refresh => Some(Self::new(
                FullRefreshReasonCode::FirstRefresh,
                "The stream table has not been populated yet.",
            )),
            RefreshAction::Full => Some(Self::new(
                FullRefreshReasonCode::ConfiguredFull,
                "FULL refresh was selected by the configured refresh mode.",
            )),
            RefreshAction::Reinitialize => Some(Self::new(
                FullRefreshReasonCode::SchemaChanged,
                "A rebuild was requested because the stored stream-table definition changed.",
            )),
            _ => None,
        }
    }
}

/// The durable result of a refresh executor before catalog finalization.
///
/// Executors are allowed to mutate only the target and CDC buffers.  Callers
/// pass this record to [`finalize_success`] so frontier, metadata, history,
/// and notifications are committed as one unit.
#[derive(Debug, Clone)]
pub struct RefreshExecution {
    pub requested_action: RefreshAction,
    pub effective_action: RefreshAction,
    pub frontier: crate::version::Frontier,
    pub rows_inserted: i64,
    pub rows_updated: i64,
    pub rows_deleted: i64,
    pub data_changed: bool,
    pub was_full_fallback: bool,
    pub full_reason: Option<FullRefreshReason>,
    pub downstream_capture_complete: bool,
}

/// Finalize a successful refresh in the caller's existing transaction.
///
/// Required durable operations deliberately propagate errors.  A failed
/// history, frontier, or outbox write therefore aborts the transaction rather
/// than reporting a partially finalized refresh.
#[allow(clippy::too_many_arguments)]
pub fn finalize_success(
    st: &StreamTableMeta,
    execution: &RefreshExecution,
    refresh_id: i64,
    data_timestamp: TimestampWithTimeZone,
    schema: &str,
    table_name: &str,
) -> Result<(), PgTrickleError> {
    if !execution.downstream_capture_complete {
        return Err(PgTrickleError::RefreshFinalizationFailed {
            pgt_id: st.pgt_id,
            stage: "downstream CDC capture".to_string(),
            reason: "executor did not prove downstream capture completion".to_string(),
        });
    }

    StreamTableMeta::store_frontier(st.pgt_id, &execution.frontier)?;
    if execution.data_changed {
        StreamTableMeta::update_after_refresh(
            st.pgt_id,
            data_timestamp,
            execution.rows_inserted + execution.rows_updated,
        )?;
    } else {
        StreamTableMeta::update_after_no_data_refresh(st.pgt_id)?;
    }

    let effective_mode = take_effective_mode();
    let history_action = match effective_mode {
        "FULL" => RefreshAction::Full,
        "NO_DATA" => RefreshAction::NoData,
        _ => execution.effective_action,
    };
    let catalog_reason = if execution.full_reason.is_none()
        && matches!(
            history_action,
            RefreshAction::Full | RefreshAction::Reinitialize
        ) {
        Spi::get_one_with_args::<String>(
            "SELECT refresh_reason FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
            &[st.pgt_id.into()],
        )
        .unwrap_or(None)
        .and_then(|code| {
            let detail = Spi::get_one_with_args::<String>(
                "SELECT refresh_reason_detail FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
                &[st.pgt_id.into()],
            )
            .unwrap_or(None);
            FullRefreshReason::from_catalog(Some(code), detail)
        })
    } else {
        None
    };
    let derived_reason = execution
        .full_reason
        .clone()
        .or(catalog_reason)
        .or_else(|| FullRefreshReason::for_action(history_action, !st.is_populated));
    let full_reason = derived_reason.as_ref();
    if matches!(
        history_action,
        RefreshAction::Full | RefreshAction::Reinitialize
    ) && full_reason.is_none()
    {
        return Err(PgTrickleError::RefreshFinalizationFailed {
            pgt_id: st.pgt_id,
            stage: "refresh reason".to_string(),
            reason: "FULL refresh completed without a typed reason".to_string(),
        });
    }
    RefreshRecord::complete_with_rows_updated_and_reason(
        refresh_id,
        "COMPLETED",
        execution.rows_inserted,
        execution.rows_updated,
        execution.rows_deleted,
        None,
        execution.rows_inserted + execution.rows_updated + execution.rows_deleted,
        Some(history_action.as_str()),
        execution.was_full_fallback,
        full_reason,
    )?;

    if let Some(reason) = full_reason {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables
                SET refresh_reason = $1, refresh_reason_detail = $2, updated_at = now()
              WHERE pgt_id = $3",
            &[
                reason.code.as_str().into(),
                reason.detail.as_str().into(),
                st.pgt_id.into(),
            ],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    if !effective_mode.is_empty() {
        StreamTableMeta::update_effective_refresh_mode(st.pgt_id, effective_mode)?;
    }

    let rows_changed = execution.rows_inserted + execution.rows_updated + execution.rows_deleted;
    if rows_changed > 0 && crate::api::outbox::is_outbox_enabled(st.pgt_id) {
        if let Some(vector_column) = crate::api::outbox::get_embedding_vector_column(st.pgt_id) {
            crate::api::outbox::write_embedding_outbox_row(
                st.pgt_id,
                None,
                execution.rows_inserted,
                execution.rows_updated,
                execution.rows_deleted,
                schema,
                table_name,
                &vector_column,
            )
            .map_err(|e| PgTrickleError::RefreshFinalizationFailed {
                pgt_id: st.pgt_id,
                stage: "embedding outbox".to_string(),
                reason: e.to_string(),
            })?;
        } else {
            crate::api::outbox::write_outbox_row(
                st.pgt_id,
                None,
                execution.rows_inserted,
                execution.rows_updated,
                execution.rows_deleted,
                0,
                schema,
                table_name,
            )
            .map_err(|e| PgTrickleError::RefreshFinalizationFailed {
                pgt_id: st.pgt_id,
                stage: "outbox".to_string(),
                reason: e.to_string(),
            })?;
        }
    }

    if rows_changed > 0 && execution.effective_action != RefreshAction::NoData {
        crate::api::fire_distance_subscriptions(
            schema,
            table_name,
            table_name,
            st.pooler_compatibility_mode,
        );
        crate::scheduler::execute_post_refresh_action(st, rows_changed);
    }

    if matches!(
        execution.effective_action,
        RefreshAction::Full | RefreshAction::Reinitialize
    ) {
        post_full_refresh_cleanup(st);
    }

    Ok(())
}

use std::cell::Cell;

// ── B-4: Query complexity classification ────────────────────────────────

/// Complexity class for a stream table's defining query.
///
/// Used by the cost model to apply per-class cost coefficients.  Higher
/// complexity classes have steeper differential cost curves (more joins /
/// aggregates → more work per delta row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryComplexityClass {
    /// Simple scan: `SELECT cols FROM single_table`
    Scan,
    /// Scan with filter: `SELECT cols FROM single_table WHERE ...`
    Filter,
    /// Aggregate: `SELECT ... GROUP BY ...` (no joins)
    Aggregate,
    /// Join(s) without aggregation
    Join,
    /// Join(s) with GROUP BY aggregation (most expensive differential path)
    JoinAggregate,
}

impl QueryComplexityClass {
    /// Default differential cost scaling factor per class.
    ///
    /// The factor represents the per-delta-row cost multiplier relative to
    /// a plain scan.  Joins and aggregates make each delta row more
    /// expensive to process incrementally.
    pub(crate) fn diff_cost_factor(self) -> f64 {
        match self {
            Self::Scan => 1.0,
            Self::Filter => 1.1,
            Self::Aggregate => 1.5,
            Self::Join => 2.5,
            Self::JoinAggregate => 4.0,
        }
    }
}

/// Classify a defining query's complexity from its SQL text using the OpTree
/// parser for accurate scan-count analysis.
///
/// P-2 (v0.78.0): This is the "deep" classifier that actually parses the
/// query AST.  It uses `dvm::query_total_scan_count()` and
/// `dvm::query_has_join()` to detect joins and aggregate structure.
/// On parse failure it falls back to the lightweight keyword classifier.
///
/// Returns a static string label suitable for storage in the catalog.
pub(crate) fn classify_query_complexity_optree(defining_query: &str) -> &'static str {
    // Try the OpTree path first (accurate, requires SPI).
    let has_join = crate::dvm::query_has_join(defining_query).unwrap_or(false);
    let has_group_by = defining_query.to_ascii_uppercase().contains("GROUP BY");

    match (has_join, has_group_by) {
        (true, true) => "JoinAggregate",
        (true, false) => "Join",
        (false, true) => "Aggregate",
        (false, false) => {
            if defining_query.to_ascii_uppercase().contains(" WHERE ") {
                "Filter"
            } else {
                "Scan"
            }
        }
    }
}

/// Classify a defining query's complexity from its SQL text.
///
/// Uses lightweight keyword analysis (no parsing or SPI).  This is
/// intentionally conservative: false positives (over-classifying) are
/// preferable to false negatives because a higher class merely biases
/// the cost model toward FULL at lower change rates, which is always safe.
pub(crate) fn classify_query_complexity(defining_query: &str) -> QueryComplexityClass {
    let upper = defining_query.to_ascii_uppercase();
    let has_join = upper.contains(" JOIN ")
        || upper.contains(" INNER JOIN ")
        || upper.contains(" LEFT JOIN ")
        || upper.contains(" RIGHT JOIN ")
        || upper.contains(" FULL JOIN ")
        || upper.contains(" CROSS JOIN ");
    let has_group_by = upper.contains("GROUP BY");

    match (has_join, has_group_by) {
        (true, true) => QueryComplexityClass::JoinAggregate,
        (true, false) => QueryComplexityClass::Join,
        (false, true) => QueryComplexityClass::Aggregate,
        (false, false) => {
            if upper.contains(" WHERE ") {
                QueryComplexityClass::Filter
            } else {
                QueryComplexityClass::Scan
            }
        }
    }
}

/// C-3/DVM-1: Detect CASE aggregate with IN-list WHERE predicate (q12-like).
///
/// Queries with this pattern have known DVM drift: the incremental delta
/// for CASE aggregates combined with IN-list predicates in WHERE can produce
/// non-deterministic results. Force FULL refresh until the root-cause
/// delta rule is fixed in v0.78.0.
///
/// Detection criteria: query has both a CASE expression inside an aggregate
/// function (SUM or COUNT) AND an IN-list predicate with string literals.
pub(crate) fn classify_case_in_list_aggregate_drift(defining_query: &str) -> bool {
    let upper = defining_query.to_ascii_uppercase();
    // Aggregate function containing a CASE expression
    let has_agg_case = upper.contains("SUM(CASE")
        || upper.contains("SUM( CASE")
        || upper.contains("COUNT(CASE")
        || upper.contains("COUNT( CASE");
    // IN-list predicate with string literals (e.g. col IN ('A', 'B'))
    let has_in_list = upper.contains("IN ('") || upper.contains("IN ( '");
    has_agg_case && has_in_list
}

/// O-1 (v0.80.0): Detect CASE aggregate with subquery predicate — classifier uncertain.
///
/// When a CASE expression inside an aggregate contains a scalar subquery or
/// EXISTS predicate (e.g. `SUM(CASE WHEN (SELECT ...) > 0 THEN 1 ELSE 0 END)`),
/// the algebraic delta safety cannot be confirmed by string analysis alone.
/// The pattern is outside the two definitive rejection rules (DVM-1, DVM-2) but
/// is complex enough that the regex-based classifier cannot guarantee correctness.
/// Force FULL refresh as a conservative safety measure.
pub(crate) fn classify_case_aggregate_subquery_uncertain(defining_query: &str) -> bool {
    let upper = defining_query.to_ascii_uppercase();
    // CASE inside any aggregate function
    let has_agg_case = upper.contains("SUM(CASE")
        || upper.contains("SUM( CASE")
        || upper.contains("COUNT(CASE")
        || upper.contains("COUNT( CASE")
        || upper.contains("AVG(CASE")
        || upper.contains("AVG( CASE")
        || upper.contains("MAX(CASE")
        || upper.contains("MAX( CASE")
        || upper.contains("MIN(CASE")
        || upper.contains("MIN( CASE");
    if !has_agg_case {
        return false;
    }
    // The CASE predicate itself contains a scalar subquery or EXISTS
    upper.contains("WHEN (SELECT")
        || upper.contains("WHEN(SELECT")
        || upper.contains("WHEN EXISTS")
        || upper.contains("WHEN NOT EXISTS")
}

/// DVM-2/P-1: Detect correlated aggregate scalar subquery in WHERE (q20-like).
///
/// Queries with this pattern produce O(delta × table) DVM delta SQL because
/// the inner aggregate subquery is re-evaluated for every changed row in the
/// CDC delta. Force FULL refresh until the pre-aggregation CTE rewrite is
/// implemented in v0.78.0.
///
/// Detection criteria: query has a comparison operator directly followed by
/// a scalar subquery that contains an aggregate function.
pub(crate) fn classify_correlated_aggregate_subquery_in_where(defining_query: &str) -> bool {
    // Normalize whitespace: collapse runs of whitespace (including newlines and
    // indentation) to single spaces, then remove any space immediately after '('
    // so that multi-line queries like ">\n  ( SELECT SUM…" and single-line
    // "> (SELECT SUM…" both match the same patterns.
    let normalized: String = defining_query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace("( ", "(");
    let upper = normalized.to_ascii_uppercase();
    // Comparison operator directly followed by a scalar subquery
    let has_subquery_comparison = upper.contains("> (SELECT")
        || upper.contains(">(SELECT")
        || upper.contains(">= (SELECT")
        || upper.contains(">=(SELECT")
        || upper.contains("< (SELECT")
        || upper.contains("<(SELECT")
        || upper.contains("<= (SELECT")
        || upper.contains("<=(SELECT");
    if !has_subquery_comparison {
        return false;
    }
    // The subquery must contain an aggregate function (correlated aggregate)
    upper.contains("SUM(")
        || upper.contains(" AVG(")
        || upper.contains(" MIN(")
        || upper.contains(" MAX(")
}

// ── G12-ERM-1: Effective refresh mode tracking ──────────────────────────

// Thread-local that records the mode actually used for the current refresh.
//
// Set by each concrete execution path (`execute_full_refresh`,
// `execute_differential_refresh`, etc.) so the scheduler can write the
// actual mode to `pgt_stream_tables.effective_refresh_mode` after the
// refresh completes — even when an internal fallback changed the mode.
thread_local! {
    static LAST_EFFECTIVE_MODE: Cell<&'static str> = const { Cell::new("") };
}

/// Record the effective refresh mode for the currently-executing refresh.
///
/// Called at the concrete execution point so fallbacks (e.g. adaptive
/// threshold → FULL, CTE → FULL) overwrite the initial mode correctly.
pub(crate) fn set_effective_mode(mode: &'static str) {
    LAST_EFFECTIVE_MODE.with(|m| m.set(mode));
}

/// Take (read and reset) the effective mode recorded by the most recent
/// execution path.  Returns `""` if no refresh has been recorded yet
/// in this thread.
pub fn take_effective_mode() -> &'static str {
    LAST_EFFECTIVE_MODE.with(|m| m.get())
}

pub(crate) fn effective_mode_is_no_data() -> bool {
    LAST_EFFECTIVE_MODE.with(|m| m.get() == "NO_DATA")
}

thread_local! {
    static LAST_ROWS_UPDATED: Cell<i64> = const { Cell::new(0) };
}

pub(crate) fn set_last_rows_updated(rows: i64) {
    LAST_ROWS_UPDATED.with(|value| value.set(rows));
}

pub fn take_last_rows_updated() -> i64 {
    LAST_ROWS_UPDATED.with(|value| value.replace(0))
}

// ── PH-E2: Last-refresh spill tracking ──────────────────────────────────

thread_local! {
    /// Temp blocks written during the most recent MERGE execution.
    /// Set after each differential refresh by querying pg_stat_statements.
    /// Read by the scheduler to track per-ST spill history.
    static LAST_TEMP_BLKS_WRITTEN: Cell<i64> = const { Cell::new(-1) };
}

/// Record the temp blocks written for the currently-executing refresh.
pub(crate) fn set_last_temp_blks_written(blks: i64) {
    LAST_TEMP_BLKS_WRITTEN.with(|c| c.set(blks));
}

/// Take the temp blocks written by the most recent differential refresh.
/// Returns -1 if not available (pg_stat_statements not installed, or not
/// a differential refresh).
pub fn take_last_temp_blks_written() -> i64 {
    LAST_TEMP_BLKS_WRITTEN.with(|c| {
        let v = c.get();
        c.set(-1);
        v
    })
}

#[cfg(test)]
mod tests;
