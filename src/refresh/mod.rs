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
use pgrx::prelude::TimestampWithTimeZone;

pub(crate) mod codegen;
pub(crate) mod fused;
pub(crate) mod merge;
pub(crate) mod orchestrator;
pub(crate) mod phd1;
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
    execute_differential_refresh, execute_full_refresh, execute_no_data_refresh,
    execute_topk_refresh, poll_foreign_table_sources_for_st, post_full_refresh_cleanup,
};
pub use orchestrator::{
    RefreshAction, determine_refresh_action, execute_reinitialize_refresh, validate_topk_metadata,
};
// phd1: cross-cycle phantom cleanup (CORR-1, deferred — see merge.rs).

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
    RefreshRecord::complete_with_rows_updated(
        refresh_id,
        "COMPLETED",
        execution.rows_inserted,
        execution.rows_updated,
        execution.rows_deleted,
        None,
        execution.rows_inserted + execution.rows_updated + execution.rows_deleted,
        Some(history_action.as_str()),
        execution.was_full_fallback,
    )?;

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

use std::cell::{Cell, RefCell};

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

// ── ARCH-2: Refresh reason tracking ──────────────────────────────────────
//
// Captures the machine-readable reason when the executor takes a non-default
// path (e.g. recomputation fallback for non-monotone recursive CTEs).
// Written to `pgt_refresh_history.refresh_reason` via the scheduler.

thread_local! {
    static LAST_REFRESH_REASON: RefCell<Option<&'static str>> = const { RefCell::new(None) };
}

/// Set the refresh reason for the current execution.
///
/// Called whenever a non-default execution path is taken; the scheduler
/// reads this with `take_refresh_reason()` and writes it to history.
pub(crate) fn set_refresh_reason(reason: &'static str) {
    LAST_REFRESH_REASON.with(|r| *r.borrow_mut() = Some(reason));
}

/// Take (read and reset) the refresh reason set by the current execution path.
/// Returns `None` if the default path was taken.
pub fn take_refresh_reason() -> Option<&'static str> {
    LAST_REFRESH_REASON.with(|r| r.borrow_mut().take())
}

#[cfg(test)]
mod tests;
