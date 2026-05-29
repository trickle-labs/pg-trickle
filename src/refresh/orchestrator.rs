// ARCH-1B: Orchestration sub-module for the refresh pipeline.
//
// Contains: RefreshAction enum, determine_refresh_action, validate_topk_metadata,
// cost-model helpers, and execute_reinitialize_refresh.
// SCAL-3 (v0.30.0): Removed dead #[allow(unused_imports)] shims; imports are now
// concrete and will warn if unused, catching future stale imports early.

use super::{QueryComplexityClass, execute_full_refresh};
use crate::catalog::StreamTableMeta;
use crate::dag::RefreshMode;
use crate::error::PgTrickleError;
use pgrx::prelude::*;

/// Determines what kind of refresh action should be taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshAction {
    /// No upstream changes — just advance the data timestamp.
    NoData,
    /// Full recompute from the defining query.
    Full,
    /// Differential delta application.
    Differential,
    /// Full recompute due to schema change or reinit flag.
    Reinitialize,
}

impl RefreshAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            RefreshAction::NoData => "NO_DATA",
            RefreshAction::Full => "FULL",
            RefreshAction::Differential => "DIFFERENTIAL",
            RefreshAction::Reinitialize => "REINITIALIZE",
        }
    }
}

/// Determine the refresh action for a stream table.
///
/// DI-7: When `max_differential_joins` is configured and the defining query
/// has more join scans than the threshold, DIFFERENTIAL is downgraded to FULL.
/// The `join_scan_count` parameter is optional — when `None`, the DI-7 check
/// is skipped (the caller doesn't have the OpTree available).
pub fn determine_refresh_action(st: &StreamTableMeta, has_upstream_changes: bool) -> RefreshAction {
    if st.needs_reinit {
        return RefreshAction::Reinitialize;
    }
    if !has_upstream_changes {
        return RefreshAction::NoData;
    }
    match st.refresh_mode {
        RefreshMode::Full => RefreshAction::Full,
        RefreshMode::Differential => RefreshAction::Differential,
        // IMMEDIATE-mode STs are maintained by triggers, not by the
        // scheduler.  If we somehow reach this point (e.g. manual
        // refresh), fall back to a full refresh.
        RefreshMode::Immediate => RefreshAction::Full,
    }
}

/// G12-2: Validate stored TopK metadata fields (pure logic — no SPI/parser).
///
/// Returns `Ok(())` when the fields are valid, or `Err(reason)` with a
/// human-readable message when something is inconsistent.  This can be
/// fully unit-tested without a PostgreSQL backend.
pub fn validate_topk_metadata_fields(
    stored_limit: i32,
    stored_order_by: &str,
    stored_offset: Option<i32>,
) -> Result<(), String> {
    if stored_limit < 0 {
        return Err(format!("stored topk_limit is negative ({})", stored_limit));
    }
    if stored_order_by.trim().is_empty() {
        return Err("stored topk_order_by is empty".to_string());
    }
    if let Some(off) = stored_offset
        && off < 0
    {
        return Err(format!("stored topk_offset is negative ({})", off));
    }
    Ok(())
}

/// G12-2: Full TopK runtime validation — validates stored fields and
/// re-parses the reconstructed query to verify the TopK pattern.
/// Requires a PostgreSQL backend (calls parser).
pub fn validate_topk_metadata(
    defining_query: &str,
    stored_limit: i32,
    stored_order_by: &str,
    stored_offset: Option<i32>,
) -> Result<(), String> {
    validate_topk_metadata_fields(stored_limit, stored_order_by, stored_offset)?;

    // Reconstruct the full query and re-parse the TopK pattern.
    let full_query = if let Some(offset) = stored_offset {
        format!(
            "{} ORDER BY {} LIMIT {} OFFSET {}",
            defining_query, stored_order_by, stored_limit, offset
        )
    } else {
        format!(
            "{} ORDER BY {} LIMIT {}",
            defining_query, stored_order_by, stored_limit
        )
    };
    match crate::dvm::detect_topk_pattern(&full_query) {
        Ok(Some(info)) => {
            if info.limit_value != stored_limit as i64 {
                return Err(format!(
                    "re-parsed LIMIT {} differs from stored topk_limit {}",
                    info.limit_value, stored_limit,
                ));
            }
            let expected_offset = stored_offset.map(|o| o as i64);
            if info.offset_value != expected_offset {
                return Err(format!(
                    "re-parsed OFFSET {:?} differs from stored topk_offset {:?}",
                    info.offset_value, stored_offset,
                ));
            }
            Ok(())
        }
        Ok(None) => Err("reconstructed query no longer matches the TopK pattern \
             (ORDER BY + LIMIT with constant integers)"
            .to_string()),
        Err(e) => Err(format!("failed to re-parse TopK pattern: {}", e)),
    }
}

/// Execute a TopK refresh: re-execute the ORDER BY + LIMIT query and MERGE
/// the result into the stream table.
///
/// TopK tables store the top-N rows as defined by ORDER BY + LIMIT. On each
/// refresh, the full query is re-executed against the source tables and the
/// result is merged using MERGE (with NOT MATCHED BY SOURCE for deletes).
///
/// This function is used for both FULL and DIFFERENTIAL refresh modes of
/// TopK tables. The caller decides whether to invoke it (DIFFERENTIAL mode
/// checks change buffers first and skips if no changes exist).
pub(crate) struct RefreshHistoryStats {
    /// Average milliseconds per delta row across recent DIFFERENTIAL refreshes.
    pub(crate) avg_ms_per_delta: f64,
    /// Average FULL refresh time in milliseconds.
    pub(crate) avg_full_ms: f64,
}

/// B-4: Query recent refresh history stats for a stream table.
///
/// Returns `None` when insufficient history exists (fewer than 3
/// completed DIFFERENTIAL refreshes or no completed FULL refresh).
pub(crate) fn query_refresh_history_stats(pgt_id: i64) -> Option<RefreshHistoryStats> {
    // P-3 (v0.78.0): Try the pre-computed summary table first.  It is
    // populated by batch_update_cost_model_summary() once per scheduler tick,
    // eliminating N per-ST subquery scans on pgt_refresh_history.
    // Fall back to the legacy live query if the summary table doesn't exist
    // yet (pre-migration) or has no entry for this ST.
    let summary: Option<(f64, f64, i32)> = Spi::connect(|client| {
        Ok::<_, pgrx::spi::SpiError>((|| {
            let row = client
                .select(
                    "SELECT avg_full_ms, avg_diff_ms, sample_count \
                     FROM pgtrickle.pgt_cost_model_summary \
                     WHERE pgt_id = $1",
                    None,
                    &[pgt_id.into()],
                )
                .ok()?
                .first();
            let avg_full_ms: f64 = row.get::<f64>(1).ok()??;
            let avg_diff_ms: f64 = row.get::<f64>(2).ok()??;
            let sample_count: i32 = row.get::<i32>(3).ok()??;
            if avg_full_ms > 0.0 && avg_diff_ms > 0.0 && sample_count >= 3 {
                Some((avg_full_ms, avg_diff_ms, sample_count))
            } else {
                None
            }
        })())
    })
    .unwrap_or(None);

    if let Some((avg_full_ms, avg_diff_ms, _)) = summary {
        return Some(RefreshHistoryStats {
            avg_ms_per_delta: avg_diff_ms,
            avg_full_ms,
        });
    }

    // Legacy path: live subquery on pgt_refresh_history.
    let stats: Option<(f64, f64)> = Spi::connect(|client| {
        let sql = format!(
            "SELECT incr.avg_ms_per_delta, full_r.avg_full_ms \
             FROM ( \
               SELECT AVG(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000.0 \
                          / GREATEST(delta_row_count, 1)) AS avg_ms_per_delta, \
                      COUNT(*)::int AS cnt \
               FROM ( \
                 SELECT end_time, start_time, delta_row_count \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} \
                   AND action = 'DIFFERENTIAL' \
                   AND status = 'COMPLETED' \
                   AND delta_row_count > 0 \
                   AND end_time IS NOT NULL \
                 ORDER BY refresh_id DESC LIMIT 10 \
               ) __pgt_incr \
             ) incr, ( \
               SELECT AVG(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000.0) AS avg_full_ms \
               FROM ( \
                 SELECT end_time, start_time \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} \
                   AND action = 'FULL' \
                   AND status = 'COMPLETED' \
                   AND end_time IS NOT NULL \
                 ORDER BY refresh_id DESC LIMIT 5 \
               ) __pgt_full \
             ) full_r \
             WHERE incr.cnt >= 3 \
               AND full_r.avg_full_ms IS NOT NULL \
               AND full_r.avg_full_ms > 0 \
               AND incr.avg_ms_per_delta IS NOT NULL \
               AND incr.avg_ms_per_delta > 0",
        );

        let result: Option<(f64, f64)> = (|| {
            let row = client.select(&sql, None, &[]).ok()?.first();
            let avg_ms_per_delta: f64 = row.get::<f64>(1).ok()??;
            let avg_full_ms: f64 = row.get::<f64>(2).ok()??;
            Some((avg_ms_per_delta, avg_full_ms))
        })();
        Ok::<_, pgrx::spi::SpiError>(result)
    })
    .unwrap_or(None);

    let (avg_ms_per_delta, avg_full_ms) = stats?;
    Some(RefreshHistoryStats {
        avg_ms_per_delta,
        avg_full_ms,
    })
}

/// D-3 / B-4: Estimate a cost-based fallback threshold from refresh history.
///
/// Queries the last N DIFFERENTIAL and FULL refreshes for a stream table
/// and computes the crossover delta ratio where incremental cost equals
/// full cost.  The `complexity` class adjusts the per-delta-row cost
/// via a multiplicative factor (joins and aggregates are more expensive
/// per delta row than plain scans).
///
/// Returns `None` if insufficient history is available (fewer
/// than 3 DIFFERENTIAL or no FULL refresh recorded).
///
/// The model:
///   incr_cost(delta_ratio) ≈ avg_incr_cost_per_delta_row × complexity_factor × delta_ratio × table_size
///   full_cost              ≈ avg_full_ms
///   crossover_ratio        = avg_full_ms / (avg_cost_per_delta_row × complexity_factor × table_size)
///
/// Clamped to [0.01, 0.80].
pub(crate) fn estimate_cost_based_threshold(
    pgt_id: i64,
    complexity: QueryComplexityClass,
) -> Option<f64> {
    // Query recent completed DIFFERENTIAL refreshes with non-zero delta.
    let stats: Option<(f64, f64, f64)> = Spi::connect(|client| {
        // avg_ms_per_delta: average milliseconds per delta row
        // avg_full_ms:      average FULL refresh time
        //
        // We use a lateral subquery to get both INCR and FULL stats.
        let sql = format!(
            "SELECT incr.avg_ms_per_delta, full_r.avg_full_ms, \
                    GREATEST(incr.avg_delta, 1)::double precision AS avg_delta \
             FROM ( \
               SELECT AVG(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000.0 \
                          / GREATEST(delta_row_count, 1)) AS avg_ms_per_delta, \
                      AVG(delta_row_count)::double precision AS avg_delta, \
                      COUNT(*)::int AS cnt \
               FROM ( \
                 SELECT end_time, start_time, delta_row_count \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} \
                   AND action = 'DIFFERENTIAL' \
                   AND status = 'COMPLETED' \
                   AND delta_row_count > 0 \
                   AND end_time IS NOT NULL \
                 ORDER BY refresh_id DESC LIMIT 10 \
               ) __pgt_incr \
             ) incr, ( \
               SELECT AVG(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000.0) AS avg_full_ms \
               FROM ( \
                 SELECT end_time, start_time \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} \
                   AND action = 'FULL' \
                   AND status = 'COMPLETED' \
                   AND end_time IS NOT NULL \
                 ORDER BY refresh_id DESC LIMIT 5 \
               ) __pgt_full \
             ) full_r \
             WHERE incr.cnt >= 3 \
               AND full_r.avg_full_ms IS NOT NULL \
               AND full_r.avg_full_ms > 0 \
               AND incr.avg_ms_per_delta IS NOT NULL \
               AND incr.avg_ms_per_delta > 0",
        );

        let result: Option<(f64, f64, f64)> = (|| {
            let row = client.select(&sql, None, &[]).ok()?.first();
            let avg_ms_per_delta: f64 = row.get::<f64>(1).ok()??;
            let avg_full_ms: f64 = row.get::<f64>(2).ok()??;
            let avg_delta: f64 = row.get::<f64>(3).ok()??;
            Some((avg_ms_per_delta, avg_full_ms, avg_delta))
        })();
        Ok::<_, pgrx::spi::SpiError>(result)
    })
    .unwrap_or(None);

    let (avg_ms_per_delta, avg_full_ms, avg_delta) = stats?;

    // crossover_delta = avg_full_ms / (avg_ms_per_delta × complexity_factor)
    // The complexity factor scales the per-delta-row cost: join_agg queries
    // have 4× the cost per delta row compared to a plain scan, so their
    // crossover point is lower (smaller change ratio triggers FULL).
    let factor = complexity.diff_cost_factor();
    let crossover_delta = avg_full_ms / (avg_ms_per_delta * factor);
    if avg_delta <= 0.0 {
        return None;
    }

    // If crossover is much higher than typical delta, current threshold is fine;
    // if crossover is near or below typical delta, we should lower the threshold.
    // Scale the global default (0.15) by how far the crossover is from the average.
    let global_ratio = crate::config::pg_trickle_differential_max_change_ratio();
    let scaling: f64 = crossover_delta / avg_delta;
    let suggested: f64 = (global_ratio * scaling).clamp(0.01, 0.80);

    Some(suggested)
}

/// B-4: Pre-refresh predictive cost comparison.
///
/// **Before** executing a refresh, estimate the DIFFERENTIAL and FULL costs
/// from historical data and the current delta size.  Returns `true` if the
/// cost model recommends FULL refresh.
///
/// When insufficient history exists (cold start), returns `None` to let the
/// caller fall through to the fixed-threshold heuristic.
///
/// Pure decision logic — called from the refresh decision path.
pub(crate) fn cost_model_prefers_full(
    avg_ms_per_delta: f64,
    avg_full_ms: f64,
    current_delta_rows: i64,
    complexity: QueryComplexityClass,
    safety_margin: f64,
) -> bool {
    let factor = complexity.diff_cost_factor();
    let estimated_diff = avg_ms_per_delta * factor * current_delta_rows as f64;
    let estimated_full = avg_full_ms * safety_margin;
    estimated_diff >= estimated_full
}

/// Compute a new adaptive fallback threshold based on observed performance.
///
/// Compares the DIFFERENTIAL refresh time against the last known FULL refresh
/// time and adjusts the threshold accordingly:
///
/// - If INCR time >= 90% of FULL → lower threshold by 20% (more aggressive fallback)
/// - If INCR time >= 70% of FULL → lower threshold by 10%
/// - If INCR time <= 30% of FULL → raise threshold by 10% (allow more INCR)
/// - Otherwise → keep the current threshold
///
/// The threshold is clamped to [0.01, 0.80] to prevent extreme values.
///
/// This is a pure function — no database access.
pub(crate) fn compute_adaptive_threshold(current: f64, incr_ms: f64, full_ms: f64) -> f64 {
    let ratio = incr_ms / full_ms;
    let adjusted = if ratio >= 0.90 {
        // INCR is nearly as slow as FULL — lower threshold aggressively
        current * 0.80
    } else if ratio >= 0.70 {
        // INCR is getting expensive — lower threshold moderately
        current * 0.90
    } else if ratio <= 0.30 {
        // INCR is much faster — raise threshold to allow more INCR
        (current * 1.10).min(0.80)
    } else {
        // INCR is reasonably faster — keep threshold
        current
    };

    adjusted.clamp(0.01, 0.80)
}

/// P-3 (v0.78.0): Batch-update the cost-model summary table.
///
/// Runs a single aggregating INSERT ... ON CONFLICT DO UPDATE that
/// replaces N per-ST history subqueries with one grouped query.
/// Called once per scheduler tick instead of once per stream table.
///
/// The summary table `pgtrickle.pgt_cost_model_summary` must already
/// exist (created by the v0.77.0→0.78.0 migration).
pub(crate) fn batch_update_cost_model_summary() {
    let sql = "
        INSERT INTO pgtrickle.pgt_cost_model_summary
            (pgt_id, avg_full_ms, avg_diff_ms, sample_count, updated_at)
        SELECT
            pgt_id,
            AVG(full_ms)                         AS avg_full_ms,
            AVG(diff_ms)                         AS avg_diff_ms,
            COUNT(*)::int                        AS sample_count,
            now()                                AS updated_at
        FROM (
            SELECT
                pgt_id,
                CASE WHEN action = 'FULL' THEN
                    EXTRACT(EPOCH FROM (end_time - start_time)) * 1000.0
                END AS full_ms,
                CASE WHEN action = 'DIFFERENTIAL' AND delta_row_count > 0 THEN
                    EXTRACT(EPOCH FROM (end_time - start_time)) * 1000.0
                    / GREATEST(delta_row_count, 1)
                END AS diff_ms
            FROM pgtrickle.pgt_refresh_history
            WHERE status = 'COMPLETED'
              AND end_time IS NOT NULL
              AND action IN ('FULL', 'DIFFERENTIAL')
        ) sub
        WHERE full_ms IS NOT NULL OR diff_ms IS NOT NULL
        GROUP BY pgt_id
        HAVING COUNT(*) >= 3
        ON CONFLICT (pgt_id) DO UPDATE
            SET avg_full_ms   = EXCLUDED.avg_full_ms,
                avg_diff_ms   = EXCLUDED.avg_diff_ms,
                sample_count  = EXCLUDED.sample_count,
                updated_at    = EXCLUDED.updated_at
    ";
    if let Err(e) = pgrx::Spi::run(sql) {
        pgrx::warning!("[pg_trickle] P-3: failed to batch-update pgt_cost_model_summary: {e}");
    }
}

/// Execute a reinitialize refresh: full recompute after schema change.
///
/// If the ST has an `original_query`, uses it for the FULL refresh
/// (so current view definitions are resolved at execution time), then
/// re-runs the rewrite pipeline to store the updated inlined query.
pub fn execute_reinitialize_refresh(st: &StreamTableMeta) -> Result<(i64, i64), PgTrickleError> {
    // Use original_query for the refresh so current view/function
    // definitions are resolved at execution time.
    let refresh_st = if let Some(oq) = &st.original_query {
        let mut tmp = st.clone();
        tmp.defining_query = oq.clone();
        tmp
    } else {
        st.clone()
    };

    let result = execute_full_refresh(&refresh_st)?;

    // After refresh, re-run the rewrite pipeline to update stored query.
    let _ = crate::api::reinit_rewrite_if_needed(st);

    // Clear reinit flag
    Spi::run(&format!(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = FALSE WHERE pgt_id = {}",
        st.pgt_id,
    ))
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── compute_adaptive_threshold tests (TEST-1) ────────────────────────────

    #[test]
    fn test_adaptive_threshold_incr_slower_than_90pct_lowers_aggressively() {
        // INCR ~= FULL → lower threshold by 20%
        let result = compute_adaptive_threshold(0.50, 950.0, 1000.0);
        assert!((result - 0.40).abs() < 1e-9, "expected 0.40, got {result}");
    }

    #[test]
    fn test_adaptive_threshold_incr_between_70_and_90pct_lowers_moderately() {
        // ratio = 0.80 → lower by 10%
        let result = compute_adaptive_threshold(0.50, 800.0, 1000.0);
        assert!((result - 0.45).abs() < 1e-9, "expected 0.45, got {result}");
    }

    #[test]
    fn test_adaptive_threshold_incr_faster_than_30pct_raises_threshold() {
        // ratio = 0.20 → raise by 10%
        let result = compute_adaptive_threshold(0.50, 200.0, 1000.0);
        assert!((result - 0.55).abs() < 1e-9, "expected 0.55, got {result}");
    }

    #[test]
    fn test_adaptive_threshold_middle_range_unchanged() {
        // ratio = 0.50 → no change
        let result = compute_adaptive_threshold(0.50, 500.0, 1000.0);
        assert!((result - 0.50).abs() < 1e-9, "expected 0.50, got {result}");
    }

    #[test]
    fn test_adaptive_threshold_clamps_to_minimum_0_01() {
        // Extreme INCR slowness → repeated 20% reductions → clamps at 0.01
        let result = compute_adaptive_threshold(0.01, 999.0, 1000.0);
        assert!(result >= 0.01, "must not go below 0.01: {result}");
    }

    #[test]
    fn test_adaptive_threshold_clamps_to_maximum_0_80() {
        // Very fast INCR + already near max → clamps at 0.80
        let result = compute_adaptive_threshold(0.79, 10.0, 1000.0);
        assert!(result <= 0.80, "must not exceed 0.80: {result}");
    }

    // ── cost_model_prefers_full tests (TEST-1) ───────────────────────────────

    #[test]
    fn test_cost_model_prefers_full_when_diff_expensive() {
        // avg_ms_per_delta=1.0, delta_rows=1000 → diff_est=1000 ms
        // full=500ms*1.2 margin=600ms → 1000 >= 600 → prefer FULL
        assert!(cost_model_prefers_full(
            1.0,
            500.0,
            1000,
            QueryComplexityClass::Scan,
            1.2
        ));
    }

    #[test]
    fn test_cost_model_prefers_diff_when_delta_small() {
        // avg_ms_per_delta=0.1, delta_rows=10 → diff_est=1 ms
        // full=500ms → 1 < 500 → prefer DIFF
        assert!(!cost_model_prefers_full(
            0.1,
            500.0,
            10,
            QueryComplexityClass::Scan,
            1.0
        ));
    }
}
