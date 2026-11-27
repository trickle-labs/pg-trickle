// ARCH-1B: Orchestration sub-module for the refresh pipeline.
//
// Contains: RefreshAction enum, determine_refresh_action, validate_topk_metadata,
// cost-model helpers, and execute_reinitialize_refresh.
// SCAL-3 (v0.30.0): Removed dead #[allow(unused_imports)] shims; imports are now
// concrete and will warn if unused, catching future stale imports early.

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
    /// Median complete differential duration among size-comparable inputs.
    pub(crate) diff_ms: f64,
    /// Median completed FULL duration in milliseconds.
    pub(crate) full_ms: f64,
}

/// B-4: Read fresh, size-comparable cost evidence. The periodic summary is
/// useful for monitoring but is not a valid prediction for every delta size.
/// Use the median duration of the last ten completed comparable refreshes
/// (within a factor of two of the current input size). This includes fixed
/// overhead without multiplying it by an unrelated delta size, and rejects
/// isolated slow plans. In the absence of comparable evidence, keep the
/// ordinary ratio heuristic. Never resurrect pre-reset history.
pub(crate) fn query_refresh_history_stats(
    pgt_id: i64,
    current_delta_rows: i64,
) -> Option<RefreshHistoryStats> {
    let min_samples = crate::config::pg_trickle_prediction_min_samples();
    if current_delta_rows <= 0 || min_samples <= 0 {
        return None;
    }
    let min_samples = min_samples.max(3);
    let window_minutes = crate::config::pg_trickle_prediction_window();
    let lower = (current_delta_rows / 2).max(1);
    let upper = current_delta_rows.saturating_mul(2);
    let stats: Option<(f64, f64)> = Spi::connect(|client| {
        let result = (|| {
            let row = client
                .select(
                    "WITH recent_diff_history AS MATERIALIZED (
                        SELECT h.refresh_id,
                               EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000.0 AS ms,
                               CASE WHEN pg_catalog.pg_input_is_valid(h.refresh_reason_detail, 'jsonb')
                                    THEN h.refresh_reason_detail::jsonb END AS evidence
                        FROM pgtrickle.pgt_refresh_history h
                        JOIN pgtrickle.pgt_refresh_summary s USING (pgt_id)
                        WHERE h.pgt_id = $1 AND h.start_time >= s.stats_reset_at
                          AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED'
                          AND h.merge_strategy_used IS DISTINCT FROM 'FULL'
                          AND h.merge_strategy_used IS DISTINCT FROM 'TOP_K'
                          AND NOT h.was_full_fallback AND h.end_time > h.start_time
                          AND h.start_time >= now() - make_interval(mins => $5)
                        ORDER BY h.refresh_id DESC LIMIT GREATEST(50, $4 * 5)
                    ), recent_diff AS (
                        SELECT ms FROM recent_diff_history
                        CROSS JOIN pgtrickle.pgt_stream_tables st
                        WHERE st.pgt_id = $1
                          AND evidence->'plan_identity' = to_jsonb(st.defining_query_hash)
                          AND evidence->'contract_generation' = to_jsonb(st.contract_generation)
                          AND evidence->'schema_version' = '1'::jsonb
                          AND evidence->'input_delta_rows_may_be_incomplete' = 'false'::jsonb
                          AND (CASE WHEN jsonb_typeof(evidence->'input_delta_rows') = 'number'
                                    THEN (evidence->>'input_delta_rows')::numeric END) BETWEEN $2 AND $3
                        ORDER BY refresh_id DESC LIMIT GREATEST(10, $4)
                    ), recent_full AS (
                        SELECT EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000.0 AS ms
                        FROM pgtrickle.pgt_refresh_history h
                        JOIN pgtrickle.pgt_refresh_summary s USING (pgt_id)
                        JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                        WHERE h.pgt_id = $1 AND h.start_time >= s.stats_reset_at
                          AND h.plan_identity = st.defining_query_hash
                          AND h.contract_generation = st.contract_generation
                          AND h.start_time >= now() - make_interval(mins => $5)
                          AND h.status = 'COMPLETED' AND h.end_time > h.start_time
                          AND (h.action = 'FULL' OR h.merge_strategy_used = 'FULL')
                          AND h.rows_inserted > 0
                        ORDER BY h.refresh_id DESC LIMIT 5
                    )
                    SELECT (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ms::float8)
                            FROM recent_diff HAVING COUNT(*) >= $4),
                           (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ms::float8)
                            FROM recent_full)",
                    None,
                    &[
                        pgt_id.into(),
                        lower.into(),
                        upper.into(),
                        min_samples.into(),
                        window_minutes.into(),
                    ],
                )
                .ok()?
                .first();
            Some((row.get::<f64>(1).ok()??, row.get::<f64>(2).ok()??))
        })();
        Ok::<_, pgrx::spi::SpiError>(result)
    })
    .unwrap_or(None);

    let (diff_ms, avg_full_ms) = stats?;
    if !diff_ms.is_finite() || !avg_full_ms.is_finite() || diff_ms <= 0.0 || avg_full_ms <= 0.0 {
        return None;
    }
    Some(RefreshHistoryStats {
        diff_ms,
        full_ms: avg_full_ms,
    })
}

/// Require a measured differential cost to exceed FULL by an availability
/// margin. Historical durations already include the query's complexity.
pub(crate) fn cost_model_prefers_full(diff_ms: f64, full_ms: f64, margin: f64) -> bool {
    diff_ms.is_finite()
        && full_ms.is_finite()
        && margin.is_finite()
        && diff_ms > 0.0
        && full_ms > 0.0
        && diff_ms / full_ms > margin.max(1.0)
}

/// Update the completed stream's monitoring averages in the refresh transaction.
/// The periodic scheduler pass only backfills missing rows and refreshes
/// percentiles: it must not overwrite fresher per-stream averages.
pub(crate) fn update_cost_model_summary_for_st(pgt_id: i64) -> Result<(), PgTrickleError> {
    update_cost_model_summary(Some(pgt_id))
}

fn update_cost_model_summary(pgt_id: Option<i64>) -> Result<(), PgTrickleError> {
    let sql = "
        INSERT INTO pgtrickle.pgt_cost_model_summary
            (pgt_id, avg_full_ms, avg_diff_ms, sample_count, updated_at)
        SELECT
            s.pgt_id,
            COALESCE(full_samples.avg_full_ms, st.last_full_ms),
            diff_samples.avg_diff_ms,
            diff_samples.sample_count,
            now()
        FROM pgtrickle.pgt_refresh_summary s
        JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
        LEFT JOIN LATERAL (
            SELECT AVG(full_ms) AS avg_full_ms
            FROM (
                SELECT EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000.0 AS full_ms
                FROM pgtrickle.pgt_refresh_history h
                WHERE h.pgt_id = s.pgt_id
                  AND h.status = 'COMPLETED'
                  AND h.end_time IS NOT NULL
                  AND h.start_time >= s.stats_reset_at
                  AND (h.action = 'FULL' OR h.merge_strategy_used = 'FULL')
                  AND h.rows_inserted > 0
                ORDER BY h.refresh_id DESC
                LIMIT 5
            ) recent_full
        ) full_samples ON true
        LEFT JOIN LATERAL (
            SELECT AVG(diff_ms) AS avg_diff_ms,
                   COUNT(*)::int AS sample_count
            FROM (
                SELECT EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000.0
                       / GREATEST(h.delta_row_count, 1) AS diff_ms
                FROM pgtrickle.pgt_refresh_history h
                WHERE h.pgt_id = s.pgt_id
                  AND h.status = 'COMPLETED'
                  AND h.end_time IS NOT NULL
                  AND h.start_time >= s.stats_reset_at
                  AND h.action = 'DIFFERENTIAL'
                  AND h.merge_strategy_used IS DISTINCT FROM 'FULL'
                  AND h.merge_strategy_used IS DISTINCT FROM 'TOP_K'
                  AND NOT h.was_full_fallback
                  AND h.delta_row_count > 0
                ORDER BY h.refresh_id DESC
                LIMIT 10
            ) recent_diff
        ) diff_samples ON true
        WHERE s.pgt_id = $1::bigint
           OR ($1::bigint IS NULL AND NOT EXISTS (
               SELECT 1 FROM pgtrickle.pgt_cost_model_summary c WHERE c.pgt_id = s.pgt_id
           ))
        ON CONFLICT (pgt_id) DO UPDATE
            SET avg_full_ms   = EXCLUDED.avg_full_ms,
                avg_diff_ms   = EXCLUDED.avg_diff_ms,
                sample_count  = EXCLUDED.sample_count,
                updated_at    = EXCLUDED.updated_at
            WHERE $1::bigint IS NOT NULL
    ";
    Spi::run_with_args(sql, &[pgt_id.into()]).map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

/// Backfill missing monitoring rows and refresh historical percentiles.
pub(crate) fn batch_update_cost_model_summary() {
    if let Err(e) = update_cost_model_summary(None) {
        pgrx::warning!("[pg_trickle] P-3: failed to backfill pgt_cost_model_summary: {e}");
    }
    let percentile_sql = "
        UPDATE pgtrickle.pgt_cost_model_summary c
           SET p95_ms = stats.p95_ms,
               p99_ms = stats.p99_ms,
               updated_at = now()
          FROM (
                SELECT h.pgt_id,
                       percentile_cont(0.95) WITHIN GROUP
                           (ORDER BY EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)::float8 AS p95_ms,
                       percentile_cont(0.99) WITHIN GROUP
                           (ORDER BY EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)::float8 AS p99_ms
                  FROM pgtrickle.pgt_refresh_history h
                  JOIN pgtrickle.pgt_refresh_summary s USING (pgt_id)
                 WHERE h.status = 'COMPLETED'
                   AND h.end_time IS NOT NULL
                   AND h.start_time >= s.stats_reset_at
                 GROUP BY h.pgt_id
               ) stats
         WHERE c.pgt_id = stats.pgt_id";
    if let Err(e) = pgrx::Spi::run(percentile_sql) {
        pgrx::warning!("[pg_trickle] P-3: failed to update refresh percentiles: {e}");
    }
}

/// Execute a reinitialize refresh: full recompute after schema change.
///
/// If the ST has an `original_query`, uses it for the FULL refresh
/// (so current view definitions are resolved at execution time), then
/// re-runs the rewrite pipeline to store the updated inlined query.
pub fn execute_reinitialize_refresh(st: &StreamTableMeta) -> Result<(i64, i64), PgTrickleError> {
    // Materialize the stored user query first so PostgreSQL resolves its
    // current dependencies under the captured search path.
    let refresh_st = if let Some(original_query) = &st.original_query {
        let mut refresh_st = st.clone();
        refresh_st.defining_query = original_query.clone();
        refresh_st
    } else {
        st.clone()
    };
    let result = crate::refresh::merge::execute_full_refresh_target(&refresh_st)?;

    // Replace the inlined query and all query-derived state in this same
    // transaction after the target reflects the current source definitions.
    let updated = crate::api::reinit_rewrite_if_needed(st)?;
    // A missed partition DDL notification is recovered through this protected
    // FULL rebuild. Only after the new output is materialized may its durable
    // source ancestry be promoted to the observed committed partition tree.
    // Otherwise every subsequent refresh would repeat the reinitialization.
    for dep in crate::catalog::StDependency::get_for_st(st.pgt_id)? {
        if dep.source_type == "TABLE"
            && dep
                .column_snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.get("partition_descendant_oids").is_some())
        {
            crate::catalog::store_column_snapshot_for_pgt_id(st.pgt_id, dep.source_relid)?;
        }
    }
    let _window_plan = crate::window_state::prepare_for_protected_refresh(&updated)?;
    // REL-101-1: rebuild private set-operation state against the reinitialized
    // (possibly rewritten) query in the same transaction. Gated on the cheap
    // set-operation string check so non-set-operation reinit stays unchanged.
    if crate::refresh::with_stream_owner(&updated, || {
        Ok(crate::dvm::query_needs_dual_count(&updated.defining_query))
    })? {
        crate::setop_state::rebuild_for_full_refresh(&updated)?;
    }

    // Clear reinit flag
    Spi::run(&format!(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = FALSE WHERE pgt_id = {}",
        st.pgt_id,
    ))
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // The protected FULL source lock also establishes a safe baseline for
    // legacy streams. An explicit OFF survives this reinitialization.
    crate::api::arm_automatic_trim_after_full_snapshot(st.pgt_id)?;

    Ok(result)
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_cost_model_summary_uses_recent_effective_samples() {
        Spi::run("CREATE TABLE public.cost_history_src (id INT PRIMARY KEY)")
            .expect("create source");
        Spi::run("INSERT INTO public.cost_history_src VALUES (1)").expect("seed source");
        Spi::run(
            "SELECT pgtrickle.create_stream_table(
                'public.cost_history_st',
                'SELECT id FROM public.cost_history_src',
                schedule => '1h',
                refresh_mode => 'DIFFERENTIAL'
            )",
        )
        .expect("create stream table");

        let pgt_id = Spi::get_one::<i64>(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables
              WHERE pgt_schema = 'public' AND pgt_name = 'cost_history_st'",
        )
        .expect("read pgt_id")
        .expect("stream table exists");

        for sql in [
            "DELETE FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1",
            "DELETE FROM pgtrickle.pgt_cost_model_summary WHERE pgt_id = $1",
            "UPDATE pgtrickle.pgt_refresh_summary
                SET stats_reset_at = clock_timestamp() - interval '1 day'
              WHERE pgt_id = $1",
            "UPDATE pgtrickle.pgt_stream_tables SET last_full_ms = 321 WHERE pgt_id = $1",
        ] {
            Spi::run_with_args(sql, &[pgt_id.into()]).expect("reset cost history");
        }

        for sql in [
            "INSERT INTO pgtrickle.pgt_refresh_history
                (pgt_id, data_timestamp, start_time, end_time, action, status,
                 rows_inserted, delta_row_count, merge_strategy_used, was_full_fallback)
             VALUES
                ($1, now(), now() - interval '30 minutes',
                 now() - interval '30 minutes' + interval '10 seconds',
                 'FULL', 'COMPLETED', 1, 1, 'FULL', false),
                ($1, now(), now() - interval '30 minutes',
                 now() - interval '30 minutes' + interval '10 seconds',
                 'DIFFERENTIAL', 'COMPLETED', 1, 1, 'delete_insert', false)",
            "INSERT INTO pgtrickle.pgt_refresh_history
                (pgt_id, data_timestamp, start_time, end_time, action, status,
                 rows_inserted, delta_row_count, merge_strategy_used, was_full_fallback)
             SELECT $1, now(), now() - interval '1 minute',
                    now() - interval '1 minute' + interval '100 milliseconds',
                    'FULL', 'COMPLETED', 1, 1, 'FULL', false
               FROM generate_series(1, 5)",
            "INSERT INTO pgtrickle.pgt_refresh_history
                (pgt_id, data_timestamp, start_time, end_time, action, status,
                 rows_inserted, delta_row_count, merge_strategy_used, was_full_fallback)
             SELECT $1, now(), now() - interval '1 minute',
                    now() - interval '1 minute' + interval '20 milliseconds',
                    'DIFFERENTIAL', 'COMPLETED', 1, 10, 'delete_insert', false
               FROM generate_series(1, 10)",
            "INSERT INTO pgtrickle.pgt_refresh_history
                (pgt_id, data_timestamp, start_time, end_time, action, status,
                 rows_inserted, delta_row_count, merge_strategy_used, was_full_fallback)
             VALUES
               ($1, now(), now() - interval '20 minutes',
                 now() - interval '20 minutes' + interval '500 milliseconds',
                 'DIFFERENTIAL', 'COMPLETED', 1, 1, 'FULL', false),
                ($1, now(), now() - interval '1 minute', now(),
                 'DIFFERENTIAL', 'COMPLETED', 1, 1, 'TOP_K', false),
                ($1, now(), now() - interval '1 minute', now(),
                 'FULL', 'COMPLETED', 0, 1, 'FULL', false)",
        ] {
            Spi::run_with_args(sql, &[pgt_id.into()]).expect("seed cost history");
        }

        batch_update_cost_model_summary();
        let (avg_full_ms, avg_diff_ms, sample_count) = read_cost_summary(pgt_id);
        assert!(
            (avg_full_ms - 180.0).abs() < 0.01,
            "expected recent effective FULL average, got {avg_full_ms}"
        );
        assert!((avg_diff_ms - 2.0).abs() < 0.01, "{avg_diff_ms}");
        assert_eq!(sample_count, 10);

        Spi::run_with_args(
            "DELETE FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .expect("clear cost history");
        Spi::run_with_args(
            "INSERT INTO pgtrickle.pgt_refresh_history
                (pgt_id, data_timestamp, start_time, end_time, action, status,
                 rows_inserted, delta_row_count, merge_strategy_used, was_full_fallback)
             SELECT $1, now(), now() - interval '1 minute',
                    now() - interval '1 minute' + interval '20 milliseconds',
                    'DIFFERENTIAL', 'COMPLETED', 1, 10, 'delete_insert', false
               FROM generate_series(1, 3)",
            &[pgt_id.into()],
        )
        .expect("seed differential-only history");

        update_cost_model_summary_for_st(pgt_id).expect("update completed stream summary");
        let (avg_full_ms, avg_diff_ms, sample_count) = read_cost_summary(pgt_id);
        assert!((avg_full_ms - 321.0).abs() < 0.01, "{avg_full_ms}");
        assert!((avg_diff_ms - 2.0).abs() < 0.01, "{avg_diff_ms}");
        assert_eq!(sample_count, 3);
    }

    #[pg_test]
    fn test_cost_prediction_is_fresh_size_aware_and_reset_aware() {
        Spi::run("CREATE TABLE public.cost_prediction_src (id INT PRIMARY KEY)")
            .expect("create source");
        Spi::run(
            "SELECT pgtrickle.create_stream_table(
                'public.cost_prediction_st',
                'SELECT id FROM public.cost_prediction_src',
                schedule => '1h',
                refresh_mode => 'DIFFERENTIAL'
            )",
        )
        .expect("create stream table");
        Spi::run("SET LOCAL pg_trickle.prediction_min_samples = 3").expect("sample floor");
        let pgt_id = Spi::get_one::<i64>(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables
              WHERE pgt_schema = 'public' AND pgt_name = 'cost_prediction_st'",
        )
        .expect("read pgt_id")
        .expect("stream exists");
        for sql in [
            "DELETE FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1",
            "UPDATE pgtrickle.pgt_refresh_summary
                SET stats_reset_at = now() - interval '1 hour'
              WHERE pgt_id = $1",
            "INSERT INTO pgtrickle.pgt_refresh_history
                (pgt_id, data_timestamp, start_time, end_time, action, status,
                 rows_inserted, delta_row_count, merge_strategy_used, was_full_fallback)
             VALUES
               ($1, now(), now() - interval '2 minutes', now() - interval '1 minute',
                'FULL', 'COMPLETED', 25000000, 1, 'FULL', false),
               ($1, now(), now() - interval '2 minutes', now() - interval '119 seconds',
                'DIFFERENTIAL', 'COMPLETED', 1, 1, 'explicit_dml', false),
               ($1, now(), now() - interval '2 minutes', now() - interval '119 seconds',
                'DIFFERENTIAL', 'COMPLETED', 50000, 50000, 'explicit_dml', false),
               ($1, now(), now() - interval '2 minutes', now() - interval '61 seconds',
                'DIFFERENTIAL', 'COMPLETED', 1, 1, 'explicit_dml', false)",
            "UPDATE pgtrickle.pgt_refresh_history
                SET refresh_reason_detail = jsonb_build_object(
                    'schema_version', 1, 'input_delta_rows', 1000,
                    'input_delta_rows_may_be_incomplete', false,
                    'plan_identity', st.defining_query_hash,
                    'contract_generation', st.contract_generation)::text
               FROM pgtrickle.pgt_stream_tables st
              WHERE pgtrickle.pgt_refresh_history.pgt_id = $1
                AND st.pgt_id = $1 AND action = 'DIFFERENTIAL'",
            "INSERT INTO pgtrickle.pgt_refresh_history
                (pgt_id, data_timestamp, start_time, end_time, action, status,
                 rows_inserted, delta_row_count, merge_strategy_used, was_full_fallback,
                 refresh_reason_detail)
             VALUES
               ($1, now(), now() - interval '2 minutes', now(),
                'DIFFERENTIAL', 'COMPLETED', 1000, 1000, 'explicit_dml', false,
                jsonb_build_object('schema_version', 1, 'input_delta_rows', 1000,
                    'input_delta_rows_may_be_incomplete', true)::text),
               ($1, now(), now() - interval '2 minutes', now(),
                'DIFFERENTIAL', 'COMPLETED', 1000, 1000, 'explicit_dml', false,
                'legacy diagnostic, not JSON')",
        ] {
            Spi::run_with_args(sql, &[pgt_id.into()]).expect("seed cost history");
        }

        // Legacy FULL history is not comparable until provenance is established.
        assert!(query_refresh_history_stats(pgt_id, 1_000).is_none());
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_refresh_history h
                SET plan_identity = st.defining_query_hash,
                    contract_generation = st.contract_generation - 1
               FROM pgtrickle.pgt_stream_tables st
              WHERE h.pgt_id = $1 AND st.pgt_id = $1 AND h.action = 'FULL'",
            &[pgt_id.into()],
        )
        .expect("seed obsolete FULL contract");
        assert!(query_refresh_history_stats(pgt_id, 1_000).is_none());
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_refresh_history h
                SET contract_generation = st.contract_generation
               FROM pgtrickle.pgt_stream_tables st
              WHERE h.pgt_id = $1 AND st.pgt_id = $1 AND h.action = 'FULL'",
            &[pgt_id.into()],
        )
        .expect("bind FULL evidence to current contract");
        let stats = query_refresh_history_stats(pgt_id, 1_000).expect("comparable samples");
        assert!((stats.diff_ms - 1_000.0).abs() < 0.01);
        assert!((stats.full_ms - 60_000.0).abs() < 0.01);
        assert!(query_refresh_history_stats(pgt_id, 50_000).is_none());
        Spi::run("SET LOCAL pg_trickle.prediction_min_samples = 0").expect("disable predictor");
        assert!(query_refresh_history_stats(pgt_id, 1_000).is_none());
        Spi::run("SET LOCAL pg_trickle.prediction_min_samples = 3").expect("enable predictor");
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_refresh_history
                SET refresh_reason_detail =
                    jsonb_set(refresh_reason_detail::jsonb, '{contract_generation}', '-1'::jsonb)::text
              WHERE pgt_id = $1 AND action = 'DIFFERENTIAL'
                AND pg_input_is_valid(refresh_reason_detail, 'jsonb')",
            &[pgt_id.into()],
        )
        .expect("invalidate the sampled contract");
        assert!(query_refresh_history_stats(pgt_id, 1_000).is_none());
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_refresh_history h
                SET refresh_reason_detail = jsonb_set(
                    h.refresh_reason_detail::jsonb, '{contract_generation}',
                    to_jsonb(st.contract_generation))::text
               FROM pgtrickle.pgt_stream_tables st
              WHERE h.pgt_id = $1 AND st.pgt_id = $1 AND h.action = 'DIFFERENTIAL'
                AND pg_input_is_valid(h.refresh_reason_detail, 'jsonb')",
            &[pgt_id.into()],
        )
        .expect("restore comparable contract");
        assert!(query_refresh_history_stats(pgt_id, 1_000).is_some());

        batch_update_cost_model_summary();
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_refresh_summary SET stats_reset_at = now()
               WHERE pgt_id = $1",
            &[pgt_id.into()],
        )
        .expect("reset cost history");
        assert!(query_refresh_history_stats(pgt_id, 1_000).is_none());
    }

    fn read_cost_summary(pgt_id: i64) -> (f64, f64, i32) {
        Spi::connect(|client| {
            let row = client
                .select(
                    "SELECT avg_full_ms, avg_diff_ms, sample_count
                       FROM pgtrickle.pgt_cost_model_summary WHERE pgt_id = $1",
                    None,
                    &[pgt_id.into()],
                )
                .expect("read cost summary")
                .first();
            (
                row.get::<f64>(1)
                    .expect("avg_full_ms")
                    .expect("full sample"),
                row.get::<f64>(2)
                    .expect("avg_diff_ms")
                    .expect("diff sample"),
                row.get::<i32>(3)
                    .expect("sample_count")
                    .expect("sample count"),
            )
        })
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_cost_model_requires_an_availability_margin() {
        assert!(!cost_model_prefers_full(80.0, 100.0, 0.8));
        assert!(!cost_model_prefers_full(110.0, 100.0, 1.5));
        assert!(!cost_model_prefers_full(150.0, 100.0, 1.5));
        assert!(cost_model_prefers_full(151.0, 100.0, 1.5));
    }

    #[test]
    fn test_cost_model_rejects_invalid_evidence() {
        for invalid in [f64::NAN, f64::INFINITY, -1.0, 0.0] {
            assert!(!cost_model_prefers_full(invalid, 100.0, 1.5));
            assert!(!cost_model_prefers_full(100.0, invalid, 1.5));
        }
        assert!(!cost_model_prefers_full(200.0, 100.0, f64::NAN));
    }
}
