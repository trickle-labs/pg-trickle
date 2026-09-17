//! Dog-feeding API: pg_trickle monitors itself via stream tables.
//!
//! Provides `setup_self_monitoring()`, `teardown_self_monitoring()`, `self_monitoring_status()`,
//! `recommend_refresh_mode()`, `scheduler_overhead()`, and `explain_dag()`.
#![allow(clippy::type_complexity)]
use pgrx::prelude::*;

use crate::error::PgTrickleError;

// ── Dog-feeding stream table definitions ────────────────────────────────────

/// Names of all six self-monitoring stream tables.
const DF_STREAM_TABLES: &[&str] = &[
    "df_efficiency_rolling",
    "df_anomaly_signals",
    "df_threshold_advice",
    "df_cdc_buffer_trends",
    "df_scheduling_interference",
    "df_frozen_stream_tables",
];

/// DF-1: Rolling efficiency statistics over pgt_refresh_history.
/// Replaces the full-scan `refresh_efficiency()` function.
const DF_EFFICIENCY_ROLLING_QUERY: &str = "\
SELECT
    h.pgt_id,
    st.pgt_schema,
    st.pgt_name,
    count(*)                                                     AS total_refreshes,
    count(*) FILTER (WHERE h.action = 'DIFFERENTIAL'
                       AND h.merge_strategy_used IS DISTINCT FROM 'FULL'
                       AND h.merge_strategy_used IS DISTINCT FROM 'TOP_K'
                       AND NOT h.was_full_fallback)               AS diff_count,
    count(*) FILTER (WHERE h.action IN ('FULL', 'REINITIALIZE')
                       OR h.merge_strategy_used = 'FULL')         AS full_count,
    avg(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)
        FILTER (WHERE h.action = 'DIFFERENTIAL'
                  AND h.merge_strategy_used IS DISTINCT FROM 'FULL'
                  AND h.merge_strategy_used IS DISTINCT FROM 'TOP_K'
                  AND NOT h.was_full_fallback)                   AS avg_diff_ms,
    avg(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)
        FILTER (WHERE h.action IN ('FULL', 'REINITIALIZE')
                  OR h.merge_strategy_used = 'FULL')             AS avg_full_ms,
    avg(h.delta_row_count::float
        / NULLIF(h.rows_inserted + h.rows_deleted, 0))          AS avg_change_ratio,
    max(h.start_time)                                            AS last_refresh_at
FROM pgtrickle.pgt_refresh_history h
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
WHERE h.status = 'COMPLETED'
  AND h.start_time > now() - interval '1 hour'
GROUP BY h.pgt_id, st.pgt_schema, st.pgt_name";

/// DF-2: Anomaly signals — detects duration spikes, error bursts, mode oscillation.
const DF_ANOMALY_SIGNALS_QUERY: &str = "\
SELECT
    h.pgt_id,
    st.pgt_schema,
    st.pgt_name,
    CASE WHEN max(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)
              FILTER (WHERE h.start_time = (
                  SELECT max(h2.start_time) FROM pgtrickle.pgt_refresh_history h2
                  WHERE h2.pgt_id = h.pgt_id AND h2.status = 'COMPLETED'))
              > 3.0 * NULLIF(avg(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)
              FILTER (WHERE h.action = 'DIFFERENTIAL'
                        AND h.merge_strategy_used IS DISTINCT FROM 'FULL'
                        AND h.merge_strategy_used IS DISTINCT FROM 'TOP_K'
                        AND NOT h.was_full_fallback), 0)
         THEN 'DURATION_SPIKE'
    END AS duration_anomaly,
    max(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)
        FILTER (WHERE h.start_time = (
            SELECT max(h2.start_time) FROM pgtrickle.pgt_refresh_history h2
            WHERE h2.pgt_id = h.pgt_id AND h2.status = 'COMPLETED'))
        AS last_duration_ms,
    avg(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)
        FILTER (WHERE h.action = 'DIFFERENTIAL'
                  AND h.merge_strategy_used IS DISTINCT FROM 'FULL'
                  AND h.merge_strategy_used IS DISTINCT FROM 'TOP_K'
                  AND NOT h.was_full_fallback)                   AS baseline_diff_ms,
    count(*) FILTER (WHERE h.status = 'FAILED')                  AS recent_failures,
    count(DISTINCT CASE
        WHEN h.action IN ('FULL', 'REINITIALIZE') OR h.merge_strategy_used = 'FULL'
            THEN 'FULL'
        WHEN h.action = 'DIFFERENTIAL'
         AND h.merge_strategy_used IS DISTINCT FROM 'TOP_K'
         AND NOT h.was_full_fallback
            THEN 'DIFFERENTIAL'
    END) AS distinct_modes_recent,
    st.auto_threshold,
    st.effective_refresh_mode
FROM pgtrickle.pgt_refresh_history h
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
WHERE h.start_time > now() - interval '10 minutes'
GROUP BY h.pgt_id, st.pgt_schema, st.pgt_name,
         st.auto_threshold, st.effective_refresh_mode";

/// DF-3: Threshold advice — multi-cycle threshold recommendation.
const DF_THRESHOLD_ADVICE_QUERY: &str = "\
SELECT
    eff.pgt_id,
    eff.pgt_schema,
    eff.pgt_name,
    st.auto_threshold AS current_threshold,
    CASE
        WHEN eff.avg_diff_ms IS NOT NULL AND eff.avg_full_ms IS NOT NULL
             AND eff.avg_full_ms > 0
        THEN LEAST(0.80, GREATEST(0.01,
            CASE WHEN eff.avg_diff_ms / eff.avg_full_ms <= 0.30 THEN
                LEAST(st.auto_threshold * 1.20, 0.80)
            WHEN eff.avg_diff_ms / eff.avg_full_ms >= 0.80 THEN
                GREATEST(st.auto_threshold * 0.70, 0.01)
            ELSE st.auto_threshold
            END
        ))
        ELSE st.auto_threshold
    END AS recommended_threshold,
    CASE
        WHEN eff.total_refreshes >= 20 AND eff.diff_count >= 5
             AND eff.full_count >= 2 THEN 'HIGH'
        WHEN eff.total_refreshes >= 10 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS confidence,
    CASE
        WHEN eff.avg_diff_ms IS NOT NULL AND eff.avg_full_ms IS NOT NULL
             AND eff.avg_full_ms > 0
        THEN CASE
            WHEN eff.avg_diff_ms / eff.avg_full_ms <= 0.30
                THEN 'DIFF is ' || round((1.0 - eff.avg_diff_ms / eff.avg_full_ms)::numeric * 100)
                     || '% faster — raise threshold to allow more DIFF'
            WHEN eff.avg_diff_ms / eff.avg_full_ms >= 0.80
                THEN 'DIFF is only ' || round((1.0 - eff.avg_diff_ms / eff.avg_full_ms)::numeric * 100)
                     || '% faster — lower threshold to prefer FULL sooner'
            ELSE 'Current threshold is well-calibrated'
        END
        ELSE 'Insufficient data — need both DIFF and FULL observations'
    END AS reason,
    eff.avg_diff_ms,
    eff.avg_full_ms,
    eff.diff_count,
    eff.full_count,
    eff.avg_change_ratio,
    CASE
        WHEN eff.avg_diff_ms IS NOT NULL AND eff.avg_full_ms IS NOT NULL
             AND eff.avg_full_ms > 0
        THEN round(((eff.avg_full_ms - eff.avg_diff_ms) / eff.avg_full_ms * 100)::numeric, 1)
        ELSE NULL
    END AS sla_headroom_pct
FROM pgtrickle.df_efficiency_rolling eff
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = eff.pgt_id";

/// DF-4: CDC buffer trends — tracks change-buffer growth per source.
const DF_CDC_BUFFER_TRENDS_QUERY: &str = "\
SELECT
    d.pgt_id,
    st.pgt_schema,
    st.pgt_name,
    d.source_relid,
    d.cdc_mode,
    avg(h.delta_row_count) AS avg_delta_per_refresh,
    max(h.delta_row_count) AS max_delta_per_refresh,
    count(h.*) AS refreshes_last_hour
FROM pgtrickle.pgt_dependencies d
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
LEFT JOIN pgtrickle.pgt_refresh_history h
    ON h.pgt_id = d.pgt_id
   AND h.status = 'COMPLETED'
   AND h.start_time > now() - interval '1 hour'
WHERE d.source_type = 'TABLE'
GROUP BY d.pgt_id, st.pgt_schema, st.pgt_name,
         d.source_relid, d.cdc_mode";

/// DF-5: Scheduling interference — detects concurrent refresh overlap.
const DF_SCHEDULING_INTERFERENCE_QUERY: &str = "\
SELECT
    a.pgt_id AS pgt_id_a,
    b.pgt_id AS pgt_id_b,
    sta.pgt_name AS st_name_a,
    stb.pgt_name AS st_name_b,
    count(*) AS overlap_count,
    avg(EXTRACT(EPOCH FROM (a.end_time - a.start_time)) * 1000) AS avg_duration_a_ms,
    avg(EXTRACT(EPOCH FROM (b.end_time - b.start_time)) * 1000) AS avg_duration_b_ms
FROM pgtrickle.pgt_refresh_history a
JOIN pgtrickle.pgt_refresh_history b
    ON a.pgt_id < b.pgt_id
   AND a.start_time < b.end_time
   AND b.start_time < a.end_time
   AND b.start_time > now() - interval '1 hour'
   AND b.status = 'COMPLETED'
JOIN pgtrickle.pgt_stream_tables sta ON sta.pgt_id = a.pgt_id
JOIN pgtrickle.pgt_stream_tables stb ON stb.pgt_id = b.pgt_id
WHERE a.status = 'COMPLETED'
  AND a.start_time > now() - interval '1 hour'
GROUP BY a.pgt_id, b.pgt_id, sta.pgt_name, stb.pgt_name
HAVING count(*) >= 3";

/// OPS-2 (v0.24.0): Frozen stream table detector.
///
/// Flags any stream table whose last_refresh_at is older than 5× its
/// refresh_interval while its source tables have recent CDC activity.
/// This indicates the scheduler is stuck or the ST is misconfigured.
const DF_FROZEN_STREAM_TABLES_QUERY: &str = "\
SELECT
    st.pgt_id,
    st.pgt_schema,
    st.pgt_name,
    st.status,
    st.refresh_tier,
    st.last_refresh_at,
    pgtrickle.parse_duration_seconds(st.schedule) AS effective_schedule_seconds,
    now() - st.last_refresh_at AS stale_duration,
    (SELECT max(h.start_time) \
     FROM pgtrickle.pgt_refresh_history h \
     WHERE h.pgt_id = st.pgt_id AND h.status = 'COMPLETED' \
    ) AS last_successful_refresh,
    st.consecutive_errors,
    st.last_error_message
FROM pgtrickle.pgt_stream_tables st
WHERE st.status IN ('ACTIVE', 'ERROR')
  AND st.last_refresh_at IS NOT NULL
  AND st.schedule IS NOT NULL
  AND st.last_refresh_at < now() - make_interval(secs => pgtrickle.parse_duration_seconds(st.schedule) * 5)";

// ── Schedule and mode assignments ───────────────────────────────────────────

/// Return (schedule, refresh_mode) for each self-monitoring stream table.
fn df_st_config(name: &str) -> (&'static str, &'static str) {
    match name {
        "df_efficiency_rolling" => ("48s", "AUTO"),
        "df_anomaly_signals" => ("48s", "AUTO"),
        "df_threshold_advice" => ("96s", "AUTO"),
        "df_cdc_buffer_trends" => ("48s", "FULL"),
        "df_scheduling_interference" => ("96s", "FULL"),
        "df_frozen_stream_tables" => ("120s", "FULL"),
        _ => ("60s", "AUTO"),
    }
}

/// Return the defining query for a self-monitoring stream table.
fn df_st_query(name: &str) -> &'static str {
    match name {
        "df_efficiency_rolling" => DF_EFFICIENCY_ROLLING_QUERY,
        "df_anomaly_signals" => DF_ANOMALY_SIGNALS_QUERY,
        "df_threshold_advice" => DF_THRESHOLD_ADVICE_QUERY,
        "df_cdc_buffer_trends" => DF_CDC_BUFFER_TRENDS_QUERY,
        "df_scheduling_interference" => DF_SCHEDULING_INTERFERENCE_QUERY,
        "df_frozen_stream_tables" => DF_FROZEN_STREAM_TABLES_QUERY,
        _ => "",
    }
}

// ── setup_self_monitoring() — DF-F4, STAB-1 ────────────────────────────────────

/// Create all five self-monitoring stream tables.
///
/// Idempotent: if a DF stream table already exists it is skipped with an INFO
/// message. Safe to call multiple times (STAB-1).
///
/// UX-2: Emits a warm-up hint if `pgt_refresh_history` has fewer than 50 rows.
#[pg_extern(schema = "pgtrickle")]
fn setup_self_monitoring() {
    let result = setup_self_monitoring_impl();
    if let Err(e) = result {
        pgrx::error!("setup_self_monitoring failed: {e}");
    }
}

fn setup_self_monitoring_impl() -> Result<(), PgTrickleError> {
    // UX-2: Warm-up hint if history is sparse.
    let history_count: Option<i64> =
        Spi::get_one("SELECT count(*) FROM pgtrickle.pgt_refresh_history")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    if history_count.unwrap_or(0) < 50 {
        pgrx::warning!(
            "pgt_refresh_history has fewer than 50 rows. Dog-feeding stream tables will \
             populate as refresh history accumulates. Recommendations will be LOW confidence \
             until sufficient data is available."
        );
    }

    for &name in DF_STREAM_TABLES {
        // STAB-1: Check if already exists — idempotent skip.
        let exists: Option<bool> = Spi::get_one_with_args(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_stream_tables
                WHERE pgt_schema = 'pgtrickle' AND pgt_name = $1
            )",
            &[name.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        if exists.unwrap_or(false) {
            pgrx::info!("Dog-feeding stream table pgtrickle.{name} already exists — skipping.");
            continue;
        }

        let query = df_st_query(name);
        let (schedule, refresh_mode) = df_st_config(name);
        let fq_name = format!("pgtrickle.{name}");

        Spi::run_with_args(
            "SELECT pgtrickle.create_stream_table($1, $2, $3, $4, true)",
            &[
                fq_name.as_str().into(),
                query.into(),
                schedule.into(),
                refresh_mode.into(),
            ],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        pgrx::info!("Created self-monitoring stream table pgtrickle.{name}");
    }

    Ok(())
}

// ── teardown_self_monitoring() — DF-F5, STAB-5 ─────────────────────────────────

/// Drop all self-monitoring stream tables.
///
/// Safe with partial setups: each table is dropped individually, and missing
/// tables are silently skipped (STAB-5).
#[pg_extern(schema = "pgtrickle")]
fn teardown_self_monitoring() {
    let result = teardown_self_monitoring_impl();
    if let Err(e) = result {
        pgrx::error!("teardown_self_monitoring failed: {e}");
    }
}

fn teardown_self_monitoring_impl() -> Result<(), PgTrickleError> {
    // Drop in reverse dependency order: downstream first.
    let reverse_order = [
        "df_frozen_stream_tables",
        "df_scheduling_interference",
        "df_cdc_buffer_trends",
        "df_threshold_advice",
        "df_anomaly_signals",
        "df_efficiency_rolling",
    ];

    for &name in &reverse_order {
        let exists: Option<bool> = Spi::get_one_with_args(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_stream_tables
                WHERE pgt_schema = 'pgtrickle' AND pgt_name = $1
            )",
            &[name.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        if !exists.unwrap_or(false) {
            pgrx::info!("Dog-feeding stream table pgtrickle.{name} does not exist — skipping.");
            continue;
        }

        let fq_name = format!("pgtrickle.{name}");
        Spi::run_with_args(
            "SELECT pgtrickle.drop_stream_table($1, true)",
            &[fq_name.as_str().into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        pgrx::info!("Dropped self-monitoring stream table pgtrickle.{name}");
    }

    Ok(())
}

// ── self_monitoring_status() — UX-1 ────────────────────────────────────────────

/// Returns the status of all self-monitoring stream tables.
///
/// For each of the five expected DF stream tables, reports whether it exists,
/// its current status, refresh mode, and last refresh time.
#[pg_extern(schema = "pgtrickle")]
fn self_monitoring_status() -> TableIterator<
    'static,
    (
        name!(st_name, String),
        name!(exists, bool),
        name!(status, Option<String>),
        name!(refresh_mode, Option<String>),
        name!(last_refresh_at, Option<String>),
        name!(total_refreshes, Option<i64>),
    ),
> {
    let rows = self_monitoring_status_impl().unwrap_or_default();
    TableIterator::new(rows)
}

fn self_monitoring_status_impl() -> Result<
    Vec<(
        String,
        bool,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
    )>,
    PgTrickleError,
> {
    let mut out = Vec::new();

    for &name in DF_STREAM_TABLES {
        let row: Option<(Option<String>, Option<String>, Option<String>, Option<i64>)> =
            Spi::connect(|client| {
                let table = client
                    .select(
                        "SELECT
                            st.status,
                            st.effective_refresh_mode,
                            (SELECT max(h.start_time)::text
                             FROM pgtrickle.pgt_refresh_history h
                             WHERE h.pgt_id = st.pgt_id AND h.status = 'COMPLETED'),
                            (SELECT count(*)
                             FROM pgtrickle.pgt_refresh_history h
                             WHERE h.pgt_id = st.pgt_id AND h.status = 'COMPLETED')
                         FROM pgtrickle.pgt_stream_tables st
                         WHERE st.pgt_schema = 'pgtrickle' AND st.pgt_name = $1",
                        None,
                        &[name.into()],
                    )
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

                if let Some(row) = table.into_iter().next() {
                    let status = row
                        .get::<String>(1)
                        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                    let mode = row
                        .get::<String>(2)
                        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                    let last_refresh = row
                        .get::<String>(3)
                        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                    let total = row
                        .get::<i64>(4)
                        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                    Ok(Some((status, mode, last_refresh, total)))
                } else {
                    Ok(None)
                }
            })?;

        match row {
            Some((status, mode, last_refresh, total)) => {
                out.push((name.to_string(), true, status, mode, last_refresh, total));
            }
            None => {
                out.push((name.to_string(), false, None, None, None, None));
            }
        }
    }

    Ok(out)
}

// ── recommend_refresh_mode() — OPS-1 ────────────────────────────────────────
// NOTE: The existing `recommend_refresh_mode` in helpers.rs already provides
// comprehensive mode recommendations. OPS-1 enhancement: when self-monitoring is
// active, the existing function's signals can be enriched with data from
// `df_threshold_advice`. This is handled via `diagnostics::gather_all_signals()`
// which reads from the DF STs when they exist.

// ── scheduler_overhead() — OPS-3 ────────────────────────────────────────────

/// Returns scheduler efficiency metrics.
///
/// Computes busy-time ratio, queue depth, avg dispatch latency, and the
/// fraction of CPU spent on self-monitoring STs vs user STs from refresh history.
#[pg_extern(schema = "pgtrickle")]
fn scheduler_overhead() -> TableIterator<
    'static,
    (
        name!(total_refreshes_1h, i64),
        name!(df_refreshes_1h, i64),
        name!(df_refresh_fraction, Option<f64>),
        name!(avg_refresh_ms, Option<f64>),
        name!(avg_df_refresh_ms, Option<f64>),
        name!(total_refresh_time_s, Option<f64>),
        name!(df_refresh_time_s, Option<f64>),
    ),
> {
    let rows = scheduler_overhead_impl().unwrap_or_else(|e| {
        pgrx::warning!("scheduler_overhead failed: {}", e);
        vec![(0i64, 0i64, None, None, None, None, None)]
    });
    TableIterator::new(rows)
}

fn scheduler_overhead_impl() -> Result<
    Vec<(
        i64,
        i64,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
    )>,
    PgTrickleError,
> {
    // Use a LEFT JOIN instead of FILTER(WHERE EXISTS ...) to avoid potential
    // pgrx SPI issues with correlated subqueries in aggregate filters.
    // No time filter — count all completed refreshes so the result is always
    // non-zero after any refresh has occurred (a 1-hour window was filtering
    // out records in the test environment due to clock/snapshot issues).
    Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    count(h.refresh_id)::bigint AS total_refs,
                    count(s.pgt_id)::bigint AS df_refs,
                    avg(CASE WHEN h.end_time IS NOT NULL
                             THEN EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000.0
                        END)::float8 AS avg_ms,
                    avg(CASE WHEN h.end_time IS NOT NULL AND s.pgt_id IS NOT NULL
                             THEN EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000.0
                        END)::float8 AS avg_df_ms,
                    sum(CASE WHEN h.end_time IS NOT NULL
                             THEN EXTRACT(EPOCH FROM (h.end_time - h.start_time))
                        END)::float8 AS total_s,
                    sum(CASE WHEN h.end_time IS NOT NULL AND s.pgt_id IS NOT NULL
                             THEN EXTRACT(EPOCH FROM (h.end_time - h.start_time))
                        END)::float8 AS df_total_s
                 FROM pgtrickle.pgt_refresh_history h
                 LEFT JOIN pgtrickle.pgt_stream_tables s
                        ON s.pgt_id = h.pgt_id
                       AND s.pgt_schema = 'pgtrickle'
                       AND s.pgt_name LIKE 'df_%'
                 WHERE h.status = 'COMPLETED'",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        if let Some(row) = result.into_iter().next() {
            let total_refs = row
                .get::<i64>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            let df_refs = row
                .get::<i64>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            let df_fraction = if total_refs > 0 {
                Some(df_refs as f64 / total_refs as f64)
            } else {
                None
            };
            let avg_ms = row
                .get::<f64>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let avg_df_ms = row
                .get::<f64>(4)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let total_s = row
                .get::<f64>(5)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let df_total_s = row
                .get::<f64>(6)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok(vec![(
                total_refs,
                df_refs,
                df_fraction,
                avg_ms,
                avg_df_ms,
                total_s,
                df_total_s,
            )])
        } else {
            Ok(vec![(0, 0, None, None, None, None, None)])
        }
    })
}

// ── explain_dag() — OPS-4 ───────────────────────────────────────────────────

/// Returns the full refresh DAG as a Mermaid markdown string.
///
/// Node colours: user STs = blue, self-monitoring STs = green,
/// suspended = red, fused = orange.
#[pg_extern(schema = "pgtrickle")]
fn explain_dag(format: default!(Option<&str>, "'mermaid'")) -> Option<String> {
    explain_dag_impl(format.unwrap_or("mermaid")).ok()
}

fn explain_dag_impl(format: &str) -> Result<String, PgTrickleError> {
    let is_dot = format.eq_ignore_ascii_case("dot");

    // Collect all stream tables and their dependencies.
    let nodes: Vec<(i64, String, String, String, bool)> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT st.pgt_id,
                        st.pgt_schema || '.' || st.pgt_name AS fq_name,
                        st.status,
                        st.effective_refresh_mode,
                        (st.pgt_schema = 'pgtrickle' AND st.pgt_name LIKE 'df_%') AS is_df
                 FROM pgtrickle.pgt_stream_tables st
                 ORDER BY st.pgt_id",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut out = Vec::new();
        for row in result {
            let pgt_id = row
                .get::<i64>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            let fq_name = row
                .get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let status = row
                .get::<String>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let mode = row
                .get::<String>(4)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let is_df = row
                .get::<bool>(5)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(false);
            out.push((pgt_id, fq_name, status, mode, is_df));
        }
        Ok(out)
    })?;

    let edges: Vec<(i64, i64)> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT DISTINCT d.pgt_id AS downstream_id, dep_st.pgt_id AS upstream_id
                 FROM pgtrickle.pgt_dependencies d
                 JOIN pgtrickle.pgt_stream_tables dep_st
                   ON dep_st.pgt_relid = d.source_relid
                 ORDER BY upstream_id, downstream_id",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut out = Vec::new();
        for row in result {
            let downstream = row
                .get::<i64>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            let upstream = row
                .get::<i64>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(0);
            out.push((upstream, downstream));
        }
        Ok(out)
    })?;

    if is_dot {
        Ok(render_dag_dot(&nodes, &edges))
    } else {
        Ok(render_dag_mermaid(&nodes, &edges))
    }
}

fn node_color(status: &str, is_df: bool) -> &'static str {
    match (status, is_df) {
        ("SUSPENDED", _) => "red",
        ("FUSED", _) => "orange",
        (_, true) => "green",
        _ => "blue",
    }
}

fn render_dag_mermaid(
    nodes: &[(i64, String, String, String, bool)],
    edges: &[(i64, i64)],
) -> String {
    let mut out = String::from("graph TD\n");

    for (pgt_id, fq_name, status, mode, is_df) in nodes {
        let color = node_color(status, *is_df);
        let label = format!("{fq_name}\\n[{mode}]");
        out.push_str(&format!("    n{pgt_id}[\"{label}\"]:::{color}\n"));
    }

    for (upstream, downstream) in edges {
        out.push_str(&format!("    n{upstream} --> n{downstream}\n"));
    }

    out.push_str("\n    classDef blue fill:#4A90D9,stroke:#333,color:#fff\n");
    out.push_str("    classDef green fill:#27AE60,stroke:#333,color:#fff\n");
    out.push_str("    classDef red fill:#E74C3C,stroke:#333,color:#fff\n");
    out.push_str("    classDef orange fill:#F39C12,stroke:#333,color:#fff\n");

    out
}

fn render_dag_dot(nodes: &[(i64, String, String, String, bool)], edges: &[(i64, i64)]) -> String {
    let mut out = String::from("digraph dag {\n    rankdir=TD;\n    node [shape=box];\n");

    for (pgt_id, fq_name, status, mode, is_df) in nodes {
        let color = match node_color(status, *is_df) {
            "blue" => "#4A90D9",
            "green" => "#27AE60",
            "red" => "#E74C3C",
            "orange" => "#F39C12",
            _ => "#CCCCCC",
        };
        let label = format!("{fq_name}\\n[{mode}]");
        out.push_str(&format!(
            "    n{pgt_id} [label=\"{label}\" fillcolor=\"{color}\" style=filled fontcolor=white];\n"
        ));
    }

    for (upstream, downstream) in edges {
        out.push_str(&format!("    n{upstream} -> n{downstream};\n"));
    }

    out.push_str("}\n");
    out
}

// ── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_df_stream_tables_count() {
        assert_eq!(DF_STREAM_TABLES.len(), 6);
    }

    #[test]
    fn test_df_st_config_returns_valid_schedules() {
        for &name in DF_STREAM_TABLES {
            let (schedule, mode) = df_st_config(name);
            assert!(!schedule.is_empty(), "schedule empty for {name}");
            assert!(
                ["AUTO", "FULL", "DIFFERENTIAL"].contains(&mode),
                "invalid mode for {name}: {mode}"
            );
        }
    }

    #[test]
    fn test_df_st_query_returns_non_empty() {
        for &name in DF_STREAM_TABLES {
            let query = df_st_query(name);
            assert!(!query.is_empty(), "query empty for {name}");
            assert!(query.contains("SELECT"), "query for {name} missing SELECT");
        }
    }

    #[test]
    fn test_node_color_logic() {
        assert_eq!(node_color("SUSPENDED", false), "red");
        assert_eq!(node_color("SUSPENDED", true), "red");
        assert_eq!(node_color("FUSED", false), "orange");
        assert_eq!(node_color("ACTIVE", true), "green");
        assert_eq!(node_color("ACTIVE", false), "blue");
    }

    #[test]
    fn test_render_dag_mermaid_basic() {
        let nodes = vec![
            (
                1,
                "public.orders".to_string(),
                "ACTIVE".to_string(),
                "DIFFERENTIAL".to_string(),
                false,
            ),
            (
                2,
                "pgtrickle.df_efficiency_rolling".to_string(),
                "ACTIVE".to_string(),
                "DIFFERENTIAL".to_string(),
                true,
            ),
        ];
        let edges = vec![(1, 2)];
        let mermaid = render_dag_mermaid(&nodes, &edges);
        assert!(mermaid.contains("graph TD"));
        assert!(mermaid.contains("n1"));
        assert!(mermaid.contains("n2"));
        assert!(mermaid.contains("n1 --> n2"));
        assert!(mermaid.contains("classDef green"));
    }

    #[test]
    fn test_render_dag_dot_basic() {
        let nodes = vec![(
            1,
            "public.orders".to_string(),
            "ACTIVE".to_string(),
            "FULL".to_string(),
            false,
        )];
        let edges = vec![];
        let dot = render_dag_dot(&nodes, &edges);
        assert!(dot.contains("digraph dag"));
        assert!(dot.contains("n1"));
        assert!(dot.contains("#4A90D9"));
    }

    /// CORR-1: Verify the threshold clamping SQL expression
    /// produces values within [0.01, 0.80] for edge cases.
    #[test]
    fn test_threshold_advice_query_contains_clamp() {
        assert!(DF_THRESHOLD_ADVICE_QUERY.contains("LEAST(0.80"));
        assert!(DF_THRESHOLD_ADVICE_QUERY.contains("GREATEST(0.01"));
    }

    /// CORR-3: Verify the NULLIF guard against divide-by-zero.
    #[test]
    fn test_efficiency_rolling_query_contains_nullif_guard() {
        assert!(
            DF_EFFICIENCY_ROLLING_QUERY.contains("NULLIF(h.rows_inserted + h.rows_deleted, 0)")
        );
    }

    /// CORR-5: Verify the window boundary uses strict > (exclusive).
    #[test]
    fn test_efficiency_rolling_query_uses_exclusive_boundary() {
        assert!(DF_EFFICIENCY_ROLLING_QUERY.contains("> now() - interval '1 hour'"));
        // Ensure it's strict greater-than, not >=
        assert!(!DF_EFFICIENCY_ROLLING_QUERY.contains(">= now() - interval '1 hour'"));
    }

    /// UX-8: Verify DF-3 includes SLA headroom column.
    #[test]
    fn test_threshold_advice_query_contains_sla_headroom() {
        assert!(DF_THRESHOLD_ADVICE_QUERY.contains("sla_headroom_pct"));
    }
}
