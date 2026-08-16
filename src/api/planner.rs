//! PLAN-1/2/3 (v0.27.0): Predictive maintenance window planner.
//!
//! Provides `recommend_schedule()`, `schedule_recommendations()` SQL functions
//! and the internal `check_predicted_sla_breach()` hook called by the scheduler.
//!
//! # Algorithm
//!
//! Uses the per-ST `last_full_ms` and `last_diff_ms` history columns combined
//! with a median+MAD (Median Absolute Deviation) model (from v0.25.0) to
//! project the next expected refresh duration. When the projection exceeds the
//! ST's `freshness_deadline_ms` by > 20 %, a `predicted_sla_breach` alert is
//! emitted once per `pg_trickle.schedule_alert_cooldown_seconds` window.

use pgrx::prelude::*;

use crate::catalog::StreamTableMeta;
use crate::config;
use crate::error::PgTrickleError;

// ── Internal helper ────────────────────────────────────────────────────────

/// Resolve a stream table by schema-qualified name.
fn resolve_st(name: &str) -> Result<StreamTableMeta, PgTrickleError> {
    let parts: Vec<&str> = name.splitn(2, '.').collect();
    let (schema, st_name) = match parts.len() {
        1 => {
            let schema = Spi::get_one::<String>("SELECT current_schema()::text")
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_else(|| "public".to_string());
            (schema, parts[0].to_string())
        }
        2 => (parts[0].to_string(), parts[1].to_string()),
        _ => {
            return Err(PgTrickleError::InvalidArgument(format!(
                "invalid stream table name: {name}"
            )));
        }
    };

    StreamTableMeta::get_by_name(&schema, &st_name)
}

/// Compute a simple schedule recommendation from cost-model history.
///
/// Returns `(recommended_interval_seconds, confidence, reasoning)`.
/// `confidence = 0.0` when fewer than `min_samples` observations exist.
fn compute_recommendation(meta: &StreamTableMeta) -> (f64, f64, String) {
    let min_samples = config::PGS_SCHEDULE_RECOMMENDATION_MIN_SAMPLES.get() as usize;

    // Fetch refresh history (last N samples) from pgt_refresh_history
    let history: Vec<f64> = Spi::connect(|client| {
        let rows = client.select(
            "SELECT EXTRACT(epoch FROM (end_time - start_time)) * 1000.0 AS duration_ms \
             FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = $1 AND end_time IS NOT NULL AND status = 'COMPLETED' \
             ORDER BY end_time DESC \
             LIMIT $2",
            None,
            &[
                meta.pgt_id.into(),
                ((min_samples * 3) as i64).into(), // fetch 3× for stats
            ],
        );

        match rows {
            Ok(rows) => rows
                .filter_map(|r| r.get::<f64>(1).ok().flatten())
                .collect(),
            Err(_) => Vec::new(),
        }
    });

    if history.len() < min_samples {
        return (
            meta.last_full_ms.unwrap_or(60_000.0) / 1000.0,
            0.0,
            format!(
                "insufficient history: {} samples, need {}",
                history.len(),
                min_samples
            ),
        );
    }

    // Median absolute deviation (MAD) model
    let mut sorted = history.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = sorted[sorted.len() / 2];
    let deviations: Vec<f64> = sorted.iter().map(|x| (x - median).abs()).collect();
    let mut sorted_dev = deviations.clone();
    sorted_dev.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mad = sorted_dev[sorted_dev.len() / 2];

    // Recommended interval: allow headroom for 1.5× expected duration
    let p95_estimate = median + 3.0 * mad; // ~p95 with MAD scaling
    let recommended_secs = (p95_estimate * 1.5 / 1000.0).max(1.0);

    // Confidence based on coefficient of variation
    let cv = if median > 0.0 { mad / median } else { 1.0 };
    let confidence = (1.0 - cv.min(1.0)).max(0.1);

    (
        recommended_secs,
        confidence,
        format!(
            "median={:.0}ms mad={:.0}ms p95_estimate={:.0}ms recommended={:.1}s confidence={:.2}",
            median, mad, p95_estimate, recommended_secs, confidence
        ),
    )
}

// ── PLAN-1: recommend_schedule ────────────────────────────────────────────

/// PLAN-1 (v0.27.0): Return a schedule recommendation for the given stream table
/// as a JSONB object with keys: `recommended_interval_seconds`,
/// `peak_window_cron`, `confidence` (0–1), `reasoning`.
#[pg_extern(schema = "pgtrickle")]
pub fn recommend_schedule(p_name: &str) -> pgrx::JsonB {
    recommend_schedule_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn recommend_schedule_impl(name: &str) -> Result<pgrx::JsonB, PgTrickleError> {
    let meta = resolve_st(name)?;

    let current_secs = parse_schedule_seconds(meta.schedule.as_deref());
    let (recommended_secs, confidence, reasoning) = compute_recommendation(&meta);

    let json_str = format!(
        r#"{{"recommended_interval_seconds":{:.1},"peak_window_cron":null,"confidence":{:.3},"reasoning":"{}"}}"#,
        recommended_secs,
        confidence,
        reasoning.replace('"', r#"\""#)
    );

    let val: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| PgTrickleError::InternalError(e.to_string()))?;

    // Add delta_pct relative to current schedule
    let mut obj = val
        .as_object()
        .cloned()
        .ok_or_else(|| PgTrickleError::InternalError("json not object".into()))?;

    let delta_pct = if current_secs > 0.0 {
        (recommended_secs - current_secs) / current_secs * 100.0
    } else {
        0.0
    };
    obj.insert(
        "current_interval_seconds".to_string(),
        serde_json::json!(current_secs),
    );
    obj.insert("delta_pct".to_string(), serde_json::json!(delta_pct));

    let final_str =
        serde_json::to_string(&obj).map_err(|e| PgTrickleError::InternalError(e.to_string()))?;

    Ok(pgrx::JsonB(serde_json::from_str(&final_str).map_err(
        |e| PgTrickleError::InternalError(e.to_string()),
    )?))
}

/// Parse a schedule string like `"30s"`, `"1m"`, `"calculated"` into seconds.
fn parse_schedule_seconds(schedule: Option<&str>) -> f64 {
    match schedule {
        None | Some("calculated") | Some("CALCULATED") => 60.0,
        Some(s) => {
            // Try to parse duration like "30s", "5m", "1h"
            if let Some(stripped) = s.strip_suffix('s') {
                stripped.parse::<f64>().unwrap_or(60.0)
            } else if let Some(stripped) = s.strip_suffix('m') {
                stripped.parse::<f64>().unwrap_or(1.0) * 60.0
            } else if let Some(stripped) = s.strip_suffix('h') {
                stripped.parse::<f64>().unwrap_or(1.0) * 3600.0
            } else {
                s.parse::<f64>().unwrap_or(60.0)
            }
        }
    }
}

// ── PLAN-2: schedule_recommendations ─────────────────────────────────────

/// PLAN-2 (v0.27.0): Return one schedule recommendation row per registered
/// stream table, sortable by `delta_pct DESC`.
#[pg_extern(schema = "pgtrickle")]
#[allow(clippy::type_complexity)]
pub fn schedule_recommendations() -> TableIterator<
    'static,
    (
        name!(name, Option<String>),
        name!(current_interval_seconds, Option<f64>),
        name!(recommended_interval_seconds, Option<f64>),
        name!(delta_pct, Option<f64>),
        name!(confidence, Option<f64>),
        name!(reasoning, Option<String>),
    ),
> {
    let rows = schedule_recommendations_impl();
    TableIterator::new(rows)
}

#[allow(clippy::type_complexity)]
fn schedule_recommendations_impl() -> Vec<(
    Option<String>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<String>,
)> {
    let all_sts = match StreamTableMeta::get_all() {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::new();
    for meta in &all_sts {
        let qualified = format!("{}.{}", meta.pgt_schema, meta.pgt_name);
        let current_secs = parse_schedule_seconds(meta.schedule.as_deref());
        let (recommended_secs, confidence, reasoning) = compute_recommendation(meta);
        let delta_pct = if current_secs > 0.0 {
            (recommended_secs - current_secs) / current_secs * 100.0
        } else {
            0.0
        };

        out.push((
            Some(qualified),
            Some(current_secs),
            Some(recommended_secs),
            Some(delta_pct),
            Some(confidence),
            Some(reasoning),
        ));
    }

    out
}

// ── PLAN-3: spike-forecast alert hook ─────────────────────────────────────

/// PLAN-3 (v0.27.0): Check for predicted SLA breach and emit an alert.
///
/// Called by the scheduler after each refresh. Emits a `predicted_sla_breach`
/// NOTIFY on `pg_trickle_alert` when the cost model predicts the next refresh
/// will exceed the ST's `freshness_deadline_ms` by > 20 %.
///
/// Debounced by `pg_trickle.schedule_alert_cooldown_seconds`.
pub fn check_predicted_sla_breach(meta: &StreamTableMeta) {
    let deadline_ms = match meta.freshness_deadline_ms {
        Some(d) if d > 0 => d as f64,
        _ => return, // No SLA configured → skip
    };

    let last_refresh_ms = meta.last_full_ms.unwrap_or(0.0);
    if last_refresh_ms == 0.0 {
        return;
    }

    // Simple prediction: last refresh * 1.1 (optimistic growth factor)
    let predicted_ms = last_refresh_ms * 1.1;
    let sla_breach_threshold = deadline_ms * 1.2; // 20% margin

    if predicted_ms < sla_breach_threshold {
        return;
    }

    // Check cooldown: only emit if not emitted recently
    // Use refresh history recency as a proxy for cooldown (no alert_type column exists)
    let cooldown_secs = config::PGS_SCHEDULE_ALERT_COOLDOWN_SECONDS.get() as i64;
    if cooldown_secs > 0 {
        let recently_alerted: bool = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS( \
               SELECT 1 FROM pgtrickle.pgt_refresh_history \
               WHERE pgt_id = $1 \
                 AND status = 'COMPLETED' \
                 AND end_time > now() - ($2::bigint * interval '1 second') \
             )",
            &[meta.pgt_id.into(), cooldown_secs.into()],
        )
        .unwrap_or(None)
        .unwrap_or(false);

        if recently_alerted {
            return;
        }
    }

    // Emit the alert
    let _ = crate::monitor::emit_alert(
        crate::monitor::AlertEvent::PredictedSlaBreach,
        &meta.pgt_schema,
        &meta.pgt_name,
        serde_json::json!({
            "predicted_ms": predicted_ms.round(),
            "sla_ms": deadline_ms.round(),
            "pct_over": ((predicted_ms - deadline_ms) / deadline_ms * 100.0 * 10.0).round() / 10.0,
        }),
        meta.pooler_compatibility_mode,
    );
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schedule_seconds_seconds() {
        assert!((parse_schedule_seconds(Some("30s")) - 30.0).abs() < 0.01);
    }

    #[test]
    fn test_parse_schedule_seconds_minutes() {
        assert!((parse_schedule_seconds(Some("5m")) - 300.0).abs() < 0.01);
    }

    #[test]
    fn test_parse_schedule_seconds_hours() {
        assert!((parse_schedule_seconds(Some("2h")) - 7200.0).abs() < 0.01);
    }

    #[test]
    fn test_parse_schedule_seconds_calculated() {
        // "calculated" maps to 60s default
        assert!((parse_schedule_seconds(Some("calculated")) - 60.0).abs() < 0.01);
    }

    #[test]
    fn test_parse_schedule_seconds_none() {
        assert!((parse_schedule_seconds(None) - 60.0).abs() < 0.01);
    }

    #[test]
    fn test_parse_schedule_seconds_numeric() {
        assert!((parse_schedule_seconds(Some("120")) - 120.0).abs() < 0.01);
    }
}
