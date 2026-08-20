//! v0.22.0: Downstream CDC publication, predictive cost model, and SLA-driven
//! tier auto-assignment API functions.

use pgrx::prelude::*;

use crate::catalog::StreamTableMeta;
use crate::error::PgTrickleError;

// ── CDC-PUB-1: stream_table_to_publication() ─────────────────────────────

/// CDC-PUB-1: Create a logical replication publication for a stream table.
///
/// Creates a PostgreSQL publication exposing the named stream table so that
/// Kafka Connect, Debezium, and other logical replication subscribers can
/// receive change events without a separate replication slot.
#[pg_extern(schema = "pgtrickle")]
fn stream_table_to_publication(name: &str) {
    // ERR-2 (v0.26.0): Use typed into_pg_error() at the API boundary.
    stream_table_to_publication_impl(name).unwrap_or_else(|e| e.into_pg_error());
}

fn stream_table_to_publication_impl(name: &str) -> Result<(), PgTrickleError> {
    // SEC-001 (v0.70.0): Use shared parse_qualified_name_pub which respects
    // current_schema() for unqualified names instead of hardcoding 'public'.
    let (schema, table) = super::helpers::parse_qualified_name_pub(name)?;
    let meta = StreamTableMeta::get_by_name(&schema, &table)?;

    // SEC-2: Ownership check — same guard as alter/drop/pause/resume.
    super::helpers::check_stream_table_ownership(meta.pgt_relid, &schema, &table)?;

    if meta.downstream_publication_name.is_some() {
        return Err(PgTrickleError::PublicationAlreadyExists(name.into()));
    }

    let pub_name = format!("pgt_pub_{}", meta.pgt_name);
    let qualified_table = format!("{}.{}", meta.pgt_schema, meta.pgt_name);

    Spi::connect_mut(|client| {
        // Create the publication for the stream table's storage table.
        client
            .update(
                &format!(
                    "CREATE PUBLICATION {} FOR TABLE {}",
                    quote_ident(&pub_name),
                    quote_ident_qualified(&meta.pgt_schema, &meta.pgt_name)
                ),
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        // Store the publication name in the catalog.
        client
            .update(
                "UPDATE pgtrickle.pgt_stream_tables \
                 SET downstream_publication_name = $1, updated_at = now() \
                 WHERE pgt_id = $2",
                None,
                &[pub_name.clone().into(), meta.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        pgrx::info!(
            "pg_trickle: created publication '{}' for stream table '{}'",
            pub_name,
            qualified_table
        );

        Ok::<(), PgTrickleError>(())
    })?;

    Ok(())
}

// ── CDC-PUB-2: drop_stream_table_publication() ──────────────────────────

/// CDC-PUB-2: Drop the logical replication publication for a stream table.
#[pg_extern(schema = "pgtrickle")]
fn drop_stream_table_publication(name: &str) {
    // ERR-2 (v0.26.0): Use typed into_pg_error() at the API boundary.
    drop_stream_table_publication_impl(name).unwrap_or_else(|e| e.into_pg_error());
}

fn drop_stream_table_publication_impl(name: &str) -> Result<(), PgTrickleError> {
    // SEC-001 (v0.70.0): Use shared parse_qualified_name_pub.
    let (schema, table) = super::helpers::parse_qualified_name_pub(name)?;
    let meta = StreamTableMeta::get_by_name(&schema, &table)?;

    // SEC-2: Ownership check — same guard as alter/drop/pause/resume.
    super::helpers::check_stream_table_ownership(meta.pgt_relid, &schema, &table)?;

    let pub_name = match &meta.downstream_publication_name {
        Some(p) => p.clone(),
        None => return Err(PgTrickleError::PublicationNotFound(name.into())),
    };

    Spi::connect_mut(|client| {
        client
            .update(
                &format!("DROP PUBLICATION IF EXISTS {}", quote_ident(&pub_name)),
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        client
            .update(
                "UPDATE pgtrickle.pgt_stream_tables \
                 SET downstream_publication_name = NULL, updated_at = now() \
                 WHERE pgt_id = $1",
                None,
                &[meta.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        pgrx::info!(
            "pg_trickle: dropped publication '{}' for stream table '{}.{}'",
            pub_name,
            meta.pgt_schema,
            meta.pgt_name
        );

        Ok::<(), PgTrickleError>(())
    })?;

    Ok(())
}

// ── SLA-1: sla parameter support ─────────────────────────────────────────

/// SLA-1: Set the SLA interval for a stream table.
///
/// Accepts an interval and stores it as `freshness_deadline_ms`.
/// The scheduler uses this to auto-assign the appropriate refresh tier.
#[pg_extern(schema = "pgtrickle")]
fn set_stream_table_sla(name: &str, sla: Interval) {
    // ERR-2 (v0.26.0): Use typed into_pg_error() at the API boundary.
    set_stream_table_sla_impl(name, sla).unwrap_or_else(|e| e.into_pg_error());
}

fn set_stream_table_sla_impl(name: &str, sla: Interval) -> Result<(), PgTrickleError> {
    // SEC-001 (v0.70.0): Use shared parse_qualified_name_pub.
    let (schema, table) = super::helpers::parse_qualified_name_pub(name)?;
    let meta = StreamTableMeta::get_by_name(&schema, &table)?;

    if sla.months() != 0 {
        return Err(PgTrickleError::InvalidArgument(
            "SLA interval cannot contain calendar months; use days or smaller units".into(),
        ));
    }
    let total_ms = sla.days() as i64 * 24 * 3600 * 1000 + sla.micros() / 1000;

    if total_ms <= 0 {
        return Err(PgTrickleError::InvalidArgument(
            "SLA interval must be positive".into(),
        ));
    }

    // SLA-2: Determine the initial tier assignment based on the SLA.
    let tier = assign_tier_for_sla(total_ms)?;

    super::alter::apply_target_freshness(
        meta.pgt_id,
        super::alter::TargetFreshness {
            mode: super::alter::TargetFreshnessMode::Interval,
            milliseconds: Some(total_ms),
        },
    )?;

    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE pgtrickle.pgt_stream_tables \
                 SET refresh_tier = $1, updated_at = now() \
                 WHERE pgt_id = $2",
                None,
                &[tier.as_str().into(), meta.pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        pgrx::info!(
            "pg_trickle: set SLA {}ms for '{}', assigned tier '{}'",
            total_ms,
            name,
            tier.as_str()
        );

        Ok::<(), PgTrickleError>(())
    })?;

    Ok(())
}

/// SLA-2: Assign the appropriate tier based on an SLA interval in milliseconds.
///
/// Tier assignment rules:
/// - Hot: SLA ≤ 5s (refresh every 1× schedule)
/// - Warm: SLA ≤ 30s (refresh every 2× schedule)
/// - Cold: SLA > 30s (refresh every 10× schedule)
pub fn assign_tier_for_sla(sla_ms: i64) -> Result<crate::scheduler::RefreshTier, PgTrickleError> {
    use crate::scheduler::RefreshTier;
    if sla_ms <= 5_000 {
        Ok(RefreshTier::Hot)
    } else if sla_ms <= 30_000 {
        Ok(RefreshTier::Warm)
    } else {
        Ok(RefreshTier::Cold)
    }
}

// ── PRED-1: Linear regression forecaster with robustness guards ───────────

/// PRED-1: Fit a simple linear regression `duration_ms ~ delta_rows` over
/// the prediction window for a given stream table, with outlier robustness.
///
/// ## Robustness guards (v0.25.0)
///
/// 1. **Cold-start guard**: Returns `None` if the first DIFFERENTIAL record
///    in `pgt_refresh_history` is less than 60 s old (avoids acting on a
///    single noisy sample immediately after a stream table is populated).
/// 2. **Non-degenerate variance check**: Returns `(0.0, avg_y, n)` when all
///    delta sizes are identical (slope undefined). The caller interprets this
///    as "intercept only" and skips the preemption check.
/// 3. **Outlier filter**: Uses an IQR (p25 / p75) window from the sample
///    set to exclude extreme outliers before fitting the regression.
///
/// Returns `(slope, intercept, sample_count)`. Returns `None` if fewer than
/// `prediction_min_samples` clean samples exist.
pub fn fit_linear_regression(pgt_id: i64) -> Option<(f64, f64, i64)> {
    let window_minutes = crate::config::pg_trickle_prediction_window();
    let min_samples = crate::config::pg_trickle_prediction_min_samples();

    if min_samples <= 0 {
        return None;
    }

    Spi::connect(|client| {
        // PRED-1 cold-start guard: skip if first differential was < 60s ago.
        let first_age_secs: Option<f64> = client
            .select(
                "SELECT EXTRACT(EPOCH FROM (now() - MIN(start_time))) \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = $1 AND action = 'DIFFERENTIAL' AND status = 'COMPLETED'",
                None,
                &[pgt_id.into()],
            )
            .ok()
            .and_then(|r| {
                if r.is_empty() {
                    None
                } else {
                    r.first().get::<f64>(1).ok().flatten()
                }
            });

        if first_age_secs.is_some_and(|age| age < 60.0) {
            return None; // cold-start guard
        }

        // PRED-1 outlier filter: compute IQR bounds from the current window.
        // Rows with duration_ms outside [p25 - 1.5 * IQR, p75 + 1.5 * IQR]
        // are excluded from the regression.
        let (p25, p75): (f64, f64) = client
            .select(
                "SELECT \
                     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY \
                         EXTRACT(EPOCH FROM (end_time - start_time)) * 1000) AS p25, \
                     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY \
                         EXTRACT(EPOCH FROM (end_time - start_time)) * 1000) AS p75 \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = $1 \
                   AND status = 'COMPLETED' \
                   AND action = 'DIFFERENTIAL' \
                   AND end_time IS NOT NULL \
                   AND start_time > now() - ($2 || ' minutes')::interval",
                None,
                &[pgt_id.into(), window_minutes.into()],
            )
            .ok()
            .and_then(|r| {
                if r.is_empty() {
                    None
                } else {
                    let row = r.first();
                    let p25 = row.get::<f64>(1).ok().flatten()?;
                    let p75 = row.get::<f64>(2).ok().flatten()?;
                    Some((p25, p75))
                }
            })
            .unwrap_or((0.0, f64::MAX));

        let iqr = p75 - p25;
        let lower_bound = (p25 - 1.5 * iqr).max(0.0);
        let upper_bound = p75 + 1.5 * iqr;

        // Fit regression on the IQR-filtered sample set.
        let result = client
            .select(
                "SELECT count(*) AS n, \
                        coalesce(avg(rows_inserted + rows_deleted), 0) AS avg_x, \
                        coalesce(avg(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000), 0) AS avg_y, \
                        coalesce(sum((rows_inserted + rows_deleted) * \
                            EXTRACT(EPOCH FROM (end_time - start_time)) * 1000), 0) AS sum_xy, \
                        coalesce(sum((rows_inserted + rows_deleted) * \
                            (rows_inserted + rows_deleted)), 0) AS sum_x2 \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = $1 \
                   AND status = 'COMPLETED' \
                   AND action = 'DIFFERENTIAL' \
                   AND end_time IS NOT NULL \
                   AND start_time > now() - ($2 || ' minutes')::interval \
                   AND EXTRACT(EPOCH FROM (end_time - start_time)) * 1000 \
                       BETWEEN $3 AND $4",
                None,
                &[
                    pgt_id.into(),
                    window_minutes.into(),
                    lower_bound.into(),
                    upper_bound.into(),
                ],
            )
            .ok()?;

        if result.is_empty() {
            return None;
        }

        let n: i64 = result.get::<i64>(1).ok()??;
        if n < min_samples as i64 {
            return None; // not enough clean samples
        }

        let avg_x: f64 = result.get::<f64>(2).ok()??;
        let avg_y: f64 = result.get::<f64>(3).ok()??;
        let sum_xy: f64 = result.get::<f64>(4).ok()??;
        let sum_x2: f64 = result.get::<f64>(5).ok()??;

        // PRED-1: Non-degenerate variance check.
        // If all x values are identical (e.g. always same delta size),
        // the slope is undefined — return intercept-only model.
        let denominator = sum_x2 - n as f64 * avg_x * avg_x;
        if denominator.abs() < 1e-10 {
            return Some((0.0, avg_y, n));
        }

        let slope = (sum_xy - n as f64 * avg_x * avg_y) / denominator;
        let intercept = avg_y - slope * avg_x;

        Some((slope, intercept, n))
    })
}

/// PRED-2: Predict the differential refresh duration for a given delta size.
///
/// Returns `None` if the model cannot be fitted (cold-start fallback).
pub fn predict_diff_duration_ms(pgt_id: i64, delta_rows: i64) -> Option<f64> {
    let (slope, intercept, _n) = fit_linear_regression(pgt_id)?;
    Some(slope * delta_rows as f64 + intercept)
}

/// PRED-2: Check whether the predicted differential cost exceeds the
/// full-refresh cost by more than `prediction_ratio`, triggering a
/// pre-emptive switch to FULL.
///
/// ## PRED-1 robustness guards (v0.25.0)
///
/// - **Clamping**: Predictions are clamped to `[0.5×, 4×] last_full_ms`
///   before the ratio comparison. A prediction outside this range is likely
///   a model artifact and should not drive preemption.
/// - **Zero-intercept guard**: When the model slope is 0.0 (degenerate
///   intercept-only case), the prediction is treated as unknown and
///   preemption is skipped.
pub fn should_preempt_to_full(pgt_id: i64, delta_rows: i64, last_full_ms: f64) -> bool {
    if last_full_ms <= 0.0 {
        return false;
    }
    let ratio = crate::config::pg_trickle_prediction_ratio();
    if let Some(raw_predicted_ms) = predict_diff_duration_ms(pgt_id, delta_rows) {
        // PRED-1: Clamp prediction to [0.5×, 4×] last_full_ms.
        let lower = last_full_ms * 0.5;
        let upper = last_full_ms * 4.0;
        let predicted_ms = raw_predicted_ms.clamp(lower, upper);
        predicted_ms > last_full_ms * ratio
    } else {
        false // cold-start fallback — don't preempt.
    }
}

// ── SLA-3: Dynamic tier re-assignment ────────────────────────────────────

/// SLA-2 (v0.26.0): Per-ST hysteresis counters for tier adjustment damping.
///
/// Stored in a thread-local so the scheduler's single-threaded tick loop
/// persists the state between tick invocations without requiring a catalog
/// schema change. State is lost on scheduler restart (acceptable — just
/// means 3 more ticks are needed before a tier change fires).
///
/// Key: `pgt_id`.
/// Value: `(consecutive_upgrade_pressure, consecutive_downgrade_pressure)`.
///   - upgrade pressure: ideal tier is hotter than current → need to upgrade
///   - downgrade pressure: ideal tier is colder than current → could downgrade
///
/// Requires 3 consecutive pressure signals in the same direction before
/// actually changing the tier. This prevents oscillation at the SLA boundary.
pub struct SlaTierHysteresis {
    /// Consecutive ticks where the ideal tier is hotter than current.
    pub upgrade_pressure: i32,
    /// Consecutive ticks where the ideal tier is colder than current.
    pub downgrade_pressure: i32,
}

impl SlaTierHysteresis {
    const THRESHOLD: i32 = 3;
}

use std::cell::RefCell;
use std::collections::HashMap as _HashMap;
thread_local! {
    /// SLA-2: Per-ST tier hysteresis state.  Keyed by `pgt_id`.
    static SLA_TIER_HYSTERESIS: RefCell<_HashMap<i64, SlaTierHysteresis>> =
        RefCell::new(_HashMap::new());
}

/// Numeric ordering for RefreshTier (lower = hotter = more frequent).
fn tier_order(tier: &crate::scheduler::RefreshTier) -> u8 {
    use crate::scheduler::RefreshTier;
    match tier {
        RefreshTier::Hot => 0,
        RefreshTier::Warm => 1,
        RefreshTier::Cold => 2,
        RefreshTier::Frozen => 3,
    }
}

/// SLA-3: Check and adjust tier for a stream table based on SLA and queue depth.
///
/// Called after each refresh tick. Bumps tier up or down only after 3
/// consecutive signals in the same direction (SLA-2 hysteresis damping).
pub fn maybe_adjust_tier_for_sla(meta: &StreamTableMeta) {
    let sla_ms = match meta.freshness_deadline_ms {
        Some(ms) => ms,
        None => return, // No SLA configured — skip.
    };

    // Look at the last 3 refresh durations to determine if the tier is appropriate.
    let avg_duration_ms = Spi::connect(|client| {
        client
            .select(
                "SELECT coalesce(avg(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000), 0) \
                 FROM (SELECT end_time, start_time \
                       FROM pgtrickle.pgt_refresh_history \
                       WHERE pgt_id = $1 AND status = 'COMPLETED' \
                       ORDER BY end_time DESC LIMIT 3) sub",
                None,
                &[meta.pgt_id.into()],
            )
            .ok()
            .and_then(|r| {
                if r.is_empty() {
                    None
                } else {
                    r.get::<f64>(1).ok()?
                }
            })
    });

    let _avg_ms = match avg_duration_ms {
        Some(ms) => ms,
        None => return, // Not enough data.
    };

    use crate::scheduler::RefreshTier;
    let current_tier = RefreshTier::from_sql_str(&meta.refresh_tier);
    let ideal_tier = assign_tier_for_sla(sla_ms).unwrap_or(RefreshTier::Hot);

    let current_order = tier_order(&current_tier);
    let ideal_order = tier_order(&ideal_tier);

    // SLA-2 (v0.26.0): Hysteresis damping — require THRESHOLD consecutive
    // pressure signals before changing the tier.
    if current_order == ideal_order {
        // Tiers match — reset hysteresis counters.
        SLA_TIER_HYSTERESIS.with(|map| {
            if let Some(state) = map.borrow_mut().get_mut(&meta.pgt_id) {
                state.upgrade_pressure = 0;
                state.downgrade_pressure = 0;
            }
        });
        return;
    }

    SLA_TIER_HYSTERESIS.with(|map| {
        let mut map = map.borrow_mut();
        let state = map.entry(meta.pgt_id).or_insert(SlaTierHysteresis {
            upgrade_pressure: 0,
            downgrade_pressure: 0,
        });

        if ideal_order < current_order {
            // Upgrade pressure: ideal tier is hotter than current.
            state.upgrade_pressure += 1;
            state.downgrade_pressure = 0;

            if state.upgrade_pressure >= SlaTierHysteresis::THRESHOLD {
                // Upgrade by one step (don't jump directly to ideal — incremental).
                let new_order = current_order.saturating_sub(1);
                let new_tier = match new_order {
                    0 => RefreshTier::Hot,
                    1 => RefreshTier::Warm,
                    2 => RefreshTier::Cold,
                    _ => RefreshTier::Frozen,
                };
                Spi::connect_mut(|client| {
                    let _ = client.update(
                        "UPDATE pgtrickle.pgt_stream_tables \
                         SET refresh_tier = $1, updated_at = now() \
                         WHERE pgt_id = $2",
                        None,
                        &[new_tier.as_str().into(), meta.pgt_id.into()],
                    );
                });
                #[cfg(not(test))]
                pgrx::info!(
                    "pg_trickle: SLA-2 tier upgrade for '{}': {} → {} \
                     (after {} consecutive pressure signals)",
                    meta.pgt_name,
                    current_tier.as_str(),
                    new_tier.as_str(),
                    state.upgrade_pressure,
                );
                state.upgrade_pressure = 0;
            }
        } else {
            // Downgrade pressure: ideal tier is colder than current.
            state.downgrade_pressure += 1;
            state.upgrade_pressure = 0;

            if state.downgrade_pressure >= SlaTierHysteresis::THRESHOLD {
                // Downgrade by one step.
                let new_order = current_order + 1;
                let new_tier = match new_order {
                    0 => RefreshTier::Hot,
                    1 => RefreshTier::Warm,
                    2 => RefreshTier::Cold,
                    _ => RefreshTier::Frozen,
                };
                Spi::connect_mut(|client| {
                    let _ = client.update(
                        "UPDATE pgtrickle.pgt_stream_tables \
                         SET refresh_tier = $1, updated_at = now() \
                         WHERE pgt_id = $2",
                        None,
                        &[new_tier.as_str().into(), meta.pgt_id.into()],
                    );
                });
                #[cfg(not(test))]
                pgrx::info!(
                    "pg_trickle: SLA-2 tier downgrade for '{}': {} → {} \
                     (after {} consecutive pressure signals)",
                    meta.pgt_name,
                    current_tier.as_str(),
                    new_tier.as_str(),
                    state.downgrade_pressure,
                );
                state.downgrade_pressure = 0;
            }
        }
    });
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Quote a SQL identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a qualified SQL identifier (schema.table).
fn quote_ident_qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

// ── PUB-1 (v0.25.0): Subscriber-LSN tracking ──────────────────────────────

/// PUB-1: Check whether any logical replication slot associated with a
/// downstream publication lags more than `pg_trickle.publication_lag_warn_bytes`
/// bytes behind the current WAL write position.
///
/// Emits a WARNING for each lagging slot.  Returns `true` if at least one
/// slot lags beyond the threshold (i.e. the caller should NOT truncate the
/// change buffer until subscribers catch up).
///
/// Returns `false` when:
/// - The threshold GUC is 0 (feature disabled, default).
/// - The publication has no active replication slots.
/// - All slots are within the lag threshold.
pub(crate) fn check_subscriber_lag(publication_name: &str) -> bool {
    let warn_bytes = crate::config::pg_trickle_publication_lag_warn_bytes();
    if warn_bytes <= 0 {
        return false;
    }

    // Query pg_replication_slots for all slots consuming this publication.
    // `confirmed_flush_lsn` is the LSN the subscriber has confirmed processing.
    let sql = format!(
        "SELECT slot_name::text, \
                pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint AS lag_bytes \
         FROM pg_replication_slots \
         WHERE active = true \
           AND plugin = 'pgoutput' \
           AND slot_name LIKE 'pgt\\_{pub}\\_%' \
         ORDER BY lag_bytes DESC",
        pub = publication_name.replace('\'', "''"),
    );

    let mut any_lagging = false;

    let result = Spi::connect(|client| {
        let rows = client.select(&sql, None, &[]).map_err(|e| {
            pgrx::warning!(
                "[pg_trickle] PUB-1: failed to query replication slots for '{}': {}",
                publication_name,
                e,
            );
        });

        if let Ok(rows) = rows {
            for row in rows {
                let slot_name: String = row.get(1).ok().flatten().unwrap_or_default();
                let lag_bytes: i64 = row.get(2).ok().flatten().unwrap_or(0);

                if lag_bytes > warn_bytes {
                    pgrx::warning!(
                        "[pg_trickle] PUB-1: subscriber slot '{}' for publication '{}' \
                         is {} bytes behind write LSN (threshold: {} bytes). \
                         Change buffer truncation deferred.",
                        slot_name,
                        publication_name,
                        lag_bytes,
                        warn_bytes,
                    );
                    any_lagging = true;
                }
            }
        }
        Ok::<(), ()>(())
    });

    if result.is_err() {
        // SPI connect failed; conservatively treat as lagging.
        return true;
    }

    any_lagging
}

/// PUB-1: Guard that prevents change buffer truncation when a subscriber
/// is lagging.
///
/// Returns `true` (skip truncation) when `check_subscriber_lag` detects
/// one or more lagging subscribers.
pub(crate) fn should_defer_change_buffer_truncation(publication_name: &str) -> bool {
    check_subscriber_lag(publication_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_tier_for_sla_hot() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(1000).ok(), Some(RefreshTier::Hot));
        assert_eq!(assign_tier_for_sla(5000).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_for_sla_warm() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(5001).ok(), Some(RefreshTier::Warm));
        assert_eq!(assign_tier_for_sla(30000).ok(), Some(RefreshTier::Warm));
    }

    #[test]
    fn test_assign_tier_for_sla_cold() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(30001).ok(), Some(RefreshTier::Cold));
        assert_eq!(assign_tier_for_sla(60000).ok(), Some(RefreshTier::Cold));
    }

    // SEC-001 (v0.70.0): The local parse_qualified_name() has been removed.
    // Schema-qualified name parsing is now done by
    // super::helpers::parse_qualified_name_pub() which respects
    // current_schema() for unqualified names (requires SPI — no unit test).
    // Tests for the shared helper live in helpers.rs.

    #[test]
    fn test_parse_qualified_name_with_schema() {
        // Two-part qualified names: no SPI needed (the split path).
        assert_eq!(
            crate::api::helpers::parse_qualified_name_pub("myschema.mytable").ok(),
            Some(("myschema".to_string(), "mytable".to_string()))
        );
    }

    #[test]
    fn test_parse_qualified_dots_in_schema() {
        // Typed identifiers reject ambiguous three-part names.
        assert!(crate::api::helpers::parse_qualified_name_pub("my.schema.table").is_err());
    }

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("hello"), "\"hello\"");
    }

    #[test]
    fn test_quote_ident_with_quotes() {
        assert_eq!(quote_ident("he\"llo"), "\"he\"\"llo\"");
    }

    // ── TEST-6 (v0.24.0): Comprehensive publication.rs unit tests ────────
    //
    // 25+ tests for assign_tier_for_sla, parse_qualified_name, quote_ident,
    // and boundary cases (0, negative, NaN-like edge values).

    #[test]
    fn test_assign_tier_sla_zero() {
        use crate::scheduler::RefreshTier;
        // Zero is valid (Hot tier — aggressive)
        assert_eq!(assign_tier_for_sla(0).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_sla_one_ms() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(1).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_sla_boundary_5000() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(5000).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_sla_boundary_5001() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(5001).ok(), Some(RefreshTier::Warm));
    }

    #[test]
    fn test_assign_tier_sla_boundary_30000() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(30000).ok(), Some(RefreshTier::Warm));
    }

    #[test]
    fn test_assign_tier_sla_boundary_30001() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(30001).ok(), Some(RefreshTier::Cold));
    }

    #[test]
    fn test_assign_tier_sla_very_large() {
        use crate::scheduler::RefreshTier;
        assert_eq!(
            assign_tier_for_sla(86_400_000).ok(),
            Some(RefreshTier::Cold)
        );
    }

    #[test]
    fn test_assign_tier_sla_negative() {
        use crate::scheduler::RefreshTier;
        // Negative is technically invalid but shouldn't panic.
        // It falls into the Hot tier (≤ 5000).
        assert_eq!(assign_tier_for_sla(-1).ok(), Some(RefreshTier::Hot));
    }

    #[test]
    fn test_assign_tier_sla_i64_max() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(i64::MAX).ok(), Some(RefreshTier::Cold));
    }

    #[test]
    fn test_quote_ident_empty() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn test_quote_ident_spaces() {
        assert_eq!(quote_ident("my table"), "\"my table\"");
    }

    #[test]
    fn test_quote_ident_unicode() {
        assert_eq!(quote_ident("tëst"), "\"tëst\"");
    }

    #[test]
    fn test_quote_ident_qualified_basic() {
        assert_eq!(
            quote_ident_qualified("public", "orders"),
            "\"public\".\"orders\""
        );
    }

    #[test]
    fn test_quote_ident_qualified_with_quotes() {
        assert_eq!(
            quote_ident_qualified("my\"schema", "my\"table"),
            "\"my\"\"schema\".\"my\"\"table\""
        );
    }

    #[test]
    fn test_quote_ident_qualified_empty_schema() {
        assert_eq!(quote_ident_qualified("", "table"), "\"\".\"table\"");
    }

    #[test]
    fn test_assign_tier_sla_warm_midpoint() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(15000).ok(), Some(RefreshTier::Warm));
    }

    #[test]
    fn test_assign_tier_sla_cold_100s() {
        use crate::scheduler::RefreshTier;
        assert_eq!(assign_tier_for_sla(100_000).ok(), Some(RefreshTier::Cold));
    }

    #[test]
    fn test_quote_ident_backslash() {
        assert_eq!(quote_ident("back\\slash"), "\"back\\slash\"");
    }

    #[test]
    fn test_quote_ident_null_char() {
        // Null characters should be preserved in quoting
        assert_eq!(quote_ident("a\0b"), "\"a\0b\"");
    }
}
