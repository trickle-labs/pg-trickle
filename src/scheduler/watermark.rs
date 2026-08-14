//! CQ-10-01 (v0.49.0): Frontier holdback tick watermark helpers.
//!
//! Extracted from scheduler/mod.rs as part of scheduler module decomposition.
//! Contains tick watermark computation, xmin holdback, and frontier advance logic.
//!
//! All functions here are accessible to sibling scheduler submodules via
//! `use super::watermark::*` because child modules inherit access to parent
//! private items.

use pgrx::prelude::*;

use crate::{cdc, config, shmem, version};

// ── #536: Frontier holdback tick watermark helpers ─────────────────────────

/// Unix-epoch timestamp of the last holdback-active WARNING, used to
/// rate-limit warnings to at most one per minute.
static LAST_HOLDBACK_WARN_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Compute the tick watermark for the **coordinator** (main scheduler loop).
///
/// Applies the `frontier_holdback_mode` GUC logic. Every mode first uses the
/// mandatory visibility probe; `lsn:<N>` may then apply an additional cap.
///
/// Side effects (when holdback fires):
/// - Updates `shmem::last_tick_oldest_xmin` for the next tick.
/// - Updates `shmem::last_tick_safe_lsn_u64` for dynamic workers.
/// - Updates the holdback gauge metrics.
/// - Emits a WARNING when holdback age exceeds the warn threshold.
///
/// # Arguments
/// - `prev_watermark_lsn`: the safe LSN from the previous tick, if any.
///
/// # Returns
/// `(tick_watermark, current_oldest_xmin, oldest_txn_age_secs)`
pub(super) fn compute_coordinator_tick_watermark(
    prev_watermark_lsn: Option<&str>,
) -> (Option<String>, u64, u64) {
    let mode = config::pg_trickle_frontier_holdback_mode();

    match mode {
        config::FrontierHoldbackMode::None
        | config::FrontierHoldbackMode::Xmin
        | config::FrontierHoldbackMode::InvalidLsn => {
            let prev_oldest_xmin = shmem::last_tick_oldest_xmin();

            match cdc::compute_safe_upper_bound(prev_watermark_lsn, prev_oldest_xmin) {
                Ok((safe_lsn, write_lsn, current_oldest_xmin, age_secs)) => {
                    // Persist for next tick and for dynamic workers under a
                    // single lock so workers never see xmin/LSN out of sync.
                    let safe_u64 = version::lsn_to_u64(&safe_lsn);
                    shmem::set_last_tick_holdback_state(current_oldest_xmin, safe_u64);

                    // Update holdback gauge metrics.
                    let write_u64 = version::lsn_to_u64(&write_lsn);
                    let holdback_bytes = write_u64.saturating_sub(safe_u64);
                    shmem::update_holdback_metrics(holdback_bytes, age_secs);

                    // Warn when holdback has been active longer than the threshold.
                    if holdback_bytes > 0 {
                        emit_holdback_warning_if_needed(age_secs);
                    }

                    (Some(safe_lsn), current_oldest_xmin, age_secs)
                }
                Err(e) => {
                    // On probe failure, hold at the previous watermark (if known)
                    // rather than advancing to the raw write LSN.  Advancing on
                    // failure is the exact unsafe behaviour the holdback is meant
                    // to prevent — the probe may have failed precisely because a
                    // long-running transaction exists.
                    warning!(
                        "pg_trickle: holdback probe failed ({}); holding at previous watermark",
                        e
                    );
                    let safe_lsn = match prev_watermark_lsn {
                        Some(prev) => {
                            // Re-use last known-safe watermark.
                            let u = version::lsn_to_u64(prev);
                            shmem::set_last_tick_safe_lsn(u);
                            Some(prev.to_string())
                        }
                        None => None,
                    };
                    shmem::update_holdback_metrics(0, 0);
                    (safe_lsn, 0, 0)
                }
            }
        }

        config::FrontierHoldbackMode::LsnBytes(offset_bytes) => {
            let prev_oldest_xmin = shmem::last_tick_oldest_xmin();
            match cdc::compute_safe_upper_bound(prev_watermark_lsn, prev_oldest_xmin) {
                Ok((mandatory_lsn, candidate_lsn, current_oldest_xmin, age_secs)) => {
                    let mandatory = version::lsn_to_u64(&mandatory_lsn);
                    let capped = version::lsn_to_u64(&candidate_lsn).saturating_sub(offset_bytes);
                    let safe_lsn = version::u64_to_lsn(mandatory.min(capped));
                    shmem::set_last_tick_holdback_state(current_oldest_xmin, mandatory.min(capped));
                    shmem::update_holdback_metrics(
                        version::lsn_to_u64(&candidate_lsn).saturating_sub(mandatory.min(capped)),
                        age_secs,
                    );
                    (Some(safe_lsn), current_oldest_xmin, age_secs)
                }
                Err(e) => {
                    warning!("pg_trickle: safe frontier probe failed: {}", e);
                    (None, 0, 0)
                }
            }
        }
    }
}

/// Rate-limited WARNING for when frontier holdback has been active longer
/// than `pg_trickle.frontier_holdback_warn_seconds`.
///
/// Emits at most one WARNING per minute.
pub(super) fn emit_holdback_warning_if_needed(oldest_txn_age_secs: u64) {
    let warn_secs = config::pg_trickle_frontier_holdback_warn_seconds();
    if warn_secs <= 0 {
        return;
    }
    if oldest_txn_age_secs < warn_secs as u64 {
        return;
    }

    // Rate-limit: emit at most once per minute.
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last_warn = LAST_HOLDBACK_WARN_SECS.load(std::sync::atomic::Ordering::Relaxed);
    if now_secs.saturating_sub(last_warn) < 60 {
        return;
    }
    LAST_HOLDBACK_WARN_SECS.store(now_secs, std::sync::atomic::Ordering::Relaxed);

    pgrx::warning!(
        "pg_trickle: frontier holdback active — the oldest in-progress transaction is {}s old \
         (threshold: {}s). Stream tables may lag behind. \
         Check pg_stat_activity for long-running sessions. \
         To suppress: SET pg_trickle.frontier_holdback_warn_seconds = 0.",
        oldest_txn_age_secs,
        warn_secs,
    );
}

/// Pure helper: decide whether to emit a holdback warning.
///
/// Extracted so it can be unit-tested without a pgrx backend.
///
/// # Arguments
/// - `warn_secs` — `pg_trickle.frontier_holdback_warn_seconds` GUC value.
/// - `oldest_txn_age_secs` — age of the oldest long-running transaction.
/// - `last_warn_secs` — Unix timestamp of the previous WARNING (0 if never).
/// - `now_secs` — current Unix timestamp.
///
/// # Returns
/// `true` when a WARNING should be emitted (and the caller should update
/// `LAST_HOLDBACK_WARN_SECS` to `now_secs`).
pub(super) fn should_emit_holdback_warning(
    warn_secs: i32,
    oldest_txn_age_secs: u64,
    last_warn_secs: u64,
    now_secs: u64,
) -> bool {
    if warn_secs <= 0 {
        return false;
    }
    if oldest_txn_age_secs < warn_secs as u64 {
        return false;
    }
    // Rate-limit: at most once per 60 seconds.
    now_secs.saturating_sub(last_warn_secs) >= 60
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // watermark.rs contains frontier holdback logic.  The main entry points
    // call SPI and read pgrx GUCs, so they require a PostgreSQL backend.
    // `should_emit_holdback_warning` is a pure extraction of the rate-limit
    // decision and is fully unit-testable here.

    #[test]
    fn test_should_emit_warning_disabled_when_warn_secs_zero() {
        // Threshold disabled — never emit.
        assert!(!should_emit_holdback_warning(0, 9999, 0, 9999));
    }

    #[test]
    fn test_should_emit_warning_disabled_when_warn_secs_negative() {
        assert!(!should_emit_holdback_warning(-5, 9999, 0, 9999));
    }

    #[test]
    fn test_should_emit_warning_below_age_threshold() {
        // Transaction age (30s) is below warn threshold (60s) — no warning.
        assert!(!should_emit_holdback_warning(60, 30, 0, 100));
    }

    #[test]
    fn test_should_emit_warning_at_age_threshold() {
        // Transaction age equals warn threshold — should emit.
        assert!(should_emit_holdback_warning(60, 60, 0, 200));
    }

    #[test]
    fn test_should_emit_warning_above_age_threshold_first_time() {
        // First warning (last_warn=0, now=100): 100 - 0 = 100 >= 60 → emit.
        assert!(should_emit_holdback_warning(30, 120, 0, 100));
    }

    #[test]
    fn test_should_emit_warning_rate_limited() {
        // Emitted 30s ago (100 - 70 = 30 < 60) → suppress.
        assert!(!should_emit_holdback_warning(30, 120, 70, 100));
    }

    #[test]
    fn test_should_emit_warning_rate_limit_expired() {
        // Last emitted 70s ago (170 - 100 = 70 >= 60) → emit again.
        assert!(should_emit_holdback_warning(30, 120, 100, 170));
    }

    #[test]
    fn test_should_emit_warning_saturating_sub_overflow() {
        // now_secs < last_warn_secs (clock skew) — saturating_sub returns 0,
        // which is < 60 → rate-limit fires and suppresses the warning.
        assert!(!should_emit_holdback_warning(30, 120, 200, 100));
    }
}
