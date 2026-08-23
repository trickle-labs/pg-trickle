//! Pure scheduler tenancy policy for v0.87.

use std::cmp::Ordering;

/// One scheduler-tick view of application pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadSnapshot {
    pub connected_clients: u64,
    pub max_connections: u64,
    pub runnable_backends: u64,
    pub available_cpus: u64,
    pub lock_waiters: u64,
    pub active_refreshes: u64,
}

impl LoadSnapshot {
    pub const fn new(
        connected_clients: u64,
        max_connections: u64,
        runnable_backends: u64,
        available_cpus: u64,
        lock_waiters: u64,
        active_refreshes: u64,
    ) -> Self {
        Self {
            connected_clients,
            max_connections,
            runnable_backends,
            available_cpus,
            lock_waiters,
            active_refreshes,
        }
    }

    pub fn pressure(self) -> f64 {
        pressure_ratio(self)
    }
}

/// Maximum normalized pressure across connections, runnable work, and lock waiters.
pub fn pressure_ratio(snapshot: LoadSnapshot) -> f64 {
    fn ratio(numerator: u64, denominator: u64) -> f64 {
        if denominator == 0 {
            if numerator == 0 { 0.0 } else { 1.0 }
        } else {
            (numerator as f64 / denominator as f64).clamp(0.0, 1.0)
        }
    }

    ratio(snapshot.connected_clients, snapshot.max_connections)
        .max(ratio(
            snapshot
                .runnable_backends
                .saturating_add(snapshot.active_refreshes),
            snapshot.available_cpus,
        ))
        .max(ratio(
            snapshot.lock_waiters,
            snapshot.connected_clients.max(1),
        ))
}

/// Hysteresis state for load-aware admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PressureState {
    deferred: bool,
    healthy_ticks: u8,
    overloaded_ticks: u8,
    deferral_factor: u8,
}

impl PressureState {
    pub const fn new() -> Self {
        Self {
            deferred: false,
            healthy_ticks: 0,
            overloaded_ticks: 0,
            deferral_factor: 1,
        }
    }

    pub const fn deferred(self) -> bool {
        self.deferred
    }

    pub const fn deferral_factor(self) -> u8 {
        self.deferral_factor
    }

    pub fn observe(&mut self, pressure: f64, threshold: f64) -> bool {
        let threshold = threshold.clamp(0.0, 1.0);
        if threshold == 0.0 {
            self.deferred = false;
            self.healthy_ticks = 0;
            self.overloaded_ticks = 0;
            self.deferral_factor = 1;
            return false;
        }
        if self.deferred {
            if pressure < threshold * 0.75 {
                self.healthy_ticks = self.healthy_ticks.saturating_add(1);
                self.overloaded_ticks = 0;
                self.deferral_factor = self.deferral_factor.saturating_sub(1).max(1);
                if self.healthy_ticks >= 2 {
                    self.deferred = false;
                    self.healthy_ticks = 0;
                    self.overloaded_ticks = 0;
                    self.deferral_factor = 1;
                }
            } else {
                self.healthy_ticks = 0;
                self.overloaded_ticks = self.overloaded_ticks.saturating_add(1);
                self.deferral_factor = 1u8
                    .checked_shl(self.overloaded_ticks.saturating_sub(1).min(3) as u32)
                    .unwrap_or(8);
            }
        } else if pressure >= threshold {
            self.deferred = true;
            self.healthy_ticks = 0;
            self.overloaded_ticks = 1;
            self.deferral_factor = 1;
        }
        self.deferred
    }
}

impl Default for PressureState {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable jitter for periodic schedules. The phase changes only with `epoch`.
pub fn deterministic_jitter_ms(
    database_oid: u32,
    pgt_id: i64,
    epoch: u64,
    interval_ms: u64,
    scheduler_tick_ms: u64,
) -> u64 {
    if interval_ms == 0 {
        return 0;
    }
    let window = (interval_ms / 10).min(5_000);
    let modulus = window.max(scheduler_tick_ms).max(1);
    let mut bytes = [0u8; 20];
    bytes[..4].copy_from_slice(&database_oid.to_le_bytes());
    bytes[4..12].copy_from_slice(&pgt_id.to_le_bytes());
    bytes[12..20].copy_from_slice(&epoch.to_le_bytes());
    xxhash_rust::xxh3::xxh3_64(&bytes) % modulus
}

/// Scheduler-originated deadlines derived from a table cadence.
pub fn derive_scheduled_deadlines(
    interval_ms: u64,
    configured_lock_timeout_ms: u64,
    configured_statement_timeout_ms: u64,
) -> (u64, u64) {
    let lock_floor = 100;
    let statement_floor = 1_000;
    let lock_limit = std::cmp::max(interval_ms / 10, lock_floor);
    let statement_limit = interval_ms
        .saturating_mul(4)
        .checked_div(5)
        .map_or(u64::MAX, |value| std::cmp::max(value, statement_floor));
    (
        configured_lock_timeout_ms.min(lock_limit),
        configured_statement_timeout_ms.min(statement_limit),
    )
}

/// Fairness key after dependency-ready filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FairnessKey {
    pub correctness_priority: u8,
    pub overdue_ratio_millionths: u64,
    pub last_dispatch_ms: u64,
    pub tier_priority: u8,
    pub stable_id: i64,
}

impl Ord for FairnessKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.correctness_priority
            .cmp(&other.correctness_priority)
            .then(
                self.overdue_ratio_millionths
                    .cmp(&other.overdue_ratio_millionths),
            )
            .then(other.last_dispatch_ms.cmp(&self.last_dispatch_ms))
            .then(other.tier_priority.cmp(&self.tier_priority))
            .then(other.stable_id.cmp(&self.stable_id))
    }
}

impl PartialOrd for FairnessKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_uses_the_maximum_proxy() {
        let snapshot = LoadSnapshot::new(50, 100, 8, 4, 0, 0);
        assert_eq!(pressure_ratio(snapshot), 1.0);
    }

    #[test]
    fn pressure_includes_active_refreshes() {
        let snapshot = LoadSnapshot::new(0, 100, 6, 10, 0, 2);
        assert_eq!(pressure_ratio(snapshot), 0.8);
    }

    #[test]
    fn hysteresis_requires_two_healthy_ticks() {
        let mut state = PressureState::new();
        assert!(state.observe(0.9, 0.8));
        assert_eq!(state.deferral_factor(), 1);
        assert!(state.observe(0.9, 0.8));
        assert_eq!(state.deferral_factor(), 2);
        assert!(state.observe(0.5, 0.8));
        assert!(!state.observe(0.5, 0.8));
    }

    #[test]
    fn jitter_is_stable_and_bounded() {
        let a = deterministic_jitter_ms(1, 2, 3, 10_000, 1_000);
        assert_eq!(a, deterministic_jitter_ms(1, 2, 3, 10_000, 1_000));
        assert!(a < 1_000);
        assert_ne!(a, deterministic_jitter_ms(1, 2, 4, 10_000, 1_000));
    }

    #[test]
    fn deadlines_respect_floors_and_configured_maxima() {
        assert_eq!(
            derive_scheduled_deadlines(500, 30_000, 900_000),
            (100, 1_000)
        );
        assert_eq!(
            derive_scheduled_deadlines(10_000, 30_000, 900_000),
            (1_000, 8_000)
        );
    }

    #[test]
    fn fairness_prefers_overdue_and_then_oldest_dispatch() {
        let overdue = FairnessKey {
            correctness_priority: 1,
            overdue_ratio_millionths: 2_000_000,
            last_dispatch_ms: 200,
            tier_priority: 2,
            stable_id: 10,
        };
        let recent = FairnessKey {
            overdue_ratio_millionths: 1_000_000,
            ..overdue
        };
        assert!(overdue > recent);

        let older = FairnessKey {
            overdue_ratio_millionths: 1_000_000,
            last_dispatch_ms: 100,
            ..overdue
        };
        assert!(older > recent);
    }
}
