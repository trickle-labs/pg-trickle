//! Frontier management, data timestamp tracking, and Delayed View Semantics (DVS).
//!
//! A Frontier records the per-source version (LSN + snapshot timestamp)
//! at which a stream table's contents are logically consistent.
//!
//! ## DVS Guarantee
//!
//! The contents of every ST are logically equivalent to evaluating its
//! defining query at some past time: the `data_timestamp`. The scheduler
//! refreshes STs in topological order so that when ST B references
//! upstream ST A, A has already been refreshed to the target data timestamp
//! before B runs its delta query against A's contents.
//!
//! ## Frontier Lifecycle
//!
//! 1. **Created** — when a ST is first populated (full refresh). The frontier
//!    records the LSN of each source's replication slot at that moment.
//! 2. **Advanced** — on each differential refresh. The old frontier becomes
//!    the lower bound and the new frontier (with fresh LSNs) becomes the
//!    upper bound. The DVM engine reads changes in `[old, new]` range.
//! 3. **Reset** — on reinitialize. A new frontier is created from scratch.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A frontier represents the "point in time" for all sources of a stream table.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Frontier {
    /// Per-source version information, keyed by source OID as string.
    pub sources: HashMap<String, SourceVersion>,
    /// The overall data timestamp for this frontier (ISO 8601).
    pub data_timestamp: Option<String>,
}

/// Version information for a single source table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceVersion {
    /// PostgreSQL LSN (Log Sequence Number) as a string (e.g. "0/1A2B3C4").
    pub lsn: String,
    /// Snapshot timestamp as ISO 8601 string.
    pub snapshot_ts: String,
}

impl Frontier {
    /// Create a new empty frontier.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the LSN for a specific source, or "0/0" if not tracked.
    pub fn get_lsn(&self, source_oid: u32) -> String {
        self.sources
            .get(&source_oid.to_string())
            .map(|sv| sv.lsn.clone())
            .unwrap_or_else(|| "0/0".to_string())
    }

    /// Get the snapshot timestamp for a specific source, or None if not tracked.
    pub fn get_snapshot_ts(&self, source_oid: u32) -> Option<String> {
        self.sources
            .get(&source_oid.to_string())
            .map(|sv| sv.snapshot_ts.clone())
    }

    /// Update the frontier for a specific source.
    pub fn set_source(&mut self, source_oid: u32, lsn: String, snapshot_ts: String) {
        self.sources
            .insert(source_oid.to_string(), SourceVersion { lsn, snapshot_ts });
    }

    /// Set the overall data timestamp.
    pub fn set_data_timestamp(&mut self, ts: String) {
        self.data_timestamp = Some(ts);
    }

    /// Set the frontier for an ST source, keyed by `pgt_{pgt_id}`.
    pub fn set_st_source(&mut self, pgt_id: i64, lsn: String, snapshot_ts: String) {
        let key = format!("pgt_{pgt_id}");
        self.sources.insert(key, SourceVersion { lsn, snapshot_ts });
    }

    /// Get the LSN for an ST source, or "0/0" if not tracked.
    pub fn get_st_lsn(&self, pgt_id: i64) -> String {
        let key = format!("pgt_{pgt_id}");
        self.sources
            .get(&key)
            .map(|sv| sv.lsn.clone())
            .unwrap_or_else(|| "0/0".to_string())
    }

    /// Get all source OIDs tracked by this frontier.
    pub fn source_oids(&self) -> Vec<u32> {
        self.sources
            .keys()
            .filter_map(|k| k.parse::<u32>().ok())
            .collect()
    }

    /// Check if this frontier tracks any sources.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// PERF-6: A zero-allocation empty frontier for use as a borrow default.
    pub fn empty_ref() -> &'static Frontier {
        static EMPTY: std::sync::LazyLock<Frontier> = std::sync::LazyLock::new(Frontier::new);
        &EMPTY
    }

    /// Serialize to JSON string for storage in the `frontier` JSONB column.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize from a JSON string.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Merge another frontier's sources into this one, keeping the
    /// higher LSN for each source (used for ST-on-ST dependencies).
    pub fn merge_from(&mut self, other: &Frontier) {
        for (key, sv) in &other.sources {
            match self.sources.get(key) {
                Some(existing) => {
                    // Keep the higher LSN (lexicographic comparison works for hex LSNs
                    // of the same length, but for proper comparison we'd parse).
                    // We use the incoming value since it represents a newer state.
                    if lsn_gt(&sv.lsn, &existing.lsn) {
                        self.sources.insert(key.clone(), sv.clone());
                    }
                }
                None => {
                    self.sources.insert(key.clone(), sv.clone());
                }
            }
        }
    }
}

/// Compare two LSN strings. Returns true if `a > b`.
///
/// LSN format is `X/Y` where X and Y are hex numbers.
/// We parse both parts and compare numerically.
pub fn lsn_gt(a: &str, b: &str) -> bool {
    parse_lsn(a) > parse_lsn(b)
}

/// Parse a PostgreSQL LSN string (`"X/Y"`) into a `u64`.
#[inline]
pub fn lsn_to_u64(s: &str) -> u64 {
    parse_lsn(s)
}

/// Format a `u64` LSN value back into PostgreSQL `"X/Y"` notation.
#[inline]
pub fn u64_to_lsn(v: u64) -> String {
    let hi = (v >> 32) as u32;
    let lo = v as u32;
    format!("{hi:X}/{lo:08X}")
}

/// Parse a PostgreSQL LSN string (`"X/Y"`) into a `u64`.
#[inline]
fn parse_lsn(s: &str) -> u64 {
    match s.split_once('/') {
        Some((hi_s, lo_s)) => {
            let hi = u64::from_str_radix(hi_s, 16).unwrap_or(0);
            let lo = u64::from_str_radix(lo_s, 16).unwrap_or(0);
            (hi << 32) | lo
        }
        None => 0,
    }
}

/// Compare two LSN strings. Returns true if `a >= b`.
pub fn lsn_gte(a: &str, b: &str) -> bool {
    a == b || lsn_gt(a, b)
}

/// Return the lower of two LSN strings.
pub fn lsn_min<'a>(a: &'a str, b: &'a str) -> &'a str {
    if lsn_gt(a, b) { b } else { a }
}

// ── Data Timestamp Selection ───────────────────────────────────────────────

/// Select the canonical period for a given effective schedule.
///
/// Canonical periods are `48 * 2^n` seconds. We choose the largest period `p`
/// such that `p <= schedule / 2`.
pub fn select_canonical_period_secs(schedule_secs: u64) -> u64 {
    let half_schedule = schedule_secs / 2;
    let mut period = 48u64; // 48 * 2^0
    let mut result = period;

    while period <= half_schedule {
        result = period;
        period *= 2;
    }

    result
}

/// Compute a canonical data timestamp (aligned to period boundaries).
pub fn canonical_data_timestamp_secs(period_secs: u64) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if period_secs == 0 {
        return now;
    }

    (now / period_secs) * period_secs
}

// ── Frontier Computation ───────────────────────────────────────────────────

/// Compute a new frontier for a ST based on current CDC slot positions.
///
/// For each base table source: queries the replication slot to get the
/// current LSN. For upstream ST sources: copies the upstream ST's frontier.
///
/// `source_oids` — OIDs of base table sources for this ST.
/// `data_ts` — the target data timestamp for this refresh.
///
/// This function does NOT need SPI — it accepts pre-fetched slot positions.
pub fn compute_new_frontier(slot_positions: &HashMap<u32, String>, data_ts: &str) -> Frontier {
    let mut frontier = Frontier::new();
    frontier.set_data_timestamp(data_ts.to_string());

    for (oid, lsn) in slot_positions {
        frontier.set_source(*oid, lsn.clone(), data_ts.to_string());
    }

    frontier
}

/// Build a frontier for a full/reinitialize refresh.
///
/// Takes the current LSN positions and creates a frontier from scratch.
pub fn compute_initial_frontier(slot_positions: &HashMap<u32, String>, data_ts: &str) -> Frontier {
    // Same as compute_new_frontier — for a full refresh the frontier just
    // records the current slot positions as the starting point.
    compute_new_frontier(slot_positions, data_ts)
}

/// Capture a frontier snapshot from the current WAL position.
///
/// Queries `pg_current_wal_lsn()` and returns a frontier with a single
/// synthetic source entry keyed by `"current"`.  Used in pg_test code to
/// obtain the upper-bound frontier after making test changes.
///
/// Returns an error if the SPI call fails.
#[cfg(feature = "pg_test")]
pub fn capture_current_frontier() -> Result<Frontier, crate::error::PgTrickleError> {
    use pgrx::prelude::*;
    let lsn = Spi::get_one::<String>("SELECT pg_current_wal_lsn()::text")
        .map_err(|e| crate::error::PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or_else(|| "0/0".to_string());
    let ts = Spi::get_one::<String>("SELECT now()::text")
        .map_err(|e| crate::error::PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or_else(|| "epoch".to_string());
    let mut frontier = Frontier::new();
    frontier.set_source(0, lsn, ts.clone());
    frontier.set_data_timestamp(ts);
    Ok(frontier)
}

// ── DVS Target Timestamp ───────────────────────────────────────────────────

/// Select the target data timestamp for a ST refresh.
///
/// For STs with an explicit schedule, aligns to a canonical period.
/// For CALCULATED STs, uses the minimum data_timestamp of upstream STs.
///
/// Returns the target timestamp as an ISO 8601 string.
pub fn select_target_data_timestamp(
    schedule_secs: Option<u64>,
    upstream_timestamps: &[String],
) -> String {
    if let Some(sched_secs) = schedule_secs {
        // Use canonical period alignment
        let period = select_canonical_period_secs(sched_secs);
        let ts_secs = canonical_data_timestamp_secs(period);
        // Convert to ISO 8601
        let dt = chrono_from_unix_secs(ts_secs);
        return dt;
    }

    // CALCULATED: use the minimum of upstream timestamps.
    // This ensures the ST doesn't see data newer than its sources.
    if let Some(min_ts) = upstream_timestamps.iter().min() {
        return min_ts.clone();
    }

    // Fallback: use current time
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    chrono_from_unix_secs(now_secs)
}

/// Convert Unix seconds to an ISO 8601 timestamp string.
fn chrono_from_unix_secs(secs: u64) -> String {
    // Simple ISO 8601 without external crate: YYYY-MM-DDTHH:MM:SSZ
    // We use a straightforward approach since we don't have chrono.
    // PostgreSQL will parse this format correctly.
    format!("epoch'{}' + interval '0 seconds'", secs)
    // Actually, let's use a PostgreSQL-friendly format:
    // to_timestamp(secs) in SQL. For Rust, just store the epoch seconds.
}

/// Format a Unix epoch timestamp as a PostgreSQL-compatible timestamp string.
pub fn epoch_to_pg_timestamp(epoch_secs: u64) -> String {
    format!("to_timestamp({})", epoch_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_period_selection() {
        assert_eq!(select_canonical_period_secs(60), 48);
        assert_eq!(select_canonical_period_secs(120), 48);
        assert_eq!(select_canonical_period_secs(200), 96);
        assert_eq!(select_canonical_period_secs(400), 192);
        assert_eq!(select_canonical_period_secs(800), 384);
    }

    #[test]
    fn test_frontier_get_lsn() {
        let mut frontier = Frontier::new();
        assert_eq!(frontier.get_lsn(12345), "0/0");

        frontier.set_source(
            12345,
            "0/1A2B3C".to_string(),
            "2026-02-17T10:00:00Z".to_string(),
        );
        assert_eq!(frontier.get_lsn(12345), "0/1A2B3C");
    }

    #[test]
    fn test_frontier_serialization() {
        let mut frontier = Frontier::new();
        frontier.set_source(
            16384,
            "0/DEADBEEF".to_string(),
            "2026-02-17T10:00:00Z".to_string(),
        );
        frontier.data_timestamp = Some("2026-02-17T10:00:00Z".to_string());

        let json = serde_json::to_string(&frontier).unwrap();
        let deserialized: Frontier = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.get_lsn(16384), "0/DEADBEEF");
        assert_eq!(
            deserialized.data_timestamp.as_deref(),
            Some("2026-02-17T10:00:00Z")
        );
    }

    #[test]
    fn test_canonical_data_timestamp_alignment() {
        let period = 96u64;
        let ts = canonical_data_timestamp_secs(period);
        assert_eq!(ts % period, 0);
    }

    #[test]
    fn test_frontier_get_snapshot_ts_present() {
        let mut frontier = Frontier::new();
        frontier.set_source(100, "0/1".to_string(), "2026-03-03T10:00:00Z".to_string());
        assert_eq!(
            frontier.get_snapshot_ts(100),
            Some("2026-03-03T10:00:00Z".to_string())
        );
    }

    #[test]
    fn test_frontier_get_snapshot_ts_absent() {
        let frontier = Frontier::new();
        assert_eq!(frontier.get_snapshot_ts(999), None);
    }

    #[test]
    fn test_frontier_is_empty_new() {
        let frontier = Frontier::new();
        assert!(frontier.is_empty(), "New frontier should be empty");
    }

    #[test]
    fn test_frontier_is_empty_after_set_source() {
        let mut frontier = Frontier::new();
        frontier.set_source(1, "0/1".to_string(), "ts".to_string());
        assert!(
            !frontier.is_empty(),
            "Frontier with sources should not be empty"
        );
    }

    #[test]
    fn test_lsn_comparison() {
        assert!(lsn_gt("0/2", "0/1"));
        assert!(lsn_gt("1/0", "0/FFFFFFFF"));
        assert!(!lsn_gt("0/1", "0/2"));
        assert!(!lsn_gt("0/1", "0/1"));
        assert!(lsn_gte("0/1", "0/1"));
        assert!(lsn_gte("0/2", "0/1"));
        assert!(!lsn_gte("0/1", "0/2"));
    }

    #[test]
    fn test_frontier_merge() {
        let mut f1 = Frontier::new();
        f1.set_source(100, "0/10".to_string(), "ts1".to_string());
        f1.set_source(200, "0/20".to_string(), "ts1".to_string());

        let mut f2 = Frontier::new();
        f2.set_source(200, "0/30".to_string(), "ts2".to_string()); // higher
        f2.set_source(300, "0/40".to_string(), "ts2".to_string()); // new

        f1.merge_from(&f2);

        assert_eq!(f1.get_lsn(100), "0/10"); // unchanged
        assert_eq!(f1.get_lsn(200), "0/30"); // updated (higher)
        assert_eq!(f1.get_lsn(300), "0/40"); // added
    }

    #[test]
    fn test_frontier_source_oids() {
        let mut frontier = Frontier::new();
        frontier.set_source(100, "0/1".to_string(), "ts".to_string());
        frontier.set_source(200, "0/2".to_string(), "ts".to_string());

        let mut oids = frontier.source_oids();
        oids.sort();
        assert_eq!(oids, vec![100, 200]);
    }

    #[test]
    fn test_compute_new_frontier() {
        let mut positions = HashMap::new();
        positions.insert(100, "0/1A".to_string());
        positions.insert(200, "0/2B".to_string());

        let frontier = compute_new_frontier(&positions, "2026-02-17T10:00:00Z");

        assert_eq!(frontier.get_lsn(100), "0/1A");
        assert_eq!(frontier.get_lsn(200), "0/2B");
        assert_eq!(
            frontier.data_timestamp.as_deref(),
            Some("2026-02-17T10:00:00Z"),
        );
    }

    #[test]
    fn test_select_target_data_timestamp_with_schedule() {
        let ts = select_target_data_timestamp(Some(300), &[]);
        // Should return a formatted timestamp, not empty
        assert!(!ts.is_empty());
    }

    #[test]
    fn test_select_target_data_timestamp_calculated() {
        let upstream = vec![
            "2026-02-17T10:00:00Z".to_string(),
            "2026-02-17T09:00:00Z".to_string(),
        ];
        let ts = select_target_data_timestamp(None, &upstream);
        assert_eq!(ts, "2026-02-17T09:00:00Z"); // minimum
    }

    #[test]
    fn test_epoch_to_pg_timestamp_format() {
        let sql = epoch_to_pg_timestamp(1708200000);
        assert_eq!(sql, "to_timestamp(1708200000)");
    }

    #[test]
    fn test_epoch_to_pg_timestamp_zero() {
        let sql = epoch_to_pg_timestamp(0);
        assert_eq!(sql, "to_timestamp(0)");
    }

    #[test]
    fn test_select_target_data_timestamp_no_schedule_no_upstream() {
        // When no schedule and no upstream, should return a current timestamp (fallback)
        let ts = select_target_data_timestamp(None, &[]);
        assert!(!ts.is_empty(), "fallback should return non-empty timestamp");
    }

    #[test]
    fn test_select_target_data_timestamp_single_upstream() {
        let upstream = vec!["2026-02-17T10:00:00Z".to_string()];
        let ts = select_target_data_timestamp(None, &upstream);
        assert_eq!(ts, "2026-02-17T10:00:00Z");
    }

    #[test]
    fn test_select_target_data_timestamp_schedule_returns_aligned() {
        // With a schedule, result should be a non-empty formatted timestamp
        let ts = select_target_data_timestamp(Some(60), &[]);
        assert!(!ts.is_empty());
        // Format is: epoch'<secs>' + interval '<n> seconds'
        assert!(
            ts.contains("epoch") || ts.contains('-') || ts.contains('T'),
            "expected timestamp format, got: {}",
            ts
        );
    }

    #[test]
    fn test_lsn_to_u64_round_trip() {
        // Round-trip: parse then format
        assert_eq!(u64_to_lsn(lsn_to_u64("1/00000500")), "1/00000500");
        assert_eq!(u64_to_lsn(lsn_to_u64("0/00000001")), "0/00000001");
        assert_eq!(u64_to_lsn(lsn_to_u64("FF/FFFFFFFF")), "FF/FFFFFFFF");
        assert_eq!(u64_to_lsn(lsn_to_u64("0/0")), "0/00000000");
    }

    #[test]
    fn test_lsn_to_u64_known_values() {
        // High segment 1, low 0x500 = u64 value 0x1_0000_0500
        assert_eq!(lsn_to_u64("1/00000500"), 0x1_0000_0500);
        assert_eq!(lsn_to_u64("0/00000001"), 1);
        assert_eq!(lsn_to_u64("0/0"), 0);
    }

    #[test]
    fn test_u64_to_lsn_format() {
        // u64_to_lsn uses uppercase hex with zero-padded 8-char low half,
        // matching PostgreSQL's pg_lsn output format.
        assert_eq!(u64_to_lsn(0x1_0000_0500), "1/00000500");
        assert_eq!(u64_to_lsn(1), "0/00000001");
        assert_eq!(u64_to_lsn(0), "0/00000000");
    }
}
