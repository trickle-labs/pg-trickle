//! METR-3 (v0.27.0): `metrics_summary()` SQL function.
//!
//! Returns a cluster-wide aggregation of key pg_trickle counters from
//! `pgtrickle.pgt_stream_tables` and `pgt_refresh_history`.
//! Designed as the data source for the Grafana cluster-overview dashboard.
//!
//! # Grafana query example
//! ```sql
//! SELECT * FROM pgtrickle.metrics_summary();
//! ```

use pgrx::prelude::*;

// ── METR-3: metrics_summary ────────────────────────────────────────────────

/// METR-3 (v0.27.0): Cluster-wide metrics summary for the Grafana
/// overview dashboard.
///
/// Aggregates refresh counts, error counts, and worker utilisation from
/// all stream tables registered in this database. Includes `db_name` so
/// multi-database Grafana panels can use this as a single data source.
///
/// v0.31.0 (PERF-3): Added `ivm_lock_parse_error_count` — cumulative count
/// of IMMEDIATE-mode lock-mode downgrades due to query parse failures.
#[pg_extern(schema = "pgtrickle")]
#[allow(clippy::type_complexity)]
pub fn metrics_summary() -> TableIterator<
    'static,
    (
        name!(db_name, Option<String>),
        name!(total_stream_tables, Option<i64>),
        name!(active_stream_tables, Option<i64>),
        name!(suspended_stream_tables, Option<i64>),
        name!(total_refreshes, Option<i64>),
        name!(successful_refreshes, Option<i64>),
        name!(failed_refreshes, Option<i64>),
        name!(total_rows_processed, Option<i64>),
        name!(active_workers, Option<i32>),
        name!(ivm_lock_parse_error_count, Option<i64>),
        name!(holdback_probe_calls, Option<i64>),
        name!(holdback_probe_cache_hits, Option<i64>),
        name!(holdback_probe_avg_ms, Option<f64>),
    ),
> {
    let rows = metrics_summary_impl();
    TableIterator::new(rows)
}

#[allow(clippy::type_complexity)]
fn metrics_summary_impl() -> Vec<(
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i32>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<f64>,
)> {
    let active_workers = crate::shmem::active_worker_count() as i32;
    // PERF-3 (v0.31.0): Read the IVM lock-mode parse error counter.
    let ivm_parse_errors = crate::shmem::read_ivm_lock_parse_errors() as i64;
    let (probe_calls, probe_cache_hits, probe_total_ms, _) =
        crate::shmem::read_holdback_probe_metrics();
    let probe_avg_ms = compute_probe_avg_ms(probe_calls, probe_total_ms);

    let row = Spi::connect(|client| {
        let result = client.select(
            "SELECT \
               current_database()::text                                             AS db_name, \
               COUNT(*)                                                              AS total, \
               COUNT(*) FILTER (WHERE status = 'ACTIVE')                            AS active, \
               COUNT(*) FILTER (WHERE status = 'SUSPENDED')                         AS suspended, \
                             COALESCE(SUM(h.total_refreshes), 0)                                   AS total_refreshes, \
                             COALESCE(SUM(h.successful_refreshes), 0)                              AS successful_refreshes, \
                             COALESCE(SUM(h.failed_refreshes), 0)                                  AS failed_refreshes, \
                             COALESCE(SUM(h.total_rows_inserted + h.total_rows_deleted), 0)        AS total_rows \
             FROM pgtrickle.pgt_stream_tables s \
                         LEFT JOIN pgtrickle.pgt_refresh_summary h \
                             ON h.pgt_id = s.pgt_id",
            None,
            &[],
        );

        match result {
            Ok(rows) => rows.into_iter().next().map(|row| {
                let db_name = row.get::<String>(1).ok().flatten();
                let total = row.get::<i64>(2).ok().flatten();
                let active = row.get::<i64>(3).ok().flatten();
                let suspended = row.get::<i64>(4).ok().flatten();
                let total_refreshes = row.get::<i64>(5).ok().flatten();
                let successful = row.get::<i64>(6).ok().flatten();
                let failed = row.get::<i64>(7).ok().flatten();
                let rows_processed = row.get::<i64>(8).ok().flatten();
                (
                    db_name,
                    total,
                    active,
                    suspended,
                    total_refreshes,
                    successful,
                    failed,
                    rows_processed,
                )
            }),
            Err(_) => None,
        }
    });

    match row {
        Some((db, total, active, susp, tr, sr, fr, rp)) => {
            vec![(
                db,
                total,
                active,
                susp,
                tr,
                sr,
                fr,
                rp,
                Some(active_workers),
                Some(ivm_parse_errors),
                Some(probe_calls as i64),
                Some(probe_cache_hits as i64),
                Some(probe_avg_ms),
            )]
        }
        None => Vec::new(),
    }
}

/// CODE-002: Pure helper — compute average probe latency in milliseconds.
///
/// Returns 0.0 when `probe_calls` is 0 to avoid division by zero.
pub(crate) fn compute_probe_avg_ms(probe_calls: u64, probe_total_ms: u64) -> f64 {
    if probe_calls > 0 {
        probe_total_ms as f64 / probe_calls as f64
    } else {
        0.0
    }
}

// CODE-002: Unit tests for pure helpers in metrics_ext.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_probe_avg_ms_zero_calls_returns_zero() {
        assert_eq!(compute_probe_avg_ms(0, 0), 0.0);
    }

    #[test]
    fn test_compute_probe_avg_ms_zero_calls_nonzero_total_returns_zero() {
        // If probe_calls == 0 we never divide, regardless of probe_total_ms.
        assert_eq!(compute_probe_avg_ms(0, 99999), 0.0);
    }

    #[test]
    fn test_compute_probe_avg_ms_single_call() {
        assert_eq!(compute_probe_avg_ms(1, 42), 42.0);
    }

    #[test]
    fn test_compute_probe_avg_ms_multiple_calls() {
        // 10 calls, 100ms total → 10ms average
        assert_eq!(compute_probe_avg_ms(10, 100), 10.0);
    }

    #[test]
    fn test_compute_probe_avg_ms_fractional_result() {
        // 3 calls, 10ms total → 3.333... ms average
        let result = compute_probe_avg_ms(3, 10);
        assert!(
            (result - 10.0 / 3.0).abs() < 1e-9,
            "expected 10/3, got {result}"
        );
    }

    #[test]
    fn test_compute_probe_avg_ms_large_values() {
        // Ensure no overflow or precision loss with large-ish counts.
        let result = compute_probe_avg_ms(1_000_000, 5_000_000);
        assert_eq!(result, 5.0);
    }
}
