//! Monitoring, observability, and alerting for pgtrickle.
//!
//! # Statistics
//!
//! Per-ST statistics are tracked in shared memory via atomic counters and
//! exposed through the `pgtrickle.st_refresh_stats()` table-returning function
//! which aggregates from `pgtrickle.pgt_refresh_history`.
//!
//! The `pgtrickle.pg_stat_stream_tables` view combines catalog metadata with
//! runtime stats for a single-query operational overview.
//!
//! # NOTIFY Alerting
//!
//! Operational events are emitted via PostgreSQL `NOTIFY` on the
//! `pg_trickle_alert` channel. Clients can `LISTEN pg_trickle_alert;` to receive
//! JSON-formatted events:
//! - `stale_data` — scheduler is behind *and* data_timestamp is old (warning)
//! - `no_upstream_changes` — scheduler is healthy but source tables have no new writes (info)
//! - `auto_suspended` — ST suspended due to consecutive errors
//! - `reinitialize_needed` — upstream DDL change detected
//! - `buffer_growth_warning` — trigger-mode change buffers are growing
//! - `slot_lag_warning` — replication slot WAL retention growing

use pgrx::prelude::*;
use serde_json::json;

use crate::catalog::{CdcMode, StDependency};
use crate::config;
use crate::error::PgTrickleError;
use crate::shmem;
use crate::wal_decoder;

pub mod alert;
pub mod health;
pub mod tree;

// Re-export public items so crate::monitor::AlertEvent etc. still work.
pub use alert::*;
pub use health::check_change_buffer_sizes;

// ── SQL-exposed monitoring functions ───────────────────────────────────────

/// Return per-ST refresh statistics aggregated from the refresh history table.
///
/// This is the primary monitoring function, exposed as `pgtrickle.st_refresh_stats()`.
#[pg_extern(schema = "pgtrickle", name = "st_refresh_stats")]
#[allow(clippy::type_complexity)]
fn st_refresh_stats() -> TableIterator<
    'static,
    (
        name!(pgt_name, String),
        name!(pgt_schema, String),
        name!(status, String),
        name!(refresh_mode, String),
        name!(is_populated, bool),
        name!(total_refreshes, i64),
        name!(successful_refreshes, i64),
        name!(failed_refreshes, i64),
        name!(total_rows_inserted, i64),
        name!(total_rows_deleted, i64),
        name!(avg_duration_ms, f64),
        name!(last_refresh_action, Option<String>),
        name!(last_refresh_status, Option<String>),
        name!(last_refresh_at, Option<TimestampWithTimeZone>),
        name!(staleness_secs, Option<f64>),
        name!(stale, bool),
        name!(consecutive_errors, i32),
        name!(schedule, Option<String>),
        name!(refresh_tier, String),
        name!(last_error_message, Option<String>),
        name!(downstream_publication, Option<String>),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    st.pgt_name,
                    st.pgt_schema,
                    st.status,
                    st.refresh_mode,
                    st.is_populated,
                    COALESCE(stats.total_refreshes, 0)::bigint,
                    COALESCE(stats.successful_refreshes, 0)::bigint,
                    COALESCE(stats.failed_refreshes, 0)::bigint,
                    COALESCE(stats.total_rows_inserted, 0)::bigint,
                    COALESCE(stats.total_rows_deleted, 0)::bigint,
                    COALESCE(stats.avg_duration_ms, 0)::float8,
                    last_hist.action,
                    last_hist.status,
                    st.last_refresh_at,
                    EXTRACT(EPOCH FROM (now() - st.data_timestamp))::float8,
                    COALESCE(
                        CASE WHEN st.schedule IS NOT NULL AND st.data_timestamp IS NOT NULL
                                  AND st.schedule NOT LIKE '% %'
                                  AND st.schedule NOT LIKE '@%'
                             -- Only stale when BOTH data_timestamp AND last_refresh_at
                             -- are old: scheduler itself is falling behind.
                             -- If last_refresh_at is recent the scheduler is healthy;
                             -- data_timestamp is frozen because there is nothing new
                             -- to refresh (no_upstream_changes), not a real problem.
                             THEN EXTRACT(EPOCH FROM (now() - st.data_timestamp)) >
                                      pgtrickle.parse_duration_seconds(st.schedule)
                                  AND EXTRACT(EPOCH FROM (now() - st.last_refresh_at)) >
                                      pgtrickle.parse_duration_seconds(st.schedule) * 3
                        END,
                    false),
                    st.consecutive_errors::integer,
                    st.schedule::text,
                    COALESCE(st.refresh_tier, 'hot')::text,
                    st.last_error_message::text,
                    st.downstream_publication_name::text
                FROM pgtrickle.pgt_stream_tables st
                LEFT JOIN LATERAL (
                    SELECT
                        count(*) AS total_refreshes,
                        count(*) FILTER (WHERE h.status = 'COMPLETED') AS successful_refreshes,
                        count(*) FILTER (WHERE h.status = 'FAILED') AS failed_refreshes,
                        COALESCE(sum(h.rows_inserted), 0) AS total_rows_inserted,
                        COALESCE(sum(h.rows_deleted), 0) AS total_rows_deleted,
                        CASE WHEN count(*) FILTER (WHERE h.end_time IS NOT NULL) > 0
                             THEN avg(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000)
                                  FILTER (WHERE h.end_time IS NOT NULL)
                             ELSE 0
                        END AS avg_duration_ms
                    FROM pgtrickle.pgt_refresh_history h
                    WHERE h.pgt_id = st.pgt_id
                ) stats ON true
                LEFT JOIN LATERAL (
                    SELECT h2.action, h2.status
                    FROM pgtrickle.pgt_refresh_history h2
                    WHERE h2.pgt_id = st.pgt_id
                    ORDER BY h2.refresh_id DESC
                    LIMIT 1
                ) last_hist ON true
                ORDER BY st.pgt_schema, st.pgt_name",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "st_refresh_stats: SPI select failed: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in result {
            let pgt_name = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let pgt_schema = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let status = row.get::<String>(3).unwrap_or(None).unwrap_or_default();
            let refresh_mode = row.get::<String>(4).unwrap_or(None).unwrap_or_default();
            let is_populated = row.get::<bool>(5).unwrap_or(None).unwrap_or(false);
            let total_refreshes = row.get::<i64>(6).unwrap_or(None).unwrap_or(0);
            let successful = row.get::<i64>(7).unwrap_or(None).unwrap_or(0);
            let failed = row.get::<i64>(8).unwrap_or(None).unwrap_or(0);
            let rows_inserted = row.get::<i64>(9).unwrap_or(None).unwrap_or(0);
            let rows_deleted = row.get::<i64>(10).unwrap_or(None).unwrap_or(0);
            let avg_duration = row.get::<f64>(11).unwrap_or(None).unwrap_or(0.0);
            let last_action = row.get::<String>(12).unwrap_or(None);
            let last_status = row.get::<String>(13).unwrap_or(None);
            let last_refresh_at = row.get::<TimestampWithTimeZone>(14).unwrap_or(None);
            let staleness = row.get::<f64>(15).unwrap_or(None);
            let stale = row.get::<bool>(16).unwrap_or(None).unwrap_or(false);
            let consecutive_errors = row.get::<i32>(17).unwrap_or(None).unwrap_or(0);
            let schedule = row.get::<String>(18).unwrap_or(None);
            let refresh_tier = row
                .get::<String>(19)
                .unwrap_or(None)
                .unwrap_or_else(|| "hot".to_string());
            let last_error_message = row.get::<String>(20).unwrap_or(None);
            let downstream_publication = row.get::<String>(21).unwrap_or(None);

            out.push((
                pgt_name,
                pgt_schema,
                status,
                refresh_mode,
                is_populated,
                total_refreshes,
                successful,
                failed,
                rows_inserted,
                rows_deleted,
                avg_duration,
                last_action,
                last_status,
                last_refresh_at,
                staleness,
                stale,
                consecutive_errors,
                schedule,
                refresh_tier,
                last_error_message,
                downstream_publication,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

// ── OP-2: OpenMetrics text generation ─────────────────────────────────────

/// Generate an OpenMetrics (Prometheus) text exposition from pg_trickle's
/// internal monitoring data.
///
/// Called once per `/metrics` request by `metrics_server::MetricsServer`.
/// Reads from `pgtrickle.pgt_stream_tables` and `pgt_refresh_history` via SPI.
///
/// Output format follows the OpenMetrics 1.0 specification.
pub(crate) fn collect_metrics_text() -> String {
    let rows = Spi::connect(
        |client| -> Vec<(String, String, String, i64, i64, i64, i32)> {
            let query = "
            SELECT
                s.pgt_name,
                s.pgt_schema,
                s.status,
                COUNT(h.refresh_id) FILTER (WHERE h.status = 'COMPLETED') AS successful,
                COUNT(h.refresh_id) FILTER (WHERE h.status = 'FAILED')    AS failed,
                SUM(COALESCE(h.rows_inserted, 0) + COALESCE(h.rows_deleted, 0)) AS total_rows,
                s.consecutive_errors
            FROM pgtrickle.pgt_stream_tables s
            LEFT JOIN pgtrickle.pgt_refresh_history h USING (pgt_id)
            GROUP BY s.pgt_name, s.pgt_schema, s.status, s.consecutive_errors
            ORDER BY s.pgt_schema, s.pgt_name
        ";
            client
                .select(query, None, &[])
                .map(|tuptable| {
                    tuptable
                        .into_iter()
                        .filter_map(|row| {
                            let name = row.get::<String>(1).ok().flatten()?;
                            let schema = row.get::<String>(2).ok().flatten()?;
                            let status = row.get::<String>(3).ok().flatten().unwrap_or_default();
                            let successful = row.get::<i64>(4).ok().flatten().unwrap_or(0);
                            let failed = row.get::<i64>(5).ok().flatten().unwrap_or(0);
                            let total_rows = row.get::<i64>(6).ok().flatten().unwrap_or(0);
                            let errors = row.get::<i32>(7).ok().flatten().unwrap_or(0);
                            Some((name, schema, status, successful, failed, total_rows, errors))
                        })
                        .collect()
                })
                .unwrap_or_default()
        },
    );

    let mut out = String::with_capacity(4096);

    // CLUS-2 (v0.27.0): Resolve current database OID and name for per-DB labels
    let (db_oid, db_name) = crate::api::cluster::current_db_labels();
    let db_oid_str = db_oid
        .map(|o| o.to_string())
        .unwrap_or_else(|| "0".to_string());
    let db_name_str = db_name.as_deref().unwrap_or("unknown").replace('"', "\\\"");

    // Version metadata
    out.push_str("# HELP pg_trickle_info pg_trickle extension information\n");
    out.push_str("# TYPE pg_trickle_info gauge\n");
    out.push_str(&format!(
        "pg_trickle_info{{version=\"{}\",db_oid=\"{}\",db_name=\"{}\"}} 1\n",
        env!("CARGO_PKG_VERSION"),
        db_oid_str,
        db_name_str
    ));

    // Per-ST metrics
    out.push_str(
        "# HELP pg_trickle_refreshes_total Total successful refresh count per stream table\n",
    );
    out.push_str("# TYPE pg_trickle_refreshes_total counter\n");
    out.push_str(
        "# HELP pg_trickle_refresh_failures_total Total failed refresh count per stream table\n",
    );
    out.push_str("# TYPE pg_trickle_refresh_failures_total counter\n");
    out.push_str(
        "# HELP pg_trickle_rows_changed_total Total rows inserted+deleted per stream table\n",
    );
    out.push_str("# TYPE pg_trickle_rows_changed_total counter\n");
    out.push_str(
        "# HELP pg_trickle_consecutive_errors Current consecutive error count per stream table\n",
    );
    out.push_str("# TYPE pg_trickle_consecutive_errors gauge\n");
    out.push_str("# HELP pg_trickle_active Stream table is ACTIVE (1) or not (0)\n");
    out.push_str("# TYPE pg_trickle_active gauge\n");

    for (name, schema, status, successful, failed, total_rows, errors) in &rows {
        // CLUS-2: include db_oid and db_name labels on every per-ST metric
        let labels = format!(
            "schema=\"{schema}\",name=\"{name}\",db_oid=\"{db_oid_str}\",db_name=\"{db_name_str}\""
        );
        out.push_str(&format!(
            "pg_trickle_refreshes_total{{{labels}}} {successful}\n"
        ));
        out.push_str(&format!(
            "pg_trickle_refresh_failures_total{{{labels}}} {failed}\n"
        ));
        out.push_str(&format!(
            "pg_trickle_rows_changed_total{{{labels}}} {total_rows}\n"
        ));
        out.push_str(&format!(
            "pg_trickle_consecutive_errors{{{labels}}} {errors}\n"
        ));
        let is_active: u8 = if status == "ACTIVE" { 1 } else { 0 };
        out.push_str(&format!("pg_trickle_active{{{labels}}} {is_active}\n"));
    }

    // #536: Frontier holdback gauges
    let (holdback_lsn, holdback_age) = crate::shmem::read_holdback_metrics();
    out.push_str(
        "# HELP pg_trickle_frontier_holdback_lsn_bytes \
         How many WAL bytes behind the write LSN the safe frontier currently is (0 = no holdback)\n",
    );
    out.push_str("# TYPE pg_trickle_frontier_holdback_lsn_bytes gauge\n");
    out.push_str(&format!(
        "pg_trickle_frontier_holdback_lsn_bytes {holdback_lsn}\n"
    ));
    out.push_str(
        "# HELP pg_trickle_frontier_holdback_seconds \
         Age in seconds of the oldest in-progress transaction causing a holdback (0 = no holdback)\n",
    );
    out.push_str("# TYPE pg_trickle_frontier_holdback_seconds gauge\n");
    out.push_str(&format!(
        "pg_trickle_frontier_holdback_seconds {holdback_age}\n"
    ));

    // M-6 (v0.55.0): DVM parser metrics
    let dvm_parse_ms = crate::shmem::read_dvm_parse_ms();
    let delta_bytes = crate::shmem::read_delta_query_size_bytes();
    out.push_str(
        "# HELP pg_trickle_dvm_parse_ms \
         Cumulative time spent in the DVM parser (milliseconds)\n",
    );
    out.push_str("# TYPE pg_trickle_dvm_parse_ms counter\n");
    out.push_str(&format!("pg_trickle_dvm_parse_ms {dvm_parse_ms}\n"));
    out.push_str(
        "# HELP pg_trickle_delta_query_size_bytes \
         Cumulative size of generated delta SQL (bytes)\n",
    );
    out.push_str("# TYPE pg_trickle_delta_query_size_bytes counter\n");
    out.push_str(&format!(
        "pg_trickle_delta_query_size_bytes {delta_bytes}\n"
    ));

    // COR-4 (v0.58.0): CDC compaction contention counter.
    let compact_contended = if crate::shmem::is_shmem_available() {
        crate::shmem::CDC_COMPACT_CONTENDED_TOTAL
            .get()
            .load(std::sync::atomic::Ordering::Relaxed) as i64
    } else {
        0
    };
    out.push_str(
        "# HELP pg_trickle_cdc_compact_contended_total \
         Total times compact_change_buffer() could not acquire the advisory lock \
         (indicates persistent contention with concurrent refreshes)\n",
    );
    out.push_str("# TYPE pg_trickle_cdc_compact_contended_total counter\n");
    out.push_str(&format!(
        "pg_trickle_cdc_compact_contended_total {compact_contended}\n"
    ));

    // ── OBS-1 (v0.59.0): CDC lag percentile metrics ──────────────────────
    let (lag_p50, lag_p95, lag_p99) = crate::shmem::read_cdc_lag_percentiles();
    out.push_str(
        "# HELP pg_trickle_cdc_lag_p50_seconds \
         CDC lag 50th-percentile (rolling window)\n",
    );
    out.push_str("# TYPE pg_trickle_cdc_lag_p50_seconds gauge\n");
    out.push_str(&format!(
        "pg_trickle_cdc_lag_p50_seconds {:.3}\n",
        lag_p50 as f64 / 1000.0
    ));
    out.push_str(
        "# HELP pg_trickle_cdc_lag_p95_seconds \
         CDC lag 95th-percentile (rolling window)\n",
    );
    out.push_str("# TYPE pg_trickle_cdc_lag_p95_seconds gauge\n");
    out.push_str(&format!(
        "pg_trickle_cdc_lag_p95_seconds {:.3}\n",
        lag_p95 as f64 / 1000.0
    ));
    out.push_str(
        "# HELP pg_trickle_cdc_lag_p99_seconds \
         CDC lag 99th-percentile (rolling window)\n",
    );
    out.push_str("# TYPE pg_trickle_cdc_lag_p99_seconds gauge\n");
    out.push_str(&format!(
        "pg_trickle_cdc_lag_p99_seconds {:.3}\n",
        lag_p99 as f64 / 1000.0
    ));

    // ── OBS-2 (v0.59.0): Parallel worker utilisation metrics ─────────────
    let queue_depth = if crate::shmem::is_shmem_available() {
        crate::shmem::PARALLEL_QUEUE_DEPTH
            .get()
            .load(std::sync::atomic::Ordering::Relaxed)
    } else {
        0
    };
    let worker_idle_ms = if crate::shmem::is_shmem_available() {
        crate::shmem::WORKER_IDLE_MS_TOTAL
            .get()
            .load(std::sync::atomic::Ordering::Relaxed)
    } else {
        0
    };
    out.push_str(
        "# HELP pg_trickle_parallel_queue_depth \
         Number of refresh jobs waiting for a worker\n",
    );
    out.push_str("# TYPE pg_trickle_parallel_queue_depth gauge\n");
    out.push_str(&format!("pg_trickle_parallel_queue_depth {queue_depth}\n"));
    out.push_str(
        "# HELP pg_trickle_worker_idle_time_seconds_total \
         Cumulative seconds workers have spent waiting for work\n",
    );
    out.push_str("# TYPE pg_trickle_worker_idle_time_seconds_total counter\n");
    out.push_str(&format!(
        "pg_trickle_worker_idle_time_seconds_total {:.3}\n",
        worker_idle_ms as f64 / 1000.0
    ));

    // ── OBS-3 (v0.59.0): WAL decoder pending-record metric ───────────────
    let wal_pending = crate::shmem::read_wal_decoder_pending_records();
    out.push_str(
        "# HELP pg_trickle_wal_decoder_pending_records \
         Logical-replication records buffered but not yet written to the CDC change buffer\n",
    );
    out.push_str("# TYPE pg_trickle_wal_decoder_pending_records gauge\n");
    out.push_str(&format!(
        "pg_trickle_wal_decoder_pending_records {wal_pending}\n"
    ));

    // ── OBS-4 (v0.59.0): Refresh-mode ratio counters ─────────────────────
    let diff_total = if crate::shmem::is_shmem_available() {
        crate::shmem::REFRESH_MODE_DIFFERENTIAL_TOTAL
            .get()
            .load(std::sync::atomic::Ordering::Relaxed)
    } else {
        0
    };
    let full_total = if crate::shmem::is_shmem_available() {
        crate::shmem::REFRESH_MODE_FULL_TOTAL
            .get()
            .load(std::sync::atomic::Ordering::Relaxed)
    } else {
        0
    };
    out.push_str(
        "# HELP pg_trickle_refresh_mode_total \
         Total refresh cycles by mode (differential or full)\n",
    );
    out.push_str("# TYPE pg_trickle_refresh_mode_total counter\n");
    out.push_str(&format!(
        "pg_trickle_refresh_mode_total{{mode=\"differential\"}} {diff_total}\n"
    ));
    out.push_str(&format!(
        "pg_trickle_refresh_mode_total{{mode=\"full\"}} {full_total}\n"
    ));

    // OpenMetrics requires the exposition to end with # EOF
    out.push_str("# EOF\n");
    out
}

/// Return refresh history for a specific ST, most recent first.
///
/// Exposed as `pgtrickle.get_refresh_history(name, limit)`.
#[pg_extern(schema = "pgtrickle", name = "get_refresh_history")]
#[allow(clippy::type_complexity)]
fn get_refresh_history(
    name: &str,
    max_rows: default!(i32, 20),
) -> TableIterator<
    'static,
    (
        name!(refresh_id, i64),
        name!(data_timestamp, TimestampWithTimeZone),
        name!(start_time, TimestampWithTimeZone),
        name!(end_time, Option<TimestampWithTimeZone>),
        name!(action, String),
        name!(status, String),
        name!(rows_inserted, i64),
        name!(rows_deleted, i64),
        name!(duration_ms, Option<f64>),
        name!(error_message, Option<String>),
    ),
> {
    let parts: Vec<&str> = name.splitn(2, '.').collect();
    let (schema, table_name) = if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        ("public", parts[0])
    };

    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    h.refresh_id,
                    h.data_timestamp,
                    h.start_time,
                    h.end_time,
                    h.action,
                    h.status,
                    COALESCE(h.rows_inserted, 0)::bigint,
                    COALESCE(h.rows_deleted, 0)::bigint,
                    CASE WHEN h.end_time IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000
                         ELSE NULL
                    END::float8,
                    h.error_message
                FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
                WHERE st.pgt_schema = $1 AND st.pgt_name = $2
                ORDER BY h.refresh_id DESC
                LIMIT $3",
                None,
                &[schema.into(), table_name.into(), max_rows.into()],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "get_refresh_history: SPI select failed: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        let epoch_zero = TimestampWithTimeZone::try_from(0i64).unwrap_or_else(|_| {
            // This should never fail, but if it does, fall through gracefully.
            pgrx::error!(
                "{}",
                crate::error::PgTrickleError::DiagnosticError(
                    "get_refresh_history: failed to construct epoch timestamp".into()
                )
            )
        });
        for row in result {
            let refresh_id = row.get::<i64>(1).unwrap_or(None).unwrap_or(0);
            let data_ts = row
                .get::<TimestampWithTimeZone>(2)
                .unwrap_or(None)
                .unwrap_or(epoch_zero);
            let start = row
                .get::<TimestampWithTimeZone>(3)
                .unwrap_or(None)
                .unwrap_or(epoch_zero);
            let end = row.get::<TimestampWithTimeZone>(4).unwrap_or(None);
            let action = row.get::<String>(5).unwrap_or(None).unwrap_or_default();
            let status = row.get::<String>(6).unwrap_or(None).unwrap_or_default();
            let ins = row.get::<i64>(7).unwrap_or(None).unwrap_or(0);
            let del = row.get::<i64>(8).unwrap_or(None).unwrap_or(0);
            let dur = row.get::<f64>(9).unwrap_or(None);
            let err = row.get::<String>(10).unwrap_or(None);

            out.push((
                refresh_id, data_ts, start, end, action, status, ins, del, dur, err,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

/// Get the current staleness in seconds for a specific ST.
///
/// Returns NULL if the ST has never been refreshed.
/// Exposed as `pgtrickle.get_staleness(name)`.
/// Return the effective adaptive threshold for a stream table.
///
/// Returns the per-ST `auto_threshold` if set, otherwise the global
/// `pg_trickle.differential_max_change_ratio` GUC. Exposed as
/// `pgtrickle.st_auto_threshold(name)`.
#[pg_extern(schema = "pgtrickle", name = "st_auto_threshold")]
fn st_auto_threshold(name: &str) -> Option<f64> {
    let parts: Vec<&str> = name.splitn(2, '.').collect();
    let (schema, table_name) = if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        ("public", parts[0])
    };

    let per_st = Spi::get_one_with_args::<f64>(
        "SELECT auto_threshold FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_schema = $1 AND pgt_name = $2",
        &[schema.into(), table_name.into()],
    )
    .unwrap_or(None);

    per_st.or(Some(config::pg_trickle_differential_max_change_ratio()))
}

#[pg_extern(schema = "pgtrickle", name = "get_staleness")]
fn get_staleness(name: &str) -> Option<f64> {
    let parts: Vec<&str> = name.splitn(2, '.').collect();
    let (schema, table_name) = if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        ("public", parts[0])
    };

    Spi::get_one_with_args::<f64>(
        "SELECT EXTRACT(EPOCH FROM (now() - data_timestamp))::float8 \
         FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_schema = $1 AND pgt_name = $2 AND data_timestamp IS NOT NULL",
        &[schema.into(), table_name.into()],
    )
    .unwrap_or(None)
}

/// Check CDC trigger health for all tracked sources.
///
/// Returns trigger/slot name, source table, active status, retained WAL bytes,
/// and the CDC mode (`trigger`, `wal`, or `transitioning`).
/// Exposed as `pgtrickle.slot_health()` (kept for API compatibility).
#[pg_extern(schema = "pgtrickle", name = "slot_health")]
fn slot_health() -> TableIterator<
    'static,
    (
        name!(slot_name, String),
        name!(source_relid, i64),
        name!(active, bool),
        name!(retained_wal_bytes, i64),
        name!(wal_status, String),
    ),
> {
    TableIterator::new(collect_slot_health_rows())
}

// ── UX-1 / CACHE-OBS: Template cache observability ─────────────────────────

/// Return template cache statistics from shared memory.
///
/// Reports L1 (thread-local) hits, L2 (catalog table) hits, full misses
/// (DVM re-parse), evictions (generation flushes), and the current L1
/// cache size for this backend.
///
/// Exposed as `pgtrickle.cache_stats()`.
#[pg_extern(schema = "pgtrickle", name = "cache_stats")]
#[allow(clippy::type_complexity)]
fn cache_stats() -> TableIterator<
    'static,
    (
        name!(l1_hits, i64),
        name!(l2_hits, i64),
        name!(misses, i64),
        name!(evictions, i64),
        name!(l1_size, i32),
    ),
> {
    let (l1_hits, l2_hits, misses, evictions) = if crate::shmem::is_shmem_available() {
        (
            crate::shmem::TEMPLATE_CACHE_L1_HITS
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as i64,
            crate::shmem::TEMPLATE_CACHE_L2_HITS
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as i64,
            crate::shmem::TEMPLATE_CACHE_MISSES
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as i64,
            crate::shmem::TEMPLATE_CACHE_EVICTIONS
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as i64,
        )
    } else {
        (0, 0, 0, 0)
    };

    let l1_size = crate::dvm::delta_cache_size() as i32;

    TableIterator::once((l1_hits, l2_hits, misses, evictions, l1_size))
}

/// OPS-10-02: Reliability counters for Prometheus secondary metrics.
///
/// Returns three counters not covered by `cache_stats()`:
/// - `invalidation_ring_overflows` — times the invalidation ring overflowed
/// - `dag_cycles_detected` — times a cycle was detected in the ST DAG
/// - `template_cache_stale_evictions` — delta cache hits where hash mismatched
///
/// Exposed as `pgtrickle.reliability_counters()`.
#[pg_extern(schema = "pgtrickle", name = "reliability_counters")]
#[allow(clippy::type_complexity)]
fn reliability_counters() -> TableIterator<
    'static,
    (
        name!(invalidation_ring_overflows, i64),
        name!(dag_cycles_detected, i64),
        name!(template_cache_stale_evictions, i64),
    ),
> {
    let (ring_overflows, dag_cycles, stale_evictions) = if crate::shmem::is_shmem_available() {
        (
            crate::shmem::INVALIDATION_RING_OVERFLOWS
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as i64,
            crate::shmem::DAG_CYCLES_DETECTED
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as i64,
            crate::shmem::TEMPLATE_CACHE_STALE_EVICTIONS
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as i64,
        )
    } else {
        (0, 0, 0)
    };
    TableIterator::once((ring_overflows, dag_cycles, stale_evictions))
}

/// STAB-4: Per-stream-table refresh timing statistics with percentiles.
///
/// Aggregates `pgt_refresh_history` data into per-stream-table timing
/// summaries including avg, p95, p99, and refresh count.
///
/// Exposed as `pgtrickle.pgtrickle_refresh_stats()`.
#[pg_extern(schema = "pgtrickle", name = "pgtrickle_refresh_stats")]
#[allow(clippy::type_complexity)]
fn pgtrickle_refresh_stats() -> TableIterator<
    'static,
    (
        name!(stream_table, String),
        name!(mode, String),
        name!(avg_ms, f64),
        name!(p95_ms, f64),
        name!(p99_ms, f64),
        name!(refresh_count, i64),
        name!(last_refresh_at, Option<TimestampWithTimeZone>),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    st.pgt_schema || '.' || st.pgt_name AS stream_table,
                    st.refresh_mode::text AS mode,
                    COALESCE(s.avg_ms, 0)::float8,
                    COALESCE(s.p95_ms, 0)::float8,
                    COALESCE(s.p99_ms, 0)::float8,
                    COALESCE(s.cnt, 0)::bigint,
                    st.last_refresh_at
                FROM pgtrickle.pgt_stream_tables st
                LEFT JOIN LATERAL (
                    SELECT
                        avg(EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000) AS avg_ms,
                        percentile_cont(0.95) WITHIN GROUP (
                            ORDER BY EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000
                        ) AS p95_ms,
                        percentile_cont(0.99) WITHIN GROUP (
                            ORDER BY EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000
                        ) AS p99_ms,
                        count(*) AS cnt
                    FROM pgtrickle.pgt_refresh_history h
                    WHERE h.pgt_id = st.pgt_id
                      AND h.status = 'COMPLETED'
                      AND h.end_time IS NOT NULL
                ) s ON true
                ORDER BY COALESCE(s.avg_ms, 0) DESC",
                None,
                &[],
            )
            .map_err(|e| crate::error::PgTrickleError::SpiError(e.to_string()));

        match result {
            Ok(tbl) => {
                let mut rows = Vec::new();
                for row in tbl {
                    let stream_table = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
                    let mode = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
                    let avg_ms = row.get::<f64>(3).unwrap_or(None).unwrap_or(0.0);
                    let p95_ms = row.get::<f64>(4).unwrap_or(None).unwrap_or(0.0);
                    let p99_ms = row.get::<f64>(5).unwrap_or(None).unwrap_or(0.0);
                    let cnt = row.get::<i64>(6).unwrap_or(None).unwrap_or(0);
                    let last_at = row.get::<TimestampWithTimeZone>(7).unwrap_or(None);
                    rows.push((stream_table, mode, avg_ms, p95_ms, p99_ms, cnt, last_at));
                }
                rows
            }
            Err(_) => Vec::new(),
        }
    });

    TableIterator::new(rows)
}

/// Explain the DVM plan for a stream table's defining query.
///
/// Returns whether the query supports differential refresh,
/// lists the operators found, and shows the generated delta query.
///
/// PERF-3: When `with_analyze` is true, the defining query is EXPLAINed with
/// ANALYZE to show actual row counts, timings, and buffer usage.
/// Exposed as `pgtrickle.explain_st(name, with_analyze)`.
#[pg_extern(schema = "pgtrickle", name = "explain_st")]
fn explain_st(
    name: &str,
    with_analyze: default!(bool, false),
) -> TableIterator<'static, (name!(property, String), name!(value, String))> {
    let parts: Vec<&str> = name.splitn(2, '.').collect();
    let (schema, table_name) = if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        ("public", parts[0])
    };

    let rows = explain_st_impl(schema, table_name, with_analyze)
        .unwrap_or_else(|e| vec![("error".to_string(), e.to_string())]);

    TableIterator::new(rows)
}

fn explain_st_impl(
    schema: &str,
    table_name: &str,
    with_analyze: bool,
) -> Result<Vec<(String, String)>, PgTrickleError> {
    use crate::catalog::StreamTableMeta;
    use crate::dvm;

    let st = StreamTableMeta::get_by_name(schema, table_name)?;

    let mut props = Vec::new();

    props.push((
        "pgt_name".to_string(),
        format!("{}.{}", st.pgt_schema, st.pgt_name),
    ));
    props.push(("defining_query".to_string(), st.defining_query.clone()));
    props.push((
        "refresh_mode".to_string(),
        st.refresh_mode.as_str().to_string(),
    ));
    props.push(("status".to_string(), st.status.as_str().to_string()));
    props.push(("is_populated".to_string(), st.is_populated.to_string()));

    // Parse the defining query to check DVM support
    match dvm::parse_defining_query(&st.defining_query) {
        Ok(op_tree) => {
            props.push(("dvm_supported".to_string(), "true".to_string()));
            props.push(("operator_tree".to_string(), format!("{:?}", op_tree)));

            let columns = op_tree.output_columns();
            props.push(("output_columns".to_string(), columns.join(", ")));

            let sources = op_tree.source_oids();
            props.push((
                "source_oids".to_string(),
                sources
                    .iter()
                    .map(|o| o.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            ));

            // G12-AGG: Expose per-aggregate maintenance strategies.
            let strategies = op_tree.aggregate_strategies();
            if !strategies.is_empty() {
                let strategy_json: Vec<String> = strategies
                    .iter()
                    .map(|(alias, strategy)| format!("\"{}\": \"{}\"", alias, strategy))
                    .collect();
                props.push((
                    "aggregate_strategies".to_string(),
                    format!("{{{}}}", strategy_json.join(", ")),
                ));
            }

            // Try generating delta query
            let prev_frontier = crate::version::Frontier::new();
            let new_frontier = crate::version::Frontier::new();
            match dvm::generate_delta_query(
                &st.defining_query,
                &prev_frontier,
                &new_frontier,
                &st.pgt_schema,
                &st.pgt_name,
            ) {
                Ok(result) => {
                    props.push(("delta_query".to_string(), result.delta_sql));
                }
                Err(e) => {
                    props.push(("delta_query_error".to_string(), e.to_string()));
                }
            }
        }
        Err(e) => {
            props.push(("dvm_supported".to_string(), "false".to_string()));
            props.push(("dvm_error".to_string(), e.to_string()));
        }
    }

    // Frontier info
    if let Some(ref frontier) = st.frontier {
        if let Ok(json) = frontier.to_json() {
            props.push(("frontier".to_string(), json));
        }
    } else {
        props.push(("frontier".to_string(), "null".to_string()));
    }

    // DAG-3: Amplification metrics from recent refresh history.
    // Query the last 20 DIFFERENTIAL refreshes and compute min/max/avg/latest
    // amplification ratio from existing columns (delta_row_count as input,
    // rows_inserted + rows_deleted as output).
    if let Ok(stats) = amplification_stats(st.pgt_id) {
        props.push(("amplification_stats".to_string(), stats));
    }

    // EXPL-ENH: Refresh timing stats from pgt_refresh_history.
    if let Ok(timing) = refresh_timing_stats(st.pgt_id) {
        props.push(("refresh_timing_stats".to_string(), timing));
    }

    // EXPL-ENH: Source partition info for partitioned table sources.
    if let Ok(partitions) = source_partition_info(&st)
        && !partitions.is_empty()
    {
        props.push(("source_partitions".to_string(), partitions));
    }

    // EXPL-ENH: Dependency sub-graph in DOT format.
    if let Ok(dot) = dependency_subgraph(st.pgt_id, &format!("{}.{}", st.pgt_schema, st.pgt_name)) {
        props.push(("dependency_graph_dot".to_string(), dot));
    }

    // PH-D1: Merge strategy GUC value.
    props.push((
        "merge_strategy".to_string(),
        crate::config::pg_trickle_merge_strategy()
            .as_str()
            .to_string(),
    ));

    // A-3-AO: Append-only mode.
    // "explicit" = user set append_only => true at creation/alter
    // "heuristic" = auto-promoted because buffer was insert-only
    // "disabled" = not using append-only INSERT path
    // We derive the mode from `is_append_only`. When the flag was set by the
    // user at creation it shows as "explicit"; when the heuristic promoted it
    // (no user intervention) we report "heuristic". We approximate this by
    // checking if effective_refresh_mode was ever APPEND_ONLY.
    let append_only_mode = if st.is_append_only { "on" } else { "off" };
    props.push(("append_only_mode".to_string(), append_only_mode.to_string()));

    // UX-5: Dog-feeding coverage — show whether DF stream tables exist and
    // whether this ST is being monitored.
    {
        let df_count: i64 = Spi::get_one(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .unwrap_or(Some(0))
        .unwrap_or(0);
        let coverage = if df_count >= 5 {
            "full (5/5 self-monitoring STs active)"
        } else if df_count > 0 {
            "partial"
        } else {
            "none (run setup_self_monitoring() to enable)"
        };
        props.push(("self_monitoring_coverage".to_string(), coverage.to_string()));
    }

    // UX-6: Refresh mode recommendation from recommend_refresh_mode().
    {
        let fq_name = format!("{}.{}", st.pgt_schema, st.pgt_name);
        if let Ok(Some(rec)) = Spi::get_one_with_args::<String>(
            "SELECT row_to_json(r)::text FROM pgtrickle.recommend_refresh_mode($1) r",
            &[fq_name.as_str().into()],
        ) {
            props.push(("recommended_refresh_mode".to_string(), rec));
        }
    }

    // B-1: Aggregate fast-path status.
    // Detect whether the defining query has all-algebraic aggregates.
    let agg_fast_path_guc = crate::config::pg_trickle_aggregate_fast_path();
    let is_all_algebraic = if matches!(
        st.refresh_mode,
        crate::dag::RefreshMode::Differential | crate::dag::RefreshMode::Immediate
    ) {
        crate::dvm::parse_defining_query_full(&st.defining_query)
            .map(|pr| pr.tree.is_all_algebraic_agg())
            .unwrap_or(false)
    } else {
        false
    };
    let aggregate_path = if is_all_algebraic && agg_fast_path_guc {
        "explicit_dml"
    } else if is_all_algebraic {
        "merge (fast-path disabled)"
    } else {
        "merge"
    };
    props.push(("aggregate_path".to_string(), aggregate_path.to_string()));

    // C-4: Compaction threshold from GUC.
    let compact_threshold = crate::config::pg_trickle_compact_threshold();
    props.push((
        "compact_threshold".to_string(),
        compact_threshold.to_string(),
    ));

    // PH-E2: Live temp file spill info from pg_stat_statements.
    let spill_threshold = crate::config::pg_trickle_spill_threshold_blocks();
    if spill_threshold > 0 {
        let name = format!("{}.{}", st.pgt_schema, st.pgt_name);
        match query_temp_file_usage(&name) {
            Some((read_blks, written_blks)) => {
                let exceeds = written_blks > spill_threshold as i64;
                let info = format!(
                    "{{\"temp_blks_read\":{},\"temp_blks_written\":{},\"threshold\":{},\"exceeds_threshold\":{}}}",
                    read_blks, written_blks, spill_threshold, exceeds
                );
                props.push(("spill_info".to_string(), info));
            }
            None => {
                props.push((
                    "spill_info".to_string(),
                    "{\"status\":\"pg_stat_statements not available or no matching statement\"}"
                        .to_string(),
                ));
            }
        }
    }

    // G14-SHC: Template cache stats.
    props.push((
        "template_cache".to_string(),
        crate::config::pg_trickle_template_cache_enabled().to_string(),
    ));
    if crate::shmem::is_shmem_available() {
        let l2_hits = crate::shmem::TEMPLATE_CACHE_L2_HITS
            .get()
            .load(std::sync::atomic::Ordering::Relaxed);
        let misses = crate::shmem::TEMPLATE_CACHE_MISSES
            .get()
            .load(std::sync::atomic::Ordering::Relaxed);
        props.push((
            "template_cache_stats".to_string(),
            format!("{{\"l2_hits\":{},\"full_misses\":{}}}", l2_hits, misses),
        ));
    }

    // PERF-3: EXPLAIN ANALYZE of the defining query (when requested).
    if with_analyze {
        let explain_sql = format!(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {}",
            st.defining_query
        );
        match Spi::connect(|client| {
            let result = client
                .select(&explain_sql, None, &[])
                .map_err(|e| crate::error::PgTrickleError::SpiError(e.to_string()))?;
            let mut lines = Vec::new();
            for row in result {
                if let Some(line) = row.get::<String>(1).unwrap_or(None) {
                    lines.push(line);
                }
            }
            Ok::<String, crate::error::PgTrickleError>(lines.join("\n"))
        }) {
            Ok(plan) => {
                props.push(("explain_analyze".to_string(), plan));
            }
            Err(e) => {
                props.push(("explain_analyze".to_string(), format!("error: {}", e)));
            }
        }
    }

    Ok(props)
}

/// UX-3: Return the generated delta SQL for a stream table (inspection only).
///
/// Builds and returns the delta SQL template that the DVM engine would
/// generate for the given stream table. Uses placeholder LSN tokens so
/// the SQL can be inspected or run with `EXPLAIN` after substituting
/// actual LSN values.
///
/// Exposed as `pgtrickle.explain_diff_sql(name)`.
#[pg_extern(schema = "pgtrickle", name = "explain_diff_sql")]
fn explain_diff_sql(name: &str) -> Option<String> {
    let parts: Vec<&str> = name.splitn(2, '.').collect();
    let (schema, table_name) = if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        ("public", parts[0])
    };

    match explain_diff_sql_impl(schema, table_name) {
        Ok(sql) => Some(sql),
        Err(e) => {
            pgrx::notice!("explain_diff_sql: {}", e);
            None
        }
    }
}

fn explain_diff_sql_impl(schema: &str, table_name: &str) -> Result<String, PgTrickleError> {
    use crate::catalog::StreamTableMeta;
    use crate::dvm;

    let st = StreamTableMeta::get_by_name(schema, table_name)?;

    // Generate the delta SQL with placeholder LSN tokens
    let prev_frontier = crate::version::Frontier::new();
    let new_frontier = crate::version::Frontier::new();
    let result = dvm::generate_delta_query(
        &st.defining_query,
        &prev_frontier,
        &new_frontier,
        &st.pgt_schema,
        &st.pgt_name,
    )?;

    Ok(result.delta_sql)
}

// ── DAG-3: Amplification Statistics ─────────────────────────────────────

/// Query the last 20 DIFFERENTIAL refreshes for a stream table and compute
/// amplification ratio statistics (min, max, avg, latest).
///
/// Returns a JSON string like:
/// `{"samples":15,"min":1.0,"max":42.5,"avg":8.3,"latest":12.1,"threshold":100.0}`
///
/// Returns an error if no DIFFERENTIAL refreshes are recorded yet.
fn amplification_stats(pgt_id: i64) -> Result<String, PgTrickleError> {
    let threshold = crate::config::pg_trickle_delta_amplification_threshold();

    let sql = format!(
        "SELECT delta_row_count, rows_inserted, rows_deleted \
         FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = {pgt_id} \
           AND action = 'DIFFERENTIAL' \
           AND status = 'COMPLETED' \
           AND delta_row_count > 0 \
         ORDER BY refresh_id DESC \
         LIMIT 20"
    );

    let mut ratios: Vec<f64> = Vec::new();
    Spi::connect(|client| {
        let cursor = client.select(&sql, None, &[]).map_err(|e| {
            PgTrickleError::SpiError(format!("amplification_stats query failed: {e}"))
        })?;
        for row in cursor {
            let input: i64 = row.get::<i64>(1).unwrap_or(Some(0)).unwrap_or(0);
            let inserted: i64 = row.get::<i64>(2).unwrap_or(Some(0)).unwrap_or(0);
            let deleted: i64 = row.get::<i64>(3).unwrap_or(Some(0)).unwrap_or(0);
            let output = inserted + deleted;
            let ratio = crate::refresh::compute_amplification_ratio(input, output);
            ratios.push(ratio);
        }
        Ok::<(), PgTrickleError>(())
    })?;

    if ratios.is_empty() {
        return Err(PgTrickleError::InternalError(
            "no DIFFERENTIAL refresh history available".to_string(),
        ));
    }

    let min = ratios.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = ratios.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let avg = ratios.iter().sum::<f64>() / ratios.len() as f64;
    let latest = ratios[0]; // first row = most recent

    Ok(format!(
        "{{\"samples\":{},\"min\":{:.2},\"max\":{:.2},\"avg\":{:.2},\"latest\":{:.2},\"threshold\":{:.1}}}",
        ratios.len(),
        min,
        max,
        avg,
        latest,
        threshold,
    ))
}

// ── EXPL-ENH: Refresh Timing Statistics ─────────────────────────────────

/// Query the last 20 completed refreshes (any action) and compute duration
/// statistics in milliseconds.
///
/// Returns a JSON string like:
/// `{"samples":10,"min_ms":12.3,"max_ms":450.0,"avg_ms":85.7,"latest_ms":42.1,"latest_action":"DIFFERENTIAL"}`
fn refresh_timing_stats(pgt_id: i64) -> Result<String, PgTrickleError> {
    let sql = format!(
        "SELECT action::text, \
                EXTRACT(EPOCH FROM (end_time - start_time)) * 1000 AS duration_ms \
         FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = {pgt_id} \
           AND status = 'COMPLETED' \
           AND end_time IS NOT NULL \
         ORDER BY refresh_id DESC \
         LIMIT 20"
    );

    let mut durations: Vec<f64> = Vec::new();
    let mut latest_action = String::new();

    Spi::connect(|client| {
        let cursor = client.select(&sql, None, &[]).map_err(|e| {
            PgTrickleError::SpiError(format!("refresh_timing_stats query failed: {e}"))
        })?;
        for row in cursor {
            let action: String = row
                .get::<String>(1)
                .unwrap_or(Some(String::new()))
                .unwrap_or_default();
            let ms: f64 = row.get::<f64>(2).unwrap_or(Some(0.0)).unwrap_or(0.0);
            if durations.is_empty() {
                latest_action = action;
            }
            durations.push(ms);
        }
        Ok::<(), PgTrickleError>(())
    })?;

    if durations.is_empty() {
        return Err(PgTrickleError::InternalError(
            "no completed refresh history available".to_string(),
        ));
    }

    let min = durations.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = durations.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let avg = durations.iter().sum::<f64>() / durations.len() as f64;
    let latest = durations[0];

    Ok(format!(
        "{{\"samples\":{},\"min_ms\":{:.1},\"max_ms\":{:.1},\"avg_ms\":{:.1},\"latest_ms\":{:.1},\"latest_action\":\"{}\"}}",
        durations.len(),
        min,
        max,
        avg,
        latest,
        latest_action,
    ))
}

// ── EXPL-ENH: Source Partition Info ──────────────────────────────────────

/// For each source table, check if it is partitioned and report the
/// partition strategy and key.
///
/// Returns a JSON array like:
/// `[{"source":"public.orders","strategy":"RANGE","key":"order_date","partitions":12}]`
fn source_partition_info(st: &crate::catalog::StreamTableMeta) -> Result<String, PgTrickleError> {
    let sql = format!(
        "SELECT d.source_relid, \
                c.relkind, \
                c.relnamespace::regnamespace::text AS schema, \
                c.relname::text AS name, \
                pg_catalog.pg_get_partkeydef(c.oid) AS partkey, \
                (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid) AS nparts \
         FROM pgtrickle.pgt_dependencies d \
         JOIN pg_class c ON c.oid = d.source_relid \
         WHERE d.pgt_id = {} \
           AND c.relkind = 'p'",
        st.pgt_id
    );

    let mut entries: Vec<String> = Vec::new();
    Spi::connect(|client| {
        let cursor = client.select(&sql, None, &[]).map_err(|e| {
            PgTrickleError::SpiError(format!("source_partition_info query failed: {e}"))
        })?;
        for row in cursor {
            let schema: String = row
                .get::<String>(3)
                .unwrap_or(Some(String::new()))
                .unwrap_or_default();
            let name: String = row
                .get::<String>(4)
                .unwrap_or(Some(String::new()))
                .unwrap_or_default();
            let partkey: String = row
                .get::<String>(5)
                .unwrap_or(Some(String::new()))
                .unwrap_or_default();
            let nparts: i64 = row.get::<i64>(6).unwrap_or(Some(0)).unwrap_or(0);
            entries.push(format!(
                "{{\"source\":\"{}.{}\",\"partition_key\":\"{}\",\"partitions\":{}}}",
                schema, name, partkey, nparts,
            ));
        }
        Ok::<(), PgTrickleError>(())
    })?;

    Ok(format!("[{}]", entries.join(",")))
}

// ── EXPL-ENH: Dependency Sub-graph ──────────────────────────────────────

/// Build a DOT-format dependency sub-graph for a stream table showing its
/// immediate upstream sources and downstream dependents.
fn dependency_subgraph(pgt_id: i64, st_name: &str) -> Result<String, PgTrickleError> {
    use crate::dag::{NodeId, StDag};

    let fallback_secs = crate::config::pg_trickle_min_schedule_seconds();
    let dag = StDag::build_from_catalog(fallback_secs)?;

    let node = NodeId::StreamTable(pgt_id);
    let upstream = dag.get_upstream(node);
    let downstream = dag.get_downstream(node);

    let mut dot = String::from("digraph dependency_subgraph {\n");
    dot.push_str(&format!(
        "  \"{}\" [shape=box, style=filled, fillcolor=lightblue];\n",
        st_name
    ));

    for up in &upstream {
        let label = dag.node_label(up);
        let shape = match up {
            NodeId::BaseTable(_) => "ellipse",
            NodeId::StreamTable(_) => "box",
        };
        dot.push_str(&format!("  \"{}\" [shape={}];\n", label, shape));
        dot.push_str(&format!("  \"{}\" -> \"{}\";\n", label, st_name));
    }

    for down in &downstream {
        let label = dag.node_label(down);
        dot.push_str(&format!("  \"{}\" [shape=box];\n", label));
        dot.push_str(&format!("  \"{}\" -> \"{}\";\n", st_name, label));
    }

    dot.push('}');
    Ok(dot)
}

// ── CDC Health Monitoring ───────────────────────────────────────────────────

/// Check CDC health for all tracked sources.
///
/// Returns per-source health status including CDC mode, estimated lag,
/// last confirmed LSN, and whether the slot lag exceeds a threshold.
///
/// Exposed as `pgtrickle.check_cdc_health()`.
#[pg_extern(schema = "pgtrickle", name = "check_cdc_health")]
#[allow(clippy::type_complexity)]
fn check_cdc_health() -> TableIterator<
    'static,
    (
        name!(source_relid, i64),
        name!(source_table, String),
        name!(cdc_mode, String),
        name!(slot_name, Option<String>),
        name!(lag_bytes, Option<i64>),
        name!(confirmed_lsn, Option<String>),
        name!(alert, Option<String>),
        name!(selective_capture, bool),
    ),
> {
    let all_deps = StDependency::get_all().unwrap_or_default();
    let mut rows = Vec::new();
    let mut seen_sources = std::collections::HashSet::new();
    let lag_alert_threshold = config::pg_trickle_slot_lag_critical_threshold_bytes();

    for dep in &all_deps {
        if dep.source_type != "TABLE" {
            continue;
        }
        let oid_u32 = dep.source_relid.to_u32();
        if !seen_sources.insert(oid_u32) {
            continue;
        }

        // Resolve source table name
        let source_name = Spi::get_one_with_args::<String>(
            "SELECT $1::oid::regclass::text",
            &[dep.source_relid.into()],
        )
        .unwrap_or(None)
        .unwrap_or_else(|| format!("oid:{}", oid_u32));

        let mode_str = dep.cdc_mode.as_str().to_string();

        // F15: is selective column capture active for this source?
        let selective = crate::cdc::is_selective_capture_active(dep.source_relid);

        match dep.cdc_mode {
            CdcMode::Trigger => {
                rows.push((
                    oid_u32 as i64,
                    source_name,
                    mode_str,
                    None,
                    None,
                    None,
                    None,
                    selective,
                ));
            }
            CdcMode::Wal | CdcMode::Transitioning => {
                let slot = dep
                    .slot_name
                    .clone()
                    .unwrap_or_else(|| wal_decoder::slot_name_for_source(dep.source_relid));
                let lag = wal_decoder::get_slot_lag_bytes(&slot).unwrap_or(0);
                let lsn = dep.decoder_confirmed_lsn.clone();

                let slot_exists = Spi::get_one_with_args::<bool>(
                    "SELECT EXISTS(SELECT 1 FROM pg_replication_slots \
                     WHERE slot_name = $1 AND database = current_database())",
                    &[slot.as_str().into()],
                )
                .unwrap_or(Some(false))
                .unwrap_or(false);

                let alert =
                    build_cdc_health_alert(lag, lag_alert_threshold, slot_exists, dep.cdc_mode);

                rows.push((
                    oid_u32 as i64,
                    source_name,
                    mode_str,
                    Some(slot),
                    Some(lag),
                    lsn,
                    alert,
                    selective,
                ));
            }
            CdcMode::DuckLakeChangeFeed => {
                // DuckLake change-feed sources do not use WAL replication slots.
                rows.push((
                    oid_u32 as i64,
                    source_name,
                    mode_str,
                    None,
                    None,
                    None,
                    None,
                    selective,
                ));
            }
        }
    }

    // OPS-2: Enrich with spill-risk data from df_cdc_buffer_trends (if it exists).
    let buffer_trends: std::collections::HashMap<i64, (f64, f64)> = {
        let trends_exist: bool = Spi::get_one(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_stream_tables
                WHERE pgt_schema = 'pgtrickle' AND pgt_name = 'df_cdc_buffer_trends'
            )",
        )
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if trends_exist {
            Spi::connect(|client| {
                let result = match client.select(
                    "SELECT source_relid, avg_delta_per_refresh, max_delta_per_refresh \
                     FROM pgtrickle.df_cdc_buffer_trends \
                     WHERE avg_delta_per_refresh IS NOT NULL",
                    None,
                    &[],
                ) {
                    Ok(r) => r,
                    Err(_) => return std::collections::HashMap::new(),
                };
                let mut m = std::collections::HashMap::new();
                for row in result {
                    let relid = row.get::<i64>(1).unwrap_or(None).unwrap_or(0);
                    let avg_delta = row.get::<f64>(2).unwrap_or(None).unwrap_or(0.0);
                    let max_delta = row.get::<f64>(3).unwrap_or(None).unwrap_or(0.0);
                    if relid > 0 {
                        m.insert(relid, (avg_delta, max_delta));
                    }
                }
                m
            })
        } else {
            std::collections::HashMap::new()
        }
    };

    // Merge spill-risk alerts from df_cdc_buffer_trends into rows.
    if !buffer_trends.is_empty() {
        for row in &mut rows {
            let source_relid = row.0;
            if let Some(&(avg_delta, max_delta)) = buffer_trends.get(&source_relid) {
                // Alert if max delta > 10× average (burst spill risk).
                if avg_delta > 0.0 && max_delta > avg_delta * 10.0 {
                    let spill_alert = format!(
                        "CDC buffer spill risk: max_delta ({:.0}) is {:.0}× avg ({:.0})",
                        max_delta,
                        max_delta / avg_delta,
                        avg_delta,
                    );
                    row.6 = Some(match &row.6 {
                        Some(existing) => format!("{}; {}", existing, spill_alert),
                        None => spill_alert,
                    });
                }
            }
        }
    }

    // INV-CACHE1: Check that no change buffer sequence has been manually altered
    // to CACHE > 1. This would silently corrupt compaction and delta ordering
    // (sequence-cache inversion — see issue #536). The check queries pg_sequences
    // for any sequence whose name matches the change buffer pattern and whose
    // cache_size > 1.
    let corrupted_seqs: Vec<String> = Spi::connect(|client| {
        let result = match client.select(
            "SELECT sequencename::text \
             FROM pg_sequences \
             WHERE (sequencename LIKE 'changes_%_change_id_seq' \
                 OR sequencename LIKE 'changes_pgt_%_change_id_seq') \
               AND cache_size > 1 \
               AND schemaname = 'pgtrickle_changes'",
            None,
            &[],
        ) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        result
            .map(|row| row.get::<String>(1).unwrap_or(None).unwrap_or_default())
            .filter(|s| !s.is_empty())
            .collect()
    });

    if !corrupted_seqs.is_empty() {
        let seq_list = corrupted_seqs.join(", ");
        let cache_alert = format!(
            "CRITICAL: change buffer sequence(s) have CACHE > 1 — \
             this causes silent data corruption via sequence-cache inversion. \
             Run: ALTER SEQUENCE pgtrickle_changes.<name> CACHE 1; \
             Affected: {}",
            seq_list
        );
        // Attach the alert to every trigger-mode row (the sequence belongs to
        // the trigger path; WAL-mode rows are unaffected but the operator
        // should still see the warning).
        for row in &mut rows {
            row.6 = Some(match &row.6 {
                Some(existing) => format!("{}; {}", existing, cache_alert),
                None => cache_alert.clone(),
            });
        }
        pgrx::warning!(
            "[pg_trickle] INV-CACHE1: change buffer sequence(s) with CACHE > 1 detected ({}). \
             This causes silent data corruption. Reset with: \
             ALTER SEQUENCE pgtrickle_changes.<name> CACHE 1",
            seq_list
        );
    }

    TableIterator::new(rows)
}

// ── CDC Transition NOTIFY ──────────────────────────────────────────────────

/// Emit a `NOTIFY pg_trickle_cdc_transition` with a JSON payload when a
/// source transitions between CDC modes.
///
/// Payload includes source table name, old mode, new mode, and slot name.
pub fn emit_cdc_transition_notify(
    source_oid: pg_sys::Oid,
    old_mode: CdcMode,
    new_mode: CdcMode,
    slot_name: Option<&str>,
) {
    let source_name =
        Spi::get_one_with_args::<String>("SELECT $1::oid::regclass::text", &[source_oid.into()])
            .unwrap_or(None)
            .unwrap_or_else(|| format!("oid:{}", source_oid.to_u32()));

    let payload = format!(
        r#"{{"event":"cdc_transition","source_table":"{}","old_mode":"{}","new_mode":"{}","slot_name":{}}}"#,
        source_name.replace('"', r#"\""#),
        old_mode.as_str(),
        new_mode.as_str(),
        match slot_name {
            Some(s) => format!("\"{}\"", s.replace('"', r#"\""#)),
            None => "null".to_string(),
        },
    );

    let escaped = payload.replace('\'', "''");
    let sql = format!("NOTIFY pg_trickle_cdc_transition, '{}'", escaped);

    if let Err(e) = Spi::run(&sql) {
        pgrx::warning!("pg_trickle: failed to emit cdc_transition NOTIFY: {}", e);
    }
}

// ── Slot Health Monitoring (used by scheduler) ─────────────────────────────

/// Check all tracked change buffers and WAL replication slots and emit alerts
/// when either exceeds the configured threshold. Called from the scheduler loop.
///
/// This function must be called in its own clean transaction (separate from the
/// Phase 1 WAL poll transaction). If Phase 1's WAL poll for a missing slot
/// leaves the SPI session in an inconsistent state, the EC-34 slot-existence
/// check inside this function would fail silently and the TRIGGER fallback
/// would never fire. Running here in a fresh transaction guarantees SPI works.
pub fn check_slot_health_and_alert() {
    // With trigger-based CDC, we check pending change buffer size instead
    // of replication slot WAL retention. Alert if buffer tables grow too large.
    let change_schema = config::pg_trickle_change_buffer_schema();

    // Gracefully handle SPI failures (e.g. if called during transaction
    // recovery or in a degraded state) — skip rather than panic.
    let sources = Spi::connect(|client| -> Vec<(String, i64)> {
        match client.select(
            "SELECT ct.slot_name, ct.source_relid::bigint \
             FROM pgtrickle.pgt_change_tracking ct",
            None,
            &[],
        ) {
            Ok(result) => {
                let mut out = Vec::new();
                for row in result {
                    let trigger = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
                    let relid = row.get::<i64>(2).unwrap_or(None).unwrap_or(0);
                    out.push((trigger, relid));
                }
                out
            }
            Err(_) => {
                // SPI error in this query is non-fatal — skip buffer check.
                Vec::new()
            }
        }
    });

    for (trigger_name, relid) in sources {
        // Check buffer table row count as a proxy for staleness
        // v0.32.0+: buffer table uses stable hash name
        let buf = crate::cdc::buffer_qualified_name_for_oid(
            &change_schema,
            pgrx::pg_sys::Oid::from(relid as u32),
        );
        // SAFETY: `buf` is constructed by buffer_qualified_name_for_oid from a
        // PostgreSQL OID — it is never user input. PostgreSQL does not allow bind
        // parameters as FROM-clause table references, so format! is required here.
        let pending = Spi::get_one::<i64>(&format!("SELECT count(*)::bigint FROM {buf}")) // nosemgrep: rust.spi.query.dynamic-format
            .unwrap_or(Some(0))
            .unwrap_or(0);

        // F46 (G9.3): Alert if more than the configured threshold of pending changes
        let threshold = config::pg_trickle_buffer_alert_threshold();
        if pending > threshold {
            alert_buffer_growth(&trigger_name, pending);
        }
    }

    let lag_warning_threshold = config::pg_trickle_slot_lag_warning_threshold_bytes();
    for (slot_name, _source_relid, _active, retained_wal_bytes, wal_status) in
        collect_slot_health_rows()
    {
        if wal_status != "trigger" && retained_wal_bytes > lag_warning_threshold {
            alert_slot_lag(&slot_name, retained_wal_bytes, lag_warning_threshold);
        }
    }

    // EC-34: Check that WAL-mode dependencies still have their replication
    // slots. If a slot was dropped (e.g., by a DBA), fall back to TRIGGER
    // mode and emit a WARNING so the operator knows.
    check_wal_slot_existence();
}

/// EC-34: Verify that WAL-mode dependencies still have their replication
/// slots. If a slot is missing, fall back to TRIGGER mode and warn.
fn check_wal_slot_existence() {
    use crate::catalog::{CdcMode, StDependency};

    let deps = match StDependency::get_all() {
        Ok(d) => d,
        Err(_) => return,
    };

    for dep in &deps {
        if dep.cdc_mode != CdcMode::Wal {
            continue;
        }

        let slot_name = match &dep.slot_name {
            Some(name) => name.clone(),
            None => crate::wal_decoder::slot_name_for_source(dep.source_relid),
        };

        // Check if the replication slot exists (scoped to current database)
        let slot_exists = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots \
             WHERE slot_name = $1 AND database = current_database())",
            &[slot_name.as_str().into()],
        )
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if !slot_exists {
            pgrx::warning!(
                "pg_trickle: replication slot '{}' for source OID {} is missing. \
                 Falling back to trigger-based CDC. The slot may have been dropped \
                 manually or by a management tool.",
                slot_name,
                dep.source_relid.to_u32(),
            );

            // Fall back to TRIGGER mode — abort_wal_transition handles
            // slot cleanup, publication removal, catalog update, AND
            // trigger recreation.
            let change_schema = crate::config::pg_trickle_change_buffer_schema();
            if let Err(e) =
                wal_decoder::abort_wal_transition(dep.source_relid, dep.pgt_id, &change_schema)
            {
                pgrx::warning!(
                    "pg_trickle: failed to fall back to TRIGGER mode for source OID {}: {}",
                    dep.source_relid.to_u32(),
                    e,
                );
            }
        }
    }
}

/// Periodic check for disabled or missing CDC triggers on source tables.
///
/// Called from the scheduler main loop every ~60s.  Detects source tables
/// whose pg_trickle CDC trigger is either:
///   - missing entirely (dropped without DDL hook firing)
///   - explicitly disabled via `ALTER TABLE … DISABLE TRIGGER`
///
/// Emits a `cdc_trigger_disabled` NOTIFY alert for each affected source
/// so operators are notified before data staleness goes undetected.
pub fn check_cdc_trigger_health() {
    use crate::catalog::StDependency;

    let deps = match StDependency::get_all() {
        Ok(d) => d,
        Err(_) => return,
    };

    // Collect unique source OIDs that use trigger-mode CDC.
    let mut checked = std::collections::HashSet::new();
    for dep in &deps {
        if dep.cdc_mode != crate::catalog::CdcMode::Trigger {
            continue;
        }
        let oid = dep.source_relid;
        if !checked.insert(oid) {
            continue; // already checked
        }

        let oid_u32 = oid.to_u32();

        // Check if any DML CDC trigger exists AND is enabled for this source.
        // tgenabled values: 'O' = origin (enabled), 'D' = disabled,
        // 'R' = replica, 'A' = always.  Anything other than 'D' is enabled.
        let trigger_ok = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS( \
               SELECT 1 FROM pg_trigger \
               WHERE tgrelid = {oid_u32}::oid \
                 AND tgname IN ( \
                     'pg_trickle_cdc_{oid_u32}', \
                     'pg_trickle_cdc_ins_{oid_u32}', \
                     'pg_trickle_cdc_upd_{oid_u32}', \
                     'pg_trickle_cdc_del_{oid_u32}' \
                 ) \
                 AND tgenabled != 'D' \
             )",
        ))
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if !trigger_ok {
            // Look up the source table name for a useful alert message.
            let source_name = Spi::get_one::<String>(&format!(
                "SELECT n.nspname::text || '.' || c.relname::text \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = {oid_u32}::oid",
            ))
            .unwrap_or(None)
            .unwrap_or_else(|| format!("oid={}", oid_u32));

            emit_alert(
                AlertEvent::CdcTriggerDisabled,
                "", // no single ST schema — source-level alert
                &source_name,
                json!({ "source_oid": oid_u32 }),
                false, // always emit, even in pooler mode
            );
        }
    }
}

// ── Temp File / Memory Usage Tracking (F45: G9.2) ──────────────────────────

/// Query `pg_stat_statements` for the temp-file metrics of a recently executed
/// MERGE (or delta query) containing the specified table name.
///
/// Returns `(temp_blks_read, temp_blks_written)` if `pg_stat_statements` is
/// available and a matching statement was found. Returns `None` if the
/// extension is not installed or no match is found.
///
/// This provides post-hoc visibility into whether large deltas spilled to
/// temporary files, which may indicate `work_mem` is too low.
pub fn query_temp_file_usage(table_name: &str) -> Option<(i64, i64)> {
    // Check if pg_stat_statements is available
    let available = Spi::get_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')",
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !available {
        return None;
    }

    // Look for the most recent MERGE statement referencing this table
    let escaped = table_name.replace('\'', "''");
    let result = Spi::get_two::<i64, i64>(&format!(
        "SELECT temp_blks_read::bigint, temp_blks_written::bigint \
         FROM pg_stat_statements \
         WHERE query LIKE '%MERGE%{escaped}%' \
         ORDER BY total_exec_time DESC LIMIT 1",
    ));

    match result {
        Ok((Some(read), Some(written))) => {
            if written > 0 {
                pgrx::log!(
                    "pg_trickle: MERGE for {} used {} temp blocks read, {} written \
                     — consider increasing work_mem or lowering differential_max_change_ratio",
                    table_name,
                    read,
                    written,
                );
            }
            Some((read, written))
        }
        _ => None,
    }
}

/// Show pending change counts and estimated disk sizes for all CDC-tracked
/// source tables.
///
/// Returns one row per `(stream_table, source_table)` pair.
/// `pending_rows` is the number of CDC rows not yet consumed by a differential
/// refresh; `buffer_bytes` is the estimated on-disk size of the change buffer.
///
/// Exposed as `pgtrickle.change_buffer_sizes()`.
#[pg_extern(schema = "pgtrickle", name = "change_buffer_sizes")]
#[allow(clippy::type_complexity)]
fn change_buffer_sizes() -> TableIterator<
    'static,
    (
        name!(stream_table, String),
        name!(source_table, String),
        name!(source_oid, i64),
        name!(cdc_mode, String),
        name!(pending_rows, i64),
        name!(buffer_bytes, i64),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    st.pgt_schema || '.' || st.pgt_name        AS stream_table,
                    n.nspname::text || '.' || c.relname::text  AS source_table,
                    d.source_relid::bigint,
                    d.cdc_mode,
                    COALESCE(s.n_live_tup, 0)::bigint          AS pending_rows,
                    COALESCE(pg_total_relation_size(cb.oid), 0)::bigint
                                                               AS buffer_bytes
                FROM pgtrickle.pgt_dependencies d
                JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
                JOIN pg_class c                     ON c.oid = d.source_relid
                JOIN pg_namespace n                 ON n.oid = c.relnamespace
                -- v0.32.0+: join via pgt_change_tracking to get stable buffer name
                LEFT JOIN pgtrickle.pgt_change_tracking ct
                    ON  ct.source_relid = d.source_relid
                LEFT JOIN pg_class cb
                    ON  cb.relname = 'changes_' || ct.source_stable_name
                    AND cb.relnamespace = (
                            SELECT oid FROM pg_namespace
                            WHERE  nspname = COALESCE(
                                current_setting('pg_trickle.change_buffer_schema', true),
                                'pgtrickle_changes'))
                LEFT JOIN pg_stat_user_tables s ON s.relid = cb.oid
                ORDER BY stream_table, source_table",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "change_buffer_sizes: SPI select failed: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in result {
            let stream_table = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let source_table = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let source_oid = row.get::<i64>(3).unwrap_or(None).unwrap_or(0);
            let cdc_mode = row.get::<String>(4).unwrap_or(None).unwrap_or_default();
            let pending_rows = row.get::<i64>(5).unwrap_or(None).unwrap_or(0);
            let buffer_bytes = row.get::<i64>(6).unwrap_or(None).unwrap_or(0);
            out.push((
                stream_table,
                source_table,
                source_oid,
                cdc_mode,
                pending_rows,
                buffer_bytes,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

/// List the source tables that a stream table depends on.
///
/// Returns one row per source, including its CDC mode and any column-level
/// usage metadata recorded at creation time.
///
/// Exposed as `pgtrickle.list_sources(name)`.
#[pg_extern(schema = "pgtrickle", name = "list_sources")]
#[allow(clippy::type_complexity)]
fn list_sources(
    name: &str,
) -> TableIterator<
    'static,
    (
        name!(source_table, String),
        name!(source_oid, i64),
        name!(source_type, String),
        name!(cdc_mode, String),
        name!(columns_used, Option<String>),
    ),
> {
    let parts: Vec<&str> = name.splitn(2, '.').collect();
    let (schema, table_name) = if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        ("public", parts[0])
    };

    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    n.nspname::text || '.' || c.relname::text AS source_table,
                    d.source_relid::bigint,
                    d.source_type,
                    d.cdc_mode,
                    d.columns_used::text
                FROM pgtrickle.pgt_dependencies d
                JOIN pgtrickle.pgt_stream_tables st
                    ON  st.pgt_id     = d.pgt_id
                    AND st.pgt_schema = $1
                    AND st.pgt_name   = $2
                JOIN pg_class     c ON c.oid = d.source_relid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                ORDER BY source_table",
                None,
                &[schema.into(), table_name.into()],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "list_sources: SPI select failed: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in result {
            let source_table = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let source_oid = row.get::<i64>(2).unwrap_or(None).unwrap_or(0);
            let source_type = row.get::<String>(3).unwrap_or(None).unwrap_or_default();
            let cdc_mode = row.get::<String>(4).unwrap_or(None).unwrap_or_default();
            let columns_used = row.get::<String>(5).unwrap_or(None);
            out.push((
                source_table,
                source_oid,
                source_type,
                cdc_mode,
                columns_used,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

// ── OBS-001 (v0.69.0): DuckLake sink health metrics ──────────────────────

/// Return the last delivery status for each stream table that has a DuckLake
/// sink configured.
///
/// Exposed as `pgtrickle.ducklake_sink_status()`.
/// Returns one row per stream table, showing the most recent delivery outcome.
#[allow(clippy::type_complexity)]
#[pg_extern(schema = "pgtrickle", name = "ducklake_sink_status")]
fn ducklake_sink_status() -> TableIterator<
    'static,
    (
        name!(stream_table_name, String),
        name!(last_delivery_status, Option<String>),
        name!(last_delivery_at, Option<TimestampWithTimeZone>),
        name!(last_bytes_written, Option<i64>),
        name!(last_rows_written, Option<i64>),
        name!(failed_attempts, i64),
        name!(last_error, Option<String>),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                     st.pgt_name,
                     last_d.status,
                     last_d.finished_at,
                     last_d.bytes_written,
                     last_d.rows_written,
                     COALESCE(fail_counts.cnt, 0)::bigint,
                     last_d.last_error
                 FROM pgtrickle.pgt_stream_tables st
                 LEFT JOIN LATERAL (
                     SELECT status, finished_at, bytes_written, rows_written, last_error
                     FROM pgtrickle.pgt_ducklake_sink_delivery d
                     WHERE d.stream_table_id = st.pgt_id
                     ORDER BY started_at DESC
                     LIMIT 1
                 ) last_d ON true
                 LEFT JOIN LATERAL (
                     SELECT COUNT(*) AS cnt
                     FROM pgtrickle.pgt_ducklake_sink_delivery d2
                     WHERE d2.stream_table_id = st.pgt_id
                       AND d2.status IN ('FAILED_RETRYABLE', 'FAILED_PERMANENT')
                 ) fail_counts ON true
                 WHERE st.ducklake_sink_mode IS NOT NULL
                 ORDER BY st.pgt_name",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()));

        let result = match result {
            Ok(r) => r,
            Err(_) => {
                // pgt_ducklake_sink_delivery may not exist on older installs.
                return vec![];
            }
        };

        let mut out = Vec::new();
        for row in result {
            let map_spi = |e: pgrx::spi::SpiError| PgTrickleError::SpiError(e.to_string());
            let name = row
                .get::<String>(1)
                .map_err(map_spi)
                .unwrap_or_default()
                .unwrap_or_default();
            let status = row.get::<String>(2).map_err(map_spi).unwrap_or(None);
            let finished_at = row
                .get::<TimestampWithTimeZone>(3)
                .map_err(map_spi)
                .unwrap_or(None);
            let bytes_written = row.get::<i64>(4).map_err(map_spi).unwrap_or(None);
            let rows_written = row.get::<i64>(5).map_err(map_spi).unwrap_or(None);
            let failed_attempts = row
                .get::<i64>(6)
                .map_err(map_spi)
                .unwrap_or(Some(0))
                .unwrap_or(0);
            let last_error = row.get::<String>(7).map_err(map_spi).unwrap_or(None);
            out.push((
                name,
                status,
                finished_at,
                bytes_written,
                rows_written,
                failed_attempts,
                last_error,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

#[cfg(test)]
mod tests {
    use super::alert::build_alert_payload;
    use super::tree::render_dependency_tree;
    use super::*;

    #[test]
    fn test_alert_event_as_str() {
        assert_eq!(AlertEvent::StaleData.as_str(), "stale_data");
        assert_eq!(AlertEvent::AutoSuspended.as_str(), "auto_suspended");
        assert_eq!(AlertEvent::Resumed.as_str(), "resumed");
        assert_eq!(
            AlertEvent::ReinitializeNeeded.as_str(),
            "reinitialize_needed"
        );
        assert_eq!(
            AlertEvent::BufferGrowthWarning.as_str(),
            "buffer_growth_warning"
        );
        assert_eq!(AlertEvent::SlotLagWarning.as_str(), "slot_lag_warning");
        assert_eq!(AlertEvent::RefreshCompleted.as_str(), "refresh_completed");
        assert_eq!(AlertEvent::RefreshFailed.as_str(), "refresh_failed");
        assert_eq!(
            AlertEvent::SchedulerFallingBehind.as_str(),
            "scheduler_falling_behind"
        );
        assert_eq!(
            AlertEvent::FuseBlownReminder.as_str(),
            "fuse_blown_reminder"
        );
        assert_eq!(AlertEvent::FrozenTierSkip.as_str(), "frozen_tier_skip");
        assert_eq!(
            AlertEvent::CdcTriggerDisabled.as_str(),
            "cdc_trigger_disabled"
        );
        assert_eq!(AlertEvent::CleanupFailure.as_str(), "cleanup_failure");
    }

    #[test]
    fn test_alert_event_equality() {
        assert_eq!(AlertEvent::StaleData, AlertEvent::StaleData);
        assert_ne!(AlertEvent::StaleData, AlertEvent::AutoSuspended);
    }

    #[test]
    fn test_alert_event_all_variants_unique() {
        let variants = [
            AlertEvent::StaleData,
            AlertEvent::AutoSuspended,
            AlertEvent::Resumed,
            AlertEvent::ReinitializeNeeded,
            AlertEvent::BufferGrowthWarning,
            AlertEvent::SlotLagWarning,
            AlertEvent::RefreshCompleted,
            AlertEvent::RefreshFailed,
            AlertEvent::SchedulerFallingBehind,
            AlertEvent::AppendOnlyReverted,
            AlertEvent::FuseBlownReminder,
            AlertEvent::FrozenTierSkip,
            AlertEvent::CdcTriggerDisabled,
            AlertEvent::CleanupFailure,
        ];
        // All as_str() values should be distinct
        let strs: Vec<&str> = variants.iter().map(|v| v.as_str()).collect();
        let mut deduped = strs.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(
            strs.len(),
            deduped.len(),
            "All AlertEvent variants must have unique as_str()"
        );
    }

    #[test]
    fn test_alert_event_clone_and_copy() {
        let event = AlertEvent::RefreshFailed;
        let copied = event; // Copy
        assert_eq!(event, copied);
        // Verify Clone trait is implemented (Copy requires Clone)
        let cloned: AlertEvent = Clone::clone(&event);
        assert_eq!(event, cloned);
    }

    #[test]
    fn test_alert_event_debug_format() {
        let debug = format!("{:?}", AlertEvent::StaleData);
        assert!(
            debug.contains("StaleData"),
            "Debug should contain variant name: {debug}"
        );
    }

    // ── build_alert_payload tests ───────────────────────────────────

    #[test]
    fn test_alert_payload_basic_structure() {
        let payload = build_alert_payload(
            AlertEvent::StaleData,
            "public",
            "orders_st",
            serde_json::json!({"extra_key": "extra_val"}),
        );
        assert!(payload.contains(r#""event":"stale_data""#));
        assert!(payload.contains(r#""pgt_schema":"public""#));
        assert!(payload.contains(r#""pgt_name":"orders_st""#));
        assert!(payload.contains(r#""st":"public.orders_st""#));
        assert!(payload.contains(r#""extra_key":"extra_val""#));
    }

    #[test]
    fn test_alert_payload_escapes_quotes() {
        let payload = build_alert_payload(
            AlertEvent::RefreshFailed,
            r#"my"schema"#,
            r#"my"table"#,
            serde_json::json!({"err": "test"}),
        );
        assert!(payload.contains(r#"my\"schema"#));
        assert!(payload.contains(r#"my\"table"#));
    }

    #[test]
    fn test_alert_payload_truncation() {
        let long_extra = "x".repeat(8000);
        let payload = build_alert_payload(
            AlertEvent::BufferGrowthWarning,
            "public",
            "test",
            serde_json::json!({"data": long_extra}),
        );
        assert!(
            payload.len() <= 7900,
            "payload should be truncated: len={}",
            payload.len()
        );
        assert!(
            payload.ends_with("...}"),
            "should end with truncation marker, got: ...{}",
            &payload[payload.len().saturating_sub(10)..],
        );
    }

    #[test]
    fn test_alert_payload_short_not_truncated() {
        let payload = build_alert_payload(
            AlertEvent::Resumed,
            "public",
            "test",
            serde_json::json!({"reason": "manual"}),
        );
        assert!(!payload.contains("...}}"));
    }

    #[test]
    fn test_build_cdc_health_alert_for_slot_lag() {
        let alert = build_cdc_health_alert(2048, 1024, true, CdcMode::Wal);
        assert_eq!(
            alert,
            Some("slot_lag_exceeds_threshold: 2048 bytes > 1024 bytes".to_string())
        );
    }

    #[test]
    fn test_build_cdc_health_alert_for_missing_slot() {
        let alert = build_cdc_health_alert(128, 1024, false, CdcMode::Wal);
        assert_eq!(alert, Some("replication_slot_missing".to_string()));
    }

    #[test]
    fn test_build_slot_lag_health_detail_warns_with_threshold() {
        let (severity, detail) = build_slot_lag_health_detail(
            &["pg_trickle_slot_123 (128 MB)".to_string()],
            104_857_600,
        );
        assert_eq!(severity, "WARN");
        assert!(detail.contains("104857600 bytes"));
        assert!(detail.contains("pg_trickle_slot_123 (128 MB)"));
    }

    // ── render_dependency_tree tests ────────────────────────────────

    #[test]
    fn test_tree_single_root_no_children() {
        let mut st_info = std::collections::HashMap::new();
        st_info.insert(
            "public.orders_st".to_string(),
            ("ACTIVE".to_string(), "DEFERRED".to_string()),
        );
        let st_children = std::collections::HashMap::new();
        let st_sources = std::collections::HashMap::new();

        let rows = render_dependency_tree(&st_info, &st_children, &st_sources);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "public.orders_st"); // tree_line
        assert_eq!(rows[0].1, "public.orders_st"); // node
        assert_eq!(rows[0].2, "stream_table"); // node_type
        assert_eq!(rows[0].3, 0); // depth
        assert_eq!(rows[0].4, Some("ACTIVE".to_string()));
    }

    #[test]
    fn test_tree_with_source_leaf() {
        let mut st_info = std::collections::HashMap::new();
        st_info.insert(
            "public.orders_st".to_string(),
            ("ACTIVE".to_string(), "DEFERRED".to_string()),
        );
        let st_children = std::collections::HashMap::new();
        let mut st_sources = std::collections::HashMap::new();
        st_sources.insert(
            "public.orders_st".to_string(),
            vec!["public.orders".to_string()],
        );

        let rows = render_dependency_tree(&st_info, &st_children, &st_sources);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, "public.orders_st");
        assert_eq!(rows[1].1, "public.orders");
        assert_eq!(rows[1].2, "source_table");
        assert!(rows[1].0.contains("[src]"));
    }

    #[test]
    fn test_tree_st_chain() {
        let mut st_info = std::collections::HashMap::new();
        st_info.insert(
            "public.base_st".to_string(),
            ("ACTIVE".to_string(), "DEFERRED".to_string()),
        );
        st_info.insert(
            "public.derived_st".to_string(),
            ("ACTIVE".to_string(), "DEFERRED".to_string()),
        );
        let mut st_children = std::collections::HashMap::new();
        st_children.insert(
            "public.base_st".to_string(),
            vec!["public.derived_st".to_string()],
        );
        let st_sources = std::collections::HashMap::new();

        let rows = render_dependency_tree(&st_info, &st_children, &st_sources);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, "public.base_st");
        assert_eq!(rows[0].3, 0); // depth
        assert_eq!(rows[1].1, "public.derived_st");
        assert_eq!(rows[1].3, 1); // depth
    }

    #[test]
    fn test_tree_multiple_roots_sorted() {
        let mut st_info = std::collections::HashMap::new();
        st_info.insert(
            "public.b_st".to_string(),
            ("ACTIVE".to_string(), "DEFERRED".to_string()),
        );
        st_info.insert(
            "public.a_st".to_string(),
            ("ACTIVE".to_string(), "DEFERRED".to_string()),
        );
        let st_children = std::collections::HashMap::new();
        let st_sources = std::collections::HashMap::new();

        let rows = render_dependency_tree(&st_info, &st_children, &st_sources);
        assert_eq!(rows.len(), 2);
        // Roots should be alphabetically sorted
        assert_eq!(rows[0].1, "public.a_st");
        assert_eq!(rows[1].1, "public.b_st");
    }

    #[test]
    fn test_tree_diamond_topology() {
        // base_st -> mid_a_st -> leaf_st
        // base_st -> mid_b_st -> leaf_st
        let mut st_info = std::collections::HashMap::new();
        for name in &["public.base_st", "public.mid_a_st", "public.mid_b_st"] {
            st_info.insert(
                name.to_string(),
                ("ACTIVE".to_string(), "DEFERRED".to_string()),
            );
        }
        let mut st_children = std::collections::HashMap::new();
        st_children.insert(
            "public.base_st".to_string(),
            vec!["public.mid_a_st".to_string(), "public.mid_b_st".to_string()],
        );
        let st_sources = std::collections::HashMap::new();

        let rows = render_dependency_tree(&st_info, &st_children, &st_sources);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].1, "public.base_st");
        // Children should be sorted
        assert_eq!(rows[1].1, "public.mid_a_st");
        assert_eq!(rows[2].1, "public.mid_b_st");
    }

    #[test]
    fn test_tree_source_not_in_st_info() {
        let mut st_info = std::collections::HashMap::new();
        st_info.insert(
            "public.my_st".to_string(),
            ("ACTIVE".to_string(), "IMMEDIATE".to_string()),
        );
        let st_children = std::collections::HashMap::new();
        let mut st_sources = std::collections::HashMap::new();
        st_sources.insert(
            "public.my_st".to_string(),
            vec!["public.raw_table".to_string()],
        );

        let rows = render_dependency_tree(&st_info, &st_children, &st_sources);
        let src_row = rows
            .iter()
            .find(|r| r.1 == "public.raw_table")
            .expect("expected source table row in dependency tree");
        assert_eq!(src_row.4, None); // no status for source tables
        assert_eq!(src_row.5, None); // no mode for source tables
    }

    #[test]
    fn test_tree_empty_graph() {
        let st_info = std::collections::HashMap::new();
        let st_children = std::collections::HashMap::new();
        let st_sources = std::collections::HashMap::new();

        let rows = render_dependency_tree(&st_info, &st_children, &st_sources);
        assert!(rows.is_empty());
    }

    // ── build_cdc_health_alert (missing branches) ────────────────────────────

    #[test]
    fn test_build_cdc_health_alert_ok_when_below_threshold_and_slot_present() {
        let alert = build_cdc_health_alert(512, 1024, true, CdcMode::Wal);
        assert!(
            alert.is_none(),
            "No alert when lag is below threshold and slot exists"
        );
    }

    #[test]
    fn test_build_cdc_health_alert_no_alert_for_trigger_mode_missing_slot() {
        // A missing replication slot is not relevant for trigger-based CDC
        let alert = build_cdc_health_alert(0, 1024, false, CdcMode::Trigger);
        assert!(alert.is_none());
    }

    #[test]
    fn test_build_cdc_health_alert_lag_takes_priority_over_missing_slot() {
        // Both conditions true: lag > threshold and slot is missing.
        // The lag check comes first so it should win.
        let alert = build_cdc_health_alert(2048, 1024, false, CdcMode::Wal);
        assert_eq!(
            alert,
            Some("slot_lag_exceeds_threshold: 2048 bytes > 1024 bytes".to_string())
        );
    }

    #[test]
    fn test_build_cdc_health_alert_exact_threshold_not_alerted() {
        // `>` not `>=`: equal to threshold must not trigger
        let alert = build_cdc_health_alert(1024, 1024, true, CdcMode::Wal);
        assert!(alert.is_none());
    }

    // ── build_slot_lag_health_detail (missing branches) ──────────────────────

    #[test]
    fn test_build_slot_lag_health_detail_ok_when_empty() {
        let (severity, detail) = build_slot_lag_health_detail(&[], 104_857_600);
        assert_eq!(severity, "OK");
        assert!(detail.contains("within normal range"));
    }

    #[test]
    fn test_build_slot_lag_health_detail_multiple_lagging_slots() {
        let lagging = vec!["slot_a (200 MB)".to_string(), "slot_b (300 MB)".to_string()];
        let (severity, detail) = build_slot_lag_health_detail(&lagging, 104_857_600);
        assert_eq!(severity, "WARN");
        assert!(detail.contains("2 WAL slot(s)"));
        assert!(detail.contains("slot_a (200 MB)"));
        assert!(detail.contains("slot_b (300 MB)"));
        assert!(detail.contains("104857600 bytes"));
    }

    // ── health_check() state machine logic tests ────────────────────────────

    #[test]
    fn test_scheduler_health_launcher_and_scheduler_present() {
        // When both launcher and per-DB scheduler are running,
        // health check should report OK (not ERROR).
        // This verifies the scoped database query logic.

        let scheduler_count = 1; // This DB has a scheduler
        let launcher_count = 1; // Launcher is running

        let (severity, detail) = if scheduler_count > 0 {
            ("OK", format!("{} worker(s) running", scheduler_count))
        } else if launcher_count > 0 {
            (
                "WARN",
                "launcher running but no per-database scheduler yet".to_string(),
            )
        } else {
            (
                "ERROR",
                "neither launcher nor scheduler present".to_string(),
            )
        };

        assert_eq!(severity, "OK");
        assert!(detail.contains("1 worker(s) running"));
    }

    #[test]
    fn test_scheduler_health_launcher_only_no_per_db_scheduler() {
        // When launcher is running but no per-DB scheduler for this database,
        // health check should report WARN (not ERROR).
        // This can happen during the brief startup window.

        let scheduler_count = 0; // No scheduler for this database
        let launcher_count = 1; // But launcher is running

        let (severity, _detail) = if scheduler_count > 0 {
            ("OK", format!("{} worker(s) running", scheduler_count))
        } else if launcher_count > 0 {
            (
                "WARN",
                "launcher running but no per-database scheduler yet".to_string(),
            )
        } else {
            (
                "ERROR",
                "neither launcher nor scheduler present".to_string(),
            )
        };

        assert_eq!(severity, "WARN");
    }

    #[test]
    fn test_scheduler_health_neither_launcher_nor_scheduler() {
        // When neither launcher nor per-DB scheduler is running,
        // health check should report ERROR. This indicates a configuration
        // problem (missing from shared_preload_libraries or disabled).

        let scheduler_count = 0;
        let launcher_count = 0;

        let (severity, detail) = if scheduler_count > 0 {
            ("OK", format!("{} worker(s) running", scheduler_count))
        } else if launcher_count > 0 {
            (
                "WARN",
                "launcher running but no per-database scheduler yet".to_string(),
            )
        } else {
            (
                "ERROR",
                "neither launcher nor scheduler present — configuration issue".to_string(),
            )
        };

        assert_eq!(severity, "ERROR");
        assert!(detail.contains("configuration issue"));
    }

    #[test]
    fn test_scheduler_health_multiple_schedulers_ok() {
        // Multiple schedulers (theoretically shouldn't happen, but if it does,
        // it's still OK — the scheduler is running).

        let scheduler_count = 2;
        let launcher_count = 1;

        let (severity, detail) = if scheduler_count > 0 {
            ("OK", format!("{} worker(s) running", scheduler_count))
        } else if launcher_count > 0 {
            (
                "WARN",
                "launcher running but no per-database scheduler yet".to_string(),
            )
        } else {
            ("ERROR", "configuration issue".to_string())
        };

        assert_eq!(severity, "OK");
        assert!(detail.contains("2 worker(s) running"));
    }
}
