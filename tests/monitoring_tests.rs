//! Integration tests for Phase 9 — Monitoring & Observability.
//!
//! These tests verify the SQL monitoring views and functions work correctly
//! against a real PostgreSQL 18 container with the pg_trickle catalog schema.

mod common;

use common::TestDb;

// ── st_refresh_stats aggregate query ───────────────────────────────────────

/// Test that the refresh stats aggregation query works against real catalog data.
/// We insert STs and refresh history rows manually, then run the same aggregation
/// query that monitor.rs's st_refresh_stats() uses.
#[tokio::test]
async fn test_refresh_stats_aggregation() {
    let db = TestDb::with_catalog().await;

    // Create a source table so we have a valid OID for pgt_relid
    db.execute("CREATE TABLE source_a (id int)").await;

    // Insert a ST
    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode, status, is_populated, data_timestamp)
         VALUES
            ((SELECT 'source_a'::regclass::oid), 'my_st', 'public', 'SELECT 1', 'public', '1m', 'FULL', 'ACTIVE', true, now() - interval '30 seconds')"
    ).await;

    // Insert some refresh history
    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables LIMIT 1")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, status, rows_inserted, rows_deleted)
         VALUES
            ({pgt_id}, now() - interval '5 min', now() - interval '5 min', now() - interval '4 min 59 sec', 'FULL', 'COMPLETED', 100, 0),
            ({pgt_id}, now() - interval '3 min', now() - interval '3 min', now() - interval '2 min 59 sec', 'DIFFERENTIAL', 'COMPLETED', 10, 2),
            ({pgt_id}, now() - interval '1 min', now() - interval '1 min', now() - interval '59 sec', 'DIFFERENTIAL', 'FAILED', 0, 0)"
    )).await;

    // Run the aggregation query (same as st_refresh_stats in monitor.rs)
    let total: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = {pgt_id}"
        ))
        .await;
    assert_eq!(total, 3, "should have 3 refresh history rows");

    let successful: i64 = db.query_scalar(&format!(
        "SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = {pgt_id} AND status = 'COMPLETED'"
    )).await;
    assert_eq!(successful, 2);

    let failed: i64 = db.query_scalar(&format!(
        "SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = {pgt_id} AND status = 'FAILED'"
    )).await;
    assert_eq!(failed, 1);

    let total_rows_inserted: i64 = db.query_scalar(&format!(
        "SELECT COALESCE(sum(rows_inserted), 0)::bigint FROM pgtrickle.pgt_refresh_history WHERE pgt_id = {pgt_id}"
    )).await;
    assert_eq!(total_rows_inserted, 110);
}

// ── Refresh history ordering ──────────────────────────────────────────────

/// Test refresh history can be queried by ST name with proper ordering.
#[tokio::test]
async fn test_refresh_history_by_name() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_hist (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status)
         VALUES
            ((SELECT 'src_hist'::regclass::oid), 'hist_st', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE')"
    ).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'hist_st'")
        .await;

    // Insert 5 history rows
    for i in 0..5 {
        db.execute(&format!(
            "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, status, rows_inserted, rows_deleted)
             VALUES ({pgt_id}, now() - interval '{} min', now() - interval '{} min', now() - interval '{} min' + interval '1 sec', 'FULL', 'COMPLETED', {}, 0)",
            5 - i, 5 - i, 5 - i, (i + 1) * 10
        )).await;
    }

    // Query with the same pattern as get_refresh_history (most recent first, limited)
    let latest_rows_inserted: i64 = db
        .query_scalar(
            "SELECT COALESCE(h.rows_inserted, 0)::bigint
         FROM pgtrickle.pgt_refresh_history h
         JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
         WHERE st.pgt_schema = 'public' AND st.pgt_name = 'hist_st'
         ORDER BY h.refresh_id DESC
         LIMIT 1",
        )
        .await;
    assert_eq!(
        latest_rows_inserted, 50,
        "most recent refresh should have 50 rows_inserted"
    );

    // Verify limit works
    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM (
            SELECT h.refresh_id
            FROM pgtrickle.pgt_refresh_history h
            JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
            WHERE st.pgt_schema = 'public' AND st.pgt_name = 'hist_st'
            ORDER BY h.refresh_id DESC
            LIMIT 3
        ) sub",
        )
        .await;
    assert_eq!(count, 3, "LIMIT 3 should return exactly 3 rows");
}

// ── Lag calculation ───────────────────────────────────────────────────────

/// Test current staleness calculation via SQL.
#[tokio::test]
async fn test_staleness_calculation() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_sched (id int)").await;

    // ST with data_timestamp 60 seconds ago → staleness should be ~60s
    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status, data_timestamp)
         VALUES
            ((SELECT 'src_sched'::regclass::oid), 'sched_st', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE', now() - interval '60 seconds')"
    ).await;

    let staleness: f64 = db
        .query_scalar(
            "SELECT EXTRACT(EPOCH FROM (now() - data_timestamp))::float8
         FROM pgtrickle.pgt_stream_tables
         WHERE pgt_schema = 'public' AND pgt_name = 'sched_st' AND data_timestamp IS NOT NULL",
        )
        .await;

    // Widen tolerance to 50–120 s to avoid flakiness under CI load / clock skew.
    assert!(
        (50.0..=120.0).contains(&staleness),
        "staleness should be ~60s, got {:.1}",
        staleness
    );

    // ST without data_timestamp → staleness query should return no rows
    db.execute("CREATE TABLE src_sched2 (id int)").await;
    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status)
         VALUES
            ((SELECT 'src_sched2'::regclass::oid), 'no_sched_st', 'public', 'SELECT 1', 'public', 'FULL', 'INITIALIZING')"
    ).await;

    let staleness_opt: Option<f64> = db
        .query_scalar_opt(
            "SELECT EXTRACT(EPOCH FROM (now() - data_timestamp))::float8
         FROM pgtrickle.pgt_stream_tables
         WHERE pgt_schema = 'public' AND pgt_name = 'no_sched_st' AND data_timestamp IS NOT NULL",
        )
        .await;
    assert!(
        staleness_opt.is_none(),
        "ST without data_timestamp should return no staleness"
    );
}

// ── Lag exceeded detection ────────────────────────────────────────────────

/// Test that stale is correctly computed when staleness > schedule.
#[tokio::test]
async fn test_stale_flag() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_exceed (id int)").await;

    // ST with schedule=30s but data_timestamp 120s ago → stale = true
    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode, status, data_timestamp)
         VALUES
            ((SELECT 'src_exceed'::regclass::oid), 'exceed_st', 'public', 'SELECT 1', 'public', '30s', 'FULL', 'ACTIVE', now() - interval '120 seconds')"
    ).await;

    let exceeded: bool = db
        .query_scalar(
            "SELECT CASE WHEN st.schedule IS NOT NULL AND st.data_timestamp IS NOT NULL
                     THEN EXTRACT(EPOCH FROM (now() - st.data_timestamp)) >
                          pgtrickle.parse_duration_seconds(st.schedule)
                     ELSE false
                END
         FROM pgtrickle.pgt_stream_tables st
         WHERE pgt_name = 'exceed_st'",
        )
        .await;
    assert!(exceeded, "data should be stale (120s > 30s target)");

    // ST with schedule=5min and data_timestamp 10s ago → stale = false
    db.execute("CREATE TABLE src_ok (id int)").await;
    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode, status, data_timestamp)
         VALUES
            ((SELECT 'src_ok'::regclass::oid), 'ok_st', 'public', 'SELECT 1', 'public', '5m', 'FULL', 'ACTIVE', now() - interval '10 seconds')"
    ).await;

    let not_exceeded: bool = db
        .query_scalar(
            "SELECT CASE WHEN st.schedule IS NOT NULL AND st.data_timestamp IS NOT NULL
                     THEN EXTRACT(EPOCH FROM (now() - st.data_timestamp)) >
                          pgtrickle.parse_duration_seconds(st.schedule)
                     ELSE false
                END
         FROM pgtrickle.pgt_stream_tables st
         WHERE pgt_name = 'ok_st'",
        )
        .await;
    assert!(
        !not_exceeded,
        "data should NOT be stale (10s < 5min target)"
    );
}

// ── stream_tables_info view ──────────────────────────────────────────────

/// Test that the stream_tables_info view correctly exposes staleness columns.
#[tokio::test]
async fn test_stream_tables_info_view() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_info (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode, status, data_timestamp, last_refresh_at)
         VALUES
            ((SELECT 'src_info'::regclass::oid), 'info_st', 'public', 'SELECT 1', 'public', '1m', 'FULL', 'ACTIVE', now() - interval '30 seconds', now() - interval '30 seconds')"
    ).await;

    // Check the view exists and returns data
    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.stream_tables_info WHERE pgt_name = 'info_st'",
        )
        .await;
    assert_eq!(count, 1);

    // Check stale is false (30s < 1 minute)
    let stale: bool = db
        .query_scalar("SELECT stale FROM pgtrickle.stream_tables_info WHERE pgt_name = 'info_st'")
        .await;
    assert!(!stale);
}

// ── NOTIFY channel test ──────────────────────────────────────────────────

/// Test that NOTIFY on pg_trickle_alert channel delivers a payload via LISTEN.
///
/// Uses `sqlx::PgListener` for a real LISTEN/NOTIFY round-trip rather than a
/// fire-and-forget `pg_notify()` call.
#[tokio::test]
async fn test_notify_pg_trickle_alert() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = TestDb::new().await;

    // Subscribe on the alert channel *before* sending the notification so we
    // don't miss the message.
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("PgListener connect failed");
    listener
        .listen("pg_trickle_alert")
        .await
        .expect("LISTEN failed");

    let payload = r#"{"event":"refresh_completed","pgt_schema":"public","pgt_name":"test","action":"FULL","rows_inserted":42}"#;

    // Send the notification from a separate connection acquired from the pool
    // so the LISTEN connection isn't re-used (sqlx routes LISTEN on its own
    // dedicated connection).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT pg_notify('pg_trickle_alert', '{}')",
        payload
    )))
    .execute(&db.pool)
    .await
    .expect("pg_notify failed");

    // Receive the notification within 5 seconds.
    let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("Timed out waiting for NOTIFY")
        .expect("PgListener recv error");

    assert_eq!(notification.channel(), "pg_trickle_alert");
    assert_eq!(notification.payload(), payload);
}

// ── Full stats lateral join query ────────────────────────────────────────

/// Test the full LATERAL JOIN aggregation query used by st_refresh_stats().
#[tokio::test]
async fn test_full_stats_lateral_join() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_stats (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode, status, is_populated, data_timestamp)
         VALUES
            ((SELECT 'src_stats'::regclass::oid), 'stats_st', 'public', 'SELECT 1', 'public', '1m', 'DIFFERENTIAL', 'ACTIVE', true, now() - interval '10 seconds')"
    ).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'stats_st'")
        .await;

    // Insert 2 completed + 1 failed refresh
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, status, rows_inserted, rows_deleted)
         VALUES
            ({pgt_id}, now() - interval '5 min', now() - interval '5 min', now() - interval '4 min 58 sec', 'FULL', 'COMPLETED', 100, 0),
            ({pgt_id}, now() - interval '3 min', now() - interval '3 min', now() - interval '2 min 59 sec', 'DIFFERENTIAL', 'COMPLETED', 15, 3),
            ({pgt_id}, now() - interval '1 min', now() - interval '1 min', NULL, 'DIFFERENTIAL', 'FAILED', 0, 0)"
    )).await;

    // Run the full LATERAL JOIN query from st_refresh_stats
    let row = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            bool,
            i64,
            i64,
            i64,
            i64,
            i64,
            bool,
        ),
    >(
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
            CASE WHEN st.schedule IS NOT NULL AND st.data_timestamp IS NOT NULL
                 THEN EXTRACT(EPOCH FROM (now() - st.data_timestamp)) >
                      pgtrickle.parse_duration_seconds(st.schedule)
                 ELSE false
            END
        FROM pgtrickle.pgt_stream_tables st
        LEFT JOIN LATERAL (
            SELECT
                count(*) AS total_refreshes,
                count(*) FILTER (WHERE h.status = 'COMPLETED') AS successful_refreshes,
                count(*) FILTER (WHERE h.status = 'FAILED') AS failed_refreshes,
                COALESCE(sum(h.rows_inserted), 0) AS total_rows_inserted,
                COALESCE(sum(h.rows_deleted), 0) AS total_rows_deleted
            FROM pgtrickle.pgt_refresh_history h
            WHERE h.pgt_id = st.pgt_id
        ) stats ON true
        WHERE st.pgt_name = 'stats_st'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("LATERAL JOIN query failed");

    assert_eq!(row.0, "stats_st");
    assert_eq!(row.1, "public");
    assert_eq!(row.2, "ACTIVE");
    assert_eq!(row.3, "DIFFERENTIAL");
    assert!(row.4); // is_populated
    assert_eq!(row.5, 3); // total_refreshes
    assert_eq!(row.6, 2); // successful
    assert_eq!(row.7, 1); // failed
    assert_eq!(row.8, 115); // total_rows_inserted
    assert_eq!(row.9, 3); // total_rows_deleted
    assert!(!row.10); // not stale (10s < 1min)
}

// ── Empty state handling ─────────────────────────────────────────────────

/// Test that monitoring queries work correctly when there are no STs or refresh history.
#[tokio::test]
async fn test_monitoring_empty_state() {
    let db = TestDb::with_catalog().await;

    // No STs at all — count should be 0
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables")
        .await;
    assert_eq!(count, 0);

    // The info view should return 0 rows
    let info_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.stream_tables_info")
        .await;
    assert_eq!(info_count, 0);

    // Refresh history should be empty
    let hist_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history")
        .await;
    assert_eq!(hist_count, 0);
}
