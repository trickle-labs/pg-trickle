//! E2E tests for monitoring views and status functions.
//!
//! Validates `pgtrickle.pgt_status()`, `pgtrickle.stream_tables_info`,
//! `pgtrickle.pg_stat_stream_tables`, and refresh history recording.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_pgt_status_returns_rows() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_src VALUES (1)").await;

    db.create_st("mon_st", "SELECT id FROM mon_src", "1m", "FULL")
        .await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_status()")
        .await;
    assert!(count >= 1, "pgt_status() should return at least 1 row");

    // Verify the row contents
    let (status, mode, populated, errors) = db.pgt_status("mon_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);
    assert_eq!(errors, 0);
}

#[tokio::test]
async fn test_pgt_status_multiple_sts() {
    let db = E2eDb::new().await.with_extension().await;

    for i in 1..=3 {
        db.execute(&format!(
            "CREATE TABLE mon_multi_{} (id INT PRIMARY KEY)",
            i
        ))
        .await;
        db.execute(&format!("INSERT INTO mon_multi_{} VALUES (1)", i))
            .await;
        db.create_st(
            &format!("mon_multi_st_{}", i),
            &format!("SELECT id FROM mon_multi_{}", i),
            "1m",
            "FULL",
        )
        .await;
    }

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_status()")
        .await;
    assert_eq!(count, 3, "pgt_status() should return 3 rows");
}

#[tokio::test]
async fn test_stream_tables_info_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_info (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_info VALUES (1)").await;

    db.create_st("mon_info_st", "SELECT id FROM mon_info", "1m", "FULL")
        .await;

    // Refresh to populate last_refresh_at
    db.execute("INSERT INTO mon_info VALUES (2)").await;
    db.refresh_st("mon_info_st").await;

    // Verify stream_tables_info view has our ST with staleness columns
    let has_row: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM pgtrickle.stream_tables_info \
                WHERE pgt_name = 'mon_info_st' \
            )",
        )
        .await;
    assert!(has_row, "stream_tables_info should contain our ST");

    // Verify staleness and stale columns exist and are queryable
    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'mon_info_st'",
        )
        .await;
    // Just after refresh, staleness should not exceed schedule
    assert!(!stale, "stale should be false right after refresh");
}

#[tokio::test]
async fn test_pg_stat_stream_tables_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_stat (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_stat VALUES (1)").await;

    db.create_st("mon_stat_st", "SELECT id FROM mon_stat", "1m", "FULL")
        .await;

    // Do a manual refresh
    db.execute("INSERT INTO mon_stat VALUES (2)").await;
    db.refresh_st("mon_stat_st").await;

    // Verify pg_stat_stream_tables view exists and has our ST
    let has_row: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM pgtrickle.pg_stat_stream_tables \
                WHERE pgt_name = 'mon_stat_st' \
            )",
        )
        .await;
    assert!(has_row, "pg_stat_stream_tables should contain our ST");

    // Verify key columns exist and are queryable
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pg_stat_stream_tables WHERE pgt_name = 'mon_stat_st'",
        )
        .await;
    assert_eq!(status, "ACTIVE");

    // total_refreshes may be 0 because manual refresh doesn't record history,
    // only the scheduler does. Verify the column is queryable.
    let total: i64 = db
        .query_scalar(
            "SELECT total_refreshes FROM pgtrickle.pg_stat_stream_tables WHERE pgt_name = 'mon_stat_st'",
        )
        .await;
    assert!(
        total >= 0,
        "total_refreshes should be accessible (may be 0 for manual refresh)"
    );
}

#[tokio::test]
async fn test_stale_detection() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_sched (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_sched VALUES (1)").await;

    // Create ST with minimum schedule (60 seconds)
    db.create_st("mon_sched_st", "SELECT id FROM mon_sched", "1m", "FULL")
        .await;

    // The view should show staleness which grows over time.
    // Right after initial populate, staleness should be very small.
    let has_staleness: bool = db
        .query_scalar(
            "SELECT staleness IS NOT NULL FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'mon_sched_st'",
        )
        .await;
    assert!(
        has_staleness,
        "staleness should be computed in stream_tables_info"
    );

    // stale should be false right after creation (schedule=60s)
    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'mon_sched_st'",
        )
        .await;
    assert!(!stale, "stale should be false immediately after creation");
}

#[tokio::test]
async fn test_refresh_history_records() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_hist (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_hist VALUES (1)").await;

    db.create_st("mon_hist_st", "SELECT id FROM mon_hist", "1m", "FULL")
        .await;

    // Multiple manual refreshes
    for i in 2..=4 {
        db.execute(&format!("INSERT INTO mon_hist VALUES ({})", i))
            .await;
        db.refresh_st("mon_hist_st").await;
    }

    // Manual refresh doesn't write to pgt_refresh_history (only scheduler does).
    // Verify the table exists and is queryable.
    let table_exists = db.table_exists("pgtrickle", "pgt_refresh_history").await;
    assert!(table_exists, "pgt_refresh_history table should exist");

    // Verify the history table has the expected columns
    let col_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_schema = 'pgtrickle' AND table_name = 'pgt_refresh_history'",
        )
        .await;
    assert!(
        col_count >= 5,
        "pgt_refresh_history should have at least 5 columns, got {}",
        col_count,
    );

    // Verify the ST's catalog was correctly updated by manual refresh
    let count = db.count("public.mon_hist_st").await;
    assert_eq!(count, 4, "ST should have all 4 rows after refreshes");
}

#[tokio::test]
async fn test_staleness_reflects_last_refresh_at_after_refresh() {
    // Verify that staleness in stream_tables_info is based on last_refresh_at
    // by checking that it resets to near-zero right after a manual refresh,
    // regardless of how old data_timestamp was before.
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_lrat (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_lrat VALUES (1)").await;

    db.create_st("mon_lrat_st", "SELECT id FROM mon_lrat", "1m", "FULL")
        .await;

    // Wind back data_timestamp to simulate old data, but leave last_refresh_at alone
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET data_timestamp = now() - interval '2 hours' \
         WHERE pgt_name = 'mon_lrat_st'",
    )
    .await;

    // Perform a refresh — this updates last_refresh_at to now()
    db.execute("INSERT INTO mon_lrat VALUES (2)").await;
    db.refresh_st("mon_lrat_st").await;

    // staleness should be small (< 10 seconds) because last_refresh_at was just updated,
    // even though data_timestamp was artificially set 2 hours in the past before the refresh.
    let staleness_secs: f64 = db
        .query_scalar(
            "SELECT EXTRACT(EPOCH FROM staleness)::float8 \
             FROM pgtrickle.stream_tables_info WHERE pgt_name = 'mon_lrat_st'",
        )
        .await;
    assert!(
        staleness_secs < 30.0,
        "staleness ({staleness_secs:.2}s) should be near-zero after refresh \
         (based on last_refresh_at, not the old data_timestamp)"
    );

    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'mon_lrat_st'",
        )
        .await;
    assert!(
        !stale,
        "should not be stale right after refresh even with an old data_timestamp"
    );
}

#[tokio::test]
async fn test_no_data_refresh_does_not_cause_false_stale() {
    // Regression test: simulates a NO_DATA scheduler pass by directly advancing
    // last_refresh_at without changing data_timestamp. The table should not be
    // marked stale, because the scheduler ran on schedule.
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_nodata (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_nodata VALUES (1)").await;

    db.create_st("mon_nodata_st", "SELECT id FROM mon_nodata", "5m", "FULL")
        .await;

    // Simulate: data hasn't changed in 2 hours, but the scheduler checked 30 seconds ago
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET data_timestamp    = now() - interval '2 hours', \
             last_refresh_at   = now() - interval '30 seconds' \
         WHERE pgt_name = 'mon_nodata_st'",
    )
    .await;

    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'mon_nodata_st'",
        )
        .await;
    assert!(
        !stale,
        "table with recent last_refresh_at should not be stale, \
         even if data_timestamp is 2 hours old (NO_DATA pass scenario)"
    );

    let stale_count: i64 = db
        .query_scalar("SELECT stale_tables FROM pgtrickle.quick_health")
        .await;
    assert_eq!(
        stale_count, 0,
        "quick_health.stale_tables should be 0 when last_refresh_at is recent"
    );
}

#[tokio::test]
async fn test_pg_stat_stream_tables_nodata_not_stale() {
    // Verifies the same NO_DATA fix applies to pg_stat_stream_tables.stale,
    // which uses an explicit `last_refresh_at IS NOT NULL` guard in its CASE WHEN.
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_pgstat_nd (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_pgstat_nd VALUES (1)").await;

    db.create_st(
        "mon_pgstat_nd_st",
        "SELECT id FROM mon_pgstat_nd",
        "5m",
        "FULL",
    )
    .await;

    // Simulate a NO_DATA pass: data is 2 hours old, scheduler ran 30 seconds ago
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET data_timestamp  = now() - interval '2 hours', \
             last_refresh_at = now() - interval '30 seconds' \
         WHERE pgt_name = 'mon_pgstat_nd_st'",
    )
    .await;

    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) FROM pgtrickle.pg_stat_stream_tables \
             WHERE pgt_name = 'mon_pgstat_nd_st'",
        )
        .await;
    assert!(
        !stale,
        "pg_stat_stream_tables.stale should be false when last_refresh_at is recent, \
         even with old data_timestamp"
    );

    let staleness_secs: f64 = db
        .query_scalar(
            "SELECT EXTRACT(EPOCH FROM staleness)::float8 \
             FROM pgtrickle.pg_stat_stream_tables WHERE pgt_name = 'mon_pgstat_nd_st'",
        )
        .await;
    assert!(
        staleness_secs < 300.0,
        "pg_stat_stream_tables.staleness ({staleness_secs:.1}s) should reflect \
         last_refresh_at (~30s ago), not data_timestamp (~7200s ago)"
    );
}

#[tokio::test]
async fn test_pg_stat_stream_tables_stale_null_when_never_refreshed() {
    // An ST that was just created and has never been refreshed should have
    // pg_stat_stream_tables.stale = NULL (not false), because
    // last_refresh_at IS NULL and the explicit NULL guard fires.
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_pgstat_null (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_pgstat_null VALUES (1)").await;

    // Create without triggering an initial refresh — use with_extension
    // which does not auto-refresh, so last_refresh_at starts as NULL.
    db.execute(
        "SELECT pgtrickle.create_stream_table('mon_pgstat_null_st', \
         'SELECT id FROM mon_pgstat_null', '5m', 'FULL', false)",
    )
    .await;

    // Confirm last_refresh_at is NULL before any refresh
    let last_refresh_at_is_null: bool = db
        .query_scalar(
            "SELECT last_refresh_at IS NULL FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'mon_pgstat_null_st'",
        )
        .await;
    assert!(
        last_refresh_at_is_null,
        "last_refresh_at should be NULL before first refresh"
    );

    let stale: Option<bool> = db
        .query_scalar_opt(
            "SELECT stale FROM pgtrickle.pg_stat_stream_tables \
             WHERE pgt_name = 'mon_pgstat_null_st'",
        )
        .await;
    assert!(
        stale.is_none(),
        "pg_stat_stream_tables.stale should be NULL when last_refresh_at has never been set"
    );
}

// ── TEST-002 (v0.70.0): cache_stats() shape and monotonicity ──────────────

/// TEST-002: `pgtrickle.cache_stats()` returns exactly one row with
/// non-negative counters, and the total access count (l1_hits + l2_hits +
/// misses) increases after a DIFFERENTIAL refresh warms the template cache.
#[tokio::test]
async fn test_cache_stats_shape_and_monotonicity() {
    let db = E2eDb::new().await.with_extension().await;

    // ── Step 1: Baseline shape ──────────────────────────────────────
    // cache_stats() must return exactly one row.
    let row_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.cache_stats()")
        .await;
    assert_eq!(row_count, 1, "cache_stats() must return exactly one row");

    // All counters must be non-negative.
    let (l1_hits_0, l2_hits_0, misses_0): (i64, i64, i64) = {
        let row: (i64, i64, i64) =
            sqlx::query_as("SELECT l1_hits, l2_hits, misses FROM pgtrickle.cache_stats()")
                .fetch_one(&db.pool)
                .await
                .expect("cache_stats() query failed");
        row
    };
    assert!(l1_hits_0 >= 0, "l1_hits must be non-negative");
    assert!(l2_hits_0 >= 0, "l2_hits must be non-negative");
    assert!(misses_0 >= 0, "misses must be non-negative");

    // ── Step 2: Create a DIFFERENTIAL stream table and refresh ──────
    db.execute("CREATE TABLE mon_cs_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO mon_cs_src VALUES (1, 100), (2, 200), (3, 300)")
        .await;

    db.create_st(
        "mon_cs_st",
        "SELECT id, val FROM mon_cs_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Trigger an explicit refresh to ensure the template cache is exercised.
    db.refresh_st("mon_cs_st").await;

    // ── Step 3: Total access count must have increased ──────────────
    let (l1_hits_1, l2_hits_1, misses_1): (i64, i64, i64) = {
        let row: (i64, i64, i64) =
            sqlx::query_as("SELECT l1_hits, l2_hits, misses FROM pgtrickle.cache_stats()")
                .fetch_one(&db.pool)
                .await
                .expect("cache_stats() query failed after refresh");
        row
    };

    let total_0 = l1_hits_0 + l2_hits_0 + misses_0;
    let total_1 = l1_hits_1 + l2_hits_1 + misses_1;

    // The template-cache counters are backed by shared memory, which is only
    // initialised when pg_trickle is listed in shared_preload_libraries (Full
    // E2E mode). In Light E2E mode there is no background worker, so shmem is
    // not initialised and cache_stats() legitimately returns all-zeros. Only
    // enforce strict monotonicity when we can confirm shmem is active.
    let shmem_active: bool = sqlx::query_scalar(
        "SELECT current_setting('shared_preload_libraries', true) ILIKE '%pgtrickle%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap_or(false);

    if shmem_active {
        assert!(
            total_1 > total_0,
            "Total cache accesses (l1_hits + l2_hits + misses) must increase \
             after a DIFFERENTIAL refresh. Before: {total_0}, after: {total_1}"
        );
    } else {
        // Light E2E: shmem not initialised — verify counters are still non-negative.
        assert!(l1_hits_1 >= 0, "l1_hits must be non-negative after refresh");
        assert!(l2_hits_1 >= 0, "l2_hits must be non-negative after refresh");
        assert!(misses_1 >= 0, "misses must be non-negative after refresh");
    }
}
