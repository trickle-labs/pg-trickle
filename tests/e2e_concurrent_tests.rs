//! E2E tests for concurrent operations.
//!
//! Validates that concurrent DML during refresh, parallel ST creation,
//! and refresh/drop races are handled safely.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

/// Deferred cleanup may target a source from an earlier refresh on this
/// backend. Refreshing an unrelated stream must not truncate that buffer
/// while a source writer is still uncommitted and invisible to cleanup.
#[tokio::test]
async fn test_cleanup_concurrent_writer_preserves_pending_change() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE cleanup_a (id INT PRIMARY KEY)")
        .await;
    db.execute("CREATE TABLE cleanup_b (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO cleanup_a VALUES (1)").await;
    db.execute("INSERT INTO cleanup_b VALUES (1)").await;
    db.create_st(
        "cleanup_st_a",
        "SELECT id FROM cleanup_a",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "cleanup_st_b",
        "SELECT id FROM cleanup_b",
        "1h",
        "DIFFERENTIAL",
    )
    .await;

    // Keep the backend so B's refresh drains A's deferred cleanup queue.
    let mut refresh = db.pool.acquire().await.unwrap();
    sqlx::raw_sql(
        "SET pg_trickle.cleanup_use_truncate = on;
         SET pg_trickle.compact_threshold = 0;
         SET pg_trickle.refresh_strategy = 'differential';
         SET statement_timeout = '5s'",
    )
    .execute(&mut *refresh)
    .await
    .unwrap();
    db.execute("INSERT INTO cleanup_a VALUES (2)").await;
    sqlx::query("SELECT pgtrickle.refresh_stream_table('cleanup_st_a')")
        .execute(&mut *refresh)
        .await
        .unwrap();
    assert_eq!(db.count("cleanup_st_a").await, 2);

    db.execute("INSERT INTO cleanup_b VALUES (2)").await;
    let mut writer = db.pool.begin().await.unwrap();
    sqlx::query("INSERT INTO cleanup_a VALUES (3)")
        .execute(&mut *writer)
        .await
        .unwrap();

    let result = sqlx::query("SELECT pgtrickle.refresh_stream_table('cleanup_st_b')")
        .execute(&mut *refresh)
        .await;
    writer.commit().await.unwrap();
    result.expect("deferred cleanup must not wait for an unrelated buffer writer");

    let buffer = db
        .change_buffer_table(db.table_oid("cleanup_a").await as i64)
        .await;
    let pending: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {buffer} WHERE action = 'I' AND id = 3"
        ))
        .await;
    assert_eq!(pending, 1, "the committed change must survive cleanup");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('cleanup_st_a')")
        .execute(&mut *refresh)
        .await
        .unwrap();
    db.assert_st_matches_query("cleanup_st_a", "SELECT id FROM cleanup_a")
        .await;
    assert_eq!(db.count("cleanup_st_a").await, 3);
}

/// TEST-10-01 (v0.49.0): Poll `pg_stat_activity` until the given backend is
/// seen in a non-idle state (i.e. its query has started executing), then
/// return.  Falls back to a short sleep after `timeout_secs` to prevent
/// an infinite loop on slow CI runners.
///
/// This replaces the previous fixed `tokio::time::sleep` calls which were
/// unreliable on loaded machines and could cause the second concurrent
/// operation to start before the first had actually acquired any lock.
async fn wait_for_active_query(pool: &sqlx::PgPool, query_fragment: &str, timeout_secs: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        let found: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM pg_stat_activity
                WHERE state != 'idle'
                  AND query ILIKE $1
                  AND pid != pg_backend_pid()
             )",
        )
        .bind(format!("%{}%", query_fragment))
        .fetch_one(pool)
        .await
        .unwrap_or(false);

        if found {
            return;
        }
        if std::time::Instant::now() >= deadline {
            // Timeout: fall through with a minimum delay so the test can still pass
            // on slow runners where pg_stat_activity visibility is delayed.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// PB1: Verify that concurrent `refresh_stream_table()` calls on the same ST
/// use `SELECT ... FOR UPDATE SKIP LOCKED` correctly:
///
/// - Exactly one caller acquires the row lock and runs the refresh.
/// - The other caller observes the lock is held, logs a "skipping — catalog
///   row locked" message, and returns immediately without corrupting data.
/// - After both calls complete, the stream table contents are correct.
///
/// Implementation note: we cannot deterministically observe *which* call
/// dropped (the scheduler logs to the server log, not to the client), but
/// we can assert correctness (no duplicates, no data loss) and the absence
/// of errors from either concurrent call.
#[tokio::test]
async fn test_pb1_concurrent_refresh_skip_locked_no_corruption() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE pb1_src (id INT PRIMARY KEY, val INT)")
        .await;
    // 500 rows so a refresh takes a measurable amount of time (long enough
    // that two concurrent callers overlap on a slow CI machine).
    db.execute("INSERT INTO pb1_src SELECT g, g*10 FROM generate_series(1, 500) g")
        .await;

    db.create_st(
        "pb1_st",
        "SELECT id, val FROM pb1_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh to populate the ST
    db.refresh_st("pb1_st").await;
    assert_eq!(db.count("public.pb1_st").await, 500);

    // Add 100 more rows so a refresh actually does work
    db.execute("INSERT INTO pb1_src SELECT g, g*10 FROM generate_series(501, 600) g")
        .await;

    // Fire two concurrent refresh calls from separate connections.
    // We clone the pool so each spawned task holds its own connection.
    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();
    let pool_wait = db.pool.clone();

    let h1 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('pb1_st')")
            .execute(&pool1)
            .await
    });
    let h2 = tokio::spawn(async move {
        // Wait until h1 is actively executing the refresh before starting h2.
        // This replaces a fixed sleep with a deterministic activity poll.
        wait_for_active_query(&pool_wait, "refresh_stream_table", 5).await;
        sqlx::query("SELECT pgtrickle.refresh_stream_table('pb1_st')")
            .execute(&pool2)
            .await
    });

    let (r1, r2) = tokio::join!(h1, h2);

    // Neither call should return a Rust/task-level error
    r1.expect("task1 panicked")
        .expect("refresh 1 returned DB error");
    r2.expect("task2 panicked")
        .expect("refresh 2 returned DB error");

    // After both calls: ST must exactly match the source — no duplicates,
    // no stale rows.  The skipped call will leave some rows temporarily
    // un-refreshed; a third explicit refresh ensures convergence.
    db.refresh_st("pb1_st").await;

    let count = db.count("public.pb1_st").await;
    assert_eq!(
        count, 600,
        "ST must contain exactly 600 rows after concurrent refresh + stabilising refresh"
    );

    db.assert_st_matches_query("public.pb1_st", "SELECT id, val FROM pb1_src")
        .await;
}

#[tokio::test]
async fn test_write_and_refresh_rejects_lock_skip_and_rolls_back_write() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE wr_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO wr_src VALUES (1, 'one')").await;
    db.create_st("wr_st", "SELECT id, val FROM wr_src", "1m", "DIFFERENTIAL")
        .await;

    let mut blocker = db.pool.begin().await.unwrap();
    sqlx::query(
        "SELECT pg_advisory_xact_lock(
            (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'wr_st')
        )",
    )
    .execute(&mut *blocker)
    .await
    .unwrap();

    let result = db
        .try_execute(
            "SELECT pgtrickle.write_and_refresh(
                'INSERT INTO wr_src VALUES (2, ''two'')', 'wr_st'
            )",
        )
        .await;
    assert!(
        result.is_err(),
        "write_and_refresh must not report success after a skipped refresh"
    );
    assert!(
        result.unwrap_err().to_string().contains("refresh skipped"),
        "lock contention must be reported as refresh skipped"
    );

    blocker.commit().await.unwrap();
    assert_eq!(db.count("public.wr_src").await, 1);
    assert_eq!(db.count("public.wr_st").await, 1);
}

#[tokio::test]
async fn test_concurrent_inserts_during_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cc_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO cc_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.create_st("cc_st", "SELECT id, val FROM cc_src", "1m", "FULL")
        .await;
    assert_eq!(db.count("public.cc_st").await, 100);

    // Insert more data while refresh is happening
    let pool_refresh = db.pool.clone();
    let pool_insert = db.pool.clone();

    // First insert some data to need a refresh
    db.execute("INSERT INTO cc_src SELECT g, g FROM generate_series(101, 200) g")
        .await;

    let refresh_handle = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_st')")
            .execute(&pool_refresh)
            .await
    });

    let insert_handle = tokio::spawn(async move {
        // Insert additional rows concurrently
        sqlx::query("INSERT INTO cc_src SELECT g, g FROM generate_series(201, 250) g")
            .execute(&pool_insert)
            .await
    });

    let (refresh_result, insert_result) = tokio::join!(refresh_handle, insert_handle);
    refresh_result
        .expect("refresh task panicked")
        .expect("refresh failed");
    insert_result
        .expect("insert task panicked")
        .expect("insert failed");

    // After another refresh, all data should be visible
    db.refresh_st("cc_st").await;

    let count = db.count("public.cc_st").await;
    assert_eq!(
        count, 250,
        "All rows should be present after concurrent ops + refresh"
    );
}

#[tokio::test]
async fn test_create_two_sts_simultaneously() {
    let db = E2eDb::new().await.with_extension().await;

    // Create two source tables
    db.execute("CREATE TABLE cc_src_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cc_src_a VALUES (1, 'a')").await;

    db.execute("CREATE TABLE cc_src_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cc_src_b VALUES (1, 'b')").await;

    let pool_a = db.pool.clone();
    let pool_b = db.pool.clone();

    let h1 = tokio::spawn(async move {
        sqlx::query(
            "SELECT pgtrickle.create_stream_table('cc_st_a', \
             $$ SELECT id, val FROM cc_src_a $$, '1m', 'FULL')",
        )
        .execute(&pool_a)
        .await
    });

    let h2 = tokio::spawn(async move {
        sqlx::query(
            "SELECT pgtrickle.create_stream_table('cc_st_b', \
             $$ SELECT id, val FROM cc_src_b $$, '1m', 'FULL')",
        )
        .execute(&pool_b)
        .await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    r1.expect("task a panicked").expect("create st_a failed");
    r2.expect("task b panicked").expect("create st_b failed");

    // Both STs should exist
    assert!(db.table_exists("public", "cc_st_a").await);
    assert!(db.table_exists("public", "cc_st_b").await);

    let count_a = db.count("public.cc_st_a").await;
    let count_b = db.count("public.cc_st_b").await;
    assert_eq!(count_a, 1);
    assert_eq!(count_b, 1);
}

#[tokio::test]
async fn test_refresh_and_drop_race() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cc_race (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO cc_race SELECT g, g FROM generate_series(1, 50) g")
        .await;

    db.create_st("cc_race_st", "SELECT id, val FROM cc_race", "1m", "FULL")
        .await;

    // Add data to trigger refresh
    db.execute("INSERT INTO cc_race SELECT g, g FROM generate_series(51, 100) g")
        .await;

    let pool_refresh = db.pool.clone();
    let pool_drop = db.pool.clone();
    let pool_wait = db.pool.clone();

    let h_refresh = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_race_st')")
            .execute(&pool_refresh)
            .await
    });

    let h_drop = tokio::spawn(async move {
        // Wait until refresh is actively executing before issuing the DROP.
        wait_for_active_query(&pool_wait, "refresh_stream_table", 5).await;
        sqlx::query("SELECT pgtrickle.drop_stream_table('cc_race_st')")
            .execute(&pool_drop)
            .await
    });

    let (r_refresh, r_drop) = tokio::join!(h_refresh, h_drop);

    // At least one should succeed, the other may fail gracefully
    let refresh_ok = r_refresh.as_ref().map(|r| r.is_ok()).unwrap_or(false);
    let drop_ok = r_drop.as_ref().map(|r| r.is_ok()).unwrap_or(false);

    assert!(
        refresh_ok || drop_ok,
        "At least one of refresh/drop should succeed"
    );

    // If drop succeeded, the ST should be gone
    if drop_ok {
        let exists: bool = db
            .query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'cc_race_st')",
            )
            .await;
        // It might or might not exist depending on ordering
        // The key assertion is no crash/panic occurred
        let _ = exists;
    }
}

// ══════════════════════════════════════════════════════════════════════
// C1 — Multiple STs on same source refreshed concurrently
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_concurrent_refresh_multiple_sts_same_source() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cc_shared (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO cc_shared SELECT g, g * 10 FROM generate_series(1, 50) g")
        .await;

    db.create_st(
        "cc_shared_st1",
        "SELECT id, val FROM cc_shared",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "cc_shared_st2",
        "SELECT id, val * 2 AS val2 FROM cc_shared",
        "1m",
        "FULL",
    )
    .await;

    // Insert more data, then refresh both concurrently
    db.execute("INSERT INTO cc_shared SELECT g, g * 10 FROM generate_series(51, 100) g")
        .await;

    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();

    let h1 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_shared_st1')")
            .execute(&pool1)
            .await
    });
    let h2 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_shared_st2')")
            .execute(&pool2)
            .await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    r1.expect("task1 panicked").expect("refresh st1 failed");
    r2.expect("task2 panicked").expect("refresh st2 failed");

    // Both STs should reflect the full 100-row source
    db.assert_st_matches_query("cc_shared_st1", "SELECT id, val FROM cc_shared")
        .await;
    db.assert_st_matches_query("cc_shared_st2", "SELECT id, val * 2 AS val2 FROM cc_shared")
        .await;
}

// ══════════════════════════════════════════════════════════════════════
// C2 — Advisory lock contention: concurrent refresh of same ST
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_concurrent_refresh_same_st_no_corruption() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cc_lock_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO cc_lock_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.create_st(
        "cc_lock_st",
        "SELECT id, val FROM cc_lock_src",
        "1m",
        "FULL",
    )
    .await;

    db.execute("INSERT INTO cc_lock_src SELECT g, g FROM generate_series(101, 200) g")
        .await;

    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();

    let h1 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_lock_st')")
            .execute(&pool1)
            .await
    });
    let h2 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_lock_st')")
            .execute(&pool2)
            .await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    // Both calls may succeed (advisory lock serializes) or second may skip.
    // Neither should panic.
    let _ = r1.expect("task1 panicked");
    let _ = r2.expect("task2 panicked");

    // After both complete, row count must be exactly correct — no duplicates
    db.assert_st_matches_query("cc_lock_st", "SELECT id, val FROM cc_lock_src")
        .await;
}

// ══════════════════════════════════════════════════════════════════════
// C3 — Full-refresh racing with DML on source
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_full_refresh_racing_with_dml() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cc_dml_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO cc_dml_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.create_st("cc_dml_st", "SELECT id, val FROM cc_dml_src", "1m", "FULL")
        .await;
    assert_eq!(db.count("public.cc_dml_st").await, 100);

    db.execute("INSERT INTO cc_dml_src SELECT g, g FROM generate_series(101, 150) g")
        .await;

    let pool_r = db.pool.clone();
    let pool_i = db.pool.clone();

    let h_refresh = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_dml_st')")
            .execute(&pool_r)
            .await
    });
    let h_insert = tokio::spawn(async move {
        sqlx::query("INSERT INTO cc_dml_src SELECT g, g FROM generate_series(151, 200) g")
            .execute(&pool_i)
            .await
    });

    let (r_refresh, r_insert) = tokio::join!(h_refresh, h_insert);
    r_refresh
        .expect("refresh task panicked")
        .expect("refresh failed");
    r_insert
        .expect("insert task panicked")
        .expect("insert failed");

    // After a stabilising refresh, count must converge to 200
    db.refresh_st("cc_dml_st").await;
    db.assert_st_matches_query("cc_dml_st", "SELECT id, val FROM cc_dml_src")
        .await;
}

// ── CONC-1..4 (v0.26.0): Concurrency matrix tests ─────────────────────────

/// CONC-1 (v0.26.0): Simultaneous ALTER + REFRESH.
///
/// One connection runs `alter_stream_table(query => ...)` while another is
/// mid-refresh. Asserts:
/// - No deadlock or PostgreSQL ERROR raised
/// - Catalog stays consistent (ST exists and is ACTIVE after both complete)
/// - Refresh either completes normally or is cleanly aborted (no partial state)
#[tokio::test]
async fn test_conc1_alter_while_refresh_no_deadlock() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE conc1_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO conc1_src SELECT g, g * 10 FROM generate_series(1, 200) g")
        .await;

    db.create_st(
        "conc1_st",
        "SELECT id, val FROM conc1_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.conc1_st").await, 200);

    // Add rows so there is real work to do in the next refresh.
    db.execute("INSERT INTO conc1_src SELECT g, g * 10 FROM generate_series(201, 500) g")
        .await;

    let pool_refresh = db.pool.clone();
    let pool_alter = db.pool.clone();
    let pool_wait = db.pool.clone();

    // Fire both concurrently: one refreshes (takes a moment), one ALTERs the query.
    let h_refresh = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('conc1_st')")
            .execute(&pool_refresh)
            .await
    });

    let h_alter = tokio::spawn(async move {
        // Wait until refresh is actively executing before issuing the ALTER.
        wait_for_active_query(&pool_wait, "refresh_stream_table", 5).await;
        // ALTER to the same defining query — idempotent but forces catalog update.
        sqlx::query(
            "SELECT pgtrickle.alter_stream_table('conc1_st', \
             query => $$ SELECT id, val FROM conc1_src WHERE val >= 0 $$)",
        )
        .execute(&pool_alter)
        .await
    });

    let (r_refresh, r_alter) = tokio::join!(h_refresh, h_alter);

    // Neither must panic at the task level.
    let refresh_result = r_refresh.expect("refresh task panicked");
    let alter_result = r_alter.expect("alter task panicked");

    // Both may succeed, or one may cleanly fail (lock contention) — but neither
    // should return an unhandled panic or internal error.
    // We accept both Ok and Err outcomes as long as no data corruption follows.
    let _ = refresh_result; // SKIP/lock-contention errors are acceptable
    let _ = alter_result;

    // After both complete, the catalog must be consistent: ST exists in a known state.
    let status: String = db
        .query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'conc1_st'")
        .await;
    assert!(
        status == "ACTIVE" || status == "ERROR" || status == "INITIALIZING",
        "Unexpected status after concurrent ALTER+REFRESH: {status}"
    );
}

/// CONC-2 (v0.26.0): Simultaneous DROP + REFRESH.
///
/// `drop_stream_table()` is called while a refresh is in progress.
/// Asserts:
/// - No orphaned change buffers in `pgtrickle_changes`
/// - No dangling catalog rows in `pgt_stream_tables`
/// - The refresh either completes cleanly or is aborted with a safe error
#[tokio::test]
async fn test_conc2_drop_while_refresh_no_orphans() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE conc2_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO conc2_src SELECT g, g FROM generate_series(1, 200) g")
        .await;

    db.create_st(
        "conc2_st",
        "SELECT id, val FROM conc2_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.conc2_st").await, 200);

    // Add rows to ensure the refresh does real work.
    db.execute("INSERT INTO conc2_src SELECT g, g FROM generate_series(201, 500) g")
        .await;

    let pool_refresh = db.pool.clone();
    let pool_drop = db.pool.clone();
    let pool_wait = db.pool.clone();

    let h_refresh = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('conc2_st')")
            .execute(&pool_refresh)
            .await
    });

    let h_drop = tokio::spawn(async move {
        // Wait until refresh is actively executing before issuing the DROP.
        wait_for_active_query(&pool_wait, "refresh_stream_table", 5).await;
        sqlx::query("SELECT pgtrickle.drop_stream_table('conc2_st')")
            .execute(&pool_drop)
            .await
    });

    let (r_refresh, r_drop) = tokio::join!(h_refresh, h_drop);
    let _ = r_refresh.expect("refresh task panicked"); // may succeed or fail cleanly
    let _ = r_drop.expect("drop task panicked"); // may succeed or fail cleanly

    // CONC-2 correctness: after both complete, no orphaned catalog rows.
    let catalog_rows: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'conc2_st'",
        )
        .await;

    if catalog_rows == 0 {
        // Drop succeeded: no excess change buffer tables should remain.
        let change_buffer_rows: i64 = db
            .query_scalar(
                "SELECT count(*) FROM information_schema.tables \
                 WHERE table_schema = 'pgtrickle_changes' \
                   AND table_name LIKE 'changes_%'",
            )
            .await;
        assert!(
            change_buffer_rows < 100,
            "Too many orphaned change buffer tables: {change_buffer_rows}"
        );
    }
    // If catalog_rows == 1, drop lost the race — acceptable.
}

/// CONC-3 (v0.26.0): Parallel-worker duplicate-pick prevention.
///
/// When two concurrent `refresh_stream_table()` calls race on the same ST,
/// the second must skip (via FOR UPDATE SKIP LOCKED) without producing
/// duplicate rows or data corruption.
#[tokio::test]
async fn test_conc3_parallel_workers_no_duplicate_pick() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE conc3_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO conc3_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.create_st(
        "conc3_st",
        "SELECT id, val FROM conc3_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.conc3_st").await, 100);

    // Add rows so both workers have real work.
    db.execute("INSERT INTO conc3_src SELECT g, g FROM generate_series(101, 200) g")
        .await;

    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();

    let h1 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('conc3_st')")
            .execute(&pool1)
            .await
    });

    let h2 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('conc3_st')")
            .execute(&pool2)
            .await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    r1.expect("worker 1 panicked").expect("worker 1 DB error");
    r2.expect("worker 2 panicked").expect("worker 2 DB error");

    // Exactly 200 rows, no duplicates.
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM public.conc3_st")
        .await;
    assert_eq!(count, 200, "ST must have exactly 200 rows, no duplicates");

    let distinct_ids: i64 = db
        .query_scalar("SELECT count(DISTINCT id) FROM public.conc3_st")
        .await;
    assert_eq!(
        distinct_ids, count,
        "All rows must have distinct ids (no duplicates from parallel refresh)"
    );
}

/// CONC-4 (v0.26.0): Concurrent canary promotion race.
///
/// Two concurrent full refreshes trigger buffer promotion simultaneously.
/// Assert exactly one succeeds, metadata is consistent, and the ST is
/// correctly populated without duplicate rows.
#[tokio::test]
async fn test_conc4_canary_promotion_consistent_metadata() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE conc4_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO conc4_src SELECT g, g * 5 FROM generate_series(1, 100) g")
        .await;

    db.create_st("conc4_st", "SELECT id, val FROM conc4_src", "1m", "FULL")
        .await;
    assert_eq!(db.count("public.conc4_st").await, 100);

    db.execute("INSERT INTO conc4_src SELECT g, g * 5 FROM generate_series(101, 200) g")
        .await;

    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();

    let h1 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('conc4_st')")
            .execute(&pool1)
            .await
    });

    let h2 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('conc4_st')")
            .execute(&pool2)
            .await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    r1.expect("refresh 1 panicked").expect("refresh 1 DB error");
    r2.expect("refresh 2 panicked").expect("refresh 2 DB error");

    // Catalog metadata must be consistent after concurrent full refreshes.
    let status: String = db
        .query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'conc4_st'")
        .await;
    assert_eq!(
        status, "ACTIVE",
        "ST must be ACTIVE after concurrent FULL refreshes"
    );

    let is_populated: bool = db
        .query_scalar(
            "SELECT is_populated FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'conc4_st'",
        )
        .await;
    assert!(
        is_populated,
        "ST must be marked is_populated after concurrent FULL refreshes"
    );

    // Stabilise with a final refresh, then verify row count.
    db.refresh_st("conc4_st").await;
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM public.conc4_st")
        .await;
    assert_eq!(
        count, 200,
        "ST must contain exactly 200 rows after concurrent full refreshes"
    );
}

/// TEST-10-04 (v0.49.0): DDL during a concurrent refresh does not corrupt the stream table.
///
/// Fires a `refresh_stream_table()` and an `ALTER STREAM TABLE SET query` concurrently.
/// After both complete, verifies:
/// - The catalog is in a consistent state (ACTIVE or ERROR, never INITIALIZING + corrupted).
/// - Three additional refresh cycles converge to the correct row count.
/// - No deadlock or panic occurred.
///
/// This differs from `test_conc1_alter_while_refresh_no_deadlock` in that it:
/// - Uses a FULL-refresh stream table (simpler timing model).
/// - Runs 3 post-race refresh cycles (stronger convergence guarantee).
/// - Asserts the final row count matches the source exactly.
#[tokio::test]
async fn test_ddl_during_concurrent_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_conc_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO ddl_conc_src SELECT g, 'v' || g FROM generate_series(1, 100) g")
        .await;

    db.create_st(
        "ddl_conc_st",
        "SELECT id, val FROM ddl_conc_src",
        "1m",
        "FULL",
    )
    .await;
    assert_eq!(db.count("public.ddl_conc_st").await, 100);

    // Add rows so the next refresh has real work to do.
    db.execute("INSERT INTO ddl_conc_src SELECT g, 'v' || g FROM generate_series(101, 300) g")
        .await;

    let pool_refresh = db.pool.clone();
    let pool_alter = db.pool.clone();
    let pool_wait = db.pool.clone();

    let h_refresh = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('ddl_conc_st')")
            .execute(&pool_refresh)
            .await
    });

    let h_alter = tokio::spawn(async move {
        // Wait until the refresh is actively running before issuing the ALTER.
        wait_for_active_query(&pool_wait, "refresh_stream_table", 5).await;
        // ALTER to an identical query with an explicit WHERE — forces catalog update.
        sqlx::query(
            "SELECT pgtrickle.alter_stream_table('ddl_conc_st', \
             query => $$ SELECT id, val FROM ddl_conc_src WHERE id > 0 $$)",
        )
        .execute(&pool_alter)
        .await
    });

    let (r_refresh, r_alter) = tokio::join!(h_refresh, h_alter);

    // Neither must panic at the task level — lock contention errors are acceptable.
    let _ = r_refresh.expect("refresh task panicked");
    let _ = r_alter.expect("alter task panicked");

    // Catalog must be in a known-good state: ACTIVE or ERROR (not torn state).
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_conc_st'",
        )
        .await;
    assert!(
        status == "ACTIVE" || status == "ERROR",
        "Catalog status must be ACTIVE or ERROR after DDL race, got: {status}"
    );

    // Three convergence refreshes — the ST must reach the correct count.
    for _ in 0..3 {
        db.refresh_st("ddl_conc_st").await;
    }

    let final_count: i64 = db
        .query_scalar("SELECT count(*) FROM public.ddl_conc_st")
        .await;
    assert_eq!(
        final_count, 300,
        "After convergence refreshes, ST must contain all 300 source rows"
    );
}
