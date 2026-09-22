//! v0.62.0: Full E2E tests for scheduler PERF-1 fanout cache and
//! API-1/2 per-node pause/resume control.
//!
//! These tests require the background scheduler (`shared_preload_libraries`)
//! and can only run in the full E2E harness (just test-e2e).

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helpers ────────────────────────────────────────────────────────────────

async fn configure_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(
        sched_running,
        "pg_trickle scheduler did not appear within 90 s"
    );
}

/// Wait until `pgt_name` has at least `min_count` COMPLETED refresh records.
async fn wait_for_n_refreshes(
    db: &E2eDb,
    pgt_name: &str,
    min_count: i64,
    timeout: Duration,
) -> i64 {
    let start = std::time::Instant::now();
    loop {
        let count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
                 WHERE d.pgt_name = '{pgt_name}' AND h.status = 'COMPLETED'"
            ))
            .await;
        if count >= min_count {
            return count;
        }
        if start.elapsed() > timeout {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Snapshot-polled sources must be polled before the fanout cache freezes
/// change visibility, even when their buffers were initially empty.
#[tokio::test]
async fn test_scheduler_fanout_shared_matview_changes_refresh_both_consumers() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.enable_change_buffer_fanout", "on", "on")
        .await;
    db.execute("CREATE TABLE fanout_mv_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO fanout_mv_src VALUES (1, 10)").await;
    db.execute("CREATE MATERIALIZED VIEW fanout_mv AS SELECT * FROM fanout_mv_src")
        .await;
    for name in ["fanout_mv_a", "fanout_mv_b"] {
        db.create_st(name, "SELECT id, val FROM fanout_mv", "1s", "FULL")
            .await;
    }

    // A failed remote poll must not starve the independent MATVIEW group.
    db.execute("CREATE EXTENSION postgres_fdw").await;
    let db_name: String = db.query_scalar("SELECT current_database()").await;
    db.execute(&format!(
        "CREATE SERVER fanout_remote FOREIGN DATA WRAPPER postgres_fdw \
         OPTIONS (dbname '{db_name}', host '127.0.0.1', port '5432')"
    ))
    .await;
    db.execute(
        "CREATE USER MAPPING FOR CURRENT_USER SERVER fanout_remote OPTIONS (user 'postgres')",
    )
    .await;
    db.execute("CREATE TABLE fanout_remote_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO fanout_remote_src VALUES (1, 10)")
        .await;
    db.execute("CREATE FOREIGN TABLE fanout_ft (id INT, val INT) SERVER fanout_remote OPTIONS (table_name 'fanout_remote_src')")
        .await;
    db.create_st(
        "fanout_ft_st",
        "SELECT id, val FROM fanout_ft",
        "1s",
        "FULL",
    )
    .await;
    db.execute("ALTER SERVER fanout_remote OPTIONS (SET port '1')")
        .await;
    db.execute("UPDATE fanout_remote_src SET val = 30").await;

    // Updating and then deleting the only row verifies both nonempty and
    // empty snapshots; no manual ST refresh may supply the missing polling.
    for mutation in [
        "UPDATE fanout_mv_src SET val = 20",
        "DELETE FROM fanout_mv_src",
    ] {
        db.execute(mutation).await;
        db.execute("REFRESH MATERIALIZED VIEW fanout_mv").await;
        assert!(
            db.wait_for_condition(
                "both fanout consumers match the materialized view",
                "SELECT NOT EXISTS (
                    (SELECT id, val FROM fanout_mv_a EXCEPT ALL SELECT id, val FROM fanout_mv)
                    UNION ALL
                    (SELECT id, val FROM fanout_mv EXCEPT ALL SELECT id, val FROM fanout_mv_a)
                    UNION ALL
                    (SELECT id, val FROM fanout_mv_b EXCEPT ALL SELECT id, val FROM fanout_mv)
                    UNION ALL
                    (SELECT id, val FROM fanout_mv EXCEPT ALL SELECT id, val FROM fanout_mv_b)
                )",
                Duration::from_secs(30),
                Duration::from_millis(100),
            )
            .await,
            "fanout polling must refresh both consumers after {mutation}"
        );
    }
    // The failed source remains retryable and catches up after recovery.
    db.execute("ALTER SERVER fanout_remote OPTIONS (SET port '5432')")
        .await;
    assert!(
        db.wait_for_condition(
            "foreign source recovers after polling failure",
            "SELECT EXISTS (SELECT 1 FROM fanout_ft_st WHERE val = 30)",
            Duration::from_secs(30),
            Duration::from_millis(100),
        )
        .await
    );
}

/// PERF-1: The change-buffer fanout cache must not suppress legitimate refresh
/// triggers.
///
/// Two stream tables share the same source table.  After a DML change, both
/// STs must be refreshed within a reasonable time.  This verifies that the
/// per-tick batched EXISTS query correctly identifies both STs as having
/// pending changes (deduplication of the _source scan_, not the refresh).
#[tokio::test]
async fn test_scheduler_fanout_deduplicates_buffer_scan() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Enable fanout explicitly (it should be on by default, but let's be sure).
    db.execute("ALTER SYSTEM SET pg_trickle.enable_change_buffer_fanout = on")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.enable_change_buffer_fanout", "on")
        .await;

    // Create one source table and two stream tables that read from it.
    db.execute("CREATE TABLE fanout_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO fanout_src VALUES (1, 100)").await;

    db.create_st(
        "fanout_st_a",
        "SELECT id, val FROM fanout_src",
        "1s",
        "FULL",
    )
    .await;
    db.create_st(
        "fanout_st_b",
        "SELECT id, val * 2 AS val2 FROM fanout_src",
        "1s",
        "FULL",
    )
    .await;

    // Wait for the initial refresh of both STs.
    let cnt_a = wait_for_n_refreshes(&db, "fanout_st_a", 1, Duration::from_secs(30)).await;
    let cnt_b = wait_for_n_refreshes(&db, "fanout_st_b", 1, Duration::from_secs(30)).await;
    assert!(
        cnt_a >= 1,
        "fanout_st_a must refresh at least once initially"
    );
    assert!(
        cnt_b >= 1,
        "fanout_st_b must refresh at least once initially"
    );

    // Verify initial content.
    assert_eq!(db.count("public.fanout_st_a").await, 1);
    assert_eq!(db.count("public.fanout_st_b").await, 1);

    // Insert a second row to create a CDC event.
    db.execute("INSERT INTO fanout_src VALUES (2, 200)").await;

    // Both STs must pick up the new row within a few seconds.
    let got_a = wait_for_n_refreshes(&db, "fanout_st_a", cnt_a + 1, Duration::from_secs(30)).await;
    let got_b = wait_for_n_refreshes(&db, "fanout_st_b", cnt_b + 1, Duration::from_secs(30)).await;

    assert!(
        got_a > cnt_a,
        "fanout_st_a must refresh after source insert (got {got_a}, expected ≥ {})",
        cnt_a + 1
    );
    assert!(
        got_b > cnt_b,
        "fanout_st_b must refresh after source insert (got {got_b}, expected ≥ {})",
        cnt_b + 1
    );

    assert_eq!(
        db.count("public.fanout_st_a").await,
        2,
        "fanout_st_a must contain 2 rows after refresh"
    );
    assert_eq!(
        db.count("public.fanout_st_b").await,
        2,
        "fanout_st_b must contain 2 rows after refresh"
    );
}

#[tokio::test]
async fn test_scheduler_records_and_cleans_up_effective_full_fallback() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.refresh_strategy", "full", "full")
        .await;

    db.execute("CREATE TABLE effective_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO effective_src VALUES (1, 'initial')")
        .await;
    db.create_st(
        "effective_st",
        "SELECT id, val FROM effective_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = i64::from(db.table_oid("effective_src").await);
    let buffer = db.change_buffer_table(source_oid).await;
    db.execute("INSERT INTO effective_src VALUES (2, 'fallback')")
        .await;
    let pending_changes = format!("SELECT count(*) FROM {buffer} WHERE action IN ('I', 'D')");
    assert!(
        db.query_scalar::<i64>(&pending_changes).await > 0,
        "CDC buffer should contain the insert"
    );

    let completed = db
        .wait_for_condition(
            "scheduler effective FULL fallback",
            "SELECT EXISTS(SELECT 1
               FROM pgtrickle.pgt_refresh_history h
               JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
              WHERE st.pgt_name = 'effective_st'
                AND h.status = 'COMPLETED'
                AND h.refresh_reason = 'CONFIGURED_FULL')",
            Duration::from_secs(60),
            Duration::from_millis(300),
        )
        .await;
    assert!(
        completed,
        "scheduler should complete the configured FULL fallback"
    );

    let (action, strategy, was_fallback): (String, String, bool) = sqlx::query_as(
        "SELECT h.action, h.merge_strategy_used, h.was_full_fallback
           FROM pgtrickle.pgt_refresh_history h
           JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
          WHERE st.pgt_name = 'effective_st'
            AND h.refresh_reason = 'CONFIGURED_FULL'
          ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read scheduler fallback history");
    assert_eq!(action, "FULL");
    assert_eq!(strategy, "FULL");
    assert!(was_fallback);

    assert_eq!(
        db.query_scalar::<i64>(&pending_changes).await,
        0,
        "effective FULL finalization should remove consumed CDC rows"
    );
    let (full_count, diff_count): (i64, i64) = sqlx::query_as(
        "SELECT total_full_refreshes, total_diff_refreshes
           FROM pgtrickle.pgt_refresh_summary
          WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                           WHERE pgt_name = 'effective_st')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read refresh summary");
    assert!(full_count >= 1);
    assert_eq!(diff_count, 0);

    db.alter_system_reset_and_wait("pg_trickle.refresh_strategy", "auto")
        .await;
}

/// API-1/2: `pause_scheduler` stops the scheduler from dispatching a node;
/// `resume_scheduler` re-enables it.
#[tokio::test]
async fn test_pause_resume_scheduler_basic() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE pause_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO pause_src VALUES (1, 10)").await;
    db.create_st("pause_st", "SELECT id, val FROM pause_src", "1s", "FULL")
        .await;

    // Wait for the initial refresh.
    let baseline = wait_for_n_refreshes(&db, "pause_st", 1, Duration::from_secs(30)).await;
    assert!(
        baseline >= 1,
        "pause_st must refresh at least once initially"
    );
    assert_eq!(db.count("public.pause_st").await, 1);

    // Pause the node.
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.pause_st'])")
        .await;

    // Insert new data — the scheduler should NOT dispatch the node.
    db.execute("INSERT INTO pause_src VALUES (2, 20)").await;

    // Wait two scheduler intervals to give the scheduler time to skip the node.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let after_pause: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'pause_st' AND h.status = 'COMPLETED'",
        )
        .await;

    assert_eq!(
        after_pause, baseline,
        "pause_st should not refresh while paused (baseline={baseline}, after={after_pause})"
    );
    // Data should still be stale.
    assert_eq!(
        db.count("public.pause_st").await,
        1,
        "pause_st must still have 1 row while paused"
    );

    // Resume and wait for the scheduler to pick it up.
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['public.pause_st'])")
        .await;

    let after_resume =
        wait_for_n_refreshes(&db, "pause_st", baseline + 1, Duration::from_secs(30)).await;
    assert!(
        after_resume > baseline,
        "pause_st must refresh after resume (got {after_resume}, expected ≥ {})",
        baseline + 1
    );
    assert_eq!(
        db.count("public.pause_st").await,
        2,
        "pause_st must contain 2 rows after resume refresh"
    );
}

/// API-1: `pause_scheduler` returns within `scheduler_drain_timeout` seconds
/// even when there are active refresh workers.
///
/// This test verifies the drain-timeout warning path by reducing the timeout
/// to 1 second and pausing a fast-refreshing node.
#[tokio::test]
async fn test_pause_scheduler_drain_timeout() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Set a short drain timeout to exercise the timeout code path.
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_drain_timeout = 1")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_drain_timeout", "1")
        .await;

    db.execute("CREATE TABLE drain_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO drain_src VALUES (1, 1)").await;
    db.create_st("drain_st", "SELECT id, val FROM drain_src", "1s", "FULL")
        .await;

    // Wait for the first refresh.
    let _ = wait_for_n_refreshes(&db, "drain_st", 1, Duration::from_secs(30)).await;

    // The call must complete (not hang) regardless of worker state.
    // We verify the call succeeds (doesn't error) and returns within a
    // generous wall-clock budget.
    let start = std::time::Instant::now();
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.drain_st'])")
        .await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(10),
        "pause_scheduler must complete within 10 s with a 1 s drain_timeout (took {elapsed:?})"
    );

    // Restore drain timeout to default.
    db.execute("ALTER SYSTEM RESET pg_trickle.scheduler_drain_timeout")
        .await;
    db.reload_config_and_wait().await;

    // Resume the node so it doesn't stay paused and affect other tests.
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['public.drain_st'])")
        .await;
}
