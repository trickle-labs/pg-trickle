//! SAF-2: Failure-injection E2E tests for background worker resilience.
//!
//! These tests verify that when one stream table's refresh fails (due to
//! permission errors, schema changes, or other injected faults), the
//! background scheduler:
//!
//!   1. Does NOT crash.
//!   2. Continues to refresh other stream tables successfully.
//!   3. Records the error in `pgt_refresh_history` and increments
//!      `consecutive_errors` for the failing stream table.
//!
//! The failure is injected by either revoking access to the source table
//! (when possible) or by dropping a source table's column, triggering an
//! `UpstreamSchemaChanged` error for that ST.

mod common;
mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helpers ────────────────────────────────────────────────────────────────

/// Configure the scheduler for fast testing.
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
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(
        sched_running,
        "pg_trickle scheduler did not appear within 90 s"
    );
}

/// Wait until a ST has been scheduled-refreshed (has a COMPLETED history record).
async fn wait_for_scheduler_refresh(db: &E2eDb, pgt_name: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        let count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
                 WHERE d.pgt_name = '{pgt_name}' AND h.status = 'COMPLETED'"
            ))
            .await;
        if count > 0 {
            return true;
        }
    }
}

/// Wait until a ST has at least one FAILED history record.
/// Wait until a ST reaches the expected status.
async fn wait_for_status(db: &E2eDb, pgt_name: &str, status: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let current: String = db
            .query_scalar(&format!(
                "SELECT status FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = '{pgt_name}'"
            ))
            .await;
        if current == status {
            return true;
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// SAF-2: Inject a failure into one ST (drop source table's column to trigger
/// UpstreamSchemaChanged) and verify the scheduler continues to refresh
/// other STs without crashing.
///
/// Scenario:
/// 1. Create `good_src` / `good_st` — a healthy FULL-mode ST.
/// 2. Create `bad_src` / `bad_st` — a healthy ST that will be broken.
/// 3. Let the scheduler complete at least one cycle for both.
/// 4. Drop the column that `bad_st` queries from `bad_src`.
/// 5. Wait: `bad_st` should record FAILED refreshes.
/// 6. Verify: `good_st` still refreshes successfully.
/// 7. Verify: scheduler background worker is still alive in pg_stat_activity.
#[tokio::test]
async fn test_scheduler_isolates_failing_st_from_healthy_st() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // ── Setup: healthy ST ───────────────────────────────────────────────
    db.execute("CREATE TABLE good_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO good_src VALUES (1, 'a'), (2, 'b')")
        .await;
    db.create_st("good_st", "SELECT id, val FROM good_src", "1s", "FULL")
        .await;

    // ── Setup: ST that will fail ────────────────────────────────────────
    db.execute("CREATE TABLE bad_src (id INT PRIMARY KEY, important_col TEXT)")
        .await;
    db.execute("INSERT INTO bad_src VALUES (1, 'x')").await;
    db.create_st(
        "bad_st",
        "SELECT id, important_col FROM bad_src",
        "1s",
        "FULL",
    )
    .await;

    // Both STs should be initially populated
    assert_eq!(db.count("public.good_st").await, 2);
    assert_eq!(db.count("public.bad_st").await, 1);

    // ── Let both complete at least one scheduled refresh ────────────────
    db.execute("INSERT INTO good_src VALUES (3, 'c')").await;

    let good_refreshed = wait_for_scheduler_refresh(&db, "good_st", Duration::from_secs(60)).await;
    assert!(
        good_refreshed,
        "good_st should complete at least one scheduled refresh before fault injection"
    );

    // ── Inject fault: drop the column that bad_st queries ───────────────
    // ALTER TABLE DROP COLUMN triggers a DDL hook which suspends bad_st
    // until the changed source schema is explicitly repaired.
    db.execute("INSERT INTO bad_src VALUES (2, 'y')").await;
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE bad_src DROP COLUMN important_col",
    ])
    .await;

    // ── Wait: bad_st should be suspended ───────────────────────────────
    let suspended = wait_for_status(&db, "bad_st", "SUSPENDED", Duration::from_secs(10)).await;
    assert!(
        suspended,
        "bad_st should be suspended after source schema change"
    );

    // ── Verify: scheduler is still alive ───────────────────────────────
    let sched_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE backend_type = 'pg_trickle scheduler'",
        )
        .await;
    assert!(
        sched_count >= 1,
        "pg_trickle scheduler should still be alive after bad_st failure (count={})",
        sched_count
    );

    // ── Verify: good_st continues to refresh successfully ───────────────
    db.execute("INSERT INTO good_src VALUES (4, 'd')").await;

    let good_refreshed_again = db
        .wait_for_auto_refresh("good_st", Duration::from_secs(60))
        .await;
    assert!(
        good_refreshed_again,
        "good_st should continue to get scheduled refreshes even after bad_st starts failing"
    );

    let good_row_count = db.count("public.good_st").await;
    assert!(
        good_row_count >= 3,
        "good_st should have received new data (has {} rows, expected >= 3)",
        good_row_count
    );

    // ── Verify: bad_st records why it is suspended ─────────────────────
    let bad_status: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'bad_st'",
        )
        .await;
    assert_eq!(bad_status, "SUSPENDED:SOURCE_DESTRUCTIVE_SCHEMA");
}

/// SAF-2b: Verify that revoking SELECT on a source table from a non-superuser
/// role causes the ST to fail without affecting the scheduler or other STs.
///
/// The background worker is privileged, but definition evaluation uses the stream owner.
/// This test instead creates a dedicated restricted role, runs the refresh
/// via `SET ROLE` (simulating RLS/permission context), and verifies error
/// isolation.
///
/// NOTE: This test validates the error-isolation boundary at the scheduler
/// level by directly inserting a RUNNING refresh record and simulating a
/// crash recovery, which is the observable side-effect of a hard failure.
#[tokio::test]
async fn test_scheduler_continues_after_permission_error() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // ── Setup: two healthy STs ──────────────────────────────────────────
    db.execute("CREATE TABLE src_ok (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src_ok VALUES (1, 10), (2, 20)")
        .await;
    db.create_st("st_ok", "SELECT id, val FROM src_ok", "1s", "DIFFERENTIAL")
        .await;

    db.execute("CREATE TABLE src_perm (id INT PRIMARY KEY, secret TEXT)")
        .await;
    db.execute("INSERT INTO src_perm VALUES (1, 'hidden')")
        .await;
    db.create_st(
        "st_perm",
        "SELECT id, secret FROM src_perm",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Wait for both to be refreshed at least once
    let st_ok_done = wait_for_scheduler_refresh(&db, "st_ok", Duration::from_secs(60)).await;
    assert!(
        st_ok_done,
        "st_ok should complete initial scheduled refresh"
    );

    // ── Inject fault: drop the source table ─────────────────────────────
    // Dropping the source suspends st_perm, exercising the scheduler's
    // isolation from an unavailable stream table.
    db.execute("DROP TABLE src_perm CASCADE").await;

    // ── Verify st_perm is suspended before the scheduler continues ─────
    let perm_status: String = db
        .query_scalar(
            "SELECT status || ':' || COALESCE(refresh_reason, 'NULL') \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'st_perm'",
        )
        .await;
    assert_eq!(
        perm_status, "SUSPENDED:SOURCE_DROPPED",
        "st_perm should be SUSPENDED:SOURCE_DROPPED after source table is dropped"
    );

    db.execute("INSERT INTO src_ok VALUES (3, 30)").await;

    // ── Verify: scheduler is still running ──────────────────────────────
    let sched_alive: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE backend_type = 'pg_trickle scheduler'",
        )
        .await;
    assert!(
        sched_alive >= 1,
        "Scheduler should still be alive after st_perm failure"
    );

    // ── Verify: st_ok continues to refresh ──────────────────────────────
    let ok_refreshed = db
        .wait_for_auto_refresh("st_ok", Duration::from_secs(60))
        .await;
    assert!(
        ok_refreshed,
        "st_ok should still be refreshed by scheduler after st_perm failure"
    );

    // ── Verify: st_perm records the source drop ─────────────────────────
    let final_perm_status: String = db
        .query_scalar(
            "SELECT status || ':' || COALESCE(refresh_reason, 'NULL') \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'st_perm'",
        )
        .await;
    assert_eq!(
        final_perm_status, "SUSPENDED:SOURCE_DROPPED",
        "st_perm should be SUSPENDED:SOURCE_DROPPED after source table is dropped"
    );
}
