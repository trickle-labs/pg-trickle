//! D-3 (v0.79.0): Cleanup chaos tests.
//!
//! Tests the behaviour of the auto-suspend circuit when the change-buffer
//! cleanup (the `DELETE FROM pgtrickle_changes.changes_<oid>` step inside
//! every differential refresh) fails repeatedly.
//!
//! Test scenario:
//!   1. Create source table + DIFFERENTIAL stream table.
//!   2. Insert rows → changes land in the CDC buffer.
//!   3. Install a BEFORE DELETE trigger on the buffer table that raises an
//!      error (`chaos trigger`).
//!   4. Let the scheduler attempt refreshes → each attempt fails at the
//!      cleanup step, incrementing `consecutive_errors`.
//!   5. After `max_consecutive_errors` failures the scheduler sets the stream
//!      table status to SUSPENDED and fires the `auto_suspended` alert.
//!   6. Remove the chaos trigger.
//!   7. Resume the stream table.
//!   8. Verify the stream table processes all pending changes correctly.

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helpers ────────────────────────────────────────────────────────────────

/// Set a fast scheduler and low `max_consecutive_errors` threshold so the
/// test completes quickly.
async fn setup_chaos_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    // Low threshold so the test does not need many refresh cycles.
    db.execute("ALTER SYSTEM SET pg_trickle.max_consecutive_errors = 3")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.max_consecutive_errors", "3")
        .await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(
        sched_running,
        "pg_trickle scheduler did not appear within 90 s"
    );
}

/// Poll until the stream table reaches `SUSPENDED` status.
/// Returns `true` if the status was reached within the timeout.
async fn wait_for_suspended(db: &E2eDb, pgt_name: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        let status: String = db
            .query_scalar(&format!(
                "SELECT status FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = '{pgt_name}'"
            ))
            .await;
        if status == "SUSPENDED" {
            return true;
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// D-3: Cleanup chaos — consecutive DELETE failures → SUSPENDED → recovery
// ══════════════════════════════════════════════════════════════════════

/// D-3 (v0.79.0): Force consecutive change-buffer cleanup failures via a chaos
/// trigger, assert the stream table auto-suspends, then remove the trigger,
/// resume, and verify the stream table recovers and produces correct output.
#[tokio::test]
async fn test_cleanup_consecutive_delete_failures_alerts_and_suspends() {
    let db = E2eDb::new().await.with_extension().await;
    setup_chaos_scheduler(&db).await;

    // ── 1. Create source table and DIFFERENTIAL stream table ────────
    db.execute("CREATE TABLE chaos_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO chaos_src VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .await;

    db.create_st(
        "chaos_st",
        "SELECT id, val FROM chaos_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Wait for the initial scheduler-driven refresh so the ST is populated.
    let populated: bool = {
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > Duration::from_secs(30) {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            let pop: bool = db
                .query_scalar(
                    "SELECT is_populated FROM pgtrickle.pgt_stream_tables \
                     WHERE pgt_name = 'chaos_st'",
                )
                .await;
            if pop {
                break true;
            }
        }
    };
    assert!(populated, "chaos_st should have been initially populated");

    let initial_count: i64 = db.count("public.chaos_st").await;
    assert_eq!(initial_count, 3, "initial population should have 3 rows");

    // ── 2. Insert additional rows → pending changes in CDC buffer ────
    db.execute("INSERT INTO chaos_src VALUES (4, 'delta'), (5, 'epsilon')")
        .await;

    // ── 3. Discover the change-buffer table ─────────────────────────
    // The buffer is named pgtrickle_changes.changes_<source_oid>.
    let source_oid: i32 = db
        .query_scalar("SELECT 'chaos_src'::regclass::oid::int")
        .await;

    let buffer_table = format!("pgtrickle_changes.changes_{source_oid}");

    // Verify the buffer has at least one pending change before installing chaos.
    let pending: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM {buffer_table}"))
        .await;
    assert!(
        pending > 0,
        "CDC buffer should have pending changes before installing chaos trigger"
    );

    // ── 4. Install the chaos trigger on the change-buffer table ─────
    //
    // A BEFORE DELETE trigger that always raises an error simulates a
    // transient storage/constraint failure during change-buffer cleanup.
    db.execute(
        "CREATE OR REPLACE FUNCTION pgtrickle_changes.chaos_delete_blocker() \
         RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN \
             RAISE EXCEPTION 'D-3 chaos: change-buffer DELETE deliberately blocked'; \
         END $$",
    )
    .await;

    db.execute(&format!(
        "CREATE TRIGGER chaos_block_delete \
         BEFORE DELETE ON {buffer_table} \
         FOR EACH ROW EXECUTE FUNCTION pgtrickle_changes.chaos_delete_blocker()",
    ))
    .await;

    // ── 5. Wait for auto-suspension ──────────────────────────────────
    //
    // The scheduler will attempt differential refreshes.  Each attempt will
    // fail (MERGE succeeds, then the cleanup DELETE fires the chaos trigger
    // and the transaction rolls back).  After max_consecutive_errors (= 3)
    // failures the scheduler suspends the ST.
    let suspended = wait_for_suspended(&db, "chaos_st", Duration::from_secs(60)).await;
    assert!(
        suspended,
        "chaos_st should have entered SUSPENDED after consecutive cleanup failures"
    );

    // Verify the consecutive_errors counter reached the threshold.
    let (_status, _mode, _populated, consecutive_errors) = db.pgt_status("chaos_st").await;
    assert!(
        consecutive_errors >= 3,
        "consecutive_errors should be >= 3 when auto-suspended, got {consecutive_errors}"
    );

    // Stream table data should still be at the last good count (3 rows)
    // because all refresh transactions were rolled back.
    let count_while_suspended: i64 = db.count("public.chaos_st").await;
    assert_eq!(
        count_while_suspended, 3,
        "chaos_st should still have 3 rows (rolled-back refreshes)"
    );

    // ── 6. Remove the chaos trigger ──────────────────────────────────
    db.execute(&format!(
        "DROP TRIGGER IF EXISTS chaos_block_delete ON {buffer_table}",
    ))
    .await;
    db.execute("DROP FUNCTION IF EXISTS pgtrickle_changes.chaos_delete_blocker()")
        .await;

    // ── 7. Resume the stream table ───────────────────────────────────
    db.execute("SELECT pgtrickle.resume_stream_table('chaos_st')")
        .await;

    let (status_after_resume, _, _, errors_after_resume) = db.pgt_status("chaos_st").await;
    assert_eq!(
        status_after_resume, "ACTIVE",
        "chaos_st should be ACTIVE after resume"
    );
    assert_eq!(
        errors_after_resume, 0,
        "consecutive_errors should be reset to 0 after resume"
    );

    // ── 8. Verify recovery — data should match after next refresh ───
    // Insert one more row to force a new change.
    db.execute("INSERT INTO chaos_src VALUES (6, 'zeta')").await;

    // Wait for the scheduler to successfully process the pending changes.
    let recovered = {
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > Duration::from_secs(60) {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            let n: i64 = db.count("public.chaos_st").await;
            if n == 6 {
                break true;
            }
        }
    };
    assert!(
        recovered,
        "chaos_st should have recovered and contain all 6 rows after cleanup chaos is resolved"
    );

    // Verify the final data matches the source.
    db.assert_st_matches_query("chaos_st", "SELECT id, val FROM chaos_src")
        .await;
}
