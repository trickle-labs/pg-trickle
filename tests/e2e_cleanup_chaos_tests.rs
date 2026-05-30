//! D-3 (v0.79.0): Cleanup chaos tests.
//!
//! Tests the behaviour of the auto-suspend circuit when differential refreshes
//! fail repeatedly.
//!
//! Test scenario:
//!   1. Create source table + DIFFERENTIAL stream table.
//!   2. Insert rows → changes land in the CDC buffer.
//!   3. Enable the `pg_trickle.test_chaos_for_table` GUC (set to 'chaos_st').
//!      The scheduler's `refresh_single_st` detects the GUC and directly
//!      increments `consecutive_errors` on every tick instead of running the
//!      actual refresh, simulating repeated failures without any trigger or
//!      PG exception handling.
//!   4. Pre-seed `consecutive_errors` to `max_consecutive_errors − 1` so that
//!      only a single increment is needed to reach the threshold.
//!   5. After `max_consecutive_errors` failures the scheduler sets the stream
//!      table status to SUSPENDED.
//!   6. Disable chaos by resetting the GUC.
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
    // Force sequential dispatch so the chaos GUC check in refresh_single_st
    // is guaranteed to run in the main scheduler BGW process (which processes
    // ConfigReloadPending and sees GUC changes after ALTER SYSTEM SET +
    // pg_reload_conf).  In parallel mode refreshes run in spawned worker
    // processes that may not see the GUC update in time.
    db.execute("ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'off'")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.max_consecutive_errors", "3")
        .await;
    db.wait_for_setting("pg_trickle.parallel_refresh_mode", "off")
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
// D-3: Refresh chaos — consecutive refresh failures → SUSPENDED → recovery
// ══════════════════════════════════════════════════════════════════════

/// D-3 (v0.79.0): Activate the test-mode chaos GUC to simulate repeated
/// differential refresh failures, assert the stream table auto-suspends, then
/// disable the GUC, resume, and verify the stream table recovers and produces
/// correct output.
///
/// The `pg_trickle.test_chaos_for_table` GUC causes `refresh_single_st` to
/// directly increment `consecutive_errors` for the named stream table on each
/// tick, bypassing the actual refresh.  This avoids depending on catching PG
/// exceptions from user trigger RAISE EXCEPTION calls (which is fragile across
/// PostgreSQL versions and executor call-stacks).
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

    // ── 2. Enable chaos via test-mode GUC ────────────────────────────
    //
    // Setting `pg_trickle.test_chaos_for_table = 'chaos_st'` causes
    // `refresh_single_st` to directly increment `consecutive_errors` on every
    // tick for chaos_st, simulating repeated refresh failures without running
    // the actual refresh.  This is more reliable than a trigger-based approach
    // because it does not depend on catching PG exceptions from user triggers.
    db.alter_system_set_and_wait("pg_trickle.test_chaos_for_table", "'chaos_st'", "chaos_st")
        .await;

    // ── 3. Insert additional rows → pending changes in CDC buffer ────
    //
    // With chaos active the scheduler will never run the actual refresh, so
    // these rows stay as pending changes.  After chaos is disabled and the
    // stream table is resumed, the scheduler processes all pending changes.
    db.execute("INSERT INTO chaos_src VALUES (4, 'delta'), (5, 'epsilon')")
        .await;

    // ── 3b. Pre-seed consecutive_errors to max_consecutive_errors − 1 ─
    //
    // Pre-seeding to one below the threshold means only a single chaos tick
    // is required to trigger suspension, making the test fast and deterministic.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET consecutive_errors = 2 \
         WHERE pgt_name = 'chaos_st'",
    )
    .await;

    // ── 4. Wait for auto-suspension ──────────────────────────────────
    //
    // The scheduler reads the test_chaos_for_table GUC and directly increments
    // consecutive_errors for chaos_st on each tick.  With consecutive_errors
    // pre-seeded to 2, a single tick increments it to 3 = max_consecutive_errors
    // → status is set to SUSPENDED.
    let suspended = wait_for_suspended(&db, "chaos_st", Duration::from_secs(60)).await;
    // Capture diagnostic info before the assertion so the panic message is informative.
    let (status_at_timeout, _mode, _populated, consecutive_errors_at_timeout) =
        db.pgt_status("chaos_st").await;
    assert!(
        suspended,
        "chaos_st should have entered SUSPENDED after chaos-injected refresh failures; \
         status={status_at_timeout}, consecutive_errors={consecutive_errors_at_timeout}"
    );

    // Verify the consecutive_errors counter reached the threshold.
    let (_status, _mode, _populated, consecutive_errors) = db.pgt_status("chaos_st").await;
    assert!(
        consecutive_errors >= 3,
        "consecutive_errors should be >= 3 when auto-suspended, got {consecutive_errors}"
    );

    // Stream table data should still be at the last good count (3 rows)
    // because the scheduler never ran the actual refresh while chaos was active.
    let count_while_suspended: i64 = db.count("public.chaos_st").await;
    assert_eq!(
        count_while_suspended, 3,
        "chaos_st should still have 3 rows (refresh skipped by chaos GUC)"
    );

    // ── 5. Disable chaos GUC ─────────────────────────────────────────
    // Reset chaos BEFORE resuming so the scheduler does not immediately
    // re-suspend the stream table after it becomes ACTIVE again.
    db.alter_system_set_and_wait("pg_trickle.test_chaos_for_table", "''", "")
        .await;

    // ── 6. Resume the stream table ───────────────────────────────────
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

    // ── 7. Verify recovery — data should match after next refresh ───
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
        "chaos_st should have recovered and contain all 6 rows after chaos is resolved"
    );

    // Verify the final data matches the source.
    db.assert_st_matches_query("chaos_st", "SELECT id, val FROM chaos_src")
        .await;
}
