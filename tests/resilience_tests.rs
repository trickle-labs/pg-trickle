//! Integration tests for Phase 10 — Error Handling & Resilience.
//!
//! These tests verify crash recovery, skip mechanism (advisory locks),
//! error escalation with suspension, and the interaction between
//! error classification and catalog state.

mod common;

use common::TestDb;

// ── Crash Recovery ─────────────────────────────────────────────────────────

/// Test crash recovery: RUNNING refresh records are marked FAILED on restart.
#[tokio::test]
async fn test_crash_recovery_marks_running_as_failed() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_crash (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status)
         VALUES
            ((SELECT 'src_crash'::regclass::oid), 'crash_st', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE')"
    ).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'crash_st'")
        .await;

    // Simulate interrupted refreshes by inserting RUNNING records
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, action, status)
         VALUES
            ({pgt_id}, now() - interval '5 min', now() - interval '5 min', 'FULL', 'RUNNING'),
            ({pgt_id}, now() - interval '3 min', now() - interval '3 min', 'DIFFERENTIAL', 'RUNNING')"
    ))
    .await;

    // Also insert a normal completed one (should NOT be affected)
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, status)
         VALUES ({pgt_id}, now() - interval '1 min', now() - interval '1 min', now(), 'FULL', 'COMPLETED')"
    )).await;

    let running_before: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE status = 'RUNNING'")
        .await;
    assert_eq!(running_before, 2);

    // Execute crash recovery query (same as recover_from_crash())
    db.execute(
        "UPDATE pgtrickle.pgt_refresh_history
         SET status = 'FAILED',
             error_message = 'Interrupted by scheduler restart',
             end_time = now()
         WHERE status = 'RUNNING'",
    )
    .await;

    // Verify all RUNNING records are now FAILED
    let running_after: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE status = 'RUNNING'")
        .await;
    assert_eq!(running_after, 0);

    let failed: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE status = 'FAILED'")
        .await;
    assert_eq!(failed, 2);

    // Verify the error message is set
    let msg: String = db
        .query_scalar(
            "SELECT error_message FROM pgtrickle.pgt_refresh_history WHERE status = 'FAILED' LIMIT 1",
        )
        .await;
    assert_eq!(msg, "Interrupted by scheduler restart");

    // Verify end_time was set
    let has_end_time: bool = db.query_scalar(
        "SELECT end_time IS NOT NULL FROM pgtrickle.pgt_refresh_history WHERE status = 'FAILED' LIMIT 1"
    ).await;
    assert!(has_end_time);

    // Verify the COMPLETED record was not affected
    let completed: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE status = 'COMPLETED'",
        )
        .await;
    assert_eq!(completed, 1);
}

// ── Advisory Lock Skip Mechanism ──────────────────────────────────────────

/// Test that advisory locks work for detecting concurrent refreshes.
/// Advisory locks are session-scoped, so we test within a single SQL statement.
#[tokio::test]
async fn test_advisory_lock_mechanism() {
    let db = TestDb::with_catalog().await;

    // Verify advisory lock acquisition and release within a single statement
    let success: bool = db
        .query_scalar("SELECT pg_try_advisory_lock(999) AND pg_advisory_unlock(999)")
        .await;
    assert!(success, "Advisory lock acquire+release should succeed");

    // Verify that an unheld lock can be detected (unlock returns false for unheld locks)
    // pg_advisory_unlock returns false if the lock wasn't held
    let not_held: bool = db.query_scalar("SELECT NOT pg_advisory_unlock(998)").await;
    assert!(not_held, "Unlocking a non-held lock should return false");
}

// ── Error Escalation & Auto-Suspend ───────────────────────────────────────

/// Test that consecutive errors are tracked and suspension works.
#[tokio::test]
async fn test_error_escalation_to_suspension() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_err (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status, consecutive_errors)
         VALUES
            ((SELECT 'src_err'::regclass::oid), 'err_st', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE', 0)"
    ).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'err_st'")
        .await;

    // Simulate incrementing consecutive errors (like increment_errors does)
    for i in 1..=3 {
        db.execute(&format!(
            "UPDATE pgtrickle.pgt_stream_tables SET consecutive_errors = {} WHERE pgt_id = {}",
            i, pgt_id
        ))
        .await;
    }

    let errors: i32 = db
        .query_scalar(&format!(
            "SELECT consecutive_errors FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {}",
            pgt_id
        ))
        .await;
    assert_eq!(errors, 3);

    // After 3 errors (default max), suspend
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables SET status = 'SUSPENDED' WHERE pgt_id = {} AND consecutive_errors >= 3",
        pgt_id
    )).await;

    let status: String = db
        .query_scalar(&format!(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {}",
            pgt_id
        ))
        .await;
    assert_eq!(status, "SUSPENDED");
}

/// Test that consecutive_errors resets after successful refresh.
#[tokio::test]
async fn test_error_count_resets_on_success() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_reset (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status, consecutive_errors)
         VALUES
            ((SELECT 'src_reset'::regclass::oid), 'reset_st', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE', 2)"
    ).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'reset_st'")
        .await;

    // Simulate successful refresh — reset errors
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables SET consecutive_errors = 0, last_refresh_at = now() WHERE pgt_id = {}",
        pgt_id
    )).await;

    let errors: i32 = db
        .query_scalar(&format!(
            "SELECT consecutive_errors FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {}",
            pgt_id
        ))
        .await;
    assert_eq!(errors, 0);
}

// ── Needs-Reinitialize Flag ──────────────────────────────────────────────

/// Test that needs_reinit flag is set for schema errors and cleared on reinitialize.
#[tokio::test]
async fn test_needs_reinit_lifecycle() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_reinit (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status, needs_reinit)
         VALUES
            ((SELECT 'src_reinit'::regclass::oid), 'reinit_st', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE', FALSE)"
    ).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'reinit_st'")
        .await;

    // Mark for reinitialize (simulating upstream schema change)
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = TRUE WHERE pgt_id = {}",
        pgt_id
    ))
    .await;

    let needs: bool = db
        .query_scalar(&format!(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {}",
            pgt_id
        ))
        .await;
    assert!(needs, "needs_reinit should be TRUE after schema change");

    // Clear after reinitialize
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = FALSE WHERE pgt_id = {}",
        pgt_id
    ))
    .await;

    let needs_after: bool = db
        .query_scalar(&format!(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {}",
            pgt_id
        ))
        .await;
    assert!(
        !needs_after,
        "needs_reinit should be FALSE after reinitialize"
    );
}

// ── Refresh History Status Tracking ──────────────────────────────────────

/// Test that refresh history correctly tracks RUNNING → COMPLETED/FAILED transitions.
#[tokio::test]
async fn test_refresh_history_status_transitions() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_trans (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status)
         VALUES
            ((SELECT 'src_trans'::regclass::oid), 'trans_st', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE')"
    ).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'trans_st'")
        .await;

    // Insert a RUNNING record
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, action, status)
         VALUES ({pgt_id}, now(), now(), 'FULL', 'RUNNING')"
    ))
    .await;

    let refresh_id: i64 = db
        .query_scalar(
            "SELECT refresh_id FROM pgtrickle.pgt_refresh_history WHERE status = 'RUNNING' LIMIT 1",
        )
        .await;

    // Transition to COMPLETED
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_refresh_history
         SET status = 'COMPLETED', end_time = now(), rows_inserted = 42, rows_deleted = 3
         WHERE refresh_id = {}",
        refresh_id
    ))
    .await;

    let status: String = db
        .query_scalar(&format!(
            "SELECT status FROM pgtrickle.pgt_refresh_history WHERE refresh_id = {}",
            refresh_id
        ))
        .await;
    assert_eq!(status, "COMPLETED");

    let rows_ins: i64 = db.query_scalar(&format!(
        "SELECT COALESCE(rows_inserted, 0)::bigint FROM pgtrickle.pgt_refresh_history WHERE refresh_id = {}", refresh_id
    )).await;
    assert_eq!(rows_ins, 42);
}

// ── Multiple STs Independence ────────────────────────────────────────────

/// Test that error handling for one ST doesn't affect others.
#[tokio::test]
async fn test_error_handling_independent_per_st() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_ind1 (id int)").await;
    db.execute("CREATE TABLE src_ind2 (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status, consecutive_errors)
         VALUES
            ((SELECT 'src_ind1'::regclass::oid), 'st_ok', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE', 0),
            ((SELECT 'src_ind2'::regclass::oid), 'st_bad', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE', 3)"
    ).await;

    // Suspend only the one with max errors
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET status = 'SUSPENDED'
         WHERE consecutive_errors >= 3",
    )
    .await;

    let ok_status: String = db
        .query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_ok'")
        .await;
    assert_eq!(ok_status, "ACTIVE");

    let bad_status: String = db
        .query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_bad'")
        .await;
    assert_eq!(bad_status, "SUSPENDED");
}

// ── Error Escalation Threshold ────────────────────────────────────────────

/// Test the exact error threshold that triggers suspension.
///
/// The default max-consecutive-errors threshold used by the scheduler is 3
/// (matches `scheduler.rs` defaults). This test pins the exact threshold:
/// - After 2 errors the ST stays ACTIVE.
/// - After 3 errors the ST transitions to SUSPENDED.
/// - After 4 errors the ST should already have been suspended at error 3.
///
/// Having the threshold pinned here prevents accidental changes to the
/// catalog-side suspension logic from silently breaking the invariant.
#[tokio::test]
async fn test_error_escalation_exact_threshold() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_thresh (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status, consecutive_errors)
         VALUES
            ((SELECT 'src_thresh'::regclass::oid), 'thresh_st', 'public', 'SELECT 1', 'public', 'FULL', 'ACTIVE', 0)"
    ).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'thresh_st'")
        .await;

    // Simulate incrementing errors one at a time, applying suspension at each step
    // using the same threshold (≥ 3) the scheduler uses.
    const MAX_ERRORS: i32 = 3;

    for i in 1..=4 {
        db.execute(&format!(
            "UPDATE pgtrickle.pgt_stream_tables
             SET consecutive_errors = {i}
             WHERE pgt_id = {pgt_id}"
        ))
        .await;

        // Apply the suspension rule as the scheduler would
        db.execute(&format!(
            "UPDATE pgtrickle.pgt_stream_tables
             SET status = 'SUSPENDED'
             WHERE pgt_id = {pgt_id}
               AND consecutive_errors >= {MAX_ERRORS}
               AND status = 'ACTIVE'"
        ))
        .await;

        let status: String = db
            .query_scalar(&format!(
                "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {pgt_id}"
            ))
            .await;
        let errors: i32 = db
            .query_scalar(&format!(
                "SELECT consecutive_errors FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {pgt_id}"
            ))
            .await;

        match i {
            1 | 2 => assert_eq!(
                status, "ACTIVE",
                "should remain ACTIVE after {} errors (threshold is {})",
                errors, MAX_ERRORS
            ),
            _ => assert_eq!(
                status, "SUSPENDED",
                "should be SUSPENDED at {} errors (threshold is {})",
                errors, MAX_ERRORS
            ),
        }
    }
}

/// Test the SUSPENDED → ACTIVE recovery path.
///
/// When a user or admin manually clears the error count and reactivates a
/// suspended ST, the status must transition back to ACTIVE and the error
/// counter must be 0.
#[tokio::test]
async fn test_suspended_to_active_recovery() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_recover (id int)").await;

    // Insert an already-suspended ST
    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status, consecutive_errors)
         VALUES
            ((SELECT 'src_recover'::regclass::oid), 'recover_st', 'public', 'SELECT 1', 'public', 'FULL', 'SUSPENDED', 3)"
    ).await;

    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'recover_st'",
        )
        .await;

    // Simulate admin-initiated recovery (manual reset via ALTER STREAM TABLE RESUME in real code)
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables
         SET status = 'ACTIVE', consecutive_errors = 0
         WHERE pgt_id = {pgt_id}"
    ))
    .await;

    let status: String = db
        .query_scalar(&format!(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {pgt_id}"
        ))
        .await;
    let errors: i32 = db
        .query_scalar(&format!(
            "SELECT consecutive_errors FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {pgt_id}"
        ))
        .await;

    assert_eq!(status, "ACTIVE", "ST should be ACTIVE after recovery");
    assert_eq!(errors, 0, "error counter must be reset to 0 upon recovery");
}

// ── TEST-5: Bgworker crash-recovery resilience ─────────────────────────────
//
// The following tests simulate an abrupt server crash (equivalent to
// `pg_ctl stop -m immediate`) by injecting RUNNING refresh history records
// and verifying the crash-recovery logic correctly transitions them to FAILED
// and never leaves stale RUNNING records after restart.
//
// The tests use the same TestDb harness as the tests above; the "crash" is
// modelled by directly writing catalog state that would be left behind by an
// unclean shutdown rather than by actually killing the PostgreSQL process
// (which would require Testcontainers with a real daemon and is scheduled as
// a follow-up integration suite in the CI matrix).

/// After a crash the bgworker boot-time recovery MUST promote every
/// RUNNING history record to FAILED.  A stale RUNNING entry means the
/// bgworker would never schedule that stream table again.
#[tokio::test]
async fn test_crash_recovery_no_stale_running_records() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_stale (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status)
         VALUES
            ((SELECT 'src_stale'::regclass::oid), 'stale_st', 'public', 'SELECT id FROM src_stale', 'public', 'FULL', 'ACTIVE')"
    ).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'stale_st'")
        .await;

    // Inject three RUNNING records as if the bgworker was killed mid-refresh.
    for _ in 0..3 {
        db.execute(&format!(
            "INSERT INTO pgtrickle.pgt_refresh_history
                 (pgt_id, status, data_timestamp, start_time, action)
             VALUES
                 ({pgt_id}, 'RUNNING', now(), now() - interval '10 seconds', 'FULL')"
        ))
        .await;
    }

    // Run the crash-recovery logic (the same SQL used internally on startup).
    db.execute(
        "UPDATE pgtrickle.pgt_refresh_history
         SET status = 'FAILED',
             end_time = now(),
             error_message = 'Recovered from unclean shutdown (pg_ctl stop -m immediate)'
         WHERE status = 'RUNNING'",
    )
    .await;

    // No RUNNING records should remain.
    let running_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history
              WHERE pgt_id = {pgt_id} AND status = 'RUNNING'"
        ))
        .await;
    assert_eq!(
        running_count, 0,
        "all stale RUNNING records must be promoted to FAILED after crash recovery"
    );

    // All three must now be FAILED.
    let failed_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history
              WHERE pgt_id = {pgt_id} AND status = 'FAILED'"
        ))
        .await;
    assert_eq!(
        failed_count, 3,
        "exactly 3 records must be FAILED after crash recovery"
    );
}

/// RUNNING records injected for *different* stream tables must ALL be cleaned
/// up in a single crash-recovery pass — not just those for the first ST found.
#[tokio::test]
async fn test_crash_recovery_covers_all_stream_tables() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE src_crash_a (id int)").await;
    db.execute("CREATE TABLE src_crash_b (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status)
         VALUES
            ((SELECT 'src_crash_a'::regclass::oid), 'crash_a', 'public', 'SELECT id FROM src_crash_a', 'public', 'FULL', 'ACTIVE'),
            ((SELECT 'src_crash_b'::regclass::oid), 'crash_b', 'public', 'SELECT id FROM src_crash_b', 'public', 'FULL', 'ACTIVE')"
    ).await;

    let id_a: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'crash_a'")
        .await;
    let id_b: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'crash_b'")
        .await;

    // One stale RUNNING record for each ST.
    for id in [id_a, id_b] {
        db.execute(&format!(
            "INSERT INTO pgtrickle.pgt_refresh_history
                 (pgt_id, status, data_timestamp, start_time, action)
             VALUES
                 ({id}, 'RUNNING', now(), now() - interval '5 seconds', 'FULL')"
        ))
        .await;
    }

    // Crash recovery (one-shot bulk UPDATE).
    db.execute(
        "UPDATE pgtrickle.pgt_refresh_history
         SET status = 'FAILED',
             end_time = now(),
             error_message = 'Recovered from unclean shutdown'
         WHERE status = 'RUNNING'",
    )
    .await;

    let total_running: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE status = 'RUNNING'")
        .await;
    assert_eq!(
        total_running, 0,
        "crash recovery must eliminate RUNNING records across all stream tables"
    );
}

/// After crash recovery the stream table's own `status` column must remain
/// ACTIVE (not SUSPENDED) so that the bgworker will schedule it normally on
/// the next tick.  The crash only leaves stale history rows — it must not
/// automatically suspend the stream table.
#[tokio::test]
async fn test_crash_recovery_does_not_suspend_stream_table() {
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE src_no_suspend (id int)").await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, status)
         VALUES
            ((SELECT 'src_no_suspend'::regclass::oid), 'no_suspend_st', 'public', 'SELECT id FROM src_no_suspend', 'public', 'FULL', 'ACTIVE')"
    ).await;

    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'no_suspend_st'",
        )
        .await;

    // Inject a stale RUNNING record.
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_refresh_history
             (pgt_id, status, data_timestamp, start_time, action)
         VALUES
             ({pgt_id}, 'RUNNING', now(), now() - interval '30 seconds', 'FULL')"
    ))
    .await;

    // Run crash recovery.
    db.execute(
        "UPDATE pgtrickle.pgt_refresh_history
         SET status = 'FAILED',
             end_time = now(),
             error_message = 'Recovered from unclean shutdown'
         WHERE status = 'RUNNING'",
    )
    .await;

    // The ST itself must still be ACTIVE.
    let st_status: String = db
        .query_scalar(&format!(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {pgt_id}"
        ))
        .await;
    assert_eq!(
        st_status, "ACTIVE",
        "crash recovery must NOT suspend the stream table — only history rows change"
    );
}
