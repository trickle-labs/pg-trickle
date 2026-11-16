//! ERR-1e: E2E tests for the error-state circuit breaker.
//!
//! Validates the full cycle:
//!   1. A stream table whose source changes incompatibly enters SUSPENDED
//!      status with error details.
//!   2. `last_error_message` is populated with a meaningful error.
//!   3. `last_error_at` is set.
//!   4. The scheduler skips SUSPENDED tables (no further refresh attempts).
//!   5. `alter_stream_table` with a fixed query clears the error and
//!      returns the ST to ACTIVE status.
//!
//! These tests require the background scheduler (full E2E only).

mod common;
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
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(
        sched_running,
        "pg_trickle scheduler did not appear within 90 s"
    );
}

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

/// ERR-1e: A permanent source schema error immediately sets SUSPENDED status with
/// last_error_message, and alter_stream_table with a fixed query clears it.
///
/// Scenario:
/// 1. Create a FULL-mode ST querying a specific column.
/// 2. Drop the column from the source table (permanent schema error).
/// 3. The DDL hook suspends the ST and records the schema error.
/// 4. Verify `last_error_message` is populated.
/// 5. Verify `last_error_at` is set.
/// 6. Fix the source table (re-add the column).
/// 7. ALTER STREAM TABLE with a fixed query → status returns to ACTIVE.
/// 8. Verify error fields are cleared.
#[tokio::test]
async fn test_permanent_error_sets_error_status_and_alter_clears() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // ── Setup ──
    db.execute("CREATE TABLE err1e_src (id INT PRIMARY KEY, val TEXT, extra TEXT)")
        .await;
    db.execute("INSERT INTO err1e_src VALUES (1, 'hello', 'x')")
        .await;
    db.create_st(
        "err1e_st",
        "SELECT id, val, extra FROM err1e_src",
        "1s",
        "FULL",
    )
    .await;

    // Verify initial population
    assert_eq!(db.count("public.err1e_st").await, 1);

    // ── Inject permanent error: drop the column ──
    // Wrap DML + DDL in a single transaction so the CDC change and schema
    // change become visible atomically.  The DML triggers CDC so the
    // scheduler detects upstream changes and attempts a FULL refresh,
    // which fails because the defining query references the dropped column.
    db.execute_seq(&[
        "BEGIN",
        "INSERT INTO err1e_src(id, val, extra) VALUES (2, 'trigger_dml', 'z')",
        "SET LOCAL pg_trickle.block_source_ddl = false",
        "ALTER TABLE err1e_src DROP COLUMN extra",
        "COMMIT",
    ])
    .await;

    // ── Wait for SUSPENDED status ──
    let entered_error =
        wait_for_status(&db, "err1e_st", "SUSPENDED", Duration::from_secs(60)).await;
    assert!(
        entered_error,
        "ST should enter SUSPENDED status after permanent schema-change error"
    );

    // ── Verify last_error_message is populated ──
    let error_msg: Option<String> = db
        .query_scalar_opt(
            "SELECT last_error_message FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'err1e_st'",
        )
        .await;
    assert!(
        error_msg.is_some(),
        "last_error_message should be populated after permanent error"
    );
    let msg = error_msg.unwrap();
    assert!(!msg.is_empty(), "last_error_message should not be empty");

    // ── Verify last_error_at is set ──
    let has_error_at: bool = db
        .query_scalar(
            "SELECT last_error_at IS NOT NULL FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'err1e_st'",
        )
        .await;
    assert!(has_error_at, "last_error_at should be set");

    // ── Verify scheduler skips ERROR tables ──
    // Record the failed refresh count and wait — no new attempts should happen
    let failed_before: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'err1e_st'",
        )
        .await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let failed_after: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'err1e_st'",
        )
        .await;
    assert_eq!(
        failed_before, failed_after,
        "Scheduler should not attempt further refreshes on ERROR table"
    );

    // ── Fix: ALTER directly on the ERROR table with the corrected query ──
    // Do NOT call resume_stream_table first — the scheduler skips ERROR tables,
    // so issuing alter_stream_table directly is race-free. alter_stream_table
    // transitions through SUSPENDED → (full refresh) → ACTIVE and now also
    // clears any previous error state (ERR-1f), so no separate resume is needed.
    db.alter_st("err1e_st", "query => 'SELECT id, val FROM err1e_src'")
        .await;

    // ── Verify status is back to ACTIVE ──
    let (status, _, _, _) = db.pgt_status("err1e_st").await;
    assert_eq!(
        status, "ACTIVE",
        "ST should be ACTIVE after alter with fixed query"
    );

    // ── Verify error fields are cleared ──
    let cleared_msg: Option<String> = db
        .query_scalar_opt(
            "SELECT last_error_message FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'err1e_st'",
        )
        .await;
    assert!(
        cleared_msg.is_none(),
        "last_error_message should be NULL after successful alter"
    );

    let cleared_at: bool = db
        .query_scalar(
            "SELECT last_error_at IS NULL FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'err1e_st'",
        )
        .await;
    assert!(cleared_at, "last_error_at should be NULL after clearing");
}

/// ERR-1e: `last_error_message` and `last_error_at` are visible in the
/// `stream_tables_info` view for SUSPENDED tables.
#[tokio::test]
async fn test_error_columns_visible_in_info_view() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE err1e_view_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO err1e_view_src VALUES (1, 'ok')")
        .await;
    db.create_st(
        "err1e_view_st",
        "SELECT id, val FROM err1e_view_src",
        "1s",
        "FULL",
    )
    .await;

    // Wait for initial population
    assert_eq!(db.count("public.err1e_view_st").await, 1);

    // Inject permanent error — wrap DML + DDL in a transaction so CDC
    // captures a change atomically with the schema break.
    db.execute_seq(&[
        "BEGIN",
        "INSERT INTO err1e_view_src(id, val) VALUES (2, 'trigger_dml')",
        "SET LOCAL pg_trickle.block_source_ddl = false",
        "ALTER TABLE err1e_view_src DROP COLUMN val",
        "COMMIT",
    ])
    .await;

    let entered_error =
        wait_for_status(&db, "err1e_view_st", "SUSPENDED", Duration::from_secs(60)).await;
    assert!(entered_error, "ST should enter SUSPENDED status");

    // Verify error fields visible in the info view
    let has_error_info: bool = db
        .query_scalar(
            "SELECT last_error_message IS NOT NULL AND last_error_at IS NOT NULL \
             FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'err1e_view_st'",
        )
        .await;
    assert!(
        has_error_info,
        "Error columns should be visible in stream_tables_info view"
    );
}

/// ERR-1e: `refresh_stream_table()` rejects SUSPENDED status.
/// Use `resume_stream_table()` to clear the suspension first.
#[tokio::test]
async fn test_refresh_rejects_error_status() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE err1e_rej_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO err1e_rej_src VALUES (1, 'ok')")
        .await;
    db.create_st(
        "err1e_rej_st",
        "SELECT id, val FROM err1e_rej_src",
        "1s",
        "FULL",
    )
    .await;

    // Inject permanent error — wrap DML + DDL in a transaction so CDC
    // captures a change atomically with the schema break.
    db.execute_seq(&[
        "BEGIN",
        "INSERT INTO err1e_rej_src(id, val) VALUES (2, 'trigger_dml')",
        "SET LOCAL pg_trickle.block_source_ddl = false",
        "ALTER TABLE err1e_rej_src DROP COLUMN val",
        "COMMIT",
    ])
    .await;

    let entered_error =
        wait_for_status(&db, "err1e_rej_st", "SUSPENDED", Duration::from_secs(60)).await;
    assert!(entered_error, "ST should enter SUSPENDED status");

    // Manual refresh should be rejected
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('err1e_rej_st')")
        .await;
    assert!(
        result.is_err(),
        "refresh_stream_table should reject SUSPENDED status"
    );
}


/// #1438: A permanent worker failure is handled asynchronously by the
/// dispatcher. A successful resume must not be revoked by that old job on the
/// following scheduler tick. Exercise the real parallel scheduler and verify
/// the protected reinitialization completes and remains ACTIVE.
#[tokio::test]
async fn test_resume_after_permanent_parallel_failure_survives_dispatcher_reap() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "on", "on")
        .await;
    db.alter_system_set_and_wait("pg_trickle.scheduler_interval_ms", "3000", "3000")
        .await;
    db.alter_system_set_and_wait("pg_trickle.min_schedule_seconds", "1", "1")
        .await;
    db.alter_system_set_and_wait("pg_trickle.auto_backoff", "off", "off")
        .await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "parallel scheduler failed to start"
    );

    db.execute("CREATE TABLE fence_live_src (id INT PRIMARY KEY, divisor INT NOT NULL)")
        .await;
    db.execute("INSERT INTO fence_live_src VALUES (1, 2)").await;
    db.create_st(
        "fence_live_st",
        "SELECT id, 100 / divisor AS quotient FROM fence_live_src",
        "1s",
        "FULL",
    )
    .await;

    // The initial view is valid. Inserting a zero divisor creates a
    // permanently failing scheduled refresh without an intervening DDL hook.
    db.execute("INSERT INTO fence_live_src VALUES (2, 0)").await;
    assert!(
        db.wait_for_condition(
            "permanent worker failure recorded before dispatch reaping",
            "SELECT EXISTS(
                SELECT 1 FROM pgtrickle.pgt_scheduler_jobs j
                JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = j.root_pgt_id
                WHERE st.pgt_name = 'fence_live_st' AND j.status = 'PERMANENT_FAILED'
            ) AND EXISTS(
                SELECT 1 FROM pgtrickle.pgt_stream_tables
                WHERE pgt_name = 'fence_live_st' AND status = 'ERROR'
            )",
            Duration::from_secs(90),
            Duration::from_millis(50),
        )
        .await,
        "the worker did not persist a permanent failure"
    );
    let previous_generation: i64 = db
        .query_scalar(
            "SELECT recovery_generation FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'fence_live_st'",
        )
        .await;
    let failed_job_generation: i64 = db
        .query_scalar(
            "SELECT j.member_recovery_generations[1]
             FROM pgtrickle.pgt_scheduler_jobs j
             JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = j.root_pgt_id
             WHERE st.pgt_name = 'fence_live_st'
               AND j.status = 'PERMANENT_FAILED'
             ORDER BY j.job_id DESC LIMIT 1",
        )
        .await;
    assert_eq!(failed_job_generation, previous_generation);

    // Repair the original source error and resume while the old dispatch
    // outcome may still be pending. A later reaper tick must not undo resume.
    db.execute("UPDATE fence_live_src SET divisor = 2 WHERE id = 2")
        .await;
    db.execute("SELECT pgtrickle.resume_stream_table('public.fence_live_st')")
        .await;
    let resumed_generation: i64 = db
        .query_scalar(
            "SELECT recovery_generation FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'fence_live_st'",
        )
        .await;
    assert_eq!(resumed_generation, previous_generation + 1);

    assert!(
        db.wait_for_condition(
            "scheduled reinitialize after resume",
            "SELECT EXISTS(
                SELECT 1 FROM pgtrickle.pgt_stream_tables st
                WHERE st.pgt_name = 'fence_live_st'
                  AND st.status = 'ACTIVE' AND NOT st.needs_reinit
                  AND EXISTS(
                    SELECT 1 FROM pgtrickle.pgt_refresh_history h
                    WHERE h.pgt_id = st.pgt_id AND h.action = 'REINITIALIZE'
                      AND h.status = 'COMPLETED'
                  )
            )",
            Duration::from_secs(90),
            Duration::from_millis(100),
        )
        .await,
        "no protected reinitialization ran after resume"
    );
    assert_eq!(db.count("public.fence_live_st").await, 2);
    let quotient: i32 = db
        .query_scalar("SELECT quotient FROM public.fence_live_st WHERE id = 2")
        .await;
    assert_eq!(quotient, 50);

    // Span more than one slow dispatcher tick after the successful rebuild.
    tokio::time::sleep(Duration::from_secs(7)).await;
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'fence_live_st'",
        )
        .await;
    assert_eq!(status, "ACTIVE", "old failed job overwrote the resume");
}
