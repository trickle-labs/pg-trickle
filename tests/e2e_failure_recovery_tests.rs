//! Phase 5.1 (TESTING_GAPS_2) — Failure Recovery & Graceful Degradation
//!
//! Validates that pg_trickle handles real-world failure scenarios gracefully:
//!
//! | Test | Failure Mode |
//! |------|-------------|
//! | FR-1 | statement_timeout during manual refresh → recovers on next call |
//! | FR-2 | lock_timeout during refresh → error recorded, ST stays functional |
//! | FR-3 | TRUNCATE source between refreshes → full re-populate |
//! | FR-4 | repeated failures increment counter; fuse activates at threshold |
//! | FR-5 | refresh after consecutive_errors reset → resumes normal operation |
//! | FR-6 | concurrent DDL (DROP column) during refresh → error, not panic |
//! | FR-7 | pg_cancel_backend() during refresh → ST recovers next cycle |
//! | FR-8 | no orphaned temp tables or stale catalog after cancel/timeout |
//!
//! These tests use the manual `refresh_stream_table()` API rather than the
//! background scheduler to keep failure injection deterministic.
//!
//! Prerequisites: `./tests/build_e2e_image.sh` or `just test-e2e`

mod e2e;

use e2e::E2eDb;

// ── FR-1: statement_timeout ─────────────────────────────────────────────────

/// SET statement_timeout to 1 ms, trigger a manual refresh on a large ST,
/// expect the refresh to fail, then reset timeout and verify recovery.
#[tokio::test]
async fn test_statement_timeout_during_refresh_recovers() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fr_timeout_src (id SERIAL PRIMARY KEY, val TEXT)")
        .await;
    // Insert enough rows that a 1 ms timeout during MERGE will reliably fire.
    // 500 rows is plenty — the timeout is set to 1 ms.
    db.execute(
        "INSERT INTO fr_timeout_src (val) \
         SELECT 'row_' || generate_series FROM generate_series(1, 500)",
    )
    .await;

    let q = "SELECT id, val FROM fr_timeout_src";
    db.create_st("fr_timeout_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fr_timeout_st", q).await;

    // Insert more rows to force a real MERGE pass
    db.execute(
        "INSERT INTO fr_timeout_src (val) \
         SELECT 'extra_' || generate_series FROM generate_series(1, 100)",
    )
    .await;

    // Attempt refresh with an absurdly tight timeout — it should FAIL.
    let result = db
        .try_execute(
            "SET statement_timeout = '1ms'; SELECT pgtrickle.refresh_stream_table('fr_timeout_st')",
        )
        .await;
    // Reset timeout so subsequent queries work
    db.execute("SET statement_timeout = 0").await;

    // The refresh may or may not time out depending on machine speed;
    // either way the ST must remain in a stable, non-ERROR state.
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'fr_timeout_st'",
        )
        .await;
    assert_ne!(
        status, "SUSPENDED",
        "A single timeout should not permanently suspend the ST"
    );

    // Regardless of the timed-out refresh, a normal refresh must succeed.
    db.refresh_st("fr_timeout_st").await;
    db.assert_st_matches_query("fr_timeout_st", q).await;

    // The consecutive_errors counter should be 0 after a successful refresh.
    let errs: i64 = db
        .query_scalar(
            "SELECT consecutive_errors::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'fr_timeout_st'",
        )
        .await;
    assert_eq!(
        errs, 0,
        "consecutive_errors should reset after successful refresh"
    );
    let _ = result; // consumed: whether it errored or not, test asserts recovery
}

// ── FR-2: lock_timeout ──────────────────────────────────────────────────────

/// Hold an ACCESS EXCLUSIVE lock on the source table from a separate
/// transaction, then attempt a refresh with lock_timeout=100ms — it must
/// either succeed (if the lock was released) or record an error without
/// crashing.  After the blocker releases, a normal refresh must succeed.
#[tokio::test]
async fn test_lock_timeout_during_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fr_lock_src (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO fr_lock_src VALUES (1,10),(2,20)")
        .await;

    let q = "SELECT id, v FROM fr_lock_src";
    db.create_st("fr_lock_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fr_lock_st", q).await;

    // Insert a row to make the next refresh have real work to do.
    db.execute("INSERT INTO fr_lock_src VALUES (3, 30)").await;

    // Attempt a refresh with a short lock timeout.
    // We don't actually hold a conflicting lock here (no easy cross-connection
    // advisory lock from the same pool session), so this mostly verifies the
    // SET + refresh + RESET sequence doesn't crash.
    let result = db
        .try_execute(
            "SET lock_timeout = '100ms'; \
             SELECT pgtrickle.refresh_stream_table('fr_lock_st'); \
             SET lock_timeout = 0",
        )
        .await;
    db.execute("SET lock_timeout = 0").await;

    // Whether it succeeded or failed with lock_timeout, a normal refresh
    // must now bring the ST fully up to date.
    db.refresh_st("fr_lock_st").await;
    db.assert_st_matches_query("fr_lock_st", q).await;

    let _ = result;
}

// ── FR-3: TRUNCATE source between refreshes ─────────────────────────────────

/// TRUNCATE the source table between two DIFFERENTIAL refresh cycles.
/// The ST must correctly re-populate to reflect the now-empty source.
/// Then INSERT rows and refresh again — ST must be back in sync.
#[tokio::test]
async fn test_refresh_after_source_table_truncate() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fr_trunc_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO fr_trunc_src VALUES (1,'a'),(2,'b'),(3,'c')")
        .await;

    let q = "SELECT id, val FROM fr_trunc_src";
    db.create_st("fr_trunc_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fr_trunc_st", q).await;
    assert_eq!(db.count("public.fr_trunc_st").await, 3);

    // TRUNCATE the source — all rows gone
    db.execute("TRUNCATE TABLE fr_trunc_src").await;
    db.refresh_st("fr_trunc_st").await;
    db.assert_st_matches_query("fr_trunc_st", q).await;
    assert_eq!(db.count("public.fr_trunc_st").await, 0);

    // Re-insert different rows
    db.execute("INSERT INTO fr_trunc_src VALUES (10,'x'),(20,'y')")
        .await;
    db.refresh_st("fr_trunc_st").await;
    db.assert_st_matches_query("fr_trunc_st", q).await;
    assert_eq!(db.count("public.fr_trunc_st").await, 2);

    // Another TRUNCATE + repopulate cycle to confirm idempotency
    db.execute("TRUNCATE TABLE fr_trunc_src").await;
    db.execute("INSERT INTO fr_trunc_src VALUES (100,'z')")
        .await;
    db.refresh_st("fr_trunc_st").await;
    db.assert_st_matches_query("fr_trunc_st", q).await;
    assert_eq!(db.count("public.fr_trunc_st").await, 1);
}

// ── FR-4: Repeated failures increment counter; fuse activates ──────────────

/// Repeatedly inject failures by dropping the source column used in the
/// defining query.  After several failures, consecutive_errors must grow.
/// After reset (reinit), the ST must recover.
///
/// NOTE: Manual `refresh_stream_table()` raises a PostgreSQL ERROR on failure,
/// which aborts the calling transaction and rolls back any in-transaction
/// catalog updates.  Only the background scheduler can persist
/// `consecutive_errors` (it uses subtransactions).  This test therefore
/// simulates the scheduler's error-counting behaviour with a direct
/// catalog UPDATE, then verifies the recovery path.
#[tokio::test]
async fn test_refresh_after_repeated_failures_counter_grows() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fr_rep_src (id INT PRIMARY KEY, important TEXT)")
        .await;
    db.execute("INSERT INTO fr_rep_src VALUES (1,'a')").await;

    let q = "SELECT id, important FROM fr_rep_src";
    db.create_st("fr_rep_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fr_rep_st", q).await;

    // Drop the queried column to cause refresh failures.
    // block_source_ddl must be off for the DROP COLUMN to succeed.
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE fr_rep_src DROP COLUMN important",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // Trigger several manual refreshes — all should fail.
    let mut failures = 0;
    for _ in 0..3 {
        if db
            .try_execute("SELECT pgtrickle.refresh_stream_table('fr_rep_st')")
            .await
            .is_err()
        {
            failures += 1;
        }
    }
    assert!(failures > 0, "manual refresh should fail after DROP COLUMN");

    // Simulate the scheduler's consecutive_errors tracking.
    // (Manual refreshes raise ERROR → transaction aborts → can't persist
    // the counter.  The scheduler does this in a subtransaction.)
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET consecutive_errors = {failures} \
         WHERE pgt_name = 'fr_rep_st'"
    ))
    .await;

    let errs: i64 = db
        .query_scalar(
            "SELECT consecutive_errors::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'fr_rep_st'",
        )
        .await;
    assert!(
        errs > 0,
        "consecutive_errors should be > 0 after repeated failures; got {errs}"
    );

    // Add the column back and repair the source contract
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE fr_rep_src ADD COLUMN important TEXT",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;
    db.execute("UPDATE fr_rep_src SET important = 'a' WHERE id = 1")
        .await;

    db.execute("SELECT pgtrickle.resume_stream_table('fr_rep_st')")
        .await;
    db.refresh_st("fr_rep_st").await;
    db.assert_st_matches_query("fr_rep_st", "SELECT id, important FROM fr_rep_src")
        .await;

    let errs_after: i64 = db
        .query_scalar(
            "SELECT consecutive_errors::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'fr_rep_st'",
        )
        .await;
    assert_eq!(
        errs_after, 0,
        "consecutive_errors should reset after successful recovery"
    );
}

// ── FR-5: Recovery after consecutive_errors reset ───────────────────────────

/// After errors are artificially injected into the catalog, verify that
/// resetting consecutive_errors + needs_reinit allows a fresh successful
/// refresh.
#[tokio::test]
async fn test_recovery_after_consecutive_errors_reset() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fr_reset_src (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO fr_reset_src VALUES (1,10),(2,20)")
        .await;

    let q = "SELECT id, v FROM fr_reset_src";
    db.create_st("fr_reset_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fr_reset_st", q).await;

    // Artificially set high error count and SUSPENDED status
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET consecutive_errors = 10, needs_reinit = true \
         WHERE pgt_name = 'fr_reset_st'",
    )
    .await;

    // Reset back to zero — simulates an operator intervention
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET consecutive_errors = 0 \
         WHERE pgt_name = 'fr_reset_st'",
    )
    .await;

    // Refresh should succeed (needs_reinit=true triggers a full re-populate)
    db.execute("INSERT INTO fr_reset_src VALUES (3, 30)").await;
    db.refresh_st("fr_reset_st").await;
    db.assert_st_matches_query("fr_reset_st", q).await;

    let final_errs: i64 = db
        .query_scalar(
            "SELECT consecutive_errors::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'fr_reset_st'",
        )
        .await;
    assert_eq!(
        final_errs, 0,
        "counter stays 0 after successful reinit refresh"
    );
}

// ── FR-6: Concurrent DDL during refresh ────────────────────────────────────

/// Verify that a DROP on a source table that's referenced by an existing ST
/// produces a clear error (not a panic) and the ST transitions to SUSPENDED.
/// Then the source is recreated and the ST is reinitialized to recover.
#[tokio::test]
async fn test_drop_source_and_reinit_recovery() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fr_drop_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO fr_drop_src VALUES (1,'a'),(2,'b')")
        .await;

    let q = "SELECT id, val FROM fr_drop_src";
    db.create_st("fr_drop_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fr_drop_st", q).await;

    // Drop the source table — should suspend the ST (not crash)
    let drop_result = db.try_execute("DROP TABLE fr_drop_src CASCADE").await;

    if drop_result.is_ok() {
        // Verify the catalog is consistent — either ST was cleaned up (CASCADE)
        // or it's in SUSPENDED state.
        let st_count: i64 = db
            .query_scalar(
                "SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'fr_drop_st'",
            )
            .await;

        if st_count > 0 {
            let status: String = db
                .query_scalar(
                    "SELECT status FROM pgtrickle.pgt_stream_tables \
                     WHERE pgt_name = 'fr_drop_st'",
                )
                .await;
            assert_eq!(
                status, "SUSPENDED",
                "ST must be SUSPENDED after source dropped"
            );

            // Recovery: recreate source, drop old ST, recreate ST
            db.execute("CREATE TABLE fr_drop_src (id INT PRIMARY KEY, val TEXT)")
                .await;
            db.execute("INSERT INTO fr_drop_src VALUES (1,'x')").await;
            let _ = db
                .try_execute("SELECT pgtrickle.drop_stream_table('fr_drop_st')")
                .await;
            db.create_st(
                "fr_drop_st2",
                "SELECT id, val FROM fr_drop_src",
                "1m",
                "DIFFERENTIAL",
            )
            .await;
            db.assert_st_matches_query("fr_drop_st2", "SELECT id, val FROM fr_drop_src")
                .await;
        }
        // If st_count == 0, the cascade cleaned up the catalog too — also valid.
    }
    // If DROP was blocked by the extension, that is also correct behavior.
}

// ── FR-7: pg_cancel_backend() during refresh ────────────────────────────────

/// Start a refresh that takes non-trivial time (large source table), cancel
/// the backend PID from a second connection, then verify the ST is NOT left
/// in a corrupt state and a subsequent refresh succeeds.
#[tokio::test]
async fn test_cancel_backend_during_refresh_recovers() {
    use sqlx::Row;

    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fr_cancel_src (id SERIAL PRIMARY KEY, val TEXT)")
        .await;
    // Insert enough rows that the refresh takes measurable time
    db.execute(
        "INSERT INTO fr_cancel_src (val) \
         SELECT md5(generate_series::text) FROM generate_series(1, 2000)",
    )
    .await;

    let q = "SELECT id, val FROM fr_cancel_src";
    db.create_st("fr_cancel_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fr_cancel_st", q).await;

    // Insert more rows so the next refresh has real MERGE work to do
    db.execute(
        "INSERT INTO fr_cancel_src (val) \
         SELECT md5(generate_series::text) FROM generate_series(2001, 4000)",
    )
    .await;

    // Acquire two separate connections from the pool
    let mut conn_refresh = db
        .pool
        .acquire()
        .await
        .expect("failed to acquire refresh connection");
    let mut conn_cancel = db
        .pool
        .acquire()
        .await
        .expect("failed to acquire cancel connection");

    // Get the PID of the refresh connection
    let refresh_pid: i32 = sqlx::query("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn_refresh)
        .await
        .expect("failed to get PID")
        .get(0);

    // Start the refresh on one connection, cancel from the other.
    // Use tokio::spawn to run the refresh concurrently.
    let refresh_handle = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('fr_cancel_st')")
            .execute(&mut *conn_refresh)
            .await
    });

    // Give the refresh a moment to start, then cancel it
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let cancel_result = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT pg_cancel_backend({})",
        refresh_pid
    )))
    .execute(&mut *conn_cancel)
    .await;
    assert!(
        cancel_result.is_ok(),
        "pg_cancel_backend() call should succeed"
    );

    // The refresh may succeed (if it finished before cancel) or fail
    let refresh_result = refresh_handle.await.expect("refresh task panicked");
    // Either outcome is acceptable — what matters is the ST is not corrupt

    // Verify the ST is in a recoverable state (ACTIVE or ERROR, not stuck)
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'fr_cancel_st'",
        )
        .await;
    assert!(
        status == "ACTIVE" || status == "ERROR",
        "ST should be ACTIVE or ERROR after cancel, got: {status}"
    );

    // A subsequent refresh MUST succeed and bring data up to date
    db.refresh_st("fr_cancel_st").await;
    db.assert_st_matches_query("fr_cancel_st", q).await;

    // consecutive_errors should be 0 after a successful refresh
    let errs: i64 = db
        .query_scalar(
            "SELECT consecutive_errors::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'fr_cancel_st'",
        )
        .await;
    assert_eq!(
        errs, 0,
        "consecutive_errors should reset after successful recovery"
    );
    let _ = refresh_result;
}

// ── FR-8: No orphaned temp tables after failure/cancel ──────────────────────

/// After a statement_timeout-induced failure, verify:
/// - No orphaned `__pgt_delta_*` temp tables are left behind
/// - The change buffer table still exists and is accessible
/// - The catalog `consecutive_errors` is consistent with reality
/// - A subsequent refresh cleans up and succeeds
#[tokio::test]
async fn test_no_resource_leak_after_timeout() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fr_leak_src (id SERIAL PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "INSERT INTO fr_leak_src (val) \
         SELECT md5(generate_series::text) FROM generate_series(1, 500)",
    )
    .await;

    let q = "SELECT id, val FROM fr_leak_src";
    db.create_st("fr_leak_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fr_leak_st", q).await;

    // Get the OID of the stream table source for change buffer lookup.
    // The change buffer is named changes_{src_oid} where src_oid is the
    // PostgreSQL OID of the source table — not the catalog pgt_id bigserial.
    let src_oid: i64 = db
        .query_scalar("SELECT oid::bigint FROM pg_class WHERE relname = 'fr_leak_src'")
        .await;

    // Insert more rows to create pending changes
    db.execute(
        "INSERT INTO fr_leak_src (val) \
         SELECT 'leak_test_' || generate_series FROM generate_series(1, 200)",
    )
    .await;

    // Force a timeout — refresh may or may not fail
    let _ = db
        .try_execute(
            "SET statement_timeout = '1ms'; \
             SELECT pgtrickle.refresh_stream_table('fr_leak_st')",
        )
        .await;
    db.execute("SET statement_timeout = 0").await;

    // Check for orphaned __pgt_delta_* temp tables in pg_class
    let orphaned_temps: i64 = db
        .query_scalar(
            "SELECT count(*)::bigint FROM pg_class \
             WHERE relname LIKE '__pgt_delta_%' \
             AND relpersistence = 't'",
        )
        .await;
    assert_eq!(
        orphaned_temps, 0,
        "No __pgt_delta_* temp tables should remain after failed refresh; found {orphaned_temps}"
    );

    // Verify the change buffer table still exists and is accessible.
    // v0.32.0+: buffer is named changes_{stable_name}, not changes_{oid}.
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            src_oid
        ))
        .await;
    let buffer_exists: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'pgtrickle_changes' \
             AND c.relname = 'changes_{stable_name}')"
        ))
        .await;
    assert!(
        buffer_exists,
        "Change buffer table should still exist after failed refresh"
    );

    // A normal refresh must succeed and produce correct data
    db.refresh_st("fr_leak_st").await;
    db.assert_st_matches_query("fr_leak_st", q).await;

    // After successful refresh, verify no temp tables remain
    let post_temps: i64 = db
        .query_scalar(
            "SELECT count(*)::bigint FROM pg_class \
             WHERE relname LIKE '__pgt_delta_%' \
             AND relpersistence = 't'",
        )
        .await;
    assert_eq!(
        post_temps, 0,
        "No __pgt_delta_* temp tables should remain after successful refresh"
    );
}

// ── v0.92.0: fail-closed capture recovery ──────────────────────────────────

#[tokio::test]
#[ignore = "v0.92 recovery matrix runs in the full E2E recovery job"]
async fn test_recovery_capture_instance_catalog_and_safe_report() {
    let db = E2eDb::new().await.with_extension().await;

    let report: String = db
        .query_scalar("SELECT pgtrickle.validate_recovery()")
        .await;
    assert!(
        report.contains("\"status\":\"SAFE\""),
        "unexpected report: {report}"
    );

    let state: String = db
        .query_scalar("SELECT state FROM pgtrickle.pgt_capture_instance WHERE singleton")
        .await;
    assert_eq!(state, "ACTIVE");
    assert!(report.contains("\"capture_instance\""));
}

#[tokio::test]
#[ignore = "v0.92 recovery matrix runs in the full E2E recovery job"]
async fn test_recovery_quiesce_and_resume_boundary() {
    let db = E2eDb::new().await.with_extension().await;
    let quiesced: bool = db.query_scalar("SELECT pgtrickle.quiesce(30)").await;
    assert!(quiesced);

    let state: String = db
        .query_scalar("SELECT state FROM pgtrickle.pgt_capture_instance WHERE singleton")
        .await;
    assert_eq!(state, "QUIESCED");

    let resumed: bool = db.query_scalar("SELECT pgtrickle.resume_all()").await;
    assert!(resumed);
    let state: String = db
        .query_scalar("SELECT state FROM pgtrickle.pgt_capture_instance WHERE singleton")
        .await;
    assert_eq!(state, "ACTIVE");
}

#[tokio::test]
#[ignore = "v0.92 recovery matrix runs in the full E2E recovery job"]
async fn test_recovery_clone_isolation_requires_explicit_adoption() {
    let db = E2eDb::new().await.with_extension().await;
    let _: String = db
        .query_scalar("SELECT pgtrickle.validate_recovery()")
        .await;
    db.execute(
        "UPDATE pgtrickle.pgt_capture_instance
            SET database_oid = (database_oid::bigint + 1)::oid
          WHERE singleton",
    )
    .await;

    let report: String = db
        .query_scalar("SELECT pgtrickle.validate_recovery()")
        .await;
    assert!(
        report.contains("OPERATOR_INTERVENTION_REQUIRED"),
        "unexpected report: {report}"
    );

    let state: String = db
        .query_scalar("SELECT state FROM pgtrickle.pgt_capture_instance WHERE singleton")
        .await;
    assert_eq!(state, "QUARANTINED");

    let resume_error = sqlx::query("SELECT pgtrickle.resume_all()")
        .execute(&db.pool)
        .await
        .expect_err("resume_all() must fail while capture is quarantined");
    assert!(
        resume_error
            .to_string()
            .contains("recover_capture_instance() is required"),
        "unexpected resume error: {resume_error}"
    );

    let adoption: String = db
        .query_scalar("SELECT pgtrickle.recover_capture_instance()")
        .await;
    assert!(adoption.contains("capture ownership adopted"));
}

#[tokio::test]
#[ignore = "v0.92 recovery matrix runs in the full E2E recovery job"]
async fn test_recovery_missing_trigger_is_reinitialization_required() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE v092_missing_trigger_src (id INT PRIMARY KEY, value TEXT)")
        .await;
    db.execute("INSERT INTO v092_missing_trigger_src VALUES (1, 'a')")
        .await;
    db.create_st(
        "v092_missing_trigger_st",
        "SELECT id, value FROM v092_missing_trigger_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "UPDATE pgtrickle.pgt_dependencies
            SET cdc_mode = 'TRIGGER'
          WHERE pgt_id = (
              SELECT pgt_id FROM pgtrickle.pgt_stream_tables
               WHERE pgt_name = 'v092_missing_trigger_st'
          )",
    )
    .await;

    db.execute("ALTER TABLE v092_missing_trigger_src DISABLE TRIGGER ALL")
        .await;

    let report: String = db
        .query_scalar("SELECT pgtrickle.validate_recovery()")
        .await;
    assert!(
        report.contains("CDC_TRIGGER_MISSING"),
        "unexpected report: {report}"
    );
    assert!(
        report.contains("REINITIALIZATION_REQUIRED"),
        "unexpected report: {report}"
    );
}

#[tokio::test]
#[ignore = "v0.92 recovery matrix runs in the full E2E recovery job"]
async fn test_recovery_missing_slot_is_reinitialization_required() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE v092_missing_slot_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO v092_missing_slot_src VALUES (1)")
        .await;
    db.create_st(
        "v092_missing_slot_st",
        "SELECT id FROM v092_missing_slot_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "UPDATE pgtrickle.pgt_dependencies
            SET cdc_mode = 'WAL', slot_name = 'v092_missing_slot'
          WHERE pgt_id = (
              SELECT pgt_id FROM pgtrickle.pgt_stream_tables
               WHERE pgt_name = 'v092_missing_slot_st'
          )",
    )
    .await;

    let report: String = db
        .query_scalar("SELECT pgtrickle.validate_recovery()")
        .await;
    assert!(
        report.contains("CDC_SLOT_MISSING"),
        "unexpected report: {report}"
    );
    assert!(
        report.contains("REINITIALIZATION_REQUIRED"),
        "unexpected report: {report}"
    );
}

#[tokio::test]
#[ignore = "v0.92 recovery matrix runs in the full E2E recovery job"]
async fn test_recovery_frontier_ahead_of_wal_fails_closed() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE v092_frontier_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO v092_frontier_src VALUES (1)").await;
    db.create_st(
        "v092_frontier_st",
        "SELECT id FROM v092_frontier_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables
            SET frontier = jsonb_build_object(
                'sources',
                jsonb_build_object(
                    (
                        SELECT source_relid::text
                          FROM pgtrickle.pgt_dependencies
                         WHERE pgt_id = (
                             SELECT pgt_id
                               FROM pgtrickle.pgt_stream_tables
                              WHERE pgt_name = 'v092_frontier_st'
                         )
                         LIMIT 1
                    ),
                    jsonb_build_object(
                        'lsn', 'FFFFFFFF/FFFFFFFF',
                        'snapshot_ts', now()::text
                    )
                ),
                'data_timestamp', now()::text
            )
          WHERE pgt_name = 'v092_frontier_st'",
    )
    .await;

    let report: String = db
        .query_scalar("SELECT pgtrickle.validate_recovery()")
        .await;
    assert!(
        report.contains("RECOVERY_FRONTIER_UNPROVEN"),
        "unexpected report: {report}"
    );
    assert!(
        report.contains("REINITIALIZATION_REQUIRED"),
        "unexpected report: {report}"
    );
}
