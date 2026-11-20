//! E2E regressions for issue #1454: CDC capture must not queue NOTIFY.
//!
//! The scheduler uses polling, not LISTEN. Committed changes must still reach
//! scheduled stream tables without serializing source transactions at COMMIT.

mod e2e;

use e2e::E2eDb;
#[cfg(not(feature = "light-e2e"))]
use std::time::Duration;

// Return the generated SQL body for a specific source's CDC trigger function.
async fn capture_function_body(db: &E2eDb, source: &str, suffix: &str) -> String {
    let source_oid = db.table_oid(source).await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT source_stable_name FROM pgtrickle.pgt_change_tracking \
             WHERE source_relid = {source_oid}"
        ))
        .await;
    let name = format!("pg_trickle_cdc_{suffix}_{stable_name}");
    db.query_scalar(&format!(
        "SELECT p.prosrc FROM pg_proc p \
         JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = 'pgtrickle_changes' AND p.proname = '{name}'"
    ))
    .await
}

async fn assert_capture_functions_do_not_notify(db: &E2eDb, source: &str, suffixes: &[&str]) {
    for suffix in suffixes {
        let body = capture_function_body(db, source, suffix).await;
        assert!(
            body.contains("INSERT INTO"),
            "{source}: expected an active {suffix} capture body: {body}"
        );
        assert!(
            !body.to_ascii_lowercase().contains("notify"),
            "{source}: {suffix} must not queue a commit-time NOTIFY: {body}"
        );
    }
}

/// All statement-level DML functions and TRUNCATE must avoid notifications.
#[tokio::test]
async fn test_wake_statement_cdc_triggers_do_not_notify() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE wake_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.create_st(
        "wake_st",
        "SELECT id, val FROM wake_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_capture_functions_do_not_notify(
        &db,
        "wake_src",
        &["ins_fn", "upd_fn", "del_fn", "truncate_fn"],
    )
    .await;
}

/// Legacy row-level CDC and its TRUNCATE trigger must also avoid notifications.
#[tokio::test]
async fn test_wake_row_cdc_triggers_do_not_notify() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE wake_row_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    // The GUC and creation must share a session.
    db.execute_seq(&[
        "SET pg_trickle.cdc_trigger_mode = 'row'",
        "SELECT pgtrickle.create_stream_table( \
         'wake_row_st', 'SELECT id, val FROM wake_row_src', '1m', 'DIFFERENTIAL')",
    ])
    .await;

    assert_capture_functions_do_not_notify(&db, "wake_row_src", &["fn", "truncate_fn"]).await;
}

/// Rebuild existing persisted PL/pgSQL bodies when upgrading a prior install.
/// A DML-only rebuild would leave the old notifying TRUNCATE body in pg_proc.
#[tokio::test]
async fn test_wake_rebuild_removes_legacy_notifications() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE wake_upgrade_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.create_st(
        "wake_upgrade_st",
        "SELECT id, val FROM wake_upgrade_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("wake_upgrade_src").await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT source_stable_name FROM pgtrickle.pgt_change_tracking \
             WHERE source_relid = {source_oid}"
        ))
        .await;

    // Simulate the persisted DML and TRUNCATE functions from the old version.
    for suffix in ["ins_fn", "truncate_fn"] {
        db.execute(&format!(
            "CREATE OR REPLACE FUNCTION \
             pgtrickle_changes.pg_trickle_cdc_{suffix}_{stable_name}() \
             RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER \
             SET search_path = pgtrickle_changes, pgtrickle, pg_catalog, pg_temp AS $$ \
             BEGIN PERFORM pg_notify('pgtrickle_wake', ''); RETURN NULL; END; $$"
        ))
        .await;
        let legacy = capture_function_body(&db, "wake_upgrade_src", suffix).await;
        assert!(
            legacy.contains("pg_notify"),
            "legacy fixture was not installed"
        );
    }

    db.execute("SELECT pgtrickle.rebuild_cdc_triggers()").await;
    assert_capture_functions_do_not_notify(
        &db,
        "wake_upgrade_src",
        &["ins_fn", "upd_fn", "del_fn", "truncate_fn"],
    )
    .await;

    // The repaired trigger still captures committed data after the rebuild.
    db.execute("INSERT INTO wake_upgrade_src VALUES (1, 'captured')")
        .await;
    db.refresh_st("wake_upgrade_st").await;
    db.assert_st_matches_query("wake_upgrade_st", "SELECT id, val FROM wake_upgrade_src")
        .await;
}

/// Verify polling actually propagates a committed CDC mutation, not just that
/// an initial FULL refresh was recorded in the history.
#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_wake_poll_only_scheduler_propagates_committed_change() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 200")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(sched_running, "scheduler did not start");

    db.execute("CREATE TABLE poll_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO poll_src VALUES (1, 'a')").await;
    db.create_st(
        "poll_st",
        "SELECT id, val FROM poll_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let initial = db
        .wait_for_condition(
            "initial poll refresh",
            "SELECT EXISTS (SELECT 1 FROM poll_st WHERE id = 1 AND val = 'a')",
            Duration::from_secs(30),
            Duration::from_millis(100),
        )
        .await;
    assert!(initial, "initial scheduled refresh did not complete");

    db.execute("UPDATE poll_src SET val = 'b' WHERE id = 1")
        .await;
    let updated = db
        .wait_for_condition(
            "poll-only CDC refresh",
            "SELECT EXISTS (SELECT 1 FROM poll_st WHERE id = 1 AND val = 'b')",
            Duration::from_secs(30),
            Duration::from_millis(100),
        )
        .await;
    assert!(updated, "committed UPDATE was not reflected by polling");
}
