//! E2E tests for WAL-based CDC (Change Data Capture) via logical replication.
//!
//! Validates:
//! - W1: Full WAL CDC lifecycle (trigger → transitioning → WAL)
//! - W1: INSERT, UPDATE, DELETE correctness through WAL CDC
//! - W1: Transition timeout and fallback to triggers
//! - W2: Automatic fallback on persistent poll errors (slot dropped)
//! - W2: Health check detects missing prerequisites
//! - W3: `auto` is the default cdc_mode (no explicit config needed)
//!
//! Prerequisites:
//! - `./tests/build_e2e_image.sh` (Docker image with wal_level=logical)
//! - Docker with `wal_level = logical` and `max_replication_slots = 10`

mod common;
mod e2e;

use e2e::E2eDb;
use std::time::Duration;

/// Helper: query the CDC mode for a source table's dependency.
async fn get_cdc_mode(db: &E2eDb, source_table: &str) -> String {
    let oid = db.table_oid(source_table).await;
    db.query_scalar(&format!(
        "SELECT d.cdc_mode FROM pgtrickle.pgt_dependencies d \
         WHERE d.source_relid = {oid} LIMIT 1"
    ))
    .await
}

/// Helper: check if a replication slot exists for a source table.
async fn slot_exists(db: &E2eDb, source_table: &str) -> bool {
    let oid = db.table_oid(source_table).await;
    let stable: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let slot_name = format!("pgtrickle_{stable}");
    db.query_scalar::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
    ))
    .await
}

/// Helper: check if a publication exists for a source table.
async fn publication_exists(db: &E2eDb, source_table: &str) -> bool {
    let oid = db.table_oid(source_table).await;
    let stable: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let pub_name = format!("pgtrickle_cdc_{stable}");
    db.query_scalar::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = '{pub_name}')"
    ))
    .await
}

/// Helper: wait until the CDC mode for a source transitions to the given value,
/// or timeout. Returns the final CDC mode.
async fn wait_for_cdc_mode(
    db: &E2eDb,
    source_table: &str,
    target: &str,
    timeout: Duration,
) -> String {
    let start = std::time::Instant::now();
    loop {
        let mode = get_cdc_mode(db, source_table).await;
        if mode.eq_ignore_ascii_case(target) {
            return mode;
        }
        if start.elapsed() > timeout {
            return mode;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ── W3: Auto is the default CDC mode ──────────────────────────────────

#[tokio::test]
async fn test_wal_auto_is_default_cdc_mode() {
    let db = E2eDb::new().await.with_extension().await;

    let cdc_mode = db.show_setting("pg_trickle.cdc_mode").await;
    assert_eq!(cdc_mode, "auto", "Default cdc_mode should be 'auto'");
}

#[tokio::test]
async fn test_wal_level_is_logical() {
    let db = E2eDb::new().await.with_extension().await;

    let wal_level: String = db.query_scalar("SHOW wal_level").await;
    assert_eq!(
        wal_level, "logical",
        "E2E container should have wal_level = logical"
    );
}

#[tokio::test]
async fn test_explicit_wal_override_transitions_even_with_global_trigger() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger'")
        .await;
    db.reload_config_and_wait().await;

    db.execute("CREATE TABLE wal_override_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_override_src REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_override_src VALUES (1, 'initial')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'wal_override_st',\
            query => $$SELECT id, val FROM wal_override_src$$,\
            schedule => '1s',\
            refresh_mode => 'DIFFERENTIAL',\
            cdc_mode => 'wal'\
        )",
    )
    .await;

    let requested_cdc_mode: String = db
        .query_scalar(
            "SELECT requested_cdc_mode FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'wal_override_st'",
        )
        .await;
    assert_eq!(requested_cdc_mode, "wal");

    let final_mode =
        wait_for_cdc_mode(&db, "wal_override_src", "WAL", Duration::from_secs(60)).await;
    assert_eq!(
        final_mode, "WAL",
        "Explicit wal override should transition to WAL mode"
    );
}

#[tokio::test]
async fn test_explicit_trigger_override_blocks_wal_transition() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wal_trigger_override_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_trigger_override_src REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_trigger_override_src VALUES (1, 'initial')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'wal_trigger_override_st',\
            query => $$SELECT id, val FROM wal_trigger_override_src$$,\
            schedule => '1s',\
            refresh_mode => 'DIFFERENTIAL',\
            cdc_mode => 'trigger'\
        )",
    )
    .await;

    let requested_cdc_mode: String = db
        .query_scalar(
            "SELECT requested_cdc_mode FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'wal_trigger_override_st'",
        )
        .await;
    assert_eq!(requested_cdc_mode, "trigger");

    wait_for_cdc_mode(
        &db,
        "wal_trigger_override_src",
        "TRIGGER",
        Duration::from_secs(30),
    )
    .await;

    let mode = get_cdc_mode(&db, "wal_trigger_override_src").await;
    assert_eq!(
        mode, "TRIGGER",
        "Explicit trigger override should keep trigger CDC"
    );
    assert!(
        !slot_exists(&db, "wal_trigger_override_src").await,
        "Explicit trigger override should prevent WAL slot creation"
    );
}

// ── W1: WAL Transition Lifecycle ──────────────────────────────────────

/// Test the full TRIGGER → TRANSITIONING → WAL lifecycle.
///
/// With `cdc_mode = 'auto'` (default) and `wal_level = logical`, the
/// scheduler should automatically start the transition and complete it
/// once the WAL decoder catches up.
#[tokio::test]
async fn test_wal_transition_lifecycle() {
    // new_on_postgres_db() now creates an isolated per-test database while
    // still resetting server-level scheduler GUCs before the test starts.
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wal_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_src REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_src VALUES (1, 'initial')")
        .await;

    db.create_st(
        "wal_lifecycle_st",
        "SELECT id, val FROM wal_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Initial state should be TRIGGER (transition hasn't started yet)
    let initial_mode = get_cdc_mode(&db, "wal_src").await;
    assert_eq!(initial_mode, "TRIGGER", "Should start in TRIGGER mode");

    // Wait for the scheduler to transition to WAL
    // The scheduler runs every 1s and the transition needs to:
    // 1. Start transition (create slot + publication)
    // 2. Poll WAL to catch up
    // 3. Complete transition (verify lag < 64KB)
    let final_mode = wait_for_cdc_mode(&db, "wal_src", "WAL", Duration::from_secs(60)).await;
    assert_eq!(
        final_mode, "WAL",
        "Should transition to WAL mode (got: {final_mode})"
    );

    // Verify infrastructure was created
    assert!(
        slot_exists(&db, "wal_src").await,
        "Replication slot should exist"
    );
    assert!(
        publication_exists(&db, "wal_src").await,
        "Publication should exist"
    );
}

/// Test that INSERTs are captured correctly through WAL-based CDC.
#[tokio::test]
async fn test_wal_cdc_captures_insert() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wal_ins (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_ins REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_ins VALUES (1, 'a')").await;

    db.create_st(
        "wal_ins_st",
        "SELECT id, val FROM wal_ins",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.wal_ins_st").await, 1);

    // Wait for WAL transition
    let mode = wait_for_cdc_mode(&db, "wal_ins", "WAL", Duration::from_secs(60)).await;
    assert_eq!(mode, "WAL", "Should transition to WAL mode");

    // Allow the WAL slot to stabilise: the first 1-2 scheduler ticks after a
    // new slot is created can encounter transient poll errors (slot briefly in
    // use or backend SPI context recovering).  Wait for a few ticks to clear
    // before injecting DML so the DML changes are not in-flight when an error
    // counter could trigger premature fallback to trigger mode.
    let _ = db
        .wait_for_auto_refresh("wal_ins_st", Duration::from_secs(15))
        .await;

    // Insert new rows — WAL decoder should capture them
    db.execute("INSERT INTO wal_ins VALUES (2, 'b'), (3, 'c')")
        .await;

    // Wait for the scheduler to do a refresh
    let refreshed = db
        .wait_for_auto_refresh("wal_ins_st", Duration::from_secs(30))
        .await;
    assert!(refreshed, "Scheduler should trigger a refresh");

    assert_eq!(
        db.count("public.wal_ins_st").await,
        3,
        "WAL CDC should capture all INSERTs"
    );

    // Verify data correctness — not just count — through the WAL decoding pipeline
    db.assert_st_matches_query("public.wal_ins_st", "SELECT id, val FROM wal_ins")
        .await;
}

/// Test that UPDATEs are captured correctly through WAL-based CDC.
#[tokio::test]
async fn test_wal_cdc_captures_update() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wal_upd (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_upd REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_upd VALUES (1, 'old')").await;

    db.create_st(
        "wal_upd_st",
        "SELECT id, val FROM wal_upd",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let mode = wait_for_cdc_mode(&db, "wal_upd", "WAL", Duration::from_secs(60)).await;
    assert_eq!(mode, "WAL", "Should transition to WAL mode");

    // Allow the WAL slot to stabilise before performing DML — wait until the
    // replication slot is visible in pg_replication_slots.
    let slot_ready = {
        let oid = db.table_oid("wal_upd").await;
        let stable: String = db
            .query_scalar(&format!(
                "SELECT pgtrickle.source_stable_name({}::oid)",
                oid
            ))
            .await;
        let slot_name = format!("pgtrickle_{stable}");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let exists: bool = db
                .query_scalar(&format!(
                    "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
                ))
                .await;
            if exists {
                break true;
            }
            if std::time::Instant::now() > deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    assert!(
        slot_ready,
        "WAL replication slot should be visible before performing DML"
    );

    db.execute("UPDATE wal_upd SET val = 'new' WHERE id = 1")
        .await;

    // Use a generous 60s timeout: on emulated environments (e.g. x86_64 container
    // on Apple Silicon via Rosetta) the WAL poll → change-buffer → differential
    // refresh cycle can take longer than the original 30s budget.
    let refreshed = db
        .wait_for_auto_refresh("wal_upd_st", Duration::from_secs(60))
        .await;
    assert!(refreshed, "Scheduler should trigger a refresh");

    let val: String = db
        .query_scalar("SELECT val FROM public.wal_upd_st WHERE id = 1")
        .await;
    assert_eq!(val, "new", "UPDATE should be reflected via WAL CDC");

    // Verify full multiset correctness — ensures no spurious rows or wrong values
    db.assert_st_matches_query("public.wal_upd_st", "SELECT id, val FROM wal_upd")
        .await;
}

/// Test that DELETEs are captured correctly through WAL-based CDC.
#[tokio::test]
async fn test_wal_cdc_captures_delete() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wal_del (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_del REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_del VALUES (1, 'keep'), (2, 'remove')")
        .await;

    db.create_st(
        "wal_del_st",
        "SELECT id, val FROM wal_del",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.wal_del_st").await, 2);

    let mode = wait_for_cdc_mode(&db, "wal_del", "WAL", Duration::from_secs(60)).await;
    assert_eq!(mode, "WAL", "Should transition to WAL mode");

    // Allow the WAL slot to stabilise before performing DML — wait until the
    // replication slot is visible in pg_replication_slots.
    let slot_ready = {
        let oid = db.table_oid("wal_del").await;
        let stable: String = db
            .query_scalar(&format!(
                "SELECT pgtrickle.source_stable_name({}::oid)",
                oid
            ))
            .await;
        let slot_name = format!("pgtrickle_{stable}");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let exists: bool = db
                .query_scalar(&format!(
                    "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
                ))
                .await;
            if exists {
                break true;
            }
            if std::time::Instant::now() > deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    assert!(
        slot_ready,
        "WAL replication slot should be visible before performing DML"
    );

    db.execute("DELETE FROM wal_del WHERE id = 2").await;

    let refreshed = db
        .wait_for_auto_refresh("wal_del_st", Duration::from_secs(30))
        .await;
    assert!(refreshed, "Scheduler should trigger a refresh");

    assert_eq!(
        db.count("public.wal_del_st").await,
        1,
        "DELETE should be reflected via WAL CDC"
    );

    // Verify data correctness — only the kept row should remain
    db.assert_st_matches_query("public.wal_del_st", "SELECT id, val FROM wal_del")
        .await;
}

// ── W1: Transition with trigger-only fallback ─────────────────────────

/// When cdc_mode = 'trigger', no WAL transition should occur even if
/// wal_level = logical.
#[tokio::test]
async fn test_trigger_mode_no_wal_transition() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    let default_cdc_mode = db.show_setting("pg_trickle.cdc_mode").await;

    // Force trigger-only mode
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;

    db.execute("CREATE TABLE trig_only (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO trig_only VALUES (1, 'a')").await;

    db.create_st(
        "trig_only_st",
        "SELECT id, val FROM trig_only",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Confirm mode has NOT transitioned away from TRIGGER by polling for a
    // short window — if WAL transition were to happen it would appear within
    // a few scheduler ticks (scheduler_interval_ms is 100ms by default).
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    let mode = loop {
        let m = get_cdc_mode(&db, "trig_only").await;
        if m.to_uppercase() != "TRIGGER" || std::time::Instant::now() > deadline {
            break m; // unexpected transition — surface the wrong mode for assertion
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(
        mode, "TRIGGER",
        "cdc_mode='trigger' should prevent WAL transition"
    );

    // No replication slot should exist
    assert!(
        !slot_exists(&db, "trig_only").await,
        "No slot should be created in trigger-only mode"
    );

    // Reset for other tests
    db.alter_system_reset_and_wait("pg_trickle.cdc_mode", &default_cdc_mode)
        .await;
}

// ── W2: Fallback hardening ────────────────────────────────────────────

/// When a replication slot is externally dropped while in WAL mode,
/// the health check should detect it and fall back to triggers.
#[tokio::test]
async fn test_wal_fallback_on_missing_slot() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wal_fb (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_fb REPLICA IDENTITY FULL").await;
    db.execute("INSERT INTO wal_fb VALUES (1, 'x')").await;

    db.create_st(
        "wal_fb_st",
        "SELECT id, val FROM wal_fb",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Wait for WAL transition to complete
    let mode = wait_for_cdc_mode(&db, "wal_fb", "WAL", Duration::from_secs(60)).await;
    assert_eq!(mode, "WAL", "Should be in WAL mode before test");

    // Externally drop the replication slot to simulate infrastructure failure
    let oid = db.table_oid("wal_fb").await;
    let stable: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let slot_name = format!("pgtrickle_{stable}");
    db.execute(&format!("SELECT pg_drop_replication_slot('{slot_name}')"))
        .await;

    // Pin cdc_mode to 'trigger' so the scheduler doesn't immediately
    // re-promote back to WAL after fallback. In 'auto' mode the scheduler
    // would re-create the slot within one tick, making TRIGGER unobservable.
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;

    // Wait for the health check / poll error to trigger fallback
    let fallback_mode = wait_for_cdc_mode(&db, "wal_fb", "TRIGGER", Duration::from_secs(60)).await;
    assert_eq!(
        fallback_mode, "TRIGGER",
        "Should fall back to TRIGGER after slot is dropped"
    );

    // Verify the stream table still works — insert data and refresh
    db.execute("INSERT INTO wal_fb VALUES (2, 'y')").await;

    let refreshed = db
        .wait_for_auto_refresh("wal_fb_st", Duration::from_secs(15))
        .await;
    assert!(refreshed, "Trigger-based CDC should resume after fallback");
    assert_eq!(db.count("public.wal_fb_st").await, 2);

    // Verify data correctness after fallback — no rows should be lost or duplicated
    db.assert_st_matches_query("public.wal_fb_st", "SELECT id, val FROM wal_fb")
        .await;
}

/// Cleanup on DROP: dropping a stream table in WAL mode should clean up
/// the replication slot and publication.
#[tokio::test]
async fn test_wal_cleanup_on_drop() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wal_drop (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_drop REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_drop VALUES (1, 'a')").await;

    db.create_st(
        "wal_drop_st",
        "SELECT id, val FROM wal_drop",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    wait_for_cdc_mode(&db, "wal_drop", "WAL", Duration::from_secs(60)).await;

    let oid = db.table_oid("wal_drop").await;
    let stable: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let slot_name = format!("pgtrickle_{stable}");
    let pub_name = format!("pgtrickle_cdc_{stable}");

    // Verify slot + publication exist before drop
    assert!(slot_exists(&db, "wal_drop").await);
    assert!(publication_exists(&db, "wal_drop").await);

    // Drop the stream table
    db.drop_st("wal_drop_st").await;

    // Verify slot and publication were cleaned up
    let slot_gone: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ))
        .await;
    let pub_gone: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = '{pub_name}')"
        ))
        .await;
    assert!(slot_gone, "Replication slot should be dropped on ST drop");
    assert!(pub_gone, "Publication should be dropped on ST drop");
}

/// Keyless tables should stay on triggers (WAL mode requires PK for pk_hash).
#[tokio::test]
async fn test_wal_keyless_table_stays_on_triggers() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // Table without primary key — need REPLICA IDENTITY FULL for cdc_mode='auto'
    db.execute("CREATE TABLE wal_keyless (val TEXT)").await;
    db.execute("ALTER TABLE wal_keyless REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_keyless VALUES ('a'), ('b')")
        .await;

    db.create_st(
        "wal_keyless_st",
        "SELECT val FROM wal_keyless",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Confirm mode has NOT transitioned away from TRIGGER by polling for a
    // short window — keyless tables (no PK) cannot transition to WAL.
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    let mode = loop {
        let m = get_cdc_mode(&db, "wal_keyless").await;
        if m.to_uppercase() != "TRIGGER" || std::time::Instant::now() > deadline {
            break m;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(
        mode, "TRIGGER",
        "Keyless table should stay on TRIGGER mode (WAL requires PK)"
    );
}

// ── EC-18: Auto CDC mode stuck — health visibility ────────────────────

/// EC-18: When auto CDC is stuck on TRIGGER (because a table has no PK),
/// check_cdc_health() should report the source as TRIGGER mode so the
/// operator can diagnose why WAL hasn't activated.
#[tokio::test]
async fn test_ec18_check_cdc_health_shows_trigger_for_stuck_auto() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // Keyless table with REPLICA IDENTITY FULL — auto CDC can't upgrade to WAL
    db.execute("CREATE TABLE ec18_src (val TEXT)").await;
    db.execute("ALTER TABLE ec18_src REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO ec18_src VALUES ('a')").await;

    db.create_st("ec18_st", "SELECT val FROM ec18_src", "1s", "DIFFERENTIAL")
        .await;

    // Wait for the scheduler to attempt WAL transition (will stay in TRIGGER for keyless src)
    wait_for_cdc_mode(&db, "ec18_src", "TRIGGER", Duration::from_secs(30)).await;

    // check_cdc_health() should show TRIGGER mode for this source
    let cdc_mode: String = db
        .query_scalar(
            "SELECT cdc_mode FROM pgtrickle.check_cdc_health() \
             WHERE source_table = 'ec18_src'",
        )
        .await;
    assert_eq!(
        cdc_mode, "TRIGGER",
        "check_cdc_health() should report TRIGGER for keyless auto-CDC source"
    );

    // No alert should fire for a healthy TRIGGER-mode source
    let alert_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.check_cdc_health() \
             WHERE source_table = 'ec18_src' AND alert IS NOT NULL",
        )
        .await;
    assert_eq!(
        alert_count, 0,
        "TRIGGER-mode source should not have a CDC health alert"
    );
}

/// EC-18: health_check() should not report errors for sources stuck on
/// TRIGGER mode via auto CDC — the system is functioning correctly, just
/// not using WAL.
#[tokio::test]
async fn test_ec18_health_check_ok_with_trigger_auto_sources() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE ec18_hc (val TEXT)").await;
    db.execute("ALTER TABLE ec18_hc REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO ec18_hc VALUES ('x')").await;

    db.create_st(
        "ec18_hc_st",
        "SELECT val FROM ec18_hc",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Wait for at least one refresh to complete before checking health.
    common::wait_for_first_refresh(&db.pool, "wal_auto_trig_st", Duration::from_secs(30)).await;

    // health_check() should not have ERROR severity for stream tables
    // that are ACTIVE but using TRIGGER mode
    let error_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.health_check() \
             WHERE check_name = 'error_tables' AND severity = 'ERROR'",
        )
        .await;
    assert_eq!(
        error_count, 0,
        "health_check() should not flag TRIGGER-mode auto-CDC sources as errors"
    );
}

// ── EC-34: Missing WAL slot detection via health check ────────────────

/// EC-34: When a WAL replication slot is externally dropped,
/// check_cdc_health() should surface a 'replication_slot_missing' alert
/// before the automatic fallback to TRIGGER kicks in.
#[tokio::test]
async fn test_ec34_check_cdc_health_detects_missing_slot() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE ec34_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE ec34_src REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO ec34_src VALUES (1, 'a')").await;

    db.create_st(
        "ec34_st",
        "SELECT id, val FROM ec34_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Wait for WAL transition to complete
    let mode = wait_for_cdc_mode(&db, "ec34_src", "WAL", Duration::from_secs(60)).await;
    assert_eq!(mode, "WAL", "Should be in WAL mode before dropping slot");

    // Drop the replication slot externally to simulate backup/restore
    let oid = db.table_oid("ec34_src").await;
    let stable: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let slot_name = format!("pgtrickle_{stable}");
    db.execute(&format!("SELECT pg_drop_replication_slot('{slot_name}')"))
        .await;

    // Immediately check CDC health — before the scheduler's fallback runs.
    // The check should detect the missing slot.
    let alert: String = db
        .query_scalar(
            "SELECT coalesce(alert, '') FROM pgtrickle.check_cdc_health() \
             WHERE source_table = 'ec34_src'",
        )
        .await;
    assert_eq!(
        alert, "replication_slot_missing",
        "check_cdc_health() should report replication_slot_missing after slot drop"
    );

    // Prevent the scheduler from re-promoting back to WAL after fallback.
    // Without this, the auto CDC mode immediately re-creates the slot and
    // transitions back to WAL, making the TRIGGER state unobservable.
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;

    let fallback_mode =
        wait_for_cdc_mode(&db, "ec34_src", "TRIGGER", Duration::from_secs(60)).await;
    assert_eq!(
        fallback_mode, "TRIGGER",
        "Should fall back to TRIGGER after slot is dropped"
    );

    // Insert data and verify refresh still works post-fallback
    db.execute("INSERT INTO ec34_src VALUES (2, 'b')").await;
    let refreshed = db
        .wait_for_auto_refresh("ec34_st", Duration::from_secs(15))
        .await;
    assert!(refreshed, "Trigger CDC should resume after fallback");
    assert_eq!(db.count("public.ec34_st").await, 2);
}

// ── EC-19: WAL + keyless without REPLICA IDENTITY FULL ─────────────────

/// EC-19: Creating a stream table with cdc_mode='wal' on a keyless table
/// without REPLICA IDENTITY FULL must be rejected at creation time to
/// prevent silent data corruption (WAL cannot send old-row values).
/// Requires wal_level=logical (full E2E harness).
#[tokio::test]
async fn test_ec19_wal_keyless_without_replica_identity_full_rejected() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // Keyless table with default REPLICA IDENTITY (not FULL)
    db.execute("CREATE TABLE ec19_keyless (val TEXT)").await;
    db.execute("INSERT INTO ec19_keyless VALUES ('a')").await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                name => 'ec19_keyless_st',\
                query => $$SELECT val FROM ec19_keyless$$,\
                schedule => '1m',\
                refresh_mode => 'DIFFERENTIAL',\
                cdc_mode => 'wal'\
            )",
        )
        .await;

    assert!(
        result.is_err(),
        "WAL CDC on keyless table without REPLICA IDENTITY FULL must be rejected"
    );

    let error = format!("{}", result.unwrap_err());
    assert!(
        error.contains("REPLICA IDENTITY FULL"),
        "Error should mention REPLICA IDENTITY FULL, got: {error}"
    );
}

/// EC-19: After setting REPLICA IDENTITY FULL, the same keyless table
/// should be accepted with cdc_mode='wal'.
/// Requires wal_level=logical (full E2E harness).
#[tokio::test]
async fn test_ec19_wal_keyless_with_replica_identity_full_accepted() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE ec19_ri_full (val TEXT)").await;
    db.execute("ALTER TABLE ec19_ri_full REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO ec19_ri_full VALUES ('a')").await;

    // Should succeed — REPLICA IDENTITY FULL is set, explicit WAL mode
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                name => 'ec19_ri_full_st',\
                query => $$SELECT val FROM ec19_ri_full$$,\
                schedule => '1m',\
                refresh_mode => 'DIFFERENTIAL',\
                cdc_mode => 'wal'\
            )",
        )
        .await;

    assert!(
        result.is_ok(),
        "WAL CDC on keyless table WITH REPLICA IDENTITY FULL should succeed: {:?}",
        result.unwrap_err()
    );
}

// ── T-A41-3: WAL transition eligibility recheck at commit ────────────────────

/// T-A41-3a: Dropping the primary key during a WAL transition must cause the
/// transition to abort and the source to fall back to Trigger CDC mode.
///
/// This test exercises the A41-3 eligibility recheck: after the background
/// worker starts the TRANSITIONING phase but before it commits, we drop the PK
/// on the source table.  The recheck detects the missing PK and should reset
/// the catalog to Trigger mode without leaving a dangling replication slot.
///
/// Because the actual transition runs asynchronously in the background worker
/// we cannot synchronise perfectly, but we can:
/// 1. Create a WAL-mode stream table.
/// 2. Wait for transition to begin (CDC mode becomes TRANSITIONING or WAL).
/// 3. If already WAL: drop PK, which the heartbeat/health-check should detect.
/// 4. Otherwise: drop PK during TRANSITIONING, let the recheck fire.
/// 5. Assert final CDC mode is NOT WAL (reverted to trigger-based mode).
#[tokio::test]
async fn test_wal_transition_pk_drop_falls_back_to_trigger() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wal_pk_drop_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_pk_drop_src REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_pk_drop_src VALUES (1, 'a')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'wal_pk_drop_st',\
            query => $$SELECT id, val FROM wal_pk_drop_src$$,\
            schedule => '1m',\
            refresh_mode => 'DIFFERENTIAL',\
            cdc_mode => 'wal'\
        )",
    )
    .await;

    // Wait a short moment for the transition to start — poll until the WAL slot
    // is visible, which confirms the background worker has begun the transition.
    let _ = {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if slot_exists(&db, "wal_pk_drop_src").await {
                break true;
            }
            if std::time::Instant::now() > deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };

    // Drop the primary key — this makes the source ineligible for WAL CDC.
    db.execute("ALTER TABLE wal_pk_drop_src DROP CONSTRAINT wal_pk_drop_src_pkey")
        .await;

    // Give the background worker time to run the eligibility recheck and
    // update the catalog (up to 30 s).
    let final_mode =
        wait_for_cdc_mode(&db, "wal_pk_drop_src", "trigger", Duration::from_secs(30)).await;

    assert!(
        final_mode.eq_ignore_ascii_case("trigger"),
        "After PK drop the CDC mode should revert to trigger, got: {final_mode}"
    );

    // Verify no dangling replication slot was left behind.
    let slot_left = slot_exists(&db, "wal_pk_drop_src").await;
    assert!(
        !slot_left,
        "No replication slot should remain after WAL transition aborted due to PK drop"
    );
}

/// T-A41-3b: Dropping REPLICA IDENTITY FULL during transition must abort to
/// Trigger mode (the recheck verifies replica identity = 'full').
#[tokio::test]
async fn test_wal_transition_replica_identity_drop_falls_back_to_trigger() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wal_ri_drop_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("ALTER TABLE wal_ri_drop_src REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO wal_ri_drop_src VALUES (1, 'b')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'wal_ri_drop_st',\
            query => $$SELECT id, val FROM wal_ri_drop_src$$,\
            schedule => '1m',\
            refresh_mode => 'DIFFERENTIAL',\
            cdc_mode => 'wal'\
        )",
    )
    .await;

    // Brief pause before changing REPLICA IDENTITY — poll until the WAL slot
    // is visible, confirming the background worker has begun the WAL transition.
    let _ = {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if slot_exists(&db, "wal_ri_drop_src").await {
                break true;
            }
            if std::time::Instant::now() > deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };

    // Reset replica identity to default — this breaks WAL CDC eligibility.
    db.execute("ALTER TABLE wal_ri_drop_src REPLICA IDENTITY DEFAULT")
        .await;

    let final_mode =
        wait_for_cdc_mode(&db, "wal_ri_drop_src", "trigger", Duration::from_secs(30)).await;

    assert!(
        final_mode.eq_ignore_ascii_case("trigger"),
        "After REPLICA IDENTITY reset the CDC mode should revert to trigger, got: {final_mode}"
    );
}
