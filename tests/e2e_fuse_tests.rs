//! FUSE-6: E2E tests for the fuse circuit breaker.
//!
//! Tests cover:
//!   - Normal baseline: fuse off, no blow on normal change volumes
//!   - Spike → blow: large change volume exceeds ceiling → fuse blows
//!   - reset_fuse with action='apply': re-arm and process changes
//!   - reset_fuse with action='reinitialize': re-arm and force full refresh
//!   - reset_fuse with action='skip_changes': re-arm and discard changes
//!   - fuse_status() introspection function
//!   - alter_stream_table fuse parameter validation

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helpers ────────────────────────────────────────────────────────────────

async fn setup_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.fuse_default_ceiling = 0")
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

async fn wait_for_fuse_blown(db: &E2eDb, pgt_name: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        let state: String = db
            .query_scalar(&format!(
                "SELECT fuse_state FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = '{pgt_name}'"
            ))
            .await;
        if state == "blown" {
            return true;
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Baseline: fuse_mode='off' (default) — no fuse blow even on large change volumes.
#[tokio::test]
async fn test_fuse_off_no_blow() {
    let db = E2eDb::new().await.with_extension().await;
    setup_fast_scheduler(&db).await;

    db.execute("CREATE TABLE src_fuse_off (id int PRIMARY KEY, val text)")
        .await;
    db.execute("INSERT INTO src_fuse_off SELECT g, 'row-' || g FROM generate_series(1, 100) g")
        .await;

    // Create ST with fuse off (default)
    db.create_st(
        "st_fuse_off",
        "SELECT id, val FROM src_fuse_off",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Wait for initial population
    let populated = wait_for_scheduler_refresh(&db, "st_fuse_off", Duration::from_secs(30)).await;
    assert!(populated, "ST should be populated within 30s");

    // Insert a large batch — should NOT blow the fuse (fuse_mode='off')
    db.execute("INSERT INTO src_fuse_off SELECT g, 'new-' || g FROM generate_series(101, 10000) g")
        .await;

    // Wait a bit for scheduler to process
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify fuse state is still 'armed' and ST is ACTIVE
    let state: String = db
        .query_scalar(
            "SELECT fuse_state FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_fuse_off'",
        )
        .await;
    assert_eq!(state, "armed", "fuse should remain armed when mode=off");

    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_fuse_off'",
        )
        .await;
    assert_eq!(status, "ACTIVE", "ST should remain ACTIVE when fuse=off");
}

/// Spike → blow: enable fuse, insert rows exceeding ceiling → fuse blows.
#[tokio::test]
async fn test_fuse_spike_blows_fuse() {
    let db = E2eDb::new().await.with_extension().await;
    setup_fast_scheduler(&db).await;

    db.execute("CREATE TABLE src_fuse_blow (id int PRIMARY KEY, val text)")
        .await;
    db.execute("INSERT INTO src_fuse_blow SELECT g, 'row-' || g FROM generate_series(1, 10) g")
        .await;

    // Create ST then enable fuse with low ceiling
    db.create_st(
        "st_fuse_blow",
        "SELECT id, val FROM src_fuse_blow",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Wait for initial population
    let populated = wait_for_scheduler_refresh(&db, "st_fuse_blow", Duration::from_secs(30)).await;
    assert!(populated, "ST should be populated within 30s");

    // Enable fuse with a ceiling of 50 rows
    db.execute(
        "SELECT pgtrickle.alter_stream_table('st_fuse_blow', fuse => 'on', fuse_ceiling => 50)",
    )
    .await;

    // Insert 200 rows — well above the 50-row ceiling
    db.execute("INSERT INTO src_fuse_blow SELECT g, 'spike-' || g FROM generate_series(11, 210) g")
        .await;

    // Wait for fuse to blow
    let blown = wait_for_fuse_blown(&db, "st_fuse_blow", Duration::from_secs(30)).await;
    assert!(blown, "fuse should have blown within 30s");

    // Verify ST is SUSPENDED
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_fuse_blow'",
        )
        .await;
    assert_eq!(
        status, "SUSPENDED",
        "ST should be SUSPENDED after fuse blow"
    );

    // Verify blow_reason is set
    let reason: String = db
        .query_scalar(
            "SELECT blow_reason FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_fuse_blow'",
        )
        .await;
    assert!(
        reason.contains("exceeded ceiling"),
        "blow_reason should explain the ceiling breach, got: {}",
        reason
    );
}

/// Reset fuse with action='apply': re-arm, changes processed on next tick.
#[tokio::test]
async fn test_fuse_reset_apply() {
    let db = E2eDb::new().await.with_extension().await;
    setup_fast_scheduler(&db).await;

    db.execute("CREATE TABLE src_fuse_apply (id int PRIMARY KEY, val text)")
        .await;
    db.execute("INSERT INTO src_fuse_apply SELECT g, 'row-' || g FROM generate_series(1, 10) g")
        .await;

    db.create_st(
        "st_fuse_apply",
        "SELECT id, val FROM src_fuse_apply",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let populated = wait_for_scheduler_refresh(&db, "st_fuse_apply", Duration::from_secs(30)).await;
    assert!(populated, "ST should be populated within 30s");

    // Enable fuse with low ceiling and trigger blow
    db.execute(
        "SELECT pgtrickle.alter_stream_table('st_fuse_apply', fuse => 'on', fuse_ceiling => 20)",
    )
    .await;
    db.execute(
        "INSERT INTO src_fuse_apply SELECT g, 'spike-' || g FROM generate_series(11, 100) g",
    )
    .await;

    let blown = wait_for_fuse_blown(&db, "st_fuse_apply", Duration::from_secs(30)).await;
    assert!(blown, "fuse should have blown");

    // Now raise the ceiling so it won't blow again, and reset with 'apply'
    db.execute("SELECT pgtrickle.alter_stream_table('st_fuse_apply', fuse_ceiling => 10000)")
        .await;
    db.execute("SELECT pgtrickle.reset_fuse('st_fuse_apply', 'apply')")
        .await;

    // Verify ST resumed and fuse is re-armed
    let state: String = db
        .query_scalar(
            "SELECT fuse_state FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_fuse_apply'",
        )
        .await;
    assert_eq!(state, "armed", "fuse should be re-armed after reset");

    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_fuse_apply'",
        )
        .await;
    assert_eq!(status, "ACTIVE", "ST should be ACTIVE after reset");
}

/// Reset fuse with action='skip_changes': re-arm and discard pending changes.
#[tokio::test]
async fn test_fuse_reset_skip_changes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_fast_scheduler(&db).await;

    db.execute("CREATE TABLE src_fuse_skip (id int PRIMARY KEY, val text)")
        .await;
    db.execute("INSERT INTO src_fuse_skip SELECT g, 'row-' || g FROM generate_series(1, 10) g")
        .await;

    db.create_st(
        "st_fuse_skip",
        "SELECT id, val FROM src_fuse_skip",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let populated = wait_for_scheduler_refresh(&db, "st_fuse_skip", Duration::from_secs(30)).await;
    assert!(populated, "ST should be populated within 30s");

    // Enable fuse and trigger blow
    db.execute(
        "SELECT pgtrickle.alter_stream_table('st_fuse_skip', fuse => 'on', fuse_ceiling => 20)",
    )
    .await;
    db.execute("INSERT INTO src_fuse_skip SELECT g, 'spike-' || g FROM generate_series(11, 100) g")
        .await;

    let blown = wait_for_fuse_blown(&db, "st_fuse_skip", Duration::from_secs(30)).await;
    assert!(blown, "fuse should have blown");

    // Reset with 'skip_changes' — this drains buffer
    db.execute("SELECT pgtrickle.reset_fuse('st_fuse_skip', 'skip_changes')")
        .await;

    // Verify fuse re-armed
    let state: String = db
        .query_scalar(
            "SELECT fuse_state FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_fuse_skip'",
        )
        .await;
    assert_eq!(state, "armed");
}

/// Reset fuse with action='reinitialize': re-arm and mark needs_reinit.
#[tokio::test]
async fn test_fuse_reset_reinitialize() {
    let db = E2eDb::new().await.with_extension().await;
    setup_fast_scheduler(&db).await;

    db.execute("CREATE TABLE src_fuse_reinit (id int PRIMARY KEY, val text)")
        .await;
    db.execute("INSERT INTO src_fuse_reinit SELECT g, 'row-' || g FROM generate_series(1, 10) g")
        .await;

    db.create_st(
        "st_fuse_reinit",
        "SELECT id, val FROM src_fuse_reinit",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let populated =
        wait_for_scheduler_refresh(&db, "st_fuse_reinit", Duration::from_secs(30)).await;
    assert!(populated, "ST should be populated within 30s");

    // Enable fuse and trigger blow
    db.execute(
        "SELECT pgtrickle.alter_stream_table('st_fuse_reinit', fuse => 'on', fuse_ceiling => 20)",
    )
    .await;
    db.execute(
        "INSERT INTO src_fuse_reinit SELECT g, 'spike-' || g FROM generate_series(11, 100) g",
    )
    .await;

    let blown = wait_for_fuse_blown(&db, "st_fuse_reinit", Duration::from_secs(30)).await;
    assert!(blown, "fuse should have blown");

    // Raise ceiling to prevent re-blow, then reset with reinitialize.
    // Pause the scheduler first so it cannot process needs_reinit before we assert it.
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = off")
        .await;
    db.reload_config_and_wait().await;

    db.execute("SELECT pgtrickle.alter_stream_table('st_fuse_reinit', fuse_ceiling => 100000)")
        .await;
    db.execute("SELECT pgtrickle.reset_fuse('st_fuse_reinit', 'reinitialize')")
        .await;

    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_fuse_reinit'",
        )
        .await;
    assert!(
        needs_reinit,
        "needs_reinit should be true after reinitialize reset"
    );

    // Re-enable scheduler.
    db.execute("ALTER SYSTEM RESET pg_trickle.enabled").await;
    db.reload_config_and_wait().await;
}

/// fuse_status() introspection function returns expected rows.
#[tokio::test]
async fn test_fuse_status_introspection() {
    let db = E2eDb::new().await.with_extension().await;
    setup_fast_scheduler(&db).await;

    db.execute("CREATE TABLE src_fuse_status (id int PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_fuse_status VALUES (1)").await;

    db.create_st(
        "st_fuse_status",
        "SELECT id FROM src_fuse_status",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let populated =
        wait_for_scheduler_refresh(&db, "st_fuse_status", Duration::from_secs(30)).await;
    assert!(populated, "ST should be populated within 30s");

    // Check fuse_status() default output (fuse off)
    let fuse_mode: String = db
        .query_scalar(
            "SELECT fuse_mode FROM pgtrickle.fuse_status() WHERE stream_table LIKE '%st_fuse_status%'",
        )
        .await;
    assert_eq!(fuse_mode, "off", "default fuse mode should be off");

    // Enable fuse and check again
    db.execute(
        "SELECT pgtrickle.alter_stream_table('st_fuse_status', fuse => 'on', fuse_ceiling => 500)",
    )
    .await;

    let fuse_mode: String = db
        .query_scalar(
            "SELECT fuse_mode FROM pgtrickle.fuse_status() WHERE stream_table LIKE '%st_fuse_status%'",
        )
        .await;
    assert_eq!(fuse_mode, "on");

    let ceiling: i64 = db
        .query_scalar(
            "SELECT fuse_ceiling FROM pgtrickle.fuse_status() WHERE stream_table LIKE '%st_fuse_status%'",
        )
        .await;
    assert_eq!(ceiling, 500);
}

/// Validate alter_stream_table rejects invalid fuse parameter values.
#[tokio::test]
async fn test_fuse_alter_validation() {
    let db = E2eDb::new().await.with_extension().await;
    setup_fast_scheduler(&db).await;

    db.execute("CREATE TABLE src_fuse_val (id int PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_fuse_val VALUES (1)").await;

    db.create_st(
        "st_fuse_val",
        "SELECT id FROM src_fuse_val",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let populated = wait_for_scheduler_refresh(&db, "st_fuse_val", Duration::from_secs(30)).await;
    assert!(populated, "ST should be populated within 30s");

    // Invalid fuse mode
    let err = db
        .try_execute("SELECT pgtrickle.alter_stream_table('st_fuse_val', fuse => 'invalid')")
        .await;
    assert!(err.is_err(), "invalid fuse mode should error");

    // Invalid ceiling (negative)
    let err = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table('st_fuse_val', fuse => 'on', fuse_ceiling => -1)",
        )
        .await;
    assert!(err.is_err(), "negative fuse_ceiling should error");
}

/// Reset fuse when fuse is not blown should error.
#[tokio::test]
async fn test_fuse_reset_not_blown_errors() {
    let db = E2eDb::new().await.with_extension().await;
    setup_fast_scheduler(&db).await;

    db.execute("CREATE TABLE src_fuse_notblown (id int PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_fuse_notblown VALUES (1)").await;

    db.create_st(
        "st_fuse_notblown",
        "SELECT id FROM src_fuse_notblown",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let populated =
        wait_for_scheduler_refresh(&db, "st_fuse_notblown", Duration::from_secs(30)).await;
    assert!(populated, "ST should be populated within 30s");

    // reset_fuse when not blown should error
    let err = db
        .try_execute("SELECT pgtrickle.reset_fuse('st_fuse_notblown', 'apply')")
        .await;
    assert!(
        err.is_err(),
        "reset_fuse on non-blown fuse should produce an error"
    );
}

/// Global fuse ceiling GUC: when set, acts as the default ceiling for all STs with fuse_mode='on'.
#[tokio::test]
async fn test_fuse_global_ceiling_guc() {
    let db = E2eDb::new().await.with_extension().await;
    setup_fast_scheduler(&db).await;

    db.execute("CREATE TABLE src_fuse_global (id int PRIMARY KEY, val text)")
        .await;
    db.execute("INSERT INTO src_fuse_global SELECT g, 'row-' || g FROM generate_series(1, 10) g")
        .await;

    db.create_st(
        "st_fuse_global",
        "SELECT id, val FROM src_fuse_global",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let populated =
        wait_for_scheduler_refresh(&db, "st_fuse_global", Duration::from_secs(30)).await;
    assert!(populated, "ST should be populated within 30s");

    // Set global ceiling to 30 and enable fuse mode (no per-ST ceiling)
    db.execute("ALTER SYSTEM SET pg_trickle.fuse_default_ceiling = 30")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.fuse_default_ceiling", "30")
        .await;

    db.execute("SELECT pgtrickle.alter_stream_table('st_fuse_global', fuse => 'on')")
        .await;

    // Insert 50 rows — above global ceiling of 30
    db.execute(
        "INSERT INTO src_fuse_global SELECT g, 'spike-' || g FROM generate_series(11, 60) g",
    )
    .await;

    // Fuse should blow from global ceiling
    let blown = wait_for_fuse_blown(&db, "st_fuse_global", Duration::from_secs(30)).await;
    assert!(blown, "fuse should blow from global ceiling");
}

#[tokio::test]
async fn test_fuse_ignores_history_retained_for_slow_sibling() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    setup_fast_scheduler(&db).await;
    db.execute("CREATE TABLE fuse1486_src(id int PRIMARY KEY, v int)")
        .await;
    db.execute("INSERT INTO fuse1486_src VALUES (1,1)").await;
    let query = "SELECT id,v FROM fuse1486_src";
    db.create_st("fuse1486_fast", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st("fuse1486_slow", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.fuse1486_slow'])")
        .await;
    db.execute("INSERT INTO fuse1486_src SELECT n,n FROM generate_series(2,100) n")
        .await;
    db.refresh_st_until_converged("fuse1486_fast").await;
    db.assert_st_matches_query("fuse1486_fast", query).await;
    assert_eq!(db.count("fuse1486_slow").await, 1);
    let buffer = db
        .change_buffer_table(i64::from(db.table_oid("fuse1486_src").await))
        .await;
    assert!(
        db.query_scalar::<i64>(&format!(
            "SELECT count(*) FROM {buffer} WHERE action IN ('I','D')"
        ))
        .await
            > 10
    );
    db.execute("SELECT pgtrickle.alter_stream_table('fuse1486_fast', schedule => '1s', fuse => 'on', fuse_ceiling => 10)").await;
    db.execute("INSERT INTO fuse1486_src VALUES (101,101)")
        .await;
    assert!(
        db.wait_for_condition(
            "fast consumer refreshes despite sibling backlog",
            "SELECT EXISTS (SELECT 1 FROM fuse1486_fast WHERE id=101)",
            Duration::from_secs(60),
            Duration::from_millis(100),
        )
        .await
    );
    assert_eq!(
        db.query_scalar::<String>(
            "SELECT fuse_state FROM pgtrickle.pgt_stream_tables WHERE pgt_name='fuse1486_fast'",
        )
        .await,
        "armed"
    );
    db.assert_st_matches_query("fuse1486_fast", query).await;
}

#[tokio::test]
async fn test_fuse_sensitivity_requires_more_than_one_observation() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    setup_fast_scheduler(&db).await;
    db.execute("CREATE TABLE streak1486_src(id int PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO streak1486_src VALUES (1)").await;
    db.create_st(
        "streak1486_st",
        "SELECT id FROM streak1486_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.alter_stream_table('streak1486_st', schedule => '1s', fuse => 'on', fuse_ceiling => 10, fuse_sensitivity => 100)").await;
    db.execute("INSERT INTO streak1486_src SELECT n FROM generate_series(2,100) n")
        .await;
    assert!(
        db.wait_for_condition(
            "first over-ceiling batch remains armed and is applied",
            "SELECT EXISTS (SELECT 1 FROM streak1486_st WHERE id=100)",
            Duration::from_secs(60),
            Duration::from_millis(100),
        )
        .await
    );
    assert_eq!(
        db.query_scalar::<String>(
            "SELECT fuse_state FROM pgtrickle.pgt_stream_tables WHERE pgt_name='streak1486_st'",
        )
        .await,
        "armed"
    );
    db.execute("SELECT pgtrickle.alter_stream_table('streak1486_st', fuse_sensitivity => 1)")
        .await;
    db.execute("INSERT INTO streak1486_src SELECT n FROM generate_series(101,200) n")
        .await;
    assert!(wait_for_fuse_blown(&db, "streak1486_st", Duration::from_secs(60)).await);
}
