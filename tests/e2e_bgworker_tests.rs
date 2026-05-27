//! E2E tests for the background worker (scheduler) and GUC configuration.
//!
//! These tests verify that:
//! - The extension loads correctly with `shared_preload_libraries`
//! - GUC parameters are registered and queryable
//! - The background scheduler automatically refreshes stale STs
//! - The scheduler respects the `pg_trickle.enabled` GUC
//!
//! **Note:** Background worker tests are timing-dependent. They use generous
//! timeouts and retry loops. The scheduler interval and minimum schedule
//! are lowered via `ALTER SYSTEM` to speed up tests.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helper ─────────────────────────────────────────────────────────────────

/// Configure the scheduler for fast testing:
/// - `pg_trickle.scheduler_interval_ms = 100` (wake every 100ms)
/// - `pg_trickle.min_schedule_seconds = 1` (allow 1-second schedule)
///
/// Uses `ALTER SYSTEM` + `pg_reload_conf()` so the background worker
/// picks up the changes.
///
/// Also waits for the pg_trickle scheduler BGW to appear in pg_stat_activity.
///
/// ## Why this is needed
///
/// `new_on_postgres_db()` now creates a fresh database for each test while
/// still resetting server-level `ALTER SYSTEM` state up front. That avoids
/// the brittle shared-`postgres` reset/bootstrap path while keeping tests
/// independent of execution order.
///
/// The wait helper nudges the launcher every 10 s via both
/// `pgtrickle._signal_launcher_rescan()` and `pg_reload_conf()`, so stale
/// `last_attempt` entries are re-evaluated and the launcher wakes promptly.
async fn configure_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    // Disable auto-backoff so 1-second schedules never get stretched in slow
    // CI containers — the default (true since v0.10.0) would double the
    // effective interval once a refresh takes > 950 ms.
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;

    if !sched_running {
        // Dump diagnostic info before panicking.
        let launcher_count: i64 = db
            .query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE backend_type = 'pg_trickle launcher'",
            )
            .await;
        let sched_count: i64 = db
            .query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE backend_type = 'pg_trickle scheduler'",
            )
            .await;
        let db_name: String = db.query_scalar("SELECT current_database()").await;
        let enabled: String = db.show_setting("pg_trickle.enabled").await;
        let worker_count: i64 = db
            .query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'background worker'",
            )
            .await;

        panic!(
            "pg_trickle scheduler did not appear in pg_stat_activity within 90 s.\n\
             Diagnostics: database={db_name}, enabled={enabled}, \
             launcher_count={launcher_count}, scheduler_count={sched_count}, \
             total_bgworkers={worker_count}\n\
             Possible causes: \
             (1) the launcher never re-probed the fresh test database after CREATE EXTENSION; \
             (2) launcher retry back-off exceeded the timeout; \
             (3) pg_trickle.enabled GUC is false; \
             (4) max_worker_processes exhausted — E2E image sets it to 128 (rebuild with \
             `just build-e2e-image` if using an older image)."
        );
    }
}

/// Wait until a ST has been auto-refreshed by checking pgt_refresh_history.
/// The scheduler (unlike manual refresh) writes history records.
/// Returns true if a completed record appears within the timeout.
#[allow(dead_code)]
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

// ── Tests ──────────────────────────────────────────────────────────────────

/// Verify the container starts with `shared_preload_libraries` configured
/// and the extension can be created without errors.
#[tokio::test]
async fn test_extension_loads_with_shared_preload() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // Verify shared_preload_libraries includes our extension
    let spl: String = db.query_scalar("SHOW shared_preload_libraries").await;
    assert!(
        spl.contains("pg_trickle"),
        "shared_preload_libraries should contain pg_trickle, got: {}",
        spl,
    );

    // Verify the extension is listed in pg_extension
    let ext_exists: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_trickle')")
        .await;
    assert!(ext_exists, "Extension should be installed");

    // Verify no ERROR-level messages — check that we can use the API
    let st_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables")
        .await;
    assert_eq!(st_count, 0, "Fresh install should have 0 STs");
}

/// Verify all GUC parameters are registered and return expected defaults.
#[tokio::test]
async fn test_gucs_registered() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // pg_trickle.enabled — default: on
    let enabled = db.show_setting("pg_trickle.enabled").await;
    assert_eq!(enabled, "on", "pg_trickle.enabled default should be 'on'");

    // pg_trickle.scheduler_interval_ms — default: 1000
    let interval = db.show_setting("pg_trickle.scheduler_interval_ms").await;
    assert_eq!(
        interval, "1000",
        "pg_trickle.scheduler_interval_ms default should be '1000'"
    );

    // pg_trickle.min_schedule_seconds — default: 1
    let min_schedule = db.show_setting("pg_trickle.min_schedule_seconds").await;
    assert_eq!(
        min_schedule, "1",
        "pg_trickle.min_schedule_seconds default should be '1'"
    );

    // pg_trickle.max_consecutive_errors — default: 3
    let max_errors = db.show_setting("pg_trickle.max_consecutive_errors").await;
    assert_eq!(
        max_errors, "3",
        "pg_trickle.max_consecutive_errors default should be '3'"
    );

    // pg_trickle.change_buffer_schema — default: pgtrickle_changes
    let buf_schema = db.show_setting("pg_trickle.change_buffer_schema").await;
    assert_eq!(
        buf_schema, "pgtrickle_changes",
        "pg_trickle.change_buffer_schema default should be 'pgtrickle_changes'"
    );

    // pg_trickle.max_concurrent_refreshes — default: 4
    let max_conc = db.show_setting("pg_trickle.max_concurrent_refreshes").await;
    assert_eq!(
        max_conc, "4",
        "pg_trickle.max_concurrent_refreshes default should be '4'"
    );

    // pg_trickle.slot_lag_warning_threshold_mb — default: 100
    let slot_lag_warning = db
        .show_setting("pg_trickle.slot_lag_warning_threshold_mb")
        .await;
    assert_eq!(
        slot_lag_warning, "100",
        "pg_trickle.slot_lag_warning_threshold_mb default should be '100'"
    );

    // pg_trickle.slot_lag_critical_threshold_mb — default: 1024
    let slot_lag_critical = db
        .show_setting("pg_trickle.slot_lag_critical_threshold_mb")
        .await;
    assert_eq!(
        slot_lag_critical, "1024",
        "pg_trickle.slot_lag_critical_threshold_mb default should be '1024'"
    );
}

/// Verify that GUCs can be changed via ALTER SYSTEM and take effect.
#[tokio::test]
async fn test_gucs_can_be_altered() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // Change scheduler_interval_ms
    db.alter_system_set_and_wait("pg_trickle.scheduler_interval_ms", "200", "200")
        .await;

    let interval = db.show_setting("pg_trickle.scheduler_interval_ms").await;
    assert_eq!(
        interval, "200",
        "scheduler_interval_ms should be updated to 200"
    );

    // Change min_schedule_seconds
    db.alter_system_set_and_wait("pg_trickle.min_schedule_seconds", "5", "5")
        .await;

    let min_schedule = db.show_setting("pg_trickle.min_schedule_seconds").await;
    assert_eq!(
        min_schedule, "5",
        "min_schedule_seconds should be updated to 5"
    );

    // Change enabled
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;

    let enabled = db.show_setting("pg_trickle.enabled").await;
    assert_eq!(enabled, "off", "pg_trickle.enabled should be 'off'");

    // Change slot_lag_warning_threshold_mb
    db.alter_system_set_and_wait("pg_trickle.slot_lag_warning_threshold_mb", "256", "256")
        .await;

    let slot_lag_warning = db
        .show_setting("pg_trickle.slot_lag_warning_threshold_mb")
        .await;
    assert_eq!(
        slot_lag_warning, "256",
        "slot_lag_warning_threshold_mb should be updated to 256"
    );

    // Reset back
    db.alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    db.alter_system_set_and_wait("pg_trickle.slot_lag_warning_threshold_mb", "100", "100")
        .await;
}

/// Create a ST with a short schedule (after lowering the minimum),
/// insert source data, and verify the background scheduler automatically
/// refreshes the ST within the expected timeframe.
#[tokio::test]
async fn test_auto_refresh_within_schedule() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // Speed up the scheduler for testing
    configure_fast_scheduler(&db).await;

    // Create source table and ST with 1-second schedule
    db.execute("CREATE TABLE auto_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO auto_src VALUES (1, 'initial')")
        .await;

    db.create_st("auto_st", "SELECT id, val FROM auto_src", "1s", "FULL")
        .await;

    // Verify initial population
    let count = db.count("public.auto_st").await;
    assert_eq!(count, 1, "ST should be populated initially");

    // Insert new data into source — this should trigger CDC
    db.execute("INSERT INTO auto_src VALUES (2, 'new_data')")
        .await;

    // Wait for the scheduler to auto-refresh
    // The scheduler detects: (now() - data_timestamp) > schedule (1s)
    // With 100ms interval, this should happen within a few seconds.
    // 60s timeout gives CI runners headroom under load.
    let refreshed = db
        .wait_for_auto_refresh("auto_st", Duration::from_secs(60))
        .await;
    assert!(refreshed, "Scheduler should auto-refresh the ST");

    // Verify the new data is materialized
    let count = db.count("public.auto_st").await;
    assert_eq!(count, 2, "ST should contain 2 rows after auto-refresh");

    // Verify refresh history was written by the scheduler
    let history_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'auto_st' AND h.status = 'COMPLETED'",
        )
        .await;
    assert!(
        history_count >= 1,
        "Scheduler should have written at least 1 refresh history record"
    );

    // Verify data correctness: ST must exactly match source after auto-refresh
    db.assert_st_matches_query("public.auto_st", "SELECT id, val FROM auto_src")
        .await;
}

/// Verify that the scheduler fires differential refresh when the ST
/// is configured with DIFFERENTIAL mode.
#[tokio::test]
async fn test_auto_refresh_differential_mode() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE inc_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO inc_src VALUES (1, 100), (2, 200)")
        .await;

    db.create_st(
        "inc_st",
        "SELECT id, val FROM inc_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.inc_st").await, 2);

    // Insert more data
    db.execute("INSERT INTO inc_src VALUES (3, 300)").await;

    let refreshed = db
        .wait_for_auto_refresh("inc_st", Duration::from_secs(60))
        .await;
    assert!(refreshed, "Scheduler should auto-refresh differential ST");

    assert_eq!(
        db.count("public.inc_st").await,
        3,
        "Differential ST should have 3 rows after auto-refresh"
    );

    // Verify data correctness
    db.assert_st_matches_query("public.inc_st", "SELECT id, val FROM inc_src")
        .await;
}

/// Verify that the scheduler writes refresh history records for
/// successful auto-refreshes (unlike manual refresh which does not).
#[tokio::test]
async fn test_scheduler_writes_refresh_history() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE hist_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO hist_src VALUES (1, 'init')").await;

    db.create_st("hist_st", "SELECT id, val FROM hist_src", "1s", "FULL")
        .await;

    // Initial population does NOT write to history (done by create_stream_table)
    let initial_history: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'hist_st'",
        )
        .await;

    // Insert new data to trigger scheduler refresh
    db.execute("INSERT INTO hist_src VALUES (2, 'new')").await;

    // Wait for the scheduler to refresh
    let refreshed = db
        .wait_for_auto_refresh("hist_st", Duration::from_secs(60))
        .await;
    assert!(refreshed, "Scheduler should auto-refresh");

    // Verify refresh history was written
    let new_history: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'hist_st' AND h.status = 'COMPLETED'",
        )
        .await;
    assert!(
        new_history > initial_history,
        "Scheduler should write COMPLETED records to pgt_refresh_history \
         (initial={}, after={})",
        initial_history,
        new_history,
    );
}

/// Verify that the scheduler correctly handles differential auto-refresh
/// with CDC change buffers, producing correct results.
#[tokio::test]
async fn test_auto_refresh_differential_with_cdc() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE buf_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO buf_src VALUES (1, 'a')").await;

    db.create_st(
        "buf_st",
        "SELECT id, val FROM buf_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.buf_st").await, 1);

    // Insert multiple rows to trigger CDC and differential refresh
    db.execute("INSERT INTO buf_src VALUES (2, 'b')").await;
    db.execute("INSERT INTO buf_src VALUES (3, 'c')").await;

    // Wait for auto-refresh to pick up the new rows
    let _ = db
        .wait_for_condition(
            "buf_st has 3 rows",
            "SELECT (SELECT count(*) FROM public.buf_st) >= 3",
            Duration::from_secs(60),
            Duration::from_millis(200),
        )
        .await;

    assert_eq!(
        db.count("public.buf_st").await,
        3,
        "Differential auto-refresh should pick up all new rows"
    );

    // Verify data correctness between source and ST
    db.assert_st_matches_query("public.buf_st", "SELECT id, val FROM buf_src")
        .await;
}

/// Verify the scheduler correctly handles two healthy STs, refreshing both.
/// The scheduler processes all STs in a single transaction per tick.
#[tokio::test]
async fn test_scheduler_refreshes_multiple_healthy_sts() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Create two independent STs
    db.execute("CREATE TABLE h_src1 (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO h_src1 VALUES (1, 10)").await;

    db.execute("CREATE TABLE h_src2 (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO h_src2 VALUES (1, 20)").await;

    db.create_st("h_st1", "SELECT id, val FROM h_src1", "1s", "FULL")
        .await;

    db.create_st("h_st2", "SELECT id, val FROM h_src2", "1s", "DIFFERENTIAL")
        .await;

    assert_eq!(db.count("public.h_st1").await, 1);
    assert_eq!(db.count("public.h_st2").await, 1);

    // Insert data into both sources
    db.execute("INSERT INTO h_src1 VALUES (2, 11)").await;
    db.execute("INSERT INTO h_src2 VALUES (2, 21)").await;

    // Wait for both to be refreshed (poll row counts)
    let _ = db
        .wait_for_condition(
            "h_st1 and h_st2 both have 2 rows",
            "SELECT (SELECT count(*) FROM public.h_st1) = 2 \
             AND (SELECT count(*) FROM public.h_st2) = 2",
            Duration::from_secs(60),
            Duration::from_millis(200),
        )
        .await;

    assert_eq!(
        db.count("public.h_st1").await,
        2,
        "First ST should have 2 rows after auto-refresh"
    );
    assert_eq!(
        db.count("public.h_st2").await,
        2,
        "Second ST should have 2 rows after auto-refresh"
    );

    // Verify data correctness: both STs must exactly match their sources
    db.assert_st_matches_query("public.h_st1", "SELECT id, val FROM h_src1")
        .await;
    db.assert_st_matches_query("public.h_st2", "SELECT id, val FROM h_src2")
        .await;
}

/// Verify the scheduler updates catalog metadata after each refresh:
/// last_refresh_at, data_timestamp, and resets consecutive_errors.
#[tokio::test]
async fn test_auto_refresh_updates_catalog_metadata() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE meta_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO meta_src VALUES (1)").await;

    db.create_st("meta_st", "SELECT id FROM meta_src", "1s", "FULL")
        .await;

    // Record initial timestamps
    let _initial_refresh_at: Option<String> = db
        .query_scalar_opt(
            "SELECT last_refresh_at::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'meta_st'",
        )
        .await;
    let initial_data_ts: Option<String> = db
        .query_scalar_opt(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'meta_st'",
        )
        .await;

    // Insert data and wait for auto-refresh
    db.execute("INSERT INTO meta_src VALUES (2)").await;

    let refreshed = db
        .wait_for_auto_refresh("meta_st", Duration::from_secs(60))
        .await;
    assert!(refreshed, "Scheduler should auto-refresh");

    // Verify timestamps advanced
    let new_refresh_at: Option<String> = db
        .query_scalar_opt(
            "SELECT last_refresh_at::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'meta_st'",
        )
        .await;
    let new_data_ts: Option<String> = db
        .query_scalar_opt(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'meta_st'",
        )
        .await;

    assert_ne!(
        initial_data_ts, new_data_ts,
        "data_timestamp should advance after auto-refresh"
    );
    // last_refresh_at should be set (might have been NULL initially if
    // the initial population doesn't set it, or it was set)
    assert!(
        new_refresh_at.is_some(),
        "last_refresh_at should be set after auto-refresh"
    );

    // consecutive_errors should be 0
    let (_, _, _, errors) = db.pgt_status("meta_st").await;
    assert_eq!(errors, 0, "consecutive_errors should be 0 after success");
}

// ── T-A41-4: Pool worker respects pg_trickle.enabled GUC ────────────────────

/// T-A41-4: While pg_trickle.enabled = off the pool worker must not execute
/// any queued refresh jobs.  When enabled is restored the scheduler must
/// resume processing normally.
#[tokio::test]
async fn test_pool_worker_does_not_run_while_disabled() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // Configure a fast scheduler so timing artefacts don't dominate.
    configure_fast_scheduler(&db).await;

    // Create a source table and a stream table with a short schedule.
    db.execute("CREATE TABLE disabled_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO disabled_src VALUES (1, 'initial')")
        .await;
    db.create_st(
        "disabled_st",
        "SELECT id, val FROM disabled_src",
        "1s",
        "FULL",
    )
    .await;

    // Record the initial refresh count from history.
    let count_before: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'disabled_st') \
             AND status = 'COMPLETED'",
        )
        .await;

    // Disable the extension — pool workers must stop claiming jobs.
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    let enabled_off = db.show_setting("pg_trickle.enabled").await;
    assert_eq!(enabled_off, "off", "pg_trickle.enabled should be 'off'");

    // Insert new data to create a pending refresh opportunity.
    db.execute("INSERT INTO disabled_src VALUES (2, 'new_data')")
        .await;

    // Wait long enough for the scheduler to have attempted a refresh IF it
    // were enabled (scheduler_interval_ms=100, so 3 seconds is ~30 wakeups).
    // This is a negative-assertion wait: we're confirming absence of an event.
    // We use wait_for_condition with an always-false SQL to get a deterministic
    // timeout window instead of a bare sleep.
    let _ = db
        .wait_for_condition(
            "no refreshes while disabled (3s stability window)",
            "SELECT false",
            Duration::from_secs(3),
            Duration::from_millis(100),
        )
        .await;

    // Verify: no new successful refreshes should have occurred.
    let count_disabled: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'disabled_st') \
             AND status = 'COMPLETED'",
        )
        .await;
    assert_eq!(
        count_before, count_disabled,
        "no refreshes should execute while pg_trickle.enabled = off; \
         before={count_before}, after_disabled_period={count_disabled}"
    );

    // Re-enable and confirm refreshes resume.
    db.alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    let enabled_on = db.show_setting("pg_trickle.enabled").await;
    assert_eq!(
        enabled_on, "on",
        "pg_trickle.enabled should be restored to 'on'"
    );

    // After re-enabling the data inserted while disabled should trigger a refresh.
    // We check pgt_refresh_history directly rather than data_timestamp:
    // data_timestamp is set to now() at refresh-transaction start, and on
    // Linux CI the first post-re-enable refresh can complete before
    // initial_ts is captured, so wait_for_auto_refresh would need *two*
    // refreshes to observe a change — an unnecessary timing dependency.
    let resumed = db
        .wait_for_condition(
            "new refresh should complete after re-enabling",
            &format!(
                "SELECT count(*) > {count_before} \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                                 WHERE pgt_name = 'disabled_st') \
                 AND status = 'COMPLETED'"
            ),
            Duration::from_secs(30),
            Duration::from_millis(500),
        )
        .await;
    assert!(
        resumed,
        "scheduler should resume refreshes after re-enabling"
    );
}
