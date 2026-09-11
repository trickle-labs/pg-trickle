//! E2E tests for `pgtrickle.alter_stream_table()`.
//!
//! Validates altering schedule, refresh_mode, and status (suspend/resume).
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_alter_schedule() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO al_src VALUES (1)").await;

    db.create_st("al_sched_st", "SELECT id FROM al_src", "1m", "FULL")
        .await;

    // Verify initial schedule (stored as text)
    let schedule_before: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'al_sched_st'",
        )
        .await;
    assert_eq!(schedule_before, "1m");

    // Alter schedule using Prometheus-style duration
    db.alter_st("al_sched_st", "schedule => '5m'").await;

    let schedule_after: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'al_sched_st'",
        )
        .await;
    assert_eq!(schedule_after, "5m", "Schedule should be updated to '5m'");

    db.execute("INSERT INTO al_src VALUES (2)").await;
    db.refresh_st("al_sched_st").await;

    db.assert_st_matches_query("al_sched_st", "SELECT id FROM al_src")
        .await;
}

#[tokio::test]
async fn test_alter_refresh_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_mode (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO al_mode VALUES (1)").await;

    db.create_st("al_mode_st", "SELECT id FROM al_mode", "1m", "DIFFERENTIAL")
        .await;

    let (_, mode_before, _, _) = db.pgt_status("al_mode_st").await;
    assert_eq!(mode_before, "DIFFERENTIAL");

    // Change to FULL
    db.alter_st("al_mode_st", "refresh_mode => 'FULL'").await;

    let (_, mode_after, _, _) = db.pgt_status("al_mode_st").await;
    assert_eq!(mode_after, "FULL");

    db.execute("INSERT INTO al_mode VALUES (2)").await;
    db.refresh_st("al_mode_st").await;

    db.assert_st_matches_query("al_mode_st", "SELECT id FROM al_mode")
        .await;
}

#[tokio::test]
async fn test_alter_to_immediate_ignores_global_wal_cdc_guc() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_mode_wal_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO al_mode_wal_src VALUES (1, 'a')")
        .await;

    db.create_st(
        "al_mode_wal_st",
        "SELECT id, val FROM al_mode_wal_src",
        "1m",
        "FULL",
    )
    .await;

    let source_oid = db.table_oid("al_mode_wal_src").await;
    let cdc_trigger_name = db.cdc_trigger_name(source_oid as i64).await;
    assert!(
        db.trigger_exists(&cdc_trigger_name, "al_mode_wal_src")
            .await,
        "Deferred mode should start with CDC trigger infrastructure"
    );

    let result = db
        .try_execute(
            "WITH wal_mode AS (\
            SELECT set_config('pg_trickle.cdc_mode', 'wal', true)\
         )\
         SELECT pgtrickle.alter_stream_table(\
            'al_mode_wal_st',\
            refresh_mode => 'IMMEDIATE'\
         )\
         FROM wal_mode",
        )
        .await;

    assert!(
        result.is_ok(),
        "global WAL mode must not override IMMEDIATE: {result:?}"
    );
}

#[tokio::test]
async fn test_alter_to_immediate_rejects_explicit_wal_cdc_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_mode_explicit_wal_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO al_mode_explicit_wal_src VALUES (1, 'a')")
        .await;

    db.create_st(
        "al_mode_explicit_wal_st",
        "SELECT id, val FROM al_mode_explicit_wal_src",
        "1m",
        "FULL",
    )
    .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table(\
                'al_mode_explicit_wal_st',\
                refresh_mode => 'IMMEDIATE',\
                cdc_mode => 'wal'\
            )",
        )
        .await;

    assert!(
        result.is_err(),
        "Explicit wal CDC must be rejected for IMMEDIATE mode switches"
    );

    let error = format!("{}", result.unwrap_err());
    assert!(
        error.contains("IMMEDIATE"),
        "Expected mode interaction error, got: {error}"
    );
}

#[tokio::test]
async fn test_alter_suspend() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_susp (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO al_susp VALUES (1)").await;

    db.create_st("al_susp_st", "SELECT id FROM al_susp", "1m", "FULL")
        .await;

    let (status_before, _, _, _) = db.pgt_status("al_susp_st").await;
    assert_eq!(status_before, "ACTIVE");

    // Suspend
    db.alter_st("al_susp_st", "status => 'SUSPENDED'").await;

    let (status_after, _, _, _) = db.pgt_status("al_susp_st").await;
    assert_eq!(status_after, "SUSPENDED");

    // Verify refresh is refused
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('al_susp_st')")
        .await;
    assert!(
        result.is_err(),
        "Refresh should be refused for SUSPENDED ST"
    );
}

#[tokio::test]
async fn test_alter_resume() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_resume (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO al_resume VALUES (1)").await;

    db.create_st("al_resume_st", "SELECT id FROM al_resume", "1m", "FULL")
        .await;

    // Suspend then resume
    db.alter_st("al_resume_st", "status => 'SUSPENDED'").await;
    let (status, _, _, _) = db.pgt_status("al_resume_st").await;
    assert_eq!(status, "SUSPENDED");

    db.alter_st("al_resume_st", "status => 'ACTIVE'").await;
    let (status, _, _, errors) = db.pgt_status("al_resume_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(errors, 0, "consecutive_errors should be reset on resume");

    // Refresh should work again
    db.execute("INSERT INTO al_resume VALUES (2)").await;
    db.refresh_st("al_resume_st").await;
    assert_eq!(db.count("public.al_resume_st").await, 2);
}

#[tokio::test]
async fn test_alter_nonexistent_fails() {
    let db = E2eDb::new().await.with_extension().await;

    let result = db
        .try_execute("SELECT pgtrickle.alter_stream_table('nonexistent_st', status => 'SUSPENDED')")
        .await;
    assert!(result.is_err(), "Altering a nonexistent ST should fail");
}

// ── Schedule Format Tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_alter_schedule_compound_duration() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_comp (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO al_comp VALUES (1)").await;

    db.create_st("al_comp_st", "SELECT id FROM al_comp", "1m", "FULL")
        .await;

    // Alter to compound duration
    db.alter_st("al_comp_st", "schedule => '1h30m'").await;

    let sched: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'al_comp_st'",
        )
        .await;
    assert_eq!(sched, "1h30m");
}

#[tokio::test]
async fn test_alter_schedule_to_cron() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_cron (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO al_cron VALUES (1)").await;

    db.create_st("al_cron_st", "SELECT id FROM al_cron", "1m", "FULL")
        .await;

    // Change from duration to cron
    db.alter_st("al_cron_st", "schedule => '*/10 * * * *'")
        .await;

    let schedule: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'al_cron_st'",
        )
        .await;
    assert_eq!(schedule, "*/10 * * * *");
}

#[tokio::test]
async fn test_alter_schedule_to_cron_alias() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_calias (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO al_calias VALUES (1)").await;

    db.create_st("al_calias_st", "SELECT id FROM al_calias", "5m", "FULL")
        .await;

    db.alter_st("al_calias_st", "schedule => '@daily'").await;

    let schedule: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'al_calias_st'",
        )
        .await;
    assert_eq!(schedule, "@daily");
}

#[tokio::test]
async fn test_alter_schedule_from_cron_to_duration() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_c2d (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO al_c2d VALUES (1)").await;

    // Start with cron
    db.execute(
        "SELECT pgtrickle.create_stream_table('al_c2d_st', \
         $$ SELECT id FROM al_c2d $$, '@hourly', 'FULL')",
    )
    .await;

    let before: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'al_c2d_st'",
        )
        .await;
    assert_eq!(before, "@hourly");

    // Switch back to duration
    db.alter_st("al_c2d_st", "schedule => '10m'").await;

    let after: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'al_c2d_st'",
        )
        .await;
    assert_eq!(after, "10m");
}

#[tokio::test]
async fn test_alter_schedule_invalid_cron_fails() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_badcron (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO al_badcron VALUES (1)").await;

    db.create_st("al_badcron_st", "SELECT id FROM al_badcron", "1m", "FULL")
        .await;

    // Invalid cron: missing fields
    let result = db
        .try_execute("SELECT pgtrickle.alter_stream_table('al_badcron_st', schedule => '* *')")
        .await;
    assert!(
        result.is_err(),
        "Altering schedule to invalid cron should fail"
    );
}
