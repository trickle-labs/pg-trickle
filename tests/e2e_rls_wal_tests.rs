//! E2E coverage for differential WAL CDC on RLS-protected sources.
//!
//! Requires the full E2E image because logical decoding and the scheduler are
//! needed to exercise the WAL path.

#![cfg(not(feature = "light-e2e"))]

mod e2e;

use e2e::E2eDb;
use std::time::{Duration, Instant};

async fn wait_for_wal_mode(db: &E2eDb, source: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mode: String = db
            .query_scalar(&format!(
                "SELECT cdc_mode FROM pgtrickle.pgt_dependencies \
                 WHERE source_relid = '{source}'::regclass AND source_type = 'TABLE' \
                 LIMIT 1"
            ))
            .await;
        if mode == "WAL" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "source {source} did not transition to WAL mode; got {mode}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn test_rls_bypassrls_owner_differential_wal_matches_query() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.alter_system_set_and_wait(
        "pg_trickle.refresh_strategy",
        "'differential'",
        "differential",
    )
    .await;
    let database: String = db.query_scalar("SELECT current_database()").await;
    // Refresh jobs use fresh background-worker connections; pin their GUC default here.
    db.execute(&format!(
        "ALTER DATABASE \"{}\" SET pg_trickle.refresh_strategy = 'differential'",
        database.replace('"', "\"\"")
    ))
    .await;

    db.execute(
        "CREATE TABLE rls_wal_src (id INT PRIMARY KEY, tenant_id INT NOT NULL, val TEXT NOT NULL)",
    )
    .await;
    db.execute("ALTER TABLE rls_wal_src REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO rls_wal_src VALUES (1, 10, 'a'), (2, 20, 'b')")
        .await;
    db.execute("ALTER TABLE rls_wal_src ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute("ALTER TABLE rls_wal_src FORCE ROW LEVEL SECURITY")
        .await;
    db.execute("CREATE POLICY tenant_only ON rls_wal_src USING (tenant_id = 10)")
        .await;

    let wal_owner = format!("rls_wal_owner_{}", database.replace('-', "_"));
    db.execute(&format!("CREATE ROLE {wal_owner} LOGIN BYPASSRLS"))
        .await;
    db.execute(&format!(
        "GRANT USAGE, CREATE ON SCHEMA public TO {wal_owner}"
    ))
    .await;
    db.execute(&format!("GRANT USAGE ON SCHEMA pgtrickle TO {wal_owner}"))
        .await;
    db.execute(&format!(
        "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO {wal_owner}"
    ))
    .await;
    db.execute(&format!("GRANT SELECT ON rls_wal_src TO {wal_owner}"))
        .await;

    let query = "SELECT id, tenant_id, val FROM rls_wal_src";
    db.try_execute_with_role(
        &format!("SET ROLE {wal_owner}"),
        "SELECT pgtrickle.create_stream_table(\
            name => 'rls_wal_st',\
            query => $$SELECT id, tenant_id, val FROM rls_wal_src$$,\
            schedule => '1s',\
            refresh_mode => 'DIFFERENTIAL',\
            cdc_mode => 'wal'\
        )",
        "RESET ROLE",
    )
    .await
    .expect("BYPASSRLS owner should be allowed to create a differential WAL stream");
    db.assert_st_matches_query("rls_wal_st", query).await;

    wait_for_wal_mode(&db, "rls_wal_src").await;
    // Drain cutover work before establishing the baseline for differential mutations.
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['rls_wal_st'])")
        .await;
    db.refresh_st_with_retry("rls_wal_st").await;
    // Keep refreshes paused until all source mutations are committed so one
    // differential pass exercises the complete batch.
    db.execute("INSERT INTO rls_wal_src VALUES (3, 30, 'c')")
        .await;
    db.execute("UPDATE rls_wal_src SET val = 'b2' WHERE id = 2")
        .await;
    db.execute("DELETE FROM rls_wal_src WHERE id = 1").await;
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['rls_wal_st'])")
        .await;

    let converged = db
        .wait_for_condition(
            "rls WAL stream table",
            "SELECT NOT EXISTS (\
                (SELECT id, tenant_id, val FROM rls_wal_st EXCEPT ALL \
                 SELECT id, tenant_id, val FROM rls_wal_src) \
                UNION ALL \
                (SELECT id, tenant_id, val FROM rls_wal_src EXCEPT ALL \
                 SELECT id, tenant_id, val FROM rls_wal_st)\
            )",
            Duration::from_secs(60),
            Duration::from_millis(200),
        )
        .await;
    assert!(
        converged,
        "scheduler should apply the WAL INSERT, UPDATE, and DELETE"
    );
    db.assert_st_matches_query("rls_wal_st", query).await;

    // A WAL cutover may legitimately cause a protected FULL reinitialization
    // when the source schema fingerprint changes. Validate the WAL capture
    // mode and exact results without requiring a particular refresh action.
    wait_for_wal_mode(&db, "rls_wal_src").await;
    db.execute("INSERT INTO rls_wal_src VALUES (4, 40, 'd')")
        .await;
    let converged_after_cutover = db
        .wait_for_condition(
            "RLS WAL replay after cutover",
            "SELECT EXISTS (SELECT 1 FROM rls_wal_st WHERE id = 4 AND val = 'd')",
            Duration::from_secs(90),
            Duration::from_millis(200),
        )
        .await;
    assert!(
        converged_after_cutover,
        "RLS WAL should replay committed rows after cutover"
    );
    db.assert_st_matches_query("rls_wal_st", query).await;
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rls_wal_st'",
        )
        .await;
    assert!(!needs_reinit, "RLS WAL stream should not need reinitialization");
}
