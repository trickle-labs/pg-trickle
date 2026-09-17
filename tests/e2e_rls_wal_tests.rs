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

    let db_suffix: String = db.query_scalar("SELECT current_database()").await;
    let wal_owner = format!("rls_wal_owner_{}", db_suffix.replace('-', "_"));
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
    db.execute("INSERT INTO rls_wal_src VALUES (3, 30, 'c')")
        .await;
    db.execute("UPDATE rls_wal_src SET val = 'b2' WHERE id = 2")
        .await;
    db.execute("DELETE FROM rls_wal_src WHERE id = 1").await;

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

    let effective_mode: String = db
        .query_scalar("SELECT effective_refresh_mode FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rls_wal_st'")
        .await;
    assert_eq!(effective_mode, "DIFFERENTIAL");
}
