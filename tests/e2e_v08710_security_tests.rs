//! v0.87.10 lifecycle-policy and upgrade-preflight probes.

mod e2e;

use e2e::E2eDb;

const CREATE_STREAM_ARGS: &str = "text, text, text, text, boolean, text, text, text, boolean, boolean, \
     text, integer, double precision, text, boolean, text, integer, text";

async fn create_role(db: &E2eDb, role: &str) {
    db.execute(&format!(
        "DO $$ BEGIN CREATE ROLE {role} LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    ))
    .await;
    db.execute(&format!("GRANT USAGE ON SCHEMA pgtrickle TO {role}"))
        .await;
    db.execute(&format!("GRANT CREATE ON SCHEMA public TO {role}"))
        .await;
    db.execute(&format!(
        "GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table({CREATE_STREAM_ARGS}) TO {role}"
    ))
    .await;
}

async fn grant_lifecycle_apis(db: &E2eDb, role: &str) {
    for function in [
        "pause_stream_table(text)",
        "refresh_stream_table(text)",
        "repair_stream_table(text)",
        "resume_stream_table(text)",
        "set_stream_table_refresh_policy(text, text)",
        "set_stream_table_sla(text, interval)",
        "set_stream_table_storage_policy(text, boolean, text)",
        "stat_reset(bigint)",
    ] {
        db.execute(&format!(
            "GRANT EXECUTE ON FUNCTION pgtrickle.{function} TO {role}"
        ))
        .await;
    }
}

async fn create_owned_stream(db: &E2eDb, role: &str, name: &str, query: &str) {
    let sql = format!(
        "SELECT pgtrickle.create_stream_table('{name}', '{query}', refresh_mode => 'FULL')"
    );
    let result = db
        .try_execute_with_role(&format!("SET ROLE {role}"), &sql, "RESET ROLE")
        .await;
    assert!(
        result.is_ok(),
        "stream creation failed for {role}: {result:?}"
    );
    assert_eq!(db.count(&format!("public.{name}")).await, 1);
}

#[tokio::test]
async fn test_v08710_owner_lifecycle_enforces_caller_owner() {
    let db = E2eDb::new().await.with_extension().await;
    create_role(&db, "v8710_owner").await;
    create_role(&db, "v8710_other").await;
    grant_lifecycle_apis(&db, "v8710_owner").await;
    grant_lifecycle_apis(&db, "v8710_other").await;

    db.execute("CREATE TABLE v8710_source (id integer primary key)")
        .await;
    db.execute("INSERT INTO v8710_source VALUES (1)").await;
    db.execute("GRANT SELECT ON v8710_source TO v8710_owner")
        .await;
    db.execute("GRANT SELECT ON v8710_source TO v8710_other")
        .await;
    create_owned_stream(
        &db,
        "v8710_owner",
        "v8710_stream",
        "SELECT id FROM v8710_source",
    )
    .await;
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'v8710_stream'",
        )
        .await;

    let owner_calls = [
        "SELECT pgtrickle.refresh_stream_table('v8710_stream')",
        "SELECT pgtrickle.set_stream_table_refresh_policy('v8710_stream', 'FULL')",
        "SELECT pgtrickle.set_stream_table_storage_policy('v8710_stream', false, 'hot')",
        "SELECT pgtrickle.set_stream_table_sla('v8710_stream', '1 second')",
        &format!("SELECT pgtrickle.stat_reset({pgt_id})"),
        "SELECT pgtrickle.repair_stream_table('v8710_stream')",
        "SELECT pgtrickle.pause_stream_table('v8710_stream')",
        "SELECT pgtrickle.resume_stream_table('v8710_stream')",
    ];
    for sql in owner_calls {
        let result = db
            .try_execute_with_role("SET ROLE v8710_owner", sql, "RESET ROLE")
            .await;
        assert!(
            result.is_ok(),
            "owner lifecycle call failed: {sql}: {result:?}"
        );
    }

    let other_calls = [
        "SELECT pgtrickle.refresh_stream_table('v8710_stream')",
        "SELECT pgtrickle.set_stream_table_refresh_policy('v8710_stream', 'FULL')",
        "SELECT pgtrickle.set_stream_table_storage_policy('v8710_stream', false, 'hot')",
        "SELECT pgtrickle.set_stream_table_sla('v8710_stream', '1 second')",
        &format!("SELECT pgtrickle.stat_reset({pgt_id})"),
        "SELECT pgtrickle.repair_stream_table('v8710_stream')",
        "SELECT pgtrickle.pause_stream_table('v8710_stream')",
        "SELECT pgtrickle.resume_stream_table('v8710_stream')",
    ];
    for sql in other_calls {
        let result = db
            .try_execute_with_role("SET ROLE v8710_other", sql, "RESET ROLE")
            .await;
        assert!(
            result.is_err(),
            "non-owner lifecycle call unexpectedly succeeded: {sql}"
        );
    }
}

#[tokio::test]
async fn test_v08710_bulk_lifecycle_authorizes_all_targets_before_mutation() {
    let db = E2eDb::new().await.with_extension().await;
    create_role(&db, "v8710_bulk_a").await;
    create_role(&db, "v8710_bulk_b").await;
    grant_lifecycle_apis(&db, "v8710_bulk_a").await;
    grant_lifecycle_apis(&db, "v8710_bulk_b").await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.bulk_alter_stream_tables(text[], json) TO v8710_bulk_a",
    )
    .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.bulk_drop_stream_tables(text[]) TO v8710_bulk_a",
    )
    .await;

    db.execute("CREATE TABLE v8710_bulk_source (id integer primary key)")
        .await;
    db.execute("INSERT INTO v8710_bulk_source VALUES (1)").await;
    db.execute("GRANT SELECT ON v8710_bulk_source TO v8710_bulk_a, v8710_bulk_b")
        .await;
    create_owned_stream(
        &db,
        "v8710_bulk_a",
        "v8710_bulk_a_stream",
        "SELECT id FROM v8710_bulk_source",
    )
    .await;
    create_owned_stream(
        &db,
        "v8710_bulk_b",
        "v8710_bulk_b_stream",
        "SELECT id FROM v8710_bulk_source",
    )
    .await;

    let altered = db
        .try_execute_with_role(
            "SET ROLE v8710_bulk_a",
            "SELECT pgtrickle.bulk_alter_stream_tables(ARRAY['v8710_bulk_a_stream'], '{\"schedule\":\"2m\"}'::json)",
            "RESET ROLE",
        )
        .await;
    assert!(
        altered.is_ok(),
        "owned bulk alter unexpectedly failed: {altered:?}"
    );

    let before: Vec<String> = db
        .query_scalar::<String>("SELECT string_agg(COALESCE(schedule::text, '<null>'), ',' ORDER BY pgt_name) FROM pgtrickle.pgt_stream_tables WHERE pgt_name LIKE 'v8710_bulk_%_stream'")
        .await
        .split(',')
        .map(str::to_owned)
        .collect();
    let mixed = db
        .try_execute_with_role(
            "SET ROLE v8710_bulk_a",
            "SELECT pgtrickle.bulk_alter_stream_tables(ARRAY['v8710_bulk_a_stream', 'v8710_bulk_b_stream'], '{\"schedule\":\"2m\"}'::json)",
            "RESET ROLE",
        )
        .await;
    assert!(
        mixed.is_err(),
        "mixed-owner bulk alter unexpectedly succeeded"
    );

    for names in [
        "ARRAY['v8710_bulk_a_stream', 'v8710_bulk_a_stream']",
        "ARRAY['v8710_bulk_a_stream', 'v8710_missing_stream']",
    ] {
        let result = db
            .try_execute_with_role(
                "SET ROLE v8710_bulk_a",
                &format!(
                    "SELECT pgtrickle.bulk_alter_stream_tables({names}, '{{\"schedule\":\"2m\"}}'::json)"
                ),
                "RESET ROLE",
            )
            .await;
        assert!(
            result.is_err(),
            "invalid bulk target set unexpectedly succeeded"
        );
    }

    let after: Vec<String> = db
        .query_scalar::<String>("SELECT string_agg(COALESCE(schedule::text, '<null>'), ',' ORDER BY pgt_name) FROM pgtrickle.pgt_stream_tables WHERE pgt_name LIKE 'v8710_bulk_%_stream'")
        .await
        .split(',')
        .map(str::to_owned)
        .collect();
    assert_eq!(after, before, "failed bulk calls partially mutated state");

    let dropped = db
        .try_execute_with_role(
            "SET ROLE v8710_bulk_a",
            "SELECT pgtrickle.bulk_drop_stream_tables(ARRAY['v8710_bulk_a_stream'])",
            "RESET ROLE",
        )
        .await;
    assert!(
        dropped.is_ok(),
        "owned bulk drop unexpectedly failed: {dropped:?}"
    );
    assert_eq!(
        db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'v8710_bulk_a_stream'")
            .await,
        0
    );
    assert_eq!(db.count("public.v8710_bulk_b_stream").await, 1);
}

#[tokio::test]
async fn test_v08710_lifecycle_preflight_reports_exact_missing_grants() {
    let db = E2eDb::new().await.with_extension().await;
    create_role(&db, "v8710_preflight_owner").await;
    db.execute("CREATE SCHEMA v8710_input AUTHORIZATION postgres")
        .await;
    db.execute("CREATE TABLE v8710_input.source (id integer primary key)")
        .await;
    db.execute("INSERT INTO v8710_input.source VALUES (1)")
        .await;
    db.execute("GRANT USAGE ON SCHEMA v8710_input TO v8710_preflight_owner")
        .await;
    db.execute("GRANT SELECT ON v8710_input.source TO v8710_preflight_owner")
        .await;
    create_owned_stream(
        &db,
        "v8710_preflight_owner",
        "v8710_preflight_stream",
        "SELECT id FROM v8710_input.source",
    )
    .await;
    let before: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables")
        .await;

    db.execute("REVOKE SELECT ON v8710_input.source FROM v8710_preflight_owner")
        .await;
    db.execute("REVOKE USAGE ON SCHEMA v8710_input FROM v8710_preflight_owner")
        .await;
    let report: String = db
        .query_scalar("SELECT pgtrickle.lifecycle_preflight()")
        .await;
    assert!(report.contains("\"ok\":false"));
    assert!(report.contains("GRANT SELECT ON TABLE v8710_input.source TO v8710_preflight_owner;"));
    assert!(report.contains("GRANT USAGE ON SCHEMA v8710_input TO v8710_preflight_owner;"));
    assert_eq!(
        db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.pgt_stream_tables")
            .await,
        before
    );

    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.lifecycle_preflight() TO v8710_preflight_owner",
    )
    .await;
    let denied = db
        .try_execute_with_role(
            "SET ROLE v8710_preflight_owner",
            "SELECT pgtrickle.lifecycle_preflight()",
            "RESET ROLE",
        )
        .await;
    assert!(
        denied.is_err(),
        "non-superuser preflight unexpectedly succeeded"
    );
}
