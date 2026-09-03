//! v0.87.11 snapshot ownership, target-schema, and provenance probes.

mod e2e;

use e2e::E2eDb;

const CREATE_STREAM_ARGS: &str = "text, text, text, text, boolean, text, text, text, boolean, boolean, \
     text, integer, double precision, text, boolean, text, integer, text, text";

async fn create_snapshot_role(db: &E2eDb, role: &str) {
    db.execute(&format!(
        "DO $$ BEGIN CREATE ROLE {role} LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    ))
    .await;
    for grant in [
        format!("GRANT USAGE ON SCHEMA pgtrickle TO {role}"),
        format!("GRANT CREATE ON SCHEMA public TO {role}"),
        format!(
            "GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table({CREATE_STREAM_ARGS}) TO {role}"
        ),
        format!("GRANT EXECUTE ON FUNCTION pgtrickle.snapshot_stream_table(text, text) TO {role}"),
        format!("GRANT EXECUTE ON FUNCTION pgtrickle.restore_from_snapshot(text, text) TO {role}"),
        format!("GRANT EXECUTE ON FUNCTION pgtrickle.list_snapshots(text) TO {role}"),
        format!("GRANT EXECUTE ON FUNCTION pgtrickle.drop_snapshot(text) TO {role}"),
    ] {
        db.execute(&grant).await;
    }
}

async fn create_owned_stream(db: &E2eDb, role: &str, name: &str) {
    let result = db
        .try_execute_with_role(
            &format!("SET ROLE {role}"),
            &format!(
                "SELECT pgtrickle.create_stream_table('{name}', 'SELECT id FROM v8711_source', refresh_mode => 'FULL')"
            ),
            "RESET ROLE",
        )
        .await;
    assert!(result.is_ok(), "stream creation failed: {result:?}");
    assert_eq!(db.count(&format!("public.{name}")).await, 1);
}

async fn snapshot_name(db: &E2eDb, stream_name: &str) -> String {
    db.query_scalar(&format!(
        "SELECT format('%I.%I', snapshot_schema, snapshot_table) \
         FROM pgtrickle.pgt_snapshots s \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE st.pgt_name = '{stream_name}' \
         ORDER BY s.created_at DESC LIMIT 1"
    ))
    .await
}

#[tokio::test]
async fn test_v08711_snapshot_target_schema_and_owner_policy() {
    let db = E2eDb::new().await.with_extension().await;
    create_snapshot_role(&db, "v8711_snapshot_owner").await;
    create_snapshot_role(&db, "v8711_snapshot_other").await;

    db.execute("CREATE TABLE v8711_source (id integer primary key)")
        .await;
    db.execute("INSERT INTO v8711_source VALUES (1), (2)").await;
    db.execute("ALTER TABLE v8711_source ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute("ALTER TABLE v8711_source FORCE ROW LEVEL SECURITY")
        .await;
    db.execute(
        "CREATE POLICY v8711_owner_rows ON v8711_source \
         FOR SELECT TO v8711_snapshot_owner USING (id = 1)",
    )
    .await;
    db.execute("GRANT SELECT ON v8711_source TO v8711_snapshot_owner")
        .await;
    db.execute("CREATE SCHEMA v8711_archive AUTHORIZATION postgres")
        .await;
    db.execute("GRANT USAGE, CREATE ON SCHEMA v8711_archive TO v8711_snapshot_owner")
        .await;
    create_owned_stream(&db, "v8711_snapshot_owner", "v8711_stream").await;

    let default_result = db
        .try_execute_with_role(
            "SET ROLE v8711_snapshot_owner",
            "SELECT pgtrickle.snapshot_stream_table('v8711_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(
        default_result.is_ok(),
        "default snapshot failed: {default_result:?}"
    );
    let default_snapshot = snapshot_name(&db, "v8711_stream").await;
    let default_owner: String = db
        .query_scalar(&format!(
            "SELECT pg_get_userbyid(c.relowner)::text FROM pg_class c \
             WHERE c.oid = '{default_snapshot}'::regclass"
        ))
        .await;
    assert_eq!(default_owner, "v8711_snapshot_owner");
    assert_eq!(db.count(&default_snapshot).await, 1);

    let custom_result = db
        .try_execute_with_role(
            "SET ROLE v8711_snapshot_owner",
            "SELECT pgtrickle.snapshot_stream_table('v8711_stream', 'v8711_archive.\"quoted snapshot\"')",
            "RESET ROLE",
        )
        .await;
    assert!(
        custom_result.is_ok(),
        "custom snapshot failed: {custom_result:?}"
    );
    let custom_owner: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(c.relowner)::text FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'v8711_archive' AND c.relname = 'quoted snapshot'",
        )
        .await;
    assert_eq!(custom_owner, "v8711_snapshot_owner");
    assert_eq!(db.count("v8711_archive.\"quoted snapshot\"").await, 1);

    db.execute("REVOKE USAGE, CREATE ON SCHEMA v8711_archive FROM v8711_snapshot_owner")
        .await;
    let denied_result = db
        .try_execute_with_role(
            "SET ROLE v8711_snapshot_owner",
            "SELECT pgtrickle.snapshot_stream_table('v8711_stream', 'v8711_archive.denied_snapshot')",
            "RESET ROLE",
        )
        .await;
    assert!(
        denied_result.is_err(),
        "custom target without schema privileges unexpectedly succeeded"
    );
}

#[tokio::test]
async fn test_v08711_snapshot_restore_select_and_transfer_policy() {
    let db = E2eDb::new().await.with_extension().await;
    create_snapshot_role(&db, "v8711_restore_owner").await;
    create_snapshot_role(&db, "v8711_restore_other").await;
    db.execute("CREATE TABLE v8711_source (id integer primary key)")
        .await;
    db.execute("INSERT INTO v8711_source VALUES (1)").await;
    db.execute("GRANT SELECT ON v8711_source TO v8711_restore_owner")
        .await;
    create_owned_stream(&db, "v8711_restore_owner", "v8711_restore_stream").await;

    let created = db
        .try_execute_with_role(
            "SET ROLE v8711_restore_owner",
            "SELECT pgtrickle.snapshot_stream_table('v8711_restore_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(created.is_ok(), "snapshot creation failed: {created:?}");
    let snapshot = snapshot_name(&db, "v8711_restore_stream").await;
    db.execute(&format!(
        "GRANT SELECT ON TABLE {snapshot} TO v8711_restore_other"
    ))
    .await;

    let not_destination_owner = db
        .try_execute_with_role(
            "SET ROLE v8711_restore_other",
            &format!(
                "SELECT pgtrickle.restore_from_snapshot('v8711_restore_stream', '{snapshot}')"
            ),
            "RESET ROLE",
        )
        .await;
    assert!(
        not_destination_owner.is_err(),
        "non-owner restore unexpectedly succeeded"
    );

    db.execute("ALTER TABLE public.v8711_restore_stream OWNER TO v8711_restore_other")
        .await;
    db.execute(&format!(
        "REVOKE SELECT ON TABLE {snapshot} FROM v8711_restore_other"
    ))
    .await;
    let no_select = db
        .try_execute_with_role(
            "SET ROLE v8711_restore_other",
            &format!(
                "SELECT pgtrickle.restore_from_snapshot('v8711_restore_stream', '{snapshot}')"
            ),
            "RESET ROLE",
        )
        .await;
    assert!(
        no_select.is_err(),
        "restore without snapshot SELECT succeeded"
    );

    db.execute(&format!(
        "GRANT SELECT ON TABLE {snapshot} TO v8711_restore_other"
    ))
    .await;
    let restored = db
        .try_execute_with_role(
            "SET ROLE v8711_restore_other",
            &format!(
                "SELECT pgtrickle.restore_from_snapshot('v8711_restore_stream', '{snapshot}')"
            ),
            "RESET ROLE",
        )
        .await;
    assert!(restored.is_ok(), "authorized restore failed: {restored:?}");

    let other_drop = db
        .try_execute_with_role(
            "SET ROLE v8711_restore_other",
            &format!("SELECT pgtrickle.drop_snapshot('{snapshot}')"),
            "RESET ROLE",
        )
        .await;
    assert!(
        other_drop.is_err(),
        "stream owner without snapshot ownership unexpectedly dropped it"
    );
    let owner_drop = db
        .try_execute_with_role(
            "SET ROLE v8711_restore_owner",
            &format!("SELECT pgtrickle.drop_snapshot('{snapshot}')"),
            "RESET ROLE",
        )
        .await;
    assert!(
        owner_drop.is_ok(),
        "snapshot owner could not drop after stream transfer: {owner_drop:?}"
    );
}

#[tokio::test]
async fn test_v08711_snapshot_name_reuse_rejected_without_dropping_lookalike() {
    let db = E2eDb::new().await.with_extension().await;
    create_snapshot_role(&db, "v8711_reuse_owner").await;
    db.execute("CREATE TABLE v8711_source (id integer primary key)")
        .await;
    db.execute("INSERT INTO v8711_source VALUES (1)").await;
    db.execute("GRANT SELECT ON v8711_source TO v8711_reuse_owner")
        .await;
    create_owned_stream(&db, "v8711_reuse_owner", "v8711_reuse_stream").await;
    let created = db
        .try_execute_with_role(
            "SET ROLE v8711_reuse_owner",
            "SELECT pgtrickle.snapshot_stream_table('v8711_reuse_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(created.is_ok(), "snapshot creation failed: {created:?}");
    let snapshot = snapshot_name(&db, "v8711_reuse_stream").await;

    db.execute(&format!("DROP TABLE {snapshot}")).await;
    db.execute(&format!("CREATE TABLE {snapshot} (id integer)"))
        .await;
    let rejected = db
        .try_execute_with_role(
            "SET ROLE v8711_reuse_owner",
            &format!("SELECT pgtrickle.drop_snapshot('{snapshot}')"),
            "RESET ROLE",
        )
        .await;
    assert!(rejected.is_err(), "name-reused relation was accepted");
    let still_there: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM {snapshot}"))
        .await;
    assert_eq!(still_there, 0);
}
