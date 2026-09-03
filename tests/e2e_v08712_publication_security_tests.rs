//! v0.87.12 publication privilege and ownership contract probes.

mod e2e;

use e2e::E2eDb;

const CREATE_STREAM_ARGS: &str = "text, text, text, text, boolean, text, text, text, boolean, boolean, \
     text, integer, double precision, text, boolean, text, integer, text, text";

async fn grant_publication_apis(db: &E2eDb, role: &str) {
    db.execute(&format!("GRANT USAGE ON SCHEMA pgtrickle TO {role}"))
        .await;
    db.execute(&format!("GRANT CREATE ON SCHEMA public TO {role}"))
        .await;
    db.execute(&format!(
        "DO $$ BEGIN EXECUTE format('GRANT CREATE ON DATABASE %I TO {role}', current_database()); END $$"
    ))
    .await;
    db.execute(&format!(
        "GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table({CREATE_STREAM_ARGS}) TO {role}"
    ))
    .await;
    for function in [
        "stream_table_to_publication(text)",
        "drop_stream_table_publication(text)",
        "drop_stream_table(text, boolean)",
        "repair_stream_table(text)",
    ] {
        db.execute(&format!(
            "GRANT EXECUTE ON FUNCTION pgtrickle.{function} TO {role}"
        ))
        .await;
    }
}

async fn create_stream(db: &E2eDb, role: &str, name: &str) {
    let result = db
        .try_execute_with_role(
            &format!("SET ROLE {role}"),
            &format!(
                "SELECT pgtrickle.create_stream_table('{name}', 'SELECT id FROM v8712_source', refresh_mode => 'FULL')"
            ),
            "RESET ROLE",
        )
        .await;
    assert!(result.is_ok(), "stream creation failed: {result:?}");
}

async fn create_bound_stream(db: &E2eDb, role: &str, stream: &str) {
    db.execute(&format!(
        "DO $$ BEGIN CREATE ROLE {role} LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    ))
    .await;
    grant_publication_apis(db, role).await;
    db.execute("CREATE TABLE v8712_source (id integer primary key)")
        .await;
    db.execute(&format!("GRANT SELECT ON v8712_source TO {role}"))
        .await;
    create_stream(db, role, stream).await;
    let result = db
        .try_execute_with_role(
            &format!("SET ROLE {role}"),
            &format!("SELECT pgtrickle.stream_table_to_publication('{stream}')"),
            "RESET ROLE",
        )
        .await;
    assert!(result.is_ok(), "publication creation failed: {result:?}");
}

#[tokio::test]
async fn test_v08712_publication_uses_caller_privileges_and_owner() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "DO $$ BEGIN CREATE ROLE v8712_pub_owner LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$",
    )
    .await;
    db.execute(
        "DO $$ BEGIN CREATE ROLE v8712_pub_other LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$",
    )
    .await;
    grant_publication_apis(&db, "v8712_pub_owner").await;
    grant_publication_apis(&db, "v8712_pub_other").await;
    db.execute("CREATE TABLE v8712_source (id integer primary key)")
        .await;
    db.execute("GRANT SELECT ON v8712_source TO v8712_pub_owner")
        .await;
    create_stream(&db, "v8712_pub_owner", "v8712_stream").await;

    let result = db
        .try_execute_with_role(
            "SET ROLE v8712_pub_owner",
            "SELECT pgtrickle.stream_table_to_publication('v8712_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(
        result.is_ok(),
        "authorized publication creation failed: {result:?}"
    );

    let owner: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(pubowner)::text FROM pg_publication \
             WHERE pubname = 'pgt_pub_v8712_stream'",
        )
        .await;
    assert_eq!(owner, "v8712_pub_owner");
    let binding_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_publication_bindings b \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             JOIN pg_publication p ON p.oid = b.publication_oid \
             WHERE st.pgt_name = 'v8712_stream' \
               AND b.stream_relid = st.pgt_relid \
               AND b.publication_name = p.pubname::text \
               AND b.publication_owner_oid = p.pubowner \
               AND b.expected_relation_oids = ARRAY[st.pgt_relid]::oid[]",
        )
        .await;
    assert_eq!(binding_count, 1);

    let denied = db
        .try_execute_with_role(
            "SET ROLE v8712_pub_other",
            "SELECT pgtrickle.drop_stream_table_publication('v8712_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(
        denied.is_err(),
        "non-owner publication drop unexpectedly succeeded"
    );
}

#[tokio::test]
async fn test_v08712_publication_requires_database_create_and_leaves_no_partial_state() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "DO $$ BEGIN CREATE ROLE v8712_no_create LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$",
    )
    .await;
    grant_publication_apis(&db, "v8712_no_create").await;
    db.execute(
        "DO $$ BEGIN EXECUTE format('REVOKE CREATE ON DATABASE %I FROM v8712_no_create', current_database()); END $$",
    )
    .await;
    db.execute("CREATE TABLE v8712_source (id integer primary key)")
        .await;
    db.execute("GRANT SELECT ON v8712_source TO v8712_no_create")
        .await;
    create_stream(&db, "v8712_no_create", "v8712_denied_stream").await;

    let result = db
        .try_execute_with_role(
            "SET ROLE v8712_no_create",
            "SELECT pgtrickle.stream_table_to_publication('v8712_denied_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(
        result.is_err(),
        "publication creation without CREATE succeeded"
    );

    let publication_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_publication \
             WHERE pubname = 'pgt_pub_v8712_denied_stream'",
        )
        .await;
    assert_eq!(publication_count, 0);
    let binding: Option<String> = db
        .query_scalar_opt(
            "SELECT downstream_publication_name FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'v8712_denied_stream'",
        )
        .await;
    assert_eq!(binding, None);
}

#[tokio::test]
async fn test_v08712_publication_functions_pin_definer_boundary() {
    let db = E2eDb::new().await.with_extension().await;
    let security_definer_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_proc p \
             JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'pgtrickle' \
               AND p.proname IN ('stream_table_to_publication', 'drop_stream_table_publication') \
               AND p.prosecdef",
        )
        .await;
    assert_eq!(security_definer_count, 2);
    let locked_paths: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_proc p \
             JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'pgtrickle' \
               AND p.proname IN ('stream_table_to_publication', 'drop_stream_table_publication') \
               AND 'search_path=pgtrickle, pg_catalog, pg_temp' = ANY(p.proconfig)",
        )
        .await;
    assert_eq!(locked_paths, 2);
}

#[tokio::test]
async fn test_v08712_publication_binding_catalog_shape_is_compatible() {
    let db = E2eDb::new().await.with_extension().await;
    let columns: i64 = db
        .query_scalar(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_schema = 'pgtrickle' \
               AND table_name = 'pgt_publication_bindings' \
               AND column_name IN ('pgt_id', 'stream_relid', 'publication_oid', \
                   'publication_name', 'publication_owner_oid', 'expected_relation_oids')",
        )
        .await;
    assert_eq!(columns, 6);
}

#[tokio::test]
async fn test_v08712_renamed_publication_is_not_dropped_by_name() {
    let db = E2eDb::new().await.with_extension().await;
    create_bound_stream(&db, "v8712_rename_owner", "v8712_rename_stream").await;
    db.execute("ALTER PUBLICATION pgt_pub_v8712_rename_stream RENAME TO v8712_renamed_publication")
        .await;

    let result = db
        .try_execute_with_role(
            "SET ROLE v8712_rename_owner",
            "SELECT pgtrickle.drop_stream_table_publication('v8712_rename_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(
        result.is_err(),
        "renamed publication was unexpectedly dropped"
    );
    let report: String = db
        .query_scalar("SELECT pgtrickle.lifecycle_preflight()")
        .await;
    assert!(report.contains("publication_renamed"), "{report}");
    assert_eq!(
        db.count("pg_publication WHERE pubname = 'v8712_renamed_publication'")
            .await,
        1
    );
    assert_eq!(
        db.count("pgtrickle.pgt_publication_bindings WHERE publication_name = 'pgt_pub_v8712_rename_stream'")
            .await,
        1
    );
}

#[tokio::test]
async fn test_v08712_same_name_recreation_is_not_dropped() {
    let db = E2eDb::new().await.with_extension().await;
    create_bound_stream(&db, "v8712_reuse_owner", "v8712_reuse_stream").await;
    db.execute("CREATE TABLE v8712_replacement (id integer primary key)")
        .await;
    db.execute_seq(&[
        "DROP PUBLICATION pgt_pub_v8712_reuse_stream",
        "CREATE PUBLICATION pgt_pub_v8712_reuse_stream FOR TABLE v8712_replacement",
    ])
    .await;

    let result = db
        .try_execute_with_role(
            "SET ROLE v8712_reuse_owner",
            "SELECT pgtrickle.drop_stream_table_publication('v8712_reuse_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(
        result.is_err(),
        "same-name replacement was unexpectedly dropped"
    );
    let report: String = db
        .query_scalar("SELECT pgtrickle.lifecycle_preflight()")
        .await;
    assert!(report.contains("publication_name_reused"), "{report}");
    assert_eq!(
        db.count("pg_publication WHERE pubname = 'pgt_pub_v8712_reuse_stream'")
            .await,
        1
    );
    let replacement_exists: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relname = 'v8712_replacement'",
        )
        .await;
    assert_eq!(
        replacement_exists, 1,
        "replacement table must remain available"
    );
}

#[tokio::test]
async fn test_v08712_stream_drop_and_repair_reject_stale_binding() {
    let db = E2eDb::new().await.with_extension().await;
    create_bound_stream(&db, "v8712_lifecycle_owner", "v8712_lifecycle_stream").await;
    db.execute(
        "ALTER PUBLICATION pgt_pub_v8712_lifecycle_stream RENAME TO v8712_lifecycle_renamed",
    )
    .await;

    let drop_result = db
        .try_execute_with_role(
            "SET ROLE v8712_lifecycle_owner",
            "SELECT pgtrickle.drop_stream_table('v8712_lifecycle_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(
        drop_result.is_err(),
        "stream drop unexpectedly ignored stale binding"
    );

    let repair_result = db
        .try_execute_with_role(
            "SET ROLE v8712_lifecycle_owner",
            "SELECT pgtrickle.repair_stream_table('v8712_lifecycle_stream')",
            "RESET ROLE",
        )
        .await;
    assert!(
        repair_result.is_err(),
        "repair unexpectedly ignored stale binding"
    );
    let report: String = db
        .query_scalar("SELECT pgtrickle.lifecycle_preflight()")
        .await;
    assert!(report.contains("publication_renamed"), "{report}");
}

#[tokio::test]
async fn test_v08712_bulk_drop_prevalidates_all_bindings_before_mutation() {
    let db = E2eDb::new().await.with_extension().await;
    let role = "v8712_bulk_owner";
    db.execute(&format!(
        "DO $$ BEGIN CREATE ROLE {role} LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    ))
    .await;
    grant_publication_apis(&db, role).await;
    db.execute(&format!(
        "GRANT EXECUTE ON FUNCTION pgtrickle.bulk_drop_stream_tables(text[]) TO {role}"
    ))
    .await;
    db.execute("CREATE TABLE v8712_bulk_source (id integer primary key)")
        .await;
    db.execute(&format!("GRANT SELECT ON v8712_bulk_source TO {role}"))
        .await;
    for stream in ["v8712_bulk_a", "v8712_bulk_b"] {
        let result = db
            .try_execute_with_role(
                &format!("SET ROLE {role}"),
                &format!(
                    "SELECT pgtrickle.create_stream_table('{stream}', 'SELECT id FROM v8712_bulk_source', refresh_mode => 'FULL')"
                ),
                "RESET ROLE",
            )
            .await;
        assert!(result.is_ok(), "stream creation failed: {result:?}");
        let result = db
            .try_execute_with_role(
                &format!("SET ROLE {role}"),
                &format!("SELECT pgtrickle.stream_table_to_publication('{stream}')"),
                "RESET ROLE",
            )
            .await;
        assert!(result.is_ok(), "publication creation failed: {result:?}");
    }
    db.execute("ALTER PUBLICATION pgt_pub_v8712_bulk_b RENAME TO v8712_bulk_renamed")
        .await;

    let result = db
        .try_execute_with_role(
            &format!("SET ROLE {role}"),
            "SELECT pgtrickle.bulk_drop_stream_tables(ARRAY['v8712_bulk_a', 'v8712_bulk_b'])",
            "RESET ROLE",
        )
        .await;
    assert!(result.is_err(), "bulk drop ignored the stale binding");
    let remaining_streams: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relname IN ('v8712_bulk_a', 'v8712_bulk_b')",
        )
        .await;
    assert_eq!(remaining_streams, 2, "bulk prevalidation must be atomic");
    assert_eq!(
        db.count("pg_publication WHERE pubname = 'v8712_bulk_renamed'")
            .await,
        1
    );
}
