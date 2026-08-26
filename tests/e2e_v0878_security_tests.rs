//! v0.87.8 refresh execution-identity probes.

mod e2e;

use e2e::E2eDb;

async fn grant_stream_create(db: &E2eDb, role: &str) {
    db.execute(&format!(
        "GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table(\
         text, text, text, text, boolean, text, text, text, boolean, boolean, \
         text, integer, double precision, text, boolean, text, integer, text) TO {role}"
    ))
    .await;
}

async fn create_identity_probe(db: &E2eDb) {
    db.execute(
        "CREATE FUNCTION v878_identity() RETURNS text \
         LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER AS $fn$ \
         BEGIN RETURN current_user::text; END $fn$",
    )
    .await;
}

/// Initial, full, differential, Top-K, and IMMEDIATE evaluation all use the
/// storage owner rather than the privileged refresh caller or DML issuer.
#[tokio::test]
async fn test_v0878_refresh_paths_run_as_stream_owner() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE v878_owner LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO v878_owner")
        .await;
    db.execute("GRANT CREATE ON SCHEMA public TO v878_owner")
        .await;
    grant_stream_create(&db, "v878_owner").await;
    create_identity_probe(&db).await;
    db.execute("CREATE TABLE v878_src (id integer primary key, score integer)")
        .await;
    db.execute("INSERT INTO v878_src VALUES (1, 10)").await;
    db.execute("GRANT SELECT ON v878_src TO v878_owner").await;

    let created = db
        .try_execute_with_role(
            "SET ROLE v878_owner",
            "SELECT pgtrickle.create_stream_table(\
                'v878_st', \
                'SELECT id, v878_identity() AS evaluated_as FROM v878_src', \
                refresh_mode => 'DIFFERENTIAL')",
            "RESET ROLE",
        )
        .await;
    assert!(
        created.is_ok(),
        "owner should be able to create the stream table: {created:?}"
    );

    for statement in [
        "SELECT pgtrickle.create_stream_table(\
            'v878_full_st', \
            'SELECT id, v878_identity() AS evaluated_as FROM v878_src', \
            refresh_mode => 'FULL')",
        "SELECT pgtrickle.create_stream_table(\
            'v878_topk_st', \
            'SELECT id, v878_identity() AS evaluated_as FROM v878_src ORDER BY score DESC LIMIT 1', \
            refresh_mode => 'DIFFERENTIAL')",
        "SELECT pgtrickle.create_stream_table(\
            'v878_immediate_st', \
            'SELECT id, v878_identity() AS evaluated_as FROM v878_src', \
            refresh_mode => 'IMMEDIATE')",
    ] {
        let result = db
            .try_execute_with_role("SET ROLE v878_owner", statement, "RESET ROLE")
            .await;
        assert!(
            result.is_ok(),
            "owner-path stream creation failed: {result:?}"
        );
    }

    let initial: String = db
        .query_scalar("SELECT evaluated_as FROM public.v878_st WHERE id = 1")
        .await;
    assert_eq!(initial, "v878_owner");

    db.execute("INSERT INTO v878_src VALUES (2, 20)").await;
    db.refresh_st("v878_st").await;
    db.refresh_st("v878_full_st").await;
    db.refresh_st("v878_topk_st").await;

    let second: String = db
        .query_scalar("SELECT evaluated_as FROM public.v878_st WHERE id = 2")
        .await;
    assert_eq!(second, "v878_owner");

    for table in ["v878_full_st", "v878_topk_st", "v878_immediate_st"] {
        let evaluated_as: String = db
            .query_scalar(&format!(
                "SELECT evaluated_as FROM public.{table} WHERE id = 2"
            ))
            .await;
        assert_eq!(evaluated_as, "v878_owner", "wrong identity for {table}");
    }
}

/// Source RLS is evaluated as the stable stream owner on initial and full
/// refreshes, independent of the privileged caller. RLS-3: DIFFERENTIAL is
/// unsafe over an RLS-protected source, so AUTO mode is used and must
/// downgrade to FULL instead of erroring.
#[tokio::test]
async fn test_v0878_source_rls_uses_stream_owner() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE v878_rls_owner LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO v878_rls_owner")
        .await;
    db.execute("GRANT CREATE ON SCHEMA public TO v878_rls_owner")
        .await;
    grant_stream_create(&db, "v878_rls_owner").await;
    create_identity_probe(&db).await;
    db.execute("CREATE TABLE v878_rls_src (id integer primary key, tenant text NOT NULL)")
        .await;
    db.execute(
        "INSERT INTO v878_rls_src VALUES \
         (1, 'v878_rls_owner'), (2, 'hidden')",
    )
    .await;
    db.execute("ALTER TABLE v878_rls_src ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute(
        "CREATE POLICY v878_owner_rows ON v878_rls_src \
         TO v878_rls_owner USING (tenant = current_user)",
    )
    .await;
    db.execute("GRANT SELECT ON v878_rls_src TO v878_rls_owner")
        .await;

    for (name, mode) in [("v878_rls_auto", "AUTO"), ("v878_rls_full", "FULL")] {
        let statement = format!(
            "SELECT pgtrickle.create_stream_table(\
                '{name}', \
                'SELECT id, tenant, v878_identity() AS evaluated_as FROM v878_rls_src', \
                refresh_mode => '{mode}')"
        );
        let result = db
            .try_execute_with_role("SET ROLE v878_rls_owner", &statement, "RESET ROLE")
            .await;
        assert!(result.is_ok(), "RLS stream creation failed: {result:?}");
        assert_eq!(db.count(&format!("public.{name}")).await, 1);
    }

    db.execute(
        "INSERT INTO v878_rls_src VALUES \
         (3, 'v878_rls_owner'), (4, 'hidden')",
    )
    .await;
    db.refresh_st("v878_rls_auto").await;
    db.refresh_st("v878_rls_full").await;

    for table in ["v878_rls_auto", "v878_rls_full"] {
        assert_eq!(db.count(&format!("public.{table}")).await, 2);
        let wrong_identity: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM public.{table} \
                 WHERE evaluated_as <> 'v878_rls_owner' OR tenant = 'hidden'"
            ))
            .await;
        assert_eq!(wrong_identity, 0, "RLS/identity mismatch for {table}");
    }
}

/// Owner-authored SQL cannot cross the temporary CDC stage boundary into the
/// private change-buffer schema; failure preserves all durable refresh state.
#[tokio::test]
async fn test_v0878_private_buffer_probe_rolls_back_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE v878_probe_owner LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO v878_probe_owner")
        .await;
    db.execute("GRANT CREATE ON SCHEMA public TO v878_probe_owner")
        .await;
    grant_stream_create(&db, "v878_probe_owner").await;
    db.execute("CREATE TABLE v878_probe_src (id integer primary key)")
        .await;
    db.execute("INSERT INTO v878_probe_src VALUES (1)").await;
    db.execute("GRANT SELECT ON v878_probe_src TO v878_probe_owner")
        .await;

    db.create_st(
        "v878_probe_seed",
        "SELECT id FROM v878_probe_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let source_oid: i64 = db
        .query_scalar("SELECT 'v878_probe_src'::regclass::oid::bigint")
        .await;
    let buffer = db.change_buffer_table(source_oid).await;
    db.execute(&format!(
        "CREATE FUNCTION v878_probe_private(i integer) RETURNS text \
         LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER AS $fn$ \
         DECLARE seen bigint; BEGIN \
           IF i = 2 THEN EXECUTE 'SELECT count(*) FROM {buffer}' INTO seen; END IF; \
           RETURN current_user::text; \
         END $fn$"
    ))
    .await;

    let created = db
        .try_execute_with_role(
            "SET ROLE v878_probe_owner",
            "SELECT pgtrickle.create_stream_table(\
                'v878_probe_st', \
                'SELECT id, v878_probe_private(id) AS evaluated_as FROM v878_probe_src', \
                refresh_mode => 'DIFFERENTIAL')",
            "RESET ROLE",
        )
        .await;
    assert!(created.is_ok(), "probe stream creation failed: {created:?}");

    db.execute("INSERT INTO v878_probe_src VALUES (2)").await;
    let frontier_before: String = db
        .query_scalar(
            "SELECT frontier::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'v878_probe_st'",
        )
        .await;
    let history_before: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
             WHERE s.pgt_name = 'v878_probe_st'",
        )
        .await;
    let buffer_before = db.count(&buffer).await;

    let failed = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('v878_probe_st')")
        .await;
    assert!(failed.is_err(), "private CDC probe unexpectedly succeeded");
    assert_eq!(db.count("public.v878_probe_st").await, 1);
    assert_eq!(db.count(&buffer).await, buffer_before);
    assert_eq!(
        db.query_scalar::<String>(
            "SELECT frontier::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'v878_probe_st'"
        )
        .await,
        frontier_before
    );
    assert_eq!(
        db.query_scalar::<i64>(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
             WHERE s.pgt_name = 'v878_probe_st'"
        )
        .await,
        history_before
    );
}

/// The reinitialize/full-fallback boundary must retain the stream owner.
#[tokio::test]
async fn test_v0878_reinitialize_uses_stream_owner() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("DO $$ BEGIN CREATE ROLE v878_reinit_owner LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO v878_reinit_owner")
        .await;
    db.execute("GRANT CREATE ON SCHEMA public TO v878_reinit_owner")
        .await;
    grant_stream_create(&db, "v878_reinit_owner").await;
    create_identity_probe(&db).await;
    db.execute("CREATE TABLE v878_reinit_src (id integer primary key)")
        .await;
    db.execute("INSERT INTO v878_reinit_src VALUES (1)").await;
    db.execute("GRANT SELECT ON v878_reinit_src TO v878_reinit_owner")
        .await;
    let create = "SELECT pgtrickle.create_stream_table(\
        'v878_reinit_st', \
        'SELECT id, v878_identity() AS evaluated_as FROM v878_reinit_src', \
        refresh_mode => 'DIFFERENTIAL')";
    assert!(
        db.try_execute_with_role("SET ROLE v878_reinit_owner", create, "RESET ROLE")
            .await
            .is_ok()
    );
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = true WHERE pgt_name = 'v878_reinit_st'").await;
    db.refresh_st("v878_reinit_st").await;
    let identity: String = db
        .query_scalar("SELECT evaluated_as FROM v878_reinit_st WHERE id = 1")
        .await;
    assert_eq!(identity, "v878_reinit_owner");
}

/// Revoking the owner's source privilege fails before durable state changes or
/// temporary staging relations are left behind.
#[tokio::test]
async fn test_v0878_revoked_source_preserves_state_and_stages() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("DO $$ BEGIN CREATE ROLE v878_revoke_owner LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO v878_revoke_owner")
        .await;
    db.execute("GRANT CREATE ON SCHEMA public TO v878_revoke_owner")
        .await;
    grant_stream_create(&db, "v878_revoke_owner").await;
    db.execute("CREATE TABLE v878_revoke_src (id integer primary key)")
        .await;
    db.execute("INSERT INTO v878_revoke_src VALUES (1)").await;
    db.execute("GRANT SELECT ON v878_revoke_src TO v878_revoke_owner")
        .await;
    let create = "SELECT pgtrickle.create_stream_table('v878_revoke_st', 'SELECT id FROM v878_revoke_src', refresh_mode => 'DIFFERENTIAL')";
    assert!(
        db.try_execute_with_role("SET ROLE v878_revoke_owner", create, "RESET ROLE")
            .await
            .is_ok()
    );
    db.execute("INSERT INTO v878_revoke_src VALUES (2)").await;
    let before: i64 = db.count("v878_revoke_st").await;
    db.execute("REVOKE SELECT ON v878_revoke_src FROM v878_revoke_owner")
        .await;
    let failed = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('v878_revoke_st')")
        .await;
    assert!(
        failed.is_err(),
        "refresh should fail after source access is revoked"
    );
    assert_eq!(db.count("v878_revoke_st").await, before);
    let temp_count: i64 = db.query_scalar("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname LIKE 'pg_temp_%'").await;
    assert_eq!(
        temp_count, 0,
        "owner refresh must not leak temporary stages"
    );
}
