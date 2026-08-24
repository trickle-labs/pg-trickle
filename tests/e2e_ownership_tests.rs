//! TEST-6: Ownership-check privilege E2E tests (SEC-1).
//!
//! Validates that non-owner, non-superuser roles cannot drop or alter
//! stream tables they don't own, while superusers can operate on any ST.
//!
//! Uses two roles:
//! - `sec1_owner`: creates the stream table (becomes owner)
//! - `sec1_other`: a regular role that should be denied access
//! - postgres (superuser): should bypass ownership checks
//!
//! Runs in both the light and full E2E harnesses.

mod e2e;

use e2e::E2eDb;

/// Helper: create the two test roles and a source table owned by sec1_owner.
async fn setup_ownership_test(db: &E2eDb) {
    // Create roles — use EXCEPTION to handle the race where parallel tests try
    // to create the same cluster-level role simultaneously.  IF NOT EXISTS inside
    // a DO block is not atomic; we catch both duplicate_object (42710, role
    // already exists) and unique_violation (23505, concurrent INSERT race).
    db.execute(
        "DO $$ BEGIN CREATE ROLE sec1_owner LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute(
        "DO $$ BEGIN CREATE ROLE sec1_other LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;

    // Grant usage on extension schemas to both roles
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO sec1_owner, sec1_other")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle_changes TO sec1_owner, sec1_other")
        .await;
    db.execute("GRANT SELECT ON ALL TABLES IN SCHEMA pgtrickle TO sec1_owner, sec1_other")
        .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.drop_stream_table(text, boolean) \
         TO sec1_owner, sec1_other",
    )
    .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.alter_stream_table(\
            text, text, text, text, text, text, text, text, boolean, boolean, text, text, \
            bigint, integer, text, integer, double precision, text, double precision, text) \
         TO sec1_owner, sec1_other",
    )
    .await;

    // Create source table and grant access
    db.execute("CREATE TABLE sec1_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sec1_src VALUES (1, 'a'), (2, 'b')")
        .await;
    db.execute("GRANT ALL ON TABLE sec1_src TO sec1_owner, sec1_other")
        .await;

    // Create stream table as superuser, then transfer ownership to sec1_owner
    db.execute("SELECT pgtrickle.create_stream_table('sec1_st', 'SELECT id, val FROM sec1_src')")
        .await;
    db.execute("ALTER TABLE sec1_st OWNER TO sec1_owner").await;
}

/// TEST-6a: Non-owner cannot drop a stream table.
#[tokio::test]
async fn test_ownership_nonowner_drop_denied() {
    let db = E2eDb::new().await.with_extension().await;
    setup_ownership_test(&db).await;

    // Attempt to drop as sec1_other (non-owner, non-superuser).
    // Use try_execute_with_role to run SET ROLE / target / RESET ROLE on the
    // same connection (sqlx rejects multi-statement prepared statements).
    let result = db
        .try_execute_with_role(
            "SET ROLE sec1_other",
            "SELECT pgtrickle.drop_stream_table('sec1_st')",
            "RESET ROLE",
        )
        .await;

    assert!(
        result.is_err(),
        "Non-owner should not be able to drop a stream table"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("must be owner"),
        "Error should mention ownership: {err}"
    );
}

/// TEST-6b: Non-owner cannot alter a stream table.
#[tokio::test]
async fn test_ownership_nonowner_alter_denied() {
    let db = E2eDb::new().await.with_extension().await;
    setup_ownership_test(&db).await;

    // Attempt to alter as sec1_other (non-owner, non-superuser).
    // Use try_execute_with_role to avoid multi-statement prepared-statement error.
    let result = db
        .try_execute_with_role(
            "SET ROLE sec1_other",
            "SELECT pgtrickle.alter_stream_table('sec1_st', schedule => '30s')",
            "RESET ROLE",
        )
        .await;

    assert!(
        result.is_err(),
        "Non-owner should not be able to alter a stream table"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("must be owner"),
        "Error should mention ownership: {err}"
    );
}

/// TEST-6c: Superuser can drop any stream table regardless of ownership.
#[tokio::test]
async fn test_ownership_superuser_override() {
    let db = E2eDb::new().await.with_extension().await;
    setup_ownership_test(&db).await;

    // Verify the ST is owned by sec1_owner, not the superuser
    let owner: String = db
        .query_scalar(
            "SELECT pg_catalog.pg_get_userbyid(relowner) \
             FROM pg_catalog.pg_class \
             WHERE relname = 'sec1_st'",
        )
        .await;
    assert_eq!(owner, "sec1_owner", "ST should be owned by sec1_owner");

    // Superuser (default role, postgres) should be able to drop it
    let result = db
        .try_execute("SELECT pgtrickle.drop_stream_table('sec1_st')")
        .await;
    assert!(
        result.is_ok(),
        "Superuser should be able to drop any stream table: {:?}",
        result.err()
    );

    // Verify it's gone
    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'sec1_st')",
        )
        .await;
    assert!(!exists, "Stream table should be dropped");
}

/// #903: A role with documented source/output privileges can create a stream
/// table without access to pg_trickle's private catalog or change-buffer schema.
#[tokio::test]
async fn test_ownership_nonsuperuser_create_uses_private_infrastructure() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE sec903_author LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("CREATE SCHEMA sec903_author AUTHORIZATION sec903_author")
        .await;
    db.execute("CREATE TABLE sec903_author.source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sec903_author.source VALUES (1, 'one'), (2, 'two')")
        .await;
    db.execute("ALTER TABLE sec903_author.source OWNER TO sec903_author")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, sec903_author TO sec903_author")
        .await;
    db.execute("GRANT USAGE, CREATE ON SCHEMA public TO sec903_author")
        .await;
    db.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO sec903_author")
        .await;

    let result = db
        .try_execute_with_role(
            "SET ROLE sec903_author",
            "SELECT pgtrickle.create_stream_table(\
                 'sec903_stream', \
                 'SELECT id, val FROM source', \
                 '1m'\
             )",
            "RESET ROLE",
        )
        .await;
    assert!(
        result.is_ok(),
        "Documented non-superuser creation should succeed: {:?}",
        result.err()
    );

    let secured_api: bool = db
        .query_scalar(
            "SELECT prosecdef \
             FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'pgtrickle' \
               AND p.proname = 'create_stream_table' \
               AND p.pronargs = 18",
        )
        .await;
    assert!(secured_api, "creation API must be SECURITY DEFINER");

    let locked_search_path: bool = db
        .query_scalar(
            "SELECT EXISTS ( \
             SELECT 1 FROM pg_proc p \
             JOIN pg_namespace n ON n.oid = p.pronamespace, \
             unnest(p.proconfig) AS setting \
             WHERE n.nspname = 'pgtrickle' \
               AND p.proname = 'create_stream_table' \
               AND p.pronargs = 18 \
               AND setting = 'search_path=pgtrickle, pg_catalog, pg_temp' \
             )",
        )
        .await;
    assert!(
        locked_search_path,
        "SECURITY DEFINER creation API must use a locked search_path"
    );

    let secured_hook: bool = db
        .query_scalar(
            "SELECT prosecdef \
             FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'pgtrickle' \
               AND p.proname = '_on_ddl_end' \
               AND p.pronargs = 0",
        )
        .await;
    assert!(
        secured_hook,
        "DDL hook must retain private catalog privileges during caller-owned DDL"
    );

    let owner: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(relowner) \
             FROM pg_class WHERE oid = 'public.sec903_stream'::regclass",
        )
        .await;
    assert_eq!(owner, "sec903_author");

    let catalog_access: bool = db
        .query_scalar(
            "SELECT has_table_privilege('sec903_author', \
             'pgtrickle.pgt_stream_tables', 'SELECT')",
        )
        .await;
    assert!(!catalog_access, "catalog tables must remain private");

    let change_schema_usage: bool = db
        .query_scalar(
            "SELECT has_schema_privilege('sec903_author', \
             'pgtrickle_changes', 'USAGE')",
        )
        .await;
    assert!(
        !change_schema_usage,
        "authors must not receive change-buffer schema access"
    );
}

/// #903: The elevated creation API must not grant its owner privileges to the
/// defining query; PostgreSQL should return a normal permission error instead.
#[tokio::test]
async fn test_ownership_nonsuperuser_create_requires_source_select() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE sec903_no_select LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("CREATE SCHEMA sec903_denied").await;
    db.execute("CREATE TABLE sec903_denied.source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sec903_denied.source VALUES (1, 'secret')")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, sec903_denied TO sec903_no_select")
        .await;
    db.execute("GRANT USAGE, CREATE ON SCHEMA public TO sec903_no_select")
        .await;
    db.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO sec903_no_select")
        .await;

    let result = db
        .try_execute_with_role(
            "SET ROLE sec903_no_select",
            "SELECT pgtrickle.create_stream_table(\
                 'sec903_denied_stream', \
                 'SELECT id, val FROM sec903_denied.source', \
                 '1m', \
                 initialize => false\
             )",
            "RESET ROLE",
        )
        .await;
    let err = result
        .expect_err("source SELECT must not be bypassed")
        .to_string();
    assert!(
        err.contains("permission denied"),
        "Expected a PostgreSQL permission error, got: {err}"
    );

    let backend_still_usable: i32 = db.query_scalar("SELECT 1").await;
    assert_eq!(
        backend_still_usable, 1,
        "permission errors must not crash PostgreSQL"
    );
}

/// SEC-1: `create_or_replace_stream_table` was the one lifecycle entry point
/// (the one `dbt-pgtrickle`'s `stream_table` materialization actually calls)
/// left SECURITY INVOKER while its siblings were SECURITY DEFINER, forcing
/// callers to hold direct grants on pg_trickle's private catalog/change-buffer
/// schemas just to create a stream table. Mirrors
/// `test_ownership_nonsuperuser_create_uses_private_infrastructure` (#903),
/// but additionally exercises the "already exists" replace path (which
/// delegates internally to `alter_stream_table_impl`), since dbt's own usage
/// is a repeated, idempotent `create_or_replace_stream_table` call.
#[tokio::test]
async fn test_ownership_nonsuperuser_create_or_replace_uses_private_infrastructure() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE sec_cor_author LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("CREATE SCHEMA sec_cor_author AUTHORIZATION sec_cor_author")
        .await;
    db.execute("CREATE TABLE sec_cor_author.source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sec_cor_author.source VALUES (1, 'one'), (2, 'two')")
        .await;
    db.execute("ALTER TABLE sec_cor_author.source OWNER TO sec_cor_author")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, sec_cor_author TO sec_cor_author")
        .await;
    db.execute("GRANT USAGE, CREATE ON SCHEMA public TO sec_cor_author")
        .await;
    db.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO sec_cor_author")
        .await;

    // First call: table does not exist yet — routes to create_stream_table_impl.
    let create_result = db
        .try_execute_with_role(
            "SET ROLE sec_cor_author",
            "SELECT pgtrickle.create_or_replace_stream_table(\
                 'sec_cor_stream', \
                 'SELECT id, val FROM source'\
             )",
            "RESET ROLE",
        )
        .await;
    assert!(
        create_result.is_ok(),
        "Non-superuser create-or-replace (create path) should succeed: {:?}",
        create_result.err()
    );

    // Second call, same definition but a changed schedule: table already
    // exists — routes to alter_stream_table_impl instead.
    let replace_result = db
        .try_execute_with_role(
            "SET ROLE sec_cor_author",
            "SELECT pgtrickle.create_or_replace_stream_table(\
                 'sec_cor_stream', \
                 'SELECT id, val FROM source', \
                 schedule => '2m'\
             )",
            "RESET ROLE",
        )
        .await;
    assert!(
        replace_result.is_ok(),
        "Non-superuser create-or-replace (replace path) should succeed: {:?}",
        replace_result.err()
    );

    let secured_api: bool = db
        .query_scalar(
            "SELECT prosecdef \
             FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'pgtrickle' \
               AND p.proname = 'create_or_replace_stream_table' \
               AND p.pronargs = 16",
        )
        .await;
    assert!(
        secured_api,
        "create_or_replace_stream_table must be SECURITY DEFINER"
    );

    let locked_search_path: bool = db
        .query_scalar(
            "SELECT EXISTS ( \
             SELECT 1 FROM pg_proc p \
             JOIN pg_namespace n ON n.oid = p.pronamespace, \
             unnest(p.proconfig) AS setting \
             WHERE n.nspname = 'pgtrickle' \
               AND p.proname = 'create_or_replace_stream_table' \
               AND p.pronargs = 16 \
               AND setting = 'search_path=pgtrickle, pg_catalog, pg_temp' \
             )",
        )
        .await;
    assert!(
        locked_search_path,
        "SECURITY DEFINER create_or_replace_stream_table must use a locked search_path"
    );

    let owner: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(relowner) \
             FROM pg_class WHERE oid = 'public.sec_cor_stream'::regclass",
        )
        .await;
    assert_eq!(owner, "sec_cor_author");

    let catalog_access: bool = db
        .query_scalar(
            "SELECT has_table_privilege('sec_cor_author', \
             'pgtrickle.pgt_stream_tables', 'SELECT')",
        )
        .await;
    assert!(!catalog_access, "catalog tables must remain private");

    let change_schema_usage: bool = db
        .query_scalar(
            "SELECT has_schema_privilege('sec_cor_author', \
             'pgtrickle_changes', 'USAGE')",
        )
        .await;
    assert!(
        !change_schema_usage,
        "authors must not receive change-buffer schema access"
    );
}

/// SEC-1: The elevated `create_or_replace_stream_table` API must not grant its
/// owner privileges to the defining query; PostgreSQL should return a normal
/// permission error instead. Mirrors
/// `test_ownership_nonsuperuser_create_requires_source_select` (#903).
#[tokio::test]
async fn test_ownership_nonsuperuser_create_or_replace_requires_source_select() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE sec_cor_no_select LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("CREATE SCHEMA sec_cor_denied").await;
    db.execute("CREATE TABLE sec_cor_denied.source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sec_cor_denied.source VALUES (1, 'secret')")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, sec_cor_denied TO sec_cor_no_select")
        .await;
    db.execute("GRANT USAGE, CREATE ON SCHEMA public TO sec_cor_no_select")
        .await;
    db.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO sec_cor_no_select")
        .await;

    let result = db
        .try_execute_with_role(
            "SET ROLE sec_cor_no_select",
            "SELECT pgtrickle.create_or_replace_stream_table(\
                 'sec_cor_denied_stream', \
                 'SELECT id, val FROM sec_cor_denied.source', \
                 initialize => false\
             )",
            "RESET ROLE",
        )
        .await;
    let err = result
        .expect_err("source SELECT must not be bypassed")
        .to_string();
    assert!(
        err.contains("permission denied"),
        "Expected a PostgreSQL permission error, got: {err}"
    );

    let backend_still_usable: i32 = db.query_scalar("SELECT 1").await;
    assert_eq!(
        backend_still_usable, 1,
        "permission errors must not crash PostgreSQL"
    );
}

/// SEC-1: An elevated ALTER must resolve unqualified sources with the caller's
/// actual search path and preserve the storage relowner across every rebuild.
#[tokio::test]
async fn test_ownership_nonsuperuser_alter_rebuild_preserves_owner_and_search_path() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE sec_alter_owner LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("CREATE SCHEMA sec_alter_tenant AUTHORIZATION sec_alter_owner")
        .await;
    db.execute("CREATE TABLE sec_alter_tenant.source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sec_alter_tenant.source VALUES (1, 'one'), (2, 'twenty')")
        .await;
    db.execute("ALTER TABLE sec_alter_tenant.source OWNER TO sec_alter_owner")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, sec_alter_tenant TO sec_alter_owner")
        .await;
    db.execute("GRANT USAGE, CREATE ON SCHEMA public TO sec_alter_owner")
        .await;
    db.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO sec_alter_owner")
        .await;

    // The source schema deliberately differs from the role name. The
    // MATERIALIZED CTE installs the caller's path before the definer function
    // is entered and SET LOCAL makes it self-cleaning after the statement.
    let create_result = db
        .try_execute_with_role(
            "SET ROLE sec_alter_owner",
            "WITH caller_path AS MATERIALIZED ( \
                 SELECT set_config('search_path', 'sec_alter_tenant, public', true) \
             ) \
             SELECT pgtrickle.create_or_replace_stream_table( \
                 'public.sec_alter_stream', \
                 'SELECT id, val FROM source', \
                 refresh_mode => 'FULL' \
             ) FROM caller_path",
            "RESET ROLE",
        )
        .await;
    assert!(
        create_result.is_ok(),
        "Non-superuser creation should honor its configured search_path: {:?}",
        create_result.err()
    );

    let oid_before: i64 = db
        .query_scalar("SELECT 'public.sec_alter_stream'::regclass::oid::bigint")
        .await;

    // Changing val from text to integer has no implicit cast, forcing the
    // incompatible-query storage recreation path.
    let alter_query_result = db
        .try_execute_with_role(
            "SET ROLE sec_alter_owner",
            "WITH caller_path AS MATERIALIZED ( \
                 SELECT set_config('search_path', 'sec_alter_tenant, public', true) \
             ) \
             SELECT pgtrickle.alter_stream_table( \
                 'public.sec_alter_stream', \
                 query => 'SELECT id, length(val)::integer AS val FROM source' \
             ) FROM caller_path",
            "RESET ROLE",
        )
        .await;
    assert!(
        alter_query_result.is_ok(),
        "Owner ALTER QUERY rebuild should succeed without private grants: {:?}",
        alter_query_result.err()
    );

    let oid_after_query: i64 = db
        .query_scalar("SELECT 'public.sec_alter_stream'::regclass::oid::bigint")
        .await;
    assert_ne!(
        oid_before, oid_after_query,
        "Incompatible ALTER QUERY should recreate storage"
    );
    let owner_after_query: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(relowner) \
             FROM pg_class WHERE oid = 'public.sec_alter_stream'::regclass",
        )
        .await;
    assert_eq!(owner_after_query, "sec_alter_owner");

    // Partition-key migration is the second ALTER storage recreation path.
    let alter_partition_result = db
        .try_execute_with_role(
            "SET ROLE sec_alter_owner",
            "WITH caller_path AS MATERIALIZED ( \
                 SELECT set_config('search_path', 'sec_alter_tenant, public', true) \
             ) \
             SELECT pgtrickle.alter_stream_table( \
                 'public.sec_alter_stream', partition_by => 'id' \
             ) FROM caller_path",
            "RESET ROLE",
        )
        .await;
    assert!(
        alter_partition_result.is_ok(),
        "Owner partition rebuild should succeed without private grants: {:?}",
        alter_partition_result.err()
    );

    let oid_after_partition: i64 = db
        .query_scalar("SELECT 'public.sec_alter_stream'::regclass::oid::bigint")
        .await;
    assert_ne!(
        oid_after_query, oid_after_partition,
        "Partition ALTER should recreate storage"
    );
    let owner_after_partition: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(relowner) \
             FROM pg_class WHERE oid = 'public.sec_alter_stream'::regclass",
        )
        .await;
    assert_eq!(owner_after_partition, "sec_alter_owner");

    let values: Vec<i32> = db
        .query_scalar("SELECT array_agg(val ORDER BY id) FROM public.sec_alter_stream")
        .await;
    assert_eq!(values, vec![3, 6]);

    let catalog_access: bool = db
        .query_scalar(
            "SELECT has_table_privilege('sec_alter_owner', \
             'pgtrickle.pgt_stream_tables', 'SELECT')",
        )
        .await;
    assert!(!catalog_access, "catalog tables must remain private");
}

/// SEC-1: A SECURITY DEFINER cascade must authorize every downstream stream
/// table rather than borrowing the root owner's authority.
#[tokio::test]
async fn test_ownership_nonsuperuser_drop_cascade_checks_each_owner() {
    let db = E2eDb::new().await.with_extension().await;

    for role in ["sec_drop_root_owner", "sec_drop_child_owner"] {
        db.execute(&format!(
            "DO $$ BEGIN CREATE ROLE {role} LOGIN; \
             EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
        ))
        .await;
    }
    db.execute("CREATE TABLE sec_drop_source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sec_drop_source VALUES (1, 'one'), (2, 'two')")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table( \
             'public.sec_drop_parent', \
             'SELECT id, val FROM public.sec_drop_source', \
             '1m', 'FULL' \
         )",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table( \
             'public.sec_drop_child', \
             'SELECT id, val FROM public.sec_drop_parent', \
             '1m', 'FULL' \
         )",
    )
    .await;
    db.execute("ALTER TABLE public.sec_drop_parent OWNER TO sec_drop_root_owner")
        .await;
    db.execute("ALTER TABLE public.sec_drop_child OWNER TO sec_drop_child_owner")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO sec_drop_root_owner")
        .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.drop_stream_table(text, boolean) \
         TO sec_drop_root_owner",
    )
    .await;

    let denied = db
        .try_execute_with_role(
            "SET ROLE sec_drop_root_owner",
            "SELECT pgtrickle.drop_stream_table( \
                 'public.sec_drop_parent', cascade => true \
             )",
            "RESET ROLE",
        )
        .await
        .expect_err("cascade must not drop another role's stream table")
        .to_string();
    assert!(
        denied.contains("must be owner") && denied.contains("sec_drop_child"),
        "Expected downstream ownership denial, got: {denied}"
    );

    let both_remain: bool = db
        .query_scalar(
            "SELECT to_regclass('public.sec_drop_parent') IS NOT NULL \
                AND to_regclass('public.sec_drop_child') IS NOT NULL",
        )
        .await;
    assert!(both_remain, "denied cascade must be fully rolled back");

    // Once both targets have the same owner, the elevated function should be
    // able to use private catalog/change-buffer infrastructure for the cascade.
    db.execute("ALTER TABLE public.sec_drop_child OWNER TO sec_drop_root_owner")
        .await;
    let allowed = db
        .try_execute_with_role(
            "SET ROLE sec_drop_root_owner",
            "SELECT pgtrickle.drop_stream_table( \
                 'public.sec_drop_parent', cascade => true \
             )",
            "RESET ROLE",
        )
        .await;
    assert!(
        allowed.is_ok(),
        "same-owner cascade should succeed without private grants: {:?}",
        allowed.err()
    );

    let both_gone: bool = db
        .query_scalar(
            "SELECT to_regclass('public.sec_drop_parent') IS NULL \
                AND to_regclass('public.sec_drop_child') IS NULL",
        )
        .await;
    assert!(both_gone, "authorized cascade should drop the full chain");

    let catalog_access: bool = db
        .query_scalar(
            "SELECT has_table_privilege('sec_drop_root_owner', \
             'pgtrickle.pgt_stream_tables', 'SELECT')",
        )
        .await;
    assert!(!catalog_access, "catalog tables must remain private");
}

/// SEC-1: Regression-lock the security_definer + locked search_path status of
/// every stream-table lifecycle function patched to close the grant-surface
/// gap (`create_or_replace_stream_table`, `alter_stream_table`,
/// `drop_stream_table`), so a future refactor can't silently drop either
/// property without a catalog-level test failing.
#[tokio::test]
async fn test_ownership_lifecycle_functions_are_security_definer() {
    let db = E2eDb::new().await.with_extension().await;

    for (name, pronargs) in [
        ("create_or_replace_stream_table", 16),
        ("alter_stream_table", 20),
        ("drop_stream_table", 2),
    ] {
        let secured: bool = db
            .query_scalar(&format!(
                "SELECT prosecdef \
                 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
                 WHERE n.nspname = 'pgtrickle' \
                   AND p.proname = '{name}' \
                   AND p.pronargs = {pronargs}",
            ))
            .await;
        assert!(secured, "pgtrickle.{name} must be SECURITY DEFINER");

        let locked_search_path: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS ( \
                 SELECT 1 FROM pg_proc p \
                 JOIN pg_namespace n ON n.oid = p.pronamespace, \
                 unnest(p.proconfig) AS setting \
                 WHERE n.nspname = 'pgtrickle' \
                   AND p.proname = '{name}' \
                   AND p.pronargs = {pronargs} \
                   AND setting = 'search_path=pgtrickle, pg_catalog, pg_temp' \
                 )",
            ))
            .await;
        assert!(
            locked_search_path,
            "pgtrickle.{name} must use a locked search_path"
        );
    }
}
