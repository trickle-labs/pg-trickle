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
               AND p.pronargs = 19",
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
               AND p.pronargs = 19 \
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

    // LSEC-7 (v0.87.9): the unqualified target name now resolves under the
    // caller's own search_path ("$user", public by default) instead of a
    // hard-coded `public` default — since a schema named `sec903_author`
    // exists and is owned by the caller, `$user` resolves to it first, so
    // the new stream table lands in `sec903_author.sec903_stream`.
    let owner: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(relowner) \
             FROM pg_class WHERE oid = 'sec903_author.sec903_stream'::regclass",
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

// ── v0.87.9 (issue #941 core APIs): create-or-replace, alter, drop ─────────

/// v0.87.9: A non-superuser owner with only the documented public-API grants
/// (no `pgtrickle_changes` grants, no private catalog SELECT, no
/// `EXECUTE ON ALL FUNCTIONS`) can create, create-or-replace, alter — both a
/// query-incompatible change and a partition-key change — and drop their own
/// stream table. Also verifies the merge-gate invariant that a
/// query-changing or partition-changing ALTER preserves the *exact* original
/// storage owner and that data survives every step.
#[tokio::test]
async fn test_ownership_nonsuperuser_lifecycle_without_private_grants() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE sec941_owner LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("CREATE SCHEMA IF NOT EXISTS sec941_owner AUTHORIZATION sec941_owner")
        .await;
    db.execute("CREATE TABLE sec941_owner.src (id INT PRIMARY KEY, val TEXT, num NUMERIC)")
        .await;
    db.execute("INSERT INTO sec941_owner.src VALUES (1, 'a', 10), (2, 'b', 20)")
        .await;
    db.execute("ALTER TABLE sec941_owner.src OWNER TO sec941_owner")
        .await;

    // Exact documented public-API grants only — no pgtrickle_changes, no
    // private catalog access, no EXECUTE ON ALL FUNCTIONS.
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO sec941_owner")
        .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.create_or_replace_stream_table(\
            text, text, text, text, boolean, text, text, text, boolean, boolean, \
            text, integer, double precision, text, boolean, text, text) \
         TO sec941_owner",
    )
    .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.alter_stream_table(\
            text, text, text, text, text, text, text, text, boolean, boolean, text, text, \
            bigint, integer, text, integer, double precision, text, double precision, text) \
         TO sec941_owner",
    )
    .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.drop_stream_table(text, boolean) TO sec941_owner",
    )
    .await;

    // Create path — SET ROLE for the whole scenario via repeated
    // try_execute_with_role calls (one target statement per call, same
    // pattern already used by the ownership-check tests above).
    db.try_execute_with_role(
        "SET ROLE sec941_owner",
        "SELECT pgtrickle.create_or_replace_stream_table(\
             'sec941_owner.sec941_st', \
             'SELECT id, val FROM sec941_owner.src', \
             '1m', 'DIFFERENTIAL')",
        "RESET ROLE",
    )
    .await
    .expect("non-superuser create-or-replace (create path) should succeed");

    let owner_after_create: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(relowner) FROM pg_class \
             WHERE oid = 'sec941_owner.sec941_st'::regclass",
        )
        .await;
    assert_eq!(owner_after_create, "sec941_owner");

    let count_after_create = db.count("sec941_owner.sec941_st").await;
    assert_eq!(count_after_create, 2);

    // Confirm no private-infrastructure access was needed to get here.
    let catalog_access: bool = db
        .query_scalar(
            "SELECT has_table_privilege('sec941_owner', \
             'pgtrickle.pgt_stream_tables', 'SELECT')",
        )
        .await;
    assert!(!catalog_access, "catalog tables must remain private");
    let change_schema_usage: bool = db
        .query_scalar("SELECT has_schema_privilege('sec941_owner', 'pgtrickle_changes', 'USAGE')")
        .await;
    assert!(
        !change_schema_usage,
        "authors must not receive change-buffer schema access"
    );

    // Replace path (config-only: schedule change, query unchanged) —
    // delegates internally to alter_stream_table_impl without a second SQL-
    // level EXECUTE grant.
    db.try_execute_with_role(
        "SET ROLE sec941_owner",
        "SELECT pgtrickle.create_or_replace_stream_table(\
             'sec941_owner.sec941_st', \
             'SELECT id, val FROM sec941_owner.src', \
             '5m', 'DIFFERENTIAL')",
        "RESET ROLE",
    )
    .await
    .expect("non-superuser create-or-replace (replace path) should succeed");

    let schedule_after_replace: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = 'sec941_owner' AND pgt_name = 'sec941_st'",
        )
        .await;
    assert_eq!(schedule_after_replace, "5m");

    // Direct ALTER with an incompatible query change (TEXT -> INTEGER on the
    // same output column) forces a full storage rebuild — the new physical
    // table must come back owned by sec941_owner, not the extension owner.
    let relid_before_query_alter: i64 = db
        .query_scalar(
            "SELECT pgt_relid::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = 'sec941_owner' AND pgt_name = 'sec941_st'",
        )
        .await;

    db.try_execute_with_role(
        "SET ROLE sec941_owner",
        "SELECT pgtrickle.alter_stream_table(\
             'sec941_owner.sec941_st', \
             query => 'SELECT id, num::integer AS val FROM sec941_owner.src')",
        "RESET ROLE",
    )
    .await
    .expect("non-superuser query-changing ALTER should succeed");

    let relid_after_query_alter: i64 = db
        .query_scalar(
            "SELECT pgt_relid::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = 'sec941_owner' AND pgt_name = 'sec941_st'",
        )
        .await;
    assert_ne!(
        relid_before_query_alter, relid_after_query_alter,
        "incompatible query change must rebuild storage (new OID)"
    );

    let owner_after_query_alter: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(relowner) FROM pgtrickle.pgt_stream_tables st \
             JOIN pg_class c ON c.oid = st.pgt_relid \
             WHERE st.pgt_schema = 'sec941_owner' AND st.pgt_name = 'sec941_st'",
        )
        .await;
    assert_eq!(
        owner_after_query_alter, "sec941_owner",
        "query-changing ALTER must preserve the exact original storage owner"
    );

    let val: i64 = db
        .query_scalar("SELECT val::bigint FROM sec941_owner.sec941_st WHERE id = 1")
        .await;
    assert_eq!(val, 10, "data must survive the query-changing ALTER");
    let count_after_query_alter = db.count("sec941_owner.sec941_st").await;
    assert_eq!(count_after_query_alter, 2);

    // Direct ALTER changing the partition key — also a full storage rebuild
    // that must preserve the original owner.
    let relid_before_partition_alter = relid_after_query_alter;

    db.try_execute_with_role(
        "SET ROLE sec941_owner",
        "SELECT pgtrickle.alter_stream_table('sec941_owner.sec941_st', partition_by => 'id')",
        "RESET ROLE",
    )
    .await
    .expect("non-superuser partition-key ALTER should succeed");

    let relid_after_partition_alter: i64 = db
        .query_scalar(
            "SELECT pgt_relid::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = 'sec941_owner' AND pgt_name = 'sec941_st'",
        )
        .await;
    assert_ne!(
        relid_before_partition_alter, relid_after_partition_alter,
        "partition-key change must rebuild storage (new OID)"
    );

    let owner_after_partition_alter: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(relowner) FROM pgtrickle.pgt_stream_tables st \
             JOIN pg_class c ON c.oid = st.pgt_relid \
             WHERE st.pgt_schema = 'sec941_owner' AND st.pgt_name = 'sec941_st'",
        )
        .await;
    assert_eq!(
        owner_after_partition_alter, "sec941_owner",
        "partition-key ALTER must preserve the exact original storage owner"
    );

    let count_after_partition_alter = db.count("sec941_owner.sec941_st").await;
    assert_eq!(
        count_after_partition_alter, 2,
        "data must survive the partition-key ALTER"
    );

    // Drop path.
    db.try_execute_with_role(
        "SET ROLE sec941_owner",
        "SELECT pgtrickle.drop_stream_table('sec941_owner.sec941_st')",
        "RESET ROLE",
    )
    .await
    .expect("non-superuser drop should succeed");

    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = 'sec941_owner' AND pgt_name = 'sec941_st')",
        )
        .await;
    assert!(!exists, "stream table should be dropped");
}

/// v0.87.9 (LSEC-9): a cascade drop that would touch a stream table the
/// caller does not own is rejected in full — every affected stream table
/// (root and every transitively cascaded dependent) is authorized before
/// the first mutation, so a mixed-owner cascade leaves zero mutations.
#[tokio::test]
async fn test_ownership_mixed_owner_cascade_drop_denied_zero_mutations() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "DO $$ BEGIN CREATE ROLE sec941_cascade_a LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute(
        "DO $$ BEGIN CREATE ROLE sec941_cascade_b LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO sec941_cascade_a, sec941_cascade_b")
        .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.drop_stream_table(text, boolean) \
         TO sec941_cascade_a, sec941_cascade_b",
    )
    .await;

    db.execute("CREATE TABLE sec941_cascade_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sec941_cascade_src VALUES (1, 'a'), (2, 'b')")
        .await;
    db.execute("GRANT ALL ON TABLE sec941_cascade_src TO sec941_cascade_a, sec941_cascade_b")
        .await;

    // Root ST, owned by sec941_cascade_a.
    db.create_st(
        "sec941_cascade_root",
        "SELECT id, val FROM sec941_cascade_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("ALTER TABLE sec941_cascade_root OWNER TO sec941_cascade_a")
        .await;
    db.execute("GRANT SELECT ON TABLE sec941_cascade_root TO sec941_cascade_b")
        .await;

    // Downstream ST depending on the root, owned by a *different* role.
    db.create_st(
        "sec941_cascade_dependent",
        "SELECT id, val FROM sec941_cascade_root",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("ALTER TABLE sec941_cascade_dependent OWNER TO sec941_cascade_b")
        .await;

    let root_relid_before: i64 = db
        .query_scalar(
            "SELECT pgt_relid::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'sec941_cascade_root'",
        )
        .await;
    let dependent_relid_before: i64 = db
        .query_scalar(
            "SELECT pgt_relid::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'sec941_cascade_dependent'",
        )
        .await;

    // sec941_cascade_a owns the root but NOT the dependent — the cascade
    // must be rejected before anything is dropped.
    let result = db
        .try_execute_with_role(
            "SET ROLE sec941_cascade_a",
            "SELECT pgtrickle.drop_stream_table('sec941_cascade_root', cascade => true)",
            "RESET ROLE",
        )
        .await;
    assert!(result.is_err(), "mixed-owner cascade drop must be rejected");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("must be owner"),
        "error should mention ownership: {err}"
    );

    // Zero mutations: both catalog rows and both storage tables survive
    // untouched, with the same relid as before the rejected cascade.
    let root_relid_after: i64 = db
        .query_scalar(
            "SELECT pgt_relid::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'sec941_cascade_root'",
        )
        .await;
    let dependent_relid_after: i64 = db
        .query_scalar(
            "SELECT pgt_relid::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'sec941_cascade_dependent'",
        )
        .await;
    assert_eq!(root_relid_before, root_relid_after);
    assert_eq!(dependent_relid_before, dependent_relid_after);

    assert!(
        db.table_exists("public", "sec941_cascade_root").await,
        "root storage table must survive a rejected cascade"
    );
    assert!(
        db.table_exists("public", "sec941_cascade_dependent").await,
        "dependent storage table must survive a rejected cascade"
    );

    let root_count = db.count("public.sec941_cascade_root").await;
    assert_eq!(root_count, 2, "root data must be untouched");
    let dependent_count = db.count("public.sec941_cascade_dependent").await;
    assert_eq!(dependent_count, 2, "dependent data must be untouched");
}
