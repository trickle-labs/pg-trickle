//! E2E tests for Row-Level Security (RLS) interaction with stream tables.
//!
//! Validates:
//! - R5: Source RLS is evaluated as the stream owner
//! - R7: RLS on stream tables filters reads per role
//! - R8: IMMEDIATE mode + RLS on stream table
//! - R10: ENABLE/DISABLE RLS on source table triggers reinit
//!
//! Prerequisites: `just build-e2e-image`

mod e2e;

use e2e::E2eDb;

/// R5: A superuser-owned stream still sees all source rows because its stable
/// stream owner bypasses RLS; the refresh caller no longer controls visibility.
#[tokio::test]
async fn test_rls_on_source_does_not_filter_stream_table() {
    let db = E2eDb::new().await.with_extension().await;

    // Create source table with tenant data
    db.execute(
        "CREATE TABLE rls_src (id INT PRIMARY KEY, tenant_id INT NOT NULL, val TEXT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO rls_src VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 10, 'c')")
        .await;

    // Enable RLS on the source table
    db.execute("ALTER TABLE rls_src ENABLE ROW LEVEL SECURITY")
        .await;

    // Create a restrictive policy that only allows tenant_id = 10
    db.execute(
        "CREATE POLICY tenant_policy ON rls_src \
         USING (tenant_id = 10)",
    )
    .await;

    // Create stream table on the RLS-protected source
    db.create_st(
        "rls_src_st",
        "SELECT id, tenant_id, val FROM rls_src",
        "1m",
        "FULL",
    )
    .await;

    // The stream owner is postgres, so owner execution sees all rows.
    db.refresh_st("rls_src_st").await;

    let count: i64 = db.count("public.rls_src_st").await;
    assert_eq!(
        count, 3,
        "superuser-owned stream table should contain all 3 rows"
    );
}

/// R5 variant: RLS on source table with DIFFERENTIAL refresh mode.
#[tokio::test]
async fn test_rls_on_source_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE rls_diff_src (id INT PRIMARY KEY, tenant_id INT NOT NULL, val TEXT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO rls_diff_src VALUES (1, 10, 'a'), (2, 20, 'b')")
        .await;

    // Enable RLS with restrictive policy
    db.execute("ALTER TABLE rls_diff_src ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute("CREATE POLICY tenant_only ON rls_diff_src USING (tenant_id = 10)")
        .await;

    // Create stream table with DIFFERENTIAL mode
    db.create_st(
        "rls_diff_st",
        "SELECT id, tenant_id, val FROM rls_diff_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.refresh_st("rls_diff_st").await;

    let count: i64 = db.count("public.rls_diff_st").await;
    assert_eq!(
        count, 2,
        "superuser-owned DIFFERENTIAL stream should contain all rows"
    );

    // Insert another row that a restricted owner would not see.
    db.execute("INSERT INTO rls_diff_src VALUES (3, 20, 'c')")
        .await;
    db.refresh_st("rls_diff_st").await;

    let count: i64 = db.count("public.rls_diff_st").await;
    assert_eq!(
        count, 3,
        "stable superuser owner should continue to see all rows"
    );
}

/// R7: RLS on the stream table itself filters reads per role.
#[tokio::test]
async fn test_rls_on_stream_table_filters_reads() {
    let db = E2eDb::new().await.with_extension().await;

    // Create source and populate
    db.execute(
        "CREATE TABLE rls_st_src (id INT PRIMARY KEY, tenant_id INT NOT NULL, val TEXT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO rls_st_src VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 10, 'c'), (4, 30, 'd')",
    )
    .await;

    // Create stream table
    db.create_st(
        "rls_st_test",
        "SELECT id, tenant_id, val FROM rls_st_src",
        "1m",
        "FULL",
    )
    .await;
    db.refresh_st("rls_st_test").await;

    // Verify all rows present before RLS
    let total: i64 = db.count("public.rls_st_test").await;
    assert_eq!(total, 4, "stream table should have all 4 rows");

    // Enable RLS on the stream table
    db.execute("ALTER TABLE public.rls_st_test ENABLE ROW LEVEL SECURITY")
        .await;

    // Create a test role and grant access
    db.execute("CREATE ROLE rls_reader LOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA public TO rls_reader")
        .await;
    db.execute("GRANT SELECT ON public.rls_st_test TO rls_reader")
        .await;

    // Create a policy that only allows tenant_id = 10
    db.execute(
        "CREATE POLICY tenant_filter ON public.rls_st_test \
         FOR SELECT TO rls_reader \
         USING (tenant_id = 10)",
    )
    .await;

    // As superuser we still see all rows (superuser bypasses RLS by default)
    let superuser_count: i64 = db
        .query_scalar("SELECT count(*) FROM public.rls_st_test")
        .await;
    assert_eq!(
        superuser_count, 4,
        "superuser should bypass RLS and see all rows"
    );

    // Actually query as the restricted role to verify RLS filtering works.
    // SET LOCAL ROLE within a transaction applies for that transaction only.
    let filtered_count: i64 = {
        let mut txn = db.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE rls_reader")
            .execute(&mut *txn)
            .await
            .unwrap();
        let cnt: i64 = sqlx::query_scalar("SELECT count(*) FROM public.rls_st_test")
            .fetch_one(&mut *txn)
            .await
            .unwrap();
        txn.rollback().await.unwrap();
        cnt
    };
    assert_eq!(
        filtered_count, 2,
        "rls_reader should only see 2 rows where tenant_id = 10 (RLS policy enforced)"
    );

    // Verify the policy exists and is correctly configured
    let policy_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_policies \
             WHERE tablename = 'rls_st_test' AND policyname = 'tenant_filter'",
        )
        .await;
    assert_eq!(policy_count, 1, "policy should exist on the stream table");
}

/// R8: IMMEDIATE mode stream table works correctly with RLS enabled on it.
#[tokio::test]
async fn test_rls_on_stream_table_immediate_mode() {
    let db = E2eDb::new().await.with_extension().await;

    // Create source table
    db.execute(
        "CREATE TABLE rls_imm_src (id INT PRIMARY KEY, tenant_id INT NOT NULL, val TEXT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO rls_imm_src VALUES (1, 10, 'a'), (2, 20, 'b')")
        .await;

    // Create IMMEDIATE mode stream table
    db.create_st(
        "rls_imm_st",
        "SELECT id, tenant_id, val FROM rls_imm_src",
        "1m",
        "IMMEDIATE",
    )
    .await;

    // Verify the initial population
    let count: i64 = db.count("public.rls_imm_st").await;
    assert_eq!(count, 2, "initial population should have 2 rows");

    // Enable RLS on the stream table
    db.execute("ALTER TABLE public.rls_imm_st ENABLE ROW LEVEL SECURITY")
        .await;

    // Create a policy
    db.execute(
        "CREATE POLICY imm_tenant ON public.rls_imm_st \
         USING (tenant_id = 10)",
    )
    .await;

    // Insert a new row into the source — IVM trigger should update the stream table
    // Privileged trigger bookkeeping switches definition evaluation to the stream owner.
    db.execute("INSERT INTO rls_imm_src VALUES (3, 10, 'c')")
        .await;

    // As superuser, we bypass RLS and should see all 3 rows
    let total: i64 = db.count("public.rls_imm_st").await;
    assert_eq!(
        total, 3,
        "IMMEDIATE mode should propagate insert despite RLS on stream table"
    );

    // Insert a row for tenant 20 — should also appear in the stream table
    db.execute("INSERT INTO rls_imm_src VALUES (4, 20, 'd')")
        .await;

    let total: i64 = db.count("public.rls_imm_st").await;
    assert_eq!(
        total, 4,
        "all rows should be in stream table regardless of RLS policy"
    );
}

/// R2: Verify that change buffer tables have RLS explicitly disabled.
#[tokio::test]
async fn test_change_buffer_rls_disabled() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rls_buf_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rls_buf_src VALUES (1, 'hello')")
        .await;

    db.create_st(
        "rls_buf_st",
        "SELECT id, val FROM rls_buf_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Check that the change buffer table has RLS disabled
    let rls_enabled: bool = db
        .query_scalar(
            "SELECT relrowsecurity FROM pg_class \
             WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgtrickle_changes') \
             AND relname LIKE 'changes_%' \
             LIMIT 1",
        )
        .await;
    assert!(
        !rls_enabled,
        "change buffer table should have RLS disabled (relrowsecurity = false)"
    );
}

/// R4: Verify that IVM trigger functions are SECURITY DEFINER.
#[tokio::test]
async fn test_ivm_trigger_functions_security_definer() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rls_secdef_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO rls_secdef_src VALUES (1, 'a')")
        .await;

    // Create an IMMEDIATE mode stream table (creates IVM triggers)
    db.create_st(
        "rls_secdef_st",
        "SELECT id, val FROM rls_secdef_src",
        "1m",
        "IMMEDIATE",
    )
    .await;

    // Check that the IVM trigger functions are SECURITY DEFINER
    let secdef_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_proc \
             WHERE proname LIKE 'pgt_ivm_%' \
             AND prosecdef = true",
        )
        .await;
    assert!(
        secdef_count > 0,
        "IVM trigger functions should be SECURITY DEFINER (prosecdef = true), found {secdef_count}"
    );

    // Also verify search_path is set (proconfig should contain search_path)
    let has_search_path: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM pg_proc \
                WHERE proname LIKE 'pgt_ivm_%' \
                AND prosecdef = true \
                AND EXISTS ( \
                    SELECT 1 FROM unnest(proconfig) AS cfg \
                    WHERE cfg LIKE 'search_path=pgtrickle_changes, pgtrickle, pg_catalog%' \
                ) \
             )",
        )
        .await;
    assert!(
        has_search_path,
        "IVM SECURITY DEFINER functions should have a locked search_path \
         starting with pgtrickle_changes, pgtrickle, pg_catalog"
    );
}

/// R4b: Verify that CDC trigger functions are SECURITY DEFINER with locked search_path.
#[tokio::test]
async fn test_cdc_trigger_functions_security_definer() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rls_cdc_secdef_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO rls_cdc_secdef_src VALUES (1, 'a')")
        .await;

    // DIFFERENTIAL mode creates the statement-level CDC triggers
    db.create_st(
        "rls_cdc_secdef_st",
        "SELECT id, val FROM rls_cdc_secdef_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // All pg_trickle_cdc_* functions must be SECURITY DEFINER
    let secdef_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_proc \
             WHERE proname LIKE 'pg_trickle_cdc_%' \
             AND prosecdef = true",
        )
        .await;
    assert!(
        secdef_count > 0,
        "CDC trigger functions should be SECURITY DEFINER (prosecdef = true), found {secdef_count}"
    );

    // All of them must also have a locked search_path
    let without_search_path: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_proc \
             WHERE proname LIKE 'pg_trickle_cdc_%' \
             AND prosecdef = true \
             AND NOT EXISTS ( \
                 SELECT 1 FROM unnest(COALESCE(proconfig, ARRAY[]::text[])) AS cfg \
                 WHERE cfg LIKE 'search_path=pgtrickle_changes, pgtrickle, pg_catalog%' \
             )",
        )
        .await;
    assert_eq!(
        without_search_path, 0,
        "all CDC SECURITY DEFINER functions must have a locked search_path \
         (starting with pgtrickle_changes, pgtrickle, pg_catalog), \
         found {without_search_path} without it"
    );
}

// ══════════════════════════════════════════════════════════════════════
// R10 — ENABLE/DISABLE RLS on source table triggers reinit
// ══════════════════════════════════════════════════════════════════════

/// R10: ENABLE ROW LEVEL SECURITY on a source table should mark stream tables
/// for reinit via the DDL event trigger hook.
#[tokio::test]
async fn test_enable_rls_on_source_triggers_reinit() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rls_ddl_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rls_ddl_src VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st(
        "rls_ddl_st",
        "SELECT id, val FROM rls_ddl_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh so the snapshot is stored.
    db.refresh_st("rls_ddl_st").await;

    let count: i64 = db.count("public.rls_ddl_st").await;
    assert_eq!(count, 2, "initial refresh should populate 2 rows");

    // Verify not marked for reinit before the DDL.
    let before: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rls_ddl_st'",
        )
        .await;
    assert!(!before, "ST should NOT need reinit before ENABLE RLS");

    // ENABLE RLS on the source table — should trigger reinit.
    db.execute("ALTER TABLE rls_ddl_src ENABLE ROW LEVEL SECURITY")
        .await;

    let after: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rls_ddl_st'",
        )
        .await;
    assert!(
        after,
        "ST should be marked for reinit after ENABLE RLS on source"
    );

    // Reinit (refresh) should succeed and the stream table should still
    // contain all rows because this stream's stable owner is the superuser.
    db.refresh_st("rls_ddl_st").await;

    let count: i64 = db.count("public.rls_ddl_st").await;
    assert_eq!(
        count, 2,
        "stream table should still contain all rows after reinit"
    );
}

/// R10 variant: DISABLE ROW LEVEL SECURITY on a source table that previously
/// had RLS enabled should also trigger reinit.
#[tokio::test]
async fn test_disable_rls_on_source_triggers_reinit() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rls_dis_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rls_dis_src VALUES (1, 'x'), (2, 'y')")
        .await;

    // Enable RLS first, then create the ST so the snapshot stores rls_enabled=true.
    db.execute("ALTER TABLE rls_dis_src ENABLE ROW LEVEL SECURITY")
        .await;

    db.create_st(
        "rls_dis_st",
        "SELECT id, val FROM rls_dis_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("rls_dis_st").await;

    // DISABLE RLS — snapshot had rls_enabled=true, now it's false.
    db.execute("ALTER TABLE rls_dis_src DISABLE ROW LEVEL SECURITY")
        .await;

    let reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rls_dis_st'",
        )
        .await;
    assert!(
        reinit,
        "ST should be marked for reinit after DISABLE RLS on source"
    );
}

/// R10 variant: FORCE ROW LEVEL SECURITY on a source table triggers reinit.
#[tokio::test]
async fn test_force_rls_on_source_triggers_reinit() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rls_force_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rls_force_src VALUES (1, 'p')")
        .await;

    db.create_st(
        "rls_force_st",
        "SELECT id, val FROM rls_force_src",
        "1m",
        "FULL",
    )
    .await;
    db.refresh_st("rls_force_st").await;

    // FORCE ROW LEVEL SECURITY — should trigger reinit.
    db.execute("ALTER TABLE rls_force_src FORCE ROW LEVEL SECURITY")
        .await;

    let reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rls_force_st'",
        )
        .await;
    assert!(
        reinit,
        "ST should be marked for reinit after FORCE RLS on source"
    );
}

// ══════════════════════════════════════════════════════════════════════
// S-3 (v0.79.0) — Global SECURITY DEFINER + search_path invariant
// ══════════════════════════════════════════════════════════════════════

/// S-3 (v0.79.0): Assert that **every** SECURITY DEFINER trigger function
/// registered by the pg_trickle extension has a locked `search_path` in its
/// `proconfig`.
///
/// This is a global invariant: regardless of function naming prefix, no
/// SECURITY DEFINER trigger function should be missing a search_path
/// constraint.  Missing search_path in a SECURITY DEFINER context is a
/// classic search-path hijacking vector.
///
/// The test creates one stream table (installing both CDC and IVM trigger
/// functions) then queries `pg_proc` for any SECURITY DEFINER *trigger*
/// function (`prorettype` matching the `trigger` pseudo-type) that lacks a
/// `search_path=...` entry in its `proconfig`.
#[tokio::test]
async fn test_all_security_definer_trigger_fns_have_search_path() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a source table and stream tables in both IMMEDIATE and DIFFERENTIAL
    // modes so that both IVM and CDC trigger functions are installed.
    db.execute("CREATE TABLE s3_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO s3_src VALUES (1, 'a'), (2, 'b')")
        .await;

    // DIFFERENTIAL: installs CDC (statement-level) trigger functions.
    db.create_st(
        "s3_diff_st",
        "SELECT id, val FROM s3_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // IMMEDIATE: installs IVM (row-level) trigger functions.
    db.create_st("s3_imm_st", "SELECT id, val FROM s3_src", "1m", "IMMEDIATE")
        .await;

    // Query for SECURITY DEFINER trigger functions that are missing search_path.
    // prorettype = 2279 is the OID of the `trigger` pseudo-type, which identifies
    // trigger functions independent of naming convention.
    let missing: Vec<String> = sqlx::query_scalar(
        "SELECT proname::text \
         FROM pg_proc \
         WHERE prosecdef = true \
           AND prorettype = 2279 \
           AND NOT EXISTS ( \
               SELECT 1 \
               FROM unnest(COALESCE(proconfig, ARRAY[]::text[])) AS cfg \
               WHERE cfg LIKE 'search_path=%' \
           ) \
         ORDER BY proname",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap_or_default();

    assert!(
        missing.is_empty(),
        "S-3: found SECURITY DEFINER trigger functions without a locked \
         search_path ({} violation(s)): {}",
        missing.len(),
        missing.join(", ")
    );
}
