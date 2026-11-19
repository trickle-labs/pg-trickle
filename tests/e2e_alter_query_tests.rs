//! E2E tests for `pgtrickle.alter_stream_table(query => ...)`.
//!
//! Validates changing the defining query of an existing stream table,
//! including same-schema, compatible-schema, and incompatible-schema
//! transitions, dependency changes, and error handling.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_storage_replacement_rejects_authorization_changes() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE ROLE auth1423_reader NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA public TO auth1423_reader")
        .await;
    db.execute("CREATE TABLE auth1423_src (id INT PRIMARY KEY, tenant_id INT)")
        .await;
    db.execute("INSERT INTO auth1423_src VALUES (1, 1), (2, 2)")
        .await;
    db.create_st(
        "auth1423_st",
        "SELECT id, tenant_id FROM auth1423_src",
        "1d",
        "FULL",
    )
    .await;
    db.execute("GRANT SELECT ON auth1423_st TO auth1423_reader")
        .await;
    db.execute("GRANT UPDATE (tenant_id) ON auth1423_st TO auth1423_reader")
        .await;
    db.execute("ALTER TABLE auth1423_st ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute("ALTER TABLE auth1423_st FORCE ROW LEVEL SECURITY")
        .await;
    db.execute("CREATE POLICY auth1423_tenant ON auth1423_st FOR SELECT TO auth1423_reader USING (tenant_id = 1)")
        .await;
    db.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO auth1423_reader",
    )
    .await;

    let oid = db.table_oid("auth1423_st").await;
    for alter in [
        "query => $$SELECT id, tenant_id FROM auth1423_src WHERE id > 0$$",
        "query => $$SELECT id FROM auth1423_src$$",
        "partition_by => 'tenant_id'",
    ] {
        let result = db
            .try_execute(&format!(
                "SELECT pgtrickle.alter_stream_table('auth1423_st', {alter})"
            ))
            .await;
        let error = result.expect_err("replacement must reject authorization loss");
        assert!(error.to_string().contains("cannot be preserved"), "{error}");
        assert_eq!(db.table_oid("auth1423_st").await, oid);
        let intact: bool = db.query_scalar(
            "SELECT c.relrowsecurity AND c.relforcerowsecurity \
             AND has_table_privilege('auth1423_reader', c.oid, 'SELECT') \
             AND has_column_privilege('auth1423_reader', c.oid, 'tenant_id', 'UPDATE') \
             AND EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = c.oid AND polname = 'auth1423_tenant') \
             FROM pg_class c WHERE c.oid = 'auth1423_st'::regclass",
        ).await;
        assert!(intact);
        let mut tx = db.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE auth1423_reader")
            .execute(&mut *tx)
            .await
            .unwrap();
        let visible: Vec<i32> = sqlx::query_scalar("SELECT tenant_id FROM auth1423_st")
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert_eq!(visible, vec![1]);
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn test_storage_replacement_rejects_individual_authorization_settings() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE ROLE auth1423_individual NOLOGIN").await;
    db.execute("CREATE TABLE auth1423_cases_src (id INT PRIMARY KEY, tenant_id INT)")
        .await;
    for (setup, cleanup) in [
        (
            "GRANT SELECT ON auth1423_cases_st TO auth1423_individual",
            "REVOKE ALL ON auth1423_cases_st FROM auth1423_individual",
        ),
        (
            "GRANT SELECT (tenant_id) ON auth1423_cases_st TO auth1423_individual",
            "REVOKE SELECT (tenant_id) ON auth1423_cases_st FROM auth1423_individual",
        ),
        (
            "ALTER TABLE auth1423_cases_st ENABLE ROW LEVEL SECURITY",
            "ALTER TABLE auth1423_cases_st DISABLE ROW LEVEL SECURITY",
        ),
        (
            "ALTER TABLE auth1423_cases_st FORCE ROW LEVEL SECURITY",
            "ALTER TABLE auth1423_cases_st NO FORCE ROW LEVEL SECURITY",
        ),
        (
            "CREATE POLICY auth1423_policy ON auth1423_cases_st USING (tenant_id = 1)",
            "DROP POLICY auth1423_policy ON auth1423_cases_st",
        ),
        (
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO auth1423_individual",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM auth1423_individual",
        ),
    ] {
        db.create_st(
            "auth1423_cases_st",
            "SELECT id, tenant_id FROM auth1423_cases_src",
            "1d",
            "FULL",
        )
        .await;
        db.execute(setup).await;
        let result = db.try_execute("SELECT pgtrickle.alter_stream_table('auth1423_cases_st', query => $$SELECT id, tenant_id FROM auth1423_cases_src WHERE id > 0$$)").await;
        assert!(
            result
                .expect_err(setup)
                .to_string()
                .contains("cannot be preserved"),
            "{setup}"
        );
        db.execute(cleanup).await;
        db.execute("SELECT pgtrickle.alter_stream_table('auth1423_cases_st', query => $$SELECT id, tenant_id FROM auth1423_cases_src WHERE id > 0$$)").await;
        db.execute("SELECT pgtrickle.drop_stream_table('auth1423_cases_st')")
            .await;
    }
}

#[tokio::test]
async fn test_explain_alter_is_read_only_and_classifies_changes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_explain_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO aq_explain_src VALUES (1, 'a'), (2, 'b')")
        .await;
    db.create_st(
        "aq_explain_st",
        "SELECT id, val FROM aq_explain_src",
        "1m",
        "FULL",
    )
    .await;
    let oid_before = db.table_oid("aq_explain_st").await;

    let compatible: bool = db
        .query_scalar(
            "SELECT (pgtrickle.explain_alter(\
                'aq_explain_st', 'SELECT id, val FROM aq_explain_src')\
             ->> 'classification') = 'compatible'",
        )
        .await;
    assert!(compatible);

    let rebuildable: bool = db
        .query_scalar(
            "SELECT (pgtrickle.explain_alter(\
                'aq_explain_st', 'SELECT id, val FROM aq_explain_src WHERE id > 1')\
             ->> 'classification') = 'rebuildable'",
        )
        .await;
    assert!(rebuildable);

    let rejected: bool = db
        .query_scalar(
            "SELECT (pgtrickle.explain_alter(\
                'aq_explain_st', 'SELECT missing FROM aq_explain_src')\
             ->> 'classification') = 'rejected'",
        )
        .await;
    assert!(rejected);

    let oid_after = db.table_oid("aq_explain_st").await;
    assert_eq!(
        oid_before, oid_after,
        "explain_alter must not mutate storage"
    );
    assert_eq!(db.count("public.aq_explain_st").await, 2);
}

// ── Same-Schema Tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_same_schema() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_same (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO aq_same VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    db.create_st("aq_same_st", "SELECT id, val FROM aq_same", "1m", "FULL")
        .await;

    assert_eq!(db.count("public.aq_same_st").await, 3);

    // Change WHERE clause — same output columns
    db.alter_st(
        "aq_same_st",
        "query => $$ SELECT id, val FROM aq_same WHERE id > 1 $$",
    )
    .await;

    let (status, _, populated, _) = db.pgt_status("aq_same_st").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated, "ST should be populated after ALTER QUERY");

    // Should have only 2 rows now (id=2, id=3)
    assert_eq!(db.count("public.aq_same_st").await, 2);

    db.assert_st_matches_query("aq_same_st", "SELECT id, val FROM aq_same WHERE id > 1")
        .await;
}

#[tokio::test]
async fn test_alter_query_preserves_immediate_cascade_triggers() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_cascade_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO aq_cascade_src VALUES (1, 10)")
        .await;
    db.create_st(
        "aq_cascade_upstream",
        "SELECT id, val FROM aq_cascade_src",
        "1m",
        "IMMEDIATE",
    )
    .await;
    db.create_st(
        "aq_cascade_downstream",
        "SELECT id, val * 2 AS result FROM aq_cascade_upstream",
        "1m",
        "IMMEDIATE",
    )
    .await;

    db.alter_st(
        "aq_cascade_downstream",
        "query => $$ SELECT id, val * 3 AS result FROM aq_cascade_upstream $$",
    )
    .await;
    db.execute("INSERT INTO aq_cascade_src VALUES (2, 20)")
        .await;
    let result: i32 = db
        .query_scalar("SELECT result FROM aq_cascade_downstream WHERE id = 2")
        .await;
    assert_eq!(result, 60);

    db.alter_st(
        "aq_cascade_upstream",
        "query => $$ SELECT id, val * 2 AS val FROM aq_cascade_src WHERE val >= 0 $$",
    )
    .await;
    let existing_result: i32 = db
        .query_scalar("SELECT result FROM aq_cascade_downstream WHERE id = 1")
        .await;
    assert_eq!(
        existing_result, 60,
        "ALTER QUERY must rebuild existing downstream rows"
    );
    db.execute("INSERT INTO aq_cascade_src VALUES (3, 30)")
        .await;
    let result: i32 = db
        .query_scalar("SELECT result FROM aq_cascade_downstream WHERE id = 3")
        .await;
    assert_eq!(result, 180);
}

#[tokio::test]
async fn test_alter_query_rebuilds_stream_change_buffer_schema() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_buffer_src (id INT PRIMARY KEY, val INT, extra INT)")
        .await;
    db.execute("INSERT INTO aq_buffer_src VALUES (1, 10, 100)")
        .await;
    db.create_st(
        "aq_buffer_upstream",
        "SELECT id, val FROM aq_buffer_src",
        "5m",
        "FULL",
    )
    .await;
    db.create_st(
        "aq_buffer_downstream",
        "SELECT id, val FROM aq_buffer_upstream",
        "5m",
        "DIFFERENTIAL",
    )
    .await;

    db.alter_st(
        "aq_buffer_upstream",
        "query => $$ SELECT id, val, extra FROM aq_buffer_src $$",
    )
    .await;

    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'aq_buffer_upstream'",
        )
        .await;
    let has_extra: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_attribute \
             WHERE attrelid = 'pgtrickle_changes.changes_pgt_{pgt_id}'::regclass \
               AND attname = 'extra' AND NOT attisdropped)"
        ))
        .await;
    assert!(
        has_extra,
        "ALTER QUERY must rebuild the output buffer schema"
    );

    db.execute("UPDATE aq_buffer_src SET val = 20, extra = 200 WHERE id = 1")
        .await;
    db.refresh_st_with_retry("aq_buffer_upstream").await;
    db.refresh_st_with_retry("aq_buffer_downstream").await;
    db.assert_st_matches_query("aq_buffer_downstream", "SELECT id, val FROM aq_buffer_src")
        .await;
}

#[tokio::test]
async fn test_alter_query_same_schema_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_diff (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO aq_diff VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st(
        "aq_diff_st",
        "SELECT id, val FROM aq_diff",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.aq_diff_st").await, 2);

    // Change the query (still same schema)
    db.alter_st(
        "aq_diff_st",
        "query => $$ SELECT id, val FROM aq_diff WHERE val != 'a' $$",
    )
    .await;

    assert_eq!(db.count("public.aq_diff_st").await, 1);

    // Verify differential refresh still works with the new query
    db.execute("INSERT INTO aq_diff VALUES (3, 'c')").await;
    db.refresh_st("aq_diff_st").await;
    assert_eq!(db.count("public.aq_diff_st").await, 2);
}

// ── Compatible-Schema Tests ────────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_add_column() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_addcol (id INT PRIMARY KEY, val TEXT, extra INT)")
        .await;
    db.execute("INSERT INTO aq_addcol VALUES (1, 'a', 10), (2, 'b', 20)")
        .await;

    db.create_st(
        "aq_addcol_st",
        "SELECT id, val FROM aq_addcol",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.aq_addcol_st").await, 2);

    // Add the extra column to the query
    db.alter_st(
        "aq_addcol_st",
        "query => $$ SELECT id, val, extra FROM aq_addcol $$",
    )
    .await;

    let (status, _, populated, _) = db.pgt_status("aq_addcol_st").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);

    // Verify the new column exists
    let extra_val: i32 = db
        .query_scalar("SELECT extra FROM public.aq_addcol_st WHERE id = 1")
        .await;
    assert_eq!(extra_val, 10);
}

#[tokio::test]
async fn test_alter_query_remove_column() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_rmcol (id INT PRIMARY KEY, val TEXT, extra INT)")
        .await;
    db.execute("INSERT INTO aq_rmcol VALUES (1, 'a', 10), (2, 'b', 20)")
        .await;

    db.create_st(
        "aq_rmcol_st",
        "SELECT id, val, extra FROM aq_rmcol",
        "1m",
        "FULL",
    )
    .await;

    // Remove extra column from the query
    db.alter_st("aq_rmcol_st", "query => $$ SELECT id, val FROM aq_rmcol $$")
        .await;

    let (status, _, populated, _) = db.pgt_status("aq_rmcol_st").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);

    // Verify the extra column is gone
    let has_extra: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_name = 'aq_rmcol_st' AND column_name = 'extra')",
        )
        .await;
    assert!(!has_extra, "extra column should be removed");

    assert_eq!(db.count("public.aq_rmcol_st").await, 2);
}

#[tokio::test]
async fn test_alter_query_type_change_compatible() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_compat (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO aq_compat VALUES (1, 100), (2, 200)")
        .await;

    db.create_st(
        "aq_compat_st",
        "SELECT id, val FROM aq_compat",
        "1m",
        "FULL",
    )
    .await;

    // Change val from INT to BIGINT (compatible implicit cast)
    db.alter_st(
        "aq_compat_st",
        "query => $$ SELECT id, val::bigint AS val FROM aq_compat $$",
    )
    .await;

    let (status, _, populated, _) = db.pgt_status("aq_compat_st").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
    assert_eq!(db.count("public.aq_compat_st").await, 2);
}

// ── Incompatible-Schema Tests ──────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_type_change_incompatible() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_incompat (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO aq_incompat VALUES (1, 100), (2, 200)")
        .await;

    db.create_st(
        "aq_incompat_st",
        "SELECT id, val FROM aq_incompat",
        "1m",
        "FULL",
    )
    .await;

    let oid_before = db.table_oid("aq_incompat_st").await;

    // Change val from INT to TEXT (incompatible — full rebuild)
    db.alter_st(
        "aq_incompat_st",
        "query => $$ SELECT id, val::text AS val FROM aq_incompat $$",
    )
    .await;

    let (status, _, populated, _) = db.pgt_status("aq_incompat_st").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
    assert_eq!(db.count("public.aq_incompat_st").await, 2);

    // OID should change for incompatible rebuild
    let oid_after = db.table_oid("aq_incompat_st").await;
    assert_ne!(
        oid_before, oid_after,
        "Storage table OID should change for incompatible schema"
    );
}

// ── Dependency Change Tests ────────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_change_sources() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_src_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE aq_src_b (id INT PRIMARY KEY, info TEXT)")
        .await;
    db.execute("INSERT INTO aq_src_a VALUES (1, 'a'), (2, 'b')")
        .await;
    db.execute("INSERT INTO aq_src_b VALUES (1, 'x'), (3, 'y')")
        .await;

    // Start with only source A
    db.create_st(
        "aq_sources_st",
        "SELECT id, val FROM aq_src_a",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.aq_sources_st").await, 2);

    // Change to join A + B (adds B as a source)
    db.alter_st(
        "aq_sources_st",
        "query => $$ SELECT a.id, a.val FROM aq_src_a a JOIN aq_src_b b ON a.id = b.id $$",
    )
    .await;

    // Only id=1 matches the join
    assert_eq!(db.count("public.aq_sources_st").await, 1);

    // Verify dependency on B is registered
    let dep_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_dependencies d \
             JOIN pgtrickle.pgt_stream_tables st ON d.pgt_id = st.pgt_id \
             WHERE st.pgt_name = 'aq_sources_st'",
        )
        .await;
    assert!(dep_count >= 2, "Should have dependencies on both A and B");
}

#[tokio::test]
async fn test_alter_query_remove_source() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_rmsrc_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE aq_rmsrc_b (id INT PRIMARY KEY, info TEXT)")
        .await;
    db.execute("INSERT INTO aq_rmsrc_a VALUES (1, 'a'), (2, 'b')")
        .await;
    db.execute("INSERT INTO aq_rmsrc_b VALUES (1, 'x')").await;

    // Start with join on A + B
    db.create_st(
        "aq_rmsrc_st",
        "SELECT a.id, a.val FROM aq_rmsrc_a a JOIN aq_rmsrc_b b ON a.id = b.id",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.aq_rmsrc_st").await, 1);

    // Switch to only A (removes B dependency)
    db.alter_st(
        "aq_rmsrc_st",
        "query => $$ SELECT id, val FROM aq_rmsrc_a $$",
    )
    .await;

    assert_eq!(db.count("public.aq_rmsrc_st").await, 2);

    // Verify dependency on B is removed
    let b_oid: i32 = db.table_oid("aq_rmsrc_b").await;
    let b_dep: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_dependencies d \
             JOIN pgtrickle.pgt_stream_tables st ON d.pgt_id = st.pgt_id \
             WHERE st.pgt_name = 'aq_rmsrc_st' AND d.source_relid = {b_oid}"
        ))
        .await;
    assert_eq!(b_dep, 0, "Dependency on removed source B should be gone");
}

// ── Aggregate / __pgt_count Transition Tests ───────────────────────────

#[tokio::test]
async fn test_alter_query_pgt_count_transition() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_agg (id INT PRIMARY KEY, category TEXT, val INT)")
        .await;
    db.execute("INSERT INTO aq_agg VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)")
        .await;

    // Start with flat query
    db.create_st(
        "aq_agg_st",
        "SELECT id, category, val FROM aq_agg",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.aq_agg_st").await, 3);

    // Switch to aggregate query (adds __pgt_count internally)
    db.alter_st(
        "aq_agg_st",
        "query => $$ SELECT category, SUM(val) AS total FROM aq_agg GROUP BY category $$",
    )
    .await;

    let (status, _, populated, _) = db.pgt_status("aq_agg_st").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);

    // Should have 2 groups: 'a' (30) and 'b' (30)
    assert_eq!(db.count("public.aq_agg_st").await, 2);
}

// ── Query + Mode Change Tests ──────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_with_mode_change() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_combo (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO aq_combo VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st(
        "aq_combo_st",
        "SELECT id, val FROM aq_combo",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Change both query and mode simultaneously
    db.alter_st(
        "aq_combo_st",
        "query => $$ SELECT id, val FROM aq_combo WHERE id = 1 $$, refresh_mode => 'FULL'",
    )
    .await;

    let (status, mode, populated, _) = db.pgt_status("aq_combo_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);
    assert_eq!(db.count("public.aq_combo_st").await, 1);
}

// ── Error / Rejection Tests ────────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_invalid_query() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_invalid (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO aq_invalid VALUES (1)").await;

    db.create_st("aq_invalid_st", "SELECT id FROM aq_invalid", "1m", "FULL")
        .await;

    // Try an invalid query
    let result = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table('aq_invalid_st', \
             query => 'SELECT nonexistent_col FROM aq_invalid')",
        )
        .await;
    assert!(result.is_err(), "Invalid query should be rejected");

    // Original ST should still be intact
    let (status, _, populated, _) = db.pgt_status("aq_invalid_st").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
    assert_eq!(db.count("public.aq_invalid_st").await, 1);
}

#[tokio::test]
async fn test_alter_query_cycle_detection() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_cyc_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO aq_cyc_src VALUES (1)").await;

    // Create two stream tables
    db.create_st("aq_cyc_a", "SELECT id FROM aq_cyc_src", "1m", "FULL")
        .await;

    db.create_st("aq_cyc_b", "SELECT id FROM aq_cyc_src", "1m", "FULL")
        .await;

    // Try to make A depend on B (which doesn't introduce a cycle since B -> src)
    // ... but if B also depended on A, it would be a cycle.
    // For a clean cycle test: make B depend on A, then try to make A depend on B.
    db.alter_st("aq_cyc_b", "query => $$ SELECT id FROM aq_cyc_a $$")
        .await;

    // Now try to make A depend on B — creating A -> B -> A cycle
    let result = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table('aq_cyc_a', \
             query => $$ SELECT id FROM aq_cyc_b $$)",
        )
        .await;
    assert!(
        result.is_err(),
        "ALTER QUERY that introduces a cycle should be rejected"
    );

    // A should still be intact with original query
    let (status, _, populated, _) = db.pgt_status("aq_cyc_a").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
}

// ── View Inlining Test ─────────────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_view_inlining() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_vw_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO aq_vw_src VALUES (1, 'hello'), (2, 'world')")
        .await;

    db.execute("CREATE VIEW aq_vw AS SELECT id, val FROM aq_vw_src WHERE id > 0")
        .await;

    db.create_st("aq_vw_st", "SELECT id, val FROM aq_vw_src", "1m", "FULL")
        .await;

    assert_eq!(db.count("public.aq_vw_st").await, 2);

    // Alter to use a view (should be inlined automatically)
    db.alter_st("aq_vw_st", "query => $$ SELECT id, val FROM aq_vw $$")
        .await;

    let (status, _, populated, _) = db.pgt_status("aq_vw_st").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
    assert_eq!(db.count("public.aq_vw_st").await, 2);

    // The original_query should be preserved in the catalog
    let original: String = db
        .query_scalar(
            "SELECT original_query FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'aq_vw_st'",
        )
        .await;
    assert!(
        original.contains("aq_vw"),
        "original_query should reference the view name"
    );
}

// ── Protected Rebuild OID Test ───────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_oid_changes_for_semantic_same_schema_change() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_oid (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO aq_oid VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st("aq_oid_st", "SELECT id, val FROM aq_oid", "1m", "FULL")
        .await;

    let oid_before = db.table_oid("aq_oid_st").await;

    // Same output schema is not enough to reuse state: the WHERE clause
    // changes the materialized result, so ALTER QUERY uses a shadow rebuild.
    db.alter_st(
        "aq_oid_st",
        "query => $$ SELECT id, val FROM aq_oid WHERE id = 1 $$",
    )
    .await;

    let oid_after = db.table_oid("aq_oid_st").await;
    assert_ne!(
        oid_before, oid_after,
        "Storage table OID should change for a semantic same-schema ALTER QUERY"
    );
}

// ── Catalog Update Verification ────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_catalog_updated() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_cat (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO aq_cat VALUES (1, 'a')").await;

    db.create_st("aq_cat_st", "SELECT id, val FROM aq_cat", "1m", "FULL")
        .await;

    let query_before: String = db
        .query_scalar(
            "SELECT defining_query FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'aq_cat_st'",
        )
        .await;

    db.alter_st(
        "aq_cat_st",
        "query => $$ SELECT id, val FROM aq_cat WHERE id > 0 $$",
    )
    .await;

    let query_after: String = db
        .query_scalar(
            "SELECT defining_query FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'aq_cat_st'",
        )
        .await;

    assert_ne!(
        query_before, query_after,
        "defining_query should be updated in catalog"
    );
    assert!(
        query_after.contains("id > 0"),
        "New defining query should contain the new WHERE clause"
    );

    // frontier should be reset (not NULL after full refresh in Phase 5)
    let populated: bool = db
        .query_scalar(
            "SELECT is_populated FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'aq_cat_st'",
        )
        .await;
    assert!(populated, "ST should be populated after ALTER QUERY");
}

// ── Data Correctness Validation ────────────────────────────────────────

#[tokio::test]
async fn test_alter_query_data_correctness_with_dml_cycle() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE aq_dml_src (id INT PRIMARY KEY, val INT, status TEXT)")
        .await;
    db.execute("INSERT INTO aq_dml_src VALUES (1, 10, 'A'), (2, 20, 'B')")
        .await;

    // Phase 1: Create ST and verify
    db.create_st(
        "aq_dml_st",
        "SELECT id, val FROM aq_dml_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("aq_dml_st", "SELECT id, val FROM aq_dml_src")
        .await;

    // Phase 2: DML cycle
    db.execute("INSERT INTO aq_dml_src VALUES (3, 30, 'C')")
        .await;
    db.execute("UPDATE aq_dml_src SET val = 25 WHERE id = 2")
        .await;
    db.execute("DELETE FROM aq_dml_src WHERE id = 1").await;

    db.refresh_st("aq_dml_st").await;
    db.assert_st_matches_query("aq_dml_st", "SELECT id, val FROM aq_dml_src")
        .await;

    // Phase 3: Alter Query (add aggregation/filtering)
    db.alter_st(
        "aq_dml_st",
        "query => $$ SELECT status, SUM(val) as total_val FROM aq_dml_src WHERE val > 0 GROUP BY status $$",
    )
    .await;

    // Phase 4: Verify post-alter match
    db.assert_st_matches_query(
        "aq_dml_st",
        "SELECT status, SUM(val) as total_val FROM aq_dml_src WHERE val > 0 GROUP BY status",
    )
    .await;

    // Phase 5: Further DML to ensure differential mode works on the altered ST
    db.execute("INSERT INTO aq_dml_src VALUES (4, 40, 'B')")
        .await;
    db.execute("UPDATE aq_dml_src SET val = 35 WHERE id = 3")
        .await;

    db.refresh_st("aq_dml_st").await;
    db.assert_st_matches_query(
        "aq_dml_st",
        "SELECT status, SUM(val) as total_val FROM aq_dml_src WHERE val > 0 GROUP BY status",
    )
    .await;
}

// Regression for #1429: ALTER must see public columns which happen to
// match the wildcard expression '__pgt_%', including quoted identifiers.
#[tokio::test]
async fn test_alter_query_and_partition_keep_public_pgt_lookalikes() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        r#"CREATE TABLE prefix_alter_source (
            id INT PRIMARY KEY,
            abpgtc INT NOT NULL,
            xxpgt_value INT,
            "AbpgtC" INT
        )"#,
    )
    .await;
    db.execute("INSERT INTO prefix_alter_source VALUES (1, 10, 100, 1000), (2, 20, 200, 2000)")
        .await;
    let query = r#"SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_alter_source"#;
    db.create_st("prefix_alter_st", query, "1d", "DIFFERENTIAL")
        .await;

    let compatible: String = db
        .query_scalar(
            r#"SELECT pgtrickle.explain_alter(
                'prefix_alter_st',
                'SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_alter_source'
            ) ->> 'classification'"#,
        )
        .await;
    assert_eq!(
        compatible, "compatible",
        "an unchanged query must retain every user-visible column"
    );

    let rebuildable: String = db
        .query_scalar(
            r#"SELECT pgtrickle.explain_alter(
                'prefix_alter_st',
                'SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_alter_source WHERE id > 1'
            ) ->> 'classification'"#,
        )
        .await;
    assert_eq!(rebuildable, "rebuildable");

    let filtered_query =
        r#"SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_alter_source WHERE id > 1"#;
    db.alter_st(
        "prefix_alter_st",
        r#"query => 'SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_alter_source WHERE id > 1'"#,
    )
    .await;
    db.assert_st_matches_query("prefix_alter_st", filtered_query)
        .await;

    // No downstream dependents: repartitioning is supported, and a column
    // accidentally filtered by LIKE used to fail key validation here.
    let missing_key = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table('prefix_alter_st', partition_by => 'missing')",
        )
        .await;
    assert!(
        missing_key.is_err(),
        "unknown partition key must be rejected"
    );
    db.alter_st("prefix_alter_st", "partition_by => 'abpgtc'")
        .await;
    let key: String = db
        .query_scalar(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'prefix_alter_st'",
        )
        .await;
    assert_eq!(key, "abpgtc");
    let is_partitioned: bool = db
        .query_scalar(
            "SELECT relkind = 'p' FROM pg_catalog.pg_class \
             WHERE oid = 'public.prefix_alter_st'::regclass",
        )
        .await;
    assert!(is_partitioned);
    db.assert_st_matches_query("prefix_alter_st", filtered_query)
        .await;

    let value: (i32, i32, i32, i32) =
        sqlx::query_as(r#"SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_alter_st"#)
            .fetch_one(&db.pool)
            .await
            .expect("read repartitioned public columns");
    assert_eq!(value, (2, 20, 200, 2000));
}
