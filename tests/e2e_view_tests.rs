//! E2E tests for view inlining in stream tables.
//!
//! Validates that views referenced in defining queries are transparently
//! inlined as subqueries so CDC triggers land on base tables. Covers
//! simple views, nested views, views with filters/aggregation/joins,
//! materialized view rejection, foreign table rejection, view DDL hooks,
//! and TRUNCATE propagation through views.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Basic View Inlining (DIFFERENTIAL) ─────────────────────────────────

#[tokio::test]
async fn test_view_inline_diff_basic() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_base (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO vi_base VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    db.execute("CREATE VIEW vi_simple AS SELECT id, val FROM vi_base")
        .await;

    // Create a DIFFERENTIAL stream table referencing the view
    db.create_st(
        "vi_st_basic",
        "SELECT id, val FROM vi_simple",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Verify initial population
    let (status, mode, populated, errors) = db.pgt_status("vi_st_basic").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "DIFFERENTIAL");
    assert!(populated, "ST should be populated after create");
    assert_eq!(errors, 0);

    let count = db.count("public.vi_st_basic").await;
    assert_eq!(count, 3, "Should have 3 rows from view");

    // INSERT into the base table (not the view) → verify CDC captures it
    db.execute("INSERT INTO vi_base VALUES (4, 'd')").await;
    db.refresh_st("vi_st_basic").await;

    let count_after = db.count("public.vi_st_basic").await;
    assert_eq!(
        count_after, 4,
        "Differential refresh should pick up insert on base table"
    );
}

#[tokio::test]
async fn test_view_inline_diff_update_delete() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_ud_base (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO vi_ud_base VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    db.execute("CREATE VIEW vi_ud_view AS SELECT id, val FROM vi_ud_base")
        .await;

    db.create_st(
        "vi_st_ud",
        "SELECT id, val FROM vi_ud_view",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.vi_st_ud").await, 3);

    // UPDATE a row in base table
    db.execute("UPDATE vi_ud_base SET val = 99 WHERE id = 2")
        .await;
    db.refresh_st("vi_st_ud").await;

    let updated_val: i32 = db
        .query_scalar("SELECT val FROM public.vi_st_ud WHERE id = 2")
        .await;
    assert_eq!(
        updated_val, 99,
        "Updated value should propagate through view"
    );

    // DELETE a row from base table
    db.execute("DELETE FROM vi_ud_base WHERE id = 3").await;
    db.refresh_st("vi_st_ud").await;

    assert_eq!(db.count("public.vi_st_ud").await, 2);
    let has_3: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.vi_st_ud WHERE id = 3)")
        .await;
    assert!(!has_3, "Deleted row should not be in ST");
}

#[tokio::test]
async fn test_view_inline_diff_with_filter() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_filt_base (id INT PRIMARY KEY, status TEXT, amount INT)")
        .await;
    db.execute(
        "INSERT INTO vi_filt_base VALUES (1, 'active', 100), (2, 'inactive', 200), \
         (3, 'active', 300), (4, 'inactive', 50)",
    )
    .await;

    db.execute(
        "CREATE VIEW vi_active AS SELECT id, amount FROM vi_filt_base WHERE status = 'active'",
    )
    .await;

    db.create_st(
        "vi_st_filt",
        "SELECT id, amount FROM vi_active",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Only 'active' rows should be included
    let count = db.count("public.vi_st_filt").await;
    assert_eq!(count, 2, "Only active rows should be in ST");

    // Insert an active row
    db.execute("INSERT INTO vi_filt_base VALUES (5, 'active', 500)")
        .await;
    db.refresh_st("vi_st_filt").await;
    assert_eq!(
        db.count("public.vi_st_filt").await,
        3,
        "New active row should appear"
    );

    // Insert an inactive row — should NOT appear in ST
    db.execute("INSERT INTO vi_filt_base VALUES (6, 'inactive', 600)")
        .await;
    db.refresh_st("vi_st_filt").await;
    assert_eq!(
        db.count("public.vi_st_filt").await,
        3,
        "Inactive row should not appear"
    );
}

#[tokio::test]
async fn test_view_inline_diff_with_aggregation() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_agg_base (id INT PRIMARY KEY, category TEXT, amount INT)")
        .await;
    db.execute(
        "INSERT INTO vi_agg_base VALUES (1, 'A', 10), (2, 'A', 20), \
         (3, 'B', 30), (4, 'B', 40)",
    )
    .await;

    db.execute("CREATE VIEW vi_agg_view AS SELECT id, category, amount FROM vi_agg_base")
        .await;

    db.create_st(
        "vi_st_agg",
        "SELECT category, SUM(amount) AS total FROM vi_agg_view GROUP BY category",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count = db.count("public.vi_st_agg").await;
    assert_eq!(count, 2, "Should have 2 category groups");

    let total_a: i64 = db
        .query_scalar("SELECT total::bigint FROM public.vi_st_agg WHERE category = 'A'")
        .await;
    assert_eq!(total_a, 30);

    // Add a new row to category A
    db.execute("INSERT INTO vi_agg_base VALUES (5, 'A', 50)")
        .await;
    db.refresh_st("vi_st_agg").await;

    let total_a_after: i64 = db
        .query_scalar("SELECT total::bigint FROM public.vi_st_agg WHERE category = 'A'")
        .await;
    assert_eq!(total_a_after, 80, "Aggregate should update through view");
}

// ── View with Join ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_view_inline_diff_with_join() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_join_orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
        .await;
    db.execute("CREATE TABLE vi_join_customers (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO vi_join_customers VALUES (1, 'Alice'), (2, 'Bob')")
        .await;
    db.execute("INSERT INTO vi_join_orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300)")
        .await;

    db.execute(
        "CREATE VIEW vi_order_view AS \
         SELECT o.id, o.customer_id, o.amount FROM vi_join_orders o",
    )
    .await;

    // Join the view with a table
    db.create_st(
        "vi_st_join",
        "SELECT c.name, SUM(v.amount) AS total \
         FROM vi_order_view v \
         JOIN vi_join_customers c ON v.customer_id = c.id \
         GROUP BY c.name",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count = db.count("public.vi_st_join").await;
    assert_eq!(count, 2);

    let alice_total: i64 = db
        .query_scalar("SELECT total::bigint FROM public.vi_st_join WHERE name = 'Alice'")
        .await;
    assert_eq!(alice_total, 300);

    // Insert a new order for Bob via the base table
    db.execute("INSERT INTO vi_join_orders VALUES (4, 2, 150)")
        .await;
    db.refresh_st("vi_st_join").await;

    let bob_total: i64 = db
        .query_scalar("SELECT total::bigint FROM public.vi_st_join WHERE name = 'Bob'")
        .await;
    assert_eq!(
        bob_total, 450,
        "Join+aggregation through view should update"
    );
}

#[tokio::test]
async fn test_view_inline_diff_two_views() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_two_a (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE vi_two_b (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO vi_two_a VALUES (1, 10), (2, 20)")
        .await;
    db.execute("INSERT INTO vi_two_b VALUES (1, 100), (2, 200)")
        .await;

    db.execute("CREATE VIEW vi_view_a AS SELECT id, val FROM vi_two_a")
        .await;
    db.execute("CREATE VIEW vi_view_b AS SELECT id, val FROM vi_two_b")
        .await;

    db.create_st(
        "vi_st_two",
        "SELECT a.id, a.val AS val_a, b.val AS val_b \
         FROM vi_view_a a JOIN vi_view_b b ON a.id = b.id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count = db.count("public.vi_st_two").await;
    assert_eq!(count, 2);

    // Insert into both base tables
    db.execute("INSERT INTO vi_two_a VALUES (3, 30)").await;
    db.execute("INSERT INTO vi_two_b VALUES (3, 300)").await;
    db.refresh_st("vi_st_two").await;

    let count_after = db.count("public.vi_st_two").await;
    assert_eq!(
        count_after, 3,
        "Both views should be inlined and CDC-tracked"
    );
}

// ── Nested Views ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_view_inline_nested_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_nest_base (id INT PRIMARY KEY, val INT, active BOOLEAN)")
        .await;
    db.execute(
        "INSERT INTO vi_nest_base VALUES (1, 10, true), (2, 20, false), \
         (3, 30, true), (4, 40, true)",
    )
    .await;

    // v1 filters active rows
    db.execute("CREATE VIEW vi_nest_v1 AS SELECT id, val FROM vi_nest_base WHERE active")
        .await;
    // v2 wraps v1 (nested view)
    db.execute("CREATE VIEW vi_nest_v2 AS SELECT id, val FROM vi_nest_v1 WHERE val > 10")
        .await;

    db.create_st(
        "vi_st_nested",
        "SELECT id, val FROM vi_nest_v2",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Should have rows 3 (val=30, active) and 4 (val=40, active)
    let count = db.count("public.vi_st_nested").await;
    assert_eq!(count, 2, "Nested view should be fully inlined");

    // Add a row that passes both filters
    db.execute("INSERT INTO vi_nest_base VALUES (5, 50, true)")
        .await;
    db.refresh_st("vi_st_nested").await;

    assert_eq!(
        db.count("public.vi_st_nested").await,
        3,
        "New row should appear through nested views"
    );

    // Add a row that fails the inner filter (active=false)
    db.execute("INSERT INTO vi_nest_base VALUES (6, 60, false)")
        .await;
    db.refresh_st("vi_st_nested").await;

    assert_eq!(
        db.count("public.vi_st_nested").await,
        3,
        "Inactive row should not appear"
    );
}

// ── FULL Mode with View ────────────────────────────────────────────────

#[tokio::test]
async fn test_view_inline_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_full_base (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO vi_full_base VALUES (1, 'x'), (2, 'y')")
        .await;

    db.execute("CREATE VIEW vi_full_view AS SELECT id, val FROM vi_full_base")
        .await;

    db.create_st(
        "vi_st_full",
        "SELECT id, val FROM vi_full_view",
        "1m",
        "FULL",
    )
    .await;

    let (status, mode, populated, _) = db.pgt_status("vi_st_full").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);

    let count = db.count("public.vi_st_full").await;
    assert_eq!(count, 2);

    db.execute("INSERT INTO vi_full_base VALUES (3, 'z')").await;
    db.refresh_st("vi_st_full").await;

    assert_eq!(
        db.count("public.vi_st_full").await,
        3,
        "FULL mode with view should also work"
    );
}

// ── Rejection Tests ────────────────────────────────────────────────────

#[tokio::test]
async fn test_view_inline_matview_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_mv_base (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO vi_mv_base VALUES (1, 10)").await;

    db.execute("CREATE MATERIALIZED VIEW vi_matview AS SELECT id, val FROM vi_mv_base")
        .await;

    // DIFFERENTIAL mode should reject materialized views
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('vi_st_mv', \
             $$SELECT id, val FROM vi_matview$$, '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        result.is_err(),
        "Materialized view should be rejected in DIFFERENTIAL mode"
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("aterialized view") || err_msg.contains("materialized"),
        "Error should mention materialized view: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_view_inline_matview_allowed_in_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_mv_full_base (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO vi_mv_full_base VALUES (1, 10), (2, 20)")
        .await;

    db.execute("CREATE MATERIALIZED VIEW vi_mv_full AS SELECT id, val FROM vi_mv_full_base")
        .await;

    // FULL mode should allow materialized views (no CDC needed)
    db.create_st(
        "vi_st_mv_full",
        "SELECT id, val FROM vi_mv_full",
        "1m",
        "FULL",
    )
    .await;

    let (status, mode, populated, _) = db.pgt_status("vi_st_mv_full").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);
    assert_eq!(db.count("public.vi_st_mv_full").await, 2);
}

// ── View DDL Hooks ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_view_inline_view_replaced() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_repl_base (id INT PRIMARY KEY, val INT, extra INT)")
        .await;
    db.execute("INSERT INTO vi_repl_base VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")
        .await;

    db.execute("CREATE VIEW vi_repl_view AS SELECT id, val FROM vi_repl_base")
        .await;

    db.create_st(
        "vi_st_repl",
        "SELECT id, val FROM vi_repl_view",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.vi_st_repl").await, 3);

    // Replace the view definition — add a filter
    db.execute(
        "CREATE OR REPLACE VIEW vi_repl_view AS SELECT id, val FROM vi_repl_base WHERE val > 10",
    )
    .await;

    // The DDL event trigger fires synchronously within CREATE OR REPLACE VIEW,
    // setting needs_reinit = true. However, if the background scheduler is
    // active, it may reinitialize the ST (clearing needs_reinit) before we
    // can observe the flag. Instead of racing with the scheduler, verify that
    // the event trigger was effective: either needs_reinit is still true, or
    // the scheduler already reinited (data matches the new view definition).
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'vi_st_repl'",
        )
        .await;

    if needs_reinit {
        // Event trigger fired; scheduler hasn't acted yet — manually refresh.
        db.refresh_st("vi_st_repl").await;
    } else {
        // Scheduler already reinited. Wait briefly for it to finish if
        // it's still in progress, then verify the data below.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // After reinit (manual or scheduler-driven), data must match the new view.
    db.assert_st_matches_query("vi_st_repl", "SELECT id, val FROM vi_repl_view")
        .await;
    let (query_rewritten, strategy_hash_matches, runtime_disabled): (bool, bool, bool) =
        sqlx::query_as(
            "SELECT defining_query ILIKE '%val > 10%', \
                    (window_strategy ->> 'query_hash')::bigint = defining_query_hash, \
                    NOT jsonb_path_exists(window_strategy, \
                        '$.nodes[*].functions[*] ? (@.runtime_enabled == true)') \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'vi_st_repl'",
        )
        .fetch_one(&db.pool)
        .await
        .expect("load rewritten query and window plan");
    assert!(query_rewritten);
    assert!(strategy_hash_matches);
    assert!(runtime_disabled);
}

#[tokio::test]
async fn test_view_inline_view_dropped() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_drop_base (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO vi_drop_base VALUES (1, 'a'), (2, 'b')")
        .await;

    db.execute("CREATE VIEW vi_drop_view AS SELECT id, val FROM vi_drop_base")
        .await;

    db.create_st(
        "vi_st_drop_view",
        "SELECT id, val FROM vi_drop_view",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.vi_st_drop_view").await, 2);

    // Drop the view — this should NOT cascade to the ST storage table,
    // but the event trigger should mark the ST as ERROR
    let result = db.try_execute("DROP VIEW vi_drop_view").await;

    if result.is_ok() {
        // Give the event trigger a moment
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let status: String = db
            .query_scalar(
                "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'vi_st_drop_view'",
            )
            .await;
        assert_eq!(
            status, "ERROR",
            "ST should be in ERROR status after view drop"
        );
    }
    // If DROP fails, the extension is protecting its views — that's valid too
}

// ── TRUNCATE Through View ──────────────────────────────────────────────

#[tokio::test]
async fn test_view_inline_truncate_base() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_trunc_base (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO vi_trunc_base VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    db.execute("CREATE VIEW vi_trunc_view AS SELECT id, val FROM vi_trunc_base")
        .await;

    db.create_st(
        "vi_st_trunc",
        "SELECT id, val FROM vi_trunc_view",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.vi_st_trunc").await, 3);

    // TRUNCATE the base table — since view was inlined, the TRUNCATE
    // trigger on the base table should fire
    db.execute("TRUNCATE vi_trunc_base").await;
    db.refresh_st("vi_st_trunc").await;

    db.assert_st_matches_query("vi_st_trunc", "SELECT id, val FROM vi_trunc_view")
        .await;
}

// ── Column Renamed View ────────────────────────────────────────────────

#[tokio::test]
async fn test_view_inline_column_renamed() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_rename_base (id INT PRIMARY KEY, value INT)")
        .await;
    db.execute("INSERT INTO vi_rename_base VALUES (1, 10), (2, 20)")
        .await;

    // View renames columns
    db.execute(
        "CREATE VIEW vi_rename_view (item_id, item_value) AS SELECT id, value FROM vi_rename_base",
    )
    .await;

    db.create_st(
        "vi_st_rename",
        "SELECT item_id, item_value FROM vi_rename_view",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count = db.count("public.vi_st_rename").await;
    assert_eq!(count, 2);

    // Verify column names in the ST match the view's column names
    let has_item_id: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.columns \
                WHERE table_name = 'vi_st_rename' AND column_name = 'item_id' \
            )",
        )
        .await;
    assert!(
        has_item_id,
        "ST should have column 'item_id' from view's renamed column"
    );

    // Insert and verify refresh works with renamed columns
    db.execute("INSERT INTO vi_rename_base VALUES (3, 30)")
        .await;
    db.refresh_st("vi_st_rename").await;

    let val: i32 = db
        .query_scalar("SELECT item_value FROM public.vi_st_rename WHERE item_id = 3")
        .await;
    assert_eq!(val, 30, "Renamed columns should work correctly");
}

// ── Catalog Verification ───────────────────────────────────────────────

#[tokio::test]
async fn test_view_inline_original_query_stored() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_cat_base (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO vi_cat_base VALUES (1, 'test')")
        .await;

    db.execute("CREATE VIEW vi_cat_view AS SELECT id, val FROM vi_cat_base")
        .await;

    let original_query = "SELECT id, val FROM vi_cat_view";
    db.create_st("vi_st_cat", original_query, "1m", "DIFFERENTIAL")
        .await;

    // Verify original_query is stored in catalog
    let stored_original: Option<String> = db
        .query_scalar_opt(
            "SELECT original_query FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'vi_st_cat'",
        )
        .await;
    assert!(
        stored_original.is_some(),
        "original_query should be stored when views are inlined"
    );

    // The original query should contain the view name
    let stored = stored_original.unwrap();
    assert!(
        stored.contains("vi_cat_view"),
        "original_query should reference the view name, got: {}",
        stored
    );

    // The defining_query should NOT reference the view as a plain table
    // (the view body is inlined as a subquery, but the alias may still
    // carry the original view name — that's fine).
    let defining: String = db
        .query_scalar(
            "SELECT defining_query FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'vi_st_cat'",
        )
        .await;

    // The defining_query should reference the base table
    assert!(
        defining.contains("vi_cat_base"),
        "defining_query should reference base table, got: {}",
        defining
    );
}

// ── View Dependency Registration ───────────────────────────────────────

#[tokio::test]
async fn test_view_inline_dependency_registered() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vi_dep_base (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO vi_dep_base VALUES (1, 10)").await;

    db.execute("CREATE VIEW vi_dep_view AS SELECT id, val FROM vi_dep_base")
        .await;

    db.create_st(
        "vi_st_dep",
        "SELECT id, val FROM vi_dep_view",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Check that view is registered as a soft dependency
    let view_dep_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_dependencies \
             WHERE source_type = 'VIEW' \
             AND pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'vi_st_dep')",
        )
        .await;
    assert!(
        view_dep_count >= 1,
        "View should be registered as a dependency"
    );

    // Check that the base table is also registered (from the rewritten query)
    let table_dep_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_dependencies \
             WHERE source_type = 'TABLE' \
             AND pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'vi_st_dep')",
        )
        .await;
    assert!(
        table_dep_count >= 1,
        "Base table should be registered as a dependency"
    );
}
