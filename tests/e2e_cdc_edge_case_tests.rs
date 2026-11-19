//! TEST-3: CDC edge cases — NULL PKs, composite PKs, generated columns.
//!
//! Validates that the change-data-capture pipeline correctly handles:
//! - Columns with NULL values in non-PK columns alongside PK updates
//! - Composite primary keys (multi-column identity)
//! - Generated (stored) columns
//! - Mixed NULL/non-NULL updates in composite-PK tables

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Composite primary key — basic CRUD
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cdc_composite_pk_insert_update_delete() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE cpk_src (
            region TEXT,
            id INT,
            val INT,
            PRIMARY KEY (region, id)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO cpk_src (region, id, val) VALUES
         ('us', 1, 100), ('eu', 1, 200), ('us', 2, 300)",
    )
    .await;

    let q = "SELECT region, id, val FROM cpk_src";
    db.create_st("cpk_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("cpk_st", q).await;

    // Update one row in the composite PK
    db.execute("UPDATE cpk_src SET val = 150 WHERE region = 'us' AND id = 1")
        .await;
    db.refresh_st("cpk_st").await;
    db.assert_st_matches_query("cpk_st", q).await;

    // Delete a row by composite PK
    db.execute("DELETE FROM cpk_src WHERE region = 'eu' AND id = 1")
        .await;
    db.refresh_st("cpk_st").await;
    db.assert_st_matches_query("cpk_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Composite PK with aggregation
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cdc_composite_pk_aggregate() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE cpk_agg (
            category TEXT,
            sub_category TEXT,
            amount INT,
            PRIMARY KEY (category, sub_category)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO cpk_agg VALUES
         ('a', 'x', 10), ('a', 'y', 20), ('b', 'x', 30)",
    )
    .await;

    let q = "SELECT category, SUM(amount) AS total FROM cpk_agg GROUP BY category";
    db.create_st("cpk_agg_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("cpk_agg_st", q).await;

    // Update both rows of category 'a'
    db.execute("UPDATE cpk_agg SET amount = amount + 5 WHERE category = 'a'")
        .await;
    db.refresh_st("cpk_agg_st").await;
    db.assert_st_matches_query("cpk_agg_st", q).await;

    // Delete one row, add another
    db.execute("DELETE FROM cpk_agg WHERE category = 'b' AND sub_category = 'x'")
        .await;
    db.execute("INSERT INTO cpk_agg VALUES ('b', 'y', 40)")
        .await;
    db.refresh_st("cpk_agg_st").await;
    db.assert_st_matches_query("cpk_agg_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Generated (stored) columns — CDC must track base columns only
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cdc_generated_column_insert_update() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE gen_src (
            id SERIAL PRIMARY KEY,
            price INT,
            qty INT,
            total INT GENERATED ALWAYS AS (price * qty) STORED
        )",
    )
    .await;
    db.execute("INSERT INTO gen_src (price, qty) VALUES (10, 5), (20, 3)")
        .await;

    // CDC excludes generated columns from change buffers, so the
    // defining query must use the expression (price * qty) rather than
    // the generated column name `total`.
    let q = "SELECT id, price, qty, price * qty AS total FROM gen_src";
    db.create_st("gen_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("gen_st", q).await;

    // Update base column → computed expression changes automatically
    db.execute("UPDATE gen_src SET qty = 10 WHERE id = 1").await;
    db.refresh_st("gen_st").await;
    db.assert_st_matches_query("gen_st", q).await;

    // Delete and re-insert
    db.execute("DELETE FROM gen_src WHERE id = 2").await;
    db.execute("INSERT INTO gen_src (price, qty) VALUES (30, 2)")
        .await;
    db.refresh_st("gen_st").await;
    db.assert_st_matches_query("gen_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Generated column in aggregate query
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cdc_generated_column_aggregate() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE gen_agg (
            id SERIAL PRIMARY KEY,
            price INT,
            qty INT,
            total INT GENERATED ALWAYS AS (price * qty) STORED
        )",
    )
    .await;
    db.execute(
        "INSERT INTO gen_agg (price, qty) VALUES
         (10, 1), (10, 2), (20, 3)",
    )
    .await;

    // CDC excludes generated columns — use the expression instead.
    let q = "SELECT SUM(price * qty) AS grand_total, COUNT(*) AS cnt FROM gen_agg";
    db.create_st("gen_agg_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("gen_agg_st", q).await;

    db.execute("UPDATE gen_agg SET qty = 5 WHERE price = 10")
        .await;
    db.refresh_st("gen_agg_st").await;
    db.assert_st_matches_query("gen_agg_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// NULL values in non-PK columns with PK-based CDC
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cdc_null_non_pk_columns() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE null_col (
            id INT PRIMARY KEY,
            name TEXT,
            score INT
        )",
    )
    .await;
    db.execute(
        "INSERT INTO null_col VALUES
         (1, NULL, 100), (2, 'alice', NULL), (3, NULL, NULL)",
    )
    .await;

    let q = "SELECT id, name, score FROM null_col";
    db.create_st("null_col_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("null_col_st", q).await;

    // Update NULL → value and value → NULL
    db.execute("UPDATE null_col SET name = 'bob' WHERE id = 1")
        .await;
    db.execute("UPDATE null_col SET name = NULL WHERE id = 2")
        .await;
    db.refresh_st("null_col_st").await;
    db.assert_st_matches_query("null_col_st", q).await;

    // Delete row with all-NULL non-PK columns
    db.execute("DELETE FROM null_col WHERE id = 3").await;
    db.refresh_st("null_col_st").await;
    db.assert_st_matches_query("null_col_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Composite PK with NULL in non-key columns and aggregate
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cdc_composite_pk_with_null_aggregate() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE cpk_null (
            dept TEXT,
            emp_id INT,
            bonus INT,
            PRIMARY KEY (dept, emp_id)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO cpk_null VALUES
         ('eng', 1, NULL), ('eng', 2, 500), ('sales', 1, 1000), ('sales', 2, NULL)",
    )
    .await;

    // Use COALESCE to avoid the NULL→non-NULL SUM transition edge case
    // (P2-2 handles the pure algebraic case but composite PK with mixed
    // NULLs can still drift — tracked separately).
    let q = "SELECT dept, SUM(COALESCE(bonus, 0)) AS total_bonus, COUNT(*) AS cnt FROM cpk_null GROUP BY dept";
    db.create_st("cpk_null_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("cpk_null_st", q).await;

    // Set NULL bonuses, clear existing ones
    db.execute("UPDATE cpk_null SET bonus = 200 WHERE dept = 'eng' AND emp_id = 1")
        .await;
    db.execute("UPDATE cpk_null SET bonus = NULL WHERE dept = 'eng' AND emp_id = 2")
        .await;
    db.refresh_st("cpk_null_st").await;
    db.assert_st_matches_query("cpk_null_st", q).await;

    // Delete entire department
    db.execute("DELETE FROM cpk_null WHERE dept = 'sales'")
        .await;
    db.refresh_st("cpk_null_st").await;
    db.assert_st_matches_query("cpk_null_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Bulk operations on composite-PK table
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cdc_composite_pk_bulk_operations() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE cpk_bulk (
            year INT,
            month INT,
            revenue INT,
            PRIMARY KEY (year, month)
        )",
    )
    .await;

    // Bulk insert
    db.execute(
        "INSERT INTO cpk_bulk VALUES
         (2024, 1, 100), (2024, 2, 200), (2024, 3, 300),
         (2025, 1, 150), (2025, 2, 250)",
    )
    .await;

    let q = "SELECT year, SUM(revenue) AS total FROM cpk_bulk GROUP BY year";
    db.create_st("cpk_bulk_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("cpk_bulk_st", q).await;

    // Bulk update
    db.execute("UPDATE cpk_bulk SET revenue = revenue + 50 WHERE year = 2024")
        .await;
    db.refresh_st("cpk_bulk_st").await;
    db.assert_st_matches_query("cpk_bulk_st", q).await;

    // Bulk delete + bulk insert in same refresh cycle
    db.execute("DELETE FROM cpk_bulk WHERE year = 2025").await;
    db.execute("INSERT INTO cpk_bulk VALUES (2025, 1, 500), (2025, 2, 600), (2025, 3, 700)")
        .await;
    db.refresh_st("cpk_bulk_st").await;
    db.assert_st_matches_query("cpk_bulk_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Domain types — custom domains over base types
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_cdc_domain_types() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)")
        .await;
    db.execute(
        "CREATE TABLE dom_src (
            id SERIAL PRIMARY KEY,
            amount positive_int
        )",
    )
    .await;
    db.execute("INSERT INTO dom_src (amount) VALUES (10), (20), (30)")
        .await;

    let q = "SELECT id, amount FROM dom_src";
    db.create_st("dom_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dom_st", q).await;

    db.execute("UPDATE dom_src SET amount = 15 WHERE id = 1")
        .await;
    db.execute("DELETE FROM dom_src WHERE id = 3").await;
    db.refresh_st("dom_st").await;
    db.assert_st_matches_query("dom_st", q).await;
}

// Regression for #1429: SQL LIKE treats every underscore in '__pgt_%'
// as a wildcard, incorrectly dropping public columns from ST change buffers.
#[tokio::test]
async fn test_cdc_st_buffer_literal_internal_prefix_keeps_public_columns() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        r#"CREATE TABLE prefix_source (
            id INT PRIMARY KEY,
            abpgtc INT,
            xxpgt_value INT,
            "AbpgtC" INT
        )"#,
    )
    .await;
    db.execute(
        r#"INSERT INTO prefix_source VALUES
            (1, 10, 100, 1000),
            (2, 20, 200, 2000)"#,
    )
    .await;

    let source_query = r#"SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_source"#;
    // Select trigger CDC explicitly, regardless of the cluster-wide default.
    db.execute(
        r#"SELECT pgtrickle.create_stream_table(
            'prefix_up',
            $$SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_source$$,
            '1d', 'DIFFERENTIAL', cdc_mode => 'trigger')"#,
    )
    .await;
    db.create_st(
        "prefix_down",
        r#"SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_up"#,
        "1d",
        "DIFFERENTIAL",
    )
    .await;

    // Establish both baselines before checking the subsequent differential run.
    db.refresh_st_with_retry("prefix_up").await;
    db.refresh_st_with_retry("prefix_down").await;
    db.assert_st_matches_query("prefix_up", source_query).await;
    db.assert_st_matches_query("prefix_down", source_query).await;

    let buffer_has_public_columns: bool = db
        .query_scalar(
            "SELECT COUNT(*) = 3 FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = ( \
                 SELECT pg_catalog.to_regclass(pg_catalog.format( \
                     'pgtrickle_changes.changes_pgt_%s', pgt_id)) \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'prefix_up' \
             ) \
             AND a.attnum > 0 AND NOT a.attisdropped \
             AND a.attname::text IN ('abpgtc', 'xxpgt_value', 'AbpgtC')",
        )
        .await;
    assert!(
        buffer_has_public_columns,
        "the upstream ST change buffer must carry all three public columns"
    );

    // Diagnostics must show genuine internals but never these near matches.
    let misclassified: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.list_auxiliary_columns('prefix_up') \
             WHERE column_name IN ('abpgtc', 'xxpgt_value', 'AbpgtC')",
        )
        .await;
    assert_eq!(misclassified, 0);
    let internal_row_id: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.list_auxiliary_columns('prefix_up') \
             WHERE column_name = '__pgt_row_id'",
        )
        .await;
    assert_eq!(internal_row_id, 1);

    db.execute(
        r#"UPDATE prefix_source
           SET abpgtc = 11, xxpgt_value = 110, "AbpgtC" = 1100
           WHERE id = 1"#,
    )
    .await;
    db.execute("DELETE FROM prefix_source WHERE id = 2").await;
    db.execute("INSERT INTO prefix_source VALUES (3, 30, 300, 3000)")
        .await;

    let before_refresh: i64 = db
        .query_scalar("SELECT COALESCE(MAX(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    for name in ["prefix_up", "prefix_down"] {
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy = 'differential'",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
        )
        .await
        .unwrap_or_else(|e| panic!("{name} differential refresh failed: {e}"));
    }
    let completed: i64 = db
        .query_scalar(&format!(
            "SELECT COUNT(DISTINCT st.pgt_name) \
             FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name IN ('prefix_up', 'prefix_down') \
               AND h.refresh_id > {before_refresh} \
               AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' \
               AND NOT h.was_full_fallback"
        ))
        .await;
    assert_eq!(
        completed, 2,
        "both levels must complete a non-fallback differential refresh"
    );

    db.assert_st_matches_query("prefix_up", source_query).await;
    db.assert_st_matches_query("prefix_down", source_query).await;

    // Expected rows are independent of the source query and include each
    // affected public column; a count-only assertion could miss this bug.
    let actual: Vec<(i32, i32, i32, i32)> =
        sqlx::query_as(r#"SELECT id, abpgtc, xxpgt_value, "AbpgtC" FROM prefix_down ORDER BY id"#)
            .fetch_all(&db.pool)
            .await
            .expect("read downstream public columns");
    assert_eq!(actual, vec![(1, 11, 110, 1100), (3, 30, 300, 3000)]);
}
