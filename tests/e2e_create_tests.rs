//! E2E tests for `pgtrickle.create_stream_table()`.
//!
//! Validates stream table creation with various queries, modes,
//! parameters, error conditions, and CDC infrastructure setup.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Basic Creation ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_simple_select() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO orders VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)")
        .await;

    db.create_st(
        "order_snapshot",
        "SELECT id, customer, amount FROM orders",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Verify catalog entry
    let cat_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'order_snapshot'",
        )
        .await;
    assert_eq!(cat_count, 1, "Catalog entry should exist");

    // Verify status
    let (status, mode, populated, errors) = db.pgt_status("order_snapshot").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "DIFFERENTIAL");
    assert!(
        populated,
        "ST should be populated after create with initialize=true"
    );
    assert_eq!(errors, 0);

    // Verify data materialized
    let row_count = db.count("public.order_snapshot").await;
    assert_eq!(row_count, 3, "ST should contain 3 rows");

    // Spot-check a value
    let alice_amount: i64 = db
        .query_scalar("SELECT amount::bigint FROM public.order_snapshot WHERE customer = 'Alice'")
        .await;
    assert_eq!(alice_amount, 100);
}

#[tokio::test]
async fn test_create_with_aggregation() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sales (id SERIAL PRIMARY KEY, customer_id INT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO sales (customer_id, amount) VALUES \
         (1, 100), (1, 200), (2, 300), (2, 150), (3, 500)",
    )
    .await;

    db.create_st(
        "customer_totals",
        "SELECT customer_id, SUM(amount) AS total_amount FROM sales GROUP BY customer_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.customer_totals",
        "SELECT customer_id, SUM(amount) AS total_amount FROM sales GROUP BY customer_id",
    )
    .await;
}

#[tokio::test]
async fn test_create_with_join() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .await;
    db.execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)")
        .await;

    db.create_st(
        "customer_orders",
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.cust_id",
        "1m",
        "FULL",
    )
    .await;

    db.assert_st_matches_query(
        "public.customer_orders",
        "SELECT c.name, o.amount FROM customers c JOIN orders o ON c.id = o.cust_id",
    )
    .await;
}

#[tokio::test]
async fn test_create_with_filter() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE regions (id INT PRIMARY KEY, region TEXT, revenue NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO regions VALUES (1, 'US', 100), (2, 'EU', 200), (3, 'US', 300), (4, 'APAC', 400)",
    )
    .await;

    db.create_st(
        "us_revenue",
        "SELECT id, revenue FROM regions WHERE region = 'US'",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.us_revenue").await;
    assert_eq!(count, 2, "Only US rows should be materialized");
}

// ── Refresh Mode Configuration ─────────────────────────────────────────

#[tokio::test]
async fn test_create_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_full (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_full VALUES (1, 'a')").await;

    db.create_st("st_full", "SELECT id, val FROM src_full", "1m", "FULL")
        .await;

    let (_, mode, _, _) = db.pgt_status("st_full").await;
    assert_eq!(mode, "FULL");
}

#[tokio::test]
async fn test_create_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_inc (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_inc VALUES (1, 'a')").await;

    db.create_st(
        "st_inc",
        "SELECT id, val FROM src_inc",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let (_, mode, _, _) = db.pgt_status("st_inc").await;
    assert_eq!(mode, "DIFFERENTIAL");
}

// ── Schedule Variants ────────────────────────────────────────────────

#[tokio::test]
async fn test_create_custom_schedule() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_sched (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_sched VALUES (1)").await;

    db.create_st("st_custom_sched", "SELECT id FROM src_sched", "5m", "FULL")
        .await;

    // Verify stored in catalog (text)
    let sched: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_custom_sched'",
        )
        .await;
    assert_eq!(sched, "5m", "Schedule should be '5m'");
}

#[tokio::test]
async fn test_create_null_schedule() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_calc (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_calc VALUES (1)").await;

    // Pass 'calculated' schedule string (CALCULATED mode; NULL is no longer accepted)
    db.execute(
        "SELECT pgtrickle.create_stream_table('st_calc', \
         $$ SELECT id FROM src_calc $$, 'calculated', 'FULL')",
    )
    .await;

    let is_null: bool = db
        .query_scalar(
            "SELECT schedule IS NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_calc'",
        )
        .await;
    assert!(
        is_null,
        "schedule should be NULL in catalog for CALCULATED mode"
    );
}

// ── Initialize Parameter ───────────────────────────────────────────────

#[tokio::test]
async fn test_create_no_initialize() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_noinit (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_noinit VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st_with_init(
        "st_noinit",
        "SELECT id, val FROM src_noinit",
        "1m",
        "DIFFERENTIAL",
        false,
    )
    .await;

    // Storage table should exist but be empty
    let exists = db.table_exists("public", "st_noinit").await;
    assert!(exists, "Storage table should exist");

    let count = db.count("public.st_noinit").await;
    assert_eq!(count, 0, "Table should be empty when initialize=false");

    // is_populated should be false
    let (_, _, populated, _) = db.pgt_status("st_noinit").await;
    assert!(!populated, "is_populated should be false");
}

// ── Schema-Qualified Name ──────────────────────────────────────────────

#[tokio::test]
async fn test_create_schema_qualified() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE SCHEMA myschema").await;
    db.execute("CREATE TABLE src_sq (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_sq VALUES (1, 'x')").await;

    db.create_st("myschema.my_st", "SELECT id, val FROM src_sq", "1m", "FULL")
        .await;

    // Verify created in the right schema
    let exists = db.table_exists("myschema", "my_st").await;
    assert!(exists, "ST should be created in myschema");

    let count = db.count("myschema.my_st").await;
    assert_eq!(count, 1);

    // Verify catalog entry has correct schema
    let cat_schema: String = db
        .query_scalar("SELECT pgt_schema FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'my_st'")
        .await;
    assert_eq!(cat_schema, "myschema");
}

// ── Error Cases ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_duplicate_name_fails() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_dup (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_dup VALUES (1)").await;

    db.create_st("dup_st", "SELECT id FROM src_dup", "1m", "FULL")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('dup_st', \
             $$ SELECT id FROM src_dup $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "Duplicate ST name should fail");
}

#[tokio::test]
async fn test_create_invalid_query_fails() {
    let db = E2eDb::new().await.with_extension().await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('bad_st', \
             $$ SELECT * FROM nonexistent_table $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "Invalid defining query should fail");
}

#[tokio::test]
async fn test_create_invalid_refresh_mode_fails() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_bogus (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_bogus VALUES (1)").await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('bogus_st', \
             $$ SELECT id FROM src_bogus $$, '1m', 'BOGUS')",
        )
        .await;
    assert!(result.is_err(), "Invalid refresh mode should fail");
}

// ── CDC Infrastructure Verification ────────────────────────────────────

#[tokio::test]
async fn test_create_cdc_trigger_installed() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO products VALUES (1, 'Widget')")
        .await;

    db.create_st(
        "product_st",
        "SELECT id, name FROM products",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("products").await;
    let trigger_name = db.cdc_trigger_name(source_oid as i64).await;
    let exists = db.trigger_exists(&trigger_name, "products").await;
    assert!(exists, "CDC trigger should be installed on source table");
}

#[tokio::test]
async fn test_create_ignores_global_wal_cdc_for_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE imm_wal_src (id INT, val TEXT)")
        .await;
    db.execute("INSERT INTO imm_wal_src VALUES (1, 'a'), (2, 'b')")
        .await;

    let result = db
        .try_execute(
            "WITH wal_mode AS (\
            SELECT set_config('pg_trickle.cdc_mode', 'wal', true)\
         )\
         SELECT pgtrickle.create_stream_table(\
            name => 'imm_wal_st',\
            query => $$SELECT id, val FROM imm_wal_src$$,\
            refresh_mode => 'IMMEDIATE'\
         )\
         FROM wal_mode",
        )
        .await;

    assert!(
        result.is_ok(),
        "global WAL mode must not override IMMEDIATE: {result:?}"
    );
}

#[tokio::test]
async fn test_create_immediate_rejects_explicit_wal_cdc_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE imm_explicit_wal_src (id INT PRIMARY KEY, val TEXT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                name => 'imm_explicit_wal_st',\
                query => $$SELECT id, val FROM imm_explicit_wal_src$$,\
                refresh_mode => 'IMMEDIATE',\
                cdc_mode => 'wal'\
            )",
        )
        .await;

    assert!(
        result.is_err(),
        "Explicit wal CDC must be rejected for IMMEDIATE mode"
    );

    let error = format!("{}", result.unwrap_err());
    assert!(
        error.contains("IMMEDIATE"),
        "Expected mode interaction error, got: {error}"
    );
}

#[tokio::test]
async fn test_create_change_buffer_exists() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE items (id INT PRIMARY KEY, qty INT)")
        .await;
    db.execute("INSERT INTO items VALUES (1, 10)").await;

    db.create_st("item_st", "SELECT id, qty FROM items", "1m", "DIFFERENTIAL")
        .await;

    let source_oid = db.table_oid("items").await;
    // v0.32.0+: buffer table is named by stable hash, not OID.
    let buf_table = db.change_buffer_table(source_oid as i64).await;
    let buf_name = buf_table
        .trim_start_matches("pgtrickle_changes.")
        .to_string();
    let buffer_exists = db.table_exists("pgtrickle_changes", &buf_name).await;
    assert!(buffer_exists, "Change buffer table should exist");
}

#[tokio::test]
async fn test_create_dependencies_recorded() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE dep_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO dep_src VALUES (1, 'a')").await;

    db.create_st(
        "dep_st",
        "SELECT id, val FROM dep_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("dep_src").await;

    // Verify pgt_dependencies has correct source
    let dep_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_dependencies \
             WHERE source_relid = {}::oid",
            source_oid
        ))
        .await;
    assert!(dep_count >= 1, "Dependency should be recorded");

    // Verify source_type
    let src_type: String = db
        .query_scalar(&format!(
            "SELECT source_type FROM pgtrickle.pgt_dependencies \
             WHERE source_relid = {}::oid LIMIT 1",
            source_oid
        ))
        .await;
    assert_eq!(src_type, "TABLE");
}

#[tokio::test]
async fn test_create_change_tracking_recorded() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ct_src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO ct_src VALUES (1)").await;

    db.create_st("ct_st", "SELECT id FROM ct_src", "1m", "DIFFERENTIAL")
        .await;

    let source_oid = db.table_oid("ct_src").await;

    // Verify pgt_change_tracking has an entry for this source
    let ct_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_change_tracking \
             WHERE source_relid = {}::oid",
            source_oid
        ))
        .await;
    assert!(ct_count >= 1, "Change tracking should be recorded");

    // Verify slot_name is non-empty
    let slot_name: String = db
        .query_scalar(&format!(
            "SELECT slot_name FROM pgtrickle.pgt_change_tracking \
             WHERE source_relid = {}::oid",
            source_oid
        ))
        .await;
    assert!(!slot_name.is_empty(), "slot_name should be non-empty");
}

// ── Schedule Format Variants ───────────────────────────────────────────

#[tokio::test]
async fn test_create_with_compound_duration() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_compound (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_compound VALUES (1)").await;

    db.create_st(
        "st_compound",
        "SELECT id FROM src_compound",
        "1h30m",
        "FULL",
    )
    .await;

    let sched: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_compound'",
        )
        .await;
    assert_eq!(sched, "1h30m", "Compound duration should be stored as-is");

    // Verify ST is functional
    assert_eq!(db.count("public.st_compound").await, 1);
}

#[tokio::test]
async fn test_create_with_seconds_duration() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_secs (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_secs VALUES (1)").await;

    db.create_st("st_secs", "SELECT id FROM src_secs", "90s", "FULL")
        .await;

    let sched: String = db
        .query_scalar("SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_secs'")
        .await;
    assert_eq!(sched, "90s");
}

#[tokio::test]
async fn test_create_with_hours_duration() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_hours (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_hours VALUES (1)").await;

    db.create_st("st_hours", "SELECT id FROM src_hours", "2h", "FULL")
        .await;

    let sched: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_hours'",
        )
        .await;
    assert_eq!(sched, "2h");
}

#[tokio::test]
async fn test_create_with_days_duration() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_days (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_days VALUES (1)").await;

    db.create_st("st_days", "SELECT id FROM src_days", "1d", "FULL")
        .await;

    let sched: String = db
        .query_scalar("SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_days'")
        .await;
    assert_eq!(sched, "1d");
}

#[tokio::test]
async fn test_create_with_cron_expression() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_cron (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_cron VALUES (1)").await;

    // Use cron expression: every 5 minutes
    db.execute(
        "SELECT pgtrickle.create_stream_table('st_cron', \
         $$ SELECT id FROM src_cron $$, '*/5 * * * *', 'FULL')",
    )
    .await;

    let schedule: String = db
        .query_scalar("SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_cron'")
        .await;
    assert_eq!(schedule, "*/5 * * * *");

    // Should still be functional
    assert_eq!(db.count("public.st_cron").await, 1);
}

#[tokio::test]
async fn test_create_with_cron_alias() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_cron_alias (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_cron_alias VALUES (1)").await;

    db.execute(
        "SELECT pgtrickle.create_stream_table('st_cron_alias', \
         $$ SELECT id FROM src_cron_alias $$, '@hourly', 'FULL')",
    )
    .await;

    let schedule: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'st_cron_alias'",
        )
        .await;
    assert_eq!(schedule, "@hourly");
}

#[tokio::test]
async fn test_create_with_invalid_cron_fails() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_badcron (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_badcron VALUES (1)").await;

    // Invalid cron: only 3 fields
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('st_badcron', \
             $$ SELECT id FROM src_badcron $$, '* * *', 'FULL')",
        )
        .await;
    assert!(
        result.is_err(),
        "Invalid cron expression should be rejected"
    );
}

#[tokio::test]
async fn test_create_with_invalid_duration_fails() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_baddur (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO src_baddur VALUES (1)").await;

    // Invalid duration: unknown unit
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('st_baddur', \
             $$ SELECT id FROM src_baddur $$, '5x', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "Invalid duration unit should be rejected");
}

// ── CROSS JOIN tests ────────────────────────────────────────────────────

#[tokio::test]
async fn test_cross_join_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cj_a (id INT PRIMARY KEY, x TEXT)")
        .await;
    db.execute("CREATE TABLE cj_b (id INT PRIMARY KEY, y TEXT)")
        .await;
    db.execute("INSERT INTO cj_a VALUES (1, 'a1'), (2, 'a2')")
        .await;
    db.execute("INSERT INTO cj_b VALUES (1, 'b1'), (2, 'b2'), (3, 'b3')")
        .await;

    db.create_st(
        "cj_full",
        "SELECT cj_a.x, cj_b.y FROM cj_a CROSS JOIN cj_b",
        "1m",
        "FULL",
    )
    .await;

    db.assert_st_matches_query(
        "public.cj_full",
        "SELECT cj_a.x, cj_b.y FROM cj_a CROSS JOIN cj_b",
    )
    .await;
}

#[tokio::test]
async fn test_cross_join_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cjd_a (id INT PRIMARY KEY, x TEXT)")
        .await;
    db.execute("CREATE TABLE cjd_b (id INT PRIMARY KEY, y TEXT)")
        .await;
    db.execute("INSERT INTO cjd_a VALUES (1, 'a1')").await;
    db.execute("INSERT INTO cjd_b VALUES (1, 'b1'), (2, 'b2')")
        .await;

    db.create_st(
        "cjd_st",
        "SELECT cjd_a.x, cjd_b.y FROM cjd_a CROSS JOIN cjd_b",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial: 1 × 2 = 2
    db.assert_st_matches_query(
        "public.cjd_st",
        "SELECT cjd_a.x, cjd_b.y FROM cjd_a CROSS JOIN cjd_b",
    )
    .await;

    // Insert another row in cjd_a → should add 2 more rows (1 × 2)
    db.execute("INSERT INTO cjd_a VALUES (2, 'a2')").await;
    db.refresh_st("cjd_st").await;
    db.assert_st_matches_query(
        "public.cjd_st",
        "SELECT cjd_a.x, cjd_b.y FROM cjd_a CROSS JOIN cjd_b",
    )
    .await;
}

#[tokio::test]
async fn test_nested_cross_join() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ncj_a (id INT PRIMARY KEY, x TEXT)")
        .await;
    db.execute("CREATE TABLE ncj_b (id INT PRIMARY KEY, y TEXT)")
        .await;
    db.execute("CREATE TABLE ncj_c (id INT PRIMARY KEY, z TEXT)")
        .await;
    db.execute("INSERT INTO ncj_a VALUES (1, 'a')").await;
    db.execute("INSERT INTO ncj_b VALUES (1, 'b'), (2, 'B')")
        .await;
    db.execute("INSERT INTO ncj_c VALUES (1, 'c'), (2, 'C'), (3, 'c3')")
        .await;

    db.create_st(
        "ncj_st",
        "SELECT ncj_a.x, ncj_b.y, ncj_c.z FROM ncj_a CROSS JOIN ncj_b CROSS JOIN ncj_c",
        "1m",
        "FULL",
    )
    .await;

    // 1 × 2 × 3 = 6 rows
    db.assert_st_matches_query(
        "public.ncj_st",
        "SELECT ncj_a.x, ncj_b.y, ncj_c.z FROM ncj_a CROSS JOIN ncj_b CROSS JOIN ncj_c",
    )
    .await;
}

#[tokio::test]
async fn test_cross_join_with_where_clause() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cjw_a (id INT PRIMARY KEY, x INT)")
        .await;
    db.execute("CREATE TABLE cjw_b (id INT PRIMARY KEY, y INT)")
        .await;
    db.execute("INSERT INTO cjw_a VALUES (1, 10), (2, 20)")
        .await;
    db.execute("INSERT INTO cjw_b VALUES (1, 15), (2, 25)")
        .await;

    db.create_st(
        "cjw_st",
        "SELECT cjw_a.x, cjw_b.y FROM cjw_a CROSS JOIN cjw_b WHERE cjw_a.x < cjw_b.y",
        "1m",
        "FULL",
    )
    .await;

    // Cartesian product: (10,15),(10,25),(20,25) — only 3 rows where x < y
    db.assert_st_matches_query(
        "public.cjw_st",
        "SELECT cjw_a.x, cjw_b.y FROM cjw_a CROSS JOIN cjw_b WHERE cjw_a.x < cjw_b.y",
    )
    .await;
}

// ── AUTO Mode Tests ────────────────────────────────────────────────────

/// AUTO + differentiable query: mode stored as DIFFERENTIAL, table populated.
#[tokio::test]
async fn test_create_auto_mode_differentiable() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE auto_diff_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO auto_diff_src VALUES (1, 10), (2, 20), (3, 5)")
        .await;

    db.create_st(
        "auto_diff_st",
        "SELECT id, val FROM auto_diff_src WHERE val > 5",
        "1m",
        "AUTO",
    )
    .await;

    let (status, mode, populated, errors) = db.pgt_status("auto_diff_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(
        mode, "DIFFERENTIAL",
        "AUTO with differentiable query should resolve to DIFFERENTIAL"
    );
    assert!(populated, "ST should be populated after creation");
    assert_eq!(errors, 0);

    // Only rows with val > 5 should be materialized
    assert_eq!(db.count("public.auto_diff_st").await, 2);
}

/// AUTO + non-differentiable source (materialized view): mode stored as FULL.
///
/// Materialized views cannot be CDC tracked in DIFFERENTIAL mode.
/// AUTO should silently downgrade to FULL and still create the ST.
#[tokio::test]
async fn test_create_auto_mode_not_differentiable() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE auto_nd_base (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO auto_nd_base VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    // A materialized view cannot be a CDC source for DIFFERENTIAL mode.
    db.execute("CREATE MATERIALIZED VIEW auto_nd_mv AS SELECT id, val FROM auto_nd_base")
        .await;

    // AUTO mode should downgrade to FULL and succeed.
    db.create_st("auto_nd_st", "SELECT id, val FROM auto_nd_mv", "1m", "AUTO")
        .await;

    let (status, mode, populated, errors) = db.pgt_status("auto_nd_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(
        mode, "FULL",
        "AUTO with matview source should downgrade to FULL"
    );
    assert!(
        populated,
        "ST should be populated after auto-downgrade to FULL"
    );
    assert_eq!(errors, 0);
    assert_eq!(db.count("public.auto_nd_st").await, 3);
}

/// Explicit DIFFERENTIAL + non-differentiable source → error.
///
/// When the user explicitly requests DIFFERENTIAL, no silent downgrade
/// occurs — the creation is rejected with an informative error.
#[tokio::test]
async fn test_create_explicit_differential_not_differentiable() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE expldf_base (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO expldf_base VALUES (1, 42)").await;

    db.execute("CREATE MATERIALIZED VIEW expldf_mv AS SELECT id, val FROM expldf_base")
        .await;

    // Explicit DIFFERENTIAL with a matview source must error — no silent downgrade.
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('expldf_st', \
             $$ SELECT id, val FROM expldf_mv $$, '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        result.is_err(),
        "Explicit DIFFERENTIAL with matview source should be rejected, not silently downgraded"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.to_lowercase().contains("materialized view")
            || err.to_lowercase().contains("differential"),
        "Error should mention materialized view or DIFFERENTIAL mode, got: {err}"
    );
}

/// Omit refresh_mode entirely → defaults to AUTO behavior.
///
/// When called with only name + query + schedule, the default 'AUTO'
/// takes effect: a differentiable query is stored as DIFFERENTIAL.
#[tokio::test]
async fn test_create_no_mode_specified() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE no_mode_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO no_mode_src VALUES (1, 10), (2, 20)")
        .await;

    // Call with only 3 positional args — refresh_mode defaults to 'AUTO'.
    db.execute(
        "SELECT pgtrickle.create_stream_table('no_mode_st', \
         $$ SELECT id, val FROM no_mode_src $$, '1m')",
    )
    .await;

    let (status, mode, populated, errors) = db.pgt_status("no_mode_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(
        mode, "DIFFERENTIAL",
        "Omitting refresh_mode should default to AUTO, resolving to DIFFERENTIAL"
    );
    assert!(populated);
    assert_eq!(errors, 0);
    assert_eq!(db.count("public.no_mode_st").await, 2);
}

/// Explicit 'DIFFERENTIAL' still works identically to before the AUTO change.
#[tokio::test]
async fn test_backward_compat_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE bkcomp_diff_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO bkcomp_diff_src VALUES (1, 100)")
        .await;

    db.create_st(
        "bkcomp_diff_st",
        "SELECT id, val FROM bkcomp_diff_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let (status, mode, populated, errors) = db.pgt_status("bkcomp_diff_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "DIFFERENTIAL");
    assert!(populated);
    assert_eq!(errors, 0);

    // Verify differential refresh applies only the delta.
    db.execute("INSERT INTO bkcomp_diff_src VALUES (2, 200)")
        .await;
    db.refresh_st("bkcomp_diff_st").await;
    assert_eq!(db.count("public.bkcomp_diff_st").await, 2);
}

/// Explicit 'FULL' still works identically to before the AUTO change.
#[tokio::test]
async fn test_backward_compat_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE bkcomp_full_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO bkcomp_full_src VALUES (1, 'hello')")
        .await;

    db.create_st(
        "bkcomp_full_st",
        "SELECT id, val FROM bkcomp_full_src",
        "1m",
        "FULL",
    )
    .await;

    let (status, mode, populated, errors) = db.pgt_status("bkcomp_full_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);
    assert_eq!(errors, 0);

    // Verify full refresh works correctly.
    db.execute("INSERT INTO bkcomp_full_src VALUES (2, 'world')")
        .await;
    db.refresh_st("bkcomp_full_st").await;
    assert_eq!(db.count("public.bkcomp_full_st").await, 2);
}
