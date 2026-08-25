//! End-to-end scenario tests for stream table lifecycle operations.
//!
//! These tests exercise full workflows against a live PostgreSQL 18
//! using Testcontainers:
//! - Create ST → full refresh → verify contents match defining query
//! - Insert/Update/Delete on source → re-refresh → verify correctness
//! - Multi-table joins → refresh → verify correctness
//! - Aggregate STs → refresh → verify SUM/COUNT correctness
//! - DAG cycle detection via catalog constraints
//! - Refresh history tracking

mod common;

use common::TestDb;

// ── Scenario 1: Simple ST creation and full refresh ────────────────────────

#[tokio::test]
async fn test_scenario_create_and_full_refresh() {
    let db = TestDb::with_catalog().await;

    // Set up source
    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price NUMERIC)")
        .await;
    db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99), (3, 'Gizmo', 29.99)")
        .await;

    // Create ST storage table matching the defining query shape
    db.execute("CREATE TABLE public.product_summary (__pgt_row_id BIGINT, id INT, name TEXT, price NUMERIC)")
        .await;

    let storage_oid: i32 = db
        .query_scalar("SELECT 'product_summary'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode) \
         VALUES ({storage_oid}, 'product_summary', 'public', 'SELECT id, name, price FROM products', 'public', '1m', 'FULL')"
    )).await;

    // Simulate full refresh: TRUNCATE + INSERT INTO ... SELECT
    db.execute("TRUNCATE product_summary").await;
    db.execute(
        "INSERT INTO product_summary (__pgt_row_id, id, name, price) \
                SELECT 0, id, name, price FROM products",
    )
    .await;

    // Update catalog to mark populated
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables SET is_populated = true, status = 'ACTIVE', \
         data_timestamp = now() WHERE pgt_relid = {storage_oid}"
    ))
    .await;

    // THE KEY INVARIANT: ST contents == defining query result
    let matches: bool = db.query_scalar(
        "SELECT NOT EXISTS (
            (SELECT id, name, price FROM product_summary EXCEPT SELECT id, name, price FROM products)
            UNION ALL
            (SELECT id, name, price FROM products EXCEPT SELECT id, name, price FROM product_summary)
        )"
    ).await;
    assert!(matches, "ST contents must match defining query result");

    // Verify row count
    assert_eq!(db.count("product_summary").await, 3);
}

// ── Scenario 2: ST refresh after INSERT ────────────────────────────────────

#[tokio::test]
async fn test_scenario_refresh_after_insert() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO orders VALUES (1, 100), (2, 200)")
        .await;

    db.execute("CREATE TABLE public.order_mirror (__pgt_row_id BIGINT, id INT, amount NUMERIC)")
        .await;
    let oid: i32 = db
        .query_scalar("SELECT 'order_mirror'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode) \
         VALUES ({oid}, 'order_mirror', 'public', 'SELECT id, amount FROM orders', 'public', '1m', 'FULL')"
    )).await;

    // Initial full refresh
    db.execute(
        "INSERT INTO order_mirror (__pgt_row_id, id, amount) SELECT 0, id, amount FROM orders",
    )
    .await;
    assert_eq!(db.count("order_mirror").await, 2);

    // Now INSERT new rows into source
    db.execute("INSERT INTO orders VALUES (3, 300), (4, 400)")
        .await;

    // Re-refresh (full)
    db.execute("TRUNCATE order_mirror").await;
    db.execute(
        "INSERT INTO order_mirror (__pgt_row_id, id, amount) SELECT 0, id, amount FROM orders",
    )
    .await;

    assert_eq!(db.count("order_mirror").await, 4);

    // Verify correctness
    let matches: bool = db
        .query_scalar(
            "SELECT NOT EXISTS (
            (SELECT id, amount FROM order_mirror EXCEPT SELECT id, amount FROM orders)
            UNION ALL
            (SELECT id, amount FROM orders EXCEPT SELECT id, amount FROM order_mirror)
        )",
        )
        .await;
    assert!(matches, "ST must reflect all rows after INSERT + refresh");
}

// ── Scenario 3: ST refresh after UPDATE ────────────────────────────────────

#[tokio::test]
async fn test_scenario_refresh_after_update() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE items (id INT PRIMARY KEY, qty INT)")
        .await;
    db.execute("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    db.execute("CREATE TABLE public.item_st (__pgt_row_id BIGINT, id INT, qty INT)")
        .await;
    let oid: i32 = db
        .query_scalar("SELECT 'item_st'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode) \
         VALUES ({oid}, 'item_st', 'public', 'SELECT id, qty FROM items', 'public', '1m', 'FULL')"
    )).await;

    // Initial full refresh
    db.execute("TRUNCATE item_st").await;
    db.execute("INSERT INTO item_st (__pgt_row_id, id, qty) SELECT 0, id, qty FROM items")
        .await;

    // Update source
    db.execute("UPDATE items SET qty = 999 WHERE id = 2").await;

    // Re-refresh
    db.execute("TRUNCATE item_st").await;
    db.execute("INSERT INTO item_st (__pgt_row_id, id, qty) SELECT 0, id, qty FROM items")
        .await;

    // Verify updated value is reflected
    let qty: i32 = db
        .query_scalar("SELECT qty FROM item_st WHERE id = 2")
        .await;
    assert_eq!(qty, 999);

    // Verify full correctness
    let matches: bool = db
        .query_scalar(
            "SELECT NOT EXISTS (
            (SELECT id, qty FROM item_st EXCEPT SELECT id, qty FROM items)
            UNION ALL
            (SELECT id, qty FROM items EXCEPT SELECT id, qty FROM item_st)
        )",
        )
        .await;
    assert!(matches);
}

// ── Scenario 4: ST refresh after DELETE ────────────────────────────────────

#[tokio::test]
async fn test_scenario_refresh_after_delete() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE records (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO records VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
        .await;

    db.execute("CREATE TABLE public.records_st (__pgt_row_id BIGINT, id INT, val TEXT)")
        .await;
    let oid: i32 = db
        .query_scalar("SELECT 'records_st'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode) \
         VALUES ({oid}, 'records_st', 'public', 'SELECT id, val FROM records', 'public', '1m', 'FULL')"
    )).await;

    // Initial refresh
    db.execute("INSERT INTO records_st (__pgt_row_id, id, val) SELECT 0, id, val FROM records")
        .await;
    assert_eq!(db.count("records_st").await, 4);

    // Delete from source
    db.execute("DELETE FROM records WHERE id IN (2, 4)").await;

    // Re-refresh
    db.execute("TRUNCATE records_st").await;
    db.execute("INSERT INTO records_st (__pgt_row_id, id, val) SELECT 0, id, val FROM records")
        .await;
    assert_eq!(db.count("records_st").await, 2);

    let matches: bool = db
        .query_scalar(
            "SELECT NOT EXISTS (
            (SELECT id, val FROM records_st EXCEPT SELECT id, val FROM records)
            UNION ALL
            (SELECT id, val FROM records EXCEPT SELECT id, val FROM records_st)
        )",
        )
        .await;
    assert!(matches);
}

// ── Scenario 5: ST with filter (WHERE clause) ─────────────────────────────

#[tokio::test]
async fn test_scenario_filtered_st() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO sales VALUES (1, 'US', 100), (2, 'EU', 200), (3, 'US', 300), (4, 'APAC', 50)",
    )
    .await;

    db.execute("CREATE TABLE public.us_sales_st (__pgt_row_id BIGINT, id INT, region TEXT, amount NUMERIC)")
        .await;
    let oid: i32 = db
        .query_scalar("SELECT 'us_sales_st'::regclass::oid::int")
        .await;

    let defining_query = "SELECT id, region, amount FROM sales WHERE region = ''US''";

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode) \
         VALUES ({oid}, 'us_sales_st', 'public', $${defining_query}$$, 'public', '1m', 'FULL')"
    )).await;

    // For actual SQL execution, use the proper single-quote version
    let exec_query = "SELECT id, region, amount FROM sales WHERE region = 'US'";

    // Full refresh with filter
    db.execute(&format!(
        "INSERT INTO us_sales_st (__pgt_row_id, id, region, amount) SELECT 0, id, region, amount FROM ({exec_query}) sub"
    )).await;

    // Only US rows
    assert_eq!(db.count("us_sales_st").await, 2);

    // Verify correctness against defining query
    let matches: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS (
            (SELECT id, region, amount FROM us_sales_st EXCEPT {exec_query})
            UNION ALL
            ({exec_query} EXCEPT SELECT id, region, amount FROM us_sales_st)
        )"
        ))
        .await;
    assert!(matches);
}

// ── Scenario 6: ST with JOIN ───────────────────────────────────────────────

#[tokio::test]
async fn test_scenario_join_st() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .await;

    db.execute("CREATE TABLE purchases (id INT PRIMARY KEY, cust_id INT, item TEXT)")
        .await;
    db.execute(
        "INSERT INTO purchases VALUES (10, 1, 'Widget'), (20, 2, 'Gadget'), (30, 1, 'Gizmo')",
    )
    .await;

    let defining_query =
        "SELECT c.name, p.item FROM customers c JOIN purchases p ON c.id = p.cust_id";

    db.execute("CREATE TABLE public.cust_purchases_st (__pgt_row_id BIGINT, name TEXT, item TEXT)")
        .await;
    let oid: i32 = db
        .query_scalar("SELECT 'cust_purchases_st'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode) \
         VALUES ({oid}, 'cust_purchases_st', 'public', $${defining_query}$$, 'public', '1m', 'FULL')"
    )).await;

    // Full refresh
    db.execute(&format!(
        "INSERT INTO cust_purchases_st (__pgt_row_id, name, item) \
         SELECT 0, sub.name, sub.item FROM ({defining_query}) sub"
    ))
    .await;

    assert_eq!(db.count("cust_purchases_st").await, 3);

    let matches: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS (
            (SELECT name, item FROM cust_purchases_st EXCEPT {defining_query})
            UNION ALL
            ({defining_query} EXCEPT SELECT name, item FROM cust_purchases_st)
        )"
        ))
        .await;
    assert!(matches);
}

// ── Scenario 7: ST with aggregate ─────────────────────────────────────────

#[tokio::test]
async fn test_scenario_aggregate_st() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE inventory (id INT PRIMARY KEY, category TEXT, qty INT)")
        .await;
    db.execute("INSERT INTO inventory VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40), (5, 'C', 5)")
        .await;

    let defining_query = "SELECT category, SUM(qty) AS total_qty, COUNT(*) AS item_count FROM inventory GROUP BY category";

    db.execute("CREATE TABLE public.inv_summary_st (__pgt_row_id BIGINT, category TEXT, total_qty BIGINT, item_count BIGINT)")
        .await;
    let oid: i32 = db
        .query_scalar("SELECT 'inv_summary_st'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode) \
         VALUES ({oid}, 'inv_summary_st', 'public', $${defining_query}$$, 'public', '1m', 'FULL')"
    )).await;

    // Full refresh
    db.execute(&format!(
        "INSERT INTO inv_summary_st (__pgt_row_id, category, total_qty, item_count) \
         SELECT 0, sub.category, sub.total_qty, sub.item_count FROM ({defining_query}) sub"
    ))
    .await;

    assert_eq!(db.count("inv_summary_st").await, 3);

    // Verify specific aggregates
    let a_total: i64 = db
        .query_scalar("SELECT total_qty FROM inv_summary_st WHERE category = 'A'")
        .await;
    assert_eq!(a_total, 40); // 10 + 30

    let b_count: i64 = db
        .query_scalar("SELECT item_count FROM inv_summary_st WHERE category = 'B'")
        .await;
    assert_eq!(b_count, 2);

    // Verify full correctness
    let matches: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS (
            (SELECT category, total_qty, item_count FROM inv_summary_st EXCEPT {defining_query})
            UNION ALL
            ({defining_query} EXCEPT SELECT category, total_qty, item_count FROM inv_summary_st)
        )"
        ))
        .await;
    assert!(matches);
}

// ── Scenario 8: Refresh history tracking ───────────────────────────────────

#[tokio::test]
async fn test_scenario_refresh_history() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src VALUES (1, 10)").await;

    db.execute("CREATE TABLE public.src_st (__pgt_row_id BIGINT, id INT, val INT)")
        .await;
    let oid: i32 = db.query_scalar("SELECT 'src_st'::regclass::oid::int").await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode) \
         VALUES ({oid}, 'src_st', 'public', 'SELECT id, val FROM src', 'public', '1m', 'FULL')"
    )).await;

    // Record first refresh in history
    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, rows_inserted, status) \
         VALUES (1, now(), now() - interval '1 second', now(), 'FULL', 1, 'COMPLETED')"
    ).await;

    // Record second refresh
    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, rows_inserted, rows_deleted, status) \
         VALUES (1, now(), now() - interval '1 second', now(), 'FULL', 2, 1, 'COMPLETED')"
    ).await;

    let history_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = 1")
        .await;
    assert_eq!(history_count, 2);

    // Verify latest refresh
    let latest_action: String = db.query_scalar(
        "SELECT action FROM pgtrickle.pgt_refresh_history WHERE pgt_id = 1 ORDER BY refresh_id DESC LIMIT 1"
    ).await;
    assert_eq!(latest_action, "FULL");
}

// ── Scenario 9: Stream tables info view ───────────────────────────────────

#[tokio::test]
async fn test_scenario_st_info_view() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE base (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE public.st_test (__pgt_row_id BIGINT, id INT)")
        .await;
    let oid: i32 = db
        .query_scalar("SELECT 'st_test'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables \
         (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode, status, data_timestamp, last_refresh_at) \
         VALUES ({oid}, 'st_test', 'public', 'SELECT id FROM base', 'public', '1m', 'FULL', 'ACTIVE', now() - interval '2 minutes', now() - interval '2 minutes')"
    )).await;

    // Check info view shows stale flag
    let exceeded: bool = db
        .query_scalar("SELECT stale FROM pgtrickle.stream_tables_info WHERE pgt_name = 'st_test'")
        .await;
    assert!(exceeded, "Lag should be exceeded (2 min > 1 min target)");
}

// ── Scenario 10: NO_DATA refresh (metadata only) ──────────────────────────

#[tokio::test]
async fn test_scenario_no_data_refresh() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE src10 (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src10 VALUES (1, 'hello')").await;

    db.execute("CREATE TABLE public.st10 (__pgt_row_id BIGINT, id INT, val TEXT)")
        .await;
    let oid: i32 = db.query_scalar("SELECT 'st10'::regclass::oid::int").await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, schedule, refresh_mode) \
         VALUES ({oid}, 'st10', 'public', 'SELECT id, val FROM src10', 'public', '1m', 'FULL')"
    )).await;

    // Record a NO_DATA refresh
    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, rows_inserted, rows_deleted, status) \
         VALUES (1, now(), now(), now(), 'NO_DATA', 0, 0, 'COMPLETED')"
    ).await;

    // Table should remain empty (no data was loaded)
    assert_eq!(db.count("st10").await, 0);

    // But history shows it
    let action: String = db
        .query_scalar("SELECT action FROM pgtrickle.pgt_refresh_history WHERE pgt_id = 1 LIMIT 1")
        .await;
    assert_eq!(action, "NO_DATA");
}
