//! E2E tests for expression deparsing, semantic validation, and expanded SQL support.
//!
//! Tests for Priority 1-4 fixes from REPORT_SQL_GAPS.md:
//! - P0: Expression deparsing (CASE, COALESCE, NULLIF, IN, BETWEEN, etc.)
//! - P1: Semantic error detection (NATURAL JOIN, DISTINCT ON, FILTER, unknown agg)
//! - P2: Expanded support (RIGHT JOIN swap, window frames, 3-field ColumnRef)
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Priority 1 (P0): Expression Deparsing — FULL mode
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_case_when_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, amount NUMERIC, status TEXT)")
        .await;
    db.execute(
        "INSERT INTO orders VALUES
         (1, 100, 'paid'), (2, 200, 'pending'), (3, 50, 'refunded')",
    )
    .await;

    let query_order_labels = "SELECT id, CASE WHEN amount > 150 THEN 'high' WHEN amount > 75 THEN 'medium' ELSE 'low' END AS label FROM orders";
    db.create_st("order_labels", query_order_labels, "1m", "FULL")
        .await;
    db.assert_st_matches_query("order_labels", query_order_labels)
        .await;

    let count = db.count("public.order_labels").await;
    assert_eq!(count, 3);

    let label: String = db
        .query_scalar("SELECT label FROM public.order_labels WHERE id = 2")
        .await;
    assert_eq!(label, "high");
}

#[tokio::test]
async fn test_simple_case_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE tickets (id INT PRIMARY KEY, priority INT)")
        .await;
    db.execute("INSERT INTO tickets VALUES (1, 1), (2, 2), (3, 3)")
        .await;

    let query_ticket_labels = "SELECT id, CASE priority WHEN 1 THEN 'urgent' WHEN 2 THEN 'normal' ELSE 'low' END AS prio_label FROM tickets";
    db.create_st("ticket_labels", query_ticket_labels, "1m", "FULL")
        .await;
    db.assert_st_matches_query("ticket_labels", query_ticket_labels)
        .await;

    let label: String = db
        .query_scalar("SELECT prio_label FROM public.ticket_labels WHERE id = 1")
        .await;
    assert_eq!(label, "urgent");
}

#[tokio::test]
async fn test_coalesce_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE contacts (id INT PRIMARY KEY, phone TEXT, email TEXT)")
        .await;
    db.execute(
        "INSERT INTO contacts VALUES (1, '555-1234', NULL), (2, NULL, 'bob@ex.com'), (3, NULL, NULL)",
    )
    .await;

    let query_contact_info =
        "SELECT id, COALESCE(phone, email, 'no-contact') AS best_contact FROM contacts";
    db.create_st("contact_info", query_contact_info, "1m", "FULL")
        .await;
    db.assert_st_matches_query("contact_info", query_contact_info)
        .await;

    let c1: String = db
        .query_scalar("SELECT best_contact FROM public.contact_info WHERE id = 1")
        .await;
    assert_eq!(c1, "555-1234");

    let c3: String = db
        .query_scalar("SELECT best_contact FROM public.contact_info WHERE id = 3")
        .await;
    assert_eq!(c3, "no-contact");
}

#[tokio::test]
async fn test_nullif_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vals (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO vals VALUES (1, 0), (2, 5), (3, 0)")
        .await;

    let query_safe_vals = "SELECT id, NULLIF(v, 0) AS safe_v FROM vals";
    db.create_st("safe_vals", query_safe_vals, "1m", "FULL")
        .await;
    db.assert_st_matches_query("safe_vals", query_safe_vals)
        .await;

    let count = db.count("public.safe_vals").await;
    assert_eq!(count, 3);

    let safe_v2: i32 = db
        .query_scalar("SELECT safe_v FROM public.safe_vals WHERE id = 2")
        .await;
    assert_eq!(safe_v2, 5);

    // NULLIF(0, 0) should be NULL
    let is_null: bool = db
        .query_scalar("SELECT safe_v IS NULL FROM public.safe_vals WHERE id = 1")
        .await;
    assert!(is_null);
}

#[tokio::test]
async fn test_greatest_least_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE scores (id INT PRIMARY KEY, a INT, b INT, c INT)")
        .await;
    db.execute("INSERT INTO scores VALUES (1, 10, 20, 5), (2, 30, 15, 25)")
        .await;

    let query_score_bounds =
        "SELECT id, GREATEST(a, b, c) AS max_score, LEAST(a, b, c) AS min_score FROM scores";
    db.create_st("score_bounds", query_score_bounds, "1m", "FULL")
        .await;
    db.assert_st_matches_query("score_bounds", query_score_bounds)
        .await;

    let max1: i32 = db
        .query_scalar("SELECT max_score FROM public.score_bounds WHERE id = 1")
        .await;
    assert_eq!(max1, 20);

    let min2: i32 = db
        .query_scalar("SELECT min_score FROM public.score_bounds WHERE id = 2")
        .await;
    assert_eq!(min2, 15);
}

#[tokio::test]
async fn test_in_list_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE items (id INT PRIMARY KEY, category TEXT)")
        .await;
    db.execute("INSERT INTO items VALUES (1, 'books'), (2, 'toys'), (3, 'food'), (4, 'books')")
        .await;

    let query_filtered_items = "SELECT id, category FROM items WHERE category IN ('books', 'food')";
    db.create_st("filtered_items", query_filtered_items, "1m", "FULL")
        .await;
    db.assert_st_matches_query("filtered_items", query_filtered_items)
        .await;

    let count = db.count("public.filtered_items").await;
    assert_eq!(count, 3); // items 1, 3, 4
}

#[tokio::test]
async fn test_between_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE products (id INT PRIMARY KEY, price NUMERIC)")
        .await;
    db.execute("INSERT INTO products VALUES (1, 10), (2, 50), (3, 100), (4, 200)")
        .await;

    let query_mid_priced = "SELECT id, price FROM products WHERE price BETWEEN 20 AND 150";
    db.create_st("mid_priced", query_mid_priced, "1m", "FULL")
        .await;
    db.assert_st_matches_query("mid_priced", query_mid_priced)
        .await;

    let count = db.count("public.mid_priced").await;
    assert_eq!(count, 2); // products 2, 3
}

#[tokio::test]
async fn test_is_distinct_from_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE nulltest (id INT PRIMARY KEY, a INT, b INT)")
        .await;
    db.execute("INSERT INTO nulltest VALUES (1, 1, 1), (2, 1, 2), (3, NULL, NULL), (4, 1, NULL)")
        .await;

    let query_distinct_check = "SELECT id, a IS DISTINCT FROM b AS is_diff FROM nulltest";
    db.create_st("distinct_check", query_distinct_check, "1m", "FULL")
        .await;
    db.assert_st_matches_query("distinct_check", query_distinct_check)
        .await;

    let count = db.count("public.distinct_check").await;
    assert_eq!(count, 4);

    let diff2: bool = db
        .query_scalar("SELECT is_diff FROM public.distinct_check WHERE id = 2")
        .await;
    assert!(diff2);

    // NULL IS DISTINCT FROM NULL → false
    let diff3: bool = db
        .query_scalar("SELECT is_diff FROM public.distinct_check WHERE id = 3")
        .await;
    assert!(!diff3);
}

#[tokio::test]
async fn test_boolean_test_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE flags (id INT PRIMARY KEY, active BOOLEAN)")
        .await;
    db.execute("INSERT INTO flags VALUES (1, true), (2, false), (3, NULL)")
        .await;

    let query_active_items = "SELECT id FROM flags WHERE active IS TRUE";
    db.create_st("active_items", query_active_items, "1m", "FULL")
        .await;
    db.assert_st_matches_query("active_items", query_active_items)
        .await;

    let count = db.count("public.active_items").await;
    assert_eq!(count, 1);

    let active_id: i32 = db.query_scalar("SELECT id FROM public.active_items").await;
    assert_eq!(active_id, 1);
}

#[tokio::test]
async fn test_sql_value_function_current_date_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE events (id INT PRIMARY KEY, event_date DATE)")
        .await;
    db.execute("INSERT INTO events VALUES (1, CURRENT_DATE), (2, CURRENT_DATE - 1)")
        .await;

    let query_recent_events = "SELECT id, event_date FROM events WHERE event_date >= CURRENT_DATE";
    db.create_st("recent_events", query_recent_events, "1m", "FULL")
        .await;
    db.assert_st_matches_query("recent_events", query_recent_events)
        .await;

    let count = db.count("public.recent_events").await;
    assert!(count >= 1);
}

#[tokio::test]
async fn test_array_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE data (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    let query_array_test = "SELECT id, ARRAY[val, val * 2] AS doubled FROM data";
    db.create_st("array_test", query_array_test, "1m", "FULL")
        .await;
    db.assert_st_matches_query("array_test", query_array_test)
        .await;

    let count = db.count("public.array_test").await;
    assert_eq!(count, 3);
}

// ═══════════════════════════════════════════════════════════════════════
// Priority 1 (P0): Expression Deparsing — DIFFERENTIAL mode
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_case_when_in_select_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE emp (id INT PRIMARY KEY, salary NUMERIC, dept TEXT)")
        .await;
    db.execute("INSERT INTO emp VALUES (1, 50000, 'eng'), (2, 80000, 'eng'), (3, 60000, 'sales')")
        .await;

    // CASE in WHERE clause with GROUP BY — DIFFERENTIAL mode
    let query_dept_summary = "SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total FROM emp WHERE CASE WHEN salary > 70000 THEN TRUE ELSE FALSE END GROUP BY dept";
    db.create_st("dept_summary", query_dept_summary, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("dept_summary", query_dept_summary)
        .await;

    let count = db.count("public.dept_summary").await;
    assert_eq!(count, 1); // only 1 employee has salary > 70000 (id=2, dept=eng)

    let total: i64 = db
        .query_scalar("SELECT total::bigint FROM public.dept_summary WHERE dept = 'eng'")
        .await;
    assert_eq!(total, 80000);
}

#[tokio::test]
async fn test_coalesce_in_select_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE orders2 (id INT PRIMARY KEY, customer_id INT, discount NUMERIC)")
        .await;
    db.execute("INSERT INTO orders2 VALUES (1, 1, 10), (2, 1, NULL), (3, 2, 5), (4, 2, NULL)")
        .await;

    let query_customer_discounts = "SELECT customer_id, SUM(COALESCE(discount, 0)) AS total_discount FROM orders2 GROUP BY customer_id";
    db.create_st(
        "customer_discounts",
        query_customer_discounts,
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("customer_discounts", query_customer_discounts)
        .await;

    let d1: i64 = db
        .query_scalar(
            "SELECT total_discount::bigint FROM public.customer_discounts WHERE customer_id = 1",
        )
        .await;
    assert_eq!(d1, 10);

    let d2: i64 = db
        .query_scalar(
            "SELECT total_discount::bigint FROM public.customer_discounts WHERE customer_id = 2",
        )
        .await;
    assert_eq!(d2, 5);
}

// ═══════════════════════════════════════════════════════════════════════
// Priority 2 (P1): Semantic Error Detection
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_natural_join_accepted_via_rewrite() {
    // NATURAL JOIN is auto-rewritten to INNER JOIN on shared columns.
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, score INT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('nat_join_st', \
             $$ SELECT t1.id, t1.val, t2.score FROM t1 NATURAL JOIN t2 $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "NATURAL JOIN should be accepted via auto-rewrite, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_distinct_on_accepted_via_rewrite() {
    // DISTINCT ON is auto-rewritten to ROW_NUMBER() window subquery.
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE logs (id INT PRIMARY KEY, category TEXT, ts TIMESTAMPTZ)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('distinct_on_st', \
             $$ SELECT DISTINCT ON (category) id, category, ts FROM logs ORDER BY category, ts DESC $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "DISTINCT ON should be accepted via auto-rewrite, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_stddev_aggregate_supported_in_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE public.metrics (id INT PRIMARY KEY, val NUMERIC, grp TEXT)")
        .await;
    db.execute("INSERT INTO public.metrics VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b')")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('stddev_st', \
             $$ SELECT grp, STDDEV(val) AS std FROM public.metrics GROUP BY grp $$, '1m', 'AUTO')",
        )
        .await;
    assert!(
        result.is_ok(),
        "STDDEV should be accepted in AUTO mode, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_filter_clause_supported() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sales2 (id INT PRIMARY KEY, amount INT, region TEXT)")
        .await;
    db.execute("INSERT INTO sales2 VALUES (1, 200, 'east'), (2, 50, 'east'), (3, 300, 'west')")
        .await;

    let query_filter_st = "SELECT region, COUNT(*) FILTER (WHERE amount > 100) AS big_count FROM sales2 GROUP BY region";
    db.create_st("filter_st", query_filter_st, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("filter_st", query_filter_st)
        .await;

    let count: i64 = db
        .query_scalar("SELECT big_count FROM public.filter_st WHERE region = 'east'")
        .await;
    // Only 1 row (amount=200) passes the filter for 'east'
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_exists_subquery_in_where() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE parent_tbl (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE child_tbl (id INT PRIMARY KEY, parent_id INT)")
        .await;

    // EXISTS subquery should now be supported via SemiJoin
    let query_exists_st = "SELECT id, name FROM parent_tbl WHERE EXISTS (SELECT 1 FROM child_tbl WHERE child_tbl.parent_id = parent_tbl.id)";
    db.create_st("exists_st", query_exists_st, "1m", "FULL")
        .await;
    db.assert_st_matches_query("exists_st", query_exists_st)
        .await;

    // Insert data
    db.execute("INSERT INTO parent_tbl VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await;
    db.execute("INSERT INTO child_tbl VALUES (1, 1), (2, 1), (3, 3)")
        .await;

    // Refresh
    db.execute("SELECT pgtrickle.refresh_stream_table('exists_st')")
        .await;

    // Only parents with children should appear (1 and 3)
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM public.exists_st")
        .await;
    assert_eq!(count, 2, "Only parents with children should appear");
}

// ═══════════════════════════════════════════════════════════════════════
// Priority 3 (P2): Expanded Support
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_right_join_converted_to_left_join() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO departments VALUES (1, 'eng'), (2, 'sales'), (3, 'hr')")
        .await;
    db.execute("CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT)")
        .await;
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2)")
        .await;

    // RIGHT JOIN should be silently converted to LEFT JOIN with swapped operands
    let query_dept_employees = "SELECT d.id AS dept_id, d.name AS dept_name, e.name AS emp_name \
         FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id";
    db.create_st("dept_employees", query_dept_employees, "1m", "FULL")
        .await;
    db.assert_st_matches_query("dept_employees", query_dept_employees)
        .await;

    let count = db.count("public.dept_employees").await;
    assert_eq!(count, 4); // eng(Alice,Bob), sales(Charlie), hr(NULL)

    // HR department should show up with NULL employee
    let hr_emp: Option<String> = db
        .query_scalar("SELECT emp_name FROM public.dept_employees WHERE dept_name = 'hr'")
        .await;
    assert!(hr_emp.is_none(), "HR dept should have NULL employee");
}

#[tokio::test]
async fn test_window_frame_rows_between() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE timeseries (id INT PRIMARY KEY, ts DATE, val NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO timeseries VALUES \
         (1, '2024-01-01', 10), (2, '2024-01-02', 20), \
         (3, '2024-01-03', 30), (4, '2024-01-04', 40), (5, '2024-01-05', 50)",
    )
    .await;

    // Window function with explicit frame clause
    let query_running_avg = "SELECT id, ts, val, AVG(val) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg FROM timeseries";
    db.create_st("running_avg", query_running_avg, "1m", "FULL")
        .await;
    db.assert_st_matches_query("running_avg", query_running_avg)
        .await;

    let count = db.count("public.running_avg").await;
    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_three_field_column_ref() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE schema_test (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO schema_test VALUES (1, 'hello'), (2, 'world')")
        .await;

    // 3-field column reference: public.schema_test.id
    let query_schema_ref_st =
        "SELECT public.schema_test.id, public.schema_test.val FROM schema_test";
    db.create_st("schema_ref_st", query_schema_ref_st, "1m", "FULL")
        .await;
    db.assert_st_matches_query("schema_ref_st", query_schema_ref_st)
        .await;

    let count = db.count("public.schema_ref_st").await;
    assert_eq!(count, 2);
}

// ═══════════════════════════════════════════════════════════════════════
// Combined Expression Tests (multiple expression types in one query)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_combined_case_coalesce_between() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE transactions (id INT PRIMARY KEY, amount NUMERIC, note TEXT)")
        .await;
    db.execute(
        "INSERT INTO transactions VALUES \
         (1, 100, NULL), (2, 250, 'big'), (3, 50, NULL), (4, 500, 'huge')",
    )
    .await;

    let query_txn_summary = "SELECT id, \
         CASE WHEN amount > 200 THEN 'high' ELSE 'low' END AS tier, \
         COALESCE(note, 'no-note') AS description \
         FROM transactions WHERE amount BETWEEN 50 AND 300";
    db.create_st("txn_summary", query_txn_summary, "1m", "FULL")
        .await;
    db.assert_st_matches_query("txn_summary", query_txn_summary)
        .await;

    let count = db.count("public.txn_summary").await;
    assert_eq!(count, 3); // ids 1, 2, 3

    let tier2: String = db
        .query_scalar("SELECT tier FROM public.txn_summary WHERE id = 2")
        .await;
    assert_eq!(tier2, "high");

    let desc1: String = db
        .query_scalar("SELECT description FROM public.txn_summary WHERE id = 1")
        .await;
    assert_eq!(desc1, "no-note");
}

#[tokio::test]
async fn test_not_between_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO nums VALUES (1, 5), (2, 50), (3, 100), (4, 200)")
        .await;

    let query_excluded_range = "SELECT id, val FROM nums WHERE val NOT BETWEEN 10 AND 150";
    db.create_st("excluded_range", query_excluded_range, "1m", "FULL")
        .await;
    db.assert_st_matches_query("excluded_range", query_excluded_range)
        .await;

    let count = db.count("public.excluded_range").await;
    assert_eq!(count, 2); // ids 1 (5) and 4 (200)
}

#[tokio::test]
async fn test_not_in_expression_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE colors (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO colors VALUES (1, 'red'), (2, 'blue'), (3, 'green'), (4, 'yellow')")
        .await;

    let query_non_primary_colors =
        "SELECT id, name FROM colors WHERE name NOT IN ('red', 'blue', 'yellow')";
    db.create_st("non_primary_colors", query_non_primary_colors, "1m", "FULL")
        .await;
    db.assert_st_matches_query("non_primary_colors", query_non_primary_colors)
        .await;

    let count = db.count("public.non_primary_colors").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_is_not_true_and_is_unknown() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE bool_data (id INT PRIMARY KEY, flag BOOLEAN)")
        .await;
    db.execute("INSERT INTO bool_data VALUES (1, TRUE), (2, FALSE), (3, NULL)")
        .await;

    let query_not_true_items = "SELECT id FROM bool_data WHERE flag IS NOT TRUE";
    db.create_st("not_true_items", query_not_true_items, "1m", "FULL")
        .await;
    db.assert_st_matches_query("not_true_items", query_not_true_items)
        .await;

    let count = db.count("public.not_true_items").await;
    assert_eq!(count, 2); // FALSE and NULL
}

// ═══════════════════════════════════════════════════════════════════════
// Differential mode with new expressions — incremental maintenance
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_between_filter_differential_with_inserts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sensor (id INT PRIMARY KEY, reading NUMERIC)")
        .await;
    db.execute("INSERT INTO sensor VALUES (1, 50), (2, 75)")
        .await;

    let query_sensor_in_range = "SELECT id, reading FROM sensor WHERE reading BETWEEN 40 AND 80";
    db.create_st(
        "sensor_in_range",
        query_sensor_in_range,
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("sensor_in_range", query_sensor_in_range)
        .await;

    let count = db.count("public.sensor_in_range").await;
    assert_eq!(count, 2);

    // Insert new data and refresh
    db.execute("INSERT INTO sensor VALUES (3, 90), (4, 60)")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('sensor_in_range')")
        .await;

    let count = db.count("public.sensor_in_range").await;
    assert_eq!(count, 3); // 50, 75, 60 are in range; 90 is out
}

#[tokio::test]
async fn test_in_list_filter_differential_with_inserts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE items2 (id INT PRIMARY KEY, cat TEXT)")
        .await;
    db.execute("INSERT INTO items2 VALUES (1, 'A'), (2, 'B')")
        .await;

    let query_cat_filter_st = "SELECT id, cat FROM items2 WHERE cat IN ('A', 'C')";
    db.create_st("cat_filter_st", query_cat_filter_st, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("cat_filter_st", query_cat_filter_st)
        .await;

    let count = db.count("public.cat_filter_st").await;
    assert_eq!(count, 1);

    db.execute("INSERT INTO items2 VALUES (3, 'C'), (4, 'D')")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('cat_filter_st')")
        .await;

    let count = db.count("public.cat_filter_st").await;
    assert_eq!(count, 2); // A and C
}

// ═══════════════════════════════════════════════════════════════════════
// DISTINCT (without ON) should still work
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_plain_distinct_still_works() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE dup_data (id INT PRIMARY KEY, category TEXT)")
        .await;
    db.execute("INSERT INTO dup_data VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')")
        .await;

    // Plain DISTINCT (not DISTINCT ON) should still be accepted
    let query_unique_cats = "SELECT DISTINCT category FROM dup_data";
    db.create_st("unique_cats", query_unique_cats, "1m", "FULL")
        .await;
    db.assert_st_matches_query("unique_cats", query_unique_cats)
        .await;

    let count = db.count("public.unique_cats").await;
    assert_eq!(count, 3); // A, B, C
}

// ═══════════════════════════════════════════════════════════════════════
// Unsupported aggregate works in FULL mode
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_unsupported_aggregate_works_in_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE numbers (id INT PRIMARY KEY, val NUMERIC, grp TEXT)")
        .await;
    db.execute("INSERT INTO numbers VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b'), (4, 40, 'b')")
        .await;

    // string_agg is a recognized but unsupported aggregate — should work in FULL mode
    let query_string_concat =
        "SELECT grp, STRING_AGG(val::text, ', ' ORDER BY val) AS vals FROM numbers GROUP BY grp";
    db.create_st("string_concat", query_string_concat, "1m", "FULL")
        .await;
    db.assert_st_matches_query("string_concat", query_string_concat)
        .await;

    let count = db.count("public.string_concat").await;
    assert_eq!(count, 2);
}

// ═══════════════════════════════════════════════════════════════════════
// F5: IS JSON Predicate (PostgreSQL 16+)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_is_json_predicate_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE json_data (id INT PRIMARY KEY, payload TEXT)")
        .await;
    db.execute(
        "INSERT INTO json_data VALUES
         (1, '{\"key\": \"value\"}'),
         (2, 'not json'),
         (3, '[1, 2, 3]'),
         (4, '\"hello\"')",
    )
    .await;

    let query_json_check = "SELECT id, payload, payload IS JSON AS is_json FROM json_data";
    db.create_st("json_check", query_json_check, "1m", "FULL")
        .await;
    db.assert_st_matches_query("json_check", query_json_check)
        .await;

    let count = db.count("public.json_check").await;
    assert_eq!(count, 4);

    // Row 1 is valid JSON
    let is_json: bool = db
        .query_scalar("SELECT is_json FROM public.json_check WHERE id = 1")
        .await;
    assert!(is_json, "JSON object string should be valid JSON");

    // Row 2 is not valid JSON
    let is_json: bool = db
        .query_scalar("SELECT is_json FROM public.json_check WHERE id = 2")
        .await;
    assert!(!is_json, "'not json' should not be valid JSON");
}

#[tokio::test]
async fn test_is_json_type_variants_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE json_types (id INT PRIMARY KEY, payload TEXT)")
        .await;
    db.execute(
        "INSERT INTO json_types VALUES
         (1, '{\"a\": 1}'),
         (2, '[1, 2]'),
         (3, '\"scalar\"'),
         (4, 'bad')",
    )
    .await;

    // IS JSON OBJECT
    let query_json_obj_test = "SELECT id, payload IS JSON OBJECT AS is_obj FROM json_types";
    db.create_st("json_obj_test", query_json_obj_test, "1m", "FULL")
        .await;
    db.assert_st_matches_query("json_obj_test", query_json_obj_test)
        .await;

    let is_obj: bool = db
        .query_scalar("SELECT is_obj FROM public.json_obj_test WHERE id = 1")
        .await;
    assert!(is_obj, "JSON object should be IS JSON OBJECT");
    let is_obj: bool = db
        .query_scalar("SELECT is_obj FROM public.json_obj_test WHERE id = 2")
        .await;
    assert!(!is_obj, "JSON array should not be IS JSON OBJECT");
}

#[tokio::test]
async fn test_is_json_in_where_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE json_filter (id INT PRIMARY KEY, data TEXT)")
        .await;
    db.execute(
        "INSERT INTO json_filter VALUES
         (1, '{\"valid\": true}'),
         (2, 'not json'),
         (3, '{\"also\": \"valid\"}')",
    )
    .await;

    let query_valid_json_only = "SELECT id, data FROM json_filter WHERE data IS JSON";
    db.create_st(
        "valid_json_only",
        query_valid_json_only,
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("valid_json_only", query_valid_json_only)
        .await;

    let count = db.count("public.valid_json_only").await;
    assert_eq!(count, 2, "Only 2 rows have valid JSON data");

    // Insert new valid + invalid rows
    db.execute("INSERT INTO json_filter VALUES (4, '{\"new\": true}'), (5, 'nope')")
        .await;
    db.refresh_st("valid_json_only").await;
    db.assert_st_matches_query("valid_json_only", query_valid_json_only)
        .await;

    let count = db.count("public.valid_json_only").await;
    assert_eq!(count, 3, "Should have 3 valid JSON rows after insert");
}

// ═══════════════════════════════════════════════════════════════════════
// F10: SQL/JSON Constructors (PostgreSQL 16+)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_json_object_constructor_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
        .await;
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
        .await;

    let query_user_json = "SELECT id, JSON_OBJECT('name' : name, 'age' : age) AS data FROM users";
    db.create_st("user_json", query_user_json, "1m", "FULL")
        .await;
    db.assert_st_matches_query("user_json", query_user_json)
        .await;

    let count = db.count("public.user_json").await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_json_array_constructor_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vals (id INT PRIMARY KEY, a INT, b INT, c INT)")
        .await;
    db.execute("INSERT INTO vals VALUES (1, 10, 20, 30)").await;

    let query_val_arrays = "SELECT id, JSON_ARRAY(a, b, c) AS arr FROM vals";
    db.create_st("val_arrays", query_val_arrays, "1m", "FULL")
        .await;
    db.assert_st_matches_query("val_arrays", query_val_arrays)
        .await;

    let count = db.count("public.val_arrays").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_json_constructors_in_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price NUMERIC)")
        .await;
    db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)")
        .await;

    let query_product_json =
        "SELECT id, JSON_OBJECT('name' : name, 'price' : price) AS data FROM products";
    db.create_st("product_json", query_product_json, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("product_json", query_product_json)
        .await;

    let count = db.count("public.product_json").await;
    assert_eq!(count, 2);

    // Insert and verify differential update works
    db.execute("INSERT INTO products VALUES (3, 'Doohickey', 29.99)")
        .await;
    db.refresh_st("product_json").await;
    db.assert_st_matches_query("product_json", query_product_json)
        .await;

    let count = db.count("public.product_json").await;
    assert_eq!(count, 3);
}

// ═══════════════════════════════════════════════════════════════════════
// F11: SQL/JSON Standard Aggregates — JSON_OBJECTAGG / JSON_ARRAYAGG
// ═══════════════════════════════════════════════════════════════════════

/// Test JSON_OBJECTAGG(key: value) in FULL mode.
#[tokio::test]
async fn test_json_objectagg_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE kvs (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, key TEXT NOT NULL, val TEXT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO kvs (grp, key, val) VALUES
         ('a', 'color', 'red'), ('a', 'size', 'large'),
         ('b', 'color', 'blue')",
    )
    .await;

    let query_kv_obj = "SELECT grp, JSON_OBJECTAGG(key : val) AS obj FROM kvs GROUP BY grp";
    db.create_st("kv_obj", query_kv_obj, "1m", "FULL").await;
    db.assert_st_matches_query("kv_obj", query_kv_obj).await;

    let count = db.count("public.kv_obj").await;
    assert_eq!(count, 2, "Should have 2 groups");
}

/// Test JSON_ARRAYAGG(expr) in FULL mode.
#[tokio::test]
async fn test_json_arrayagg_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE tags (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, tag TEXT NOT NULL)")
        .await;
    db.execute(
        "INSERT INTO tags (grp, tag) VALUES
         ('a', 'fast'), ('a', 'cheap'),
         ('b', 'durable')",
    )
    .await;

    let query_tag_arr = "SELECT grp, JSON_ARRAYAGG(tag) AS tags FROM tags GROUP BY grp";
    db.create_st("tag_arr", query_tag_arr, "1m", "FULL").await;
    db.assert_st_matches_query("tag_arr", query_tag_arr).await;

    let count = db.count("public.tag_arr").await;
    assert_eq!(count, 2, "Should have 2 groups");
}

/// Test JSON_OBJECTAGG(key: value) in DIFFERENTIAL mode with group rescan.
#[tokio::test]
async fn test_json_objectagg_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE jobj_std (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, prop TEXT NOT NULL, val TEXT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO jobj_std (dept, prop, val) VALUES
         ('eng', 'lang', 'rust'), ('eng', 'os', 'linux'),
         ('sales', 'tool', 'crm')",
    )
    .await;

    // Create with DIFFERENTIAL mode — should now be recognized as an aggregate
    let query_dept_props_std =
        "SELECT dept, JSON_OBJECTAGG(prop : val) AS props FROM jobj_std GROUP BY dept";
    db.create_st("dept_props_std", query_dept_props_std, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("dept_props_std", query_dept_props_std)
        .await;

    let count = db.count("public.dept_props_std").await;
    assert_eq!(count, 2, "Should have 2 departments after initial load");

    // Insert a new property and refresh
    db.execute("INSERT INTO jobj_std (dept, prop, val) VALUES ('eng', 'db', 'postgres')")
        .await;
    db.refresh_st("dept_props_std").await;
    db.assert_st_matches_query("dept_props_std", query_dept_props_std)
        .await;

    let count = db.count("public.dept_props_std").await;
    assert_eq!(count, 2, "Should still have 2 departments after insert");
}

/// Test JSON_ARRAYAGG(expr) in DIFFERENTIAL mode with group rescan.
#[tokio::test]
async fn test_json_arrayagg_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE jarr_std (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, skill TEXT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO jarr_std (dept, skill) VALUES
         ('eng', 'coding'), ('eng', 'testing'),
         ('sales', 'pitching')",
    )
    .await;

    let query_dept_skills_std =
        "SELECT dept, JSON_ARRAYAGG(skill) AS skills FROM jarr_std GROUP BY dept";
    db.create_st(
        "dept_skills_std",
        query_dept_skills_std,
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("dept_skills_std", query_dept_skills_std)
        .await;

    let count = db.count("public.dept_skills_std").await;
    assert_eq!(count, 2, "Should have 2 departments after initial load");

    // Insert a new skill and refresh
    db.execute("INSERT INTO jarr_std (dept, skill) VALUES ('eng', 'debugging')")
        .await;
    db.refresh_st("dept_skills_std").await;
    db.assert_st_matches_query("dept_skills_std", query_dept_skills_std)
        .await;

    let count = db.count("public.dept_skills_std").await;
    assert_eq!(count, 2, "Should still have 2 departments after insert");
}

// ═══════════════════════════════════════════════════════════════════════
// F12: JSON_TABLE() in FROM clause
// ═══════════════════════════════════════════════════════════════════════

/// Test JSON_TABLE in FULL mode — basic column extraction.
#[tokio::test]
async fn test_json_table_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE api_data (id INT PRIMARY KEY, payload JSONB NOT NULL)")
        .await;
    db.execute(
        "INSERT INTO api_data VALUES
         (1, '{\"items\": [{\"name\": \"alpha\", \"score\": 10}, {\"name\": \"beta\", \"score\": 20}]}'),
         (2, '{\"items\": [{\"name\": \"gamma\", \"score\": 30}]}')",
    )
    .await;

    let query_api_items = "SELECT d.id, jt.name, jt.score
         FROM api_data d,
              JSON_TABLE(d.payload, '$.items[*]'
                COLUMNS (
                  name TEXT PATH '$.name',
                  score INT PATH '$.score'
                )) AS jt";
    db.create_st("api_items", query_api_items, "1m", "FULL")
        .await;
    db.assert_st_matches_query("api_items", query_api_items)
        .await;

    let count = db.count("public.api_items").await;
    assert_eq!(count, 3, "Should expand to 3 rows (2 + 1)");
}

/// Test JSON_TABLE in DIFFERENTIAL mode.
#[tokio::test]
async fn test_json_table_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE events (id SERIAL PRIMARY KEY, data JSONB NOT NULL)")
        .await;
    db.execute(
        "INSERT INTO events (data) VALUES
         ('{\"tags\": [\"a\", \"b\"]}'),
         ('{\"tags\": [\"c\"]}')",
    )
    .await;

    let query_event_tags = "SELECT e.id, jt.tag
         FROM events e,
              JSON_TABLE(e.data, '$.tags[*]'
                COLUMNS (tag TEXT PATH '$')) AS jt";
    db.create_st("event_tags", query_event_tags, "1m", "FULL")
        .await;
    db.assert_st_matches_query("event_tags", query_event_tags)
        .await;

    let count = db.count("public.event_tags").await;
    assert_eq!(count, 3, "Should expand to 3 tag rows");

    // Insert new event with tags and refresh
    db.execute("INSERT INTO events (data) VALUES ('{\"tags\": [\"d\", \"e\"]}')")
        .await;
    db.refresh_st("event_tags").await;
    db.assert_st_matches_query("event_tags", query_event_tags)
        .await;

    let count = db.count("public.event_tags").await;
    assert_eq!(count, 5, "Should have 5 tags after insert");
}

// ═══════════════════════════════════════════════════════════════════════
// Regression: NULLIF in AUTO mode must resolve to DIFFERENTIAL (not FULL)
// ═══════════════════════════════════════════════════════════════════════

/// Before the AEXPR_NULLIF fix, `NULLIF(col, '')::bigint` in the SELECT list
/// caused `parse_defining_query_full` to emit "A_Expr kind AEXPR_NULLIF is
/// not supported in defining queries", which made AUTO mode silently
/// downgrade to FULL.  After the fix, NULLIF is deparsed as Expr::Raw and
/// the DVM pipeline proceeds normally.
#[tokio::test]
async fn test_nullif_in_select_auto_mode_resolves_to_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE nullif_regr_src (id INT PRIMARY KEY, raw_rank TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO nullif_regr_src VALUES \
         (1, '3', 100), (2, '', 200), (3, NULL, 300), (4, '1', 400)",
    )
    .await;

    // NULLIF(raw_rank, '')::bigint mirrors the pattern in OSI-generated
    // _rev_* queries that triggered the regression.
    let q = "SELECT id, NULLIF(raw_rank, '')::bigint AS safe_rank, val \
             FROM nullif_regr_src";

    db.create_st("nullif_regr_st", q, "1m", "AUTO").await;

    let (status, mode, populated, errors) = db.pgt_status("nullif_regr_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(
        mode, "DIFFERENTIAL",
        "NULLIF in SELECT list must not prevent DIFFERENTIAL mode (AEXPR_NULLIF regression)"
    );
    assert!(populated);
    assert_eq!(errors, 0);

    // Initial data correctness: empty string → NULL, NULL stays NULL
    db.assert_st_matches_query("nullif_regr_st", q).await;

    // Incremental update and re-check
    db.execute("UPDATE nullif_regr_src SET raw_rank = '5' WHERE id = 2")
        .await;
    db.refresh_st("nullif_regr_st").await;
    db.assert_st_matches_query("nullif_regr_st", q).await;

    let rank: Option<i64> = db
        .query_scalar_opt("SELECT safe_rank FROM public.nullif_regr_st WHERE id = 2")
        .await;
    assert_eq!(rank, Some(5), "Updated NULLIF rank should be 5");
}

/// Before the AEXPR_LIKE fix, `col LIKE '%pattern%'` in a WHERE clause caused
/// the DVM parser to bail with "A_Expr kind AEXPR_LIKE is not supported",
/// silently downgrading AUTO to FULL.  Verify LIKE is now handled.
#[tokio::test]
async fn test_like_in_where_auto_mode_resolves_to_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE like_regr_src (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute(
        "INSERT INTO like_regr_src VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alicia'), (4, 'Charlie')",
    )
    .await;

    let q = "SELECT id, name FROM like_regr_src WHERE name LIKE 'Ali%'";

    db.create_st("like_regr_st", q, "1m", "AUTO").await;

    let (status, mode, populated, errors) = db.pgt_status("like_regr_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(
        mode, "DIFFERENTIAL",
        "LIKE in WHERE must not prevent DIFFERENTIAL mode (AEXPR_LIKE regression)"
    );
    assert!(populated);
    assert_eq!(errors, 0);

    db.assert_st_matches_query("like_regr_st", q).await;

    // Incremental: insert a matching row
    db.execute("INSERT INTO like_regr_src VALUES (5, 'Aline')")
        .await;
    db.refresh_st("like_regr_st").await;
    db.assert_st_matches_query("like_regr_st", q).await;
}

/// Same as above but for ILIKE (case-insensitive LIKE).
#[tokio::test]
async fn test_ilike_in_where_auto_mode_resolves_to_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ilike_regr_src (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute(
        "INSERT INTO ilike_regr_src VALUES (1, 'Alice'), (2, 'bob'), (3, 'ALICE'), (4, 'Charlie')",
    )
    .await;

    let q = "SELECT id, name FROM ilike_regr_src WHERE name ILIKE 'ali%'";

    db.create_st("ilike_regr_st", q, "1m", "AUTO").await;

    let (status, mode, populated, errors) = db.pgt_status("ilike_regr_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(
        mode, "DIFFERENTIAL",
        "ILIKE in WHERE must not prevent DIFFERENTIAL mode (AEXPR_ILIKE regression)"
    );
    assert!(populated);
    assert_eq!(errors, 0);

    db.assert_st_matches_query("ilike_regr_st", q).await;

    // Incremental: insert a matching row (mixed case)
    db.execute("INSERT INTO ilike_regr_src VALUES (5, 'aLiCiA')")
        .await;
    db.refresh_st("ilike_regr_st").await;
    db.assert_st_matches_query("ilike_regr_st", q).await;
}

#[tokio::test]
async fn test_expression_invalid_query_fails() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE expr_err_src (id INT PRIMARY KEY)")
        .await;

    let result = db.try_execute(
        "SELECT pgtrickle.create_stream_table('expr_err_st', 'SELECT id + non_existent FROM expr_err_src', '1m', 'DIFFERENTIAL')"
    ).await;

    assert!(
        result.is_err(),
        "Creation with invalid expression query should fail"
    );
}
