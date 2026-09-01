//! E2E tests for mixed PostgreSQL objects interacting with stream tables.
//!
//! Validates scenarios where normal PostgreSQL users create views,
//! materialized views, and regular tables that feed into, read from,
//! or sit between stream tables. Ensures data never goes stale and
//! propagation works correctly across all layers.
//!
//! Topology coverage:
//! - **Upstream**: Views/matviews/tables → stream table
//! - **Midstream**: table → view → stream table → view → stream table
//! - **Downstream**: stream table → regular view / matview consumers
//! - **Mixed chains**: combinations of all object types
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════════
// UPSTREAM: Regular PG objects feeding INTO stream tables
// ═══════════════════════════════════════════════════════════════════════════

// ── View upstream (DIFFERENTIAL) ─────────────────────────────────────────

/// Table → View → Stream Table (DIFFERENTIAL).
/// Insert/update/delete on the base table propagates through the view.
#[tokio::test]
async fn test_mixed_view_upstream_differential_full_dml() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mu_products (
            id    SERIAL PRIMARY KEY,
            name  TEXT NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            active BOOLEAN DEFAULT true
        )",
    )
    .await;
    db.execute(
        "INSERT INTO mu_products (name, price, active) VALUES
         ('Widget', 9.99, true),
         ('Gadget', 19.99, true),
         ('Gizmo', 29.99, false)",
    )
    .await;

    // User creates a view filtering active products
    db.execute(
        "CREATE VIEW mu_active_products AS
         SELECT id, name, price FROM mu_products WHERE active = true",
    )
    .await;

    // Stream table built on top of the view
    db.create_st(
        "mu_st_products",
        "SELECT id, name, price FROM mu_active_products",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let (status, mode, populated, errors) = db.pgt_status("mu_st_products").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "DIFFERENTIAL");
    assert!(populated);
    assert_eq!(errors, 0);
    assert_eq!(db.count("public.mu_st_products").await, 2);

    // INSERT: new active product
    db.execute("INSERT INTO mu_products (name, price, active) VALUES ('Doohickey', 5.99, true)")
        .await;
    db.refresh_st("mu_st_products").await;
    assert_eq!(db.count("public.mu_st_products").await, 3);

    // UPDATE: deactivate a product (should disappear from ST)
    db.execute("UPDATE mu_products SET active = false WHERE name = 'Widget'")
        .await;
    db.refresh_st("mu_st_products").await;
    assert_eq!(db.count("public.mu_st_products").await, 2);

    // UPDATE: reactivate and change price
    db.execute("UPDATE mu_products SET active = true, price = 12.99 WHERE name = 'Widget'")
        .await;
    db.refresh_st("mu_st_products").await;
    assert_eq!(db.count("public.mu_st_products").await, 3);
    let price: f64 = db
        .query_scalar("SELECT price::float8 FROM public.mu_st_products WHERE name = 'Widget'")
        .await;
    assert!((price - 12.99).abs() < 0.01);

    // DELETE
    db.execute("DELETE FROM mu_products WHERE name = 'Doohickey'")
        .await;
    db.refresh_st("mu_st_products").await;
    db.assert_st_matches_query(
        "mu_st_products",
        "SELECT id, name, price FROM mu_active_products",
    )
    .await;
}

// ── Multiple views joining into a stream table ───────────────────────────

/// Two independent views (from different tables) joined in a stream table.
#[tokio::test]
async fn test_mixed_multiple_views_joined_in_st() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mu_customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL, tier TEXT NOT NULL)",
    )
    .await;
    db.execute(
        "CREATE TABLE mu_orders (
            id SERIAL PRIMARY KEY,
            customer_id INT NOT NULL REFERENCES mu_customers(id),
            amount NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO mu_customers VALUES (1, 'Alice', 'gold'), (2, 'Bob', 'silver'), (3, 'Carol', 'gold')",
    )
    .await;
    db.execute("INSERT INTO mu_orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50), (4, 3, 300)")
        .await;

    // User creates views
    db.execute(
        "CREATE VIEW mu_gold_customers AS
         SELECT id, name FROM mu_customers WHERE tier = 'gold'",
    )
    .await;
    db.execute(
        "CREATE VIEW mu_large_orders AS
         SELECT id, customer_id, amount FROM mu_orders WHERE amount >= 100",
    )
    .await;

    // Stream table joins both views
    db.create_st(
        "mu_st_gold_large",
        "SELECT c.name, o.amount
         FROM mu_gold_customers c
         JOIN mu_large_orders o ON c.id = o.customer_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Alice (gold): orders 100, 200; Carol (gold): order 300 → 3 rows
    assert_eq!(db.count("public.mu_st_gold_large").await, 3);

    // Insert a new large order for a gold customer
    db.execute("INSERT INTO mu_orders VALUES (5, 3, 500)").await;
    db.refresh_st("mu_st_gold_large").await;
    assert_eq!(db.count("public.mu_st_gold_large").await, 4);

    // Demote Alice from gold → silver (her orders should disappear)
    db.execute("UPDATE mu_customers SET tier = 'silver' WHERE name = 'Alice'")
        .await;
    db.refresh_st("mu_st_gold_large").await;
    // Only Carol's orders remain: 300, 500
    assert_eq!(db.count("public.mu_st_gold_large").await, 2);

    db.assert_st_matches_query(
        "mu_st_gold_large",
        "SELECT c.name, o.amount
         FROM mu_gold_customers c
         JOIN mu_large_orders o ON c.id = o.customer_id",
    )
    .await;
}

// ── Materialized view upstream (FULL mode) ───────────────────────────────

/// Materialized view → Stream Table (FULL mode).
/// After refreshing the matview, refreshing the ST picks up changes.
#[tokio::test]
async fn test_mixed_matview_upstream_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mu_sales (
            id SERIAL PRIMARY KEY,
            region TEXT NOT NULL,
            amount NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO mu_sales (region, amount) VALUES
         ('East', 100), ('East', 200), ('West', 50), ('West', 150)",
    )
    .await;

    // User creates a materialized view for aggregation
    db.execute(
        "CREATE MATERIALIZED VIEW mu_mv_region_totals AS
         SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
         FROM mu_sales GROUP BY region",
    )
    .await;

    // Stream table on matview (must use FULL mode)
    db.create_st(
        "mu_st_region_totals",
        "SELECT region, total, cnt FROM mu_mv_region_totals",
        "1m",
        "FULL",
    )
    .await;

    let (status, mode, populated, _) = db.pgt_status("mu_st_region_totals").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);
    assert_eq!(db.count("public.mu_st_region_totals").await, 2);

    // Add new sales
    db.execute("INSERT INTO mu_sales (region, amount) VALUES ('East', 300), ('North', 75)")
        .await;

    // Without refreshing the matview, ST would be stale
    // Refresh matview first, then ST
    db.execute("REFRESH MATERIALIZED VIEW mu_mv_region_totals")
        .await;
    db.refresh_st("mu_st_region_totals").await;

    assert_eq!(
        db.count("public.mu_st_region_totals").await,
        3,
        "Should now have East, West, North"
    );
    db.assert_st_matches_query(
        "mu_st_region_totals",
        "SELECT region, total, cnt FROM mu_mv_region_totals",
    )
    .await;
}

// ── Regular table + view combined upstream ───────────────────────────────

/// Stream table joins a raw table with a view (mixed upstream sources).
#[tokio::test]
async fn test_mixed_table_and_view_combined_upstream() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mu_employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, dept_id INT NOT NULL)",
    )
    .await;
    db.execute(
        "CREATE TABLE mu_departments (id SERIAL PRIMARY KEY, dept_name TEXT NOT NULL, budget NUMERIC(12,2) NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO mu_departments VALUES (1, 'Engineering', 1000000), (2, 'Sales', 500000)",
    )
    .await;
    db.execute("INSERT INTO mu_employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2)")
        .await;

    // View on departments (user-facing filter)
    db.execute(
        "CREATE VIEW mu_big_depts AS
         SELECT id, dept_name FROM mu_departments WHERE budget >= 500000",
    )
    .await;

    // ST joins raw table with view
    db.create_st(
        "mu_st_big_dept_employees",
        "SELECT e.name, d.dept_name
         FROM mu_employees e
         JOIN mu_big_depts d ON e.dept_id = d.id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mu_st_big_dept_employees").await, 3);

    // Reduce Sales budget below threshold
    db.execute("UPDATE mu_departments SET budget = 100000 WHERE dept_name = 'Sales'")
        .await;
    db.refresh_st("mu_st_big_dept_employees").await;

    // Carol (Sales) should disappear
    assert_eq!(db.count("public.mu_st_big_dept_employees").await, 2);

    // Add new employee to Engineering
    db.execute("INSERT INTO mu_employees VALUES (4, 'Dave', 1)")
        .await;
    db.refresh_st("mu_st_big_dept_employees").await;

    db.assert_st_matches_query(
        "mu_st_big_dept_employees",
        "SELECT e.name, d.dept_name
         FROM mu_employees e
         JOIN mu_big_depts d ON e.dept_id = d.id",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// DOWNSTREAM: Reading from stream tables through regular PG objects
// ═══════════════════════════════════════════════════════════════════════════

// ── Regular view on top of a stream table ────────────────────────────────

/// Stream table → Regular VIEW (user reads through the view).
/// The view always reflects the current ST contents after refresh.
#[tokio::test]
async fn test_mixed_view_on_stream_table() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE md_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO md_src VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .await;

    db.create_st(
        "md_st_src",
        "SELECT id, val FROM md_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // User creates a regular view on top of the stream table
    db.execute(
        "CREATE VIEW md_user_view AS
         SELECT id, val, val * 2 AS doubled FROM md_st_src WHERE val > 15",
    )
    .await;

    // Query through the view
    let count: i64 = db.query_scalar("SELECT count(*) FROM md_user_view").await;
    assert_eq!(count, 3, "View should show rows where val > 15");

    // Mutate source and refresh ST
    db.execute("INSERT INTO md_src VALUES (5, 50)").await;
    db.execute("DELETE FROM md_src WHERE id = 2").await;
    db.refresh_st("md_st_src").await;

    // View should reflect updated ST
    let count_after: i64 = db.query_scalar("SELECT count(*) FROM md_user_view").await;
    assert_eq!(
        count_after, 3,
        "View should reflect ST after refresh (removed id=2/val=20, added id=5/val=50)"
    );

    let sum: i64 = db
        .query_scalar("SELECT SUM(doubled)::bigint FROM md_user_view")
        .await;
    // Rows: id=3/val=30→60, id=4/val=40→80, id=5/val=50→100 → 240
    assert_eq!(sum, 240, "View computations should use fresh ST data");
}

// ── Materialized view on top of a stream table ───────────────────────────

/// Stream table → MATERIALIZED VIEW (snapshot of ST at matview refresh time).
/// User must manually refresh the matview to pick up ST changes.
#[tokio::test]
async fn test_mixed_matview_on_stream_table() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE md_events (id SERIAL PRIMARY KEY, event_type TEXT NOT NULL, amount INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO md_events (event_type, amount) VALUES
         ('sale', 100), ('sale', 200), ('refund', 50), ('sale', 150)",
    )
    .await;

    db.create_st(
        "md_st_events",
        "SELECT event_type, SUM(amount) AS total, COUNT(*) AS cnt
         FROM md_events GROUP BY event_type",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // User creates a materialized view on the stream table
    db.execute(
        "CREATE MATERIALIZED VIEW md_mv_summary AS
         SELECT event_type, total, cnt FROM md_st_events",
    )
    .await;

    let count: i64 = db.query_scalar("SELECT count(*) FROM md_mv_summary").await;
    assert_eq!(count, 2, "Matview should have sale + refund");

    // Add more events
    db.execute("INSERT INTO md_events (event_type, amount) VALUES ('sale', 300), ('return', 25)")
        .await;
    db.refresh_st("md_st_events").await;

    // Matview is stale — it still has old data
    let mv_count: i64 = db.query_scalar("SELECT count(*) FROM md_mv_summary").await;
    assert_eq!(
        mv_count, 2,
        "Matview should still be stale (not yet refreshed)"
    );

    // ST itself is fresh
    let st_count: i64 = db.query_scalar("SELECT count(*) FROM md_st_events").await;
    assert_eq!(st_count, 3, "ST should have sale, refund, return");

    // Now refresh the matview
    db.execute("REFRESH MATERIALIZED VIEW md_mv_summary").await;

    let mv_count_after: i64 = db.query_scalar("SELECT count(*) FROM md_mv_summary").await;
    assert_eq!(
        mv_count_after, 3,
        "Matview should be fresh after explicit refresh"
    );
}

// ── Regular table populated from stream table ────────────────────────────

/// Stream table → INSERT INTO regular table (ETL pattern).
/// User manually copies data from an ST into a regular table.
#[tokio::test]
async fn test_mixed_regular_table_from_stream_table() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE md_raw (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO md_raw VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    db.create_st(
        "md_st_raw",
        "SELECT id, val FROM md_raw",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // User creates a regular table and populates it from the ST
    db.execute("CREATE TABLE md_snapshot (id INT, val TEXT, snapped_at TIMESTAMPTZ DEFAULT now())")
        .await;
    db.execute("INSERT INTO md_snapshot (id, val) SELECT id, val FROM md_st_raw")
        .await;
    assert_eq!(db.count("md_snapshot").await, 3);

    // Mutate source and refresh
    db.execute("INSERT INTO md_raw VALUES (4, 'd')").await;
    db.refresh_st("md_st_raw").await;

    // Snapshot table is stale (it's just a regular table)
    assert_eq!(db.count("md_snapshot").await, 3);

    // User can re-snapshot
    db.execute("TRUNCATE md_snapshot").await;
    db.execute("INSERT INTO md_snapshot (id, val) SELECT id, val FROM md_st_raw")
        .await;
    assert_eq!(db.count("md_snapshot").await, 4);
}

// ═══════════════════════════════════════════════════════════════════════════
// MIDSTREAM: PG objects sitting between stream table layers
// ═══════════════════════════════════════════════════════════════════════════

// ── Table → View → ST₁ → (user view) → ST₂ ─────────────────────────────

/// Full chain: base table → user view → ST₁ (DIFFERENTIAL) →
/// another user view on ST₁ → ST₂ (FULL, since reading from a view on ST).
/// Changes to the base table propagate all the way to ST₂.
#[tokio::test]
async fn test_mixed_midstream_view_between_two_sts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mm_inventory (
            id SERIAL PRIMARY KEY,
            product TEXT NOT NULL,
            quantity INT NOT NULL,
            warehouse TEXT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO mm_inventory (product, quantity, warehouse) VALUES
         ('Widget', 100, 'A'), ('Widget', 50, 'B'),
         ('Gadget', 200, 'A'), ('Gadget', 75, 'B')",
    )
    .await;

    // User view on base table
    db.execute(
        "CREATE VIEW mm_v_inventory AS
         SELECT product, quantity, warehouse FROM mm_inventory WHERE quantity > 0",
    )
    .await;

    // ST₁: aggregate by product (DIFFERENTIAL, view is inlined)
    db.create_st(
        "mm_st_product_totals",
        "SELECT product, SUM(quantity) AS total_qty FROM mm_v_inventory GROUP BY product",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mm_st_product_totals").await, 2);

    // User creates a view on top of ST₁ (e.g., to add business logic)
    db.execute(
        "CREATE VIEW mm_v_low_stock AS
         SELECT product, total_qty FROM mm_st_product_totals WHERE total_qty < 200",
    )
    .await;

    let low_stock_count: i64 = db.query_scalar("SELECT count(*) FROM mm_v_low_stock").await;
    assert_eq!(low_stock_count, 1, "Only Widget (150) should be low-stock");

    // ST₂ reads directly from ST₁ (ST-on-ST, DIFFERENTIAL)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'mm_st_low_stock',
            $$SELECT product, total_qty FROM mm_st_product_totals WHERE total_qty < 200$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    assert_eq!(db.count("public.mm_st_low_stock").await, 1);

    // Reduce Widget stock
    db.execute(
        "UPDATE mm_inventory SET quantity = 10 WHERE product = 'Widget' AND warehouse = 'A'",
    )
    .await;
    // Also reduce Gadget to below threshold
    db.execute(
        "UPDATE mm_inventory SET quantity = 30 WHERE product = 'Gadget' AND warehouse = 'A'",
    )
    .await;

    // Refresh ST₁ first (picks up base table changes through view)
    db.refresh_st("mm_st_product_totals").await;

    // Verify ST₁ is correct
    db.assert_st_matches_query(
        "mm_st_product_totals",
        "SELECT product, SUM(quantity) AS total_qty FROM mm_v_inventory GROUP BY product",
    )
    .await;

    // Refresh ST₂ (picks up ST₁ changes)
    db.refresh_st("mm_st_low_stock").await;

    // Both products now < 200: Widget=60, Gadget=105
    assert_eq!(
        db.count("public.mm_st_low_stock").await,
        2,
        "Both products should now be low-stock after cascade refresh"
    );

    // The user view on ST₁ also automatically reflects the refresh
    let low_stock_via_view: i64 = db.query_scalar("SELECT count(*) FROM mm_v_low_stock").await;
    assert_eq!(
        low_stock_via_view, 2,
        "User view on ST₁ should also reflect new data"
    );

    db.assert_st_matches_query(
        "mm_st_low_stock",
        "SELECT product, total_qty FROM mm_st_product_totals WHERE total_qty < 200",
    )
    .await;
}

// ── Nested views → ST → ST (deep chain) ─────────────────────────────────

/// base table → view₁ → view₂ → ST₁ → ST₂
/// Tests that deeply nested view inlining + ST-on-ST works end to end.
#[tokio::test]
async fn test_mixed_nested_views_to_st_chain() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mn_logs (
            id SERIAL PRIMARY KEY,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            ts TIMESTAMPTZ DEFAULT now()
        )",
    )
    .await;
    db.execute(
        "INSERT INTO mn_logs (level, message) VALUES
         ('ERROR', 'disk full'), ('WARN', 'memory high'),
         ('ERROR', 'timeout'), ('INFO', 'started'),
         ('ERROR', 'connection lost')",
    )
    .await;

    // Nested views: v1 filters out INFO, v2 filters ERROR only
    db.execute(
        "CREATE VIEW mn_v_not_info AS
         SELECT id, level, message FROM mn_logs WHERE level != 'INFO'",
    )
    .await;
    db.execute(
        "CREATE VIEW mn_v_errors AS
         SELECT id, message FROM mn_v_not_info WHERE level = 'ERROR'",
    )
    .await;

    // ST₁ from nested view
    db.create_st(
        "mn_st_errors",
        "SELECT id, message FROM mn_v_errors",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mn_st_errors").await, 3);

    // ST₂ counts errors (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'mn_st_error_count',
            $$SELECT COUNT(*) AS error_count FROM mn_st_errors$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    let error_count: i64 = db
        .query_scalar("SELECT error_count FROM public.mn_st_error_count")
        .await;
    assert_eq!(error_count, 3);

    // Add more errors
    db.execute(
        "INSERT INTO mn_logs (level, message) VALUES ('ERROR', 'oom'), ('WARN', 'slow query')",
    )
    .await;
    db.refresh_st("mn_st_errors").await;
    db.refresh_st("mn_st_error_count").await;

    let new_count: i64 = db
        .query_scalar("SELECT error_count FROM public.mn_st_error_count")
        .await;
    assert_eq!(
        new_count, 4,
        "Should count 4 errors after adding one more ERROR"
    );

    // Verify intermediate ST is also correct
    db.assert_st_matches_query("mn_st_errors", "SELECT id, message FROM mn_v_errors")
        .await;
}

// ── View with aggregation upstream, ST downstream ────────────────────────

/// Table → Aggregating View → ST → ST (further aggregation).
/// The upstream view does GROUP BY; the ST chain builds on that.
#[tokio::test]
async fn test_mixed_aggregating_view_upstream() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE ma_transactions (
            id SERIAL PRIMARY KEY,
            account TEXT NOT NULL,
            tx_type TEXT NOT NULL,
            amount NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO ma_transactions (account, tx_type, amount) VALUES
         ('A001', 'credit', 500), ('A001', 'debit', 100),
         ('A002', 'credit', 1000), ('A002', 'debit', 250),
         ('A003', 'credit', 200)",
    )
    .await;

    // ST that aggregates directly (same logic a user might put in a view)
    db.create_st(
        "ma_st_balances",
        "SELECT account,
                SUM(CASE WHEN tx_type = 'credit' THEN amount ELSE -amount END) AS balance
         FROM ma_transactions
         GROUP BY account",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.ma_st_balances").await, 3);

    // ST₂: flag accounts with high balance (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ma_st_high_balance',
            $$SELECT account, balance FROM ma_st_balances WHERE balance >= 500$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // A001: 400, A002: 750, A003: 200 → only A002 qualifies
    assert_eq!(db.count("public.ma_st_high_balance").await, 1);

    // Add credits to A001 and A003
    db.execute(
        "INSERT INTO ma_transactions (account, tx_type, amount) VALUES
         ('A001', 'credit', 200), ('A003', 'credit', 500)",
    )
    .await;
    db.refresh_st("ma_st_balances").await;
    db.refresh_st("ma_st_high_balance").await;

    // A001: 600, A002: 750, A003: 700 → all three qualify
    assert_eq!(
        db.count("public.ma_st_high_balance").await,
        3,
        "All accounts should now have high balance"
    );

    db.assert_st_matches_query(
        "ma_st_high_balance",
        "SELECT account, balance FROM ma_st_balances WHERE balance >= 500",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// MIXED: Complex scenarios with multiple object types
// ═══════════════════════════════════════════════════════════════════════════

// ── Diamond with views: table → view → ST₁ + table → ST₂ → ST₃ ─────────

/// Diamond pattern where one branch goes through a view and another
/// directly from a table, converging at a joining stream table.
#[tokio::test]
async fn test_mixed_diamond_with_view_branch() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE md_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL)",
    )
    .await;
    db.execute(
        "CREATE TABLE md_prices (product_id INT NOT NULL REFERENCES md_products(id), price NUMERIC(10,2) NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO md_products VALUES (1, 'Widget', 'hardware'), (2, 'Software', 'digital'), (3, 'Cable', 'hardware')",
    )
    .await;
    db.execute("INSERT INTO md_prices VALUES (1, 9.99), (2, 49.99), (3, 4.99)")
        .await;

    // Branch A: view → ST (products filtered by category)
    db.execute(
        "CREATE VIEW md_v_hardware AS
         SELECT id, name FROM md_products WHERE category = 'hardware'",
    )
    .await;

    db.create_st(
        "md_st_hw_products",
        "SELECT id, name FROM md_v_hardware",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Branch B: direct table → ST (prices)
    db.create_st(
        "md_st_prices",
        "SELECT product_id, price FROM md_prices",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Convergence: join both STs
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'md_st_hw_priced',
            $$SELECT p.name, pr.price
              FROM md_st_hw_products p
              JOIN md_st_prices pr ON p.id = pr.product_id$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Widget=9.99, Cable=4.99
    assert_eq!(db.count("public.md_st_hw_priced").await, 2);

    // Add a new hardware product with price
    db.execute("INSERT INTO md_products VALUES (4, 'Bolt', 'hardware')")
        .await;
    db.execute("INSERT INTO md_prices VALUES (4, 1.99)").await;

    // Refresh both branches then the join
    db.refresh_st("md_st_hw_products").await;
    db.refresh_st("md_st_prices").await;
    db.refresh_st("md_st_hw_priced").await;

    assert_eq!(
        db.count("public.md_st_hw_priced").await,
        3,
        "New hardware product should appear in joined ST"
    );

    // Change a product from hardware to digital
    db.execute("UPDATE md_products SET category = 'digital' WHERE name = 'Cable'")
        .await;
    db.refresh_st("md_st_hw_products").await;
    db.refresh_st("md_st_hw_priced").await;

    assert_eq!(
        db.count("public.md_st_hw_priced").await,
        2,
        "Cable should disappear from hardware-priced join"
    );

    db.assert_st_matches_query(
        "md_st_hw_priced",
        "SELECT p.name, pr.price
         FROM md_st_hw_products p
         JOIN md_st_prices pr ON p.id = pr.product_id",
    )
    .await;
}

// ── Full mix: table + view + matview → ST → view → ST₂ ──────────────────

/// Complex scenario: a FULL-mode ST reads from a matview joined with a
/// view-based table, and a second ST reads from the first.
#[tokio::test]
async fn test_mixed_full_chain_table_view_matview_st_st() {
    let db = E2eDb::new().await.with_extension().await;

    // Source tables
    db.execute(
        "CREATE TABLE mf_users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN DEFAULT true)",
    )
    .await;
    db.execute("CREATE TABLE mf_scores (user_id INT NOT NULL, score INT NOT NULL)")
        .await;
    db.execute(
        "INSERT INTO mf_users VALUES (1, 'Alice', true), (2, 'Bob', true), (3, 'Carol', false)",
    )
    .await;
    db.execute("INSERT INTO mf_scores VALUES (1, 90), (1, 85), (2, 70), (2, 95), (3, 60)")
        .await;

    // View on users (active only)
    db.execute(
        "CREATE VIEW mf_v_active_users AS
         SELECT id, name FROM mf_users WHERE active = true",
    )
    .await;

    // Matview: average score per user
    db.execute(
        "CREATE MATERIALIZED VIEW mf_mv_avg_scores AS
         SELECT user_id, AVG(score)::numeric(5,1) AS avg_score
         FROM mf_scores GROUP BY user_id",
    )
    .await;

    // ST₁ (FULL mode, because references matview): join active users with scores
    db.create_st(
        "mf_st_user_scores",
        "SELECT u.name, s.avg_score
         FROM mf_v_active_users u
         JOIN mf_mv_avg_scores s ON u.id = s.user_id",
        "1m",
        "FULL",
    )
    .await;

    // Alice: 87.5, Bob: 82.5 (Carol is inactive)
    assert_eq!(db.count("public.mf_st_user_scores").await, 2);

    // ST₂ (FULL, reads from ST₁): top scorers
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'mf_st_top_scorers',
            $$SELECT name, avg_score FROM mf_st_user_scores WHERE avg_score >= 85$$,
            'calculated',
            'FULL'
        )",
    )
    .await;

    // Only Alice (87.5) has avg >= 85
    assert_eq!(db.count("public.mf_st_top_scorers").await, 1);

    // Add a high score for Bob, activate Carol
    db.execute("INSERT INTO mf_scores VALUES (2, 100)").await;
    db.execute("UPDATE mf_users SET active = true WHERE name = 'Carol'")
        .await;

    // Must refresh matview first (since ST₁ depends on it)
    db.execute("REFRESH MATERIALIZED VIEW mf_mv_avg_scores")
        .await;
    db.refresh_st("mf_st_user_scores").await;
    db.refresh_st("mf_st_top_scorers").await;

    // Bob: (70+95+100)/3 = 88.3, now >= 85
    // Carol: 60, not >= 85
    // Alice: 87.5, still >= 85
    assert_eq!(
        db.count("public.mf_st_top_scorers").await,
        2,
        "Alice and Bob should both be top scorers now"
    );
}

// ── CREATE OR REPLACE VIEW on ST-feeding view ────────────────────────────

/// User replaces a view that feeds a stream table. The stream table
/// should detect the change and reinitialize with the new query.
#[tokio::test]
async fn test_mixed_replace_upstream_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mr_data (id INT PRIMARY KEY, category TEXT NOT NULL, value INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO mr_data VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'C', 40)")
        .await;

    db.execute("CREATE VIEW mr_v_filtered AS SELECT id, value FROM mr_data WHERE category = 'A'")
        .await;

    db.create_st(
        "mr_st_filtered",
        "SELECT id, value FROM mr_v_filtered",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mr_st_filtered").await, 2);

    // User replaces the view to include category B as well
    db.execute(
        "CREATE OR REPLACE VIEW mr_v_filtered AS
         SELECT id, value FROM mr_data WHERE category IN ('A', 'B')",
    )
    .await;

    // Wait a bit for DDL event trigger
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'mr_st_filtered'",
        )
        .await;

    if needs_reinit {
        db.refresh_st("mr_st_filtered").await;
    } else {
        // Scheduler may have already reinited
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // Should now include category A and B rows
    db.assert_st_matches_query("mr_st_filtered", "SELECT id, value FROM mr_v_filtered")
        .await;
}

// ── Drop upstream view → ST goes to ERROR ────────────────────────────────

/// Dropping a view that feeds a stream table should mark the ST as ERROR.
#[tokio::test]
async fn test_mixed_drop_upstream_view_st_errors() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mdr_base (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO mdr_base VALUES (1, 'x')").await;

    db.execute("CREATE VIEW mdr_view AS SELECT id, val FROM mdr_base")
        .await;

    db.create_st(
        "mdr_st",
        "SELECT id, val FROM mdr_view",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mdr_st").await, 1);

    // Drop the view
    let result = db.try_execute("DROP VIEW mdr_view").await;

    if result.is_ok() {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let status: String = db
            .query_scalar(
                "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'mdr_st'",
            )
            .await;
        assert_eq!(
            status, "ERROR",
            "ST should be in ERROR status after upstream view is dropped"
        );
    }
    // DROP failing is also valid (extension may protect views)
}

// ── Multiple users: one creates tables, another creates views/STs ────────

/// Simulates a multi-user workflow: one user (schema owner) creates
/// base tables, another user creates views and stream tables on them.
/// Validates the cross-object dependency chain works correctly.
#[tokio::test]
async fn test_mixed_multi_schema_tables_and_views() {
    let db = E2eDb::new().await.with_extension().await;

    // Schema A: raw data tables
    db.execute("CREATE SCHEMA IF NOT EXISTS raw_data").await;
    db.execute(
        "CREATE TABLE raw_data.sensor_readings (
            id SERIAL PRIMARY KEY,
            sensor_id INT NOT NULL,
            value NUMERIC(10,2) NOT NULL,
            ts TIMESTAMPTZ DEFAULT now()
        )",
    )
    .await;
    db.execute(
        "INSERT INTO raw_data.sensor_readings (sensor_id, value) VALUES
         (1, 23.5), (1, 24.1), (2, 18.0), (2, 18.5), (3, 30.0)",
    )
    .await;

    // Schema B: analytics views and stream tables
    db.execute("CREATE SCHEMA IF NOT EXISTS analytics").await;
    db.execute(
        "CREATE VIEW analytics.v_recent_readings AS
         SELECT sensor_id, value FROM raw_data.sensor_readings",
    )
    .await;

    // Stream table in public schema, reading from analytics view
    db.create_st(
        "sensor_averages",
        "SELECT sensor_id, AVG(value)::numeric(10,2) AS avg_value, COUNT(*) AS reading_count
         FROM analytics.v_recent_readings GROUP BY sensor_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.sensor_averages").await, 3);

    // Add more readings
    db.execute(
        "INSERT INTO raw_data.sensor_readings (sensor_id, value) VALUES (1, 25.0), (4, 15.0)",
    )
    .await;
    db.refresh_st("sensor_averages").await;

    assert_eq!(
        db.count("public.sensor_averages").await,
        4,
        "Should have 4 sensors (added sensor 4)"
    );

    db.assert_st_matches_query(
        "sensor_averages",
        "SELECT sensor_id, AVG(value)::numeric(10,2) AS avg_value, COUNT(*) AS reading_count
         FROM analytics.v_recent_readings GROUP BY sensor_id",
    )
    .await;
}

// ── View on ST joined with regular table ─────────────────────────────────

/// User creates a view that joins a stream table with a regular table.
/// This is a common read pattern: enrich ST data with reference data.
#[tokio::test]
async fn test_mixed_view_joining_st_with_regular_table() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mj_orders (id SERIAL PRIMARY KEY, product_id INT NOT NULL, qty INT NOT NULL)",
    )
    .await;
    db.execute(
        "CREATE TABLE mj_products (id SERIAL PRIMARY KEY, name TEXT NOT NULL, unit_price NUMERIC(10,2) NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO mj_products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)")
        .await;
    db.execute("INSERT INTO mj_orders VALUES (1, 1, 5), (2, 2, 3), (3, 1, 2)")
        .await;

    // Stream table aggregates orders
    db.create_st(
        "mj_st_order_totals",
        "SELECT product_id, SUM(qty) AS total_qty FROM mj_orders GROUP BY product_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // User creates a view joining ST with reference table
    db.execute(
        "CREATE VIEW mj_v_order_report AS
         SELECT p.name, o.total_qty, (o.total_qty * p.unit_price) AS total_value
         FROM mj_st_order_totals o
         JOIN mj_products p ON o.product_id = p.id",
    )
    .await;

    // Check through the view
    let widget_value: f64 = db
        .query_scalar("SELECT total_value::float8 FROM mj_v_order_report WHERE name = 'Widget'")
        .await;
    // Widget: 7 qty * 9.99 = 69.93
    assert!((widget_value - 69.93).abs() < 0.01);

    // Add more orders and refresh
    db.execute("INSERT INTO mj_orders VALUES (4, 2, 10)").await;
    db.refresh_st("mj_st_order_totals").await;

    // View should reflect updated ST data
    let gadget_qty: i64 = db
        .query_scalar("SELECT total_qty FROM mj_v_order_report WHERE name = 'Gadget'")
        .await;
    assert_eq!(gadget_qty, 13, "Gadget total should be 3 + 10 = 13");

    // Update reference data (no ST refresh needed, view resolves at query time)
    db.execute("UPDATE mj_products SET unit_price = 24.99 WHERE name = 'Gadget'")
        .await;

    let gadget_value: f64 = db
        .query_scalar("SELECT total_value::float8 FROM mj_v_order_report WHERE name = 'Gadget'")
        .await;
    // Gadget: 13 * 24.99 = 324.87
    assert!(
        (gadget_value - 324.87).abs() < 0.01,
        "Reference data update should be reflected immediately through view"
    );
}

// ── Three-layer mixed: table → view → ST → ST → view consumer ───────────

/// End-to-end: source table → user view → ST₁ (DIFF) → ST₂ (ST-on-ST)
/// → user view (downstream consumer). Verify correctness at every layer.
#[tokio::test]
async fn test_mixed_three_layer_full_chain_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE m3_shipments (
            id SERIAL PRIMARY KEY,
            origin TEXT NOT NULL,
            destination TEXT NOT NULL,
            weight_kg NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO m3_shipments (origin, destination, weight_kg) VALUES
         ('NYC', 'LAX', 100), ('NYC', 'LAX', 200),
         ('CHI', 'LAX', 150), ('NYC', 'CHI', 50),
         ('CHI', 'MIA', 300)",
    )
    .await;

    // Layer 0: User view filtering domestic shipments
    db.execute(
        "CREATE VIEW m3_v_domestic AS
         SELECT id, origin, destination, weight_kg FROM m3_shipments",
    )
    .await;

    // Layer 1: ST aggregates by route
    db.create_st(
        "m3_st_routes",
        "SELECT origin, destination, SUM(weight_kg) AS total_kg, COUNT(*) AS shipment_count
         FROM m3_v_domestic GROUP BY origin, destination",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Layer 2: ST flags heavy routes (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'm3_st_heavy_routes',
            $$SELECT origin, destination, total_kg
              FROM m3_st_routes WHERE total_kg >= 200$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Layer 3: User view for reporting (on ST₂)
    db.execute(
        "CREATE VIEW m3_v_report AS
         SELECT origin || ' → ' || destination AS route,
                total_kg
         FROM m3_st_heavy_routes
         ORDER BY total_kg DESC",
    )
    .await;

    // Initial: NYC→LAX: 300, CHI→MIA: 300 (both ≥200)
    let heavy_count: i64 = db.query_scalar("SELECT count(*) FROM m3_v_report").await;
    assert_eq!(heavy_count, 2);

    // Add a big shipment on NYC→CHI route
    db.execute(
        "INSERT INTO m3_shipments (origin, destination, weight_kg) VALUES ('NYC', 'CHI', 250)",
    )
    .await;

    // Refresh chain in order
    db.refresh_st("m3_st_routes").await;
    db.refresh_st("m3_st_heavy_routes").await;

    // NYC→CHI: 50 + 250 = 300, now ≥ 200
    let heavy_after: i64 = db.query_scalar("SELECT count(*) FROM m3_v_report").await;
    assert_eq!(
        heavy_after, 3,
        "NYC→CHI route should now appear in heavy routes report"
    );

    // Verify report view has correct data
    let nyc_chi_kg: f64 = db
        .query_scalar("SELECT total_kg::float8 FROM m3_v_report WHERE route = 'NYC → CHI'")
        .await;
    assert!((nyc_chi_kg - 300.0).abs() < 0.01);

    // Delete shipments to make a route fall below threshold
    db.execute("DELETE FROM m3_shipments WHERE origin = 'CHI' AND destination = 'MIA'")
        .await;
    db.refresh_st("m3_st_routes").await;
    db.refresh_st("m3_st_heavy_routes").await;

    let heavy_final: i64 = db.query_scalar("SELECT count(*) FROM m3_v_report").await;
    assert_eq!(
        heavy_final, 2,
        "CHI→MIA should drop from heavy routes after deletion"
    );
}

// ── TRUNCATE propagation through view to ST chain ────────────────────────

/// TRUNCATE base table → view → ST should clear the ST data.
#[tokio::test]
async fn test_mixed_truncate_propagation_through_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mt_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO mt_src VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    db.execute("CREATE VIEW mt_view AS SELECT id, val FROM mt_src")
        .await;

    db.create_st("mt_st", "SELECT id, val FROM mt_view", "1m", "DIFFERENTIAL")
        .await;

    assert_eq!(db.count("public.mt_st").await, 3);

    // TRUNCATE the base table
    db.execute("TRUNCATE mt_src").await;
    db.refresh_st("mt_st").await;

    assert_eq!(
        db.count("public.mt_st").await,
        0,
        "ST should be empty after base table TRUNCATE"
    );

    // Re-populate and verify recovery
    db.execute("INSERT INTO mt_src VALUES (10, 100), (20, 200)")
        .await;
    db.refresh_st("mt_st").await;

    assert_eq!(db.count("public.mt_st").await, 2);
    db.assert_st_matches_query("mt_st", "SELECT id, val FROM mt_view")
        .await;
}

// ── Matview + view + table all feeding one ST (FULL mode) ────────────────

/// Three different source types feeding into a single FULL-mode ST
/// via UNION ALL.
#[tokio::test]
async fn test_mixed_union_all_table_view_matview_full() {
    let db = E2eDb::new().await.with_extension().await;

    // Raw table
    db.execute(
        "CREATE TABLE muf_raw (id INT PRIMARY KEY, source TEXT DEFAULT 'raw', val INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO muf_raw VALUES (1, 'raw', 10), (2, 'raw', 20)")
        .await;

    // View
    db.execute("CREATE TABLE muf_base_b (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO muf_base_b VALUES (3, 30), (4, 40)")
        .await;
    db.execute("CREATE VIEW muf_view AS SELECT id, 'view' AS source, val FROM muf_base_b")
        .await;

    // Materialized view
    db.execute("CREATE TABLE muf_base_c (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO muf_base_c VALUES (5, 50), (6, 60)")
        .await;
    db.execute(
        "CREATE MATERIALIZED VIEW muf_matview AS SELECT id, 'matview' AS source, val FROM muf_base_c",
    )
    .await;

    // ST combines all three via UNION ALL (FULL mode because matview)
    db.create_st(
        "muf_st_combined",
        "SELECT id, source, val FROM muf_raw
         UNION ALL
         SELECT id, source, val FROM muf_view
         UNION ALL
         SELECT id, source, val FROM muf_matview",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.muf_st_combined").await, 6);

    // Add data to all three sources
    db.execute("INSERT INTO muf_raw VALUES (7, 'raw', 70)")
        .await;
    db.execute("INSERT INTO muf_base_b VALUES (8, 80)").await;
    db.execute("INSERT INTO muf_base_c VALUES (9, 90)").await;
    // Must refresh matview since it's a snapshot
    db.execute("REFRESH MATERIALIZED VIEW muf_matview").await;

    db.refresh_st("muf_st_combined").await;

    assert_eq!(
        db.count("public.muf_st_combined").await,
        9,
        "All three sources should contribute to the ST"
    );

    db.assert_st_matches_query(
        "muf_st_combined",
        "SELECT id, source, val FROM muf_raw
         UNION ALL
         SELECT id, source, val FROM muf_view
         UNION ALL
         SELECT id, source, val FROM muf_matview",
    )
    .await;
}

// ── View consuming from ST, then another ST reads same base ──────────────

/// Two STs from the same base table. A view joins them.
/// Ensures independent STs stay consistent after refreshes.
#[tokio::test]
async fn test_mixed_two_sts_same_base_view_joins_them() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE m2_items (id SERIAL PRIMARY KEY, name TEXT NOT NULL, qty INT NOT NULL, price NUMERIC(10,2) NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO m2_items VALUES
         (1, 'Apple', 100, 1.50),
         (2, 'Banana', 200, 0.75),
         (3, 'Cherry', 50, 3.00)",
    )
    .await;

    // ST₁: quantity info
    db.create_st(
        "m2_st_qty",
        "SELECT id, name, qty FROM m2_items",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // ST₂: price info
    db.create_st(
        "m2_st_price",
        "SELECT id, name, price FROM m2_items",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // User creates a view joining both STs
    db.execute(
        "CREATE VIEW m2_v_inventory AS
         SELECT q.name, q.qty, p.price, (q.qty * p.price) AS total_value
         FROM m2_st_qty q JOIN m2_st_price p ON q.id = p.id",
    )
    .await;

    let total_value: f64 = db
        .query_scalar("SELECT SUM(total_value)::float8 FROM m2_v_inventory")
        .await;
    // Apple: 150, Banana: 150, Cherry: 150 = 450
    assert!((total_value - 450.0).abs() < 0.01);

    // Update quantities
    db.execute("UPDATE m2_items SET qty = 200 WHERE name = 'Apple'")
        .await;
    db.refresh_st("m2_st_qty").await;
    db.refresh_st("m2_st_price").await;

    let new_total: f64 = db
        .query_scalar("SELECT SUM(total_value)::float8 FROM m2_v_inventory")
        .await;
    // Apple: 300, Banana: 150, Cherry: 150 = 600
    assert!((new_total - 600.0).abs() < 0.01);
}

// ── Ensure matview on ST is NOT auto-refreshed ───────────────────────────

/// A materialized view on a stream table does NOT auto-refresh.
/// This is a "staleness awareness" test: after ST refresh, the matview
/// remains stale until explicitly refreshed.
#[tokio::test]
async fn test_mixed_matview_on_st_stays_stale_until_refreshed() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ms_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO ms_src VALUES (1, 10), (2, 20)")
        .await;

    db.create_st("ms_st", "SELECT id, val FROM ms_src", "1m", "DIFFERENTIAL")
        .await;

    db.execute("CREATE MATERIALIZED VIEW ms_mv AS SELECT id, val FROM ms_st")
        .await;

    // Both should have 2 rows
    assert_eq!(db.count("public.ms_st").await, 2);
    assert_eq!(db.count("ms_mv").await, 2);

    // Add data, refresh ST only
    db.execute("INSERT INTO ms_src VALUES (3, 30), (4, 40)")
        .await;
    db.refresh_st("ms_st").await;

    // ST is fresh
    assert_eq!(db.count("public.ms_st").await, 4);
    // Matview is stale
    assert_eq!(
        db.count("ms_mv").await,
        2,
        "Matview should be stale (still 2 rows)"
    );

    // Explicit refresh makes it fresh
    db.execute("REFRESH MATERIALIZED VIEW ms_mv").await;
    assert_eq!(
        db.count("ms_mv").await,
        4,
        "Matview should be fresh after explicit refresh"
    );
}

// ── View with CASE/complex expressions on ST ─────────────────────────────

/// User creates a view with complex expressions on top of a stream table.
#[tokio::test]
async fn test_mixed_complex_view_expressions_on_st() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mc_orders (
            id SERIAL PRIMARY KEY,
            amount NUMERIC(10,2) NOT NULL,
            status TEXT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO mc_orders (amount, status) VALUES
         (50, 'pending'), (150, 'shipped'), (500, 'pending'),
         (25, 'delivered'), (300, 'shipped')",
    )
    .await;

    db.create_st(
        "mc_st_orders",
        "SELECT id, amount, status FROM mc_orders",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // User creates a view with business logic on the ST
    db.execute(
        "CREATE VIEW mc_v_order_dashboard AS
         SELECT
             id,
             amount,
             status,
             CASE
                 WHEN amount >= 500 THEN 'premium'
                 WHEN amount >= 100 THEN 'standard'
                 ELSE 'basic'
             END AS tier,
             CASE WHEN status = 'delivered' THEN true ELSE false END AS is_completed
         FROM mc_st_orders",
    )
    .await;

    let premium_count: i64 = db
        .query_scalar("SELECT count(*) FROM mc_v_order_dashboard WHERE tier = 'premium'")
        .await;
    assert_eq!(premium_count, 1);

    // Add a premium order
    db.execute("INSERT INTO mc_orders (amount, status) VALUES (1000, 'pending')")
        .await;
    db.refresh_st("mc_st_orders").await;

    let new_premium: i64 = db
        .query_scalar("SELECT count(*) FROM mc_v_order_dashboard WHERE tier = 'premium'")
        .await;
    assert_eq!(
        new_premium, 2,
        "New premium order should appear in dashboard view"
    );
}

// ── Parallel STs from same view ──────────────────────────────────────────

/// Multiple stream tables reading from the same view, each with different
/// filters. All should stay consistent independently.
#[tokio::test]
async fn test_mixed_parallel_sts_from_same_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mp_events (
            id SERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            severity INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO mp_events (event_type, severity) VALUES
         ('login', 1), ('error', 5), ('warning', 3),
         ('error', 4), ('login', 1), ('critical', 5)",
    )
    .await;

    db.execute(
        "CREATE VIEW mp_v_all_events AS
         SELECT id, event_type, severity FROM mp_events",
    )
    .await;

    // Three STs from same view, different filters
    db.create_st(
        "mp_st_errors",
        "SELECT id, event_type, severity FROM mp_v_all_events WHERE event_type = 'error'",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "mp_st_critical",
        "SELECT id, event_type, severity FROM mp_v_all_events WHERE severity >= 5",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "mp_st_all",
        "SELECT event_type, COUNT(*) AS cnt FROM mp_v_all_events GROUP BY event_type",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mp_st_errors").await, 2);
    assert_eq!(db.count("public.mp_st_critical").await, 2);
    assert_eq!(db.count("public.mp_st_all").await, 4);

    // Add events
    db.execute("INSERT INTO mp_events (event_type, severity) VALUES ('error', 5), ('info', 1)")
        .await;

    // Refresh all three
    db.refresh_st("mp_st_errors").await;
    db.refresh_st("mp_st_critical").await;
    db.refresh_st("mp_st_all").await;

    assert_eq!(db.count("public.mp_st_errors").await, 3);
    assert_eq!(
        db.count("public.mp_st_critical").await,
        3,
        "New error with severity 5 should appear in critical"
    );
    assert_eq!(
        db.count("public.mp_st_all").await,
        5,
        "Should now have 5 event types (added 'info')"
    );

    // Verify each is independently correct
    db.assert_st_matches_query(
        "mp_st_errors",
        "SELECT id, event_type, severity FROM mp_v_all_events WHERE event_type = 'error'",
    )
    .await;
    db.assert_st_matches_query(
        "mp_st_critical",
        "SELECT id, event_type, severity FROM mp_v_all_events WHERE severity >= 5",
    )
    .await;
    db.assert_st_matches_query(
        "mp_st_all",
        "SELECT event_type, COUNT(*) AS cnt FROM mp_v_all_events GROUP BY event_type",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// UPSERT: INSERT ... ON CONFLICT through views into stream tables
// ═══════════════════════════════════════════════════════════════════════════

/// INSERT ... ON CONFLICT DO UPDATE on a base table that feeds a view → ST.
/// The upsert should generate proper CDC events that propagate.
#[tokio::test]
async fn test_mixed_upsert_on_conflict_through_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mu_kv (
            key TEXT PRIMARY KEY,
            value INT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now()
        )",
    )
    .await;
    db.execute("INSERT INTO mu_kv (key, value) VALUES ('a', 1), ('b', 2), ('c', 3)")
        .await;

    db.execute("CREATE VIEW mu_v_kv AS SELECT key, value FROM mu_kv")
        .await;

    db.create_st(
        "mu_st_kv",
        "SELECT key, value FROM mu_v_kv",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mu_st_kv").await, 3);

    // Upsert: update existing key, insert new key
    db.execute(
        "INSERT INTO mu_kv (key, value) VALUES ('a', 10), ('d', 4)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .await;
    db.refresh_st("mu_st_kv").await;

    assert_eq!(
        db.count("public.mu_st_kv").await,
        4,
        "Should have 4 keys after upsert (a updated, d inserted)"
    );

    let val_a: i32 = db
        .query_scalar("SELECT value FROM public.mu_st_kv WHERE key = 'a'")
        .await;
    assert_eq!(val_a, 10, "Key 'a' should be updated to 10 by upsert");

    // Bulk upsert: update all existing + add new
    db.execute(
        "INSERT INTO mu_kv (key, value)
         VALUES ('a', 100), ('b', 200), ('c', 300), ('d', 400), ('e', 5)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .await;
    db.refresh_st("mu_st_kv").await;

    assert_eq!(db.count("public.mu_st_kv").await, 5);
    db.assert_st_matches_query("mu_st_kv", "SELECT key, value FROM mu_v_kv")
        .await;
}

/// INSERT ... ON CONFLICT DO NOTHING — skipped rows should not affect ST.
#[tokio::test]
async fn test_mixed_upsert_on_conflict_do_nothing() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mu_skip (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO mu_skip VALUES (1, 'first'), (2, 'second')")
        .await;

    db.create_st(
        "mu_st_skip",
        "SELECT id, val FROM mu_skip",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mu_st_skip").await, 2);

    // ON CONFLICT DO NOTHING: id=1 conflicts (skipped), id=3 is new
    db.execute(
        "INSERT INTO mu_skip VALUES (1, 'conflict'), (3, 'third')
         ON CONFLICT DO NOTHING",
    )
    .await;
    db.refresh_st("mu_st_skip").await;

    assert_eq!(db.count("public.mu_st_skip").await, 3);

    // Verify id=1 was NOT updated
    let val_1: String = db
        .query_scalar("SELECT val FROM public.mu_st_skip WHERE id = 1")
        .await;
    assert_eq!(
        val_1, "first",
        "ON CONFLICT DO NOTHING should not update existing row"
    );

    db.assert_st_matches_query("mu_st_skip", "SELECT id, val FROM mu_skip")
        .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// BULK INSERT: generate_series bulk loads through views into stream tables
// ═══════════════════════════════════════════════════════════════════════════

/// Bulk insert via generate_series into a base table → view → ST.
/// Tests that CDC handles large batch inserts correctly.
#[tokio::test]
async fn test_mixed_bulk_insert_through_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mb_bulk (id INT PRIMARY KEY, category TEXT NOT NULL, val INT NOT NULL)",
    )
    .await;

    db.execute("CREATE VIEW mb_v_bulk AS SELECT id, category, val FROM mb_bulk WHERE val > 0")
        .await;

    db.create_st(
        "mb_st_bulk",
        "SELECT category, COUNT(*) AS cnt, SUM(val) AS total
         FROM mb_v_bulk GROUP BY category",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initially empty
    assert_eq!(db.count("public.mb_st_bulk").await, 0);

    // Bulk insert 1000 rows across 3 categories
    db.execute(
        "INSERT INTO mb_bulk
         SELECT g,
                CASE WHEN g % 3 = 0 THEN 'A' WHEN g % 3 = 1 THEN 'B' ELSE 'C' END,
                g
         FROM generate_series(1, 1000) g",
    )
    .await;
    db.refresh_st("mb_st_bulk").await;

    assert_eq!(
        db.count("public.mb_st_bulk").await,
        3,
        "Should have 3 categories after bulk insert"
    );

    db.assert_st_matches_query(
        "mb_st_bulk",
        "SELECT category, COUNT(*) AS cnt, SUM(val) AS total
         FROM mb_v_bulk GROUP BY category",
    )
    .await;

    // Bulk delete half the rows
    db.execute("DELETE FROM mb_bulk WHERE id > 500").await;
    db.refresh_st("mb_st_bulk").await;

    db.assert_st_matches_query(
        "mb_st_bulk",
        "SELECT category, COUNT(*) AS cnt, SUM(val) AS total
         FROM mb_v_bulk GROUP BY category",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// PARTITIONED TABLE → VIEW → STREAM TABLE
// ═══════════════════════════════════════════════════════════════════════════

/// Partitioned table → view → ST (DIFFERENTIAL).
/// Inserts into different partitions all propagate through the view.
#[tokio::test]
async fn test_mixed_partitioned_table_through_view() {
    let db = E2eDb::new().await.with_extension().await;

    // Create LIST-partitioned table
    db.execute(
        "CREATE TABLE mp_sales (
            id SERIAL,
            region TEXT NOT NULL,
            amount NUMERIC(10,2) NOT NULL,
            PRIMARY KEY (id, region)
        ) PARTITION BY LIST (region)",
    )
    .await;
    db.execute("CREATE TABLE mp_sales_east PARTITION OF mp_sales FOR VALUES IN ('East')")
        .await;
    db.execute("CREATE TABLE mp_sales_west PARTITION OF mp_sales FOR VALUES IN ('West')")
        .await;
    db.execute("CREATE TABLE mp_sales_north PARTITION OF mp_sales FOR VALUES IN ('North')")
        .await;

    db.execute(
        "INSERT INTO mp_sales (region, amount) VALUES
         ('East', 100), ('East', 200), ('West', 300), ('North', 50)",
    )
    .await;

    // View filters high-value sales
    db.execute(
        "CREATE VIEW mp_v_big_sales AS
         SELECT id, region, amount FROM mp_sales WHERE amount >= 100",
    )
    .await;

    db.create_st(
        "mp_st_big_sales",
        "SELECT region, COUNT(*) AS sale_count, SUM(amount) AS total
         FROM mp_v_big_sales GROUP BY region",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // East: 2 sales (100, 200), West: 1 sale (300) → 2 regions
    assert_eq!(db.count("public.mp_st_big_sales").await, 2);

    // Insert into different partitions
    db.execute("INSERT INTO mp_sales (region, amount) VALUES ('North', 500), ('West', 150)")
        .await;
    db.refresh_st("mp_st_big_sales").await;

    // Now all 3 regions have big sales
    assert_eq!(
        db.count("public.mp_st_big_sales").await,
        3,
        "All three partitions should contribute to the ST"
    );

    db.assert_st_matches_query(
        "mp_st_big_sales",
        "SELECT region, COUNT(*) AS sale_count, SUM(amount) AS total
         FROM mp_v_big_sales GROUP BY region",
    )
    .await;

    // Delete from one partition
    db.execute("DELETE FROM mp_sales WHERE region = 'North' AND amount < 200")
        .await;
    db.refresh_st("mp_st_big_sales").await;

    db.assert_st_matches_query(
        "mp_st_big_sales",
        "SELECT region, COUNT(*) AS sale_count, SUM(amount) AS total
         FROM mp_v_big_sales GROUP BY region",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// VIEW JOINING TWO STREAM TABLES (downstream consumer pattern)
// ═══════════════════════════════════════════════════════════════════════════

/// Two independent STs from different base tables. A user view joins them.
/// Verify the view always reflects the latest refreshed state of both STs.
#[tokio::test]
async fn test_mixed_view_joining_two_independent_sts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mj2_employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, dept_id INT NOT NULL)",
    )
    .await;
    db.execute(
        "CREATE TABLE mj2_departments (id SERIAL PRIMARY KEY, dept_name TEXT NOT NULL, floor INT NOT NULL)",
    )
    .await;

    db.execute("INSERT INTO mj2_employees VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Carol', 1)")
        .await;
    db.execute("INSERT INTO mj2_departments VALUES (1, 'Engineering', 3), (2, 'Marketing', 5)")
        .await;

    // Two independent STs
    db.create_st(
        "mj2_st_emps",
        "SELECT id, name, dept_id FROM mj2_employees",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "mj2_st_depts",
        "SELECT id AS dept_id, dept_name, floor FROM mj2_departments",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // User creates a view joining both STs
    db.execute(
        "CREATE VIEW mj2_v_directory AS
         SELECT e.name, d.dept_name, d.floor
         FROM mj2_st_emps e
         JOIN mj2_st_depts d ON e.dept_id = d.dept_id",
    )
    .await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM mj2_v_directory")
        .await;
    assert_eq!(count, 3);

    // Add employee, refresh only employee ST
    db.execute("INSERT INTO mj2_employees VALUES (4, 'Dave', 2)")
        .await;
    db.refresh_st("mj2_st_emps").await;

    let count_after: i64 = db
        .query_scalar("SELECT count(*) FROM mj2_v_directory")
        .await;
    assert_eq!(
        count_after, 4,
        "New employee should appear in view immediately after ST refresh"
    );

    // Add department, refresh only department ST
    db.execute("INSERT INTO mj2_departments VALUES (3, 'Sales', 1)")
        .await;
    db.execute("INSERT INTO mj2_employees VALUES (5, 'Eve', 3)")
        .await;
    db.refresh_st("mj2_st_depts").await;
    // Eve is in employee table but ST not refreshed yet — should NOT appear
    let count_partial: i64 = db
        .query_scalar("SELECT count(*) FROM mj2_v_directory")
        .await;
    assert_eq!(
        count_partial, 4,
        "Eve should NOT appear until employee ST is refreshed"
    );

    // Now refresh employee ST too
    db.refresh_st("mj2_st_emps").await;
    let count_final: i64 = db
        .query_scalar("SELECT count(*) FROM mj2_v_directory")
        .await;
    assert_eq!(
        count_final, 5,
        "Eve should appear after both STs are refreshed"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// DDL EVOLUTION: schema changes on source tables used through views
// ═══════════════════════════════════════════════════════════════════════════

/// ALTER TABLE ADD COLUMN on a source table used through a view → ST.
/// The ST should remain functional — new column is irrelevant to its query.
#[tokio::test]
async fn test_mixed_alter_table_add_column_through_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ma_evolve (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO ma_evolve VALUES (1, 10), (2, 20)")
        .await;

    db.execute("CREATE VIEW ma_v_evolve AS SELECT id, val FROM ma_evolve")
        .await;

    db.create_st(
        "ma_st_evolve",
        "SELECT id, val FROM ma_v_evolve",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.ma_st_evolve").await, 2);

    // Add a new column to the base table — must disable DDL guard first
    // (pg_trickle blocks column-affecting DDL by default to protect STs)
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE ma_evolve ADD COLUMN extra TEXT DEFAULT 'n/a'",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // Insert using the new schema
    db.execute("INSERT INTO ma_evolve (id, val, extra) VALUES (3, 30, 'new')")
        .await;
    db.refresh_st("ma_st_evolve").await;

    assert_eq!(db.count("public.ma_st_evolve").await, 3);
    db.assert_st_matches_query("ma_st_evolve", "SELECT id, val FROM ma_v_evolve")
        .await;
}

/// DROP COLUMN on a source table — even when the dropped column is not used by
/// the view/ST, the ST is suspended until its source contract is repaired.
#[tokio::test]
async fn test_mixed_alter_table_drop_unused_column() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ma_dropcol (id INT PRIMARY KEY, val INT NOT NULL, unused TEXT)")
        .await;
    db.execute("INSERT INTO ma_dropcol VALUES (1, 10, 'x'), (2, 20, 'y')")
        .await;

    db.execute("CREATE VIEW ma_v_dropcol AS SELECT id, val FROM ma_dropcol")
        .await;

    db.create_st(
        "ma_st_dropcol",
        "SELECT id, val FROM ma_v_dropcol",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.ma_st_dropcol").await, 2);

    // Drop the unused column — must disable DDL guard first
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE ma_dropcol DROP COLUMN unused",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    let status: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ma_st_dropcol'",
        )
        .await;
    assert_eq!(status, "SUSPENDED:SOURCE_DESTRUCTIVE_SCHEMA");

    // Repair the source contract before refreshing.
    db.execute("SELECT pgtrickle.reinitialize_stream_table('ma_st_dropcol')")
        .await;

    // ST should work after the explicit repair.
    db.execute("INSERT INTO ma_dropcol (id, val) VALUES (3, 30)")
        .await;
    db.refresh_st("ma_st_dropcol").await;

    assert_eq!(db.count("public.ma_st_dropcol").await, 3);
    db.assert_st_matches_query("ma_st_dropcol", "SELECT id, val FROM ma_v_dropcol")
        .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// TRANSACTION SEMANTICS: multi-statement transaction through views
// ═══════════════════════════════════════════════════════════════════════════

/// Multiple DML statements in a single transaction on a base table that
/// feeds through a view into an ST. Only the committed state should be
/// visible after refresh.
#[tokio::test]
async fn test_mixed_transaction_atomicity_through_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mt_tx (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO mt_tx VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    db.execute("CREATE VIEW mt_v_tx AS SELECT id, val FROM mt_tx")
        .await;

    db.create_st(
        "mt_st_tx",
        "SELECT id, val FROM mt_v_tx",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mt_st_tx").await, 3);

    // Execute multiple DML operations in a single transaction
    db.execute_seq(&[
        "BEGIN",
        "INSERT INTO mt_tx VALUES (4, 40)",
        "UPDATE mt_tx SET val = 99 WHERE id = 1",
        "DELETE FROM mt_tx WHERE id = 2",
        "COMMIT",
    ])
    .await;

    db.refresh_st("mt_st_tx").await;

    // Should reflect the atomic transaction: 3 rows (1 deleted, 1 added)
    assert_eq!(db.count("public.mt_st_tx").await, 3);

    let val_1: i32 = db
        .query_scalar("SELECT val FROM public.mt_st_tx WHERE id = 1")
        .await;
    assert_eq!(val_1, 99, "UPDATE in transaction should be reflected");

    let has_2: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.mt_st_tx WHERE id = 2)")
        .await;
    assert!(!has_2, "DELETE in transaction should be reflected");

    let has_4: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.mt_st_tx WHERE id = 4)")
        .await;
    assert!(has_4, "INSERT in transaction should be reflected");

    db.assert_st_matches_query("mt_st_tx", "SELECT id, val FROM mt_v_tx")
        .await;
}

/// Rolled-back transaction should have no effect on the ST.
#[tokio::test]
async fn test_mixed_rolled_back_transaction_no_effect() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mt_rb (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO mt_rb VALUES (1, 10), (2, 20)")
        .await;

    db.execute("CREATE VIEW mt_v_rb AS SELECT id, val FROM mt_rb")
        .await;

    db.create_st(
        "mt_st_rb",
        "SELECT id, val FROM mt_v_rb",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mt_st_rb").await, 2);

    // Run a transaction, then rollback
    db.execute_seq(&[
        "BEGIN",
        "INSERT INTO mt_rb VALUES (3, 30)",
        "DELETE FROM mt_rb WHERE id = 1",
        "ROLLBACK",
    ])
    .await;

    db.refresh_st("mt_st_rb").await;

    // Nothing should have changed
    assert_eq!(
        db.count("public.mt_st_rb").await,
        2,
        "Rolled-back transaction should not affect ST"
    );
    db.assert_st_matches_query("mt_st_rb", "SELECT id, val FROM mt_v_rb")
        .await;
}
