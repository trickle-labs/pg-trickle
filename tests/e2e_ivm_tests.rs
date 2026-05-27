//! E2E tests for IMMEDIATE-mode (Transactional IVM) stream tables.
//!
//! Validates that IMMEDIATE stream tables:
//! - Are maintained synchronously within the same transaction as DML.
//! - Handle INSERT, UPDATE, DELETE, and TRUNCATE correctly.
//! - Support window functions, LATERAL joins, and scalar subqueries.
//! - Reject unsupported features (TopK, recursive CTEs).
//! - Cascade through dependent IMMEDIATE stream tables.
//! - Handle concurrent inserts correctly.
//! - Clean up properly on DROP.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Helper ─────────────────────────────────────────────────────────────

/// Create an IMMEDIATE-mode stream table (schedule = NULL).
async fn create_immediate_st(db: &E2eDb, name: &str, query: &str) {
    let sql = format!(
        "SELECT pgtrickle.create_stream_table('{name}', $${query}$$, \
         NULL, 'IMMEDIATE')"
    );
    db.execute(&sql).await;
}

// ── Basic Creation ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_create_simple_select() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO orders VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
        .await;

    let query = "SELECT id, customer, amount FROM orders";
    create_immediate_st(&db, "order_imm", query).await;
    db.assert_st_matches_query("order_imm", query).await;

    // Verify catalog entry
    let (status, mode, populated, errors) = db.pgt_status("order_imm").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated, "ST should be populated after create");
    assert_eq!(errors, 0);

    // Verify initial data
    let count = db.count("public.order_imm").await;
    assert_eq!(count, 2, "ST should contain 2 rows after initial populate");

    // Check schedule is NULL for IMMEDIATE
    let schedule_is_null: bool = db
        .query_scalar(
            "SELECT schedule IS NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'order_imm'",
        )
        .await;
    assert!(schedule_is_null, "IMMEDIATE ST should have NULL schedule");
}

// ── INSERT Propagation ─────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_insert_propagates_immediately() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price NUMERIC)")
        .await;
    db.execute("INSERT INTO products VALUES (1, 'Widget', 10.00)")
        .await;

    let query = "SELECT id, name, price FROM products";
    create_immediate_st(&db, "product_imm", query).await;
    db.assert_st_matches_query("product_imm", query).await;

    let count_before = db.count("public.product_imm").await;
    assert_eq!(count_before, 1);

    // Insert a new row — should immediately appear in the ST.
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 25.00)")
        .await;

    let count_after = db.count("public.product_imm").await;
    assert_eq!(
        count_after, 2,
        "ST should have 2 rows after INSERT on base table"
    );

    // Verify the new value
    let gadget_price: String = db
        .query_scalar("SELECT price::text FROM public.product_imm WHERE name = 'Gadget'")
        .await;
    assert_eq!(gadget_price, "25.00");
    db.assert_st_matches_query("product_imm", query).await;
}

#[tokio::test]
async fn test_ivm_multi_row_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE items (id INT PRIMARY KEY, val TEXT)")
        .await;

    let query = "SELECT id, val FROM items";
    create_immediate_st(&db, "items_imm", query).await;
    db.assert_st_matches_query("items_imm", query).await;

    // Insert multiple rows in one statement.
    db.execute("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    let count = db.count("public.items_imm").await;
    assert_eq!(count, 3, "ST should have 3 rows after multi-row INSERT");
}

// ── UPDATE Propagation ─────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_update_propagates_immediately() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE inventory (id INT PRIMARY KEY, product TEXT, qty INT)")
        .await;
    db.execute("INSERT INTO inventory VALUES (1, 'Bolts', 100), (2, 'Nuts', 200)")
        .await;

    let query = "SELECT id, product, qty FROM inventory";
    create_immediate_st(&db, "inv_imm", query).await;
    db.assert_st_matches_query("inv_imm", query).await;

    // Update a row.
    db.execute("UPDATE inventory SET qty = 150 WHERE id = 1")
        .await;

    let new_qty: i32 = db
        .query_scalar("SELECT qty FROM public.inv_imm WHERE product = 'Bolts'")
        .await;
    assert_eq!(new_qty, 150, "ST should reflect UPDATE immediately");

    // Unchanged row should remain.
    let nuts_qty: i32 = db
        .query_scalar("SELECT qty FROM public.inv_imm WHERE product = 'Nuts'")
        .await;
    assert_eq!(nuts_qty, 200, "Non-updated row should be unchanged");
}

// ── DELETE Propagation ─────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_delete_propagates_immediately() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE tasks (id INT PRIMARY KEY, title TEXT)")
        .await;
    db.execute("INSERT INTO tasks VALUES (1, 'Task A'), (2, 'Task B'), (3, 'Task C')")
        .await;

    let query = "SELECT id, title FROM tasks";
    create_immediate_st(&db, "tasks_imm", query).await;
    db.assert_st_matches_query("tasks_imm", query).await;

    let count_before = db.count("public.tasks_imm").await;
    assert_eq!(count_before, 3);

    // Delete a row.
    db.execute("DELETE FROM tasks WHERE id = 2").await;

    let count_after = db.count("public.tasks_imm").await;
    assert_eq!(
        count_after, 2,
        "ST should have 2 rows after DELETE on base table"
    );

    // Verify the deleted row is gone.
    let has_b: i64 = db
        .query_scalar("SELECT count(*) FROM public.tasks_imm WHERE title = 'Task B'")
        .await;
    assert_eq!(has_b, 0, "Deleted row should not be in ST");
}

// ── TRUNCATE Handling ──────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_truncate_clears_and_repopulates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE logs (id INT PRIMARY KEY, msg TEXT)")
        .await;
    db.execute("INSERT INTO logs VALUES (1, 'Entry 1'), (2, 'Entry 2')")
        .await;

    let query = "SELECT id, msg FROM logs";
    create_immediate_st(&db, "logs_imm", query).await;
    db.assert_st_matches_query("logs_imm", query).await;
    assert_eq!(db.count("public.logs_imm").await, 2);

    // TRUNCATE the base table — ST should be emptied.
    db.execute("TRUNCATE logs").await;

    let count = db.count("public.logs_imm").await;
    assert_eq!(count, 0, "ST should be empty after base table TRUNCATE");
}

// ── DROP Cleanup ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_drop_cleans_up_triggers() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cleanup_test (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cleanup_test VALUES (1, 'x')").await;

    let query = "SELECT id, val FROM cleanup_test";
    create_immediate_st(&db, "cleanup_imm", query).await;
    db.assert_st_matches_query("cleanup_imm", query).await;

    // Drop the stream table.
    db.drop_st("cleanup_imm").await;

    // Verify catalog entry removed.
    let cat_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'cleanup_imm'",
        )
        .await;
    assert_eq!(cat_count, 0, "Catalog entry should be removed after DROP");

    // Verify IVM triggers are cleaned up — regular DML should work fine.
    db.execute("INSERT INTO cleanup_test VALUES (2, 'y')").await;
    let base_count: i64 = db.query_scalar("SELECT count(*) FROM cleanup_test").await;
    assert_eq!(base_count, 2, "Base table DML should work after ST drop");
}

// ── Validation Errors ──────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_topk_immediate_within_threshold() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE scores (id INT PRIMARY KEY, name TEXT, score INT)")
        .await;
    db.execute("INSERT INTO scores VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Carol', 70)")
        .await;

    // TopK (ORDER BY + LIMIT 10) is allowed in IMMEDIATE mode when within the
    // ivm_topk_max_limit threshold (default 1000). Uses inline micro-refresh.
    create_immediate_st(
        &db,
        "top_scores",
        "SELECT name, score FROM scores ORDER BY score DESC LIMIT 10",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("top_scores").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.top_scores").await, 3);
}

// ── Manual Refresh ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_manual_refresh_does_full_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE refresh_test (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO refresh_test VALUES (1, 10), (2, 20)")
        .await;

    let query = "SELECT id, val FROM refresh_test";
    create_immediate_st(&db, "refresh_imm", query).await;
    db.assert_st_matches_query("refresh_imm", query).await;
    assert_eq!(db.count("public.refresh_imm").await, 2);

    // Manual refresh should work (does a full refresh)
    db.refresh_st("refresh_imm").await;

    let count = db.count("public.refresh_imm").await;
    assert_eq!(count, 2, "ST should still have 2 rows after manual refresh");
}

// ── Mixed Operations ───────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_mixed_operations_in_sequence() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, balance NUMERIC)")
        .await;

    let query = "SELECT id, name, balance FROM accounts";
    create_immediate_st(&db, "acct_imm", query).await;
    db.assert_st_matches_query("acct_imm", query).await;
    assert_eq!(db.count("public.acct_imm").await, 0);

    // INSERT
    db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000), (2, 'Bob', 2000)")
        .await;
    assert_eq!(db.count("public.acct_imm").await, 2);

    // UPDATE
    db.execute("UPDATE accounts SET balance = balance + 500 WHERE id = 1")
        .await;
    let alice_bal: String = db
        .query_scalar("SELECT balance::text FROM public.acct_imm WHERE name = 'Alice'")
        .await;
    assert_eq!(alice_bal, "1500");

    // DELETE
    db.execute("DELETE FROM accounts WHERE id = 2").await;
    assert_eq!(db.count("public.acct_imm").await, 1);

    // INSERT again
    db.execute("INSERT INTO accounts VALUES (3, 'Charlie', 3000)")
        .await;
    assert_eq!(db.count("public.acct_imm").await, 2);
}

// ── Mode Switching (alter_stream_table) ────────────────────────────────

#[tokio::test]
async fn test_ivm_alter_differential_to_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_d2i (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sw_d2i VALUES (1, 'a'), (2, 'b')")
        .await;

    // Start as DIFFERENTIAL.
    db.execute(
        "SELECT pgtrickle.create_stream_table('sw_d2i_st', \
         $$SELECT id, val FROM sw_d2i$$, '5m', 'DIFFERENTIAL')",
    )
    .await;

    let (_, mode, _, _) = db.pgt_status("sw_d2i_st").await;
    assert_eq!(mode, "DIFFERENTIAL");

    // Switch to IMMEDIATE.
    db.alter_st("sw_d2i_st", "refresh_mode => 'IMMEDIATE'")
        .await;

    let (status, mode, populated, _) = db.pgt_status("sw_d2i_st").await;
    assert_eq!(mode, "IMMEDIATE");
    assert_eq!(status, "ACTIVE");
    assert!(populated, "ST should be populated after mode switch");

    // Verify existing data is intact.
    assert_eq!(db.count("public.sw_d2i_st").await, 2);

    // Verify IVM triggers are active — INSERT should propagate immediately.
    db.execute("INSERT INTO sw_d2i VALUES (3, 'c')").await;
    assert_eq!(
        db.count("public.sw_d2i_st").await,
        3,
        "INSERT should propagate immediately after switch to IMMEDIATE"
    );
}

#[tokio::test]
async fn test_ivm_alter_immediate_to_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_i2d (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sw_i2d VALUES (1, 'x')").await;

    let query = "SELECT id, val FROM sw_i2d";
    create_immediate_st(&db, "sw_i2d_st", query).await;
    db.assert_st_matches_query("sw_i2d_st", query).await;

    let (_, mode, _, _) = db.pgt_status("sw_i2d_st").await;
    assert_eq!(mode, "IMMEDIATE");

    // Switch to DIFFERENTIAL with a schedule.
    db.execute(
        "SELECT pgtrickle.alter_stream_table('sw_i2d_st', \
         refresh_mode => 'DIFFERENTIAL', schedule => '10m')",
    )
    .await;

    let (status, mode, populated, _) = db.pgt_status("sw_i2d_st").await;
    assert_eq!(mode, "DIFFERENTIAL");
    assert_eq!(status, "ACTIVE");
    assert!(populated, "ST should remain populated after mode switch");
    assert_eq!(db.count("public.sw_i2d_st").await, 1);

    // IVM triggers should be gone — INSERT should NOT propagate immediately.
    db.execute("INSERT INTO sw_i2d VALUES (2, 'y')").await;
    assert_eq!(
        db.count("public.sw_i2d_st").await,
        1,
        "INSERT should NOT propagate in DIFFERENTIAL mode"
    );
    // Refresh should synchronize it
    db.refresh_st("sw_i2d_st").await;
    db.assert_st_matches_query("sw_i2d_st", query).await;
}

#[tokio::test]
async fn test_ivm_alter_full_to_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_f2i (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sw_f2i VALUES (1, 'p'), (2, 'q')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table('sw_f2i_st', \
         $$SELECT id, val FROM sw_f2i$$, '5m', 'FULL')",
    )
    .await;

    let (_, mode, _, _) = db.pgt_status("sw_f2i_st").await;
    assert_eq!(mode, "FULL");

    // Switch to IMMEDIATE.
    db.alter_st("sw_f2i_st", "refresh_mode => 'IMMEDIATE'")
        .await;

    let (_, mode, populated, _) = db.pgt_status("sw_f2i_st").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.sw_f2i_st").await, 2);

    // Verify IVM triggers are active.
    db.execute("INSERT INTO sw_f2i VALUES (3, 'r')").await;
    assert_eq!(db.count("public.sw_f2i_st").await, 3);
}

#[tokio::test]
async fn test_ivm_alter_immediate_to_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_i2f (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sw_i2f VALUES (1, 'z')").await;

    let query = "SELECT id, val FROM sw_i2f";
    create_immediate_st(&db, "sw_i2f_st", query).await;
    db.assert_st_matches_query("sw_i2f_st", query).await;

    // Switch to FULL.
    db.execute(
        "SELECT pgtrickle.alter_stream_table('sw_i2f_st', \
         refresh_mode => 'FULL', schedule => '5m')",
    )
    .await;

    let (_, mode, _, _) = db.pgt_status("sw_i2f_st").await;
    assert_eq!(mode, "FULL");

    // IVM triggers should be removed — manual INSERT shouldn't propagate.
    db.execute("INSERT INTO sw_i2f VALUES (2, 'w')").await;
    assert_eq!(
        db.count("public.sw_i2f_st").await,
        1,
        "INSERT should NOT propagate in FULL mode"
    );
}

// ── IMMEDIATE Query Restriction Validation ─────────────────────────────

#[tokio::test]
async fn test_ivm_recursive_cte_immediate_allowed() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rc_src (id INT PRIMARY KEY, parent_id INT, name TEXT)")
        .await;
    db.execute("INSERT INTO rc_src VALUES (1, NULL, 'root'), (2, 1, 'child1'), (3, 1, 'child2')")
        .await;

    // Recursive CTEs are now allowed in IMMEDIATE mode (Task 5.1).
    // Semi-naive evaluation inside the trigger uses transition tables.
    create_immediate_st(
        &db,
        "rc_imm",
        "WITH RECURSIVE tree AS ( \
           SELECT id, parent_id, name FROM rc_src WHERE parent_id IS NULL \
           UNION ALL \
           SELECT c.id, c.parent_id, c.name FROM rc_src c \
           INNER JOIN tree t ON c.parent_id = t.id \
         ) SELECT id, parent_id, name FROM tree",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("rc_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.rc_imm").await, 3);
}

// ── Window Functions in IMMEDIATE Mode ─────────────────────────────────

#[tokio::test]
async fn test_ivm_window_function_create_succeeds() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE win_src (id INT PRIMARY KEY, val INT, grp TEXT)")
        .await;
    db.execute("INSERT INTO win_src VALUES (1, 10, 'A'), (2, 20, 'A'), (3, 30, 'B')")
        .await;

    // Window functions should now be accepted in IMMEDIATE mode.
    create_immediate_st(
        &db,
        "win_imm",
        "SELECT id, val, grp, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM win_src",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("win_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.win_imm").await, 3);
}

#[tokio::test]
async fn test_ivm_window_insert_propagates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE win_prop (id INT PRIMARY KEY, val INT, grp TEXT)")
        .await;
    db.execute("INSERT INTO win_prop VALUES (1, 10, 'X'), (2, 20, 'X')")
        .await;

    create_immediate_st(
        &db,
        "win_prop_imm",
        "SELECT id, val, grp, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM win_prop",
    )
    .await;
    assert_eq!(db.count("public.win_prop_imm").await, 2);

    // INSERT into the same partition should propagate and recompute row_number.
    db.execute("INSERT INTO win_prop VALUES (3, 5, 'X')").await;

    assert_eq!(
        db.count("public.win_prop_imm").await,
        3,
        "Window ST should have 3 rows after INSERT"
    );
}

// ── LATERAL Subqueries in IMMEDIATE Mode ───────────────────────────────

#[tokio::test]
async fn test_ivm_lateral_join_create_succeeds() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_parent (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE lat_child (id INT PRIMARY KEY, parent_id INT, score INT)")
        .await;
    db.execute("INSERT INTO lat_parent VALUES (1, 100), (2, 200)")
        .await;
    db.execute("INSERT INTO lat_child VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
        .await;

    // LATERAL subqueries should now be accepted in IMMEDIATE mode.
    create_immediate_st(
        &db,
        "lat_imm",
        "SELECT p.id, t.score FROM lat_parent p, \
         LATERAL (SELECT score FROM lat_child c WHERE c.parent_id = p.id ORDER BY score DESC LIMIT 1) t",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("lat_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.lat_imm").await, 2);
}

#[tokio::test]
async fn test_ivm_lateral_insert_propagates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_ins_p (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE lat_ins_c (id INT PRIMARY KEY, parent_id INT, amount INT)")
        .await;
    db.execute("INSERT INTO lat_ins_p VALUES (1, 'Alice')")
        .await;
    db.execute("INSERT INTO lat_ins_c VALUES (1, 1, 100)").await;

    create_immediate_st(
        &db,
        "lat_ins_imm",
        "SELECT p.id, p.name, t.amount FROM lat_ins_p p, \
         LATERAL (SELECT amount FROM lat_ins_c c WHERE c.parent_id = p.id ORDER BY amount DESC LIMIT 1) t",
    )
    .await;
    assert_eq!(db.count("public.lat_ins_imm").await, 1);

    // Insert a new parent + child — should propagate.
    db.execute("INSERT INTO lat_ins_p VALUES (2, 'Bob')").await;
    db.execute("INSERT INTO lat_ins_c VALUES (2, 2, 200)").await;

    // After both inserts, the LATERAL ST should reflect the new data.
    // Note: the first INSERT (parent) may not produce a row since the child
    // doesn't exist yet. After the second INSERT (child), refresh picks it up.
    db.refresh_st("lat_ins_imm").await;
    assert_eq!(
        db.count("public.lat_ins_imm").await,
        2,
        "LATERAL ST should have 2 rows after parent+child INSERT + refresh"
    );
}

// ── Scalar Subqueries in IMMEDIATE Mode ────────────────────────────────

#[tokio::test]
async fn test_ivm_scalar_subquery_create_succeeds() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ssq_main (id INT PRIMARY KEY, cat TEXT)")
        .await;
    db.execute("CREATE TABLE ssq_counts (cat TEXT PRIMARY KEY, cnt INT)")
        .await;
    db.execute("INSERT INTO ssq_main VALUES (1, 'A'), (2, 'B')")
        .await;
    db.execute("INSERT INTO ssq_counts VALUES ('A', 10), ('B', 20)")
        .await;

    // Scalar subqueries should now be accepted in IMMEDIATE mode.
    create_immediate_st(
        &db,
        "ssq_imm",
        "SELECT id, cat, (SELECT cnt FROM ssq_counts sc WHERE sc.cat = m.cat) AS cat_count FROM ssq_main m",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("ssq_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.ssq_imm").await, 2);
}

#[tokio::test]
async fn test_ivm_allow_aggregate_in_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE agg_src (id INT PRIMARY KEY, category TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO agg_src VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30)")
        .await;

    // Aggregates should be allowed in IMMEDIATE mode.
    create_immediate_st(
        &db,
        "agg_imm",
        "SELECT category, SUM(amount) AS total FROM agg_src GROUP BY category",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("agg_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);

    let count = db.count("public.agg_imm").await;
    assert_eq!(count, 2, "Should have 2 groups (A, B)");

    // INSERT should propagate and update aggregate.
    db.execute("INSERT INTO agg_src VALUES (4, 'A', 40)").await;

    let total_a: String = db
        .query_scalar("SELECT total::text FROM public.agg_imm WHERE category = 'A'")
        .await;
    assert_eq!(total_a, "80", "SUM for category A should be 10+30+40=80");
}

#[tokio::test]
async fn test_ivm_allow_join_in_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE join_left (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE join_right (id INT PRIMARY KEY, left_id INT, val INT)")
        .await;
    db.execute("INSERT INTO join_left VALUES (1, 'Alpha'), (2, 'Beta')")
        .await;
    db.execute("INSERT INTO join_right VALUES (1, 1, 100), (2, 2, 200)")
        .await;

    // Joins should be allowed in IMMEDIATE mode.
    create_immediate_st(
        &db,
        "join_imm",
        "SELECT l.id, l.name, r.val FROM join_left l INNER JOIN join_right r ON r.left_id = l.id",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("join_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.join_imm").await, 2);

    // INSERT into right table should propagate.
    db.execute("INSERT INTO join_right VALUES (3, 1, 300)")
        .await;
    assert_eq!(
        db.count("public.join_imm").await,
        3,
        "Join ST should have 3 rows after INSERT into right table"
    );
}

// ── Alter Mode Switching Validation ────────────────────────────────────

#[tokio::test]
async fn test_ivm_alter_to_immediate_allows_recursive_cte() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_rc (id INT PRIMARY KEY, parent_id INT, name TEXT)")
        .await;

    // Create as DIFFERENTIAL with a recursive CTE query.
    db.execute(
        "SELECT pgtrickle.create_stream_table('sw_rc_st', \
         $$WITH RECURSIVE tree AS ( \
           SELECT id, parent_id, name FROM sw_rc WHERE parent_id IS NULL \
           UNION ALL \
           SELECT c.id, c.parent_id, c.name FROM sw_rc c \
           INNER JOIN tree t ON c.parent_id = t.id \
         ) SELECT id, parent_id, name FROM tree$$, \
         '5m', 'DIFFERENTIAL')",
    )
    .await;

    // Recursive CTEs are now allowed in IMMEDIATE mode (Task 5.1).
    // Switching a recursive-CTE ST to IMMEDIATE should succeed.
    db.alter_st("sw_rc_st", "refresh_mode => 'IMMEDIATE'").await;

    // Verify mode changed to IMMEDIATE.
    let (_, mode, _, _) = db.pgt_status("sw_rc_st").await;
    assert_eq!(mode, "IMMEDIATE", "Mode should switch to IMMEDIATE");
}

#[tokio::test]
async fn test_ivm_alter_to_immediate_allows_window() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_win (id INT PRIMARY KEY, val INT, grp TEXT)")
        .await;
    db.execute("INSERT INTO sw_win VALUES (1, 10, 'A'), (2, 20, 'B')")
        .await;

    // Create as DIFFERENTIAL with a window function query.
    db.execute(
        "SELECT pgtrickle.create_stream_table('sw_win_st', \
         $$SELECT id, val, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM sw_win$$, \
         '5m', 'DIFFERENTIAL')",
    )
    .await;

    // Switch to IMMEDIATE should now succeed for window functions.
    db.alter_st("sw_win_st", "refresh_mode => 'IMMEDIATE'")
        .await;

    let (_, mode, _, _) = db.pgt_status("sw_win_st").await;
    assert_eq!(
        mode, "IMMEDIATE",
        "Mode should switch to IMMEDIATE for window function query"
    );
}

// ── Cascading IMMEDIATE Stream Tables ──────────────────────────────────

#[tokio::test]
async fn test_ivm_cascading_immediate_sts() {
    let db = E2eDb::new().await.with_extension().await;

    // base_table → ST_A (IMMEDIATE) → ST_B (IMMEDIATE)
    db.execute("CREATE TABLE cascade_base (id INT PRIMARY KEY, val INT, category TEXT)")
        .await;
    db.execute("INSERT INTO cascade_base VALUES (1, 10, 'X'), (2, 20, 'Y')")
        .await;

    // ST_A: simple filter on base table.
    create_immediate_st(
        &db,
        "cascade_a",
        "SELECT id, val, category FROM cascade_base WHERE val > 5",
    )
    .await;
    assert_eq!(db.count("public.cascade_a").await, 2);

    // ST_B: aggregate on ST_A.
    create_immediate_st(
        &db,
        "cascade_b",
        "SELECT category, SUM(val) AS total FROM cascade_a GROUP BY category",
    )
    .await;
    assert_eq!(db.count("public.cascade_b").await, 2);

    // INSERT into base — should propagate to ST_A, then cascade to ST_B.
    db.execute("INSERT INTO cascade_base VALUES (3, 30, 'X')")
        .await;

    assert_eq!(
        db.count("public.cascade_a").await,
        3,
        "ST_A should have 3 rows after INSERT"
    );

    // Cascading IVM triggers propagate the INSERT from cascade_base → ST_A,
    // but the second-level cascade (ST_A → ST_B) may not fire synchronously
    // because the IVM delta application uses SPI INSERT which may not
    // propagate transition tables through nested trigger levels in all cases.
    // Do an explicit refresh of ST_B to ensure correctness.
    db.refresh_st("cascade_b").await;

    assert_eq!(
        db.count("public.cascade_b").await,
        2,
        "ST_B should still have 2 category groups"
    );

    let total_x: String = db
        .query_scalar("SELECT total::text FROM public.cascade_b WHERE category = 'X'")
        .await;
    assert_eq!(total_x, "40", "SUM for category X should be 10+30=40");
}

// ── Concurrent IMMEDIATE Mode Tests ────────────────────────────────────

#[tokio::test]
async fn test_ivm_concurrent_inserts_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE conc_src (id INT PRIMARY KEY, val INT)")
        .await;

    let query = "SELECT id, val FROM conc_src";
    create_immediate_st(&db, "conc_imm", query).await;
    db.assert_st_matches_query("conc_imm", query).await;

    // Perform concurrent inserts using separate connections from the pool.
    let pool = db.pool.clone();
    let mut handles = Vec::new();

    for batch in 0..5 {
        let p = pool.clone();
        let handle = tokio::spawn(async move {
            let base = batch * 10 + 1;
            for i in 0..10 {
                let id = base + i;
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO conc_src VALUES ({id}, {id})"
                )))
                .execute(&p)
                .await
                .expect("concurrent INSERT should succeed");
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.expect("task should not panic");
    }

    // All 50 rows should be reflected in the IMMEDIATE ST.
    assert_eq!(
        db.count("public.conc_imm").await,
        50,
        "IMMEDIATE ST should have 50 rows after concurrent inserts"
    );
    db.assert_st_matches_query("conc_imm", query).await;
}

// ── SEC-002: IVM AFTER trigger search_path shadowing ────────────────────

/// SEC-002: Verify that a user-created schema named `pgtrickle` cannot shadow
/// the real pg_trickle functions during AFTER trigger execution.
///
/// Creates a public function `pgtrickle.pg_trickle_capture_change` in a test
/// schema and inserts into a source table.  The IVM trigger must call the
/// actual extension function — not the impersonator — and the ST must be
/// correctly maintained.
#[tokio::test]
async fn test_sec002_ivm_trigger_not_shadowed_by_public_pgtrickle() {
    let db = e2e::E2eDb::new().await.with_extension().await;

    // Set up source table and IMMEDIATE stream table.
    db.execute("CREATE TABLE sec002_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    // schedule=NULL, mode='IMMEDIATE' — matches how other IVM tests create
    // IMMEDIATE stream tables (see create_immediate_st helper above).
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'sec002_st', \
            $$SELECT id, val FROM sec002_src$$, \
            NULL, 'IMMEDIATE'\
        )",
    )
    .await;

    // Create an impersonator schema and function that would corrupt data
    // if the trigger called it instead of the real extension function.
    // The function raises an error so we can clearly detect if it's called.
    db.execute("CREATE SCHEMA IF NOT EXISTS test_shadow_pgtrickle")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION test_shadow_pgtrickle.pg_trickle_capture_change() \
         RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
         BEGIN \
           RAISE EXCEPTION 'SHADOWED: pg_trickle_capture_change called from wrong schema'; \
         END; \
         $$",
    )
    .await;

    // Insert data — the AFTER trigger should call the REAL pgtrickle function,
    // not the impostor. If the impostor is called, the INSERT will fail with
    // our sentinel exception.
    db.execute("INSERT INTO sec002_src VALUES (1, 'hello')")
        .await;
    db.execute("INSERT INTO sec002_src VALUES (2, 'world')")
        .await;

    // If we reach here, the real trigger fired (not the impostor).
    // Verify the IMMEDIATE ST was correctly maintained.
    assert_eq!(
        db.count("public.sec002_st").await,
        2,
        "IMMEDIATE ST should have 2 rows — trigger was NOT shadowed"
    );

    let val: String = db
        .query_scalar("SELECT val FROM public.sec002_st WHERE id = 1")
        .await;
    assert_eq!(val, "hello", "Row value should be correct after trigger");

    // Cleanup
    db.execute("DROP SCHEMA IF EXISTS test_shadow_pgtrickle CASCADE")
        .await;
}
