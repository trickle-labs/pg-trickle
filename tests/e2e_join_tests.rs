mod e2e;
use e2e::E2eDb;
#[tokio::test]
async fn test_full_join_multi_row_unmatched() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fj_multi_left (id INT, val TEXT)")
        .await;
    db.execute("CREATE TABLE fj_multi_right (id INT, val TEXT)")
        .await;

    db.execute("INSERT INTO fj_multi_left VALUES (1, 'L1'), (2, 'L2')")
        .await;
    db.execute("INSERT INTO fj_multi_right VALUES (3, 'R3'), (4, 'R4'), (5, 'R5')")
        .await;

    let q = "SELECT l.id as lid, r.id as rid FROM fj_multi_left l FULL JOIN fj_multi_right r ON l.id = r.id";

    db.create_st("fj_multi_st", q, "1m", "DIFFERENTIAL").await;

    db.assert_st_matches_query("fj_multi_st", q).await;

    // Add match
    db.execute("INSERT INTO fj_multi_left VALUES (3, 'L3')")
        .await;
    db.refresh_st("fj_multi_st").await;

    db.assert_st_matches_query("fj_multi_st", q).await;
}

// ── TG2-JOIN: Multi-cycle UPDATE/DELETE correctness ──────────────────

/// INNER JOIN: source row updated → stream table reflects new join value.
#[tokio::test]
async fn test_inner_join_update_propagation() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ij_orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
        .await;
    db.execute("CREATE TABLE ij_customers (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO ij_customers VALUES (1, 'Alice'), (2, 'Bob')")
        .await;
    db.execute("INSERT INTO ij_orders VALUES (10, 1, 100), (11, 2, 200), (12, 1, 50)")
        .await;

    let q = "SELECT o.id AS oid, c.name, o.amount \
             FROM ij_orders o JOIN ij_customers c ON o.customer_id = c.id";
    db.create_st("ij_upd_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("ij_upd_st", q).await;

    // UPDATE: change customer name → reflected in join result
    db.execute("UPDATE ij_customers SET name = 'ALICE' WHERE id = 1")
        .await;
    db.refresh_st("ij_upd_st").await;
    db.assert_st_matches_query("ij_upd_st", q).await;

    // UPDATE: change join key on order → row moves to different customer
    db.execute("UPDATE ij_orders SET customer_id = 2 WHERE id = 12")
        .await;
    db.refresh_st("ij_upd_st").await;
    db.assert_st_matches_query("ij_upd_st", q).await;

    // DELETE: remove an order → row disappears from join
    db.execute("DELETE FROM ij_orders WHERE id = 10").await;
    db.refresh_st("ij_upd_st").await;
    db.assert_st_matches_query("ij_upd_st", q).await;

    // DELETE: remove a customer with remaining orders → their orders leave join
    db.execute("DELETE FROM ij_customers WHERE id = 2").await;
    db.refresh_st("ij_upd_st").await;
    db.assert_st_matches_query("ij_upd_st", q).await;
}

/// LEFT JOIN: right-side DELETE → stream table row transitions to NULL.
#[tokio::test]
async fn test_left_join_right_delete_null_transition() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE lj_dept (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE lj_emp (id INT PRIMARY KEY, dept_id INT, emp_name TEXT)")
        .await;
    db.execute("INSERT INTO lj_dept VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')")
        .await;
    db.execute("INSERT INTO lj_emp VALUES (10, 1, 'Alice'), (11, 2, 'Bob'), (12, 1, 'Carol')")
        .await;

    let q = "SELECT d.id AS did, d.name AS dept_name, e.emp_name \
             FROM lj_dept d LEFT JOIN lj_emp e ON e.dept_id = d.id";
    db.create_st("lj_del_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("lj_del_st", q).await;

    // DELETE: remove all employees from Engineering → dept rows get NULL emp_name
    db.execute("DELETE FROM lj_emp WHERE dept_id = 1").await;
    db.refresh_st("lj_del_st").await;
    db.assert_st_matches_query("lj_del_st", q).await;

    // UPDATE: move Bob to Marketing
    db.execute("UPDATE lj_emp SET dept_id = 3 WHERE id = 11")
        .await;
    db.refresh_st("lj_del_st").await;
    db.assert_st_matches_query("lj_del_st", q).await;

    // INSERT: add employee to empty dept
    db.execute("INSERT INTO lj_emp VALUES (13, 2, 'Dave')")
        .await;
    db.refresh_st("lj_del_st").await;
    db.assert_st_matches_query("lj_del_st", q).await;
}

/// FULL JOIN: both-side UPDATE in same cycle → no phantom rows.
#[tokio::test]
async fn test_full_join_both_side_update() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fj_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE fj_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO fj_a VALUES (1, 'A1'), (2, 'A2'), (3, 'A3')")
        .await;
    db.execute("INSERT INTO fj_b VALUES (2, 'B2'), (3, 'B3'), (4, 'B4')")
        .await;

    let q = "SELECT a.id AS aid, a.val AS aval, b.id AS bid, b.val AS bval \
             FROM fj_a a FULL JOIN fj_b b ON a.id = b.id";
    db.create_st("fj_both_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fj_both_st", q).await;

    // Both-side UPDATE in same cycle: update matched rows on both sides
    db.execute("UPDATE fj_a SET val = 'A2-updated' WHERE id = 2")
        .await;
    db.execute("UPDATE fj_b SET val = 'B2-updated' WHERE id = 2")
        .await;
    db.refresh_st("fj_both_st").await;
    db.assert_st_matches_query("fj_both_st", q).await;

    // DELETE from left side → previously matched row becomes right-only
    db.execute("DELETE FROM fj_a WHERE id = 3").await;
    db.refresh_st("fj_both_st").await;
    db.assert_st_matches_query("fj_both_st", q).await;

    // INSERT + DELETE in same cycle
    db.execute("INSERT INTO fj_a VALUES (4, 'A4')").await;
    db.execute("DELETE FROM fj_b WHERE id = 4").await;
    db.refresh_st("fj_both_st").await;
    db.assert_st_matches_query("fj_both_st", q).await;
}

/// INNER JOIN with large stream table: simultaneous UPDATE on both sides forces
/// PH-D1 strategy (ratio < 1%) and must produce correct results. This is the
/// G17-SOAK soak_join scenario that triggered a duplicate-key constraint error.
#[tokio::test]
async fn test_inner_join_simultaneous_both_sides_update_large() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE ij_big_left (
            id SERIAL PRIMARY KEY,
            category INT NOT NULL,
            value NUMERIC(12,2) NOT NULL,
            label TEXT NOT NULL
        )",
    )
    .await;
    db.execute(
        "CREATE TABLE ij_big_right (
            id SERIAL PRIMARY KEY,
            category INT NOT NULL,
            value NUMERIC(12,2) NOT NULL,
            label TEXT NOT NULL
        )",
    )
    .await;

    // Insert 100 rows per source with 5 categories (20 rows/cat each).
    // Join output: 20×20×5 = 2000 rows. With 10 changes → ratio 0.5% < 1% → PH-D1.
    db.execute(
        "INSERT INTO ij_big_left (category, value, label)
         SELECT (g % 5), (g * 10)::numeric(12,2), 'L' || g
         FROM generate_series(1, 100) g",
    )
    .await;
    db.execute(
        "INSERT INTO ij_big_right (category, value, label)
         SELECT (g % 5), (g * 10)::numeric(12,2), 'R' || g
         FROM generate_series(1, 100) g",
    )
    .await;

    let q = "SELECT l.id, l.category, l.value, r.label AS label_r
             FROM ij_big_left l
             JOIN ij_big_right r ON l.category = r.category";
    let expected_q = q;
    db.create_st("ij_big_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("ij_big_st", expected_q).await;

    // Round 1: UPDATE 5 rows in left AND 5 rows in right simultaneously.
    // Both sources have changes in the same refresh cycle.
    db.execute("UPDATE ij_big_left SET value = value + 100, label = label || '_u' WHERE id <= 5")
        .await;
    db.execute("UPDATE ij_big_right SET value = value + 100, label = label || '_u' WHERE id <= 5")
        .await;
    db.refresh_st("ij_big_st").await;
    db.assert_st_matches_query("ij_big_st", expected_q).await;

    // Round 2: UPDATE different rows simultaneously.
    db.execute(
        "UPDATE ij_big_left SET value = value + 50, label = label || '_v' WHERE id BETWEEN 10 AND 15",
    )
    .await;
    db.execute(
        "UPDATE ij_big_right SET value = value + 50, label = label || '_v' WHERE id BETWEEN 10 AND 15",
    )
    .await;
    db.refresh_st("ij_big_st").await;
    db.assert_st_matches_query("ij_big_st", expected_q).await;

    // Round 3: INSERT new rows into BOTH sources + UPDATE existing rows.
    db.execute(
        "INSERT INTO ij_big_left (category, value, label)
         SELECT (g % 5), (g * 7)::numeric(12,2), 'NewL' || g
         FROM generate_series(1, 10) g",
    )
    .await;
    db.execute(
        "INSERT INTO ij_big_right (category, value, label)
         SELECT (g % 5), (g * 7)::numeric(12,2), 'NewR' || g
         FROM generate_series(1, 10) g",
    )
    .await;
    db.execute("UPDATE ij_big_left SET value = value + 25 WHERE id BETWEEN 20 AND 25")
        .await;
    db.execute("UPDATE ij_big_right SET value = value + 25 WHERE id BETWEEN 20 AND 25")
        .await;
    db.refresh_st("ij_big_st").await;
    db.assert_st_matches_query("ij_big_st", expected_q).await;

    // Round 4: Multi-cycle stress — UPDATE same rows multiple times in sequence.
    for i in 1..=5_u32 {
        db.execute(&format!(
            "UPDATE ij_big_left SET value = value + {i}, label = label || '.{i}'
             WHERE id BETWEEN 1 AND 10"
        ))
        .await;
        db.execute(&format!(
            "UPDATE ij_big_right SET value = value + {i}, label = label || '.{i}'
             WHERE id BETWEEN 1 AND 10"
        ))
        .await;
        db.refresh_st("ij_big_st").await;
        db.assert_st_matches_query("ij_big_st", expected_q).await;
    }
}

/// 3-table join chain: delete from middle table → correct propagation.
#[tokio::test]
async fn test_three_table_join_middle_delete() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE tj_region (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE tj_store (id INT PRIMARY KEY, region_id INT, store_name TEXT)")
        .await;
    db.execute("CREATE TABLE tj_sale (id INT PRIMARY KEY, store_id INT, amount INT)")
        .await;
    db.execute("INSERT INTO tj_region VALUES (1, 'North'), (2, 'South')")
        .await;
    db.execute(
        "INSERT INTO tj_store VALUES (10, 1, 'Store-A'), (11, 1, 'Store-B'), (12, 2, 'Store-C')",
    )
    .await;
    db.execute(
        "INSERT INTO tj_sale VALUES (100, 10, 500), (101, 11, 300), (102, 12, 700), (103, 10, 200)",
    )
    .await;

    let q = "SELECT r.name AS region, s.store_name, sa.amount \
             FROM tj_region r \
             JOIN tj_store s ON s.region_id = r.id \
             JOIN tj_sale sa ON sa.store_id = s.id";
    db.create_st("tj_mid_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("tj_mid_st", q).await;

    // DELETE from middle table: remove Store-A → its sales disappear
    db.execute("DELETE FROM tj_store WHERE id = 10").await;
    db.refresh_st("tj_mid_st").await;
    db.assert_st_matches_query("tj_mid_st", q).await;

    // UPDATE middle table: move Store-B to South region
    db.execute("UPDATE tj_store SET region_id = 2 WHERE id = 11")
        .await;
    db.refresh_st("tj_mid_st").await;
    db.assert_st_matches_query("tj_mid_st", q).await;

    // DELETE from leaf: remove a sale
    db.execute("DELETE FROM tj_sale WHERE id = 102").await;
    db.refresh_st("tj_mid_st").await;
    db.assert_st_matches_query("tj_mid_st", q).await;

    // INSERT new chain: new store + sale
    db.execute("INSERT INTO tj_store VALUES (13, 1, 'Store-D')")
        .await;
    db.execute("INSERT INTO tj_sale VALUES (104, 13, 999)")
        .await;
    db.refresh_st("tj_mid_st").await;
    db.assert_st_matches_query("tj_mid_st", q).await;
}

// ── TEST-1: JOIN delta R₀ co-delete tests (CORR-2) ─────────────────────
//
// Validate the co-delete scenario: UPDATE join key + DELETE join partner
// in the same refresh cycle. These verify that the R₀ fix correctly
// handles simultaneous key reassignment and partner removal.

/// TEST-1a: Simultaneous key change + right-side delete in same cycle.
///
/// Scenario: Order 10 changes customer from Alice to Charlie,
/// and Bob (customer 2) is deleted, all before a single refresh.
#[tokio::test]
async fn test_join_r0_key_change_plus_partner_delete() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE r0_orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
        .await;
    db.execute("CREATE TABLE r0_customers (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO r0_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await;
    db.execute("INSERT INTO r0_orders VALUES (10, 1, 100), (11, 2, 200)")
        .await;

    let q = "SELECT o.id AS oid, c.name, o.amount \
             FROM r0_orders o JOIN r0_customers c ON o.customer_id = c.id";
    db.create_st("r0_codelete_st", q, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("r0_codelete_st", q).await;

    // Same cycle: reassign order 10 to Charlie + delete Bob
    db.execute("UPDATE r0_orders SET customer_id = 3 WHERE id = 10")
        .await;
    db.execute("DELETE FROM r0_customers WHERE id = 2").await;
    db.refresh_st("r0_codelete_st").await;
    db.assert_st_matches_query("r0_codelete_st", q).await;
}

/// TEST-1b: UPDATE key + DELETE multiple right-side rows in same cycle.
///
/// Scenario: An order switches customer while two other customers are
/// deleted simultaneously, removing their associated order rows.
#[tokio::test]
async fn test_join_r0_key_change_plus_multi_partner_delete() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE r0m_orders (id INT PRIMARY KEY, cust_id INT, amt INT)")
        .await;
    db.execute("CREATE TABLE r0m_custs (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO r0m_custs VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')")
        .await;
    db.execute("INSERT INTO r0m_orders VALUES (10, 1, 10), (11, 2, 20), (12, 3, 30), (13, 4, 40)")
        .await;

    let q = "SELECT o.id AS oid, c.name, o.amt \
             FROM r0m_orders o JOIN r0m_custs c ON o.cust_id = c.id";
    db.create_st("r0m_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("r0m_st", q).await;

    // Same cycle: order 10 moves to D, delete customers B and C
    db.execute("UPDATE r0m_orders SET cust_id = 4 WHERE id = 10")
        .await;
    db.execute("DELETE FROM r0m_custs WHERE id IN (2, 3)").await;
    db.refresh_st("r0m_st").await;
    db.assert_st_matches_query("r0m_st", q).await;
}

/// TEST-1c: Multi-cycle correctness after R₀ co-delete scenario.
///
/// Verifies that subsequent refresh cycles remain correct after an
/// initial co-delete cycle (no stale state lingers in change buffers).
#[tokio::test]
async fn test_join_r0_multi_cycle_after_codelete() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE r0c_items (id INT PRIMARY KEY, cat_id INT, val INT)")
        .await;
    db.execute("CREATE TABLE r0c_cats (id INT PRIMARY KEY, label TEXT)")
        .await;
    db.execute("INSERT INTO r0c_cats VALUES (1, 'X'), (2, 'Y'), (3, 'Z')")
        .await;
    db.execute("INSERT INTO r0c_items VALUES (10, 1, 100), (11, 2, 200)")
        .await;

    let q = "SELECT i.id AS iid, c.label, i.val \
             FROM r0c_items i JOIN r0c_cats c ON i.cat_id = c.id";
    db.create_st("r0c_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("r0c_st", q).await;

    // Cycle 1: co-delete — move item 10 to Z, delete Y
    db.execute("UPDATE r0c_items SET cat_id = 3 WHERE id = 10")
        .await;
    db.execute("DELETE FROM r0c_cats WHERE id = 2").await;
    db.refresh_st("r0c_st").await;
    db.assert_st_matches_query("r0c_st", q).await;

    // Cycle 2: normal insert
    db.execute("INSERT INTO r0c_cats VALUES (4, 'W')").await;
    db.execute("INSERT INTO r0c_items VALUES (12, 4, 300)")
        .await;
    db.refresh_st("r0c_st").await;
    db.assert_st_matches_query("r0c_st", q).await;

    // Cycle 3: update + delete again
    db.execute("UPDATE r0c_items SET val = 999 WHERE id = 10")
        .await;
    db.execute("DELETE FROM r0c_items WHERE id = 12").await;
    db.refresh_st("r0c_st").await;
    db.assert_st_matches_query("r0c_st", q).await;
}
