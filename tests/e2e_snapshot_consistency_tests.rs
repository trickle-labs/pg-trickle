//! E2E tests for cross-source snapshot consistency (Category K).
//!
//! Validates that stream tables built from multiple sources produce
//! consistent snapshots after mutations and refreshes. This includes:
//!
//! - K1: Two STs on overlapping sources produce consistent join results
//! - K2: Diamond topology: two paths from shared source converge correctly
//! - K3: Interleaved mutations across sources + serial refreshes converge
//! - K4: Atomic diamond consistency mode produces a consistent snapshot
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_snapshot_restore_after_cdc_cleanup_reinitializes_from_sources() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE snap_restore_source (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO snap_restore_source VALUES (1)")
        .await;
    let query = "SELECT id FROM snap_restore_source";
    db.create_st("snap_restore_st", query, "1m", "DIFFERENTIAL")
        .await;

    let snapshot: String = db
        .query_scalar("SELECT pgtrickle.snapshot_stream_table('snap_restore_st')")
        .await;
    db.execute("INSERT INTO snap_restore_source VALUES (2)")
        .await;
    // A successful full refresh consumes the change and may remove its CDC
    // history, leaving the older snapshot frontier unsafe to restore.
    db.execute_seq(&[
        "SET pg_trickle.internal_refresh = 'true'",
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = true \
         WHERE pgt_name = 'snap_restore_st'",
        "RESET pg_trickle.internal_refresh",
    ])
    .await;
    db.refresh_st("snap_restore_st").await;

    let source_oid = db.table_oid("snap_restore_source").await;
    let buffer: String = db
        .query_scalar(&format!(
            "SELECT format('%I.%I', current_setting('pg_trickle.change_buffer_schema'), \
             'changes_'||coalesce(source_stable_name,source_relid::text)) \
             FROM pgtrickle.pgt_change_tracking WHERE source_relid={source_oid}"
        ))
        .await;
    let retained_changes: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {buffer} WHERE action IN ('I', 'D', 'T')"
        ))
        .await;
    assert_eq!(
        retained_changes, 0,
        "the refresh should have cleaned its CDC rows"
    );

    db.execute(&format!(
        "SELECT pgtrickle.restore_from_snapshot('snap_restore_st', '{snapshot}')"
    ))
    .await;
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'snap_restore_st'",
        )
        .await;
    assert!(
        needs_reinit,
        "restoring behind cleaned CDC history must reinitialize"
    );

    db.refresh_st("snap_restore_st").await;
    db.assert_st_matches_query("public.snap_restore_st", query)
        .await;
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'snap_restore_st'",
        )
        .await;
    assert!(
        !needs_reinit,
        "the successful full refresh should clear reinit state"
    );
}

// ══════════════════════════════════════════════════════════════════════
// K1 — Two STs on overlapping sources produce consistent results
// ══════════════════════════════════════════════════════════════════════

/// Two stream tables reading from the same pair of sources via different
/// join paths. After mutations and refresh, both STs must reflect the
/// same underlying data and produce multiset-equal results with their
/// respective defining queries.
#[tokio::test]
async fn test_cross_source_two_st_overlapping_sources() {
    let db = E2eDb::new().await.with_extension().await;

    // Two source tables with a foreign-key relationship
    db.execute("CREATE TABLE ks_orders (id INT PRIMARY KEY, customer TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "CREATE TABLE ks_items (id INT PRIMARY KEY, order_id INT REFERENCES ks_orders(id), \
         product TEXT, qty INT)",
    )
    .await;

    db.execute(
        "INSERT INTO ks_orders VALUES \
         (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)",
    )
    .await;
    db.execute(
        "INSERT INTO ks_items VALUES \
         (1, 1, 'Widget', 2), (2, 1, 'Gadget', 1), \
         (3, 2, 'Widget', 5), (4, 3, 'Doohickey', 3)",
    )
    .await;

    // ST-A: total amount per customer (from orders only)
    let q_a = "SELECT customer, SUM(amount) AS total FROM ks_orders GROUP BY customer";
    db.create_st("ks_st_a", q_a, "1m", "DIFFERENTIAL").await;

    // ST-B: order details with items (join of both sources)
    let q_b = "SELECT o.id AS order_id, o.customer, i.product, i.qty \
               FROM ks_orders o JOIN ks_items i ON o.id = i.order_id";
    db.create_st("ks_st_b", q_b, "1m", "DIFFERENTIAL").await;

    // Verify initial state
    assert_eq!(db.count("public.ks_st_a").await, 3); // 3 customers
    assert_eq!(db.count("public.ks_st_b").await, 4); // 4 item rows

    // Mutate both sources
    db.execute("UPDATE ks_orders SET amount = 150 WHERE id = 1")
        .await;
    db.execute("INSERT INTO ks_orders VALUES (4, 'Diana', 400)")
        .await;
    db.execute("INSERT INTO ks_items VALUES (5, 4, 'Sprocket', 10)")
        .await;
    db.execute("DELETE FROM ks_items WHERE id = 2").await; // Remove Gadget

    // Refresh both STs
    db.refresh_st("ks_st_a").await;
    db.refresh_st("ks_st_b").await;

    // Both must match their defining queries exactly
    db.assert_st_matches_query("public.ks_st_a", q_a).await;
    db.assert_st_matches_query("public.ks_st_b", q_b).await;

    // Cross-check: the set of customers in ST-A must be a superset of
    // those in ST-B (every order with items has a customer row).
    let orphan_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM ( \
                SELECT DISTINCT customer FROM public.ks_st_b \
                EXCEPT \
                SELECT customer FROM public.ks_st_a \
             ) x",
        )
        .await;
    assert_eq!(
        orphan_count, 0,
        "Every customer in ST-B must appear in ST-A"
    );
}

// ══════════════════════════════════════════════════════════════════════
// K2 — Diamond topology: two paths from shared source converge
// ══════════════════════════════════════════════════════════════════════

/// Diamond pattern:
///   src_products ──┬── st_expensive (price > 50)
///                  └── st_cheap     (price <= 50)
///
/// After mutations, refreshing both and then checking that the UNION
/// of st_expensive + st_cheap equals the full source.
#[tokio::test]
async fn test_cross_source_diamond_convergence() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ks_products (id INT PRIMARY KEY, name TEXT, price NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO ks_products VALUES \
         (1, 'Pen', 10), (2, 'Notebook', 30), (3, 'Laptop', 999), \
         (4, 'Mouse', 25), (5, 'Monitor', 300)",
    )
    .await;

    let q_exp = "SELECT id, name, price FROM ks_products WHERE price > 50";
    let q_chp = "SELECT id, name, price FROM ks_products WHERE price <= 50";

    db.create_st("ks_expensive", q_exp, "1m", "DIFFERENTIAL")
        .await;
    db.create_st("ks_cheap", q_chp, "1m", "DIFFERENTIAL").await;

    // Initial: 2 expensive (Laptop, Monitor), 3 cheap (Pen, Notebook, Mouse)
    assert_eq!(db.count("public.ks_expensive").await, 2);
    assert_eq!(db.count("public.ks_cheap").await, 3);

    // Mutate: change a cheap item to expensive, add one, delete one
    db.execute("UPDATE ks_products SET price = 75 WHERE id = 2") // Notebook 30→75
        .await;
    db.execute("INSERT INTO ks_products VALUES (6, 'Cable', 5)")
        .await;
    db.execute("DELETE FROM ks_products WHERE id = 4").await; // Remove Mouse

    db.refresh_st("ks_expensive").await;
    db.refresh_st("ks_cheap").await;

    // Each ST must match its defining query
    db.assert_st_matches_query("public.ks_expensive", q_exp)
        .await;
    db.assert_st_matches_query("public.ks_cheap", q_chp).await;

    // Cross-source invariant: UNION ALL of both STs must equal the full source
    let union_matches: bool = db
        .query_scalar(
            "SELECT NOT EXISTS ( \
                (SELECT id, name, price FROM public.ks_expensive \
                 UNION ALL \
                 SELECT id, name, price FROM public.ks_cheap \
                 EXCEPT \
                 SELECT id, name, price FROM ks_products) \
                UNION ALL \
                (SELECT id, name, price FROM ks_products \
                 EXCEPT \
                 (SELECT id, name, price FROM public.ks_expensive \
                  UNION ALL \
                  SELECT id, name, price FROM public.ks_cheap)) \
            )",
        )
        .await;
    assert!(
        union_matches,
        "UNION ALL of expensive + cheap must equal full source"
    );
}

// ══════════════════════════════════════════════════════════════════════
// K3 — Interleaved mutations across sources + serial refreshes converge
// ══════════════════════════════════════════════════════════════════════

/// Three source tables joined in a single ST. Mutations are interleaved
/// across sources in multiple rounds, with a refresh after each round.
/// After every refresh the ST must match its defining query.
#[tokio::test]
async fn test_cross_source_interleaved_mutations_converge() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ks_dept (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE ks_emp (id INT PRIMARY KEY, dept_id INT, name TEXT, salary INT)")
        .await;
    db.execute("CREATE TABLE ks_bonus (emp_id INT PRIMARY KEY, bonus INT)")
        .await;

    db.execute("INSERT INTO ks_dept VALUES (1, 'Eng'), (2, 'Sales')")
        .await;
    db.execute(
        "INSERT INTO ks_emp VALUES \
         (1, 1, 'Alice', 100), (2, 1, 'Bob', 120), (3, 2, 'Charlie', 90)",
    )
    .await;
    db.execute("INSERT INTO ks_bonus VALUES (1, 10), (2, 15), (3, 5)")
        .await;

    let query = "SELECT d.name AS dept, e.name AS emp, e.salary + COALESCE(b.bonus, 0) AS total \
                 FROM ks_emp e \
                 JOIN ks_dept d ON e.dept_id = d.id \
                 LEFT JOIN ks_bonus b ON e.id = b.emp_id";

    db.create_st("ks_payroll", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("public.ks_payroll", query).await;

    // Round 1: Add a department and move an employee
    db.execute("INSERT INTO ks_dept VALUES (3, 'HR')").await;
    db.execute("UPDATE ks_emp SET dept_id = 3 WHERE id = 3") // Charlie → HR
        .await;

    db.refresh_st("ks_payroll").await;
    db.assert_st_matches_query("public.ks_payroll", query).await;

    // Round 2: Change salaries and bonuses
    db.execute("UPDATE ks_emp SET salary = 130 WHERE id = 2") // Bob raise
        .await;
    db.execute("UPDATE ks_bonus SET bonus = 20 WHERE emp_id = 1") // Alice bonus up
        .await;
    db.execute("INSERT INTO ks_emp VALUES (4, 2, 'Diana', 95)")
        .await; // No bonus yet

    db.refresh_st("ks_payroll").await;
    db.assert_st_matches_query("public.ks_payroll", query).await;

    // Round 3: Delete across all three sources
    db.execute("DELETE FROM ks_bonus WHERE emp_id = 3").await; // Charlie loses bonus
    db.execute("DELETE FROM ks_emp WHERE id = 2").await; // Bob leaves
    db.execute("DELETE FROM ks_dept WHERE id = 2").await; // Sales dissolved (Diana orphaned from join)

    db.refresh_st("ks_payroll").await;
    db.assert_st_matches_query("public.ks_payroll", query).await;

    // Final count: only employees whose dept still exists
    let final_count = db.count("public.ks_payroll").await;
    let expected: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM ({query}) _q"))
        .await;
    assert_eq!(final_count, expected);
}

// ══════════════════════════════════════════════════════════════════════
// K4 — Atomic diamond consistency: multi-ST snapshot coherence
// ══════════════════════════════════════════════════════════════════════

/// Two STs with `diamond_consistency = 'atomic'` on the same source.
/// After mutations, both are refreshed and must reflect the exact same
/// snapshot of the underlying source (verified by cross-ST invariant).
#[tokio::test]
async fn test_cross_source_atomic_diamond_snapshot() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ks_ledger (id INT PRIMARY KEY, account TEXT, amount INT)")
        .await;
    db.execute(
        "INSERT INTO ks_ledger VALUES \
         (1, 'A', 100), (2, 'A', -30), (3, 'B', 200), (4, 'B', -50)",
    )
    .await;

    // ST for per-account balance (aggregate)
    let q_bal = "SELECT account, SUM(amount) AS balance FROM ks_ledger GROUP BY account";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('ks_balance', \
         $${q_bal}$$, '1m', 'DIFFERENTIAL', true, 'atomic')"
    ))
    .await;

    // ST for total across all accounts (scalar aggregate)
    let q_tot = "SELECT SUM(amount) AS grand_total FROM ks_ledger";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('ks_grand_total', \
         $${q_tot}$$, '1m', 'DIFFERENTIAL', true, 'atomic')"
    ))
    .await;

    // Cross-check invariant: sum of per-account balances == grand total
    async fn check_invariant(db: &E2eDb) {
        let sum_balances: i64 = db
            .query_scalar("SELECT COALESCE(SUM(balance), 0)::bigint FROM public.ks_balance")
            .await;
        let grand_total: i64 = db
            .query_scalar("SELECT COALESCE(grand_total, 0)::bigint FROM public.ks_grand_total")
            .await;
        assert_eq!(
            sum_balances, grand_total,
            "Sum of balances ({}) must equal grand total ({})",
            sum_balances, grand_total,
        );
    }

    check_invariant(&db).await;

    // Mutate: transfer between accounts + new entry
    db.execute("INSERT INTO ks_ledger VALUES (5, 'A', -20), (6, 'B', 20)") // Transfer A→B
        .await;
    db.execute("INSERT INTO ks_ledger VALUES (7, 'C', 500)") // New account
        .await;

    db.refresh_st("ks_balance").await;
    db.refresh_st("ks_grand_total").await;

    // Both STs must match their queries
    db.assert_st_matches_query("public.ks_balance", q_bal).await;
    db.assert_st_matches_query("public.ks_grand_total", q_tot)
        .await;

    // Cross-ST invariant still holds
    check_invariant(&db).await;
}

// ══════════════════════════════════════════════════════════════════════
// K5 — Multi-round stress: repeated mutations + refresh cycles
// ══════════════════════════════════════════════════════════════════════

/// 10 rounds of random-ish mutations on two joined sources, verifying
/// ST correctness after each refresh. Tests that incremental state
/// doesn't accumulate drift over many cycles.
#[tokio::test]
async fn test_cross_source_multi_round_no_drift() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ks_regions (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE ks_sales (id SERIAL PRIMARY KEY, region_id INT, revenue INT)")
        .await;

    db.execute("INSERT INTO ks_regions VALUES (1, 'North'), (2, 'South'), (3, 'East')")
        .await;
    db.execute(
        "INSERT INTO ks_sales (region_id, revenue) \
         SELECT (g % 3) + 1, g * 10 FROM generate_series(1, 30) g",
    )
    .await;

    let query = "SELECT r.name AS region, SUM(s.revenue) AS total_revenue \
                 FROM ks_sales s JOIN ks_regions r ON s.region_id = r.id \
                 GROUP BY r.name";

    db.create_st("ks_rev_summary", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("public.ks_rev_summary", query)
        .await;

    for round in 1..=10 {
        // Each round: insert some sales, update some, delete some
        let offset = round * 100;
        db.execute(&format!(
            "INSERT INTO ks_sales (region_id, revenue) \
             SELECT (g % 3) + 1, {offset} + g FROM generate_series(1, 5) g"
        ))
        .await;

        db.execute(&format!(
            "UPDATE ks_sales SET revenue = revenue + 1 WHERE id % 7 = {round}"
        ))
        .await;

        db.execute(&format!(
            "DELETE FROM ks_sales WHERE id IN ( \
                SELECT id FROM ks_sales WHERE region_id = ({round} % 3) + 1 \
                ORDER BY id LIMIT 2 \
             )"
        ))
        .await;

        db.refresh_st("ks_rev_summary").await;
        db.assert_st_matches_query("public.ks_rev_summary", query)
            .await;
    }
}
