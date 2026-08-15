//! E2E tests for LATERAL subquery support.
//!
//! Tests both FULL and DIFFERENTIAL modes for LATERAL subqueries
//! (as distinct from LATERAL SRFs in `e2e_lateral_tests.rs`):
//! - Comma syntax: `FROM t, LATERAL (SELECT ...) AS alias`
//! - JOIN syntax: `FROM t LEFT JOIN LATERAL (SELECT ...) AS alias ON true`
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// FULL Mode Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_lateral_subquery_top_n_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_orders (id INT PRIMARY KEY, customer TEXT)")
        .await;
    db.execute(
        "CREATE TABLE lsq_items (
            id INT PRIMARY KEY,
            order_id INT,
            amount INT,
            created_at TIMESTAMP DEFAULT now()
        )",
    )
    .await;
    db.execute("INSERT INTO lsq_orders VALUES (1, 'Alice'), (2, 'Bob')")
        .await;
    db.execute(
        "INSERT INTO lsq_items VALUES
            (1, 1, 100, '2024-01-01'),
            (2, 1, 200, '2024-01-02'),
            (3, 2, 50, '2024-01-01'),
            (4, 2, 75, '2024-01-03')",
    )
    .await;

    let query_lsq_top_item = "SELECT o.id, o.customer, latest.amount \
         FROM lsq_orders o, \
         LATERAL (SELECT li.amount FROM lsq_items li \
                  WHERE li.order_id = o.id \
                  ORDER BY li.created_at DESC LIMIT 1) AS latest";
    db.create_st("lsq_top_item", query_lsq_top_item, "1m", "FULL")
        .await;
    db.assert_st_matches_query("lsq_top_item", query_lsq_top_item)
        .await;

    assert_eq!(db.count("public.lsq_top_item").await, 2);

    // Alice's latest item should be 200 (2024-01-02)
    let amount: i32 = db
        .query_scalar("SELECT amount FROM public.lsq_top_item WHERE customer = 'Alice'")
        .await;
    assert_eq!(amount, 200);

    // Bob's latest item should be 75 (2024-01-03)
    let amount: i32 = db
        .query_scalar("SELECT amount FROM public.lsq_top_item WHERE customer = 'Bob'")
        .await;
    assert_eq!(amount, 75);
}

#[tokio::test]
async fn test_lateral_subquery_left_join_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_depts (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_emps (id INT PRIMARY KEY, dept_id INT, salary INT)")
        .await;
    db.execute("INSERT INTO lsq_depts VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Empty')")
        .await;
    db.execute("INSERT INTO lsq_emps VALUES (1, 1, 100), (2, 1, 120), (3, 2, 90)")
        .await;

    // LEFT JOIN LATERAL: 'Empty' dept should appear with NULLs
    let query_lsq_dept_stats = "SELECT d.id, d.name, stats.total, stats.cnt \
         FROM lsq_depts d \
         LEFT JOIN LATERAL (\
             SELECT SUM(e.salary) AS total, COUNT(*) AS cnt \
             FROM lsq_emps e \
             WHERE e.dept_id = d.id\
         ) AS stats ON true";
    db.create_st("lsq_dept_stats", query_lsq_dept_stats, "1m", "FULL")
        .await;
    db.assert_st_matches_query("lsq_dept_stats", query_lsq_dept_stats)
        .await;

    assert_eq!(db.count("public.lsq_dept_stats").await, 3);

    // Engineering: total=220, cnt=2
    let total: i64 = db
        .query_scalar("SELECT total FROM public.lsq_dept_stats WHERE name = 'Engineering'")
        .await;
    assert_eq!(total, 220);

    // Empty dept: should exist (LEFT JOIN preserves it)
    let has_empty: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.lsq_dept_stats WHERE name = 'Empty')")
        .await;
    assert!(has_empty, "LEFT JOIN LATERAL should preserve outer rows");
}

#[tokio::test]
async fn test_lateral_subquery_correlated_agg_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_parent (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_child (id INT PRIMARY KEY, parent_id INT, val INT)")
        .await;
    db.execute("INSERT INTO lsq_parent VALUES (1, 'P1'), (2, 'P2')")
        .await;
    db.execute("INSERT INTO lsq_child VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
        .await;

    let query_lsq_parent_totals = "SELECT p.id, p.name, agg.total \
         FROM lsq_parent p, \
         LATERAL (SELECT SUM(c.val) AS total \
                  FROM lsq_child c \
                  WHERE c.parent_id = p.id) AS agg";
    db.create_st("lsq_parent_totals", query_lsq_parent_totals, "1m", "FULL")
        .await;
    db.assert_st_matches_query("lsq_parent_totals", query_lsq_parent_totals)
        .await;

    assert_eq!(db.count("public.lsq_parent_totals").await, 2);

    let total: i64 = db
        .query_scalar("SELECT total FROM public.lsq_parent_totals WHERE name = 'P1'")
        .await;
    assert_eq!(total, 30);
}

#[tokio::test]
async fn test_lateral_subquery_full_mode_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_rf_orders (id INT PRIMARY KEY, customer TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_rf_items (id INT PRIMARY KEY, order_id INT, amount INT)")
        .await;
    db.execute("INSERT INTO lsq_rf_orders VALUES (1, 'Alice')")
        .await;
    db.execute("INSERT INTO lsq_rf_items VALUES (1, 1, 100)")
        .await;

    let query_lsq_rf_top = "SELECT o.id, o.customer, sub.amount \
         FROM lsq_rf_orders o, \
         LATERAL (SELECT li.amount FROM lsq_rf_items li \
                  WHERE li.order_id = o.id LIMIT 1) AS sub";
    db.create_st("lsq_rf_top", query_lsq_rf_top, "1m", "FULL")
        .await;
    db.assert_st_matches_query("lsq_rf_top", query_lsq_rf_top)
        .await;

    assert_eq!(db.count("public.lsq_rf_top").await, 1);

    // Add new order + item
    db.execute("INSERT INTO lsq_rf_orders VALUES (2, 'Bob')")
        .await;
    db.execute("INSERT INTO lsq_rf_items VALUES (2, 2, 250)")
        .await;
    db.refresh_st("lsq_rf_top").await;
    db.assert_st_matches_query("lsq_rf_top", query_lsq_rf_top)
        .await;

    assert_eq!(db.count("public.lsq_rf_top").await, 2);
}

// ═══════════════════════════════════════════════════════════════════════
// DIFFERENTIAL / AUTO Mode Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_lateral_subquery_differential_initial() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_di_orders (id INT PRIMARY KEY, customer TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_di_items (id INT PRIMARY KEY, order_id INT, amount INT)")
        .await;
    db.execute("INSERT INTO lsq_di_orders VALUES (1, 'Alice'), (2, 'Bob')")
        .await;
    db.execute("INSERT INTO lsq_di_items VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)")
        .await;

    let query_lsq_di_top = "SELECT o.id, o.customer, sub.amount \
         FROM lsq_di_orders o, \
         LATERAL (SELECT li.amount FROM lsq_di_items li \
                  WHERE li.order_id = o.id LIMIT 1) AS sub";
    db.create_st("lsq_di_top", query_lsq_di_top, "1m", "AUTO")
        .await;
    let (_, mode, _, _) = db.pgt_status("lsq_di_top").await;
    assert_eq!(mode, "FULL");
    db.assert_st_matches_query("lsq_di_top", query_lsq_di_top)
        .await;

    // Initial load should produce correct results
    assert_eq!(db.count("public.lsq_di_top").await, 2);
}

#[tokio::test]
async fn test_lateral_subquery_differential_outer_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_oi_orders (id INT PRIMARY KEY, customer TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_oi_items (id INT PRIMARY KEY, order_id INT, amount INT)")
        .await;
    db.execute("INSERT INTO lsq_oi_orders VALUES (1, 'Alice')")
        .await;
    db.execute("INSERT INTO lsq_oi_items VALUES (1, 1, 100), (10, 2, 250)")
        .await;

    let query_lsq_oi_top = "SELECT o.id, o.customer, sub.amount \
         FROM lsq_oi_orders o, \
         LATERAL (SELECT li.amount FROM lsq_oi_items li \
                  WHERE li.order_id = o.id LIMIT 1) AS sub";
    db.create_st("lsq_oi_top", query_lsq_oi_top, "1m", "AUTO")
        .await;
    let (_, mode, _, _) = db.pgt_status("lsq_oi_top").await;
    assert_eq!(mode, "FULL");
    db.assert_st_matches_query("lsq_oi_top", query_lsq_oi_top)
        .await;
    assert_eq!(db.count("public.lsq_oi_top").await, 1);

    // Insert new outer row → subquery runs for new row → expanded rows added
    db.execute("INSERT INTO lsq_oi_orders VALUES (2, 'Bob')")
        .await;
    db.refresh_st("lsq_oi_top").await;
    db.assert_st_matches_query("lsq_oi_top", query_lsq_oi_top)
        .await;

    assert_eq!(db.count("public.lsq_oi_top").await, 2);

    let amount: i32 = db
        .query_scalar("SELECT amount FROM public.lsq_oi_top WHERE customer = 'Bob'")
        .await;
    assert_eq!(amount, 250);
}

#[tokio::test]
async fn test_lateral_subquery_differential_outer_delete() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_od_orders (id INT PRIMARY KEY, customer TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_od_items (id INT PRIMARY KEY, order_id INT, amount INT)")
        .await;
    db.execute("INSERT INTO lsq_od_orders VALUES (1, 'Alice'), (2, 'Bob')")
        .await;
    db.execute("INSERT INTO lsq_od_items VALUES (1, 1, 100), (2, 2, 200)")
        .await;

    let query_lsq_od_top = "SELECT o.id, o.customer, sub.amount \
         FROM lsq_od_orders o, \
         LATERAL (SELECT li.amount FROM lsq_od_items li \
                  WHERE li.order_id = o.id LIMIT 1) AS sub";
    db.create_st("lsq_od_top", query_lsq_od_top, "1m", "AUTO")
        .await;
    let (_, mode, _, _) = db.pgt_status("lsq_od_top").await;
    assert_eq!(mode, "FULL");
    db.assert_st_matches_query("lsq_od_top", query_lsq_od_top)
        .await;
    assert_eq!(db.count("public.lsq_od_top").await, 2);

    // Delete outer row → all expanded rows for that outer row removed
    db.execute("DELETE FROM lsq_od_orders WHERE id = 1").await;
    db.refresh_st("lsq_od_top").await;
    db.assert_st_matches_query("lsq_od_top", query_lsq_od_top)
        .await;

    assert_eq!(db.count("public.lsq_od_top").await, 1);

    let has_alice: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.lsq_od_top WHERE customer = 'Alice')")
        .await;
    assert!(!has_alice, "Deleted outer row should be removed");
}

#[tokio::test]
async fn test_lateral_subquery_left_join_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_lj_depts (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_lj_emps (id INT PRIMARY KEY, dept_id INT, salary INT)")
        .await;
    db.execute("INSERT INTO lsq_lj_depts VALUES (1, 'Eng'), (2, 'Empty')")
        .await;
    db.execute("INSERT INTO lsq_lj_emps VALUES (1, 1, 100)")
        .await;

    let query_lsq_lj_stats = "SELECT d.id, d.name, stats.total \
         FROM lsq_lj_depts d \
         LEFT JOIN LATERAL (\
             SELECT SUM(e.salary) AS total \
             FROM lsq_lj_emps e \
             WHERE e.dept_id = d.id\
         ) AS stats ON true";
    db.create_st("lsq_lj_stats", query_lsq_lj_stats, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("lsq_lj_stats", query_lsq_lj_stats)
        .await;

    // Both depts should be present (LEFT JOIN preserves outer rows)
    assert_eq!(db.count("public.lsq_lj_stats").await, 2);

    // 'Empty' dept should have NULL total
    let has_empty: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.lsq_lj_stats WHERE name = 'Empty')")
        .await;
    assert!(
        has_empty,
        "LEFT JOIN should preserve outer row with no match"
    );

    // Add new dept → should appear in ST after refresh
    db.execute("INSERT INTO lsq_lj_depts VALUES (3, 'Marketing')")
        .await;
    db.refresh_st("lsq_lj_stats").await;
    db.assert_st_matches_query("lsq_lj_stats", query_lsq_lj_stats)
        .await;

    assert_eq!(db.count("public.lsq_lj_stats").await, 3);
}

#[tokio::test]
async fn test_lateral_subquery_differential_mixed_dml() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_mx_orders (id INT PRIMARY KEY, customer TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_mx_items (id INT PRIMARY KEY, order_id INT, amount INT)")
        .await;
    db.execute("INSERT INTO lsq_mx_orders VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await;
    db.execute("INSERT INTO lsq_mx_items VALUES (1, 1, 100), (2, 2, 200), (3, 3, 300)")
        .await;

    let query_lsq_mx_top = "SELECT o.id, o.customer, sub.amount \
         FROM lsq_mx_orders o, \
         LATERAL (SELECT li.amount FROM lsq_mx_items li \
                  WHERE li.order_id = o.id LIMIT 1) AS sub";
    db.create_st("lsq_mx_top", query_lsq_mx_top, "1m", "AUTO")
        .await;
    let (_, mode, _, _) = db.pgt_status("lsq_mx_top").await;
    assert_eq!(mode, "FULL");
    db.assert_st_matches_query("lsq_mx_top", query_lsq_mx_top)
        .await;
    assert_eq!(db.count("public.lsq_mx_top").await, 3);

    // Mixed DML: insert + update + delete in one batch
    db.execute("INSERT INTO lsq_mx_orders VALUES (4, 'Dave')")
        .await;
    db.execute("INSERT INTO lsq_mx_items VALUES (4, 4, 400)")
        .await;
    db.execute("UPDATE lsq_mx_orders SET customer = 'Bobby' WHERE id = 2")
        .await;
    db.execute("DELETE FROM lsq_mx_orders WHERE id = 1").await;
    db.refresh_st("lsq_mx_top").await;
    db.assert_st_matches_query("lsq_mx_top", query_lsq_mx_top)
        .await;

    assert_eq!(db.count("public.lsq_mx_top").await, 3);

    // Dave should be present
    let has_dave: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.lsq_mx_top WHERE customer = 'Dave')")
        .await;
    assert!(has_dave, "Inserted row should appear");

    // Alice should be gone
    let has_alice: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.lsq_mx_top WHERE customer = 'Alice')")
        .await;
    assert!(!has_alice, "Deleted row should be gone");
}

#[tokio::test]
async fn test_lateral_subquery_multi_row_result() {
    let db = E2eDb::new().await.with_extension().await;

    // LATERAL subquery returning multiple rows per outer row (no LIMIT)
    db.execute("CREATE TABLE lsq_mr_parent (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_mr_child (id INT PRIMARY KEY, parent_id INT, val TEXT)")
        .await;
    db.execute("INSERT INTO lsq_mr_parent VALUES (1, 'P1'), (2, 'P2')")
        .await;
    db.execute("INSERT INTO lsq_mr_child VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c')")
        .await;

    let query_lsq_mr_expanded = "SELECT p.id, p.name, sub.val \
         FROM lsq_mr_parent p, \
         LATERAL (SELECT c.val FROM lsq_mr_child c \
                  WHERE c.parent_id = p.id) AS sub";
    db.create_st("lsq_mr_expanded", query_lsq_mr_expanded, "1m", "FULL")
        .await;
    db.assert_st_matches_query("lsq_mr_expanded", query_lsq_mr_expanded)
        .await;

    // P1 has 2 children, P2 has 1 → total 3 rows
    assert_eq!(db.count("public.lsq_mr_expanded").await, 3);
}

#[tokio::test]
async fn test_lateral_subquery_empty_result_cross_join() {
    let db = E2eDb::new().await.with_extension().await;

    // CROSS JOIN LATERAL: outer row excluded when subquery returns 0 rows
    db.execute("CREATE TABLE lsq_empty_parent (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE lsq_empty_child (id INT PRIMARY KEY, parent_id INT, val TEXT)")
        .await;
    db.execute("INSERT INTO lsq_empty_parent VALUES (1, 'HasChild'), (2, 'NoChild')")
        .await;
    db.execute("INSERT INTO lsq_empty_child VALUES (1, 1, 'x')")
        .await;

    let query_lsq_empty_st = "SELECT p.id, p.name, sub.val \
         FROM lsq_empty_parent p, \
         LATERAL (SELECT c.val FROM lsq_empty_child c \
                  WHERE c.parent_id = p.id) AS sub";
    db.create_st("lsq_empty_st", query_lsq_empty_st, "1m", "FULL")
        .await;
    db.assert_st_matches_query("lsq_empty_st", query_lsq_empty_st)
        .await;

    // Only 'HasChild' should appear (CROSS JOIN excludes empty results)
    assert_eq!(db.count("public.lsq_empty_st").await, 1);

    let name: String = db
        .query_scalar("SELECT name FROM public.lsq_empty_st")
        .await;
    assert_eq!(name, "HasChild");
}

#[tokio::test]
async fn test_lateral_subquery_with_group_by() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsq_gb_orders (id INT PRIMARY KEY, customer TEXT)")
        .await;
    db.execute(
        "CREATE TABLE lsq_gb_items (id INT PRIMARY KEY, order_id INT, category TEXT, amount INT)",
    )
    .await;
    db.execute("INSERT INTO lsq_gb_orders VALUES (1, 'Alice')")
        .await;
    db.execute(
        "INSERT INTO lsq_gb_items VALUES
            (1, 1, 'A', 10),
            (2, 1, 'A', 20),
            (3, 1, 'B', 30)",
    )
    .await;

    let query_lsq_gb_summary = "SELECT o.id, o.customer, sub.category, sub.total \
         FROM lsq_gb_orders o, \
         LATERAL (SELECT li.category, SUM(li.amount) AS total \
                  FROM lsq_gb_items li \
                  WHERE li.order_id = o.id \
                  GROUP BY li.category) AS sub";
    db.create_st("lsq_gb_summary", query_lsq_gb_summary, "1m", "FULL")
        .await;
    db.assert_st_matches_query("lsq_gb_summary", query_lsq_gb_summary)
        .await;

    // Alice has 2 categories: A(30) and B(30)
    assert_eq!(db.count("public.lsq_gb_summary").await, 2);
}

#[tokio::test]
async fn test_lateral_invalid_query_fails() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE lat_err_src (id INT PRIMARY KEY)")
        .await;

    let result = db.try_execute(
        "SELECT pgtrickle.create_stream_table('lat_err_st', 'SELECT l.id, lat.x FROM lat_err_src l, LATERAL (SELECT l.id + non_existent as x) lat', '1m', 'DIFFERENTIAL')"
    ).await;

    assert!(
        result.is_err(),
        "Creation with invalid lateral query should fail"
    );
}

#[tokio::test]
async fn test_lateral_with_nulls() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE lat_null_src (id INT, val INT)")
        .await;

    db.execute("INSERT INTO lat_null_src VALUES (1, NULL), (NULL, 10), (NULL, NULL)")
        .await;

    let q = "SELECT l.id, l.val, lat.x FROM lat_null_src l LEFT JOIN LATERAL (SELECT l.val * 2 as x) lat ON true";

    db.create_st("lat_null_st", q, "1m", "AUTO").await;
    let (_, mode, _, _) = db.pgt_status("lat_null_st").await;
    assert_eq!(mode, "FULL");

    db.assert_st_matches_query("lat_null_st", q).await;

    db.execute("INSERT INTO lat_null_src VALUES (2, 20)").await;
    db.refresh_st("lat_null_st").await;

    db.assert_st_matches_query("lat_null_st", q).await;
}

/// Self-referencing LATERAL: when an INSERT changes the inner aggregate
/// result (MAX), ALL existing outer rows in that partition must update.
#[tokio::test]
async fn test_lateral_subquery_self_ref_inner_aggregate_change() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE lat_self (id INT PRIMARY KEY, region TEXT NOT NULL, amount INT NOT NULL, score INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO lat_self VALUES (1,'west',100,50),(2,'west',200,60),(3,'east',300,70),(4,'east',400,80)",
    )
    .await;

    let q = "SELECT s.id, s.region, s.amount, l.top_score \
             FROM lat_self s, \
             LATERAL (SELECT MAX(score) AS top_score FROM lat_self s2 WHERE s2.region = s.region) l";

    db.create_st("lat_self_agg", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("lat_self_agg", q).await;

    // Initial: west top_score=60, east top_score=80
    let west_top: i32 = db
        .query_scalar("SELECT top_score FROM public.lat_self_agg WHERE id = 1")
        .await;
    assert_eq!(west_top, 60);

    // Insert a row that changes MAX(score) for 'west' from 60 to 99
    db.execute("INSERT INTO lat_self VALUES (5, 'west', 500, 99)")
        .await;
    db.refresh_st("lat_self_agg").await;
    db.assert_st_matches_query("lat_self_agg", q).await;

    // ALL west rows (including previously-existing id=1,2) must now show top_score=99
    let west_top_after: i32 = db
        .query_scalar("SELECT top_score FROM public.lat_self_agg WHERE id = 1")
        .await;
    assert_eq!(
        west_top_after, 99,
        "existing west row must update top_score"
    );

    // East rows should be unchanged
    let east_top: i32 = db
        .query_scalar("SELECT top_score FROM public.lat_self_agg WHERE id = 3")
        .await;
    assert_eq!(east_top, 80);

    assert_eq!(db.count("public.lat_self_agg").await, 5);
}

/// Self-referencing LATERAL with multiple DML cycles — reproduces the
/// bench_full_matrix 100K failure pattern where UPDATE + DELETE + INSERT
/// accumulate stale top_score values across cycles.
#[tokio::test]
async fn test_lateral_subquery_self_ref_multi_cycle() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE lat_mc (id SERIAL PRIMARY KEY, region TEXT NOT NULL, amount INT NOT NULL, score INT NOT NULL)",
    )
    .await;
    // 1K rows, 5 regions, score 0-99 — sufficient to exercise the self-ref
    // LATERAL correctness path without the cost of a 10K-row scan per cycle.
    db.execute(
        "INSERT INTO lat_mc (region, amount, score) \
         SELECT CASE (i%5) WHEN 0 THEN 'north' WHEN 1 THEN 'south' \
                WHEN 2 THEN 'east' WHEN 3 THEN 'west' ELSE 'central' END, \
                (i*17+13)%10000, (i*31+7)%100 \
         FROM generate_series(1,1000) s(i)",
    )
    .await;

    let q = "SELECT s.id, s.region, s.amount, l.top_score \
             FROM lat_mc s, \
             LATERAL (SELECT MAX(score) AS top_score FROM lat_mc s2 WHERE s2.region = s.region) l";

    db.create_st("lat_mc_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("lat_mc_st", q).await;

    // 5 cycles (2 warmup + 3 measured) with deterministic predicates.
    // Using id % N instead of ORDER BY random() avoids a seq-scan+sort on
    // every DML statement and keeps the test reproducible.
    for cycle in 1..=5 {
        // ~7% update, ~1% delete, re-insert to keep row count stable
        db.execute(&format!(
            "UPDATE lat_mc SET amount = amount + 1 WHERE id % 14 = {}",
            cycle % 14
        ))
        .await;
        db.execute(&format!(
            "DELETE FROM lat_mc WHERE id % 97 = {}",
            cycle % 97
        ))
        .await;
        db.execute(
            "INSERT INTO lat_mc (region, amount, score) \
             SELECT CASE (i%5) WHEN 0 THEN 'north' WHEN 1 THEN 'south' \
                    WHEN 2 THEN 'east' WHEN 3 THEN 'west' ELSE 'central' END, \
                    (i*17+13)%10000, (i*31+7)%100 \
             FROM generate_series(1,10) s(i)",
        )
        .await;

        db.refresh_st("lat_mc_st").await;
        db.assert_st_matches_query("lat_mc_st", q).await;
    }
}
