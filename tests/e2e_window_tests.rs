//! E2E tests for window function support in stream tables.
//!
//! Tests ROW_NUMBER(), RANK(), DENSE_RANK(), SUM() OVER(), etc.
//! with both FULL and DIFFERENTIAL refresh modes.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── FULL refresh with window functions ────────────────────────────────

#[tokio::test]
async fn test_window_row_number_full_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_rn (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_rn (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('sales', 90), ('sales', 70)",
    )
    .await;

    db.create_st(
        "wf_rn_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_rn",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.wf_rn_st").await, 4);

    // Verify correctness: in eng partition, salary=100 gets rn=1
    let rn: i64 = db
        .query_scalar("SELECT rn FROM public.wf_rn_st WHERE dept = 'eng' AND salary = 100")
        .await;
    assert_eq!(rn, 1);

    // In sales partition, salary=90 gets rn=1
    let rn: i64 = db
        .query_scalar("SELECT rn FROM public.wf_rn_st WHERE dept = 'sales' AND salary = 90")
        .await;
    assert_eq!(rn, 1);
}

#[tokio::test]
async fn test_window_sum_over_full_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_sum (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_sum (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('sales', 90)",
    )
    .await;

    db.create_st(
        "wf_sum_st",
        "SELECT dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM public.wf_sum",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.wf_sum_st").await, 3);

    // eng total = 100 + 80 = 180
    let total: i64 = db
        .query_scalar("SELECT dept_total FROM public.wf_sum_st WHERE dept = 'eng' LIMIT 1")
        .await;
    assert_eq!(total, 180);

    // sales total = 90
    let total: i64 = db
        .query_scalar("SELECT dept_total FROM public.wf_sum_st WHERE dept = 'sales' LIMIT 1")
        .await;
    assert_eq!(total, 90);
}

#[tokio::test]
async fn test_window_rank_full_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_rank (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_rank (dept, salary) VALUES
         ('eng', 100), ('eng', 100), ('eng', 80)",
    )
    .await;

    db.create_st(
        "wf_rank_st",
        "SELECT dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk FROM public.wf_rank",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.wf_rank_st").await, 3);

    // Two rows with salary=100 get rank=1
    let cnt: i64 = db
        .query_scalar("SELECT count(*) FROM public.wf_rank_st WHERE rnk = 1")
        .await;
    assert_eq!(cnt, 2);

    // Row with salary=80 gets rank=3 (not 2, because RANK skips)
    let rnk: i64 = db
        .query_scalar("SELECT rnk FROM public.wf_rank_st WHERE salary = 80")
        .await;
    assert_eq!(rnk, 3);
}

// ── FULL refresh with DML + refresh ──────────────────────────────────

#[tokio::test]
async fn test_window_full_refresh_after_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_fi (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO wf_fi (dept, salary) VALUES ('eng', 100), ('eng', 80)")
        .await;

    db.create_st(
        "wf_fi_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_fi",
        "1m",
        "FULL",
    )
    .await;
    assert_eq!(db.count("public.wf_fi_st").await, 2);

    // Insert a new highest salary
    db.execute("INSERT INTO wf_fi (dept, salary) VALUES ('eng', 120)")
        .await;
    db.refresh_st("wf_fi_st").await;

    assert_eq!(db.count("public.wf_fi_st").await, 3);

    // New row should be rn=1
    let rn: i64 = db
        .query_scalar("SELECT rn FROM public.wf_fi_st WHERE salary = 120")
        .await;
    assert_eq!(rn, 1);

    // Old top row should be rn=2 now
    let rn: i64 = db
        .query_scalar("SELECT rn FROM public.wf_fi_st WHERE salary = 100")
        .await;
    assert_eq!(rn, 2);
}

#[tokio::test]
async fn test_window_full_refresh_after_delete() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_fd (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_fd (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('eng', 60)",
    )
    .await;

    db.create_st(
        "wf_fd_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_fd",
        "1m",
        "FULL",
    )
    .await;

    // Delete top salary
    db.execute("DELETE FROM wf_fd WHERE salary = 100").await;
    db.refresh_st("wf_fd_st").await;

    assert_eq!(db.count("public.wf_fd_st").await, 2);

    // salary=80 should now be rn=1
    let rn: i64 = db
        .query_scalar("SELECT rn FROM public.wf_fd_st WHERE salary = 80")
        .await;
    assert_eq!(rn, 1);
}

// ── DIFFERENTIAL refresh with window functions ────────────────────────

#[tokio::test]
async fn test_window_differential_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_ii (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_ii (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('sales', 90)",
    )
    .await;

    db.create_st(
        "wf_ii_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_ii",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.wf_ii_st").await, 3);

    // Insert into eng department — should trigger recomputation of eng partition
    db.execute("INSERT INTO wf_ii (dept, salary) VALUES ('eng', 95)")
        .await;
    db.refresh_st("wf_ii_st").await;

    assert_eq!(db.count("public.wf_ii_st").await, 4);

    // Verify eng partition is correctly recomputed
    let rn_100: i64 = db
        .query_scalar("SELECT rn FROM public.wf_ii_st WHERE dept = 'eng' AND salary = 100")
        .await;
    assert_eq!(rn_100, 1, "salary=100 should be rn=1");

    let rn_95: i64 = db
        .query_scalar("SELECT rn FROM public.wf_ii_st WHERE dept = 'eng' AND salary = 95")
        .await;
    assert_eq!(rn_95, 2, "salary=95 should be rn=2");

    let rn_80: i64 = db
        .query_scalar("SELECT rn FROM public.wf_ii_st WHERE dept = 'eng' AND salary = 80")
        .await;
    assert_eq!(rn_80, 3, "salary=80 should be rn=3");

    // Sales partition should be unchanged
    let rn_sales: i64 = db
        .query_scalar("SELECT rn FROM public.wf_ii_st WHERE dept = 'sales' AND salary = 90")
        .await;
    assert_eq!(rn_sales, 1, "sales partition should be unaffected");
}

#[tokio::test]
async fn test_window_differential_delete() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_id (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_id (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('eng', 60), ('sales', 90)",
    )
    .await;

    db.create_st(
        "wf_id_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.wf_id_st").await, 4);

    // Delete middle row from eng partition
    db.execute("DELETE FROM wf_id WHERE dept = 'eng' AND salary = 80")
        .await;
    db.refresh_st("wf_id_st").await;

    assert_eq!(db.count("public.wf_id_st").await, 3);

    // salary=60 should now be rn=2 (was rn=3)
    let rn_60: i64 = db
        .query_scalar("SELECT rn FROM public.wf_id_st WHERE dept = 'eng' AND salary = 60")
        .await;
    assert_eq!(rn_60, 2, "salary=60 should be rn=2 after delete");
}

#[tokio::test]
async fn test_window_differential_update() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_iu (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_iu (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('eng', 60)",
    )
    .await;

    db.create_st(
        "wf_iu_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_iu",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Update salary=60 to 110, making it the new top
    db.execute("UPDATE wf_iu SET salary = 110 WHERE salary = 60")
        .await;
    db.refresh_st("wf_iu_st").await;

    let rn_110: i64 = db
        .query_scalar("SELECT rn FROM public.wf_iu_st WHERE salary = 110")
        .await;
    assert_eq!(rn_110, 1, "salary=110 should be rn=1 after update");

    let rn_100: i64 = db
        .query_scalar("SELECT rn FROM public.wf_iu_st WHERE salary = 100")
        .await;
    assert_eq!(rn_100, 2, "salary=100 should be rn=2 after update");
}

#[tokio::test]
async fn test_window_differential_multiple_partitions_changed() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_mp (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_mp (dept, salary) VALUES
         ('eng', 100), ('eng', 80),
         ('sales', 90), ('sales', 70)",
    )
    .await;

    db.create_st(
        "wf_mp_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_mp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Insert into both partitions
    db.execute("INSERT INTO wf_mp (dept, salary) VALUES ('eng', 95), ('sales', 85)")
        .await;
    db.refresh_st("wf_mp_st").await;

    assert_eq!(db.count("public.wf_mp_st").await, 6);

    // Verify both partitions recomputed
    db.assert_st_matches_query(
        "public.wf_mp_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_mp",
    )
    .await;
}

#[tokio::test]
async fn test_window_differential_sum_over() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_is (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_is (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('sales', 90)",
    )
    .await;

    db.create_st(
        "wf_is_st",
        "SELECT dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM public.wf_is",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Insert new eng row
    db.execute("INSERT INTO wf_is (dept, salary) VALUES ('eng', 50)")
        .await;
    db.refresh_st("wf_is_st").await;

    // eng total should now be 100+80+50=230
    let total: i64 = db
        .query_scalar("SELECT dept_total FROM public.wf_is_st WHERE dept = 'eng' LIMIT 1")
        .await;
    assert_eq!(total, 230);

    // sales should be unchanged at 90
    let total: i64 = db
        .query_scalar("SELECT dept_total FROM public.wf_is_st WHERE dept = 'sales' LIMIT 1")
        .await;
    assert_eq!(total, 90);
}

// ── Window function with filter ──────────────────────────────────────

#[tokio::test]
async fn test_window_with_where_clause() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_wh (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL, active BOOL NOT NULL DEFAULT TRUE)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_wh (dept, salary, active) VALUES
         ('eng', 100, true), ('eng', 80, false), ('eng', 60, true), ('sales', 90, true)",
    )
    .await;

    db.create_st(
        "wf_wh_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_wh WHERE active = true",
        "1m",
        "FULL",
    )
    .await;

    // eng: only 100 and 60 are active → rn 1 and 2
    assert_eq!(db.count("public.wf_wh_st").await, 3);

    let rn: i64 = db
        .query_scalar("SELECT rn FROM public.wf_wh_st WHERE dept = 'eng' AND salary = 100")
        .await;
    assert_eq!(rn, 1);

    let rn: i64 = db
        .query_scalar("SELECT rn FROM public.wf_wh_st WHERE dept = 'eng' AND salary = 60")
        .await;
    assert_eq!(rn, 2);
}

// ── DENSE_RANK ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_window_dense_rank() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_dr (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_dr (dept, salary) VALUES
         ('eng', 100), ('eng', 100), ('eng', 80)",
    )
    .await;

    db.create_st(
        "wf_dr_st",
        "SELECT dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS drnk FROM public.wf_dr",
        "1m",
        "FULL",
    )
    .await;

    // salary=80 should get dense_rank=2 (not 3 like RANK)
    let drnk: i64 = db
        .query_scalar("SELECT drnk FROM public.wf_dr_st WHERE salary = 80")
        .await;
    assert_eq!(drnk, 2);
}

// ── Nested window function detection (Gap 7.4) ──────────────────────
// Note: nested window functions are rejected in DIFFERENTIAL mode because
// the DVM parser lifts nested window expressions to inner subqueries via
// rewrite_nested_window_exprs() (Task 1.3 / EC-03). These queries are now
// accepted in DIFFERENTIAL mode rather than rejected.

#[tokio::test]
async fn test_window_in_case_expression_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_nested (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO wf_nested (dept, salary) VALUES ('eng', 100), ('eng', 80), ('hr', 90)")
        .await;

    // Window function inside CASE is now handled via subquery-lift rewrite (EC-03).
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('wf_nested_st', \
             $$ SELECT CASE WHEN ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) <= 3 \
             THEN 'top' ELSE 'other' END AS tier FROM public.wf_nested $$, '1m', 'DIFFERENTIAL')",
        )
        .await;

    assert!(
        result.is_ok(),
        "Nested window function in CASE should be accepted via subquery-lift rewrite (EC-03)"
    );
}

#[tokio::test]
async fn test_window_in_coalesce_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE wf_coal (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, val INT)")
        .await;
    db.execute("INSERT INTO wf_coal (dept, val) VALUES ('eng', 10), ('eng', 20), ('hr', 30)")
        .await;

    // Window function inside COALESCE is now handled via subquery-lift rewrite (EC-03).
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('wf_coal_st', \
             $$ SELECT COALESCE(SUM(val) OVER (PARTITION BY dept), 0) AS total FROM public.wf_coal $$, '1m', 'DIFFERENTIAL')",
        )
        .await;

    assert!(
        result.is_ok(),
        "Nested window function in COALESCE should be accepted via subquery-lift rewrite (EC-03)"
    );
}

#[tokio::test]
async fn test_window_in_arithmetic_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_arith (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO wf_arith (dept, salary) VALUES ('eng', 100), ('hr', 80)")
        .await;

    // Window function inside arithmetic is now handled via subquery-lift rewrite (EC-03).
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('wf_arith_st', \
             $$ SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) * 10 AS scaled_rank \
             FROM public.wf_arith $$, '1m', 'DIFFERENTIAL')",
        )
        .await;

    assert!(
        result.is_ok(),
        "Nested window function in arithmetic should be accepted via subquery-lift rewrite (EC-03): {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_window_in_cast_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_cast (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO wf_cast (dept, salary) VALUES ('eng', 100), ('hr', 80)")
        .await;

    // Window function inside CAST is now handled via subquery-lift rewrite (EC-03).
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('wf_cast_st', \
             $$ SELECT CAST(ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS TEXT) AS rn_text \
             FROM public.wf_cast $$, '1m', 'DIFFERENTIAL')",
        )
        .await;

    assert!(
        result.is_ok(),
        "Nested window function in CAST should be accepted via subquery-lift rewrite (EC-03): {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_window_deeply_nested_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_deep (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO wf_deep (dept, salary) VALUES ('eng', 100), ('eng', 80), ('hr', 90)")
        .await;

    // Deeply nested window function (CASE → COALESCE → window) is now handled via
    // subquery-lift rewrite (EC-03).
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('wf_deep_st', \
             $$ SELECT CASE WHEN COALESCE(ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC), 0) <= 3 \
             THEN 'top' ELSE 'other' END AS tier FROM public.wf_deep $$, '1m', 'DIFFERENTIAL')"
        )
        .await;

    assert!(
        result.is_ok(),
        "Deeply nested window function should be accepted via subquery-lift rewrite (EC-03): {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_top_level_window_still_works() {
    // Regression: top-level window functions should still work fine
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_ok (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO wf_ok (dept, salary) VALUES ('eng', 100), ('eng', 80), ('hr', 90)")
        .await;

    db.create_st(
        "wf_ok_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_ok",
        "1m",
        "FULL",
    )
    .await;

    let rn: i64 = db
        .query_scalar("SELECT rn FROM public.wf_ok_st WHERE dept = 'eng' AND salary = 100")
        .await;
    assert_eq!(rn, 1);
}

// ── EC-03: Data correctness for window-in-expression rewrite ─────────

/// EC-03: Verify CASE WHEN ROW_NUMBER() OVER ... produces correct data
/// with both FULL and DIFFERENTIAL refresh, and that incremental updates
/// (INSERT/DELETE) maintain correctness.
#[tokio::test]
async fn test_ec03_case_window_data_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE ec03_case (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO ec03_case (dept, salary) VALUES \
         ('eng', 100), ('eng', 80), ('eng', 60), ('hr', 90), ('hr', 70)",
    )
    .await;

    let query = "SELECT id, dept, \
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1 \
        THEN 'top' ELSE 'other' END AS tier \
        FROM public.ec03_case";

    db.create_st("ec03_case_st", query, "1m", "FULL").await;

    // Verify initial data: top earner in each dept gets 'top'.
    // Use id (which is in the SELECT) rather than salary (not selected).
    // INSERT order: id=1 eng/100, id=2 eng/80, id=3 eng/60, id=4 hr/90, id=5 hr/70
    let eng_top: String = db
        .query_scalar("SELECT tier FROM public.ec03_case_st WHERE id = 1")
        .await;
    assert_eq!(
        eng_top, "top",
        "Highest eng salary (id=1, salary=100) should be 'top'"
    );

    let eng_other: String = db
        .query_scalar("SELECT tier FROM public.ec03_case_st WHERE id = 2")
        .await;
    assert_eq!(
        eng_other, "other",
        "Second eng salary (id=2, salary=80) should be 'other'"
    );

    let hr_top: String = db
        .query_scalar("SELECT tier FROM public.ec03_case_st WHERE id = 4")
        .await;
    assert_eq!(
        hr_top, "top",
        "Highest hr salary (id=4, salary=90) should be 'top'"
    );

    assert_eq!(db.count("public.ec03_case_st").await, 5);
}

/// EC-03: Verify window-in-arithmetic rewrite produces correct results
/// with FULL refresh. (Note: DIFFERENTIAL refresh of window-in-expression
/// queries is not yet supported — the DVM delta engine cannot handle the
/// inner subquery alias introduced by the rewrite.)
#[tokio::test]
async fn test_ec03_arithmetic_window_data_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE ec03_arith (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, score INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO ec03_arith (dept, score) VALUES ('a', 50), ('a', 30), ('b', 40)")
        .await;

    let query = "SELECT id, dept, \
        ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) * 10 AS scaled_rank \
        FROM public.ec03_arith";

    db.create_st("ec03_arith_st", query, "1m", "FULL").await;

    // dept=a: scores [50, 30] → rn [1, 2] → scaled_rank [10, 20]
    // id=1 a/50, id=2 a/30, id=3 b/40
    let top_rank: i64 = db
        .query_scalar("SELECT scaled_rank FROM public.ec03_arith_st WHERE id = 1")
        .await;
    assert_eq!(
        top_rank, 10,
        "Top score in dept a (id=1, score=50) should have scaled_rank=10"
    );

    let second_rank: i64 = db
        .query_scalar("SELECT scaled_rank FROM public.ec03_arith_st WHERE id = 2")
        .await;
    assert_eq!(
        second_rank, 20,
        "Second score in dept a (id=2, score=30) should have scaled_rank=20"
    );

    // Insert a new top scorer and do a FULL refresh
    db.execute("INSERT INTO ec03_arith (dept, score) VALUES ('a', 70)")
        .await;
    db.refresh_st("ec03_arith_st").await;

    db.assert_st_matches_query("public.ec03_arith_st", query)
        .await;
    assert_eq!(db.count("public.ec03_arith_st").await, 4);
}

/// EC-03: Verify COALESCE(window_func, default) rewrite produces correct
/// data with FULL refresh.
#[tokio::test]
async fn test_ec03_coalesce_window_data_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE ec03_coal (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, val INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO ec03_coal (dept, val) VALUES ('eng', 10), ('eng', 20), ('hr', 30)")
        .await;

    let query = "SELECT id, dept, COALESCE(SUM(val) OVER (PARTITION BY dept), 0) AS dept_sum FROM public.ec03_coal";

    db.create_st("ec03_coal_st", query, "1m", "FULL").await;

    // eng dept_sum should be 10+20=30 for both eng rows
    let eng_sum: i64 = db
        .query_scalar("SELECT dept_sum FROM public.ec03_coal_st WHERE dept = 'eng' LIMIT 1")
        .await;
    assert_eq!(eng_sum, 30, "eng dept sum should be 30");

    // hr dept_sum should be 30
    let hr_sum: i64 = db
        .query_scalar("SELECT dept_sum FROM public.ec03_coal_st WHERE dept = 'hr' LIMIT 1")
        .await;
    assert_eq!(hr_sum, 30, "hr dept sum should be 30");
}

// ── G1.2: Partition key change tests ─────────────────────────────────

/// Test that UPDATE on a PARTITION BY key column correctly moves a row
/// between partitions and recomputes both old and new partitions.
///
/// This is the verification test for G1.2 (SQL_GAPS_7). The scan emits
/// DELETE(old) + INSERT(new); the old partition-key triggers recompute of
/// partition A, the new partition-key triggers recompute of partition B.
#[tokio::test]
async fn test_window_differential_partition_key_change() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_pkc (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_pkc (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('eng', 60),
         ('sales', 90), ('sales', 70)",
    )
    .await;

    db.create_st(
        "wf_pkc_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_pkc",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.wf_pkc_st").await, 5);

    // Move the top eng employee (salary=100) to the sales partition.
    // This changes the PARTITION BY key (dept), so:
    // - Old partition (eng): must recompute without salary=100
    //   → eng now has [80, 60] → rn=[1, 2]
    // - New partition (sales): must recompute with salary=100
    //   → sales now has [100, 90, 70] → rn=[1, 2, 3]
    db.execute("UPDATE wf_pkc SET dept = 'sales' WHERE dept = 'eng' AND salary = 100")
        .await;
    db.refresh_st("wf_pkc_st").await;

    // Total row count unchanged — 5 rows
    assert_eq!(db.count("public.wf_pkc_st").await, 5);

    // Verify the full result matches a from-scratch execution
    db.assert_st_matches_query(
        "public.wf_pkc_st",
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM public.wf_pkc",
    )
    .await;

    // Verify specific partition membership:
    // salary=100 should now be in sales partition, rn=1
    let dept_100: String = db
        .query_scalar("SELECT dept FROM public.wf_pkc_st WHERE salary = 100")
        .await;
    assert_eq!(dept_100, "sales", "salary=100 should have moved to sales");

    let rn_100: i64 = db
        .query_scalar("SELECT rn FROM public.wf_pkc_st WHERE salary = 100")
        .await;
    assert_eq!(rn_100, 1, "salary=100 should be rn=1 in sales");

    // Old partition: salary=80 should now be rn=1 in eng
    let rn_80: i64 = db
        .query_scalar("SELECT rn FROM public.wf_pkc_st WHERE dept = 'eng' AND salary = 80")
        .await;
    assert_eq!(rn_80, 1, "salary=80 should be rn=1 in eng after key change");
}

/// Test partition key change with SUM() OVER — verifies aggregate
/// window functions also update correctly when rows move between partitions.
#[tokio::test]
async fn test_window_differential_partition_key_change_sum() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_pkcs (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_pkcs (dept, salary) VALUES
         ('eng', 100), ('eng', 80),
         ('sales', 90), ('sales', 70)",
    )
    .await;

    db.create_st(
        "wf_pkcs_st",
        "SELECT dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM public.wf_pkcs",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Move salary=100 from eng to sales
    db.execute("UPDATE wf_pkcs SET dept = 'sales' WHERE salary = 100")
        .await;
    db.refresh_st("wf_pkcs_st").await;

    // Verify the result matches from-scratch query
    db.assert_st_matches_query(
        "public.wf_pkcs_st",
        "SELECT dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM public.wf_pkcs",
    )
    .await;

    // eng total should now be 80 (only salary=80 remains)
    let eng_total: i64 = db
        .query_scalar("SELECT dept_total FROM public.wf_pkcs_st WHERE dept = 'eng' LIMIT 1")
        .await;
    assert_eq!(eng_total, 80, "eng total should be 80 after move");

    // sales total should now be 90+70+100=260
    let sales_total: i64 = db
        .query_scalar("SELECT dept_total FROM public.wf_pkcs_st WHERE dept = 'sales' LIMIT 1")
        .await;
    assert_eq!(sales_total, 260, "sales total should be 260 after move");
}

#[tokio::test]
async fn test_window_with_nulls() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE window_null_src (id INT, grp INT, val INT)")
        .await;

    db.execute("INSERT INTO window_null_src VALUES (1, NULL, 10), (2, 1, NULL), (3, NULL, NULL), (4, 1, 20)").await;

    let q =
        "SELECT id, grp, val, SUM(val) OVER (PARTITION BY grp) as s FROM public.window_null_src";

    db.create_st("window_null_st", q, "1m", "DIFFERENTIAL")
        .await;

    db.assert_st_matches_query("window_null_st", q).await;

    db.execute("INSERT INTO window_null_src VALUES (5, NULL, 30)")
        .await;
    db.refresh_st("window_null_st").await;

    db.assert_st_matches_query("window_null_st", q).await;
}

// ── TG2-WIN: LAG / LEAD differential tests ───────────────────────────

#[tokio::test]
async fn test_window_lag_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE wf_lag (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_lag (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('sales', 90), ('sales', 70)",
    )
    .await;

    let q = "SELECT dept, salary, LAG(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS prev_salary FROM public.wf_lag";
    db.create_st("wf_lag_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("wf_lag_st", q).await;

    // INSERT: new row shifts LAG values
    db.execute("INSERT INTO wf_lag (dept, salary) VALUES ('eng', 95)")
        .await;
    db.refresh_st("wf_lag_st").await;
    db.assert_st_matches_query("wf_lag_st", q).await;

    // UPDATE: change salary to shift ordering
    db.execute("UPDATE wf_lag SET salary = 110 WHERE salary = 80")
        .await;
    db.refresh_st("wf_lag_st").await;
    db.assert_st_matches_query("wf_lag_st", q).await;

    // DELETE: remove a row, LAG recalculated
    db.execute("DELETE FROM wf_lag WHERE salary = 95").await;
    db.refresh_st("wf_lag_st").await;
    db.assert_st_matches_query("wf_lag_st", q).await;
}

#[tokio::test]
async fn test_window_lead_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE wf_lead (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_lead (dept, salary) VALUES
         ('eng', 100), ('eng', 80), ('eng', 60), ('sales', 90)",
    )
    .await;

    let q = "SELECT dept, salary, LEAD(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS next_salary FROM public.wf_lead";
    db.create_st("wf_lead_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("wf_lead_st", q).await;

    // INSERT: new row into a different partition
    db.execute("INSERT INTO wf_lead (dept, salary) VALUES ('sales', 70)")
        .await;
    db.refresh_st("wf_lead_st").await;
    db.assert_st_matches_query("wf_lead_st", q).await;

    // UPDATE: move row to new partition
    db.execute("UPDATE wf_lead SET dept = 'sales' WHERE salary = 60")
        .await;
    db.refresh_st("wf_lead_st").await;
    db.assert_st_matches_query("wf_lead_st", q).await;

    // DELETE: remove top earner, LEAD shifts
    db.execute("DELETE FROM wf_lead WHERE salary = 100").await;
    db.refresh_st("wf_lead_st").await;
    db.assert_st_matches_query("wf_lead_st", q).await;
}

#[tokio::test]
async fn test_window_dense_rank_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE wf_dr (id SERIAL PRIMARY KEY, dept TEXT NOT NULL, salary INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO wf_dr (dept, salary) VALUES
         ('eng', 100), ('eng', 100), ('eng', 80), ('sales', 90), ('sales', 70)",
    )
    .await;

    let q = "SELECT dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dr FROM public.wf_dr";
    db.create_st("wf_dr_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("wf_dr_st", q).await;

    // INSERT: tied salary — DENSE_RANK should not increment
    db.execute("INSERT INTO wf_dr (dept, salary) VALUES ('eng', 100)")
        .await;
    db.refresh_st("wf_dr_st").await;
    db.assert_st_matches_query("wf_dr_st", q).await;

    // UPDATE: break a tie
    db.execute("UPDATE wf_dr SET salary = 90 WHERE dept = 'eng' AND salary = 80")
        .await;
    db.refresh_st("wf_dr_st").await;
    db.assert_st_matches_query("wf_dr_st", q).await;

    // DELETE: remove tied row
    db.execute(
        "DELETE FROM wf_dr WHERE id = (SELECT min(id) FROM wf_dr WHERE dept = 'eng' AND salary = 100)",
    )
    .await;
    db.refresh_st("wf_dr_st").await;
    db.assert_st_matches_query("wf_dr_st", q).await;
}
