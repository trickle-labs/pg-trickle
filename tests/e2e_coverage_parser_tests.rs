//! E2E tests for parser edge cases and query pattern coverage.
//!
//! Phase 3 of PLAN_COVERAGE_2.md — exercises parser code paths
//! for HAVING, CASE, complex JOINs, subqueries in FROM, and
//! advanced window frames.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── HAVING Clause ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_aggregate_with_having_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE having_src (id INT PRIMARY KEY, region TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO having_src VALUES \
         (1, 'east', 100), (2, 'east', 200), (3, 'west', 50), \
         (4, 'west', 75), (5, 'north', 300)",
    )
    .await;

    db.create_st(
        "having_st",
        "SELECT region, SUM(amount) AS total FROM having_src GROUP BY region HAVING SUM(amount) > 100",
        "1m",
        "FULL",
    )
    .await;

    // east=300 (>100), west=125 (>100), north=300 (>100) → 3 rows
    let count = db.count("public.having_st").await;
    assert_eq!(count, 3, "All three regions exceed 100");

    // Insert data to make north drop below threshold via update-by-delete
    db.execute("DELETE FROM having_src WHERE id = 3").await; // west now 75 (<= 100? no, 75)
    db.execute("DELETE FROM having_src WHERE id = 4").await; // west now 0

    db.refresh_st("having_st").await;

    // west is gone (0 total), east=300, north=300 → 2 rows
    let count = db.count("public.having_st").await;
    assert_eq!(
        count, 2,
        "West should be filtered out by HAVING after delete"
    );
}

#[tokio::test]
async fn test_aggregate_with_having_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE having_inc_src (id INT PRIMARY KEY, category TEXT, score INT)")
        .await;
    db.execute(
        "INSERT INTO having_inc_src VALUES \
         (1, 'A', 10), (2, 'A', 20), (3, 'B', 5), (4, 'B', 3)",
    )
    .await;

    db.create_st(
        "having_inc_st",
        "SELECT category, SUM(score) AS total_score FROM having_inc_src GROUP BY category HAVING SUM(score) > 5",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // A=30 (>5), B=8 (>5) → 2 rows
    let count = db.count("public.having_inc_st").await;
    assert_eq!(count, 2);

    // Add more data and refresh
    db.execute("INSERT INTO having_inc_src VALUES (5, 'C', 100)")
        .await;
    db.refresh_st("having_inc_st").await;

    let count = db.count("public.having_inc_st").await;
    assert_eq!(count, 3, "Category C should appear after insert + refresh");
}

// ── CASE Expressions ───────────────────────────────────────────────────

#[tokio::test]
async fn test_case_expression_in_defining_query() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE case_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO case_src VALUES (1, 10), (2, 50), (3, 100)")
        .await;

    db.create_st(
        "case_st",
        "SELECT id, CASE WHEN val < 25 THEN 'low' WHEN val < 75 THEN 'mid' ELSE 'high' END AS bucket FROM case_src",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.case_st").await;
    assert_eq!(count, 3);

    // Verify values
    let low_count: i64 = db
        .query_scalar("SELECT count(*) FROM public.case_st WHERE bucket = 'low'")
        .await;
    assert_eq!(low_count, 1);

    let high_count: i64 = db
        .query_scalar("SELECT count(*) FROM public.case_st WHERE bucket = 'high'")
        .await;
    assert_eq!(high_count, 1);
}

#[tokio::test]
async fn test_case_expression_in_aggregate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE case_agg_src (id INT PRIMARY KEY, status TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO case_agg_src VALUES \
         (1, 'active', 100), (2, 'inactive', 50), (3, 'active', 200), (4, 'inactive', 75)",
    )
    .await;

    db.create_st(
        "case_agg_st",
        "SELECT status, SUM(CASE WHEN amount > 60 THEN amount ELSE 0 END) AS big_total FROM case_agg_src GROUP BY status",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.case_agg_st").await;
    assert_eq!(count, 2, "Two status groups");

    // active: 100+200=300 (both > 60), inactive: 0+75=75 (only 75 > 60)
    let active_total: i64 = db
        .query_scalar("SELECT big_total::bigint FROM public.case_agg_st WHERE status = 'active'")
        .await;
    assert_eq!(active_total, 300);
}

// ── Complex JOINs ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_multi_condition_join() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mc_orders (id INT PRIMARY KEY, customer_id INT, region TEXT, amount NUMERIC)",
    )
    .await;
    db.execute("CREATE TABLE mc_customers (id INT PRIMARY KEY, region TEXT, name TEXT)")
        .await;

    db.execute("INSERT INTO mc_customers VALUES (1, 'east', 'Alice'), (2, 'west', 'Bob')")
        .await;
    db.execute(
        "INSERT INTO mc_orders VALUES \
         (1, 1, 'east', 100), (2, 2, 'west', 200), (3, 1, 'west', 50)",
    )
    .await;

    // Join on both customer_id and region (multi-condition ON)
    db.create_st(
        "mc_join_st",
        "SELECT o.id, c.name, o.amount FROM mc_orders o JOIN mc_customers c ON o.customer_id = c.id AND o.region = c.region",
        "1m",
        "FULL",
    )
    .await;

    // Only orders where customer_id AND region match: order 1 (Alice, east) and order 2 (Bob, west)
    // Order 3 (customer_id=1=Alice, region=west) doesn't match Alice's region (east)
    let count = db.count("public.mc_join_st").await;
    assert_eq!(count, 2, "Multi-condition join should match 2 rows");
}

#[tokio::test]
async fn test_left_join_with_null_handling() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lj_products (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE lj_reviews (id INT PRIMARY KEY, product_id INT, rating INT)")
        .await;

    db.execute("INSERT INTO lj_products VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey')")
        .await;
    db.execute("INSERT INTO lj_reviews VALUES (1, 1, 5), (2, 1, 4), (3, 2, 3)")
        .await;

    db.create_st(
        "lj_st",
        "SELECT p.id, p.name, COALESCE(AVG(r.rating), 0) AS avg_rating FROM lj_products p LEFT JOIN lj_reviews r ON p.id = r.product_id GROUP BY p.id, p.name",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.lj_st").await;
    assert_eq!(count, 3, "LEFT JOIN should include all products");

    // Doohickey has no reviews, COALESCE should give 0
    let doohickey_rating: i64 = db
        .query_scalar("SELECT avg_rating::bigint FROM public.lj_st WHERE name = 'Doohickey'")
        .await;
    assert_eq!(doohickey_rating, 0);
}

// ── Subquery in FROM (Derived Table) ───────────────────────────────────

#[tokio::test]
async fn test_subquery_in_from_clause() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sub_src (id INT PRIMARY KEY, category TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO sub_src VALUES \
         (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)",
    )
    .await;

    db.create_st(
        "sub_st",
        "SELECT category, total FROM (SELECT category, SUM(val) AS total FROM sub_src GROUP BY category) sub WHERE total > 25",
        "1m",
        "FULL",
    )
    .await;

    // A=30 (>25), B=70 (>25) → 2 rows
    let count = db.count("public.sub_st").await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_subquery_in_from_with_join() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sub_join_facts (id INT PRIMARY KEY, dim_id INT, measure INT)")
        .await;
    db.execute("CREATE TABLE sub_join_dims (id INT PRIMARY KEY, label TEXT)")
        .await;

    db.execute("INSERT INTO sub_join_dims VALUES (1, 'Alpha'), (2, 'Beta')")
        .await;
    db.execute("INSERT INTO sub_join_facts VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)")
        .await;

    db.create_st(
        "sub_join_st",
        "SELECT d.label, f.total FROM sub_join_dims d JOIN (SELECT dim_id, SUM(measure) AS total FROM sub_join_facts GROUP BY dim_id) f ON d.id = f.dim_id",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.sub_join_st").await;
    assert_eq!(count, 2, "Should have one row per dimension");

    let alpha_total: i64 = db
        .query_scalar("SELECT total::bigint FROM public.sub_join_st WHERE label = 'Alpha'")
        .await;
    assert_eq!(alpha_total, 300);
}

// ── Advanced Window Frames ─────────────────────────────────────────────

#[tokio::test]
async fn test_window_rows_between_frame() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE win_frame_src (id INT PRIMARY KEY, ts DATE, val INT)")
        .await;
    db.execute(
        "INSERT INTO win_frame_src VALUES \
         (1, '2026-01-01', 10), (2, '2026-01-02', 20), \
         (3, '2026-01-03', 30), (4, '2026-01-04', 40), \
         (5, '2026-01-05', 50)",
    )
    .await;

    // 3-day moving average using ROWS BETWEEN
    db.create_st(
        "win_frame_st",
        "SELECT id, ts, val, AVG(val) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg FROM public.win_frame_src",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.win_frame_st").await;
    assert_eq!(count, 5);

    // Middle row (id=3): avg of 20,30,40 = 30
    let mid_avg: i64 = db
        .query_scalar("SELECT moving_avg::bigint FROM public.win_frame_st WHERE id = 3")
        .await;
    assert_eq!(mid_avg, 30, "Moving average of 20,30,40 should be 30");
}

#[tokio::test]
async fn test_window_range_frame() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE win_range_src (id INT PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO win_range_src VALUES \
         (1, 'A', 10), (2, 'A', 20), (3, 'A', 30), \
         (4, 'B', 100), (5, 'B', 200)",
    )
    .await;

    // Cumulative sum with RANGE
    db.create_st(
        "win_range_st",
        "SELECT id, grp, val, SUM(val) OVER (PARTITION BY grp ORDER BY val RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumsum FROM public.win_range_src",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.win_range_st").await;
    assert_eq!(count, 5);

    // A group: 10, 30 (10+20), 60 (10+20+30)
    let a_last: i64 = db
        .query_scalar("SELECT cumsum::bigint FROM public.win_range_st WHERE id = 3")
        .await;
    assert_eq!(a_last, 60, "Cumulative sum for A group should be 60");
}

#[tokio::test]
async fn test_window_nth_value() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE win_nth_src (id INT PRIMARY KEY, dept TEXT, salary INT)")
        .await;
    db.execute(
        "INSERT INTO win_nth_src VALUES \
         (1, 'eng', 100), (2, 'eng', 120), (3, 'eng', 90), \
         (4, 'sales', 80), (5, 'sales', 110)",
    )
    .await;

    db.create_st(
        "win_nth_st",
        "SELECT id, dept, salary, NTH_VALUE(salary, 2) OVER (PARTITION BY dept ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_highest FROM public.win_nth_src",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.win_nth_st").await;
    assert_eq!(count, 5);

    // eng: sorted desc = 120, 100, 90 → 2nd = 100
    let eng_second: i64 = db
        .query_scalar(
            "SELECT DISTINCT second_highest::bigint FROM public.win_nth_st WHERE dept = 'eng'",
        )
        .await;
    assert_eq!(eng_second, 100);
}
