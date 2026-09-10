//! E2E tests for multi-partition window functions (F22: G5.2).
//!
//! Validates multiple window functions with different PARTITION BY,
//! ORDER BY, and frame clauses under differential refresh.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Multiple window functions with different partitions
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multi_window_different_partitions_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE mw_sales (id SERIAL PRIMARY KEY, region TEXT, dept TEXT, amount INT)")
        .await;
    db.execute(
        "INSERT INTO mw_sales (region, dept, amount) VALUES \
         ('east', 'eng', 100), ('east', 'eng', 200), \
         ('east', 'sales', 150), ('west', 'eng', 300)",
    )
    .await;

    let q = "SELECT region, dept, amount, \
             ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS region_rank, \
             RANK() OVER (PARTITION BY dept ORDER BY amount DESC) AS dept_rank \
             FROM public.mw_sales";
    db.create_st("mw_part_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("mw_part_st", q).await;

    db.execute("INSERT INTO mw_sales (region, dept, amount) VALUES ('west', 'sales', 500)")
        .await;
    db.refresh_st("mw_part_st").await;
    db.assert_st_matches_query("mw_part_st", q).await;

    db.execute("DELETE FROM mw_sales WHERE amount = 100").await;
    db.refresh_st("mw_part_st").await;
    db.assert_st_matches_query("mw_part_st", q).await;

    db.execute("UPDATE mw_sales SET amount = 999 WHERE region = 'east' AND dept = 'sales'")
        .await;
    db.refresh_st("mw_part_st").await;
    db.assert_st_matches_query("mw_part_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Window frame clauses (ROWS/RANGE)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_window_frame_rows_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE wf_ts (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO wf_ts (grp, val) VALUES \
         ('a', 10), ('a', 20), ('a', 30), ('a', 40), ('b', 100)",
    )
    .await;

    let q = "SELECT grp, val, \
             SUM(val) OVER (PARTITION BY grp ORDER BY val \
                 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling_sum \
             FROM public.wf_ts";
    db.create_st("wf_rows_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("wf_rows_st", q).await;

    db.execute("INSERT INTO wf_ts (grp, val) VALUES ('a', 25)")
        .await;
    db.refresh_st("wf_rows_st").await;
    db.assert_st_matches_query("wf_rows_st", q).await;

    db.execute("DELETE FROM wf_ts WHERE val = 30").await;
    db.refresh_st("wf_rows_st").await;
    db.assert_st_matches_query("wf_rows_st", q).await;
}

#[tokio::test]
async fn test_window_frame_range_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE wf_range (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO wf_range (grp, val) VALUES \
         ('a', 10), ('a', 20), ('a', 30), ('b', 100)",
    )
    .await;

    let q = "SELECT grp, val, \
             AVG(val) OVER (PARTITION BY grp ORDER BY val \
                 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg \
             FROM public.wf_range";
    db.create_st("wf_range_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("wf_range_st", q).await;

    db.execute("INSERT INTO wf_range (grp, val) VALUES ('a', 15)")
        .await;
    db.refresh_st("wf_range_st").await;
    db.assert_st_matches_query("wf_range_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// LAG / LEAD / NTH_VALUE
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_window_lag_lead_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE wf_ll (id SERIAL PRIMARY KEY, grp TEXT, seq INT, val INT)")
        .await;
    db.execute(
        "INSERT INTO wf_ll (grp, seq, val) VALUES \
         ('a', 1, 10), ('a', 2, 20), ('a', 3, 30), ('b', 1, 100)",
    )
    .await;

    let q = "SELECT grp, seq, val, \
             LAG(val) OVER (PARTITION BY grp ORDER BY seq) AS prev_val, \
             LEAD(val) OVER (PARTITION BY grp ORDER BY seq) AS next_val \
             FROM public.wf_ll";
    db.create_st("wf_ll_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("wf_ll_st", q).await;

    db.execute("INSERT INTO wf_ll (grp, seq, val) VALUES ('a', 4, 40)")
        .await;
    db.refresh_st("wf_ll_st").await;
    db.assert_st_matches_query("wf_ll_st", q).await;

    db.execute("DELETE FROM wf_ll WHERE grp = 'a' AND seq = 2")
        .await;
    db.refresh_st("wf_ll_st").await;
    db.assert_st_matches_query("wf_ll_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// DENSE_RANK / PERCENT_RANK / CUME_DIST / NTILE
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_window_ranking_functions_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE wf_rank (id SERIAL PRIMARY KEY, dept TEXT, salary INT)")
        .await;
    // Use unique (dept, salary) pairs to avoid hash collisions on __pgt_row_id.
    // Duplicate pass-through values are a known limitation (see keyless_duplicate_tests).
    db.execute(
        "INSERT INTO wf_rank (dept, salary) VALUES \
         ('eng', 100), ('eng', 150), ('eng', 200), ('sales', 150), ('sales', 300)",
    )
    .await;

    let q = "SELECT dept, salary, \
             DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) AS drank, \
             NTILE(2) OVER (PARTITION BY dept ORDER BY salary) AS tile \
             FROM public.wf_rank";
    db.create_st("wf_rank_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("wf_rank_st", q).await;

    db.execute("INSERT INTO wf_rank (dept, salary) VALUES ('eng', 175)")
        .await;
    db.refresh_st("wf_rank_st").await;
    db.assert_st_matches_query("wf_rank_st", q).await;

    db.execute("DELETE FROM wf_rank WHERE dept = 'eng' AND salary = 100")
        .await;
    db.refresh_st("wf_rank_st").await;
    db.assert_st_matches_query("wf_rank_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Window + aggregate combined
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_window_over_aggregate_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE wa_data (id SERIAL PRIMARY KEY, region TEXT, dept TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO wa_data (region, dept, val) VALUES \
         ('east', 'eng', 100), ('east', 'eng', 200), \
         ('east', 'sales', 150), ('west', 'eng', 300), ('west', 'sales', 50)",
    )
    .await;

    let q = "SELECT region, dept, SUM(val) AS dept_total, \
             RANK() OVER (PARTITION BY region ORDER BY SUM(val) DESC) AS rank_in_region \
             FROM public.wa_data GROUP BY region, dept";
    db.create_st("wa_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("wa_st", q).await;

    // Shift sales above eng in east
    db.execute("INSERT INTO wa_data (region, dept, val) VALUES ('east', 'sales', 500)")
        .await;
    db.refresh_st("wa_st").await;
    db.assert_st_matches_query("wa_st", q).await;

    db.execute("DELETE FROM wa_data WHERE region = 'west' AND dept = 'sales'")
        .await;
    db.refresh_st("wa_st").await;
    db.assert_st_matches_query("wa_st", q).await;
}

#[tokio::test]
async fn test_multi_window_lag_lead_nulls() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE mwin_ll_src (id INT, grp INT, val INT)")
        .await;
    db.execute("INSERT INTO mwin_ll_src VALUES (1, 1, NULL), (2, 1, 10), (3, 1, NULL)")
        .await;

    let q = "SELECT id, grp, val, LAG(val) OVER (PARTITION BY grp ORDER BY id) as l1, LEAD(val) OVER (PARTITION BY grp ORDER BY id) as l2 FROM public.mwin_ll_src";

    db.create_st("mwin_ll_st", q, "1m", "DIFFERENTIAL").await;

    db.assert_st_matches_query("mwin_ll_st", q).await;

    db.execute("INSERT INTO mwin_ll_src VALUES (4, 1, 40)")
        .await;
    db.refresh_st("mwin_ll_st").await;

    db.assert_st_matches_query("mwin_ll_st", q).await;
}
