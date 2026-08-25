//! E2E tests for TopK (ORDER BY … LIMIT N) stream tables.
//!
//! Validates that queries with a top-level ORDER BY + LIMIT are accepted,
//! correctly materialized, and refreshed via the scoped-recomputation
//! (MERGE-based) path.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Creation ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_create_basic() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_src (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_src VALUES (1,10),(2,50),(3,30),(4,40),(5,20)")
        .await;

    let query = "SELECT id, score FROM topk_src ORDER BY score DESC LIMIT 3";
    db.create_st("topk_basic", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_basic", query).await;

    // Should have exactly 3 rows — the top 3 by score
    assert_eq!(db.count("public.topk_basic").await, 3);

    // Verify the correct rows are present (scores 50, 40, 30)
    let top_score: i32 = db
        .query_scalar("SELECT score FROM public.topk_basic ORDER BY score DESC LIMIT 1")
        .await;
    assert_eq!(top_score, 50);

    let min_score: i32 = db
        .query_scalar("SELECT score FROM public.topk_basic ORDER BY score ASC LIMIT 1")
        .await;
    assert_eq!(min_score, 30);
}

#[tokio::test]
async fn test_topk_create_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_diff_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO topk_diff_src VALUES (1,100),(2,200),(3,300)")
        .await;

    // TopK tables should be accepted even with DIFFERENTIAL mode
    let query = "SELECT id, val FROM topk_diff_src ORDER BY val DESC LIMIT 2";
    db.create_st("topk_diff", query, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("topk_diff", query).await;

    assert_eq!(db.count("public.topk_diff").await, 2);
}

#[tokio::test]
async fn test_topk_volatility_uses_resolved_overload() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE FUNCTION topk_probe(int) RETURNS int \
         LANGUAGE sql IMMUTABLE AS 'SELECT $1'",
    )
    .await;
    db.execute(
        "CREATE FUNCTION topk_probe(text) RETURNS int \
         LANGUAGE sql VOLATILE AS 'SELECT length($1)'",
    )
    .await;
    db.execute("CREATE TABLE topk_vol_src (id INT PRIMARY KEY, score INT, label TEXT)")
        .await;
    db.execute("INSERT INTO topk_vol_src VALUES (1, 10, 'a'), (2, 30, 'bbb'), (3, 20, 'cc')")
        .await;

    let immutable_query =
        "SELECT id, score FROM topk_vol_src ORDER BY topk_probe(score) DESC LIMIT 2";
    db.create_st(
        "topk_vol_immutable_st",
        immutable_query,
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("public.topk_vol_immutable_st", immutable_query)
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
             'topk_vol_rejected_st', \
             $$ SELECT id, score FROM topk_vol_src \
                ORDER BY topk_probe(label) DESC LIMIT 2 $$, \
             '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        result.is_err(),
        "TopK must reject the resolved VOLATILE overload"
    );
}

// ── Catalog ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_catalog_fields_populated() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_cat (id INT PRIMARY KEY, rank INT)")
        .await;
    db.execute("INSERT INTO topk_cat VALUES (1,1),(2,2),(3,3)")
        .await;

    let query = "SELECT id, rank FROM topk_cat ORDER BY rank ASC LIMIT 2";
    db.create_st("topk_cat_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_cat_st", query).await;

    let topk_limit: i32 = db
        .query_scalar(
            "SELECT topk_limit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'topk_cat_st'",
        )
        .await;
    assert_eq!(topk_limit, 2, "topk_limit should be stored in catalog");

    let topk_order_by: String = db
        .query_scalar(
            "SELECT topk_order_by FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'topk_cat_st'",
        )
        .await;
    assert!(
        topk_order_by.to_lowercase().contains("rank"),
        "topk_order_by should contain 'rank', got: {}",
        topk_order_by
    );
}

#[tokio::test]
async fn test_topk_monitoring_view_shows_is_topk() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_mon (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO topk_mon VALUES (1,1),(2,2)").await;

    let query = "SELECT id, v FROM topk_mon ORDER BY v DESC LIMIT 1";
    db.create_st("topk_mon_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_mon_st", query).await;

    let is_topk: bool = db
        .query_scalar(
            "SELECT is_topk FROM pgtrickle.stream_tables_info WHERE pgt_name = 'topk_mon_st'",
        )
        .await;
    assert!(is_topk, "is_topk should be true in monitoring view");
}

// ── Refresh — Inserts ──────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_refresh_new_row_enters_top_n() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_ins (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_ins VALUES (1,10),(2,20),(3,30)")
        .await;

    let query = "SELECT id, score FROM topk_ins ORDER BY score DESC LIMIT 2";
    db.create_st("topk_ins_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_ins_st", query).await;
    assert_eq!(db.count("public.topk_ins_st").await, 2);

    // Insert a row with a higher score that should enter the top 2
    db.execute("INSERT INTO topk_ins VALUES (4, 50)").await;
    db.refresh_st("topk_ins_st").await;
    db.assert_st_matches_query("topk_ins_st", query).await;

    assert_eq!(db.count("public.topk_ins_st").await, 2);

    // The new row (score=50) should be present, old bottom (score=20) should be gone
    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_ins_st")
        .await;
    assert_eq!(max_score, 50);

    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_ins_st")
        .await;
    assert_eq!(min_score, 30, "Bottom of top-2 should now be 30");
}

#[tokio::test]
async fn test_topk_refresh_new_row_below_cutoff() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_below (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_below VALUES (1,100),(2,200),(3,300)")
        .await;

    let query = "SELECT id, score FROM topk_below ORDER BY score DESC LIMIT 2";
    db.create_st("topk_below_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_below_st", query).await;

    // Insert a row below the top-2 cutoff
    db.execute("INSERT INTO topk_below VALUES (4, 50)").await;
    db.refresh_st("topk_below_st").await;
    db.assert_st_matches_query("topk_below_st", query).await;

    assert_eq!(db.count("public.topk_below_st").await, 2);

    // Top-2 should be unchanged (300, 200)
    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_below_st")
        .await;
    assert_eq!(min_score, 200, "Top-2 should be unchanged");
}

// ── Refresh — Updates ──────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_refresh_update_changes_ranking() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_upd (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_upd VALUES (1,10),(2,20),(3,30)")
        .await;

    let query = "SELECT id, score FROM topk_upd ORDER BY score DESC LIMIT 2";
    db.create_st("topk_upd_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_upd_st", query).await;

    // Update the lowest-scoring row to become the highest
    db.execute("UPDATE topk_upd SET score = 100 WHERE id = 1")
        .await;
    db.refresh_st("topk_upd_st").await;
    db.assert_st_matches_query("topk_upd_st", query).await;

    assert_eq!(db.count("public.topk_upd_st").await, 2);

    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_upd_st")
        .await;
    assert_eq!(max_score, 100, "Updated row should now be top");

    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_upd_st")
        .await;
    assert_eq!(min_score, 30, "Second place should be 30");
}

// ── Refresh — Deletes ──────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_refresh_delete_top_row() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_del (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_del VALUES (1,10),(2,20),(3,30),(4,40)")
        .await;

    let query = "SELECT id, score FROM topk_del ORDER BY score DESC LIMIT 3";
    db.create_st("topk_del_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_del_st", query).await;
    assert_eq!(db.count("public.topk_del_st").await, 3);

    // Delete the highest-scoring row
    db.execute("DELETE FROM topk_del WHERE id = 4").await;
    db.refresh_st("topk_del_st").await;
    db.assert_st_matches_query("topk_del_st", query).await;

    assert_eq!(db.count("public.topk_del_st").await, 3);

    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_del_st")
        .await;
    assert_eq!(max_score, 30, "After deleting 40, new top should be 30");

    // Row with score=10 should now be in the top 3
    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_del_st")
        .await;
    assert_eq!(min_score, 10, "Row with score 10 should now be in top 3");
}

// ── Refresh — Fewer rows than LIMIT ────────────────────────────────────

#[tokio::test]
async fn test_topk_fewer_rows_than_limit() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_few (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO topk_few VALUES (1,10)").await;

    let query = "SELECT id, val FROM topk_few ORDER BY val DESC LIMIT 5";
    db.create_st("topk_few_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_few_st", query).await;

    // Only 1 row exists, LIMIT 5 — should have 1 row
    assert_eq!(db.count("public.topk_few_st").await, 1);

    db.execute("INSERT INTO topk_few VALUES (2,20),(3,30)")
        .await;
    db.refresh_st("topk_few_st").await;
    db.assert_st_matches_query("topk_few_st", query).await;

    assert_eq!(db.count("public.topk_few_st").await, 3);
}

// ── Refresh — Multiple refreshes ───────────────────────────────────────

#[tokio::test]
async fn test_topk_multiple_refreshes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_multi (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_multi VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .await;

    let query = "SELECT id, score FROM topk_multi ORDER BY score DESC LIMIT 3";
    db.create_st("topk_multi_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_multi_st", query).await;
    assert_eq!(db.count("public.topk_multi_st").await, 3);

    // Refresh 1: insert new top row
    db.execute("INSERT INTO topk_multi VALUES (6, 100)").await;
    db.refresh_st("topk_multi_st").await;
    db.assert_st_matches_query("topk_multi_st", query).await;
    assert_eq!(db.count("public.topk_multi_st").await, 3);
    let max1: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_multi_st")
        .await;
    assert_eq!(max1, 100);

    // Refresh 2: delete the new top row
    db.execute("DELETE FROM topk_multi WHERE id = 6").await;
    db.refresh_st("topk_multi_st").await;
    db.assert_st_matches_query("topk_multi_st", query).await;
    assert_eq!(db.count("public.topk_multi_st").await, 3);
    let max2: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_multi_st")
        .await;
    assert_eq!(max2, 50);
}

// ── TopK with JOIN ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_with_join() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_orders (id INT PRIMARY KEY, customer_id INT, amount NUMERIC)")
        .await;
    db.execute("CREATE TABLE topk_customers (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO topk_customers VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')")
        .await;
    db.execute("INSERT INTO topk_orders VALUES (1,1,100),(2,2,200),(3,3,300),(4,1,400),(5,2,500)")
        .await;

    let query = "SELECT o.id, c.name, o.amount FROM topk_orders o JOIN topk_customers c ON o.customer_id = c.id ORDER BY o.amount DESC LIMIT 3";
    db.create_st("topk_join_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_join_st", query).await;

    assert_eq!(db.count("public.topk_join_st").await, 3);

    let top_amount: f64 = db
        .query_scalar("SELECT amount::float8 FROM public.topk_join_st ORDER BY amount DESC LIMIT 1")
        .await;
    assert!((top_amount - 500.0).abs() < 0.01);
}

// ── TopK with aggregate ────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_with_aggregate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_sales (id INT PRIMARY KEY, product TEXT, revenue INT)")
        .await;
    db.execute(
        "INSERT INTO topk_sales VALUES (1,'A',100),(2,'B',200),(3,'A',150),(4,'C',300),(5,'B',50)",
    )
    .await;

    let query = "SELECT product, SUM(revenue) as total FROM topk_sales GROUP BY product ORDER BY total DESC LIMIT 2";
    db.create_st("topk_agg_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_agg_st", query).await;

    assert_eq!(db.count("public.topk_agg_st").await, 2);

    // C=300, A=250, B=250 — top 2 should be C and one of A/B
    let top_total: i64 = db
        .query_scalar("SELECT MAX(total) FROM public.topk_agg_st")
        .await;
    assert_eq!(top_total, 300);
}

// ── Drop ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_drop_stream_table() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_drop_src (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO topk_drop_src VALUES (1,1),(2,2)")
        .await;

    let query = "SELECT id, v FROM topk_drop_src ORDER BY v DESC LIMIT 1";
    db.create_st("topk_drop_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_drop_st", query).await;

    db.drop_st("topk_drop_st").await;

    let exists = db.table_exists("public", "topk_drop_st").await;
    assert!(!exists, "TopK stream table should be dropped");
}

// ── Full refresh ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_full_refresh_matches_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_fr_src (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_fr_src VALUES (1,10),(2,50),(3,30),(4,40),(5,20)")
        .await;

    // Create with DIFFERENTIAL — TopK tables use scoped recomputation for both modes
    let query = "SELECT id, score FROM topk_fr_src ORDER BY score DESC LIMIT 3";
    db.create_st("topk_fr_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("topk_fr_st", query).await;

    assert_eq!(db.count("public.topk_fr_st").await, 3);

    // Mutate and do a manual (full) refresh
    db.execute("INSERT INTO topk_fr_src VALUES (6, 100)").await;
    db.refresh_st("topk_fr_st").await;
    db.assert_st_matches_query("topk_fr_st", query).await;

    assert_eq!(db.count("public.topk_fr_st").await, 3);
    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_fr_st")
        .await;
    assert_eq!(
        max_score, 100,
        "Full refresh should pick up the new top row"
    );
}

// ── No-change skip ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_no_change_skips_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_nc_src (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_nc_src VALUES (1,10),(2,20),(3,30)")
        .await;

    let query = "SELECT id, score FROM topk_nc_src ORDER BY score DESC LIMIT 2";
    db.create_st("topk_nc_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("topk_nc_st", query).await;

    assert_eq!(db.count("public.topk_nc_st").await, 2);

    // Refresh without any source changes — should succeed without error
    // and stream table contents should be unchanged.
    db.refresh_st("topk_nc_st").await;
    db.assert_st_matches_query("topk_nc_st", query).await;
    assert_eq!(db.count("public.topk_nc_st").await, 2);

    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_nc_st")
        .await;
    assert_eq!(
        max_score, 30,
        "Content should be unchanged after no-op refresh"
    );
}

// ── LIMIT edge cases ───────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_limit_zero_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_lz_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO topk_lz_src VALUES (1,10),(2,20)")
        .await;

    let query = "SELECT id, val FROM topk_lz_src ORDER BY val DESC LIMIT 0";
    let result = db
        .try_execute(&format!(
            "SELECT pgtrickle.create_stream_table('topk_lz_st', $${query}$$, '1m', 'FULL')"
        ))
        .await;
    let error = result.expect_err("LIMIT 0 should be rejected");
    assert!(
        error.to_string().contains("topk_limit must be positive"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn test_topk_limit_all_no_topk() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_la_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO topk_la_src VALUES (1,10),(2,20),(3,30)")
        .await;

    // LIMIT ALL is equivalent to no LIMIT — should produce a normal ST, not TopK
    let query = "SELECT id, val FROM topk_la_src ORDER BY val DESC LIMIT ALL";
    db.create_st("topk_la_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_la_st", query).await;

    // All rows should be present (no TopK restriction)
    assert_eq!(db.count("public.topk_la_st").await, 3);

    // Catalog should show no TopK metadata (topk_limit is NULL)
    let has_topk: bool = db
        .query_scalar(
            "SELECT topk_limit IS NOT NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'topk_la_st'",
        )
        .await;
    assert!(!has_topk, "LIMIT ALL should not set topk_limit in catalog");
}

// ── Rejection cases ────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_offset_without_limit_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_rej_off (id INT PRIMARY KEY, val INT)")
        .await;

    // ORDER BY + OFFSET without LIMIT → rejected (unbounded result set)
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('topk_rej_off_st', \
             $$ SELECT id, val FROM topk_rej_off ORDER BY val DESC OFFSET 5 $$, \
             '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_err(),
        "ORDER BY + OFFSET (no LIMIT) should be rejected"
    );
}

#[tokio::test]
async fn test_topk_offset_without_order_by_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_rej_noob (id INT PRIMARY KEY, val INT)")
        .await;

    // LIMIT + OFFSET without ORDER BY → rejected (non-deterministic)
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('topk_rej_noob_st', \
             $$ SELECT id, val FROM topk_rej_noob LIMIT 5 OFFSET 2 $$, \
             '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_err(),
        "LIMIT + OFFSET without ORDER BY should be rejected"
    );
}

#[tokio::test]
async fn test_topk_non_constant_limit_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_rej_nc (id INT PRIMARY KEY, val INT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('topk_rej_nc_st', \
             $$ SELECT id, val FROM topk_rej_nc ORDER BY val DESC LIMIT (SELECT 5) $$, \
             '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_err(),
        "Non-constant LIMIT expression should be rejected"
    );
}

// ── FETCH FIRST syntax as TopK ─────────────────────────────────────────

// ── OFFSET support (ORDER BY + LIMIT + OFFSET) ────────────────────────

#[tokio::test]
async fn test_topk_offset_create_basic() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_off_src (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_off_src VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70)")
        .await;

    // Top 7 by score DESC = 70,60,50,40,30,20,10. LIMIT 3 OFFSET 2 = rows 3-5 = 50,40,30
    let query = "SELECT id, score FROM topk_off_src ORDER BY score DESC LIMIT 3 OFFSET 2";
    db.create_st("topk_off_basic", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_off_basic", query).await;

    assert_eq!(db.count("public.topk_off_basic").await, 3);

    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_off_basic")
        .await;
    assert_eq!(max_score, 50, "Top of page should be 50 (3rd highest)");

    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_off_basic")
        .await;
    assert_eq!(min_score, 30, "Bottom of page should be 30 (5th highest)");
}

#[tokio::test]
async fn test_topk_offset_catalog_metadata() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_offcat (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO topk_offcat VALUES (1,1),(2,2),(3,3)")
        .await;

    let query = "SELECT id, v FROM topk_offcat ORDER BY v DESC LIMIT 2 OFFSET 1";
    db.create_st("topk_offcat_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_offcat_st", query).await;

    let topk_offset: i32 = db
        .query_scalar(
            "SELECT topk_offset FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'topk_offcat_st'",
        )
        .await;
    assert_eq!(topk_offset, 1, "topk_offset should be stored in catalog");

    let topk_limit: i32 = db
        .query_scalar(
            "SELECT topk_limit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'topk_offcat_st'",
        )
        .await;
    assert_eq!(topk_limit, 2, "topk_limit should be stored in catalog");
}

#[tokio::test]
async fn test_topk_offset_zero_is_no_offset() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_off0 (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_off0 VALUES (1,10),(2,50),(3,30)")
        .await;

    // OFFSET 0 is semantically equivalent to no OFFSET
    let query = "SELECT id, score FROM topk_off0 ORDER BY score DESC LIMIT 2 OFFSET 0";
    db.create_st("topk_off0_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_off0_st", query).await;

    assert_eq!(db.count("public.topk_off0_st").await, 2);

    // Should get top 2: 50, 30 (same as no offset)
    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_off0_st")
        .await;
    assert_eq!(max_score, 50);

    // topk_offset should be NULL (OFFSET 0 treated as no offset)
    let has_offset: bool = db
        .query_scalar(
            "SELECT topk_offset IS NOT NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'topk_off0_st'",
        )
        .await;
    assert!(
        !has_offset,
        "OFFSET 0 should not set topk_offset in catalog"
    );
}

#[tokio::test]
async fn test_topk_offset_refresh_page_shifts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_offref (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_offref VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .await;

    // DESC order: 50,40,30,20,10. LIMIT 2 OFFSET 1 → rows 2-3 = 40,30
    let query = "SELECT id, score FROM topk_offref ORDER BY score DESC LIMIT 2 OFFSET 1";
    db.create_st("topk_offref_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_offref_st", query).await;

    assert_eq!(db.count("public.topk_offref_st").await, 2);
    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_offref_st")
        .await;
    assert_eq!(max_score, 40);

    // Insert a row with score 45 → DESC: 50,45,40,30,20,10. OFFSET 1 LIMIT 2 → 45,40
    db.execute("INSERT INTO topk_offref VALUES (6, 45)").await;
    db.refresh_st("topk_offref_st").await;
    db.assert_st_matches_query("topk_offref_st", query).await;

    assert_eq!(db.count("public.topk_offref_st").await, 2);
    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_offref_st")
        .await;
    assert_eq!(max_score, 45, "Page should shift: new second-highest is 45");

    let min_score: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_offref_st")
        .await;
    assert_eq!(min_score, 40, "Third-highest is now 40");
}

#[tokio::test]
async fn test_topk_offset_with_aggregates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_offagg (id INT PRIMARY KEY, dept TEXT, salary INT)")
        .await;
    db.execute(
        "INSERT INTO topk_offagg VALUES \
         (1,'A',100),(2,'A',200),(3,'B',300),(4,'B',400),(5,'C',500),(6,'C',600),(7,'D',50)",
    )
    .await;

    // dept totals: D=50, A=300, B=700, C=1100. DESC: C=1100, B=700, A=300, D=50
    // LIMIT 2 OFFSET 1 → B=700, A=300
    let query = "SELECT dept, SUM(salary) AS total FROM topk_offagg GROUP BY dept ORDER BY total DESC LIMIT 2 OFFSET 1";
    db.create_st("topk_offagg_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_offagg_st", query).await;

    assert_eq!(db.count("public.topk_offagg_st").await, 2);

    let max_total: i64 = db
        .query_scalar("SELECT MAX(total) FROM public.topk_offagg_st")
        .await;
    assert_eq!(max_total, 700, "Second-highest dept total is B=700");

    let min_total: i64 = db
        .query_scalar("SELECT MIN(total) FROM public.topk_offagg_st")
        .await;
    assert_eq!(min_total, 300, "Third-highest dept total is A=300");
}

#[tokio::test]
async fn test_topk_offset_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_offdiff (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_offdiff VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .await;

    // DESC: 50,40,30,20,10. LIMIT 2 OFFSET 2 → 30,20
    let query = "SELECT id, score FROM topk_offdiff ORDER BY score DESC LIMIT 2 OFFSET 2";
    db.create_st("topk_offdiff_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("topk_offdiff_st", query).await;

    assert_eq!(db.count("public.topk_offdiff_st").await, 2);

    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_offdiff_st")
        .await;
    assert_eq!(max_score, 30);

    // Delete score=50 → DESC: 40,30,20,10. OFFSET 2 LIMIT 2 → 20,10
    db.execute("DELETE FROM topk_offdiff WHERE score = 50")
        .await;
    db.refresh_st("topk_offdiff_st").await;
    db.assert_st_matches_query("topk_offdiff_st", query).await;

    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_offdiff_st")
        .await;
    assert_eq!(max_score, 20, "After delete, page shifts down");
}

#[tokio::test]
async fn test_topk_offset_non_constant_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_rej_ncoff (id INT PRIMARY KEY, val INT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('topk_rej_ncoff_st', \
             $$ SELECT id, val FROM topk_rej_ncoff ORDER BY val DESC LIMIT 10 OFFSET (SELECT 5) $$, \
             '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_err(),
        "Non-constant OFFSET expression should be rejected"
    );
}

// ── FETCH FIRST syntax as TopK ─────────────────────────────────────────

#[tokio::test]
async fn test_topk_fetch_first_syntax_accepted() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_ff_src (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_ff_src VALUES (1,10),(2,50),(3,30)")
        .await;

    // FETCH FIRST N ROWS ONLY with ORDER BY should be accepted as TopK
    let query = "SELECT id, score FROM topk_ff_src ORDER BY score DESC FETCH FIRST 2 ROWS ONLY";
    db.create_st("topk_ff_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_ff_st", query).await;

    assert_eq!(db.count("public.topk_ff_st").await, 2);

    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_ff_st")
        .await;
    assert_eq!(max_score, 50);
}

// ── TopK with WHERE clause ─────────────────────────────────────────────

#[tokio::test]
async fn test_topk_with_where() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_where (id INT PRIMARY KEY, score INT, active BOOLEAN)")
        .await;
    db.execute(
        "INSERT INTO topk_where VALUES \
         (1,10,true),(2,50,false),(3,30,true),(4,40,true),(5,20,true)",
    )
    .await;

    let query = "SELECT id, score FROM topk_where WHERE active ORDER BY score DESC LIMIT 2";
    db.create_st("topk_where_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_where_st", query).await;

    assert_eq!(db.count("public.topk_where_st").await, 2);

    // Top-2 active by score: 40, 30 (50 is inactive)
    let max_score: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_where_st")
        .await;
    assert_eq!(max_score, 40);
}

// ── Alter behavior ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_topk_alter_schedule_works() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_alt_src (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO topk_alt_src VALUES (1,1),(2,2)")
        .await;

    let query = "SELECT id, v FROM topk_alt_src ORDER BY v DESC LIMIT 1";
    db.create_st("topk_alt_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("topk_alt_st", query).await;

    // Altering schedule/status should work on TopK tables
    db.alter_st("topk_alt_st", "schedule => '5m'").await;

    let schedule: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'topk_alt_st'",
        )
        .await;
    assert!(
        schedule.contains("300") || schedule.contains("5m") || schedule.contains("5 min"),
        "Schedule should be updated to 5m, got: {schedule}"
    );
}

// ── Subquery OFFSET without ORDER BY (G2) ──────────────────────────────
// The warning is emitted to the PG log — we can't easily assert on it from
// E2E, but we *can* verify the stream table is created successfully
// (warning is non-fatal) and that the query with ORDER BY + OFFSET also works.

#[tokio::test]
async fn test_subquery_offset_without_order_by_accepted_with_warning() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sub_off_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO sub_off_src VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .await;

    // Subquery uses OFFSET without ORDER BY — should succeed with a warning
    let query = "SELECT * FROM (SELECT id, val FROM sub_off_src OFFSET 2) sub";
    db.create_st("sub_off_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("sub_off_st", query).await;

    // Stream table should be populated (non-deterministic subset, but 3 rows)
    let count = db.count("public.sub_off_st").await;
    assert_eq!(count, 3, "OFFSET 2 from 5 rows should yield 3 rows");
}

#[tokio::test]
async fn test_subquery_offset_with_order_by_no_warning() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sub_oob_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO sub_oob_src VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .await;

    // Subquery uses OFFSET with ORDER BY — should succeed without warning
    let query = "SELECT * FROM (SELECT id, val FROM sub_oob_src ORDER BY val OFFSET 2) sub";
    db.create_st("sub_oob_st", query, "1m", "FULL").await;
    db.assert_st_matches_query("sub_oob_st", query).await;

    let count = db.count("public.sub_oob_st").await;
    assert_eq!(
        count, 3,
        "OFFSET 2 with ORDER BY from 5 rows should yield 3 rows"
    );
}

// ── IMMEDIATE mode TopK ────────────────────────────────────────────────
//
// TopK stream tables in IMMEDIATE mode use statement-level micro-refresh
// (`apply_topk_micro_refresh`) — the full ORDER BY + LIMIT query is
// re-executed and diffed against the current storage on every DML.

/// Helper: create an IMMEDIATE-mode stream table (NULL schedule).
async fn create_immediate_st(db: &E2eDb, name: &str, query: &str) {
    let sql = format!(
        "SELECT pgtrickle.create_stream_table('{name}', $${query}$$, \
         NULL, 'IMMEDIATE')"
    );
    db.execute(&sql).await;
}

#[tokio::test]
async fn test_topk_immediate_basic_creation() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_imm_src (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_imm_src VALUES (1,10),(2,50),(3,30),(4,40),(5,20)")
        .await;

    create_immediate_st(
        &db,
        "topk_imm_basic",
        "SELECT id, score FROM topk_imm_src ORDER BY score DESC LIMIT 3",
    )
    .await;

    assert_eq!(db.count("public.topk_imm_basic").await, 3);

    // Verify correct rows: top 3 scores are 50, 40, 30
    let max: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_imm_basic")
        .await;
    assert_eq!(max, 50);

    let min: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_imm_basic")
        .await;
    assert_eq!(min, 30);

    // Catalog should record IMMEDIATE mode + TopK metadata
    let (status, mode, populated, _) = db.pgt_status("topk_imm_basic").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);

    let topk_limit: i32 = db
        .query_scalar(
            "SELECT topk_limit FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'topk_imm_basic'",
        )
        .await;
    assert_eq!(topk_limit, 3);
}

#[tokio::test]
async fn test_topk_immediate_insert_enters_top_n() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_imm_ins (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_imm_ins VALUES (1,10),(2,20),(3,30)")
        .await;

    create_immediate_st(
        &db,
        "topk_imm_ins_st",
        "SELECT id, score FROM topk_imm_ins ORDER BY score DESC LIMIT 2",
    )
    .await;

    assert_eq!(db.count("public.topk_imm_ins_st").await, 2);

    // Insert a row with a higher score — should immediately enter top-2
    db.execute("INSERT INTO topk_imm_ins VALUES (4, 50)").await;

    assert_eq!(
        db.count("public.topk_imm_ins_st").await,
        2,
        "Still 2 rows after insert"
    );

    let max: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_imm_ins_st")
        .await;
    assert_eq!(max, 50, "New high-score row should be in the top-2");

    let min: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_imm_ins_st")
        .await;
    assert_eq!(min, 30, "Bottom of top-2 should now be 30");
}

#[tokio::test]
async fn test_topk_immediate_insert_below_threshold_no_change() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_imm_lo (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_imm_lo VALUES (1,100),(2,200),(3,300)")
        .await;

    create_immediate_st(
        &db,
        "topk_imm_lo_st",
        "SELECT id, score FROM topk_imm_lo ORDER BY score DESC LIMIT 2",
    )
    .await;

    // Top-2: 300, 200
    let min_before: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_imm_lo_st")
        .await;
    assert_eq!(min_before, 200);

    // Insert a row below the top-2 threshold — should not change the ST
    db.execute("INSERT INTO topk_imm_lo VALUES (4, 50)").await;

    assert_eq!(db.count("public.topk_imm_lo_st").await, 2);

    let min_after: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_imm_lo_st")
        .await;
    assert_eq!(min_after, 200, "Top-2 unchanged by below-threshold insert");
}

#[tokio::test]
async fn test_topk_immediate_delete_expands_window() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_imm_del (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_imm_del VALUES (1,10),(2,20),(3,30),(4,40)")
        .await;

    create_immediate_st(
        &db,
        "topk_imm_del_st",
        "SELECT id, score FROM topk_imm_del ORDER BY score DESC LIMIT 3",
    )
    .await;

    // Top-3: 40, 30, 20
    assert_eq!(db.count("public.topk_imm_del_st").await, 3);

    // Delete the top row — row with score=10 should now enter the top-3
    db.execute("DELETE FROM topk_imm_del WHERE id = 4").await;

    assert_eq!(
        db.count("public.topk_imm_del_st").await,
        3,
        "Still 3 rows after delete"
    );

    let max: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_imm_del_st")
        .await;
    assert_eq!(max, 30, "New top score after deleting 40");

    let min: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_imm_del_st")
        .await;
    assert_eq!(min, 10, "Score 10 should now be in top-3");
}

#[tokio::test]
async fn test_topk_immediate_update_changes_ranking() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_imm_upd (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_imm_upd VALUES (1,10),(2,20),(3,30),(4,40)")
        .await;

    create_immediate_st(
        &db,
        "topk_imm_upd_st",
        "SELECT id, score FROM topk_imm_upd ORDER BY score DESC LIMIT 2",
    )
    .await;

    // Top-2: 40, 30
    assert_eq!(db.count("public.topk_imm_upd_st").await, 2);

    // Update row id=1 (score 10 → 50) — should enter top-2 and push out id=3
    db.execute("UPDATE topk_imm_upd SET score = 50 WHERE id = 1")
        .await;

    assert_eq!(db.count("public.topk_imm_upd_st").await, 2);

    let max: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_imm_upd_st")
        .await;
    assert_eq!(max, 50, "Updated row should be new top");

    let min: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_imm_upd_st")
        .await;
    assert_eq!(min, 40, "Second place should be 40");
}

#[tokio::test]
async fn test_topk_immediate_multiple_dml_in_transaction() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_imm_tx (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_imm_tx VALUES (1,10),(2,20),(3,30)")
        .await;

    create_immediate_st(
        &db,
        "topk_imm_tx_st",
        "SELECT id, score FROM topk_imm_tx ORDER BY score DESC LIMIT 2",
    )
    .await;

    // Top-2: 30, 20
    assert_eq!(db.count("public.topk_imm_tx_st").await, 2);

    // Multiple DML in a single transaction — each triggers micro-refresh
    {
        let mut tx = db.pool.begin().await.unwrap();
        sqlx::query("INSERT INTO topk_imm_tx VALUES (4, 50)")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("DELETE FROM topk_imm_tx WHERE id = 3")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("INSERT INTO topk_imm_tx VALUES (5, 40)")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    assert_eq!(db.count("public.topk_imm_tx_st").await, 2);

    // After: available scores are 10, 20, 40, 50 → top-2: 50, 40
    let max: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_imm_tx_st")
        .await;
    assert_eq!(max, 50);

    let min: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_imm_tx_st")
        .await;
    assert_eq!(min, 40);
}

#[tokio::test]
async fn test_topk_immediate_with_aggregate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE topk_imm_agg (category TEXT, amount INT, \
         id SERIAL PRIMARY KEY)",
    )
    .await;
    db.execute(
        "INSERT INTO topk_imm_agg (category, amount) VALUES \
         ('a',100),('a',200),('b',150),('b',250),('c',50)",
    )
    .await;

    create_immediate_st(
        &db,
        "topk_imm_agg_st",
        "SELECT category, SUM(amount) AS total \
         FROM topk_imm_agg GROUP BY category \
         ORDER BY total DESC LIMIT 2",
    )
    .await;

    // Categories: a=300, b=400, c=50 → top-2: b(400), a(300)
    assert_eq!(db.count("public.topk_imm_agg_st").await, 2);

    let top: String = db
        .query_scalar("SELECT category FROM public.topk_imm_agg_st ORDER BY total DESC LIMIT 1")
        .await;
    assert_eq!(top, "b");

    // Add a big amount to category c — should overtake a
    db.execute("INSERT INTO topk_imm_agg (category, amount) VALUES ('c', 500)")
        .await;

    // Now: a=300, b=400, c=550 → top-2: c(550), b(400)
    let new_top: String = db
        .query_scalar("SELECT category FROM public.topk_imm_agg_st ORDER BY total DESC LIMIT 1")
        .await;
    assert_eq!(
        new_top, "c",
        "Category c should now be #1 after huge insert"
    );
}

#[tokio::test]
async fn test_topk_immediate_with_offset() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_imm_off (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_imm_off VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .await;

    create_immediate_st(
        &db,
        "topk_imm_off_st",
        "SELECT id, score FROM topk_imm_off ORDER BY score DESC LIMIT 2 OFFSET 1",
    )
    .await;

    // Rows 2–3 by descending score: 40, 30
    assert_eq!(db.count("public.topk_imm_off_st").await, 2);

    let max: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_imm_off_st")
        .await;
    assert_eq!(max, 40);

    // Insert a new top score — should shift the window
    db.execute("INSERT INTO topk_imm_off VALUES (6, 60)").await;

    // New order: 60, 50, 40, 30, 20, 10 → OFFSET 1 LIMIT 2: 50, 40
    let new_max: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_imm_off_st")
        .await;
    assert_eq!(new_max, 50, "Window should shift to 50,40 after new top");

    let new_min: i32 = db
        .query_scalar("SELECT MIN(score) FROM public.topk_imm_off_st")
        .await;
    assert_eq!(new_min, 40);
}

#[tokio::test]
async fn test_topk_immediate_threshold_rejection() {
    let db = E2eDb::new().await.with_extension().await;
    let default_limit = db.show_setting("pg_trickle.ivm_topk_max_limit").await;

    db.execute("CREATE TABLE topk_imm_rej (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO topk_imm_rej VALUES (1,1)").await;

    // Set the GUC to a very low limit (system-wide so it applies across pool connections)
    db.alter_system_set_and_wait("pg_trickle.ivm_topk_max_limit", "5", "5")
        .await;

    // Creating a TopK with LIMIT > threshold should fail
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('topk_imm_rej_st', \
             $$SELECT id, val FROM topk_imm_rej ORDER BY val DESC LIMIT 10$$, \
             NULL, 'IMMEDIATE')",
        )
        .await;

    // Clean up the system-wide GUC before asserting
    db.alter_system_reset_and_wait("pg_trickle.ivm_topk_max_limit", &default_limit)
        .await;

    assert!(
        result.is_err(),
        "TopK LIMIT 10 > ivm_topk_max_limit 5 should be rejected"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("exceeds the IMMEDIATE mode threshold"),
        "Error should mention threshold: {err_msg}"
    );
}

#[tokio::test]
async fn test_topk_immediate_mode_switch_from_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_imm_sw (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_imm_sw VALUES (1,10),(2,20),(3,30),(4,40)")
        .await;

    // Create as DIFFERENTIAL first
    let query = "SELECT id, score FROM topk_imm_sw ORDER BY score DESC LIMIT 2";
    db.create_st("topk_imm_sw_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("topk_imm_sw_st", query).await;

    assert_eq!(db.count("public.topk_imm_sw_st").await, 2);

    // Switch to IMMEDIATE mode
    db.alter_st("topk_imm_sw_st", "refresh_mode => 'IMMEDIATE'")
        .await;

    let (_, mode, _, _) = db.pgt_status("topk_imm_sw_st").await;
    assert_eq!(mode, "IMMEDIATE");

    // DML should now propagate immediately
    db.execute("INSERT INTO topk_imm_sw VALUES (5, 50)").await;

    let max: i32 = db
        .query_scalar("SELECT MAX(score) FROM public.topk_imm_sw_st")
        .await;
    assert_eq!(max, 50, "IMMEDIATE micro-refresh should pick up new top");
}

#[tokio::test]
async fn test_topk_fetch_next_syntax() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE topk_fetch_src (id INT, val INT)")
        .await;
    db.execute("INSERT INTO topk_fetch_src VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    let q = "SELECT id, val FROM topk_fetch_src ORDER BY val DESC FETCH NEXT 2 ROWS ONLY";

    db.create_st("topk_fetch_st", q, "1m", "DIFFERENTIAL").await;

    db.assert_st_matches_query("topk_fetch_st", q).await;

    db.execute("INSERT INTO topk_fetch_src VALUES (4, 40)")
        .await;
    db.refresh_st("topk_fetch_st").await;

    db.assert_st_matches_query("topk_fetch_st", q).await;
}
