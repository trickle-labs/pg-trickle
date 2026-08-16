//! E2E tests for INTERSECT / EXCEPT FULL-refresh correctness (F19: G2.4).
//!
//! Validates set operations (INTERSECT, INTERSECT ALL, EXCEPT, EXCEPT ALL)
//! Set operations remain FULL-only until private incremental state is proven.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// INTERSECT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_intersect_basic_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO isect_a (val) VALUES (1), (2), (3)")
        .await;
    db.execute("INSERT INTO isect_b (val) VALUES (2), (3), (4)")
        .await;

    let q = "SELECT val FROM isect_a INTERSECT SELECT val FROM isect_b";
    db.create_st("isect_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("isect_st", q).await;

    // Add value to both → appears in intersection
    db.execute("INSERT INTO isect_a (val) VALUES (4)").await;
    db.refresh_st("isect_st").await;
    db.assert_st_matches_query("isect_st", q).await;

    // Remove shared value from one side
    db.execute("DELETE FROM isect_b WHERE val = 2").await;
    db.refresh_st("isect_st").await;
    db.assert_st_matches_query("isect_st", q).await;
}

#[tokio::test]
async fn test_intersect_all_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_all_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_all_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO isect_all_a (val) VALUES (1), (1), (2), (3)")
        .await;
    db.execute("INSERT INTO isect_all_b (val) VALUES (1), (2), (2), (3)")
        .await;

    let q = "SELECT val FROM isect_all_a INTERSECT ALL SELECT val FROM isect_all_b";
    db.create_st("isect_all_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("isect_all_st", q).await;

    // Add duplicate in A
    db.execute("INSERT INTO isect_all_a (val) VALUES (2)").await;
    db.refresh_st("isect_all_st").await;
    db.assert_st_matches_query("isect_all_st", q).await;

    // Remove from B
    db.execute("DELETE FROM isect_all_b WHERE val = 1").await;
    db.refresh_st("isect_all_st").await;
    db.assert_st_matches_query("isect_all_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// EXCEPT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_except_basic_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE exc_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE exc_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO exc_a (val) VALUES (1), (2), (3)")
        .await;
    db.execute("INSERT INTO exc_b (val) VALUES (2), (4)").await;

    let q = "SELECT val FROM exc_a EXCEPT SELECT val FROM exc_b";
    db.create_st("exc_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("exc_st", q).await;

    // Add to B a value that exists in A → shrinks result
    db.execute("INSERT INTO exc_b (val) VALUES (1)").await;
    db.refresh_st("exc_st").await;
    db.assert_st_matches_query("exc_st", q).await;

    // Add to A a new value
    db.execute("INSERT INTO exc_a (val) VALUES (5)").await;
    db.refresh_st("exc_st").await;
    db.assert_st_matches_query("exc_st", q).await;

    // Remove from B → re-exposes value in A
    db.execute("DELETE FROM exc_b WHERE val = 2").await;
    db.refresh_st("exc_st").await;
    db.assert_st_matches_query("exc_st", q).await;
}

#[tokio::test]
async fn test_except_all_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE exc_all_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE exc_all_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO exc_all_a (val) VALUES (1), (1), (1), (2)")
        .await;
    db.execute("INSERT INTO exc_all_b (val) VALUES (1), (2)")
        .await;

    let q = "SELECT val FROM exc_all_a EXCEPT ALL SELECT val FROM exc_all_b";
    db.create_st("exc_all_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("exc_all_st", q).await;

    // Remove duplicate from A
    db.execute("DELETE FROM exc_all_a WHERE id = (SELECT MIN(id) FROM exc_all_a WHERE val = 1)")
        .await;
    db.refresh_st("exc_all_st").await;
    db.assert_st_matches_query("exc_all_st", q).await;

    // Add to B → more subtractions
    db.execute("INSERT INTO exc_all_b (val) VALUES (1)").await;
    db.refresh_st("exc_all_st").await;
    db.assert_st_matches_query("exc_all_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-way chain (A UNION ALL B EXCEPT C)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_set_ops_chain_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE so_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE so_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE so_c (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO so_a (val) VALUES (1), (2)").await;
    db.execute("INSERT INTO so_b (val) VALUES (3), (4)").await;
    db.execute("INSERT INTO so_c (val) VALUES (2), (3)").await;

    let q = "(SELECT val FROM so_a UNION ALL SELECT val FROM so_b) \
             EXCEPT SELECT val FROM so_c";
    db.create_st("so_chain_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("so_chain_st", q).await;

    db.execute("INSERT INTO so_c (val) VALUES (1)").await;
    db.refresh_st("so_chain_st").await;
    db.assert_st_matches_query("so_chain_st", q).await;

    db.execute("DELETE FROM so_b WHERE val = 4").await;
    db.refresh_st("so_chain_st").await;
    db.assert_st_matches_query("so_chain_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-column set operations
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_intersect_multi_column_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_mc_a (id SERIAL PRIMARY KEY, x INT, y TEXT)")
        .await;
    db.execute("CREATE TABLE isect_mc_b (id SERIAL PRIMARY KEY, x INT, y TEXT)")
        .await;
    db.execute("INSERT INTO isect_mc_a (x, y) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    db.execute("INSERT INTO isect_mc_b (x, y) VALUES (1, 'a'), (2, 'z'), (4, 'd')")
        .await;

    let q = "SELECT x, y FROM isect_mc_a INTERSECT SELECT x, y FROM isect_mc_b";
    db.create_st("isect_mc_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("isect_mc_st", q).await;

    // Make (2,'b') match by updating B
    db.execute("UPDATE isect_mc_b SET y = 'b' WHERE x = 2")
        .await;
    db.refresh_st("isect_mc_st").await;
    db.assert_st_matches_query("isect_mc_st", q).await;

    // Remove matching row from A
    db.execute("DELETE FROM isect_mc_a WHERE x = 1").await;
    db.refresh_st("isect_mc_st").await;
    db.assert_st_matches_query("isect_mc_st", q).await;
}

#[tokio::test]
async fn test_set_operation_with_nulls() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE set_null_a (id INT, val TEXT)")
        .await;
    db.execute("CREATE TABLE set_null_b (id INT, val TEXT)")
        .await;

    db.execute("INSERT INTO set_null_a VALUES (1, NULL), (NULL, 'A')")
        .await;
    db.execute("INSERT INTO set_null_b VALUES (1, NULL), (NULL, 'B')")
        .await;

    let q = "SELECT id, val FROM set_null_a UNION ALL SELECT id, val FROM set_null_b";

    db.create_st("set_null_st", q, "1m", "DIFFERENTIAL").await;

    db.assert_st_matches_query("set_null_st", q).await;

    db.execute("INSERT INTO set_null_a VALUES (NULL, NULL)")
        .await;
    db.refresh_st("set_null_st").await;

    db.assert_st_matches_query("set_null_st", q).await;
}
