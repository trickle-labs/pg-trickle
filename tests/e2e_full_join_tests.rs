//! E2E tests for FULL OUTER JOIN differential correctness (F18+F26).
//!
//! Validates FULL JOIN behaviour under INSERT, UPDATE, DELETE, including
//! NULL join keys (F26) and row migrations across matched/unmatched sides.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Basic FULL JOIN differential
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_full_join_basic_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fj_left (id SERIAL PRIMARY KEY, key INT, lval TEXT)")
        .await;
    db.execute("CREATE TABLE fj_right (id SERIAL PRIMARY KEY, key INT, rval TEXT)")
        .await;
    db.execute("INSERT INTO fj_left (key, lval) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    db.execute("INSERT INTO fj_right (key, rval) VALUES (2, 'x'), (3, 'y'), (4, 'z')")
        .await;

    let q = "SELECT l.key AS lkey, l.lval, r.key AS rkey, r.rval \
             FROM fj_left l FULL OUTER JOIN fj_right r ON l.key = r.key";
    db.create_st("fj_basic_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fj_basic_st", q).await;

    // INSERT on left only — creates new unmatched row
    db.execute("INSERT INTO fj_left (key, lval) VALUES (5, 'd')")
        .await;
    db.refresh_st("fj_basic_st").await;
    db.assert_st_matches_query("fj_basic_st", q).await;

    // INSERT on right matching existing left row
    db.execute("INSERT INTO fj_right (key, rval) VALUES (1, 'w')")
        .await;
    db.refresh_st("fj_basic_st").await;
    db.assert_st_matches_query("fj_basic_st", q).await;

    // DELETE from left — unmatched right side
    db.execute("DELETE FROM fj_left WHERE key = 2").await;
    db.refresh_st("fj_basic_st").await;
    db.assert_st_matches_query("fj_basic_st", q).await;

    // UPDATE right side value
    db.execute("UPDATE fj_right SET rval = 'updated' WHERE key = 3")
        .await;
    db.refresh_st("fj_basic_st").await;
    db.assert_st_matches_query("fj_basic_st", q).await;
}

#[tokio::test]
async fn test_full_join_multi_event_null_transitions_emit_once() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fj_transition_l (id INT PRIMARY KEY, key INT, val TEXT)")
        .await;
    db.execute("CREATE TABLE fj_transition_r (id INT PRIMARY KEY, key INT, val TEXT)")
        .await;
    db.execute(
        "INSERT INTO fj_transition_l VALUES \
         (1, 1, 'l1'), (2, 2, 'l2a'), (3, 2, 'l2b')",
    )
    .await;
    db.execute(
        "INSERT INTO fj_transition_r VALUES \
         (1, 1, 'r1a'), (2, 1, 'r1b'), (3, 2, 'r2')",
    )
    .await;

    let q = "SELECT l.id AS lid, l.key AS lkey, l.val AS lval, \
                    r.id AS rid, r.key AS rkey, r.val AS rval \
             FROM fj_transition_l l FULL JOIN fj_transition_r r ON l.key = r.key";
    db.create_st("fj_transition_st", q, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("fj_transition_st", q).await;

    db.execute("DELETE FROM fj_transition_r WHERE key = 1")
        .await;
    db.execute("DELETE FROM fj_transition_l WHERE key = 2")
        .await;
    db.refresh_st("fj_transition_st").await;
    db.assert_st_matches_query("fj_transition_st", q).await;

    db.execute("INSERT INTO fj_transition_r VALUES (4, 1, 'r1c'), (5, 1, 'r1d')")
        .await;
    db.execute("INSERT INTO fj_transition_l VALUES (4, 2, 'l2c'), (5, 2, 'l2d')")
        .await;
    db.refresh_st("fj_transition_st").await;
    db.assert_st_matches_query("fj_transition_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// FULL JOIN with NULL join keys (F26)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_full_join_null_keys_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fj_nl (id SERIAL PRIMARY KEY, key INT, val TEXT)")
        .await;
    db.execute("CREATE TABLE fj_nr (id SERIAL PRIMARY KEY, key INT, val TEXT)")
        .await;
    db.execute("INSERT INTO fj_nl (key, val) VALUES (1, 'a'), (NULL, 'null_left')")
        .await;
    db.execute("INSERT INTO fj_nr (key, val) VALUES (1, 'x'), (NULL, 'null_right')")
        .await;

    let q = "SELECT l.key AS lkey, l.val AS lval, r.key AS rkey, r.val AS rval \
             FROM fj_nl l FULL OUTER JOIN fj_nr r ON l.key = r.key";
    db.create_st("fj_null_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fj_null_st", q).await;

    // Add another NULL key row on left
    db.execute("INSERT INTO fj_nl (key, val) VALUES (NULL, 'null_left_2')")
        .await;
    db.refresh_st("fj_null_st").await;
    db.assert_st_matches_query("fj_null_st", q).await;

    // Remove the right NULL key row
    db.execute("DELETE FROM fj_nr WHERE key IS NULL").await;
    db.refresh_st("fj_null_st").await;
    db.assert_st_matches_query("fj_null_st", q).await;

    // Update a non-null key to NULL
    db.execute("UPDATE fj_nl SET key = NULL WHERE key = 1")
        .await;
    db.refresh_st("fj_null_st").await;
    db.assert_st_matches_query("fj_null_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// FULL JOIN — join key updates (row migrates matched ↔ unmatched)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_full_join_key_update_migration() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fj_ml (id SERIAL PRIMARY KEY, key INT, val TEXT)")
        .await;
    db.execute("CREATE TABLE fj_mr (id SERIAL PRIMARY KEY, key INT, val TEXT)")
        .await;
    db.execute("INSERT INTO fj_ml (key, val) VALUES (1, 'a'), (2, 'b')")
        .await;
    db.execute("INSERT INTO fj_mr (key, val) VALUES (1, 'x'), (3, 'z')")
        .await;

    let q = "SELECT l.key AS lkey, l.val AS lval, r.key AS rkey, r.val AS rval \
             FROM fj_ml l FULL OUTER JOIN fj_mr r ON l.key = r.key";
    db.create_st("fj_key_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fj_key_st", q).await;

    // Change left key=2 to key=3 → now matches right side
    db.execute("UPDATE fj_ml SET key = 3 WHERE key = 2").await;
    db.refresh_st("fj_key_st").await;
    db.assert_st_matches_query("fj_key_st", q).await;

    // Change left key=3 to key=99 → now unmatched again
    db.execute("UPDATE fj_ml SET key = 99 WHERE key = 3").await;
    db.refresh_st("fj_key_st").await;
    db.assert_st_matches_query("fj_key_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// FULL JOIN with aggregation
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_full_join_with_aggregate_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fj_al (id SERIAL PRIMARY KEY, dept TEXT, budget INT)")
        .await;
    db.execute("CREATE TABLE fj_ar (id SERIAL PRIMARY KEY, dept TEXT, revenue INT)")
        .await;
    db.execute("INSERT INTO fj_al (dept, budget) VALUES ('eng', 100), ('eng', 200), ('sales', 50)")
        .await;
    db.execute("INSERT INTO fj_ar (dept, revenue) VALUES ('eng', 500), ('mkt', 300)")
        .await;

    let q = "SELECT COALESCE(l.dept, r.dept) AS dept, \
             SUM(l.budget) AS total_budget, SUM(r.revenue) AS total_revenue \
             FROM fj_al l \
             FULL OUTER JOIN fj_ar r ON l.dept = r.dept \
             GROUP BY COALESCE(l.dept, r.dept)";
    db.create_st("fj_agg_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fj_agg_st", q).await;

    db.execute("INSERT INTO fj_al (dept, budget) VALUES ('mkt', 75)")
        .await;
    db.refresh_st("fj_agg_st").await;
    db.assert_st_matches_query("fj_agg_st", q).await;

    db.execute("DELETE FROM fj_ar WHERE dept = 'eng'").await;
    db.refresh_st("fj_agg_st").await;
    db.assert_st_matches_query("fj_agg_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// FULL JOIN — multi-column join key
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_full_join_multi_column_key_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fj_mcl (id SERIAL PRIMARY KEY, a INT, b TEXT, lval INT)")
        .await;
    db.execute("CREATE TABLE fj_mcr (id SERIAL PRIMARY KEY, a INT, b TEXT, rval INT)")
        .await;
    db.execute("INSERT INTO fj_mcl (a, b, lval) VALUES (1, 'x', 10), (2, 'y', 20)")
        .await;
    db.execute("INSERT INTO fj_mcr (a, b, rval) VALUES (1, 'x', 100), (3, 'z', 300)")
        .await;

    let q = "SELECT l.a AS la, l.b AS lb, l.lval, r.a AS ra, r.b AS rb, r.rval \
             FROM fj_mcl l FULL OUTER JOIN fj_mcr r ON l.a = r.a AND l.b = r.b";
    db.create_st("fj_mc_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fj_mc_st", q).await;

    db.execute("INSERT INTO fj_mcr (a, b, rval) VALUES (2, 'y', 200)")
        .await;
    db.refresh_st("fj_mc_st").await;
    db.assert_st_matches_query("fj_mc_st", q).await;

    db.execute("DELETE FROM fj_mcl WHERE a = 1").await;
    db.refresh_st("fj_mc_st").await;
    db.assert_st_matches_query("fj_mc_st", q).await;
}
