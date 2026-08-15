//! E2E tests for ROWS FROM with multiple set-returning functions.
//!
//! Validates the `rewrite_rows_from()` pass that transforms
//! `ROWS FROM(f1(), f2(), ...)` into either multi-arg `unnest()` (all-unnest
//! optimisation) or an ordinal-based LEFT JOIN LATERAL chain (general case).
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════════
//  All-unnest optimisation (multi-arg unnest merge)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rows_from_dual_unnest_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_src (id INT PRIMARY KEY, names TEXT[], scores INT[])")
        .await;
    db.execute(
        "INSERT INTO rf_src VALUES \
         (1, ARRAY['alice', 'bob', 'carol'], ARRAY[90, 85, 70]), \
         (2, ARRAY['dave'], ARRAY[60, 95])",
    )
    .await;

    // ROWS FROM(unnest(names), unnest(scores)) → rewritten to unnest(names, scores)
    db.create_st(
        "rf_dual_unnest",
        "SELECT rf.id, u.name, u.score \
         FROM rf_src rf, \
         ROWS FROM(unnest(rf.names), unnest(rf.scores)) AS u(name, score)",
        "1m",
        "FULL",
    )
    .await;

    let (status, mode, populated, errors) = db.pgt_status("rf_dual_unnest").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);
    assert_eq!(errors, 0);

    // id=1: 3 rows (alice/90, bob/85, carol/70)
    // id=2: 2 rows (dave/60, NULL/95)
    let query = "SELECT rf.id, u.name, u.score \
         FROM rf_src rf, \
         ROWS FROM(unnest(rf.names), unnest(rf.scores)) AS u(name, score)";
    db.assert_st_matches_query("rf_dual_unnest", query).await;
}

#[tokio::test]
async fn test_rows_from_triple_unnest_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_tri (id INT PRIMARY KEY, a TEXT[], b INT[], c BOOLEAN[])")
        .await;
    db.execute(
        "INSERT INTO rf_tri VALUES \
         (1, ARRAY['x', 'y'], ARRAY[10, 20, 30], ARRAY[true])",
    )
    .await;

    db.create_st(
        "rf_tri_unnest",
        "SELECT t.id, u.col_a, u.col_b, u.col_c \
         FROM rf_tri t, \
         ROWS FROM(unnest(t.a), unnest(t.b), unnest(t.c)) AS u(col_a, col_b, col_c)",
        "1m",
        "FULL",
    )
    .await;

    let (status, _, populated, errors) = db.pgt_status("rf_tri_unnest").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
    assert_eq!(errors, 0);

    // Longest array has 3 elements, so 3 rows total (shorter arrays padded with NULL)
    let query = "SELECT t.id, u.col_a, u.col_b, u.col_c \
         FROM rf_tri t, \
         ROWS FROM(unnest(t.a), unnest(t.b), unnest(t.c)) AS u(col_a, col_b, col_c)";
    db.assert_st_matches_query("rf_tri_unnest", query).await;
}

// ═══════════════════════════════════════════════════════════════════════════
//  General case (ordinal-based LEFT JOIN LATERAL chain)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rows_from_mixed_srfs_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_mixed (id INT PRIMARY KEY, arr TEXT[])")
        .await;
    db.execute(
        "INSERT INTO rf_mixed VALUES \
         (1, ARRAY['a', 'b', 'c'])",
    )
    .await;

    // ROWS FROM(unnest(arr), generate_series(1, 5)) — mixed SRF types
    // triggers the general (non-all-unnest) rewrite path.
    db.create_st(
        "rf_mixed_srfs",
        "SELECT m.id, u.val, u.n \
         FROM rf_mixed m, \
         ROWS FROM(unnest(m.arr), generate_series(1, 5)) AS u(val, n)",
        "1m",
        "FULL",
    )
    .await;

    let (status, _, populated, errors) = db.pgt_status("rf_mixed_srfs").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
    assert_eq!(errors, 0);

    // unnest produces 3 rows, generate_series produces 5 rows.
    // ROWS FROM zips them → 5 rows (the longer of the two).
    let query = "SELECT m.id, u.val, u.n \
         FROM rf_mixed m, \
         ROWS FROM(unnest(m.arr), generate_series(1, 5)) AS u(val, n)";
    db.assert_st_matches_query("rf_mixed_srfs", query).await;
}

// ═══════════════════════════════════════════════════════════════════════════
//  Differential mode — verifies rewrite works with IVM refresh
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rows_from_dual_unnest_differential_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_diff (id INT PRIMARY KEY, xs INT[], ys INT[])")
        .await;
    db.execute("INSERT INTO rf_diff VALUES (1, ARRAY[10, 20], ARRAY[100, 200])")
        .await;

    let query = "SELECT d.id, u.x, u.y \
         FROM rf_diff d, \
         ROWS FROM(unnest(d.xs), unnest(d.ys)) AS u(x, y)";

    db.create_st("rf_diff_view", query, "1m", "AUTO").await;

    db.assert_st_matches_query("rf_diff_view", query).await;

    // Insert a new row
    db.execute("INSERT INTO rf_diff VALUES (2, ARRAY[30, 40, 50], ARRAY[300])")
        .await;
    db.refresh_st("rf_diff_view").await;

    // id=1: 2 rows, id=2: 3 rows = 5 total
    db.assert_st_matches_query("rf_diff_view", query).await;
}

#[tokio::test]
async fn test_rows_from_dual_unnest_differential_delete() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_del (id INT PRIMARY KEY, xs INT[], ys TEXT[])")
        .await;
    db.execute(
        "INSERT INTO rf_del VALUES \
         (1, ARRAY[1, 2], ARRAY['a', 'b']), \
         (2, ARRAY[3], ARRAY['c'])",
    )
    .await;

    let query = "SELECT d.id, u.x, u.y \
         FROM rf_del d, \
         ROWS FROM(unnest(d.xs), unnest(d.ys)) AS u(x, y)";

    db.create_st("rf_del_view", query, "1m", "AUTO").await;

    // id=1: 2 rows, id=2: 1 row = 3
    db.assert_st_matches_query("rf_del_view", query).await;

    db.execute("DELETE FROM rf_del WHERE id = 1").await;
    db.refresh_st("rf_del_view").await;

    // Only id=2 remains: 1 row
    db.assert_st_matches_query("rf_del_view", query).await;
}

#[tokio::test]
async fn test_rows_from_dual_unnest_differential_update() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_upd (id INT PRIMARY KEY, xs INT[], ys INT[])")
        .await;
    db.execute(
        "INSERT INTO rf_upd VALUES \
         (1, ARRAY[10, 20], ARRAY[100, 200]), \
         (2, ARRAY[30], ARRAY[300, 400])",
    )
    .await;

    let query = "SELECT d.id, u.x, u.y \
         FROM rf_upd d, \
         ROWS FROM(unnest(d.xs), unnest(d.ys)) AS u(x, y)";

    db.create_st("rf_upd_view", query, "1m", "AUTO").await;

    db.assert_st_matches_query("rf_upd_view", query).await;

    // Update: change array contents (different lengths to verify padding)
    db.execute("UPDATE rf_upd SET xs = ARRAY[50, 60, 70], ys = ARRAY[500] WHERE id = 1")
        .await;
    db.refresh_st("rf_upd_view").await;

    // id=1 now has 3 rows (longest array), id=2 still has 2 rows
    db.assert_st_matches_query("rf_upd_view", query).await;
}

// ═══════════════════════════════════════════════════════════════════════════
//  Single-function ROWS FROM (no rewrite needed — pass-through)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rows_from_single_function_passthrough() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_single (id INT PRIMARY KEY, arr INT[])")
        .await;
    db.execute("INSERT INTO rf_single VALUES (1, ARRAY[10, 20, 30])")
        .await;

    let query = "SELECT s.id, u.val \
         FROM rf_single s, \
         ROWS FROM(unnest(s.arr)) AS u(val)";

    // Single-function ROWS FROM — should pass through without rewriting
    db.create_st("rf_single_view", query, "1m", "FULL").await;

    let (status, _, populated, errors) = db.pgt_status("rf_single_view").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
    assert_eq!(errors, 0);
    db.assert_st_matches_query("rf_single_view", query).await;
}
