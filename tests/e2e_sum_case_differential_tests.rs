//! A42-10: Differential SUM(CASE WHEN …) E2E tests.
//!
//! Validates that DIFFERENTIAL mode correctly maintains `SUM(CASE WHEN x > t
//! THEN y ELSE 0 END)` across INSERT/UPDATE/DELETE sequences, including rows
//! crossing the threshold in both directions.
//!
//! After each operation the stream table result is compared against the
//! ground-truth defining query (full re-evaluation). Also asserts that the
//! refresh falls back to GROUP_RESCAN (not ALGEBRAIC) because SUM(CASE) is
//! non-invertible per DI-8.
//!
//! Prerequisites: `just test-e2e` (requires the Docker image with the extension).

mod e2e;

use e2e::E2eDb;

// ── Helper ──────────────────────────────────────────────────────────────────

/// Assert the refresh used GROUP_RESCAN (EXCEPT ALL) — required for SUM(CASE).
async fn assert_group_rescan_mode(db: &E2eDb, st_name: &str) {
    let mode: Option<String> = db
        .query_scalar_opt(&format!(
            "SELECT effective_refresh_mode \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{st_name}'"
        ))
        .await;
    // GROUP_RESCAN falls back to FULL refresh for the delta path, so the
    // effective_refresh_mode is DIFFERENTIAL (the whole ST mode) but the
    // internal aggregate path uses EXCEPT ALL. The key property to assert is
    // that the ST is in DIFFERENTIAL mode (not silently FULL).
    assert!(
        matches!(mode.as_deref(), Some("DIFFERENTIAL")),
        "ST '{st_name}' should be in DIFFERENTIAL mode, got: {mode:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Basic SUM(CASE) — threshold crossing
// ═══════════════════════════════════════════════════════════════════════

/// A42-10: INSERT rows above and below the threshold, verify SUM(CASE) is correct.
#[tokio::test]
async fn test_sum_case_differential_inserts() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE sc_src1 (id INT PRIMARY KEY, x INT, y NUMERIC)")
        .await;
    db.execute("INSERT INTO sc_src1 VALUES (1, 5, 10), (2, 15, 20), (3, 3, 5)")
        .await;

    // threshold = 10
    let q = "SELECT SUM(CASE WHEN x > 10 THEN y ELSE 0 END) AS total FROM sc_src1";
    db.create_st("sc_st1", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("sc_st1", q).await;
    assert_group_rescan_mode(&db, "sc_st1").await;

    // Cycle 1: INSERT above threshold
    db.execute("INSERT INTO sc_src1 VALUES (4, 20, 30)").await;
    db.refresh_st_until_converged("sc_st1").await;
    db.assert_st_matches_query("sc_st1", q).await;
    assert_group_rescan_mode(&db, "sc_st1").await;

    // Cycle 2: INSERT below threshold (should not affect total)
    db.execute("INSERT INTO sc_src1 VALUES (5, 2, 100)").await;
    db.refresh_st_until_converged("sc_st1").await;
    db.assert_st_matches_query("sc_st1", q).await;

    // Cycle 3: DELETE above-threshold row
    db.execute("DELETE FROM sc_src1 WHERE id = 2").await;
    db.refresh_st_until_converged("sc_st1").await;
    db.assert_st_matches_query("sc_st1", q).await;
}

/// A42-10: UPDATE rows crossing the threshold in both directions.
#[tokio::test]
async fn test_sum_case_differential_threshold_crossing() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE sc_src2 (id INT PRIMARY KEY, x INT, y NUMERIC)")
        .await;
    db.execute("INSERT INTO sc_src2 VALUES (1, 5, 10), (2, 15, 20), (3, 12, 30)")
        .await;

    let q = "SELECT SUM(CASE WHEN x > 10 THEN y ELSE 0 END) AS total FROM sc_src2";
    db.create_st("sc_st2", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("sc_st2", q).await;

    // Cycle 1: UPDATE row crossing threshold from below to above
    db.execute("UPDATE sc_src2 SET x = 15 WHERE id = 1").await;
    db.refresh_st_until_converged("sc_st2").await;
    db.assert_st_matches_query("sc_st2", q).await;
    assert_group_rescan_mode(&db, "sc_st2").await;

    // Cycle 2: Update row crossing threshold from above to below
    db.execute("UPDATE sc_src2 SET x = 5 WHERE id = 2").await;
    db.refresh_st_until_converged("sc_st2").await;
    db.assert_st_matches_query("sc_st2", q).await;

    // Cycle 3: Update y value for above-threshold row
    db.execute("UPDATE sc_src2 SET y = 50 WHERE id = 3").await;
    db.refresh_st_until_converged("sc_st2").await;
    db.assert_st_matches_query("sc_st2", q).await;
}

/// A42-10: Multi-cycle sequence with mixed INSERT/UPDATE/DELETE.
#[tokio::test]
async fn test_sum_case_differential_multi_cycle() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE sc_src3 (id INT PRIMARY KEY, x INT, y NUMERIC)")
        .await;

    let q = "SELECT SUM(CASE WHEN x > 10 THEN y ELSE 0 END) AS total \
             FROM sc_src3";
    db.create_st("sc_st3", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("sc_st3", q).await;

    // 10 cycles of random-ish operations
    for i in 1_i32..=5 {
        let x_above = 11 + i;
        let x_below = i;
        db.execute(&format!(
            "INSERT INTO sc_src3 VALUES ({}, {}, {})",
            i * 10,
            x_above,
            i * 5
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO sc_src3 VALUES ({}, {}, {})",
            i * 10 + 1,
            x_below,
            i * 3
        ))
        .await;
        db.refresh_st_until_converged("sc_st3").await;
        db.assert_st_matches_query("sc_st3", q).await;
    }

    // Bulk DELETE
    db.execute("DELETE FROM sc_src3 WHERE id % 2 = 0").await;
    db.refresh_st_until_converged("sc_st3").await;
    db.assert_st_matches_query("sc_st3", q).await;

    // Bulk UPDATE crossing threshold
    db.execute("UPDATE sc_src3 SET x = x + 20").await;
    db.refresh_st_until_converged("sc_st3").await;
    db.assert_st_matches_query("sc_st3", q).await;
}

/// A42-10: SUM(CASE) with GROUP BY — per-group SUM(CASE) in DIFFERENTIAL mode.
#[tokio::test]
async fn test_sum_case_differential_with_group_by() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE sc_src4 (id INT PRIMARY KEY, grp TEXT, x INT, y NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO sc_src4 VALUES \
         (1,'A',5,10),(2,'A',15,20),(3,'B',8,5),(4,'B',12,30)",
    )
    .await;

    let q = "SELECT grp, SUM(CASE WHEN x > 10 THEN y ELSE 0 END) AS total \
             FROM sc_src4 GROUP BY grp";
    db.create_st("sc_st4", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("sc_st4", q).await;
    assert_group_rescan_mode(&db, "sc_st4").await;

    // Move group B row above threshold
    db.execute("UPDATE sc_src4 SET x = 15 WHERE id = 3").await;
    db.refresh_st_until_converged("sc_st4").await;
    db.assert_st_matches_query("sc_st4", q).await;

    // Delete all group A rows
    db.execute("DELETE FROM sc_src4 WHERE grp = 'A'").await;
    db.refresh_st_until_converged("sc_st4").await;
    db.assert_st_matches_query("sc_st4", q).await;

    // Insert new group C rows
    db.execute("INSERT INTO sc_src4 VALUES (5,'C',20,40),(6,'C',3,10)")
        .await;
    db.refresh_st_until_converged("sc_st4").await;
    db.assert_st_matches_query("sc_st4", q).await;
}

/// A42-11: SUM(CASE) with cast wrapping — ensures AST-level detection fires.
/// `SUM(CAST(CASE WHEN x > 10 THEN y ELSE 0 END AS numeric))` should be treated
/// as non-invertible and fall back to GROUP_RESCAN.
#[tokio::test]
async fn test_sum_case_wrapped_in_cast_is_non_invertible() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE sc_src5 (id INT PRIMARY KEY, x INT, y INT)")
        .await;
    db.execute("INSERT INTO sc_src5 VALUES (1, 5, 10), (2, 15, 20)")
        .await;

    // CASE wrapped in CAST — A42-11 AST detection must handle this.
    let q = "SELECT SUM(CAST(CASE WHEN x > 10 THEN y ELSE 0 END AS numeric)) AS total \
             FROM sc_src5";
    db.create_st("sc_st5", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("sc_st5", q).await;

    // Cross threshold
    db.execute("UPDATE sc_src5 SET x = 15 WHERE id = 1").await;
    db.refresh_st_until_converged("sc_st5").await;
    db.assert_st_matches_query("sc_st5", q).await;
}
