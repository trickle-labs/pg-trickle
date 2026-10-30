//! A42-12: FULL JOIN aggregate property tests — DIFF vs FULL equivalence.
//!
//! Validates that DIFFERENTIAL mode produces the same result as a full
//! re-evaluation for queries combining FULL OUTER JOIN with aggregate functions,
//! across multi-cycle insert/update/delete sequences including NULL keys and
//! both-side changes in the same cycle.
//!
//! Prerequisites: `just test-e2e`

mod e2e;

use e2e::E2eDb;

// ── Helper ───────────────────────────────────────────────────────────────────

/// Assert the most recent effective_refresh_mode is DIFFERENTIAL (or a valid
/// sub-mode).
#[allow(dead_code)]
async fn assert_diff_mode(db: &E2eDb, st_name: &str) {
    let mode: Option<String> = db
        .query_scalar_opt(&format!(
            "SELECT effective_refresh_mode \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{st_name}'"
        ))
        .await;
    assert!(
        matches!(
            mode.as_deref(),
            Some("DIFFERENTIAL") | Some("APPEND_ONLY") | Some("GROUP_RESCAN")
        ),
        "ST '{st_name}' fell back to FULL refresh; mode = {mode:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// FULL JOIN + COUNT — basic property
// ═══════════════════════════════════════════════════════════════════════

/// A42-12: FULL JOIN + COUNT(*) — multi-cycle correctness.
#[tokio::test]
async fn test_full_join_aggregate_count_multi_cycle() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fja_l (id INT PRIMARY KEY, k INT, v TEXT)")
        .await;
    db.execute("CREATE TABLE fja_r (id INT PRIMARY KEY, k INT, w TEXT)")
        .await;
    db.execute("INSERT INTO fja_l VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')")
        .await;
    db.execute("INSERT INTO fja_r VALUES (1,10,'x'),(2,20,'y')")
        .await;

    let q = "SELECT COALESCE(l.k, r.k) AS k, \
             COUNT(*) AS cnt, \
             COUNT(l.v) AS l_cnt, \
             COUNT(r.w) AS r_cnt \
             FROM fja_l l FULL OUTER JOIN fja_r r ON l.k = r.k \
             GROUP BY COALESCE(l.k, r.k)";
    db.create_st("fja_st1", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fja_st1", q).await;

    // Cycle 1: Insert right-only row
    db.execute("INSERT INTO fja_r VALUES (3, 40, 'z')").await;
    db.refresh_st("fja_st1").await;
    db.assert_st_matches_query("fja_st1", q).await;

    // Cycle 2: Update left row to create a match
    db.execute("UPDATE fja_l SET k = 40 WHERE id = 3").await;
    db.refresh_st("fja_st1").await;
    db.assert_st_matches_query("fja_st1", q).await;

    // Cycle 3: Delete from both sides in the same cycle
    db.execute("DELETE FROM fja_l WHERE id = 1").await;
    db.execute("DELETE FROM fja_r WHERE id = 1").await;
    db.refresh_st("fja_st1").await;
    db.assert_st_matches_query("fja_st1", q).await;

    // Cycle 4: Insert NULL-key rows (should appear as separate groups)
    db.execute("INSERT INTO fja_l VALUES (10, NULL, 'null_l')")
        .await;
    db.execute("INSERT INTO fja_r VALUES (10, NULL, 'null_r')")
        .await;
    db.refresh_st("fja_st1").await;
    db.assert_st_matches_query("fja_st1", q).await;
}

/// A42-12: FULL JOIN + SUM — changes on both sides in the same cycle.
#[tokio::test]
async fn test_full_join_aggregate_sum_both_sides_same_cycle() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fja_l2 (id INT PRIMARY KEY, k INT, amt NUMERIC)")
        .await;
    db.execute("CREATE TABLE fja_r2 (id INT PRIMARY KEY, k INT, amt NUMERIC)")
        .await;
    db.execute("INSERT INTO fja_l2 VALUES (1,1,100),(2,2,200),(3,3,300)")
        .await;
    db.execute("INSERT INTO fja_r2 VALUES (1,1,10),(2,2,20)")
        .await;

    let q = "SELECT COALESCE(l.k, r.k) AS k, \
             COALESCE(SUM(l.amt), 0) AS l_sum, \
             COALESCE(SUM(r.amt), 0) AS r_sum \
             FROM fja_l2 l FULL OUTER JOIN fja_r2 r ON l.k = r.k \
             GROUP BY COALESCE(l.k, r.k)";
    db.create_st("fja_st2", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fja_st2", q).await;

    // Cycle 1: Update both sides for same key in same cycle
    db.execute("UPDATE fja_l2 SET amt = 150 WHERE k = 1").await;
    db.execute("UPDATE fja_r2 SET amt = 15 WHERE k = 1").await;
    db.refresh_st_with_retry("fja_st2").await;
    db.assert_st_matches_query("fja_st2", q).await;

    // Cycle 2: Insert right row for previously left-only key
    db.execute("INSERT INTO fja_r2 VALUES (3, 3, 30)").await;
    db.refresh_st_with_retry("fja_st2").await;
    db.assert_st_matches_query("fja_st2", q).await;

    // Cycle 3: Delete all right rows, add new left-only row
    db.execute("DELETE FROM fja_r2").await;
    db.execute("INSERT INTO fja_l2 VALUES (4, 4, 400)").await;
    db.refresh_st_with_retry("fja_st2").await;
    db.assert_st_matches_query("fja_st2", q).await;
}

/// A42-12: Nested FULL JOIN (left side is itself a FULL JOIN) + aggregate.
#[tokio::test]
async fn test_full_join_nested_aggregate_multi_cycle() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fja_a (id INT PRIMARY KEY, k INT, v INT)")
        .await;
    db.execute("CREATE TABLE fja_b (id INT PRIMARY KEY, k INT, v INT)")
        .await;
    db.execute("CREATE TABLE fja_c (id INT PRIMARY KEY, k INT, v INT)")
        .await;
    db.execute("INSERT INTO fja_a VALUES (1,1,10),(2,2,20)")
        .await;
    db.execute("INSERT INTO fja_b VALUES (1,1,5),(3,3,15)")
        .await;
    db.execute("INSERT INTO fja_c VALUES (1,1,1),(2,2,2)").await;

    let q = "SELECT COALESCE(ab.k, c.k) AS k, \
             COUNT(*) AS total_cnt, \
             COALESCE(SUM(ab.av), 0) + COALESCE(SUM(c.v), 0) AS combined_sum \
             FROM (SELECT COALESCE(a.k, b.k) AS k, a.v AS av, b.v AS bv \
                   FROM fja_a a FULL OUTER JOIN fja_b b ON a.k = b.k) ab \
             FULL OUTER JOIN fja_c c ON ab.k = c.k \
             GROUP BY COALESCE(ab.k, c.k)";
    db.create_st("fja_st3", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fja_st3", q).await;

    // Cycle 1: Insert into all three tables
    db.execute("INSERT INTO fja_a VALUES (3, 3, 30)").await;
    db.execute("INSERT INTO fja_b VALUES (2, 2, 10)").await;
    db.execute("INSERT INTO fja_c VALUES (3, 3, 3)").await;
    db.refresh_st("fja_st3").await;
    db.assert_st_matches_query("fja_st3", q).await;

    // Cycle 2: NULL key rows
    db.execute("INSERT INTO fja_a VALUES (10, NULL, 99)").await;
    db.refresh_st("fja_st3").await;
    db.assert_st_matches_query("fja_st3", q).await;
}

/// A42-12: FULL JOIN with DIFF vs FULL comparison across 10 random cycles.
///
/// This property test verifies the invariant:
///   ∀ cycle: DIFF_ST(query) == eval(query)
#[tokio::test]
async fn test_full_join_diff_vs_full_property_10_cycles() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fja_prop_l (id INT PRIMARY KEY, k INT, val INT)")
        .await;
    db.execute("CREATE TABLE fja_prop_r (id INT PRIMARY KEY, k INT, val INT)")
        .await;

    let q = "SELECT COALESCE(l.k, r.k) AS k, \
             COUNT(*) AS cnt, \
             COALESCE(SUM(l.val), 0) AS l_sum, \
             COALESCE(SUM(r.val), 0) AS r_sum \
             FROM fja_prop_l l FULL OUTER JOIN fja_prop_r r ON l.k = r.k \
             GROUP BY COALESCE(l.k, r.k)";
    db.create_st("fja_prop_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fja_prop_st", q).await;

    // 10 cycles: alternating inserts, updates, deletes across both tables
    for cycle in 1_i32..=10 {
        match cycle % 4 {
            0 => {
                // Insert on both sides
                db.execute(&format!(
                    "INSERT INTO fja_prop_l VALUES ({c}, {k}, {v}) \
                     ON CONFLICT (id) DO UPDATE SET val = excluded.val",
                    c = cycle * 100,
                    k = (cycle % 5) + 1,
                    v = cycle * 10
                ))
                .await;
                db.execute(&format!(
                    "INSERT INTO fja_prop_r VALUES ({c}, {k}, {v}) \
                     ON CONFLICT (id) DO UPDATE SET val = excluded.val",
                    c = cycle * 100 + 1,
                    k = (cycle % 5) + 1,
                    v = cycle * 7
                ))
                .await;
            }
            1 => {
                // Update left, delete right
                db.execute(&format!(
                    "UPDATE fja_prop_l SET val = val + 1 WHERE id = {}",
                    cycle * 100 - 100
                ))
                .await;
                db.execute(&format!(
                    "DELETE FROM fja_prop_r WHERE id = {}",
                    cycle * 100 - 99
                ))
                .await;
            }
            2 => {
                // Insert left-only row
                db.execute(&format!(
                    "INSERT INTO fja_prop_l VALUES ({}, {}, {}) ON CONFLICT DO NOTHING",
                    cycle * 100 + 2,
                    cycle + 10,
                    cycle * 3
                ))
                .await;
            }
            _ => {
                // Insert right-only row
                db.execute(&format!(
                    "INSERT INTO fja_prop_r VALUES ({}, {}, {}) ON CONFLICT DO NOTHING",
                    cycle * 100 + 3,
                    cycle + 10,
                    cycle * 4
                ))
                .await;
            }
        }
        db.refresh_st("fja_prop_st").await;
        db.assert_st_matches_query("fja_prop_st", q).await;
    }
}
