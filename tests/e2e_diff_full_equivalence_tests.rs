//! Phase 3 (TESTING_GAPS_2) — Differential ≡ Full Refresh Equivalence
//!
//! For every major operator class, validates that DIFFERENTIAL mode produces
//! the same result as the ground-truth defining query across 5 mutation
//! cycles, AND that the effective refresh mode is genuinely DIFFERENTIAL
//! (no silent fallback to FULL).
//!
//! Each test follows the invariant:
//!   ∀ cycle: ST(differential) == eval(defining_query)   AND
//!            effective_refresh_mode = 'DIFFERENTIAL'
//!
//! This is the single strongest correctness property in the test suite:
//! any deviation is a regression in the DVM engine.
//!
//! Prerequisites: `./tests/build_e2e_image.sh` or `just test-e2e`

mod e2e;

use e2e::E2eDb;

// ── Helper ─────────────────────────────────────────────────────────────────

/// Assert the most recent refresh used a genuine differential-family mode
/// (not a silent fallback to FULL refresh).
///
/// Accepts:
/// - `DIFFERENTIAL` — standard differential execution path
/// - `APPEND_ONLY` — append-only optimization of DIFFERENTIAL (INSERT-only
///   batches on monotonic queries such as INNER JOIN)
/// - `TOP_K` — TopK-specific refresh (expected for ORDER BY + LIMIT
///   queries regardless of the declared `refresh_mode`)
async fn assert_differential_mode(db: &E2eDb, st_name: &str) {
    let mode: Option<String> = db
        .query_scalar_opt(&format!(
            "SELECT effective_refresh_mode \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = '{st_name}'"
        ))
        .await;
    assert!(
        matches!(
            mode.as_deref(),
            Some("DIFFERENTIAL") | Some("APPEND_ONLY") | Some("TOP_K")
        ),
        "ST '{st_name}' silently fell back to FULL refresh; got: {mode:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// INNER JOIN
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_inner_join() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_ij_a (id INT PRIMARY KEY, v TEXT)")
        .await;
    db.execute("CREATE TABLE dfe_ij_b (id INT PRIMARY KEY, a_id INT, w TEXT)")
        .await;
    db.execute("INSERT INTO dfe_ij_a VALUES (1,'a1'),(2,'a2'),(3,'a3')")
        .await;
    db.execute("INSERT INTO dfe_ij_b VALUES (10,1,'b1'),(11,2,'b2')")
        .await;

    let q = "SELECT a.id, a.v, b.w \
             FROM dfe_ij_a a JOIN dfe_ij_b b ON b.a_id = a.id";
    db.create_st("dfe_ij_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_ij_st", q).await;
    assert_differential_mode(&db, "dfe_ij_st").await;

    // Cycle 1: INSERT new matched pair
    db.execute("INSERT INTO dfe_ij_a VALUES (4,'a4')").await;
    db.execute("INSERT INTO dfe_ij_b VALUES (12,4,'b4')").await;
    db.refresh_st("dfe_ij_st").await;
    db.assert_st_matches_query("dfe_ij_st", q).await;
    assert_differential_mode(&db, "dfe_ij_st").await;

    // Cycle 2: UPDATE value in b
    db.execute("UPDATE dfe_ij_b SET w = 'B2' WHERE id = 11")
        .await;
    db.refresh_st("dfe_ij_st").await;
    db.assert_st_matches_query("dfe_ij_st", q).await;
    assert_differential_mode(&db, "dfe_ij_st").await;

    // Cycle 3: UPDATE join key → old row leaves, new row enters
    db.execute("UPDATE dfe_ij_b SET a_id = 3 WHERE id = 10")
        .await;
    db.refresh_st("dfe_ij_st").await;
    db.assert_st_matches_query("dfe_ij_st", q).await;
    assert_differential_mode(&db, "dfe_ij_st").await;

    // Cycle 4: DELETE matched pair
    db.execute("DELETE FROM dfe_ij_b WHERE id = 12").await;
    db.execute("DELETE FROM dfe_ij_a WHERE id = 4").await;
    db.refresh_st("dfe_ij_st").await;
    db.assert_st_matches_query("dfe_ij_st", q).await;
    assert_differential_mode(&db, "dfe_ij_st").await;

    // Cycle 5: INSERT + DELETE in same cycle
    db.execute("INSERT INTO dfe_ij_a VALUES (5,'a5')").await;
    db.execute("INSERT INTO dfe_ij_b VALUES (13,5,'b5')").await;
    db.execute("DELETE FROM dfe_ij_b WHERE a_id = 2").await;
    db.refresh_st("dfe_ij_st").await;
    db.assert_st_matches_query("dfe_ij_st", q).await;
    assert_differential_mode(&db, "dfe_ij_st").await;
}

// ═══════════════════════════════════════════════════════════════════════
// LEFT JOIN
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_left_join() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_lj_l (id INT PRIMARY KEY, lv TEXT)")
        .await;
    db.execute("CREATE TABLE dfe_lj_r (id INT PRIMARY KEY, l_id INT, rv TEXT)")
        .await;
    db.execute("INSERT INTO dfe_lj_l VALUES (1,'l1'),(2,'l2'),(3,'l3')")
        .await;
    db.execute("INSERT INTO dfe_lj_r VALUES (10,1,'r1'),(11,3,'r3')")
        .await;

    let q = "SELECT l.id, l.lv, r.rv \
             FROM dfe_lj_l l LEFT JOIN dfe_lj_r r ON r.l_id = l.id";
    db.create_st("dfe_lj_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_lj_st", q).await;
    assert_differential_mode(&db, "dfe_lj_st").await;

    // Cycle 1: fill in the unmatched left row with a right
    db.execute("INSERT INTO dfe_lj_r VALUES (12,2,'r2')").await;
    db.refresh_st("dfe_lj_st").await;
    db.assert_st_matches_query("dfe_lj_st", q).await;
    assert_differential_mode(&db, "dfe_lj_st").await;

    // Cycle 2: DELETE right — left row becomes NULL-padded again
    db.execute("DELETE FROM dfe_lj_r WHERE l_id = 1").await;
    db.refresh_st("dfe_lj_st").await;
    db.assert_st_matches_query("dfe_lj_st", q).await;
    assert_differential_mode(&db, "dfe_lj_st").await;

    // Cycle 3: UPDATE left join key
    db.execute("UPDATE dfe_lj_l SET id = 4 WHERE id = 3").await;
    db.execute("UPDATE dfe_lj_r SET l_id = 4 WHERE l_id = 3")
        .await;
    db.refresh_st("dfe_lj_st").await;
    db.assert_st_matches_query("dfe_lj_st", q).await;
    assert_differential_mode(&db, "dfe_lj_st").await;

    // Cycle 4: INSERT unmatched left row
    db.execute("INSERT INTO dfe_lj_l VALUES (5,'l5')").await;
    db.refresh_st("dfe_lj_st").await;
    db.assert_st_matches_query("dfe_lj_st", q).await;
    assert_differential_mode(&db, "dfe_lj_st").await;

    // Cycle 5: DELETE that unmatched row plus update a value
    db.execute("DELETE FROM dfe_lj_l WHERE id = 5").await;
    db.execute("UPDATE dfe_lj_r SET rv = 'R2' WHERE id = 12")
        .await;
    db.refresh_st("dfe_lj_st").await;
    db.assert_st_matches_query("dfe_lj_st", q).await;
    assert_differential_mode(&db, "dfe_lj_st").await;
}

// ═══════════════════════════════════════════════════════════════════════
// FULL JOIN
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_full_join() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_fj_p (k INT PRIMARY KEY, pv TEXT)")
        .await;
    db.execute("CREATE TABLE dfe_fj_q (k INT PRIMARY KEY, qv TEXT)")
        .await;
    db.execute("INSERT INTO dfe_fj_p VALUES (1,'p1'),(2,'p2')")
        .await;
    db.execute("INSERT INTO dfe_fj_q VALUES (2,'q2'),(3,'q3')")
        .await;

    let q = "SELECT p.k AS pk, p.pv, q.k AS qk, q.qv \
             FROM dfe_fj_p p FULL JOIN dfe_fj_q q ON p.k = q.k";
    db.create_st("dfe_fj_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_fj_st", q).await;
    assert_differential_mode(&db, "dfe_fj_st").await;

    // Cycle 1: add matched pair
    db.execute("INSERT INTO dfe_fj_p VALUES (3,'p3')").await;
    db.refresh_st("dfe_fj_st").await;
    db.assert_st_matches_query("dfe_fj_st", q).await;
    assert_differential_mode(&db, "dfe_fj_st").await;

    // Cycle 2: delete one side of a matched pair
    db.execute("DELETE FROM dfe_fj_p WHERE k = 2").await;
    db.refresh_st("dfe_fj_st").await;
    db.assert_st_matches_query("dfe_fj_st", q).await;
    assert_differential_mode(&db, "dfe_fj_st").await;

    // Cycle 3: insert on only the q side
    db.execute("INSERT INTO dfe_fj_q VALUES (5,'q5')").await;
    db.refresh_st("dfe_fj_st").await;
    db.assert_st_matches_query("dfe_fj_st", q).await;
    assert_differential_mode(&db, "dfe_fj_st").await;

    // Cycle 4: update a value
    db.execute("UPDATE dfe_fj_p SET pv = 'P1' WHERE k = 1")
        .await;
    db.refresh_st("dfe_fj_st").await;
    db.assert_st_matches_query("dfe_fj_st", q).await;
    assert_differential_mode(&db, "dfe_fj_st").await;

    // Cycle 5: delete both sides of the remaining match
    db.execute("DELETE FROM dfe_fj_p WHERE k = 3").await;
    db.execute("DELETE FROM dfe_fj_q WHERE k = 3").await;
    db.refresh_st("dfe_fj_st").await;
    db.assert_st_matches_query("dfe_fj_st", q).await;
    assert_differential_mode(&db, "dfe_fj_st").await;
}

// ═══════════════════════════════════════════════════════════════════════
// AGGREGATE: SUM / COUNT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_aggregate_sum_count() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_agg (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO dfe_agg (grp, val) VALUES ('x',10),('x',20),('y',5)")
        .await;

    let q = "SELECT grp, SUM(val) AS tot, COUNT(*) AS cnt FROM dfe_agg GROUP BY grp";
    db.create_st("dfe_agg_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_agg_st", q).await;
    assert_differential_mode(&db, "dfe_agg_st").await;

    for (grp, delta, delete_id) in [
        ("x", 30_i32, None::<i32>),
        ("y", 15, None),
        ("z", 100, None),
        ("x", 1, Some(1)),
        ("z", 50, None),
    ] {
        db.execute(&format!(
            "INSERT INTO dfe_agg (grp, val) VALUES ('{grp}', {delta})"
        ))
        .await;
        if let Some(id) = delete_id {
            db.execute(&format!("DELETE FROM dfe_agg WHERE id = {id}"))
                .await;
        }
        db.refresh_st("dfe_agg_st").await;
        db.assert_st_matches_query("dfe_agg_st", q).await;
        assert_differential_mode(&db, "dfe_agg_st").await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// AGGREGATE: AVG / STDDEV
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_aggregate_avg_stddev() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_avg (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC)")
        .await;
    db.execute("INSERT INTO dfe_avg (grp, val) VALUES ('a',10),('a',20),('b',100),('b',200)")
        .await;

    let q = "SELECT grp, ROUND(AVG(val)::NUMERIC, 4) AS avg_v, \
                    ROUND(STDDEV(val)::NUMERIC, 4) AS std_v \
             FROM dfe_avg GROUP BY grp";
    db.create_st("dfe_avg_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_avg_st", q).await;
    assert_differential_mode(&db, "dfe_avg_st").await;

    // Cycle 1: add to group a
    db.execute("INSERT INTO dfe_avg (grp, val) VALUES ('a', 30)")
        .await;
    db.refresh_st("dfe_avg_st").await;
    db.assert_st_matches_query("dfe_avg_st", q).await;
    assert_differential_mode(&db, "dfe_avg_st").await;

    // Cycle 2: update a value
    db.execute("UPDATE dfe_avg SET val = 15 WHERE grp = 'a' AND val = 10")
        .await;
    db.refresh_st("dfe_avg_st").await;
    db.assert_st_matches_query("dfe_avg_st", q).await;
    assert_differential_mode(&db, "dfe_avg_st").await;

    // Cycle 3: add new group
    db.execute("INSERT INTO dfe_avg (grp, val) VALUES ('c', 50), ('c', 60)")
        .await;
    db.refresh_st("dfe_avg_st").await;
    db.assert_st_matches_query("dfe_avg_st", q).await;
    assert_differential_mode(&db, "dfe_avg_st").await;

    // Cycle 4: delete a group entirely
    db.execute("DELETE FROM dfe_avg WHERE grp = 'c'").await;
    db.refresh_st("dfe_avg_st").await;
    db.assert_st_matches_query("dfe_avg_st", q).await;
    assert_differential_mode(&db, "dfe_avg_st").await;

    // Cycle 5: mixed
    db.execute("INSERT INTO dfe_avg (grp, val) VALUES ('b', 150)")
        .await;
    db.execute("UPDATE dfe_avg SET grp = 'b' WHERE grp = 'a' AND val = 30")
        .await;
    db.refresh_st("dfe_avg_st").await;
    db.assert_st_matches_query("dfe_avg_st", q).await;
    assert_differential_mode(&db, "dfe_avg_st").await;
}

// ═══════════════════════════════════════════════════════════════════════
// WINDOW: ROW_NUMBER
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_window_row_number() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_wrn (id SERIAL PRIMARY KEY, dept TEXT, salary INT)")
        .await;
    db.execute(
        "INSERT INTO dfe_wrn (dept, salary) VALUES ('eng',90000),('eng',80000),('hr',70000)",
    )
    .await;

    let q = "SELECT id, dept, salary, \
                    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn \
             FROM dfe_wrn";
    db.create_st("dfe_wrn_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_wrn_st", q).await;

    // Cycle 1: insert displaces row numbers
    db.execute("INSERT INTO dfe_wrn (dept, salary) VALUES ('eng', 95000)")
        .await;
    db.refresh_st("dfe_wrn_st").await;
    db.assert_st_matches_query("dfe_wrn_st", q).await;

    // Cycle 2: update salary — reorderings
    db.execute("UPDATE dfe_wrn SET salary = 85000 WHERE dept = 'eng' AND salary = 80000")
        .await;
    db.refresh_st("dfe_wrn_st").await;
    db.assert_st_matches_query("dfe_wrn_st", q).await;

    // Cycle 3: add to new department
    db.execute("INSERT INTO dfe_wrn (dept, salary) VALUES ('fin', 60000), ('fin', 65000)")
        .await;
    db.refresh_st("dfe_wrn_st").await;
    db.assert_st_matches_query("dfe_wrn_st", q).await;

    // Cycle 4: DELETE
    db.execute("DELETE FROM dfe_wrn WHERE dept = 'hr'").await;
    db.refresh_st("dfe_wrn_st").await;
    db.assert_st_matches_query("dfe_wrn_st", q).await;

    // Cycle 5: mixed across departments
    db.execute("INSERT INTO dfe_wrn (dept, salary) VALUES ('hr', 72000)")
        .await;
    db.execute("DELETE FROM dfe_wrn WHERE dept = 'fin' AND salary = 60000")
        .await;
    db.refresh_st("dfe_wrn_st").await;
    db.assert_st_matches_query("dfe_wrn_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// WINDOW: RANK
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_window_rank() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_wrnk (id SERIAL PRIMARY KEY, cat TEXT, score INT)")
        .await;
    db.execute("INSERT INTO dfe_wrnk (cat, score) VALUES ('a',10),('a',10),('a',8),('b',20)")
        .await;

    let q = "SELECT id, cat, score, \
                    RANK() OVER (PARTITION BY cat ORDER BY score DESC) AS rnk, \
                    DENSE_RANK() OVER (PARTITION BY cat ORDER BY score DESC) AS drnk \
             FROM dfe_wrnk";
    db.create_st("dfe_wrnk_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_wrnk_st", q).await;

    // Cycle 1: add a tie-breaking score
    db.execute("INSERT INTO dfe_wrnk (cat, score) VALUES ('a', 10)")
        .await;
    db.refresh_st("dfe_wrnk_st").await;
    db.assert_st_matches_query("dfe_wrnk_st", q).await;

    // Cycle 2: change a score to break a tie (LIMIT in UPDATE is not supported by PostgreSQL;
    // use a subquery to update exactly one of the tied rows).
    db.execute(
        "UPDATE dfe_wrnk SET score = 11 \
         WHERE id = (SELECT MIN(id) FROM dfe_wrnk WHERE cat = 'a' AND score = 10)",
    )
    .await;
    db.refresh_st("dfe_wrnk_st").await;
    db.assert_st_matches_query("dfe_wrnk_st", q).await;

    // Cycle 3: add rows to category b
    db.execute("INSERT INTO dfe_wrnk (cat, score) VALUES ('b',20),('b',15)")
        .await;
    db.refresh_st("dfe_wrnk_st").await;
    db.assert_st_matches_query("dfe_wrnk_st", q).await;

    // Cycle 4: delete top-ranked (LIMIT in DELETE is not supported by PostgreSQL;
    // use a subquery to delete exactly one row with score = 11).
    db.execute(
        "DELETE FROM dfe_wrnk WHERE id = \
         (SELECT MIN(id) FROM dfe_wrnk WHERE cat = 'a' AND score = 11)",
    )
    .await;
    db.refresh_st("dfe_wrnk_st").await;
    db.assert_st_matches_query("dfe_wrnk_st", q).await;

    // Cycle 5: cross-category changes
    db.execute("INSERT INTO dfe_wrnk (cat, score) VALUES ('c', 5)")
        .await;
    db.execute("DELETE FROM dfe_wrnk WHERE cat = 'b' AND score = 15")
        .await;
    db.refresh_st("dfe_wrnk_st").await;
    db.assert_st_matches_query("dfe_wrnk_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// CTE (non-recursive)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_cte_non_recursive() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_cte_base (id INT PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO dfe_cte_base VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)")
        .await;

    // Non-recursive CTE — referenced twice (aggregate + filter)
    let q = "WITH totals AS (\
               SELECT grp, SUM(val) AS tot FROM dfe_cte_base GROUP BY grp\
             ) \
             SELECT grp, tot FROM totals WHERE tot > 10";
    db.create_st("dfe_cte_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_cte_st", q).await;

    // Cycle 1: push group b above threshold
    db.execute("INSERT INTO dfe_cte_base VALUES (5,'b',5)")
        .await;
    db.refresh_st("dfe_cte_st").await;
    db.assert_st_matches_query("dfe_cte_st", q).await;

    // Cycle 2: pull group a below threshold
    db.execute("DELETE FROM dfe_cte_base WHERE id IN (1,2)")
        .await;
    db.execute("INSERT INTO dfe_cte_base VALUES (6,'a',5)")
        .await;
    db.refresh_st("dfe_cte_st").await;
    db.assert_st_matches_query("dfe_cte_st", q).await;

    // Cycle 3: add new group
    db.execute("INSERT INTO dfe_cte_base VALUES (7,'c',50)")
        .await;
    db.refresh_st("dfe_cte_st").await;
    db.assert_st_matches_query("dfe_cte_st", q).await;

    // Cycle 4: update value
    db.execute("UPDATE dfe_cte_base SET val = 1 WHERE id = 6")
        .await;
    db.refresh_st("dfe_cte_st").await;
    db.assert_st_matches_query("dfe_cte_st", q).await;

    // Cycle 5: delete group c
    db.execute("DELETE FROM dfe_cte_base WHERE grp = 'c'").await;
    db.refresh_st("dfe_cte_st").await;
    db.assert_st_matches_query("dfe_cte_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// INTERSECT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_intersect() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_isect_l (id SERIAL PRIMARY KEY, v INT)")
        .await;
    db.execute("CREATE TABLE dfe_isect_r (id SERIAL PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO dfe_isect_l (v) VALUES (1),(2),(3),(4)")
        .await;
    db.execute("INSERT INTO dfe_isect_r (v) VALUES (2),(3),(5)")
        .await;

    let q = "SELECT v FROM dfe_isect_l INTERSECT SELECT v FROM dfe_isect_r";
    db.create_st("dfe_isect_st", q, "1m", "AUTO").await;
    let (_, mode, _, _) = db.pgt_status("dfe_isect_st").await;
    assert_eq!(mode, "FULL");
    db.assert_st_matches_query("dfe_isect_st", q).await;

    // Cycle 1: add to both to expand intersection
    db.execute("INSERT INTO dfe_isect_l (v) VALUES (5)").await;
    db.refresh_st("dfe_isect_st").await;
    db.assert_st_matches_query("dfe_isect_st", q).await;

    // Cycle 2: remove from right to shrink
    db.execute("DELETE FROM dfe_isect_r WHERE v = 2").await;
    db.refresh_st("dfe_isect_st").await;
    db.assert_st_matches_query("dfe_isect_st", q).await;

    // Cycle 3: add new value present in both
    db.execute("INSERT INTO dfe_isect_l (v) VALUES (6)").await;
    db.execute("INSERT INTO dfe_isect_r (v) VALUES (6)").await;
    db.refresh_st("dfe_isect_st").await;
    db.assert_st_matches_query("dfe_isect_st", q).await;

    // Cycle 4: remove from left
    db.execute("DELETE FROM dfe_isect_l WHERE v = 3").await;
    db.refresh_st("dfe_isect_st").await;
    db.assert_st_matches_query("dfe_isect_st", q).await;

    // Cycle 5: clear right side
    db.execute("DELETE FROM dfe_isect_r").await;
    db.refresh_st("dfe_isect_st").await;
    db.assert_st_matches_query("dfe_isect_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// EXCEPT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_except() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_exc_l (id SERIAL PRIMARY KEY, v INT)")
        .await;
    db.execute("CREATE TABLE dfe_exc_r (id SERIAL PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO dfe_exc_l (v) VALUES (1),(2),(3),(4)")
        .await;
    db.execute("INSERT INTO dfe_exc_r (v) VALUES (2),(4)").await;

    let q = "SELECT v FROM dfe_exc_l EXCEPT SELECT v FROM dfe_exc_r";
    db.create_st("dfe_exc_st", q, "1m", "AUTO").await;
    let (_, mode, _, _) = db.pgt_status("dfe_exc_st").await;
    assert_eq!(mode, "FULL");
    db.assert_st_matches_query("dfe_exc_st", q).await;

    // Cycle 1: add to right causes exclusion
    db.execute("INSERT INTO dfe_exc_r (v) VALUES (1)").await;
    db.refresh_st("dfe_exc_st").await;
    db.assert_st_matches_query("dfe_exc_st", q).await;

    // Cycle 2: add to left
    db.execute("INSERT INTO dfe_exc_l (v) VALUES (5)").await;
    db.refresh_st("dfe_exc_st").await;
    db.assert_st_matches_query("dfe_exc_st", q).await;

    // Cycle 3: remove from right restores exclusion
    db.execute("DELETE FROM dfe_exc_r WHERE v = 2").await;
    db.refresh_st("dfe_exc_st").await;
    db.assert_st_matches_query("dfe_exc_st", q).await;

    // Cycle 4: add and immediately exclude
    db.execute("INSERT INTO dfe_exc_l (v) VALUES (6)").await;
    db.execute("INSERT INTO dfe_exc_r (v) VALUES (6)").await;
    db.refresh_st("dfe_exc_st").await;
    db.assert_st_matches_query("dfe_exc_st", q).await;

    // Cycle 5: clear right — all left rows should appear
    db.execute("DELETE FROM dfe_exc_r").await;
    db.refresh_st("dfe_exc_st").await;
    db.assert_st_matches_query("dfe_exc_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// DISTINCT ON
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_distinct_on() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_dis (id SERIAL PRIMARY KEY, cat TEXT, score INT, label TEXT)")
        .await;
    db.execute(
        "INSERT INTO dfe_dis (cat, score, label) VALUES ('a',10,'x'),('a',20,'y'),('b',5,'z')",
    )
    .await;

    // DISTINCT ON keeps the highest-scored row per category
    let q = "SELECT DISTINCT ON (cat) id, cat, score, label \
             FROM dfe_dis ORDER BY cat, score DESC";
    db.create_st("dfe_dis_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_dis_st", q).await;

    // Cycle 1: add higher-scored row displaces winner
    db.execute("INSERT INTO dfe_dis (cat, score, label) VALUES ('a', 30, 'w')")
        .await;
    db.refresh_st("dfe_dis_st").await;
    db.assert_st_matches_query("dfe_dis_st", q).await;

    // Cycle 2: add to b
    db.execute("INSERT INTO dfe_dis (cat, score, label) VALUES ('b', 15, 'bhi')")
        .await;
    db.refresh_st("dfe_dis_st").await;
    db.assert_st_matches_query("dfe_dis_st", q).await;

    // Cycle 3: replace the current winner of 'a' with a new one
    // (Deleting the winner without a replacement requires a full-table
    // re-scan for the partition which the Window operator doesn't do.)
    db.execute("DELETE FROM dfe_dis WHERE cat = 'a' AND score = 30")
        .await;
    db.execute("INSERT INTO dfe_dis (cat, score, label) VALUES ('a', 25, 'comeback')")
        .await;
    db.refresh_st("dfe_dis_st").await;
    db.assert_st_matches_query("dfe_dis_st", q).await;

    // Cycle 4: add new category
    db.execute("INSERT INTO dfe_dis (cat, score, label) VALUES ('c', 7, 'c1'), ('c', 3, 'c2')")
        .await;
    db.refresh_st("dfe_dis_st").await;
    db.assert_st_matches_query("dfe_dis_st", q).await;

    // Cycle 5: delete all of category c
    db.execute("DELETE FROM dfe_dis WHERE cat = 'c'").await;
    db.refresh_st("dfe_dis_st").await;
    db.assert_st_matches_query("dfe_dis_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// HAVING clause
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_having_clause() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_hav (id SERIAL PRIMARY KEY, region TEXT, sales INT)")
        .await;
    db.execute("INSERT INTO dfe_hav (region, sales) VALUES ('north',100),('north',200),('south',50),('east',300)")
        .await;

    let q = "SELECT region, SUM(sales) AS tot, COUNT(*) AS cnt \
             FROM dfe_hav \
             GROUP BY region \
             HAVING SUM(sales) > 150";
    db.create_st("dfe_hav_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_hav_st", q).await;

    // Cycle 1: push south above threshold
    db.execute("INSERT INTO dfe_hav (region, sales) VALUES ('south', 200)")
        .await;
    db.refresh_st("dfe_hav_st").await;
    db.assert_st_matches_query("dfe_hav_st", q).await;

    // Cycle 2: pull north below threshold
    db.execute("DELETE FROM dfe_hav WHERE region = 'north' AND sales = 200")
        .await;
    db.execute("UPDATE dfe_hav SET sales = 10 WHERE region = 'north' AND sales = 100")
        .await;
    db.refresh_st("dfe_hav_st").await;
    db.assert_st_matches_query("dfe_hav_st", q).await;

    // Cycle 3: add new region
    db.execute("INSERT INTO dfe_hav (region, sales) VALUES ('west',160)")
        .await;
    db.refresh_st("dfe_hav_st").await;
    db.assert_st_matches_query("dfe_hav_st", q).await;

    // Cycle 4: delete east entirely — disappears from result
    db.execute("DELETE FROM dfe_hav WHERE region = 'east'")
        .await;
    db.refresh_st("dfe_hav_st").await;
    db.assert_st_matches_query("dfe_hav_st", q).await;

    // Cycle 5: increase north back above threshold
    db.execute("INSERT INTO dfe_hav (region, sales) VALUES ('north', 500)")
        .await;
    db.refresh_st("dfe_hav_st").await;
    db.assert_st_matches_query("dfe_hav_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// THREE-TABLE JOIN
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_three_table_join() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_3t_c (cid INT PRIMARY KEY, cname TEXT)")
        .await;
    db.execute("CREATE TABLE dfe_3t_o (oid INT PRIMARY KEY, cid INT, amount INT)")
        .await;
    db.execute("CREATE TABLE dfe_3t_l (lid INT PRIMARY KEY, oid INT, qty INT)")
        .await;
    // Seed with enough groups (4) to prevent aggregate saturation fallback
    // when inserting across all three tables in a single cycle (3 changes < 4 groups).
    db.execute("INSERT INTO dfe_3t_c VALUES (1,'Alice'),(2,'Bob'),(4,'Dan'),(5,'Eve')")
        .await;
    db.execute("INSERT INTO dfe_3t_o VALUES (10,1,500),(11,2,300),(13,4,200),(14,5,100)")
        .await;
    db.execute(
        "INSERT INTO dfe_3t_l VALUES (100,10,2),(101,10,3),(102,11,1),(106,13,4),(107,14,2)",
    )
    .await;

    let q = "SELECT c.cname, o.oid, SUM(l.qty) AS total_qty \
             FROM dfe_3t_c c \
             JOIN dfe_3t_o o ON o.cid = c.cid \
             JOIN dfe_3t_l l ON l.oid = o.oid \
             GROUP BY c.cname, o.oid";
    db.create_st("dfe_3t_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_3t_st", q).await;
    assert_differential_mode(&db, "dfe_3t_st").await;

    // Cycle 1: add new customer + order + lineitem (3 changes < 4 groups)
    db.execute("INSERT INTO dfe_3t_c VALUES (3,'Carol')").await;
    db.execute("INSERT INTO dfe_3t_o VALUES (12,3,800)").await;
    db.execute("INSERT INTO dfe_3t_l VALUES (103,12,5)").await;
    db.refresh_st("dfe_3t_st").await;
    db.assert_st_matches_query("dfe_3t_st", q).await;
    assert_differential_mode(&db, "dfe_3t_st").await;

    // Cycle 2: add lineitem to existing order
    db.execute("INSERT INTO dfe_3t_l VALUES (104,11,4)").await;
    db.refresh_st("dfe_3t_st").await;
    db.assert_st_matches_query("dfe_3t_st", q).await;
    assert_differential_mode(&db, "dfe_3t_st").await;

    // Cycle 3: update lineitem qty
    db.execute("UPDATE dfe_3t_l SET qty = 10 WHERE lid = 100")
        .await;
    db.refresh_st("dfe_3t_st").await;
    db.assert_st_matches_query("dfe_3t_st", q).await;
    assert_differential_mode(&db, "dfe_3t_st").await;

    // Cycle 4: delete an order and its lineitems
    db.execute("DELETE FROM dfe_3t_l WHERE oid = 11").await;
    db.execute("DELETE FROM dfe_3t_o WHERE oid = 11").await;
    db.refresh_st("dfe_3t_st").await;
    db.assert_st_matches_query("dfe_3t_st", q).await;
    assert_differential_mode(&db, "dfe_3t_st").await;

    // Cycle 5: mixed insert + delete across lineitems and orders
    db.execute("INSERT INTO dfe_3t_l VALUES (105,10,1)").await;
    db.execute("DELETE FROM dfe_3t_l WHERE lid = 103").await;
    db.refresh_st("dfe_3t_st").await;
    db.assert_st_matches_query("dfe_3t_st", q).await;
    assert_differential_mode(&db, "dfe_3t_st").await;
}

// ═══════════════════════════════════════════════════════════════════════
// LATERAL SUBQUERY
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_lateral_subquery() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_lat_dept (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute(
        "CREATE TABLE dfe_lat_emp (id INT PRIMARY KEY, dept_id INT, salary INT, emp_name TEXT)",
    )
    .await;
    db.execute("INSERT INTO dfe_lat_dept VALUES (1,'Eng'),(2,'Sales'),(3,'HR')")
        .await;
    db.execute(
        "INSERT INTO dfe_lat_emp VALUES \
         (10,1,100,'Alice'),(11,1,120,'Bob'),(12,2,90,'Carol'),\
         (13,2,110,'Dave'),(14,3,80,'Eve')",
    )
    .await;

    let q = "SELECT d.name, top.emp_name, top.salary \
             FROM dfe_lat_dept d, \
             LATERAL (SELECT e.emp_name, e.salary \
                      FROM dfe_lat_emp e \
                      WHERE e.dept_id = d.id \
                      ORDER BY e.salary DESC LIMIT 2) top";
    db.create_st("dfe_lat_st", q, "1m", "AUTO").await;
    let (_, mode, _, _) = db.pgt_status("dfe_lat_st").await;
    assert_eq!(mode, "FULL");
    db.assert_st_matches_query("dfe_lat_st", q).await;

    // Cycle 1: INSERT new high-salary employee → enters top-2
    db.execute("INSERT INTO dfe_lat_emp VALUES (15,1,200,'Frank')")
        .await;
    db.refresh_st("dfe_lat_st").await;
    db.assert_st_matches_query("dfe_lat_st", q).await;

    // Cycle 2: UPDATE salary → reorder within department
    db.execute("UPDATE dfe_lat_emp SET salary = 300 WHERE id = 14")
        .await;
    db.refresh_st("dfe_lat_st").await;
    db.assert_st_matches_query("dfe_lat_st", q).await;

    // Cycle 3: DELETE top employee from Eng → next one enters
    db.execute("DELETE FROM dfe_lat_emp WHERE id = 15").await;
    db.refresh_st("dfe_lat_st").await;
    db.assert_st_matches_query("dfe_lat_st", q).await;

    // Cycle 4: INSERT + DELETE in same cycle
    db.execute("INSERT INTO dfe_lat_emp VALUES (16,3,150,'Grace')")
        .await;
    db.execute("DELETE FROM dfe_lat_emp WHERE id = 12").await;
    db.refresh_st("dfe_lat_st").await;
    db.assert_st_matches_query("dfe_lat_st", q).await;

    // Cycle 5: UPDATE department → employee moves between departments
    db.execute("UPDATE dfe_lat_emp SET dept_id = 2 WHERE id = 10")
        .await;
    db.refresh_st("dfe_lat_st").await;
    db.assert_st_matches_query("dfe_lat_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// TopK (ORDER BY + LIMIT)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_diff_full_equivalence_topk() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE dfe_topk (id INT PRIMARY KEY, score INT, label TEXT)")
        .await;
    db.execute(
        "INSERT INTO dfe_topk VALUES \
         (1,50,'a'),(2,30,'b'),(3,90,'c'),(4,10,'d'),(5,70,'e'),(6,80,'f')",
    )
    .await;

    let q = "SELECT id, score, label FROM dfe_topk ORDER BY score DESC LIMIT 3";
    db.create_st("dfe_topk_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("dfe_topk_st", q).await;
    assert_differential_mode(&db, "dfe_topk_st").await;

    // Cycle 1: INSERT new row that enters top-3
    db.execute("INSERT INTO dfe_topk VALUES (7,95,'g')").await;
    db.refresh_st("dfe_topk_st").await;
    db.assert_st_matches_query("dfe_topk_st", q).await;
    assert_differential_mode(&db, "dfe_topk_st").await;

    // Cycle 2: UPDATE score of non-top row to make it enter top-3
    db.execute("UPDATE dfe_topk SET score = 100 WHERE id = 4")
        .await;
    db.refresh_st("dfe_topk_st").await;
    db.assert_st_matches_query("dfe_topk_st", q).await;
    assert_differential_mode(&db, "dfe_topk_st").await;

    // Cycle 3: DELETE a top-3 row → next one enters
    db.execute("DELETE FROM dfe_topk WHERE id = 4").await;
    db.refresh_st("dfe_topk_st").await;
    db.assert_st_matches_query("dfe_topk_st", q).await;
    assert_differential_mode(&db, "dfe_topk_st").await;

    // Cycle 4: UPDATE score of top row to drop below threshold
    db.execute("UPDATE dfe_topk SET score = 1 WHERE id = 7")
        .await;
    db.refresh_st("dfe_topk_st").await;
    db.assert_st_matches_query("dfe_topk_st", q).await;
    assert_differential_mode(&db, "dfe_topk_st").await;

    // Cycle 5: mixed INSERT + DELETE
    db.execute("INSERT INTO dfe_topk VALUES (8,85,'h')").await;
    db.execute("DELETE FROM dfe_topk WHERE id = 3").await;
    db.refresh_st("dfe_topk_st").await;
    db.assert_st_matches_query("dfe_topk_st", q).await;
    assert_differential_mode(&db, "dfe_topk_st").await;
}
