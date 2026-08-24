//! Sensitivity Baseline and Negative Control Tests for COR-3 (#938 and #939).
//!
//! Validates:
//! - Exact Oracle Self-Tests:
//!   - Same count, different content detection.
//!   - Duplicate multiplicity (bag semantics) detection.
//!   - Schema type and arity mismatch detection.
//! - Issue #938 / #939 sensitivity baseline:
//!   - Deterministic reproduction of chained LEFT JOINs with aggregate CTEs and column pruning.
//!   - Reduced integer-key variant.
//!   - Wide-table / narrow-projection variant.
//!   - Simultaneous two-leaf mutations in a single refresh.
//! - Negative controls:
//!   - Physical width vs logical pruned width rescan.
//!   - Multi-leaf simultaneous change snapshot consistency.

mod e2e;

use e2e::E2eDb;
use e2e::oracle;

// ═══════════════════════════════════════════════════════════════════════════
//  1. Exact Oracle Self-Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_oracle_detects_same_count_different_content() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "CREATE TABLE oracle_sc_st (id INT PRIMARY KEY, val TEXT)",
        "CREATE TABLE oracle_sc_exp (id INT PRIMARY KEY, val TEXT)",
        "INSERT INTO oracle_sc_st VALUES (1, 'apple'), (2, 'banana')",
        "INSERT INTO oracle_sc_exp VALUES (1, 'apple'), (2, 'orange')",
    ])
    .await;

    let diff_result =
        oracle::compare_st_to_query(&db, "oracle_sc_st", "SELECT id, val FROM oracle_sc_exp").await;

    assert!(
        diff_result.is_err(),
        "Oracle must fail when contents differ even though counts are both 2"
    );

    let diff = diff_result.unwrap_err();
    assert_eq!(diff.actual_count, 2);
    assert_eq!(diff.expected_count, 2);
    assert_eq!(diff.extra_count, 1, "Should have 1 extra row ('banana')");
    assert_eq!(
        diff.missing_count, 1,
        "Should have 1 missing row ('orange')"
    );
    assert!(
        !diff.extra_rows.is_empty(),
        "Extra rows sample should be populated"
    );
    assert!(
        !diff.missing_rows.is_empty(),
        "Missing rows sample should be populated"
    );
    assert!(diff.extra_rows[0].contains("banana"));
    assert!(diff.missing_rows[0].contains("orange"));
}

#[tokio::test]
async fn test_oracle_detects_duplicate_multiplicity_mismatch() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "CREATE TABLE oracle_dup_st (category TEXT)",
        "CREATE TABLE oracle_dup_exp (category TEXT)",
        "INSERT INTO oracle_dup_st VALUES ('A'), ('A'), ('B')",
        "INSERT INTO oracle_dup_exp VALUES ('A'), ('B')",
    ])
    .await;

    let diff_result =
        oracle::compare_st_to_query(&db, "oracle_dup_st", "SELECT category FROM oracle_dup_exp")
            .await;

    assert!(
        diff_result.is_err(),
        "Oracle must fail on duplicate count multiplicity mismatch (bag equality)"
    );

    let diff = diff_result.unwrap_err();
    assert_eq!(diff.actual_count, 3);
    assert_eq!(diff.expected_count, 2);
    assert_eq!(diff.extra_count, 1);
    assert_eq!(diff.missing_count, 0);
}

#[tokio::test]
async fn test_oracle_detects_schema_mismatch() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "CREATE TABLE oracle_schema_st (id INT PRIMARY KEY, name TEXT)",
        "CREATE TABLE oracle_schema_exp (id INT PRIMARY KEY, name TEXT, extra_col INT)",
        "INSERT INTO oracle_schema_st VALUES (1, 'Alice')",
        "INSERT INTO oracle_schema_exp VALUES (1, 'Alice', 42)",
    ])
    .await;

    let diff_result = oracle::compare_st_to_query(
        &db,
        "oracle_schema_st",
        "SELECT id, name, extra_col FROM oracle_schema_exp",
    )
    .await;

    assert!(
        diff_result.is_err(),
        "Oracle must detect column count and schema mismatch"
    );

    let diff = diff_result.unwrap_err();
    assert!(
        diff.schema_mismatch.is_some(),
        "schema_mismatch must be set"
    );
    let mismatch = diff.schema_mismatch.unwrap();
    assert!(
        mismatch.contains("Column count mismatch"),
        "Got: {mismatch}"
    );
}

#[tokio::test]
async fn test_oracle_detects_incompatible_type_mismatch() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "CREATE TABLE oracle_type_st (id INT PRIMARY KEY, payload INT)",
        "CREATE TABLE oracle_type_exp (id INT PRIMARY KEY, payload TEXT)",
        "INSERT INTO oracle_type_st VALUES (1, 100)",
        "INSERT INTO oracle_type_exp VALUES (1, '100')",
    ])
    .await;

    let diff_result = oracle::compare_st_to_query(
        &db,
        "oracle_type_st",
        "SELECT id, payload FROM oracle_type_exp",
    )
    .await;

    assert!(
        diff_result.is_err(),
        "Oracle must detect incompatible column type mismatch (INT vs TEXT)"
    );

    let diff = diff_result.unwrap_err();
    assert!(diff.schema_mismatch.is_some());
    let mismatch = diff.schema_mismatch.unwrap();
    assert!(
        mismatch.contains("incompatible type OID"),
        "Got: {mismatch}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
//  2. Issue #938 / #939 Sensitivity Baseline Reproductions
// ═══════════════════════════════════════════════════════════════════════════

/// Full deterministic reproduction of the #938 / #939 chained LEFT JOINs
/// with aggregate CTE leaves and column pruning.
#[tokio::test]
async fn test_sensitivity_reproduction_938_939_chained_left_joins_aggregate_ctes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "CREATE TABLE sens_parent (id INT PRIMARY KEY, info TEXT)",
        "CREATE TABLE sens_d (\
             id INT PRIMARY KEY, parent_id INT NOT NULL REFERENCES sens_parent(id), \
             unneeded1 TEXT, unneeded2 INT, flag INT NOT NULL, created_at INT NOT NULL\
         )",
        "CREATE TABLE sens_e (\
             id INT PRIMARY KEY, parent_id INT NOT NULL REFERENCES sens_parent(id), \
             unneeded3 TEXT, label TEXT NOT NULL, created_at INT NOT NULL\
         )",
        "INSERT INTO sens_parent VALUES (1, 'p1'), (2, 'p2')",
    ])
    .await;

    let query = "WITH agg_d AS (\
                     SELECT d.parent_id, \
                            count(*) FILTER (WHERE d.flag = 1) AS cnt_pos, \
                            count(*) FILTER (WHERE d.flag = 0) AS cnt_zero, \
                            max(d.created_at) AS latest_d \
                     FROM sens_d d GROUP BY d.parent_id\
                 ), agg_e AS (\
                     SELECT e.parent_id, count(*) AS cnt_e, \
                            min(e.created_at) AS earliest_e, \
                            string_agg(e.label, ',' ORDER BY e.label) AS labels \
                     FROM sens_e e GROUP BY e.parent_id\
                 ) \
                 SELECT p.id AS parent_id, \
                        coalesce(d.cnt_pos, 0)::bigint AS cnt_pos, \
                        coalesce(d.cnt_zero, 0)::bigint AS cnt_zero, \
                        coalesce(e.cnt_e, 0)::bigint AS cnt_e, \
                        d.latest_d, e.earliest_e, e.labels \
                 FROM sens_parent p \
                 LEFT JOIN agg_d d ON d.parent_id = p.id \
                 LEFT JOIN agg_e e ON e.parent_id = p.id";

    db.create_st("sens_938_st", query, "1m", "DIFFERENTIAL")
        .await;
    oracle::assert_st_query_exact(&db, "sens_938_st", query, "initial setup").await;
    oracle::assert_effective_refresh_mode(&db, "sens_938_st", "DIFFERENTIAL")
        .await
        .unwrap();

    // Insert into d
    db.execute("INSERT INTO sens_d VALUES (1, 1, 'u1', 10, 1, 30)")
        .await;
    db.refresh_st("sens_938_st").await;
    oracle::assert_st_query_exact(&db, "sens_938_st", query, "after d insert").await;

    // Insert into e
    db.execute("INSERT INTO sens_e VALUES (1, 1, 'u2', 'beta', 40)")
        .await;
    db.refresh_st("sens_938_st").await;
    oracle::assert_st_query_exact(&db, "sens_938_st", query, "after e insert").await;

    // Simultaneous changes to both branches
    db.execute("INSERT INTO sens_d VALUES (2, 1, 'u1', 11, 0, 50), (3, 2, 'u1', 12, 1, 60)")
        .await;
    db.execute("INSERT INTO sens_e VALUES (2, 1, 'u2', 'alpha', 35), (3, 2, 'u2', 'gamma', 70)")
        .await;
    db.refresh_st("sens_938_st").await;
    oracle::assert_st_query_exact(&db, "sens_938_st", query, "after simultaneous inserts").await;

    // Updates
    db.execute("UPDATE sens_d SET flag = 1, created_at = 55 WHERE id = 2")
        .await;
    db.execute("UPDATE sens_e SET label = 'aardvark', created_at = 25 WHERE id = 2")
        .await;
    db.refresh_st("sens_938_st").await;
    oracle::assert_st_query_exact(&db, "sens_938_st", query, "after updates").await;

    // Deletes (transition back to NULL-padded)
    db.execute("DELETE FROM sens_d WHERE id IN (2, 3)").await;
    db.refresh_st("sens_938_st").await;
    oracle::assert_st_query_exact(&db, "sens_938_st", query, "after sens_d delete").await;

    db.execute("DELETE FROM sens_e WHERE id IN (2, 3)").await;
    db.refresh_st("sens_938_st").await;
    oracle::assert_st_query_exact(&db, "sens_938_st", query, "after sens_e delete").await;
}

/// Reduced integer-key variant preserving the logical composition failure mode.
#[tokio::test]
async fn test_sensitivity_reduced_integer_key_variant() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "CREATE TABLE red_p (id INT PRIMARY KEY)",
        "CREATE TABLE red_c1 (id INT PRIMARY KEY, p_id INT REFERENCES red_p(id), val INT)",
        "CREATE TABLE red_c2 (id INT PRIMARY KEY, p_id INT REFERENCES red_p(id), val INT)",
        "INSERT INTO red_p VALUES (1), (2), (3)",
    ])
    .await;

    let query = "WITH a1 AS (\
                     SELECT p_id, max(val) AS m1 FROM red_c1 GROUP BY p_id\
                 ), a2 AS (\
                     SELECT p_id, min(val) AS m2 FROM red_c2 GROUP BY p_id\
                 ) \
                 SELECT p.id, a1.m1, a2.m2 \
                 FROM red_p p \
                 LEFT JOIN a1 ON a1.p_id = p.id \
                 LEFT JOIN a2 ON a2.p_id = p.id";

    db.create_st("red_int_st", query, "1m", "DIFFERENTIAL")
        .await;
    oracle::assert_st_query_exact(&db, "red_int_st", query, "initial").await;

    // Mutate both c1 and c2 simultaneously
    db.execute("INSERT INTO red_c1 VALUES (1, 1, 10), (2, 2, 20)")
        .await;
    db.execute("INSERT INTO red_c2 VALUES (1, 1, 100), (2, 2, 200)")
        .await;
    db.refresh_st("red_int_st").await;
    oracle::assert_st_query_exact(&db, "red_int_st", query, "after simultaneous c1+c2").await;

    // Mutate parent and children together
    db.execute("INSERT INTO red_p VALUES (4)").await;
    db.execute("INSERT INTO red_c1 VALUES (3, 4, 40)").await;
    db.execute("DELETE FROM red_c2 WHERE id = 1").await;
    db.refresh_st("red_int_st").await;
    oracle::assert_st_query_exact(&db, "red_int_st", query, "after mixed mutations").await;
}

/// Wide physical table with many unselected columns pruned down to a narrow logical projection.
#[tokio::test]
async fn test_sensitivity_wide_table_narrow_projection_variant() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "CREATE TABLE wide_parent (id INT PRIMARY KEY, extra1 TEXT, extra2 TEXT, extra3 INT)",
        "CREATE TABLE wide_child (\
             id INT PRIMARY KEY, \
             parent_id INT NOT NULL REFERENCES wide_parent(id), \
             c1 INT, c2 INT, c3 INT, c4 TEXT, c5 TEXT, c6 TEXT, \
             target_val INT NOT NULL\
         )",
        "INSERT INTO wide_parent VALUES (1, 'x', 'y', 10), (2, 'x2', 'y2', 20)",
        "INSERT INTO wide_child VALUES \
             (1, 1, 0,0,0,'a','b','c', 100), \
             (2, 1, 0,0,0,'d','e','f', 200), \
             (3, 2, 0,0,0,'g','h','i', 300)",
    ])
    .await;

    let query = "WITH child_max AS (\
                     SELECT parent_id, max(target_val) AS top_val \
                     FROM wide_child GROUP BY parent_id\
                 ) \
                 SELECT p.id, cm.top_val \
                 FROM wide_parent p \
                 LEFT JOIN child_max cm ON cm.parent_id = p.id";

    db.create_st("wide_pruned_st", query, "1m", "DIFFERENTIAL")
        .await;
    oracle::assert_st_query_exact(&db, "wide_pruned_st", query, "initial").await;

    // Delete winner in group 1 to force group rescan on the wide table
    db.execute("DELETE FROM wide_child WHERE id = 2").await;
    db.refresh_st("wide_pruned_st").await;
    oracle::assert_st_query_exact(&db, "wide_pruned_st", query, "after delete winner").await;

    // Add new row with higher target_val
    db.execute("INSERT INTO wide_child VALUES (4, 1, 9,9,9,'j','k','l', 500)")
        .await;
    db.refresh_st("wide_pruned_st").await;
    oracle::assert_st_query_exact(&db, "wide_pruned_st", query, "after insert new max").await;
}

/// Simultaneous mutations across two aggregate leaves in a single cycle.
#[tokio::test]
async fn test_sensitivity_simultaneous_two_leaf_changes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "CREATE TABLE sim_p (id INT PRIMARY KEY, name TEXT)",
        "CREATE TABLE sim_l (id INT PRIMARY KEY, p_id INT REFERENCES sim_p(id), score INT)",
        "CREATE TABLE sim_r (id INT PRIMARY KEY, p_id INT REFERENCES sim_p(id), rating INT)",
        "INSERT INTO sim_p VALUES (1, 'p1'), (2, 'p2')",
        "INSERT INTO sim_l VALUES (1, 1, 50), (2, 2, 80)",
        "INSERT INTO sim_r VALUES (1, 1, 4), (2, 2, 5)",
    ])
    .await;

    let query = "WITH al AS (\
                     SELECT p_id, sum(score) AS sum_s, count(*) AS cnt_l \
                     FROM sim_l GROUP BY p_id\
                 ), ar AS (\
                     SELECT p_id, avg(rating) AS avg_r, count(*) AS cnt_r \
                     FROM sim_r GROUP BY p_id\
                 ) \
                 SELECT p.id, p.name, al.sum_s, al.cnt_l, ar.avg_r, ar.cnt_r \
                 FROM sim_p p \
                 LEFT JOIN al ON al.p_id = p.id \
                 LEFT JOIN ar ON ar.p_id = p.id";

    db.create_st("sim_two_leaf_st", query, "1m", "DIFFERENTIAL")
        .await;
    oracle::assert_st_query_exact(&db, "sim_two_leaf_st", query, "initial").await;

    // Mutate both left and right branches simultaneously
    db.execute("INSERT INTO sim_l VALUES (3, 1, 25), (4, 2, 10)")
        .await;
    db.execute("UPDATE sim_r SET rating = 3 WHERE id = 1").await;
    db.execute("INSERT INTO sim_r VALUES (3, 2, 1)").await;
    db.refresh_st("sim_two_leaf_st").await;
    oracle::assert_st_query_exact(&db, "sim_two_leaf_st", query, "after simultaneous deltas").await;
}

// ═══════════════════════════════════════════════════════════════════════════
//  3. Negative Controls (Intentional Defect Detection Verification)
// ═══════════════════════════════════════════════════════════════════════════

/// Negative control: proves the exact oracle catches physical-width rescan mismatch.
#[tokio::test]
async fn test_sensitivity_negative_control_physical_width() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a physical table with extra columns
    db.execute_seq(&[
        "CREATE TABLE nc_phys_src (id INT PRIMARY KEY, grp INT, val INT, col_extra1 TEXT, col_extra2 INT)",
        "INSERT INTO nc_phys_src VALUES (1, 1, 10, 'extra', 100), (2, 1, 20, 'extra', 200)",
    ])
    .await;

    // If an engine bug were to select the entire physical relation (5 cols)
    // instead of the logical relation (3 cols), the oracle must catch it:
    let logical_query = "SELECT grp, max(val) AS max_v FROM nc_phys_src GROUP BY grp";
    let physical_query = "SELECT grp, max(val) AS max_v, max(col_extra1) AS c1, max(col_extra2) AS c2 FROM nc_phys_src GROUP BY grp";

    db.create_st("nc_phys_st", logical_query, "1m", "FULL")
        .await;

    // When compared against the erroneous wider query, the oracle MUST fail:
    let diff = oracle::compare_st_to_query(&db, "nc_phys_st", physical_query).await;
    assert!(
        diff.is_err(),
        "Oracle must catch physical-width divergence against pruned logical schema"
    );
    let err = diff.unwrap_err();
    assert!(err.schema_mismatch.is_some());
}

/// Negative control: proves that an incorrect aggregate CTE snapshot that leaves
/// stale cross-product rows is detected by the multiset oracle even if total row count matches.
#[tokio::test]
async fn test_sensitivity_negative_control_cte_leaf_snapshot() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "CREATE TABLE nc_snap_st (id INT, v1 INT, v2 INT)",
        "CREATE TABLE nc_snap_exp (id INT, v1 INT, v2 INT)",
        // 2 rows in actual ST, 2 rows in expected query (equal counts)
        // but row 2 has a stale value (v2=100 instead of v2=200):
        "INSERT INTO nc_snap_st VALUES (1, 10, 50), (2, 20, 100)",
        "INSERT INTO nc_snap_exp VALUES (1, 10, 50), (2, 20, 200)",
    ])
    .await;

    let diff =
        oracle::compare_st_to_query(&db, "nc_snap_st", "SELECT id, v1, v2 FROM nc_snap_exp").await;
    assert!(
        diff.is_err(),
        "Oracle must catch stale snapshot data even when actual and expected row counts match"
    );
    let err = diff.unwrap_err();
    assert_eq!(err.actual_count, 2);
    assert_eq!(err.expected_count, 2);
    assert_eq!(err.extra_count, 1);
    assert_eq!(err.missing_count, 1);
    assert!(err.extra_rows[0].contains("100"));
    assert!(err.missing_rows[0].contains("200"));
}
