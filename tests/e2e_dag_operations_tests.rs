//! E2E tests for operational mid-pipeline changes on DAG stream tables.
//!
//! Validates that SUSPEND, ALTER (schedule/mode/query), and DROP on
//! intermediate nodes in a multi-layer pipeline behave correctly.
//!
//! ## Key architecture paths exercised
//!
//! - `DAG_REBUILD_SIGNAL` — incremented on ALTER/DROP, scheduler rebuilds its in-memory `StDag`
//! - `status = 'SUSPENDED'` — scheduler skips SUSPENDED STs in topological walk
//! - `determine_refresh_action()` returning `NoAction` for SUSPENDED upstream
//! - DROP cascade — `drop_stream_table()` uses `DROP TABLE ... CASCADE`
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════════
// Shared Setup Helpers
// ═══════════════════════════════════════════════════════════════════════════

/// Create a 3-layer linear pipeline for operation tests.
///
/// ```text
/// ops_src (base)
///   → ops_l1 (GROUP BY grp, SUM)
///       → ops_l2 (project: doubled = total * 2)
///           → ops_l3 (filter: doubled > 30)
/// ```
async fn setup_ops_pipeline(db: &E2eDb) {
    db.execute(
        "CREATE TABLE ops_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO ops_src (grp, val) VALUES
            ('a', 10), ('a', 20),
            ('b', 5),  ('b', 10),
            ('c', 50)",
    )
    .await;

    // L1: aggregate
    db.create_st(
        "ops_l1",
        "SELECT grp, SUM(val) AS total FROM ops_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L2: project (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ops_l2',
            $$SELECT grp, total * 2 AS doubled FROM ops_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // L3: filter (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ops_l3',
            $$SELECT grp, doubled FROM ops_l2 WHERE doubled > 30$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;
}

/// Refresh the ops pipeline in topological order.
async fn refresh_ops_pipeline(db: &E2eDb) {
    db.refresh_st_until_converged("ops_l1").await;
    db.refresh_st_until_converged("ops_l2").await;
    db.refresh_st_until_converged("ops_l3").await;
}

/// Ground-truth queries for the ops pipeline.
const OPS_L1_Q: &str = "SELECT grp, SUM(val) AS total FROM ops_src GROUP BY grp";
const OPS_L2_Q: &str = "SELECT grp, SUM(val) * 2 AS doubled FROM ops_src GROUP BY grp";
const OPS_L3_Q: &str =
    "SELECT grp, SUM(val) * 2 AS doubled FROM ops_src GROUP BY grp HAVING SUM(val) * 2 > 30";

/// Assert correctness at all layers.
async fn assert_ops_pipeline_correct(db: &E2eDb) {
    db.assert_st_matches_query("ops_l1", OPS_L1_Q).await;
    db.assert_st_matches_query("ops_l2", OPS_L2_Q).await;
    db.assert_st_matches_query("ops_l3", OPS_L3_Q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3.1 — SUSPEND middle layer blocks downstream
// ═══════════════════════════════════════════════════════════════════════════

/// Suspend the middle layer (L2). Mutate base, refresh L1, then verify
/// that L3's data_timestamp doesn't advance (manual refresh of suspended
/// ST should be rejected). Resume L2, refresh L2→L3, verify convergence.
#[tokio::test]
async fn test_suspend_middle_layer_blocks_downstream() {
    let db = E2eDb::new().await.with_extension().await;
    setup_ops_pipeline(&db).await;
    assert_ops_pipeline_correct(&db).await;

    // Suspend L2
    db.alter_st("ops_l2", "status => 'SUSPENDED'").await;
    let (status, _, _, _) = db.pgt_status("ops_l2").await;
    assert_eq!(status, "SUSPENDED");

    // Record L3's data_timestamp
    let ts_before: String = db
        .query_scalar(
            "SELECT COALESCE(data_timestamp::text, 'null') \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ops_l3'",
        )
        .await;

    // Mutate base and refresh only L1
    db.execute("INSERT INTO ops_src (grp, val) VALUES ('a', 100)")
        .await;
    db.refresh_st_until_converged("ops_l1").await;
    db.assert_st_matches_query("ops_l1", OPS_L1_Q).await;

    // Refreshing L2 should fail (SUSPENDED)
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('ops_l2')")
        .await;
    assert!(
        result.is_err(),
        "refresh on SUSPENDED ST should be rejected"
    );

    // L3 should not have advanced
    let ts_after: String = db
        .query_scalar(
            "SELECT COALESCE(data_timestamp::text, 'null') \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ops_l3'",
        )
        .await;
    assert_eq!(
        ts_before, ts_after,
        "L3 data_timestamp should not advance while L2 is SUSPENDED"
    );

    // Resume L2 and refresh the rest of the pipeline
    db.alter_st("ops_l2", "status => 'ACTIVE'").await;
    db.refresh_st_until_converged("ops_l2").await;
    db.refresh_st_until_converged("ops_l3").await;
    assert_ops_pipeline_correct(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3.2 — ALTER schedule mid-pipeline
// ═══════════════════════════════════════════════════════════════════════════

/// Alter the middle layer's schedule. Verify that manual refresh still
/// works and the pipeline converges after the schedule change.
#[tokio::test]
async fn test_alter_schedule_mid_pipeline() {
    let db = E2eDb::new().await.with_extension().await;
    setup_ops_pipeline(&db).await;
    assert_ops_pipeline_correct(&db).await;

    // Change L1's schedule from 1m to 5s
    db.alter_st("ops_l1", "schedule => '5s'").await;

    // Verify schedule changed
    let schedule: String = db
        .query_scalar("SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ops_l1'")
        .await;
    assert_eq!(schedule, "5s");

    // Keep both mutation cycles under manual control after changing the schedule.
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['ops_l1', 'ops_l2', 'ops_l3'])")
        .await;

    // Mutate and refresh — pipeline should still converge
    db.execute("INSERT INTO ops_src (grp, val) VALUES ('d', 99)")
        .await;
    refresh_ops_pipeline(&db).await;
    assert_ops_pipeline_correct(&db).await;

    // Another cycle to confirm stability
    db.execute("DELETE FROM ops_src WHERE grp = 'd'").await;
    let mut writer = db.pool.begin().await.expect("begin controlled DAG writer");
    sqlx::query("LOCK TABLE ops_src IN ROW EXCLUSIVE MODE")
        .execute(&mut *writer)
        .await
        .expect("hold controlled DAG writer");
    let last_refresh: i64 = db
        .query_scalar("SELECT COALESCE(MAX(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    tokio::join!(refresh_ops_pipeline(&db), async {
        assert!(
            db.wait_for_condition(
                "DAG delete held back by a source writer",
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                     JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
                     WHERE st.pgt_name = 'ops_l1' AND h.refresh_id > {last_refresh} \
                       AND h.initiated_by = 'MANUAL' AND h.status = 'COMPLETED')"
                ),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_millis(50),
            )
            .await,
            "DAG refresh must complete a held-back attempt before releasing the writer"
        );
        let stale_group: bool = db
            .query_scalar("SELECT EXISTS(SELECT 1 FROM ops_l1 WHERE grp = 'd')")
            .await;
        assert!(
            stale_group,
            "writer holdback must preserve the previous group"
        );
        writer
            .rollback()
            .await
            .expect("release controlled DAG writer");
    });
    assert_ops_pipeline_correct(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3.3 — ALTER mode mid-pipeline (DIFF → FULL)
// ═══════════════════════════════════════════════════════════════════════════

/// Change the middle layer (L1) from DIFFERENTIAL to FULL.
/// Verify next refresh does full recompute and downstream layers converge.
#[tokio::test]
async fn test_alter_mode_mid_pipeline_diff_to_full() {
    let db = E2eDb::new().await.with_extension().await;
    setup_ops_pipeline(&db).await;
    assert_ops_pipeline_correct(&db).await;

    // ALTER L1 from DIFFERENTIAL → FULL
    db.alter_st("ops_l1", "refresh_mode => 'FULL'").await;

    let (_, mode, _, _) = db.pgt_status("ops_l1").await;
    assert_eq!(mode, "FULL", "L1 should now be FULL mode");

    // Mutate and refresh — L1 does full recompute, downstream follows
    db.execute("INSERT INTO ops_src (grp, val) VALUES ('e', 75)")
        .await;
    refresh_ops_pipeline(&db).await;
    assert_ops_pipeline_correct(&db).await;

    // DELETE cycle
    db.execute("DELETE FROM ops_src WHERE grp = 'b'").await;
    refresh_ops_pipeline(&db).await;
    assert_ops_pipeline_correct(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3.4 — ALTER query mid-pipeline
// ═══════════════════════════════════════════════════════════════════════════

/// Change L1's defining query (add a WHERE filter).
/// After the mode-change reinit, the narrower result set should cascade
/// through L2 and L3.
///
/// Note: `alter_stream_table` doesn't directly support changing the
/// defining query. Instead we DROP + re-CREATE the ST. This test validates
/// that the pipeline can be repaired after a middle-node re-creation.
#[tokio::test]
async fn test_alter_query_mid_pipeline() {
    let db = E2eDb::new().await.with_extension().await;
    setup_ops_pipeline(&db).await;
    assert_ops_pipeline_correct(&db).await;

    // Drop L3 and L2 first (reverse dependency order), then L1
    db.drop_st("ops_l3").await;
    db.drop_st("ops_l2").await;
    db.drop_st("ops_l1").await;

    // Re-create L1 with a different query (filtered)
    db.create_st(
        "ops_l1",
        "SELECT grp, SUM(val) AS total FROM ops_src WHERE grp != 'b' GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Re-create L2 and L3
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ops_l2',
            $$SELECT grp, total * 2 AS doubled FROM ops_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ops_l3',
            $$SELECT grp, doubled FROM ops_l2 WHERE doubled > 30$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Ground-truth for filtered pipeline
    let l1_filtered_q = "SELECT grp, SUM(val) AS total FROM ops_src WHERE grp != 'b' GROUP BY grp";
    let l2_filtered_q =
        "SELECT grp, SUM(val) * 2 AS doubled FROM ops_src WHERE grp != 'b' GROUP BY grp";
    let l3_filtered_q = "SELECT grp, SUM(val) * 2 AS doubled FROM ops_src \
                         WHERE grp != 'b' GROUP BY grp HAVING SUM(val) * 2 > 30";

    db.assert_st_matches_query("ops_l1", l1_filtered_q).await;
    db.assert_st_matches_query("ops_l2", l2_filtered_q).await;
    db.assert_st_matches_query("ops_l3", l3_filtered_q).await;

    // Mutate and verify cascade still works
    db.execute("INSERT INTO ops_src (grp, val) VALUES ('a', 50)")
        .await;
    db.refresh_st_until_converged("ops_l1").await;
    db.refresh_st_until_converged("ops_l2").await;
    db.refresh_st_until_converged("ops_l3").await;

    db.assert_st_matches_query("ops_l1", l1_filtered_q).await;
    db.assert_st_matches_query("ops_l2", l2_filtered_q).await;
    db.assert_st_matches_query("ops_l3", l3_filtered_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3.5 — DROP leaf keeps pipeline intact
// ═══════════════════════════════════════════════════════════════════════════

/// Drop the leaf (L3). Verify L1 and L2 are unaffected and can still refresh.
#[tokio::test]
async fn test_drop_leaf_keeps_pipeline_intact() {
    let db = E2eDb::new().await.with_extension().await;
    setup_ops_pipeline(&db).await;
    assert_ops_pipeline_correct(&db).await;

    db.drop_st("ops_l3").await;

    // L3 should be gone from catalog
    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ops_l3')",
        )
        .await;
    assert!(!exists, "L3 should be gone from catalog after DROP");

    // Mutate and refresh L1, L2 — should still work
    db.execute("INSERT INTO ops_src (grp, val) VALUES ('z', 999)")
        .await;
    db.refresh_st_until_converged("ops_l1").await;
    db.refresh_st_until_converged("ops_l2").await;

    db.assert_st_matches_query("ops_l1", OPS_L1_Q).await;
    db.assert_st_matches_query("ops_l2", OPS_L2_Q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3.6 — DROP middle layer cascades to downstream
// ═══════════════════════════════════════════════════════════════════════════

/// Drop the middle layer (L2). Because L3 depends on L2's storage table
/// via its defining query, dropping L2 (which uses CASCADE) should cascade
/// and invalidate or destroy L3 as well. L1 should remain intact.
#[tokio::test]
async fn test_drop_middle_layer_cascades() {
    let db = E2eDb::new().await.with_extension().await;
    setup_ops_pipeline(&db).await;
    assert_ops_pipeline_correct(&db).await;

    // Drop L2 with cascade — L3 depends on L2 so cascade is required
    db.drop_st_cascade("ops_l2").await;

    // L2 should be gone from catalog
    let l2_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ops_l2')",
        )
        .await;
    assert!(!l2_exists, "L2 should be gone from catalog");

    // L3's storage table should also be gone (CASCADE from DROP TABLE)
    let l3_table_exists = db.table_exists("public", "ops_l3").await;
    assert!(
        !l3_table_exists,
        "L3 storage table should be cascaded away when L2 is dropped"
    );

    // L1 should still be intact and refreshable
    db.execute("INSERT INTO ops_src (grp, val) VALUES ('z', 42)")
        .await;
    db.refresh_st_until_converged("ops_l1").await;
    db.assert_st_matches_query("ops_l1", OPS_L1_Q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3.7 — SUSPEND + RESUME cycle preserves data consistency
// ═══════════════════════════════════════════════════════════════════════════

/// Suspend L1, mutate base several times (no refresh on L1), resume L1,
/// single refresh. Verify L1 has cumulative changes and downstream converges.
#[tokio::test]
async fn test_suspend_resume_cycle_data_consistency() {
    let db = E2eDb::new().await.with_extension().await;

    // Simple 2-layer pipeline for this test
    db.execute(
        "CREATE TABLE sr_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO sr_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    db.create_st(
        "sr_l1",
        "SELECT grp, SUM(val) AS total FROM sr_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'sr_l2',
            $$SELECT grp, total * 2 AS doubled FROM sr_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    let l1_q = "SELECT grp, SUM(val) AS total FROM sr_src GROUP BY grp";
    let l2_q = "SELECT grp, SUM(val) * 2 AS doubled FROM sr_src GROUP BY grp";

    db.assert_st_matches_query("sr_l1", l1_q).await;
    db.assert_st_matches_query("sr_l2", l2_q).await;

    // Suspend L1
    db.alter_st("sr_l1", "status => 'SUSPENDED'").await;

    // Multiple mutations while L1 is suspended
    db.execute("INSERT INTO sr_src (grp, val) VALUES ('c', 30)")
        .await;
    db.execute("UPDATE sr_src SET val = val + 5 WHERE grp = 'a'")
        .await;
    db.execute("INSERT INTO sr_src (grp, val) VALUES ('d', 40)")
        .await;

    // Resume L1 and do a single refresh
    db.alter_st("sr_l1", "status => 'ACTIVE'").await;
    db.refresh_st_until_converged("sr_l1").await;
    db.refresh_st_until_converged("sr_l2").await;

    // All cumulative changes should be reflected
    db.assert_st_matches_query("sr_l1", l1_q).await;
    db.assert_st_matches_query("sr_l2", l2_q).await;
}
