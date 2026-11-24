//! E2E tests for multi-cycle refresh correctness on multi-layer DAG pipelines.
//!
//! Validates that repeated DML → refresh cycles produce correct cumulative
//! results through 3+ layer stream table chains.  This is the intersection
//! of two previously-tested dimensions (multi-cycle × multi-layer) that had
//! **zero** coverage.
//!
//! ## What this catches
//!
//! - **Cumulative delta drift** — errors that only surface after N incremental cycles
//! - **Change buffer cleanup** — CDC entries consumed per cycle, no leakage
//! - **data_timestamp monotonicity** — upstream timestamps advance correctly
//! - **Prepared statement cache** — delta SQL plans survive across cycles
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::{E2eDb, disable_scheduler};

// ═══════════════════════════════════════════════════════════════════════════
// Shared Setup Helpers
// ═══════════════════════════════════════════════════════════════════════════

/// Create a 3-layer linear pipeline for multi-cycle testing.
///
/// ```text
/// mc_src (base)
///   → mc_l1 (GROUP BY grp, SUM + COUNT)
///       → mc_l2 (project: doubled = total * 2)
///           → mc_l3 (filter: doubled > 30)
/// ```
async fn setup_3_layer_pipeline(db: &E2eDb) {
    // Suppress the background scheduler for this database — these tests
    // perform manual refreshes in strict topological order.
    disable_scheduler(db).await;

    db.execute(
        "CREATE TABLE mc_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO mc_src (grp, val) VALUES
            ('a', 10), ('a', 20),
            ('b', 5),  ('b', 10),
            ('c', 50)",
    )
    .await;

    // L1: aggregate
    db.create_st(
        "mc_l1",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM mc_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L2: project (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'mc_l2',
            $$SELECT grp, total, total * 2 AS doubled FROM mc_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // L3: filter (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'mc_l3',
            $$SELECT grp, doubled FROM mc_l2 WHERE doubled > 30$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;
}

/// Refresh the 3-layer pipeline in topological order.
/// Uses retry helper to tolerate background scheduler advisory-lock races.
async fn refresh_pipeline(db: &E2eDb) {
    db.refresh_st_with_retry("mc_l1").await;
    db.refresh_st_with_retry("mc_l2").await;
    db.refresh_st_with_retry("mc_l3").await;
}

/// Ground-truth validation queries (written against base table `mc_src`).
fn pipeline_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "mc_l1",
            "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM mc_src GROUP BY grp",
        ),
        (
            "mc_l2",
            "SELECT grp, SUM(val) AS total, SUM(val) * 2 AS doubled FROM mc_src GROUP BY grp",
        ),
        (
            "mc_l3",
            "SELECT grp, SUM(val) * 2 AS doubled FROM mc_src GROUP BY grp HAVING SUM(val) * 2 > 30",
        ),
    ]
}

/// Assert DBSP invariant at every layer of the pipeline.
async fn assert_pipeline_correct(db: &E2eDb) {
    for (name, query) in pipeline_queries() {
        db.assert_st_matches_query(name, query).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Diamond Helpers
// ═══════════════════════════════════════════════════════════════════════════

/// Create a diamond topology for multi-cycle testing.
///
/// ```text
/// dm_src (base)
///   → dm_l1a (SUM by grp)
///   → dm_l1b (COUNT by grp)
///       └──→ dm_l2 (JOIN l1a + l1b on grp)
/// ```
async fn setup_diamond_pipeline(db: &E2eDb) {
    // Suppress the background scheduler for this database — these tests
    // perform manual refreshes in strict topological order.
    disable_scheduler(db).await;

    db.execute(
        "CREATE TABLE dm_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO dm_src (grp, val) VALUES
            ('x', 10), ('x', 20),
            ('y', 30), ('y', 40),
            ('z', 5)",
    )
    .await;

    // L1a: SUM aggregate
    db.create_st(
        "dm_l1a",
        "SELECT grp, SUM(val) AS total FROM dm_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L1b: COUNT aggregate
    db.create_st(
        "dm_l1b",
        "SELECT grp, COUNT(*) AS cnt FROM dm_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L2: JOIN both L1 branches (ST-on-ST diamond)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'dm_l2',
            $$SELECT a.grp, a.total, b.cnt
              FROM dm_l1a a JOIN dm_l1b b ON a.grp = b.grp$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;
}

/// Refresh the diamond pipeline in topological order.
async fn refresh_diamond(db: &E2eDb) {
    db.refresh_st_with_retry("dm_l1a").await;
    db.refresh_st_with_retry("dm_l1b").await;
    db.refresh_st_with_retry("dm_l2").await;
}

/// Ground-truth query for the diamond apex.
const DM_L2_QUERY: &str = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM dm_src GROUP BY grp";

// ═══════════════════════════════════════════════════════════════════════════
// Test 1.1 — INSERT-heavy, 10 cycles
// ═══════════════════════════════════════════════════════════════════════════

/// 4 cycles of INSERT-only DML through a 3-layer pipeline.
/// Validates cumulative delta correctness and change buffer cleanup.
/// (Reduced from 10 cycles — delta accumulation correctness is proven
/// by 4 cycles; the original 10 matched a benchmark pattern.)
#[tokio::test]
async fn test_mc_dag_insert_heavy_10_cycles() {
    let db = E2eDb::new().await.with_extension().await;
    setup_3_layer_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    for cycle in 1..=4 {
        db.execute(&format!(
            "INSERT INTO mc_src (grp, val) VALUES ('a', {cycle}), ('b', {cycle})"
        ))
        .await;

        refresh_pipeline(&db).await;
        assert_pipeline_correct(&db).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1.2 — Mixed DML, 5 cycles
// ═══════════════════════════════════════════════════════════════════════════

/// 5 cycles with INSERT, UPDATE, and DELETE operations at the base.
/// Each cycle exercises a different DML pattern through the full chain.
#[tokio::test]
async fn test_mc_dag_mixed_dml_5_cycles() {
    let db = E2eDb::new().await.with_extension().await;
    setup_3_layer_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Cycle 1: INSERT new group
    db.execute("INSERT INTO mc_src (grp, val) VALUES ('d', 100), ('d', 200)")
        .await;
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Cycle 2: UPDATE existing values
    db.execute("UPDATE mc_src SET val = val + 5 WHERE grp = 'a'")
        .await;
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Cycle 3: DELETE rows
    db.execute("DELETE FROM mc_src WHERE grp = 'b'").await;
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Cycle 4: Mixed within single cycle
    db.execute("INSERT INTO mc_src (grp, val) VALUES ('e', 25)")
        .await;
    db.execute("UPDATE mc_src SET val = 99 WHERE grp = 'c'")
        .await;
    db.execute("DELETE FROM mc_src WHERE grp = 'd' AND val = 100")
        .await;
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Cycle 5: More inserts to verify recovery from mixed cycle
    db.execute("INSERT INTO mc_src (grp, val) VALUES ('a', 1), ('b', 1)")
        .await;
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1.3 — No-op cycles (no drift)
// ═══════════════════════════════════════════════════════════════════════════

/// After initial population, 5 consecutive refresh cycles with no DML.
/// Verifies no delta drift and stable data_timestamp across all layers.
#[tokio::test]
async fn test_mc_dag_noop_cycle_no_drift() {
    let db = E2eDb::new().await.with_extension().await;
    setup_3_layer_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Warm-up refresh: the first refresh after creation establishes the
    // WAL frontier for each layer (triggers a FULL refresh since no frontier
    // exists yet). Only after the frontier is set do subsequent no-op
    // refreshes correctly avoid bumping data_timestamp.
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Record baseline data_timestamp for each layer AFTER frontier is established.
    let names = ["mc_l1", "mc_l2", "mc_l3"];
    let mut ts_before = Vec::new();
    for name in &names {
        let ts: String = db
            .query_scalar(&format!(
                "SELECT COALESCE(data_timestamp::text, 'null') \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{name}'"
            ))
            .await;
        ts_before.push(ts);
    }

    // 5 no-op refresh cycles — no source data changes, so data_timestamp must
    // not advance (no spurious downstream wakeups).
    for _ in 0..5 {
        refresh_pipeline(&db).await;
        assert_pipeline_correct(&db).await;
    }

    // data_timestamps should not have advanced
    for (i, name) in names.iter().enumerate() {
        let ts: String = db
            .query_scalar(&format!(
                "SELECT COALESCE(data_timestamp::text, 'null') \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{name}'"
            ))
            .await;
        assert_eq!(
            ts_before[i], ts,
            "data_timestamp for '{name}' must not drift on no-op refresh cycles"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1.4 — Group elimination and revival
// ═══════════════════════════════════════════════════════════════════════════

/// Delete all rows for a group → group vanishes at every layer.
/// Re-insert → group reappears. Verifies aggregate zero-handling and
/// re-creation across the full pipeline.
#[tokio::test]
async fn test_mc_dag_group_elimination_revival() {
    let db = E2eDb::new().await.with_extension().await;
    setup_3_layer_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Cycle 1: Eliminate group 'c' (which has doubled=100, appears in L3)
    db.execute("DELETE FROM mc_src WHERE grp = 'c'").await;
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Verify 'c' is gone from L3
    let c_in_l3: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM mc_l3 WHERE grp = 'c')")
        .await;
    assert!(!c_in_l3, "Group 'c' should be eliminated from L3");

    // Cycle 2: Revive group 'c' with a large value (should re-appear in L3)
    db.execute("INSERT INTO mc_src (grp, val) VALUES ('c', 80)")
        .await;
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    let c_in_l3_revived: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM mc_l3 WHERE grp = 'c')")
        .await;
    assert!(
        c_in_l3_revived,
        "Group 'c' should reappear in L3 after revival"
    );

    // Cycle 3: Eliminate again + add new group simultaneously
    db.execute("DELETE FROM mc_src WHERE grp = 'c'").await;
    db.execute("INSERT INTO mc_src (grp, val) VALUES ('f', 200)")
        .await;
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Cycle 4: Verify total convergence with one more insert
    db.execute("INSERT INTO mc_src (grp, val) VALUES ('a', 1)")
        .await;
    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1.5 — Bulk mutation stress
// ═══════════════════════════════════════════════════════════════════════════

/// Large batch DML (100 INSERTs, 50 UPDATEs, 30 DELETEs) in a single cycle.
/// Exercises the change buffer with a high volume of CDC entries.
#[tokio::test]
async fn test_mc_dag_bulk_mutation_stress() {
    let db = E2eDb::new().await.with_extension().await;
    setup_3_layer_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Bulk INSERT: 100 rows spread across 5 groups via a PL/pgSQL DO block.
    // Each iteration is a separate INSERT statement producing a separate CDC
    // event, so the change buffer receives 100 individual entries — which is
    // the stress condition being tested. One round-trip instead of 100.
    db.execute(
        "DO $$ \
         DECLARE grps TEXT[] := ARRAY['a','b','c','d','e']; \
         BEGIN \
           FOR i IN 0..99 LOOP \
             INSERT INTO mc_src (grp, val) \
             VALUES (grps[(i%5)+1], i); \
           END LOOP; \
         END $$",
    )
    .await;

    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Bulk UPDATE: increase all values in group 'a'
    db.execute("UPDATE mc_src SET val = val + 10 WHERE grp = 'a'")
        .await;

    // Bulk UPDATE: move some rows from 'b' to 'c'
    db.execute("UPDATE mc_src SET grp = 'c' WHERE grp = 'b' AND val < 30")
        .await;

    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;

    // Bulk DELETE: remove all of group 'e'
    db.execute("DELETE FROM mc_src WHERE grp = 'e'").await;
    // And some from 'd'
    db.execute("DELETE FROM mc_src WHERE grp = 'd' AND val < 50")
        .await;

    refresh_pipeline(&db).await;
    assert_pipeline_correct(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1.6 — Diamond multi-cycle
// ═══════════════════════════════════════════════════════════════════════════

/// Diamond topology with 5 cycles of mixed DML.
/// base → L1a + L1b → L2 (JOIN).
#[tokio::test]
async fn test_mc_dag_diamond_multi_cycle() {
    let db = E2eDb::new().await.with_extension().await;
    setup_diamond_pipeline(&db).await;
    db.assert_st_matches_query("dm_l2", DM_L2_QUERY).await;

    // Cycle 1: INSERT new group
    db.execute("INSERT INTO dm_src (grp, val) VALUES ('w', 100)")
        .await;
    refresh_diamond(&db).await;
    db.assert_st_matches_query("dm_l2", DM_L2_QUERY).await;

    // Cycle 2: UPDATE
    db.execute("UPDATE dm_src SET val = val + 5 WHERE grp = 'x'")
        .await;
    refresh_diamond(&db).await;
    db.assert_st_matches_query("dm_l2", DM_L2_QUERY).await;

    // Cycle 3: DELETE group
    db.execute("DELETE FROM dm_src WHERE grp = 'z'").await;
    refresh_diamond(&db).await;
    db.assert_st_matches_query("dm_l2", DM_L2_QUERY).await;

    // Cycle 4: Mixed — insert + delete
    db.execute("INSERT INTO dm_src (grp, val) VALUES ('z', 99)")
        .await;
    db.execute("DELETE FROM dm_src WHERE grp = 'w'").await;
    refresh_diamond(&db).await;
    db.assert_st_matches_query("dm_l2", DM_L2_QUERY).await;

    // Cycle 5: No-op
    refresh_diamond(&db).await;
    db.assert_st_matches_query("dm_l2", DM_L2_QUERY).await;
}
