//! CIRC-IMM: Circular Dependencies + IMMEDIATE Mode Hardening Tests
//!
//! Tests the interaction between complex dependency topologies (diamond deps,
//! near-circular patterns) and IMMEDIATE mode. Verifies that:
//!
//! 1. Diamond dependencies with IMMEDIATE triggers on both branches work correctly.
//! 2. Near-circular topologies (A→B→C→D where D also reads A's source) are handled.
//! 3. Lock ordering under concurrent DML on multiple sources in diamond topologies
//!    does not deadlock.
//! 4. Mixed IMMEDIATE/DIFFERENTIAL in diamond topologies produces correct results.
//!
//! Prerequisites: `just build-e2e-image`

mod e2e;

use e2e::E2eDb;

// ── Helpers ──────────────────────────────────────────────────────────────

/// Create an IMMEDIATE-mode stream table (schedule = NULL).
async fn create_immediate_st(db: &E2eDb, name: &str, query: &str) {
    let sql = format!(
        "SELECT pgtrickle.create_stream_table('{name}', $${query}$$, \
         NULL, 'IMMEDIATE')"
    );
    db.execute(&sql).await;
}

async fn test_db() -> E2eDb {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1 — Diamond with IMMEDIATE on both branches
// ═══════════════════════════════════════════════════════════════════════════

/// Diamond: base → {IMMED_B, IMMED_C} → DIFF_D (joins B and C).
/// INSERT into base: both B and C auto-refresh via IMMEDIATE triggers.
/// Manual refresh of D should pick up both branches' changes correctly.
#[tokio::test]
async fn test_diamond_immediate_both_branches_insert() {
    let db = test_db().await;

    db.execute(
        "CREATE TABLE circ_diam_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO circ_diam_src VALUES (1, 100), (2, 200)")
        .await;

    // Branch B: IMMEDIATE, adds 10
    create_immediate_st(
        &db,
        "circ_diam_b",
        "SELECT id, val + 10 AS score_b FROM circ_diam_src",
    )
    .await;

    // Branch C: IMMEDIATE, subtracts 10
    create_immediate_st(
        &db,
        "circ_diam_c",
        "SELECT id, val - 10 AS score_c FROM circ_diam_src",
    )
    .await;

    // Diamond tip D: DIFFERENTIAL, joins B and C
    db.create_st(
        "circ_diam_d",
        "SELECT b.id, b.score_b, c.score_c FROM circ_diam_b b JOIN circ_diam_c c ON b.id = c.id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial state
    assert_eq!(db.count("public.circ_diam_b").await, 2);
    assert_eq!(db.count("public.circ_diam_c").await, 2);
    assert_eq!(db.count("public.circ_diam_d").await, 2);

    // INSERT into base — both B and C should auto-refresh via IMMEDIATE
    db.execute("INSERT INTO circ_diam_src VALUES (3, 300)")
        .await;

    // B and C should have 3 rows (IMMEDIATE triggered)
    assert_eq!(
        db.count("public.circ_diam_b").await,
        3,
        "B should auto-refresh via IMMEDIATE trigger"
    );
    assert_eq!(
        db.count("public.circ_diam_c").await,
        3,
        "C should auto-refresh via IMMEDIATE trigger"
    );

    // Refresh D — should pick up both branches
    db.refresh_st_with_retry("circ_diam_d").await;

    assert_eq!(
        db.count("public.circ_diam_d").await,
        3,
        "D should have 3 rows after refreshing with both branches updated"
    );

    // Verify correct values at D
    db.assert_st_matches_query(
        "circ_diam_d",
        "SELECT b.id, b.score_b, c.score_c \
         FROM circ_diam_b b JOIN circ_diam_c c ON b.id = c.id",
    )
    .await;
}

/// Diamond with IMMEDIATE branches: UPDATE propagates correctly.
#[tokio::test]
async fn test_diamond_immediate_both_branches_update() {
    let db = test_db().await;

    db.execute(
        "CREATE TABLE circ_diamu_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO circ_diamu_src VALUES (1, 100), (2, 200)")
        .await;

    create_immediate_st(
        &db,
        "circ_diamu_b",
        "SELECT id, val * 2 AS doubled FROM circ_diamu_src",
    )
    .await;

    create_immediate_st(
        &db,
        "circ_diamu_c",
        "SELECT id, val + 50 AS shifted FROM circ_diamu_src",
    )
    .await;

    db.create_st(
        "circ_diamu_d",
        "SELECT b.id, b.doubled, c.shifted \
         FROM circ_diamu_b b JOIN circ_diamu_c c ON b.id = c.id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Update base
    db.execute("UPDATE circ_diamu_src SET val = 500 WHERE id = 1")
        .await;

    // Both branches auto-refresh (IMMEDIATE)
    let doubled: i32 = db
        .query_scalar("SELECT doubled FROM circ_diamu_b WHERE id = 1")
        .await;
    assert_eq!(doubled, 1000, "B: 500*2=1000");

    let shifted: i32 = db
        .query_scalar("SELECT shifted FROM circ_diamu_c WHERE id = 1")
        .await;
    assert_eq!(shifted, 550, "C: 500+50=550");

    // Refresh D
    db.refresh_st_with_retry("circ_diamu_d").await;

    db.assert_st_matches_query(
        "circ_diamu_d",
        "SELECT b.id, b.doubled, c.shifted \
         FROM circ_diamu_b b JOIN circ_diamu_c c ON b.id = c.id",
    )
    .await;
}

/// Diamond with IMMEDIATE branches: DELETE removes rows through all layers.
#[tokio::test]
async fn test_diamond_immediate_both_branches_delete() {
    let db = test_db().await;

    db.execute(
        "CREATE TABLE circ_diamd_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO circ_diamd_src VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    create_immediate_st(
        &db,
        "circ_diamd_b",
        "SELECT id, val FROM circ_diamd_src WHERE val > 5",
    )
    .await;

    create_immediate_st(
        &db,
        "circ_diamd_c",
        "SELECT id, val FROM circ_diamd_src WHERE val < 35",
    )
    .await;

    db.create_st(
        "circ_diamd_d",
        "SELECT b.id, b.val \
         FROM circ_diamd_b b JOIN circ_diamd_c c ON b.id = c.id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.circ_diamd_d").await, 3);

    // Delete id=2 from base — both branches lose it via IMMEDIATE
    db.execute("DELETE FROM circ_diamd_src WHERE id = 2").await;

    assert_eq!(db.count("public.circ_diamd_b").await, 2);
    assert_eq!(db.count("public.circ_diamd_c").await, 2);

    db.refresh_st_with_retry("circ_diamd_d").await;

    assert_eq!(
        db.count("public.circ_diamd_d").await,
        2,
        "D should have 2 rows after deleting id=2"
    );

    db.assert_st_matches_query(
        "circ_diamd_d",
        "SELECT b.id, b.val \
         FROM circ_diamd_b b JOIN circ_diamd_c c ON b.id = c.id",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2 — Near-circular topology
// ═══════════════════════════════════════════════════════════════════════════

/// Near-circular: base_A → ST_B → ST_C, and ST_D reads both base_A and ST_C.
/// This is NOT a true cycle (D doesn't feed back), but D depends on the same
/// source as B through two different paths with differing depths.
/// Validates that topological refresh order produces correct results.
#[tokio::test]
async fn test_near_circular_topology_insert() {
    let db = test_db().await;

    db.execute(
        "CREATE TABLE nc_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO nc_src VALUES (1, 10), (2, 20)")
        .await;

    // B reads from base
    db.create_st(
        "nc_b",
        "SELECT id, val * 2 AS doubled FROM nc_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // C reads from B (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'nc_c',
            $$SELECT id, doubled + 5 AS adjusted FROM nc_b$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // D reads from BOTH base_A (direct) and C (through B chain)
    // This creates two paths from nc_src to nc_d: direct and via B→C
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'nc_d',
            $$SELECT s.id, s.val AS direct_val, c.adjusted AS chain_val
              FROM nc_src s JOIN nc_c c ON s.id = c.id$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Initial: id=1 → direct_val=10, chain_val=10*2+5=25
    //          id=2 → direct_val=20, chain_val=20*2+5=45
    assert_eq!(db.count("public.nc_d").await, 2);

    // Insert into base
    db.execute("INSERT INTO nc_src VALUES (3, 30)").await;

    // Refresh in topological order: B → C → D
    db.refresh_st_with_retry("nc_b").await;
    db.refresh_st_with_retry("nc_c").await;
    db.refresh_st_with_retry("nc_d").await;

    assert_eq!(db.count("public.nc_d").await, 3);

    // Verify id=3: direct_val=30, chain_val=30*2+5=65
    let direct: i32 = db
        .query_scalar("SELECT direct_val FROM nc_d WHERE id = 3")
        .await;
    assert_eq!(direct, 30);

    let chain: i32 = db
        .query_scalar("SELECT chain_val FROM nc_d WHERE id = 3")
        .await;
    assert_eq!(chain, 65, "30*2+5=65");

    db.assert_st_matches_query(
        "nc_d",
        "SELECT s.id, s.val AS direct_val, c.adjusted AS chain_val \
         FROM nc_src s JOIN nc_c c ON s.id = c.id",
    )
    .await;
}

/// Near-circular: UPDATE at the base propagates through both direct and
/// chain paths to the convergence point.
#[tokio::test]
async fn test_near_circular_topology_update() {
    let db = test_db().await;

    db.execute(
        "CREATE TABLE ncu_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO ncu_src VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "ncu_b",
        "SELECT id, val * 2 AS doubled FROM ncu_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ncu_c',
            $$SELECT id, doubled + 5 AS adjusted FROM ncu_b$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ncu_d',
            $$SELECT s.id, s.val AS direct_val, c.adjusted AS chain_val
              FROM ncu_src s JOIN ncu_c c ON s.id = c.id$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Update id=1: val 10 → 50
    db.execute("UPDATE ncu_src SET val = 50 WHERE id = 1").await;

    db.refresh_st_with_retry("ncu_b").await;
    db.refresh_st_with_retry("ncu_c").await;
    db.refresh_st_with_retry("ncu_d").await;

    // direct_val=50, chain_val=50*2+5=105
    let direct: i32 = db
        .query_scalar("SELECT direct_val FROM ncu_d WHERE id = 1")
        .await;
    assert_eq!(direct, 50);

    let chain: i32 = db
        .query_scalar("SELECT chain_val FROM ncu_d WHERE id = 1")
        .await;
    assert_eq!(chain, 105, "50*2+5=105");

    db.assert_st_matches_query(
        "ncu_d",
        "SELECT s.id, s.val AS direct_val, c.adjusted AS chain_val \
         FROM ncu_src s JOIN ncu_c c ON s.id = c.id",
    )
    .await;
}

/// Near-circular: DELETE at the base removes rows from both paths.
#[tokio::test]
async fn test_near_circular_topology_delete() {
    let db = test_db().await;

    db.execute(
        "CREATE TABLE ncd_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO ncd_src VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    db.create_st(
        "ncd_b",
        "SELECT id, val * 2 AS doubled FROM ncd_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ncd_c',
            $$SELECT id, doubled + 5 AS adjusted FROM ncd_b$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ncd_d',
            $$SELECT s.id, s.val AS direct_val, c.adjusted AS chain_val
              FROM ncd_src s JOIN ncd_c c ON s.id = c.id$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    assert_eq!(db.count("public.ncd_d").await, 3);

    // Delete id=2
    db.execute("DELETE FROM ncd_src WHERE id = 2").await;

    db.refresh_st_with_retry("ncd_b").await;
    db.refresh_st_with_retry("ncd_c").await;
    db.refresh_st_with_retry("ncd_d").await;

    assert_eq!(
        db.count("public.ncd_d").await,
        2,
        "id=2 gone from both paths"
    );

    db.assert_st_matches_query(
        "ncd_d",
        "SELECT s.id, s.val AS direct_val, c.adjusted AS chain_val \
         FROM ncd_src s JOIN ncd_c c ON s.id = c.id",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3 — Near-circular with IMMEDIATE branches
// ═══════════════════════════════════════════════════════════════════════════

/// Near-circular topology where both intermediate STs are IMMEDIATE:
/// base → IMMED_B → IMMED_C, and DIFF_D reads base + C.
/// Verifies IMMEDIATE auto-refresh propagates through the chain before
/// the DIFFERENTIAL tip is manually refreshed.
#[tokio::test]
async fn test_near_circular_immediate_branches() {
    let db = test_db().await;

    db.execute(
        "CREATE TABLE nci_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO nci_src VALUES (1, 10), (2, 20)")
        .await;

    // B: IMMEDIATE
    create_immediate_st(&db, "nci_b", "SELECT id, val * 2 AS doubled FROM nci_src").await;

    // C: IMMEDIATE on B
    create_immediate_st(
        &db,
        "nci_c",
        "SELECT id, doubled + 5 AS adjusted FROM nci_b",
    )
    .await;

    // D: DIFFERENTIAL, reads base + C
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'nci_d',
            $$SELECT s.id, s.val AS direct_val, c.adjusted AS chain_val
              FROM nci_src s JOIN nci_c c ON s.id = c.id$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // INSERT into base — B should auto-refresh, then C may auto-refresh
    db.execute("INSERT INTO nci_src VALUES (3, 30)").await;

    // B should have 3 rows (direct IMMEDIATE trigger)
    assert_eq!(
        db.count("public.nci_b").await,
        3,
        "B auto-refreshes via IMMEDIATE"
    );

    // C may or may not auto-cascade — do explicit refresh to ensure correctness
    db.refresh_st("nci_c").await;
    assert_eq!(db.count("public.nci_c").await, 3);

    // Refresh D
    db.refresh_st_with_retry("nci_d").await;
    assert_eq!(db.count("public.nci_d").await, 3);

    // Verify id=3: direct_val=30, chain_val=30*2+5=65
    let chain: i32 = db
        .query_scalar("SELECT chain_val FROM nci_d WHERE id = 3")
        .await;
    assert_eq!(chain, 65);

    db.assert_st_matches_query(
        "nci_d",
        "SELECT s.id, s.val AS direct_val, c.adjusted AS chain_val \
         FROM nci_src s JOIN nci_c c ON s.id = c.id",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4 — Concurrent DML on diamond with IMMEDIATE branches
// ═══════════════════════════════════════════════════════════════════════════

/// Diamond with IMMEDIATE branches: rapid sequential DML on the same source
/// table exercises the advisory lock acquire/release path. Verifies no
/// deadlocks occur and final state is correct.
#[tokio::test]
async fn test_diamond_immediate_rapid_sequential_dml() {
    let db = test_db().await;

    db.execute(
        "CREATE TABLE circ_rapid_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO circ_rapid_src VALUES (1, 10)")
        .await;

    create_immediate_st(
        &db,
        "circ_rapid_b",
        "SELECT id, val + 1 AS b_val FROM circ_rapid_src",
    )
    .await;

    create_immediate_st(
        &db,
        "circ_rapid_c",
        "SELECT id, val + 2 AS c_val FROM circ_rapid_src",
    )
    .await;

    db.create_st(
        "circ_rapid_d",
        "SELECT b.id, b.b_val, c.c_val \
         FROM circ_rapid_b b JOIN circ_rapid_c c ON b.id = c.id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Rapid sequential DML — each fires IMMEDIATE on both branches
    for i in 2..=10 {
        db.execute(&format!("INSERT INTO circ_rapid_src VALUES ({i}, {i}0)"))
            .await;
    }

    // Both branches should have all 10 rows via IMMEDIATE
    assert_eq!(db.count("public.circ_rapid_b").await, 10);
    assert_eq!(db.count("public.circ_rapid_c").await, 10);

    // Refresh D
    db.refresh_st_with_retry("circ_rapid_d").await;
    assert_eq!(db.count("public.circ_rapid_d").await, 10);

    db.assert_st_matches_query(
        "circ_rapid_d",
        "SELECT b.id, b.b_val, c.c_val \
         FROM circ_rapid_b b JOIN circ_rapid_c c ON b.id = c.id",
    )
    .await;
}

/// Diamond with IMMEDIATE branches: mixed INSERT/UPDATE/DELETE sequence
/// verifies no stale rows or phantom rows after diamond tip refresh.
#[tokio::test]
async fn test_diamond_immediate_mixed_dml_sequence() {
    let db = test_db().await;

    db.execute(
        "CREATE TABLE circ_mix_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO circ_mix_src VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .await;

    create_immediate_st(
        &db,
        "circ_mix_b",
        "SELECT id, val AS b_val FROM circ_mix_src",
    )
    .await;

    create_immediate_st(
        &db,
        "circ_mix_c",
        "SELECT id, val * 10 AS c_val FROM circ_mix_src",
    )
    .await;

    db.create_st(
        "circ_mix_d",
        "SELECT b.id, b.b_val, c.c_val \
         FROM circ_mix_b b JOIN circ_mix_c c ON b.id = c.id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Mixed DML sequence
    db.execute("INSERT INTO circ_mix_src VALUES (6, 60)").await;
    db.execute("UPDATE circ_mix_src SET val = 999 WHERE id = 2")
        .await;
    db.execute("DELETE FROM circ_mix_src WHERE id = 4").await;

    // After all DML: ids {1,2,3,5,6}, val for id=2 is 999
    assert_eq!(db.count("public.circ_mix_b").await, 5);
    assert_eq!(db.count("public.circ_mix_c").await, 5);

    db.refresh_st_with_retry("circ_mix_d").await;

    assert_eq!(
        db.count("public.circ_mix_d").await,
        5,
        "D should have 5 rows after mixed DML"
    );

    // Verify specific values
    let b_val_2: i32 = db
        .query_scalar("SELECT b_val FROM circ_mix_d WHERE id = 2")
        .await;
    assert_eq!(b_val_2, 999, "id=2 should have updated val=999");

    let c_val_2: i32 = db
        .query_scalar("SELECT c_val FROM circ_mix_d WHERE id = 2")
        .await;
    assert_eq!(c_val_2, 9990, "id=2 C branch: 999*10=9990");

    let id4_exists: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM circ_mix_d WHERE id = 4)")
        .await;
    assert!(!id4_exists, "id=4 should be gone after DELETE");

    db.assert_st_matches_query(
        "circ_mix_d",
        "SELECT b.id, b.b_val, c.c_val \
         FROM circ_mix_b b JOIN circ_mix_c c ON b.id = c.id",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5 — Fan-in topology: multiple base tables feeding through IMMEDIATE
// ═══════════════════════════════════════════════════════════════════════════

/// Fan-in: two separate base tables → two IMMEDIATE STs → DIFF tip joining them.
/// DML on both bases fires separate IMMEDIATE triggers; tip refresh must
/// see both sides updated. Exercises the multi-source advisory lock path.
#[tokio::test]
async fn test_fan_in_immediate_multi_source() {
    let db = test_db().await;

    db.execute("CREATE TABLE fan_src_a (id INT PRIMARY KEY, val_a INT NOT NULL)")
        .await;
    db.execute("INSERT INTO fan_src_a VALUES (1, 10), (2, 20)")
        .await;

    db.execute("CREATE TABLE fan_src_b (id INT PRIMARY KEY, val_b INT NOT NULL)")
        .await;
    db.execute("INSERT INTO fan_src_b VALUES (1, 100), (2, 200)")
        .await;

    // IMMEDIATE on source A
    create_immediate_st(&db, "fan_imm_a", "SELECT id, val_a FROM fan_src_a").await;

    // IMMEDIATE on source B
    create_immediate_st(&db, "fan_imm_b", "SELECT id, val_b FROM fan_src_b").await;

    // Tip: DIFFERENTIAL joining A and B
    db.create_st(
        "fan_tip",
        "SELECT a.id, a.val_a, b.val_b \
         FROM fan_imm_a a JOIN fan_imm_b b ON a.id = b.id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.fan_tip").await, 2);

    // DML on both sources
    db.execute("INSERT INTO fan_src_a VALUES (3, 30)").await;
    db.execute("INSERT INTO fan_src_b VALUES (3, 300)").await;

    // Both IMMEDIATE STs auto-refreshed
    assert_eq!(db.count("public.fan_imm_a").await, 3);
    assert_eq!(db.count("public.fan_imm_b").await, 3);

    // Refresh tip
    db.refresh_st_with_retry("fan_tip").await;
    assert_eq!(db.count("public.fan_tip").await, 3);

    let val_a: i32 = db
        .query_scalar("SELECT val_a FROM fan_tip WHERE id = 3")
        .await;
    assert_eq!(val_a, 30);

    let val_b: i32 = db
        .query_scalar("SELECT val_b FROM fan_tip WHERE id = 3")
        .await;
    assert_eq!(val_b, 300);

    db.assert_st_matches_query(
        "fan_tip",
        "SELECT a.id, a.val_a, b.val_b \
         FROM fan_imm_a a JOIN fan_imm_b b ON a.id = b.id",
    )
    .await;
}
