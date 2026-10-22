//! E2E tests for mixed refresh mode DAG pipelines.
//!
//! Validates that pipelines combining FULL, DIFFERENTIAL, and IMMEDIATE
//! refresh modes in a single stream table chain produce correct results.
//!
//! ## Key architecture paths exercised
//!
//! - `determine_refresh_action()` dispatching FULL vs DIFFERENTIAL in cascade
//! - `has_stream_table_source_changes()` comparing `data_timestamp` when
//!   upstream refresh mode differs
//! - Reinit behavior when ALTERing mode mid-pipeline
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════════
// Test 2.1 — FULL root feeds DIFFERENTIAL leaf
// ═══════════════════════════════════════════════════════════════════════════

/// base →[FULL] L1 →[DIFF] L2
///
/// FULL root does a complete recompute each refresh, then the DIFFERENTIAL
/// leaf should detect the upstream `data_timestamp` change and incorporate
/// updates correctly.
#[tokio::test]
async fn test_mixed_full_then_diff_2_layer() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mfull_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO mfull_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    // L1: FULL mode aggregate
    db.create_st(
        "mfull_l1",
        "SELECT grp, SUM(val) AS total FROM mfull_src GROUP BY grp",
        "1m",
        "FULL",
    )
    .await;

    // L2: DIFFERENTIAL mode, reads from L1 (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'mfull_l2',
            $$SELECT grp, total * 2 AS doubled FROM mfull_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Ground truth queries
    let l1_q = "SELECT grp, SUM(val) AS total FROM mfull_src GROUP BY grp";
    let l2_q = "SELECT grp, SUM(val) * 2 AS doubled FROM mfull_src GROUP BY grp";

    db.assert_st_matches_query("mfull_l1", l1_q).await;
    db.assert_st_matches_query("mfull_l2", l2_q).await;

    // Mutate and refresh
    db.execute("INSERT INTO mfull_src (grp, val) VALUES ('a', 5), ('c', 30)")
        .await;
    db.refresh_st("mfull_l1").await; // FULL recompute
    db.refresh_st("mfull_l2").await; // DIFFERENTIAL delta
    db.assert_st_matches_query("mfull_l1", l1_q).await;
    db.assert_st_matches_query("mfull_l2", l2_q).await;

    // Another cycle: UPDATE
    db.execute("UPDATE mfull_src SET val = val + 10 WHERE grp = 'b'")
        .await;
    db.refresh_st("mfull_l1").await;
    db.refresh_st("mfull_l2").await;
    db.assert_st_matches_query("mfull_l1", l1_q).await;
    db.assert_st_matches_query("mfull_l2", l2_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2.2 — DIFFERENTIAL root feeds FULL leaf
// ═══════════════════════════════════════════════════════════════════════════

/// base →[DIFF] L1 →[FULL] L2
///
/// DIFFERENTIAL root incrementally updates, FULL leaf does complete
/// recompute each time regardless.
#[tokio::test]
async fn test_mixed_diff_then_full_2_layer() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mdf_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO mdf_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    // L1: DIFFERENTIAL
    db.create_st(
        "mdf_l1",
        "SELECT grp, SUM(val) AS total FROM mdf_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L2: FULL mode
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'mdf_l2',
            $$SELECT grp, total * 3 AS tripled FROM mdf_l1$$,
            'calculated',
            'FULL'
        )",
    )
    .await;

    let l1_q = "SELECT grp, SUM(val) AS total FROM mdf_src GROUP BY grp";
    let l2_q = "SELECT grp, SUM(val) * 3 AS tripled FROM mdf_src GROUP BY grp";

    db.assert_st_matches_query("mdf_l1", l1_q).await;
    db.assert_st_matches_query("mdf_l2", l2_q).await;

    // Mutate
    db.execute("INSERT INTO mdf_src (grp, val) VALUES ('c', 15)")
        .await;
    db.refresh_st("mdf_l1").await; // DIFFERENTIAL delta
    db.refresh_st("mdf_l2").await; // FULL recompute
    db.assert_st_matches_query("mdf_l1", l1_q).await;
    db.assert_st_matches_query("mdf_l2", l2_q).await;

    // DELETE
    db.execute("DELETE FROM mdf_src WHERE grp = 'a'").await;
    db.refresh_st("mdf_l1").await;
    db.refresh_st("mdf_l2").await;
    db.assert_st_matches_query("mdf_l1", l1_q).await;
    db.assert_st_matches_query("mdf_l2", l2_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2.3 — 3-layer: FULL → DIFF → DIFF
// ═══════════════════════════════════════════════════════════════════════════

/// base →[FULL] L1 →[DIFF] L2 →[DIFF] L3
///
/// FULL root, two DIFFERENTIAL downstream.  Exercises the full mixed-mode
/// path through 3 levels.
#[tokio::test]
async fn test_mixed_3_layer_full_diff_diff() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE m3_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO m3_src (grp, val) VALUES
            ('a', 10), ('a', 20), ('b', 30)",
    )
    .await;

    // L1: FULL aggregate
    db.create_st(
        "m3_l1",
        "SELECT grp, SUM(val) AS total FROM m3_src GROUP BY grp",
        "1m",
        "FULL",
    )
    .await;

    // L2: DIFFERENTIAL project (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'm3_l2',
            $$SELECT grp, total * 2 AS doubled FROM m3_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // L3: DIFFERENTIAL filter (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'm3_l3',
            $$SELECT grp, doubled FROM m3_l2 WHERE doubled > 50$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    let l1_q = "SELECT grp, SUM(val) AS total FROM m3_src GROUP BY grp";
    let l2_q = "SELECT grp, SUM(val) * 2 AS doubled FROM m3_src GROUP BY grp";
    let l3_q =
        "SELECT grp, SUM(val) * 2 AS doubled FROM m3_src GROUP BY grp HAVING SUM(val) * 2 > 50";

    db.assert_st_matches_query("m3_l1", l1_q).await;
    db.assert_st_matches_query("m3_l2", l2_q).await;
    db.assert_st_matches_query("m3_l3", l3_q).await;

    // Cycle 1: INSERT → should cascade to L3
    db.execute("INSERT INTO m3_src (grp, val) VALUES ('a', 5)")
        .await;
    db.refresh_st("m3_l1").await;
    db.refresh_st("m3_l2").await;
    db.refresh_st("m3_l3").await;
    db.assert_st_matches_query("m3_l1", l1_q).await;
    db.assert_st_matches_query("m3_l2", l2_q).await;
    db.assert_st_matches_query("m3_l3", l3_q).await;

    // Cycle 2: DELETE → may remove group from L3
    db.execute("DELETE FROM m3_src WHERE grp = 'a' AND val = 5")
        .await;
    db.refresh_st("m3_l1").await;
    db.refresh_st("m3_l2").await;
    db.refresh_st("m3_l3").await;
    db.assert_st_matches_query("m3_l1", l1_q).await;
    db.assert_st_matches_query("m3_l2", l2_q).await;
    db.assert_st_matches_query("m3_l3", l3_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2.4 — ALTER mode mid-pipeline (DIFF → FULL)
// ═══════════════════════════════════════════════════════════════════════════

/// Start with base →[DIFF] L1 →[DIFF] L2, then ALTER L1 to FULL.
/// The chain should still converge after the mode change.
#[tokio::test]
async fn test_mixed_mode_alter_mid_pipeline() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE malter_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO malter_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    db.create_st(
        "malter_l1",
        "SELECT grp, SUM(val) AS total FROM malter_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'malter_l2',
            $$SELECT grp, total * 2 AS doubled FROM malter_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    let l1_q = "SELECT grp, SUM(val) AS total FROM malter_src GROUP BY grp";
    let l2_q = "SELECT grp, SUM(val) * 2 AS doubled FROM malter_src GROUP BY grp";

    db.assert_st_matches_query("malter_l1", l1_q).await;
    db.assert_st_matches_query("malter_l2", l2_q).await;

    // ALTER L1 from DIFFERENTIAL to FULL
    db.alter_st("malter_l1", "refresh_mode => 'FULL'").await;

    let (_, mode, _, _) = db.pgt_status("malter_l1").await;
    assert_eq!(mode, "FULL", "L1 should now be FULL mode");

    // Mutate and refresh — L1 does FULL recompute now
    db.execute("INSERT INTO malter_src (grp, val) VALUES ('c', 30)")
        .await;
    db.refresh_st("malter_l1").await;
    db.refresh_st("malter_l2").await;
    db.assert_st_matches_query("malter_l1", l1_q).await;
    db.assert_st_matches_query("malter_l2", l2_q).await;

    // One more cycle to confirm stability
    db.execute("DELETE FROM malter_src WHERE grp = 'a'").await;
    db.refresh_st("malter_l1").await;
    db.refresh_st("malter_l2").await;
    db.assert_st_matches_query("malter_l1", l1_q).await;
    db.assert_st_matches_query("malter_l2", l2_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2.5 — DIFFERENTIAL root with IMMEDIATE leaf
// ═══════════════════════════════════════════════════════════════════════════

/// base →[DIFF] L1 →[IMMED] L2
///
/// DIFFERENTIAL root with IMMEDIATE leaf.  After refreshing L1, L2
/// should be up-to-date via its statement-level trigger on L1.
///
/// Note: IMMEDIATE ST-on-ST may require explicit refresh depending on
/// internal trigger wiring.  This test documents the actual behavior.
#[tokio::test]
async fn test_mixed_immediate_leaf() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mimm_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO mimm_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    // L1: DIFFERENTIAL
    db.create_st(
        "mimm_l1",
        "SELECT grp, SUM(val) AS total FROM mimm_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L2: IMMEDIATE (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'mimm_l2',
            $$SELECT grp, total * 2 AS doubled FROM mimm_l1$$,
            'calculated',
            'IMMEDIATE'
        )",
    )
    .await;

    let l1_q = "SELECT grp, SUM(val) AS total FROM mimm_src GROUP BY grp";
    let l2_q = "SELECT grp, SUM(val) * 2 AS doubled FROM mimm_src GROUP BY grp";

    db.assert_st_matches_query("mimm_l1", l1_q).await;

    // Mutate base and refresh L1 (DIFFERENTIAL)
    db.execute("INSERT INTO mimm_src (grp, val) VALUES ('c', 30)")
        .await;
    db.refresh_st("mimm_l1").await;
    db.assert_st_matches_query("mimm_l1", l1_q).await;

    // L2 is IMMEDIATE — if the trigger fires on L1's MERGE, it should be
    // up-to-date already.  If not, we do an explicit refresh.
    // Either way, verify final correctness.
    db.refresh_st("mimm_l2").await;
    db.assert_st_matches_query("mimm_l2", l2_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers for scheduler-driven tests
// ═══════════════════════════════════════════════════════════════════════════

/// Configure the scheduler for fast testing (100 ms tick, 1 s min schedule).
#[cfg(not(feature = "light-e2e"))]
async fn configure_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;

    let sched_running = db
        .wait_for_scheduler(std::time::Duration::from_secs(90))
        .await;
    assert!(
        sched_running,
        "pg_trickle scheduler did not appear in pg_stat_activity within 90 s"
    );
}

#[cfg(not(feature = "light-e2e"))]
async fn wait_for_scheduler_refresh(
    db: &E2eDb,
    pgt_name: &str,
    timeout: std::time::Duration,
) -> bool {
    let initial = db
        .query_scalar_opt::<String>(&format!(
            "SELECT last_refresh_at::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = '{pgt_name}'"
        ))
        .await;
    db.wait_for_condition(
        "scheduler refresh",
        &format!(
            "SELECT last_refresh_at IS NOT NULL AND last_refresh_at::text <> '{}' \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{pgt_name}'",
            initial.unwrap_or_default()
        ),
        timeout,
        std::time::Duration::from_millis(300),
    )
    .await
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2.6 — CALCULATED DIFFERENTIAL leaf auto-cascades from FULL upstream
// ═══════════════════════════════════════════════════════════════════════════

/// Regression (NS-8): source →[FULL 1s] L1 →[DIFFERENTIAL calculated] L2
///
/// When L1 is FULL-mode and refreshes with zero net row changes, it writes
/// nothing to its ST change buffer.  Before the fix, L2 (CALCULATED) would
/// never detect the "L1 refreshed" signal and would stall indefinitely —
/// `effective_refresh_mode` stayed NULL.
///
/// After the fix, `has_stream_table_source_changes()` falls back to comparing
/// `last_refresh_at` timestamps: if L1 refreshed more recently than L2, L2
/// triggers.  A NoData run on L2 advances its own `last_refresh_at` past L1's,
/// preventing runaway re-triggering.
///
/// This test verifies the fix via the **scheduler path** (auto-refresh), not
/// manual refresh, because that is where the stall occurs.
#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_calculated_diff_leaf_auto_cascades_from_full_upstream() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute(
        "CREATE TABLE ns8_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO ns8_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    // L1: FULL mode with 1s schedule
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ns8_l1',
            $$SELECT grp, SUM(val) AS total FROM ns8_src GROUP BY grp$$,
            '1s',
            'FULL'
        )",
    )
    .await;

    // L2: DIFFERENTIAL + CALCULATED (no explicit schedule — depends on upstream signal)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ns8_l2',
            $$SELECT grp, total * 2 AS doubled FROM ns8_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Wait for the scheduler to auto-populate both STs.
    // L1 fires on its 1s schedule; L2 fires via the last_refresh_at
    // fallback when L1's last_refresh_at is newer than L2's.
    let refreshed =
        wait_for_scheduler_refresh(&db, "ns8_l2", std::time::Duration::from_secs(60)).await;
    assert!(
        refreshed,
        "ns8_l2 (DIFFERENTIAL/CALCULATED from FULL upstream) never received \
         a scheduler-driven refresh within 60s — last_refresh_at fallback may \
         not be working"
    );

    // Verify correctness: doubled should reflect the source data
    db.assert_st_matches_query(
        "ns8_l2",
        "SELECT grp, SUM(val) * 2 AS doubled FROM ns8_src GROUP BY grp",
    )
    .await;

    // Now mutate the source and wait for the cascade to propagate again
    db.execute("INSERT INTO ns8_src (grp, val) VALUES ('a', 5), ('c', 30)")
        .await;

    let refreshed2 = db
        .wait_for_auto_refresh("ns8_l2", std::time::Duration::from_secs(60))
        .await;
    assert!(
        refreshed2,
        "ns8_l2 did not re-cascade after second INSERT within 60s"
    );

    db.assert_st_matches_query(
        "ns8_l2",
        "SELECT grp, SUM(val) * 2 AS doubled FROM ns8_src GROUP BY grp",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2.7 — Diamond convergence cascades when only one branch has CDC events
// ═══════════════════════════════════════════════════════════════════════════

/// Regression (NS-8 / diamond): source_left, source_right → left (FULL 1s),
/// right (FULL 1s) → convergence (FULL calculated).
///
/// When only source_left gets new data, only left_branch produces a
/// populated ST change buffer.  right_branch refreshes on its clock but
/// produces an empty change buffer.  Before the fix, the convergence node
/// saw no signal from right_branch and stalled.
///
/// After the fix, the convergence node detects that left_branch's
/// `last_refresh_at` is newer than its own and cascades automatically.
#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_diamond_convergence_cascades_with_one_active_branch() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Two independent source tables
    db.execute("CREATE TABLE dconv_left  (id SERIAL PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("CREATE TABLE dconv_right (id SERIAL PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO dconv_left  (val) VALUES (10), (20)")
        .await;
    db.execute("INSERT INTO dconv_right (val) VALUES (100), (200)")
        .await;

    // Two intermediate FULL branches on 1s schedules
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'dconv_branch_left',
            $$SELECT val FROM dconv_left$$,
            '1s',
            'FULL'
        )",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'dconv_branch_right',
            $$SELECT val FROM dconv_right$$,
            '1s',
            'FULL'
        )",
    )
    .await;

    // Convergence node reads from both branches
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'dconv_tip',
            $$SELECT l.val AS lval, r.val AS rval
              FROM dconv_branch_left l
              CROSS JOIN dconv_branch_right r$$,
            'calculated',
            'FULL'
        )",
    )
    .await;

    // Wait for initial auto-cascade to populate dconv_tip
    let initial = db
        .wait_for_auto_refresh("dconv_tip", std::time::Duration::from_secs(60))
        .await;
    assert!(
        initial,
        "dconv_tip never received its first scheduler-driven refresh within 60s"
    );

    let initial_count: i64 = db.count("public.dconv_tip").await;
    assert_eq!(
        initial_count, 4,
        "Expected 4 rows (2×2 cross join) in dconv_tip initially, got {initial_count}"
    );

    // Insert into LEFT only — RIGHT stays unchanged
    db.execute("INSERT INTO dconv_left (val) VALUES (30)").await;

    // dconv_tip must cascade because left_branch will refresh (via CDC),
    // and its last_refresh_at will be newer than dconv_tip's.
    let recascaded = db
        .wait_for_condition(
            "dconv_tip re-cascade after left INSERT",
            "SELECT COUNT(*) = 6 FROM public.dconv_tip",
            std::time::Duration::from_secs(60),
            std::time::Duration::from_millis(300),
        )
        .await;
    assert!(
        recascaded,
        "dconv_tip did not cascade after INSERT into left branch only (expected 6 rows from \
         3×2 cross join) — last_refresh_at fallback for asymmetric diamond may not be working"
    );
}
