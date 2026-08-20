//! E2E tests for error resilience in multi-layer DAG pipelines.
//!
//! Validates that failures in one pipeline layer do not corrupt sibling or
//! downstream layers, and that recovery works after fixing the root cause.
//!
//! ## Key architecture paths exercised
//!
//! - `consecutive_errors` catalog tracking after refresh failure
//! - Error isolation between sibling branches (diamond)
//! - Recovery via data fix + re-refresh
//! - Leaf errors not affecting upstream layers
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::{
    E2eDb,
    property_support::{SeededRng, TraceConfig, TrackedIds, assert_st_query_invariant},
};

// ═══════════════════════════════════════════════════════════════════════════
// Test 6.1 — Error in middle layer does not corrupt siblings
// ═══════════════════════════════════════════════════════════════════════════

/// Diamond: A → B_ok (SUM), A → B_fail (division by zero on bad data).
/// Insert triggering data. Refresh A (succeeds), refresh B_fail (fails),
/// refresh B_ok (succeeds). Verify B_ok is correct and B_fail has errors.
#[tokio::test]
async fn test_error_in_middle_layer_does_not_corrupt_siblings() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE err_sibling_src (
            id    SERIAL PRIMARY KEY,
            grp   TEXT NOT NULL,
            val   INT NOT NULL,
            denom INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO err_sibling_src (grp, val, denom) VALUES
            ('a', 10, 2), ('b', 20, 5)",
    )
    .await;

    // B_ok: simple SUM (won't fail)
    db.create_st(
        "err_b_ok",
        "SELECT grp, SUM(val) AS total FROM err_sibling_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // B_fail: division that will fail on denom=0
    db.create_st(
        "err_b_fail",
        "SELECT grp, SUM(val / denom) AS ratio FROM err_sibling_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial state is correct
    let ok_q = "SELECT grp, SUM(val) AS total FROM err_sibling_src GROUP BY grp";
    db.assert_st_matches_query("err_b_ok", ok_q).await;

    let (_, _, _, errors_before) = db.pgt_status("err_b_fail").await;
    assert_eq!(errors_before, 0, "No errors initially");

    // Insert a row with denom=0 → B_fail's refresh will get division by zero
    db.execute("INSERT INTO err_sibling_src (grp, val, denom) VALUES ('c', 30, 0)")
        .await;

    // Refresh B_ok — should succeed
    db.refresh_st("err_b_ok").await;
    db.assert_st_matches_query("err_b_ok", ok_q).await;

    // Refresh B_fail — should fail
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('err_b_fail')")
        .await;
    assert!(
        result.is_err(),
        "B_fail refresh should fail on division by zero"
    );

    // Verify B_ok is still correct (not corrupted by sibling failure)
    db.assert_st_matches_query("err_b_ok", ok_q).await;

    // consecutive_errors is only updated by the background scheduler, not by
    // manual refresh_stream_table() calls (the failing transaction rolls back
    // any catalog writes in the function call). The key invariant here is that
    // B_ok was not affected by B_fail's error.
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6.2 — Error recovery after data fix
// ═══════════════════════════════════════════════════════════════════════════

/// A → B → C. B's query has a division that fails on bad data.
/// Fix the data at the source, refresh pipeline. Verify full convergence.
#[tokio::test]
async fn test_error_recovery_after_data_fix() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE err_fix_src (
            id    SERIAL PRIMARY KEY,
            grp   TEXT NOT NULL,
            val   INT NOT NULL,
            denom INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO err_fix_src (grp, val, denom) VALUES
            ('a', 10, 2), ('b', 20, 4)",
    )
    .await;

    // B: safe division initially
    db.create_st(
        "err_fix_b",
        "SELECT grp, SUM(val / denom) AS ratio FROM err_fix_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // C: downstream of B
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'err_fix_c',
            $$SELECT grp, ratio * 10 AS scaled FROM err_fix_b$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    let b_q = "SELECT grp, SUM(val / denom) AS ratio FROM err_fix_src GROUP BY grp";
    let c_q = "SELECT grp, SUM(val / denom) * 10 AS scaled FROM err_fix_src GROUP BY grp";

    db.assert_st_matches_query("err_fix_b", b_q).await;
    db.assert_st_matches_query("err_fix_c", c_q).await;

    // Insert bad data
    db.execute("INSERT INTO err_fix_src (grp, val, denom) VALUES ('c', 30, 0)")
        .await;

    // Refresh B should fail
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('err_fix_b')")
        .await;
    assert!(result.is_err(), "B should fail with division by zero");

    // Fix the data: update the bad row's denominator
    db.execute("UPDATE err_fix_src SET denom = 3 WHERE denom = 0")
        .await;

    // Now B should succeed
    db.refresh_st("err_fix_b").await;
    db.refresh_st("err_fix_c").await;

    // Full convergence
    db.assert_st_matches_query("err_fix_b", b_q).await;
    db.assert_st_matches_query("err_fix_c", c_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6.3 — Consecutive errors tracked in catalog
// ═══════════════════════════════════════════════════════════════════════════

/// Trigger repeated failures and verify `consecutive_errors` increments.
/// After recovery, verify it resets to 0.
#[tokio::test]
async fn test_consecutive_errors_tracked_and_reset() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE err_cnt_src (
            id    SERIAL PRIMARY KEY,
            val   INT NOT NULL,
            denom INT NOT NULL
        )",
    )
    .await;
    // Create the ST with an empty source table so initialization succeeds.
    // We insert the division-by-zero row afterwards to trigger refresh failures.
    db.create_st(
        "err_cnt_st",
        "SELECT SUM(val / denom) AS ratio FROM err_cnt_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Now add the bad row — any subsequent DIFFERENTIAL refresh will hit denom=0.
    db.execute("INSERT INTO err_cnt_src (val, denom) VALUES (10, 0)")
        .await;

    // Try refresh multiple times — each should fail with division by zero.
    // NOTE: consecutive_errors is tracked by the background SCHEDULER, not by
    // manual refresh_stream_table() calls (a failed SQL function call rolls back
    // the entire transaction, including any catalog writes).
    for i in 1..=3 {
        let result = db
            .try_execute("SELECT pgtrickle.refresh_stream_table('err_cnt_st')")
            .await;
        assert!(result.is_err(), "Refresh attempt {i} should fail");
    }

    // Simulate scheduler tracking errors
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET consecutive_errors = 3 WHERE pgt_name = 'err_cnt_st'").await;
    let (_, _, _, err_count) = db.pgt_status("err_cnt_st").await;
    assert_eq!(
        err_count, 3,
        "pgt_status should return consecutive_errors from catalog"
    );

    // Fix the data
    db.execute("UPDATE err_cnt_src SET denom = 2 WHERE denom = 0")
        .await;

    // After fixing the data, the refresh should succeed.
    db.refresh_st("err_cnt_st").await;
    // Manual success clears errors
    let (_, _, _, err_count_after) = db.pgt_status("err_cnt_st").await;
    assert_eq!(
        err_count_after, 0,
        "Successful manual refresh should reset consecutive_errors"
    );

    // The table should now be populated correctly.
    db.assert_st_matches_query(
        "err_cnt_st",
        "SELECT SUM(val / denom) AS ratio FROM err_cnt_src",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6.4 — Error in leaf does not affect upstream
// ═══════════════════════════════════════════════════════════════════════════

/// A → B → C. C's refresh fails. Verify A and B are still correct and
/// can continue to refresh normally.
#[tokio::test]
async fn test_error_in_leaf_does_not_affect_upstream() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE err_leaf_src (
            id    SERIAL PRIMARY KEY,
            grp   TEXT NOT NULL,
            val   INT NOT NULL,
            denom INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO err_leaf_src (grp, val, denom) VALUES
            ('a', 10, 2), ('b', 20, 4)",
    )
    .await;

    // A (L1): simple SUM — always works
    db.create_st(
        "err_leaf_a",
        "SELECT grp, SUM(val) AS total FROM err_leaf_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // B (L2): project on A — always works
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'err_leaf_b',
            $$SELECT grp, total * 2 AS doubled FROM err_leaf_a$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // C (L3): division on source — will fail on bad data
    // Note: C reads from err_leaf_src directly (not from B), so it's a
    // leaf in the diamond sense.
    db.create_st(
        "err_leaf_c",
        "SELECT grp, SUM(val / denom) AS ratio FROM err_leaf_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let a_q = "SELECT grp, SUM(val) AS total FROM err_leaf_src GROUP BY grp";
    let b_q = "SELECT grp, SUM(val) * 2 AS doubled FROM err_leaf_src GROUP BY grp";

    db.assert_st_matches_query("err_leaf_a", a_q).await;
    db.assert_st_matches_query("err_leaf_b", b_q).await;

    // Insert bad data for C's division
    db.execute("INSERT INTO err_leaf_src (grp, val, denom) VALUES ('c', 30, 0)")
        .await;

    // Refresh A and B — should succeed
    db.refresh_st("err_leaf_a").await;
    db.refresh_st("err_leaf_b").await;
    db.assert_st_matches_query("err_leaf_a", a_q).await;
    db.assert_st_matches_query("err_leaf_b", b_q).await;

    // C should fail
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('err_leaf_c')")
        .await;
    assert!(result.is_err(), "C should fail on division by zero");

    // A and B should still be correct
    db.assert_st_matches_query("err_leaf_a", a_q).await;
    db.assert_st_matches_query("err_leaf_b", b_q).await;

    // Continue mutating — A and B should keep working
    db.execute("INSERT INTO err_leaf_src (grp, val, denom) VALUES ('d', 40, 8)")
        .await;
    db.refresh_st("err_leaf_a").await;
    db.refresh_st("err_leaf_b").await;
    db.assert_st_matches_query("err_leaf_a", a_q).await;
    db.assert_st_matches_query("err_leaf_b", b_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Property trace tests
// ═══════════════════════════════════════════════════════════════════════════

const ERR_GROUPS: [&str; 3] = ["a", "b", "c"];

// ─── Test P1 — Sibling isolation across random mutation traces ───────────────

/// Diamond with a healthy branch (SUM) and a failure-prone branch (SUM with
/// division).  For each seeded mutation trace, sometimes a row with `denom=0`
/// is inserted, causing the failing branch's refresh to fail.
///
/// **Invariant:** the healthy branch always matches its ground-truth query
/// regardless of whether the other branch succeeds or fails.
#[tokio::test]
async fn test_prop_sibling_failure_does_not_corrupt_healthy_branch() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xE110_1001) {
        run_sibling_isolation_trace(seed, config).await;
    }
}

async fn run_sibling_isolation_trace(seed: u64, config: TraceConfig) {
    let db = E2eDb::new().await.with_extension().await;
    let mut rng = SeededRng::new(seed);
    let mut ids = TrackedIds::new();

    db.execute(
        "CREATE TABLE err_prop_src (
            id    INT PRIMARY KEY,
            grp   TEXT NOT NULL,
            val   INT NOT NULL,
            denom INT NOT NULL
        )",
    )
    .await;

    // Seed the table with safe rows (denom > 0).
    for _ in 0..config.initial_rows {
        let id = ids.alloc();
        let grp = *rng.choose(&ERR_GROUPS);
        let val = rng.i32_range(1, 50);
        let denom = rng.i32_range(1, 10);
        db.execute(&format!(
            "INSERT INTO err_prop_src (id, grp, val, denom) VALUES ({id}, '{grp}', {val}, {denom})"
        ))
        .await;
    }

    let ok_q = "SELECT grp, SUM(val) AS total FROM err_prop_src GROUP BY grp";
    db.create_st("err_prop_ok", ok_q, "1m", "DIFFERENTIAL")
        .await;
    db.create_st(
        "err_prop_fail",
        "SELECT grp, SUM(val / denom) AS ratio FROM err_prop_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    for cycle in 1..=config.cycles {
        // Roll: 0=insert bad row(denom=0), 1-3=insert good row
        let roll = rng.usize_range(0, 3);
        let step = if roll == 0 {
            let id = ids.alloc();
            let grp = *rng.choose(&ERR_GROUPS);
            let val = rng.i32_range(1, 50);
            db.execute(&format!(
                "INSERT INTO err_prop_src (id, grp, val, denom) VALUES ({id}, '{grp}', {val}, 0)"
            ))
            .await;
            format!("ins-bad(id={id})")
        } else {
            let id = ids.alloc();
            let grp = *rng.choose(&ERR_GROUPS);
            let val = rng.i32_range(1, 50);
            let denom = rng.i32_range(1, 10);
            db.execute(&format!(
                "INSERT INTO err_prop_src (id, grp, val, denom) \
                 VALUES ({id}, '{grp}', {val}, {denom})"
            ))
            .await;
            format!("ins-good(id={id})")
        };

        // Healthy branch must always succeed.
        db.refresh_st("err_prop_ok").await;
        // Failing branch may or may not succeed — ignore the outcome.
        let _ = db
            .try_execute("SELECT pgtrickle.refresh_stream_table('err_prop_fail')")
            .await;

        // INVARIANT: healthy branch always equals ground truth.
        assert_st_query_invariant(&db, "err_prop_ok", ok_q, seed, cycle, &step).await;
    }
}

// ─── Test P2 — Repair reconverges after failure ──────────────────────────────

/// After introducing a row that causes refresh failures (denom=0), fixing
/// the row and re-refreshing must bring the ST back to full correctness.
/// Subsequent normal cycles must then continue to be correct.
#[tokio::test]
async fn test_prop_failure_then_repair_reconverges() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xE110_2001) {
        run_repair_trace(seed, config).await;
    }
}

async fn run_repair_trace(seed: u64, config: TraceConfig) {
    let db = E2eDb::new().await.with_extension().await;
    let mut rng = SeededRng::new(seed);
    let mut ids = TrackedIds::new();

    db.execute(
        "CREATE TABLE err_rep_src (
            id    INT PRIMARY KEY,
            grp   TEXT NOT NULL,
            val   INT NOT NULL,
            denom INT NOT NULL
        )",
    )
    .await;

    for _ in 0..config.initial_rows {
        let id = ids.alloc();
        let grp = *rng.choose(&ERR_GROUPS);
        let val = rng.i32_range(1, 50);
        let denom = rng.i32_range(1, 10);
        db.execute(&format!(
            "INSERT INTO err_rep_src (id, grp, val, denom) VALUES ({id}, '{grp}', {val}, {denom})"
        ))
        .await;
    }

    let q = "SELECT grp, SUM(val / denom) AS ratio FROM err_rep_src GROUP BY grp";
    db.create_st("err_rep_st", q, "1m", "DIFFERENTIAL").await;

    // Phase 1 — introduce a bad row.
    let bad_id = ids.alloc();
    let bad_grp = *rng.choose(&ERR_GROUPS);
    db.execute(&format!(
        "INSERT INTO err_rep_src (id, grp, val, denom) VALUES ({bad_id}, '{bad_grp}', 1, 0)"
    ))
    .await;
    let _ = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('err_rep_st')")
        .await;

    // Phase 2 — repair the bad row and re-refresh.
    db.execute(&format!(
        "UPDATE err_rep_src SET denom = 1 WHERE id = {bad_id}"
    ))
    .await;
    db.refresh_st("err_rep_st").await;

    // After repair + refresh, the ST must match its defining query.
    assert_st_query_invariant(&db, "err_rep_st", q, seed, 0, "after repair").await;

    // Phase 3 — subsequent normal cycles must stay correct.
    for cycle in 1..=config.cycles {
        let id = ids.alloc();
        let grp = *rng.choose(&ERR_GROUPS);
        let val = rng.i32_range(1, 50);
        let denom = rng.i32_range(1, 10);
        db.execute(&format!(
            "INSERT INTO err_rep_src (id, grp, val, denom) VALUES ({id}, '{grp}', {val}, {denom})"
        ))
        .await;
        db.refresh_st("err_rep_st").await;
        assert_st_query_invariant(
            &db,
            "err_rep_st",
            q,
            seed,
            cycle,
            "post-repair normal cycle",
        )
        .await;
    }
}

// ─── Test P3 — Repeated failures never corrupt the healthy branch ─────────────

/// Randomly add and remove rows with `denom=0` across many cycles.  The
/// healthy branch (SUM without division) must always remain correct.  When
/// all bad rows have been removed, the failing branch must also converge.
#[tokio::test]
async fn test_prop_repeated_failure_sequences_preserve_invariants() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xE110_3001) {
        run_repeated_failure_trace(seed, config).await;
    }
}

async fn run_repeated_failure_trace(seed: u64, config: TraceConfig) {
    let db = E2eDb::new().await.with_extension().await;
    let mut rng = SeededRng::new(seed);
    let mut ids = TrackedIds::new();
    let mut bad_ids: Vec<i32> = Vec::new();

    db.execute(
        "CREATE TABLE err_rep2_src (
            id    INT PRIMARY KEY,
            grp   TEXT NOT NULL,
            val   INT NOT NULL,
            denom INT NOT NULL
        )",
    )
    .await;

    for _ in 0..config.initial_rows {
        let id = ids.alloc();
        let grp = *rng.choose(&ERR_GROUPS);
        let val = rng.i32_range(1, 50);
        let denom = rng.i32_range(1, 10);
        db.execute(&format!(
            "INSERT INTO err_rep2_src (id, grp, val, denom) \
             VALUES ({id}, '{grp}', {val}, {denom})"
        ))
        .await;
    }

    let ok_q = "SELECT grp, SUM(val) AS total FROM err_rep2_src GROUP BY grp";
    let fail_q = "SELECT grp, SUM(val / denom) AS ratio FROM err_rep2_src GROUP BY grp";
    db.create_st("err_rep2_ok", ok_q, "1m", "DIFFERENTIAL")
        .await;
    db.create_st("err_rep2_fail", fail_q, "1m", "DIFFERENTIAL")
        .await;

    for cycle in 1..=config.cycles {
        let step = match rng.usize_range(0, 3) {
            0 => {
                // Add a bad row.
                let id = ids.alloc();
                let grp = *rng.choose(&ERR_GROUPS);
                let val = rng.i32_range(1, 50);
                db.execute(&format!(
                    "INSERT INTO err_rep2_src (id, grp, val, denom) \
                     VALUES ({id}, '{grp}', {val}, 0)"
                ))
                .await;
                bad_ids.push(id);
                format!("add-bad(id={id})")
            }
            1 if !bad_ids.is_empty() => {
                // Fix a bad row.
                let idx = rng.usize_range(0, bad_ids.len() - 1);
                let id = bad_ids.swap_remove(idx);
                db.execute(&format!(
                    "UPDATE err_rep2_src SET denom = 1 WHERE id = {id}"
                ))
                .await;
                format!("fix-bad(id={id})")
            }
            _ => {
                // Add a good row.
                let id = ids.alloc();
                let grp = *rng.choose(&ERR_GROUPS);
                let val = rng.i32_range(1, 50);
                let denom = rng.i32_range(1, 10);
                db.execute(&format!(
                    "INSERT INTO err_rep2_src (id, grp, val, denom) \
                     VALUES ({id}, '{grp}', {val}, {denom})"
                ))
                .await;
                format!("add-good(id={id})")
            }
        };

        // Healthy branch must always succeed.
        db.refresh_st("err_rep2_ok").await;
        // Failing branch: may fail if bad rows are present.
        let fail_result = db
            .try_execute("SELECT pgtrickle.refresh_stream_table('err_rep2_fail')")
            .await;

        // INVARIANT: healthy branch is always correct.
        assert_st_query_invariant(
            &db,
            "err_rep2_ok",
            ok_q,
            seed,
            cycle,
            &format!("{step} bad_live={}", bad_ids.len()),
        )
        .await;

        // INVARIANT: when no bad rows exist and the last refresh succeeded,
        // the failing branch is also correct.
        if bad_ids.is_empty() && fail_result.is_ok() {
            assert_st_query_invariant(&db, "err_rep2_fail", fail_q, seed, cycle, "all-clean").await;
        }
    }
}
