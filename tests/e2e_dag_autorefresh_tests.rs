//! E2E tests for auto-refresh chain propagation through multi-layer DAGs.
//!
//! Validates that the background scheduler correctly refreshes 3+ layer
//! stream table chains in topological order, detecting upstream changes
//! via `data_timestamp` comparison.
//!
//! ## Key architecture paths exercised
//!
//! - `has_stream_table_source_changes()` with 3+ topological levels
//! - Topological traversal in the scheduler tick
//! - `CALCULATED` schedule resolution for ST-on-ST
//! - No spurious cascades on no-op cycles
//!
//! ## Important
//!
//! These tests use `E2eDb::new_on_postgres_db()`, which now creates a fresh
//! per-test database and resets server-level scheduler config before the test.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Configure the scheduler for fast testing:
/// - `pg_trickle.scheduler_interval_ms = 100` (wake every 100ms)
/// - `pg_trickle.min_schedule_seconds = 1` (allow 1-second schedules)
async fn configure_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    // Disable auto-backoff so 1-second schedules never get stretched in slow
    // CI containers — the default (true since v0.10.0) would double the
    // effective interval once a refresh takes > 950 ms.
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;

    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler did not appear in pg_stat_activity within 90 s"
    );
}

/// Wait until `last_refresh_at` advances for a given ST.
/// Returns the new `last_refresh_at` value.
async fn wait_for_refresh_cycle(db: &E2eDb, pgt_name: &str, timeout: Duration) -> String {
    let initial: String = db
        .query_scalar(&format!(
            "SELECT COALESCE(last_refresh_at::text, 'never') \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{pgt_name}'"
        ))
        .await;

    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for scheduler refresh of '{pgt_name}' \
                 (initial last_refresh_at = {initial})"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let current: String = db
            .query_scalar(&format!(
                "SELECT COALESCE(last_refresh_at::text, 'never') \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{pgt_name}'"
            ))
            .await;

        if current != initial {
            return current;
        }
    }
}

/// Wait for each layer in order so initial CDC has propagated through the
/// whole chain before a test starts measuring later refreshes.
async fn wait_for_chain_refresh_cycles(db: &E2eDb, pgt_names: &[&str], timeout: Duration) {
    for pgt_name in pgt_names {
        wait_for_refresh_cycle(db, pgt_name, timeout).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4.1 — 3-layer auto-refresh cascade
// ═══════════════════════════════════════════════════════════════════════════

/// base → L1 → L2 → L3, all with 1s schedule.
/// Insert into base, wait for L3 to auto-refresh, verify correctness.
#[tokio::test]
async fn test_autorefresh_3_layer_cascade() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE ar3_src (id SERIAL PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO ar3_src VALUES (1, 10), (2, 20)")
        .await;

    // L1: passthrough aggregate
    db.create_st(
        "ar3_l1",
        "SELECT id, val FROM ar3_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // L2: arithmetic (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ar3_l2',
            $$SELECT id, val * 2 AS doubled FROM ar3_l1$$,
            '1s',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // L3: further transform (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ar3_l3',
            $$SELECT id, doubled + 1 AS result FROM ar3_l2$$,
            '1s',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Wait for initial scheduler stabilization at every layer.
    wait_for_chain_refresh_cycles(
        &db,
        &["ar3_l1", "ar3_l2", "ar3_l3"],
        Duration::from_secs(30),
    )
    .await;

    // Mutate base
    db.execute("INSERT INTO ar3_src VALUES (3, 30)").await;

    // Wait for the deepest layer to pick up the change
    let refreshed = db
        .wait_for_auto_refresh("ar3_l3", Duration::from_secs(60))
        .await;
    assert!(refreshed, "ar3_l3 should auto-refresh after base mutation");

    // Verify correctness at all layers
    db.assert_st_matches_query("ar3_l1", "SELECT id, val FROM ar3_src")
        .await;
    db.assert_st_matches_query("ar3_l2", "SELECT id, val * 2 AS doubled FROM ar3_src")
        .await;
    db.assert_st_matches_query("ar3_l3", "SELECT id, val * 2 + 1 AS result FROM ar3_src")
        .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4.2 — Diamond auto-refresh cascade
// ═══════════════════════════════════════════════════════════════════════════

/// Diamond: base → L1a + L1b → L2, all with 1s schedule.
/// Insert into base, wait for L2 to auto-refresh, verify convergence.
#[tokio::test]
async fn test_autorefresh_diamond_cascade() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute(
        "CREATE TABLE ard_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO ard_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    // L1a: SUM
    db.create_st(
        "ard_l1a",
        "SELECT grp, SUM(val) AS total FROM ard_src GROUP BY grp",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // L1b: COUNT
    db.create_st(
        "ard_l1b",
        "SELECT grp, COUNT(*) AS cnt FROM ard_src GROUP BY grp",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // L2: JOIN both branches
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ard_l2',
            $$SELECT a.grp, a.total, b.cnt
              FROM ard_l1a a JOIN ard_l1b b ON a.grp = b.grp$$,
            '1s',
            'DIFFERENTIAL'
        )",
    )
    .await;

    let l2_q = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM ard_src GROUP BY grp";

    // Wait for initial stabilization
    wait_for_refresh_cycle(&db, "ard_l2", Duration::from_secs(30)).await;

    // Mutate
    db.execute("INSERT INTO ard_src (grp, val) VALUES ('a', 5), ('c', 30)")
        .await;

    let refreshed = db
        .wait_for_auto_refresh("ard_l2", Duration::from_secs(60))
        .await;
    assert!(refreshed, "ard_l2 should auto-refresh after base mutation");

    db.assert_st_matches_query("ard_l2", l2_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4.3 — CALCULATED schedule
// ═══════════════════════════════════════════════════════════════════════════

/// L1 (schedule 1s) → L2 (schedule 'calculated' = CALCULATED).
/// L2 should refresh whenever L1 has pending changes.
#[tokio::test]
async fn test_autorefresh_calculated_schedule() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE arc_src (id SERIAL PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO arc_src VALUES (1, 100)").await;

    // L1: explicit 1s schedule
    db.create_st(
        "arc_l1",
        "SELECT id, val FROM arc_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // L2: CALCULATED schedule ('calculated')
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'arc_l2',
            $$SELECT id, val * 10 AS scaled FROM arc_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Wait for initial stabilization: arc_l1 has an explicit 1s schedule so
    // the scheduler will refresh it shortly regardless of whether there are
    // source changes.  arc_l2 uses CALCULATED schedule — it only fires when
    // upstream data_timestamp advances, so we cannot use it as a readiness
    // signal here (no new rows arrived since creation).
    wait_for_refresh_cycle(&db, "arc_l1", Duration::from_secs(30)).await;

    // Mutate
    db.execute("INSERT INTO arc_src VALUES (2, 200)").await;

    // Wait for L2 to auto-refresh (CALCULATED schedule should trigger it).
    // 90s timeout gives headroom for loaded CI environments where the scheduler
    // worker may need up to 60s to respawn after a transient crash (SCAL-002).
    let refreshed = db
        .wait_for_auto_refresh("arc_l2", Duration::from_secs(90))
        .await;
    assert!(
        refreshed,
        "arc_l2 (CALCULATED) should auto-refresh when upstream L1 changes"
    );

    db.assert_st_matches_query("arc_l2", "SELECT id, val * 10 AS scaled FROM arc_src")
        .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4.4 — No spurious cascades (3-layer)
// ═══════════════════════════════════════════════════════════════════════════

/// No DML → all 3 data_timestamps should remain stable across 2+
/// scheduler ticks.  Extension of the 2-layer test from
/// `e2e_cascade_regression_tests.rs`.
#[tokio::test]
async fn test_autorefresh_no_spurious_3_layer() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE arns_src (id SERIAL PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO arns_src VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "arns_l1",
        "SELECT id, val FROM arns_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'arns_l2',
            $$SELECT id, val * 2 AS doubled FROM arns_l1$$,
            '1s',
            'DIFFERENTIAL'
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'arns_l3',
            $$SELECT id, doubled + 1 AS result FROM arns_l2$$,
            '1s',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Wait for each layer to consume any stale buffer entries before recording
    // the no-op baseline.
    wait_for_chain_refresh_cycles(
        &db,
        &["arns_l1", "arns_l2", "arns_l3"],
        Duration::from_secs(30),
    )
    .await;

    // Record data_timestamps after first cycle
    let ts_after_first: Vec<String> = {
        let mut v = Vec::new();
        for name in &["arns_l1", "arns_l2", "arns_l3"] {
            let ts: String = db
                .query_scalar(&format!(
                    "SELECT COALESCE(data_timestamp::text, 'null') \
                     FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{name}'"
                ))
                .await;
            v.push(ts);
        }
        v
    };

    // Wait for second scheduler cycle (no DML)
    let lr_after_first: String = db
        .query_scalar(
            "SELECT last_refresh_at::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'arns_l3'",
        )
        .await;

    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(30) {
            panic!("Timed out waiting for second scheduler cycle");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let lr: String = db
            .query_scalar(
                "SELECT last_refresh_at::text FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = 'arns_l3'",
            )
            .await;
        if lr != lr_after_first {
            break;
        }
    }

    // data_timestamps must remain stable — no spurious advance
    let names = ["arns_l1", "arns_l2", "arns_l3"];
    for (i, name) in names.iter().enumerate() {
        let ts: String = db
            .query_scalar(&format!(
                "SELECT COALESCE(data_timestamp::text, 'null') \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{name}'"
            ))
            .await;
        assert_eq!(
            ts_after_first[i], ts,
            "data_timestamp for '{name}' must not advance on no-op scheduler ticks"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4.5 — Staggered schedules
// ═══════════════════════════════════════════════════════════════════════════

/// L1=1s, L2=3s, L3=1s.
/// After DML, L1 refreshes quickly, L2 must wait for its 3s schedule, and
/// L3 cannot advance until L2 has caught up.  Verify eventual convergence.
#[tokio::test]
async fn test_autorefresh_staggered_schedules() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE ars_src (id SERIAL PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO ars_src VALUES (1, 10)").await;

    // L1: fast (1s)
    db.create_st(
        "ars_l1",
        "SELECT id, val FROM ars_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // L2: slower (3s)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ars_l2',
            $$SELECT id, val * 2 AS doubled FROM ars_l1$$,
            '3s',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // L3: fast again (1s) but dependent on L2
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ars_l3',
            $$SELECT id, doubled + 1 AS result FROM ars_l2$$,
            '1s',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Wait for initial cycle at every layer.
    wait_for_chain_refresh_cycles(
        &db,
        &["ars_l1", "ars_l2", "ars_l3"],
        Duration::from_secs(30),
    )
    .await;

    // Now insert new data
    db.execute("INSERT INTO ars_src VALUES (2, 20)").await;

    // L3 should eventually converge — give it enough time for L2's 3s schedule
    let refreshed = db
        .wait_for_auto_refresh("ars_l3", Duration::from_secs(60))
        .await;
    assert!(
        refreshed,
        "ars_l3 should eventually auto-refresh after base mutation with staggered schedules"
    );

    // Verify final correctness
    db.assert_st_matches_query("ars_l1", "SELECT id, val FROM ars_src")
        .await;
    db.assert_st_matches_query("ars_l2", "SELECT id, val * 2 AS doubled FROM ars_src")
        .await;
    db.assert_st_matches_query("ars_l3", "SELECT id, val * 2 + 1 AS result FROM ars_src")
        .await;
}

// Property-Based Invariant Traces
// ═══════════════════════════════════════════════════════════════════════════
use crate::e2e::property_support::{
    SeededRng, TraceConfig, TrackedIds, assert_st_query_invariants,
};

const AUTO_INVARIANTS: [(&str, &str); 3] = [
    (
        "prop_auto_l1",
        "SELECT id, val FROM prop_auto_src WHERE val > 1",
    ),
    (
        "prop_auto_l2",
        "SELECT id, val FROM prop_auto_l1 WHERE val > 2",
    ),
    (
        "prop_auto_l3",
        "SELECT id, val FROM prop_auto_l2 WHERE val > 3",
    ),
];

/// Check whether all AUTO_INVARIANTS hold without panicking.
///
/// Returns `true` if every ST matches its defining query, `false` otherwise.
/// Used by `settle_auto_invariants` to retry after a `wait_for_refresh_cycle`
/// that may have returned on a NO_DATA scheduler cycle (where `last_refresh_at`
/// advances even though no pending CDC changes were processed).
async fn check_auto_invariants(db: &E2eDb) -> bool {
    for (st_table, defining_query) in AUTO_INVARIANTS {
        // Query the non-internal column names (exclude __pgt_* columns) to
        // avoid "different number of columns" errors in EXCEPT ALL.
        let cols: Option<String> = db
            .query_scalar_opt(&format!(
                "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) \
                 FROM information_schema.columns \
                 WHERE table_name = '{st_table}' \
                   AND column_name NOT LIKE '__pgt_%'"
            ))
            .await;
        let cols = match cols {
            Some(c) if !c.is_empty() => c,
            _ => return false,
        };
        let matches: bool = db
            .query_scalar(&format!(
                "SELECT NOT EXISTS ( \
                    (({defining_query}) EXCEPT ALL (SELECT {cols} FROM {st_table})) \
                    UNION ALL \
                    ((SELECT {cols} FROM {st_table}) EXCEPT ALL ({defining_query})) \
                )"
            ))
            .await;
        if !matches {
            return false;
        }
    }
    true
}

/// Wait until all AUTO_INVARIANTS hold, retrying scheduler cycles as needed.
///
/// `wait_for_refresh_cycle` uses `last_refresh_at` which advances even on
/// NO_DATA cycles — so the scheduler may tick l3 before CDC changes from
/// `prop_auto_src` have been ingested by `prop_auto_l1`.  This helper
/// adds a retry loop so we only assert once the data has actually converged.
async fn settle_auto_invariants(
    db: &E2eDb,
    seed: u64,
    cycle: usize,
    step: &str,
    timeout: Duration,
) {
    let deadline = std::time::Instant::now() + timeout;
    // First wait: give the fused chain one opportunity to run.
    wait_for_refresh_cycle(db, "prop_auto_l3", timeout).await;

    loop {
        if check_auto_invariants(db).await {
            return;
        }
        if std::time::Instant::now() >= deadline {
            // Timeout — run the standard assertion to produce a clear failure.
            assert_st_query_invariants(db, &AUTO_INVARIANTS, seed, cycle, step).await;
            return;
        }
        // Wait for another scheduler cycle (l1 is the leaf and must process
        // CDC before l2/l3 can be correct).  Cap the inner wait at the
        // remaining time so the outer deadline is always respected (important
        // for slow coverage / instrumented builds where 15 s may expire).
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining < Duration::from_millis(500) {
            assert_st_query_invariants(db, &AUTO_INVARIANTS, seed, cycle, step).await;
            return;
        }
        wait_for_refresh_cycle(db, "prop_auto_l1", remaining.min(Duration::from_secs(30))).await;
    }
}

#[tokio::test]
async fn test_prop_autorefresh_no_spurious_changes() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xAA11_0001) {
        run_autorefresh_trace(seed, &config).await;
    }
}

async fn run_autorefresh_trace(seed: u64, config: &TraceConfig) {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    let mut rng = SeededRng::new(seed);
    let mut ids = TrackedIds::new();

    db.execute("CREATE TABLE prop_auto_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;

    for (st, query) in AUTO_INVARIANTS {
        db.create_st(st, query, "1s", "DIFFERENTIAL").await;
    }

    for _ in 0..config.initial_rows {
        let (id, val) = (ids.alloc(), rng.i32_range(0, 10));
        db.execute(&format!("INSERT INTO prop_auto_src VALUES ({id}, {val})"))
            .await;
    }

    // Wait for the full cascade to propagate through all 3 layers and for
    // the invariants to actually hold.  Under load the scheduler may fire a
    // NO_DATA cycle before the initial inserts' CDC rows are ingested by l1,
    // so we use settle_auto_invariants which retries until convergence.
    // 90s timeout gives extra headroom for loaded CI environments.
    settle_auto_invariants(&db, seed, 0, "init", Duration::from_secs(90)).await;

    for cycle in 1..=(config.cycles / 2).max(1) {
        let op = rng.usize_range(0, 100);
        if op < 40 {
            let (id, val) = (ids.alloc(), rng.i32_range(0, 10));
            db.execute(&format!("INSERT INTO prop_auto_src VALUES ({id}, {val})"))
                .await;
        } else if op < 70 {
            if let Some(id) = ids.pick(&mut rng) {
                let new_val = rng.i32_range(0, 10);
                db.execute(&format!(
                    "UPDATE prop_auto_src SET val = {new_val} WHERE id = {id}"
                ))
                .await;
            }
        } else {
            if let Some(id) = ids.remove_random(&mut rng) {
                db.execute(&format!("DELETE FROM prop_auto_src WHERE id = {id}"))
                    .await;
            }
        }

        // Wait for the cascade to propagate the DML change and verify
        // invariants hold.  Retries if wait_for_refresh_cycle returned early
        // on a NO_DATA cycle before the DML was processed.
        // 90s timeout gives extra headroom for loaded CI environments.
        settle_auto_invariants(&db, seed, cycle, "auto", Duration::from_secs(90)).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4.7 — Auto-refresh through view upstream chain
// ═══════════════════════════════════════════════════════════════════════════

/// base → user view → ST₁ (1s schedule) → ST₂ (calculated schedule).
/// Insert into base, verify the scheduler propagates through the view
/// to ST₂ without manual intervention. This covers the gap where all
/// previous auto-refresh tests used direct ST-on-ST chains only.
#[tokio::test]
async fn test_autorefresh_view_upstream_chain() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute(
        "CREATE TABLE arv_orders (
            id SERIAL PRIMARY KEY,
            product TEXT NOT NULL,
            qty INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO arv_orders (product, qty) VALUES
         ('Widget', 10), ('Gadget', 5), ('Widget', 20)",
    )
    .await;

    // User view: filter positive quantities
    db.execute(
        "CREATE VIEW arv_v_orders AS
         SELECT id, product, qty FROM arv_orders WHERE qty > 0",
    )
    .await;

    // ST₁: aggregate by product through the view (1s schedule)
    db.create_st(
        "arv_st_summary",
        "SELECT product, SUM(qty) AS total_qty, COUNT(*) AS order_count
         FROM arv_v_orders GROUP BY product",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // ST₂: filter high-volume products (ST-on-ST, calculated schedule)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'arv_st_high_volume',
            $$SELECT product, total_qty FROM arv_st_summary WHERE total_qty >= 20$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Wait for initial stabilization
    wait_for_refresh_cycle(&db, "arv_st_high_volume", Duration::from_secs(30)).await;

    // Only Widget qualifies (30 >= 20)
    assert_eq!(db.count("public.arv_st_high_volume").await, 1);

    // Insert more Gadgets to push them over the threshold
    db.execute("INSERT INTO arv_orders (product, qty) VALUES ('Gadget', 25)")
        .await;

    // Wait for auto-refresh to propagate: base → view → ST₁ → ST₂
    let refreshed = db
        .wait_for_auto_refresh("arv_st_high_volume", Duration::from_secs(60))
        .await;
    assert!(
        refreshed,
        "Auto-refresh should propagate base change through view to ST₂"
    );

    // Both products should now qualify
    db.assert_st_matches_query(
        "arv_st_summary",
        "SELECT product, SUM(qty) AS total_qty, COUNT(*) AS order_count
         FROM arv_v_orders GROUP BY product",
    )
    .await;
    db.assert_st_matches_query(
        "arv_st_high_volume",
        "SELECT product, total_qty FROM arv_st_summary WHERE total_qty >= 20",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4.8 — Auto-refresh diamond with a view branch
// ═══════════════════════════════════════════════════════════════════════════

/// Diamond: base → view → ST₁(SUM) + base → ST₂(COUNT) → ST₃(JOIN).
/// The scheduler must correctly handle the view-inlined branch alongside
/// the direct-table branch in the same diamond DAG.
#[tokio::test]
async fn test_autorefresh_diamond_with_view_branch() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute(
        "CREATE TABLE ardv_src (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            amount INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO ardv_src (category, amount) VALUES ('x', 10), ('x', 20), ('y', 30)")
        .await;

    // Branch A: through a view (SUM)
    db.execute(
        "CREATE VIEW ardv_v_src AS
         SELECT id, category, amount FROM ardv_src WHERE amount > 0",
    )
    .await;
    db.create_st(
        "ardv_branch_a",
        "SELECT category, SUM(amount) AS total FROM ardv_v_src GROUP BY category",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Branch B: direct from table (COUNT)
    db.create_st(
        "ardv_branch_b",
        "SELECT category, COUNT(*) AS cnt FROM ardv_src GROUP BY category",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // ST₃: join both branches (calculated schedule)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ardv_joined',
            $$SELECT a.category, a.total, b.cnt
              FROM ardv_branch_a a JOIN ardv_branch_b b ON a.category = b.category$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Wait for initial stabilization
    wait_for_refresh_cycle(&db, "ardv_joined", Duration::from_secs(30)).await;

    // x: total=30, cnt=2; y: total=30, cnt=1
    assert_eq!(db.count("public.ardv_joined").await, 2);

    // Add data
    db.execute("INSERT INTO ardv_src (category, amount) VALUES ('x', 40), ('z', 100)")
        .await;

    let refreshed = db
        .wait_for_auto_refresh("ardv_joined", Duration::from_secs(60))
        .await;
    assert!(refreshed, "Diamond join ST should auto-refresh");

    // Verify correctness — the joined result should match a direct query
    let expected_q = "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt \
                      FROM ardv_src WHERE amount > 0 GROUP BY category";
    db.assert_st_matches_query("ardv_joined", expected_q).await;
}
