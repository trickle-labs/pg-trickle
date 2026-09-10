//! E2E tests for wide and deep DAG topologies.
//!
//! Validates correctness for topologies beyond simple linear chains:
//! wide fan-out (1 base → 4+ leaves), fan-out-then-converge (fan-in),
//! deep linear chains (5 layers), and multi-source diamonds.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use e2e::property_support::{SeededRng, TraceConfig, TrackedIds, assert_st_query_invariants};

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler Suppression
// ═══════════════════════════════════════════════════════════════════════════

/// Disable the background scheduler for the current test database.
///
/// Tests that perform manual refreshes in topological order can race with the
/// scheduler, which may consume CDC changes out of order and produce stale
/// results. This helper disables the scheduler per-database without affecting
/// other parallel tests.
async fn disable_scheduler(db: &E2eDb) {
    db.execute(
        "DO $$ BEGIN \
           EXECUTE format('ALTER DATABASE %I SET pg_trickle.enabled = off', current_database()); \
         END $$",
    )
    .await;
    db.execute(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = current_database() \
           AND backend_type = 'pg_trickle scheduler' \
           AND pid <> pg_backend_pid()",
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5.1 — Wide fan-out: 1 base → 4 leaf STs
// ═══════════════════════════════════════════════════════════════════════════

/// One base table feeds 4 independent leaf STs with different queries.
/// INSERT into base, refresh all leaves, verify each is independently correct.
#[tokio::test]
async fn test_fanout_4_leaves() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE fo4_src (
            id   SERIAL PRIMARY KEY,
            grp  TEXT NOT NULL,
            val  INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO fo4_src (grp, val) VALUES
            ('a', 10), ('a', 20), ('b', 30), ('c', 40)",
    )
    .await;

    // 4 different leaf STs from the same base table
    db.create_st(
        "fo4_sum",
        "SELECT grp, SUM(val) AS total FROM fo4_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "fo4_cnt",
        "SELECT grp, COUNT(*) AS cnt FROM fo4_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "fo4_max",
        "SELECT grp, MAX(val) AS mx FROM fo4_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "fo4_min",
        "SELECT grp, MIN(val) AS mn FROM fo4_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let queries: Vec<(&str, &str)> = vec![
        (
            "fo4_sum",
            "SELECT grp, SUM(val) AS total FROM fo4_src GROUP BY grp",
        ),
        (
            "fo4_cnt",
            "SELECT grp, COUNT(*) AS cnt FROM fo4_src GROUP BY grp",
        ),
        (
            "fo4_max",
            "SELECT grp, MAX(val) AS mx FROM fo4_src GROUP BY grp",
        ),
        (
            "fo4_min",
            "SELECT grp, MIN(val) AS mn FROM fo4_src GROUP BY grp",
        ),
    ];

    for (name, q) in &queries {
        db.assert_st_matches_query(name, q).await;
    }

    // INSERT new data
    db.execute("INSERT INTO fo4_src (grp, val) VALUES ('a', 5), ('d', 100)")
        .await;

    // Refresh all leaves
    for (name, _) in &queries {
        db.refresh_st(name).await;
    }

    // Verify all independently correct
    for (name, q) in &queries {
        db.assert_st_matches_query(name, q).await;
    }

    // UPDATE
    db.execute("UPDATE fo4_src SET val = val + 10 WHERE grp = 'b'")
        .await;
    for (name, _) in &queries {
        db.refresh_st(name).await;
    }
    for (name, q) in &queries {
        db.assert_st_matches_query(name, q).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5.2 — Fan-out then converge (3 branches → JOIN)
// ═══════════════════════════════════════════════════════════════════════════

/// base → L1a (SUM), L1b (COUNT), L1c (MAX) → L2 (JOIN all three)
///
/// Tests wide fan-out at L1 that converges at L2 via a 3-way join.
#[tokio::test]
async fn test_fanout_then_converge() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute(
        "CREATE TABLE foc_src (
            id   SERIAL PRIMARY KEY,
            grp  TEXT NOT NULL,
            val  INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO foc_src (grp, val) VALUES
            ('a', 10), ('a', 20), ('b', 30), ('b', 40)",
    )
    .await;

    // L1a: SUM by group
    db.create_st(
        "foc_sum",
        "SELECT grp, SUM(val) AS total FROM foc_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L1b: COUNT by group
    db.create_st(
        "foc_cnt",
        "SELECT grp, COUNT(*) AS cnt FROM foc_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L1c: MAX by group
    db.create_st(
        "foc_max",
        "SELECT grp, MAX(val) AS mx FROM foc_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L2: 3-way JOIN (ST-on-ST × 3)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'foc_merged',
            $$SELECT s.grp, s.total, c.cnt, m.mx
              FROM foc_sum s
              JOIN foc_cnt c ON s.grp = c.grp
              JOIN foc_max m ON s.grp = m.grp$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    let l2_q = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt, MAX(val) AS mx \
                FROM foc_src GROUP BY grp";

    db.assert_st_matches_query("foc_merged", l2_q).await;

    // INSERT
    db.execute("INSERT INTO foc_src (grp, val) VALUES ('c', 50)")
        .await;
    db.refresh_st("foc_sum").await;
    db.refresh_st("foc_cnt").await;
    db.refresh_st("foc_max").await;
    db.refresh_st_with_retry("foc_merged").await;
    db.assert_st_matches_query("foc_merged", l2_q).await;

    // DELETE
    db.execute("DELETE FROM foc_src WHERE grp = 'a' AND val = 10")
        .await;
    db.refresh_st("foc_sum").await;
    db.refresh_st("foc_cnt").await;
    db.refresh_st("foc_max").await;
    db.refresh_st_with_retry("foc_merged").await;
    db.assert_st_matches_query("foc_merged", l2_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5.3 — Deep 5-layer linear chain
// ═══════════════════════════════════════════════════════════════════════════

/// 5-layer linear chain exercising scan → project → aggregate → window → filter
/// across the chain, covering the full DVM operator repertoire.
///
/// ```text
/// d5_src → L1 (passthrough) → L2 (arithmetic) → L3 (aggregate)
///          → L4 (window: rank) → L5 (filter: top-N)
/// ```
#[tokio::test]
async fn test_deep_linear_5_layers() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute(
        "CREATE TABLE d5_src (
            id   SERIAL PRIMARY KEY,
            grp  TEXT NOT NULL,
            val  INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO d5_src (grp, val) VALUES
            ('a', 10), ('a', 20), ('b', 30), ('b', 40), ('c', 50)",
    )
    .await;

    // L1: passthrough (scan)
    db.create_st(
        "d5_l1",
        "SELECT id, grp, val FROM public.d5_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L2: arithmetic (project)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'd5_l2',
            $$SELECT id, grp, val * 2 AS v2 FROM public.d5_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // L3: aggregate
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'd5_l3',
            $$SELECT grp, SUM(v2) AS total FROM public.d5_l2 GROUP BY grp$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // L4: window function (rank)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'd5_l4',
            $$SELECT grp, total, RANK() OVER (ORDER BY total DESC) AS rnk FROM public.d5_l3$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // L5: filter (TopK)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'd5_l5',
            $$SELECT grp, total FROM public.d5_l4 WHERE rnk <= 2$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Ground truth: compose the full pipeline against the base table
    let l5_q = "SELECT grp, total FROM ( \
                    SELECT grp, SUM(val * 2) AS total, \
                           RANK() OVER (ORDER BY SUM(val * 2) DESC) AS rnk \
                    FROM d5_src GROUP BY grp \
                ) sub WHERE rnk <= 2";

    db.assert_st_matches_query("d5_l5", l5_q).await;

    // Verify intermediate layers too
    let l3_q = "SELECT grp, SUM(val * 2) AS total FROM d5_src GROUP BY grp";
    db.assert_st_matches_query("d5_l3", l3_q).await;

    // Mutate: INSERT
    db.execute("INSERT INTO d5_src (grp, val) VALUES ('d', 100), ('d', 200)")
        .await;

    // Refresh in topological order.
    // Use refresh_st_with_retry for cascade layers: the scheduler may start
    // a concurrent refresh when it sees upstream data_timestamp advance.
    db.refresh_st("d5_l1").await;
    db.refresh_st_with_retry("d5_l2").await;
    db.refresh_st_with_retry("d5_l3").await;
    db.refresh_st_with_retry("d5_l4").await;
    db.refresh_st_with_retry("d5_l5").await;

    db.assert_st_matches_query("d5_l3", l3_q).await;
    db.assert_st_matches_query("d5_l5", l5_q).await;

    // Mutate: DELETE — remove group 'd' completely
    db.execute("DELETE FROM d5_src WHERE grp = 'd'").await;
    db.refresh_st("d5_l1").await;
    db.refresh_st_with_retry("d5_l2").await;
    db.refresh_st_with_retry("d5_l3").await;
    db.refresh_st_with_retry("d5_l4").await;
    db.refresh_st_with_retry("d5_l5").await;
    db.assert_st_matches_query("d5_l3", l3_q).await;
    db.assert_st_matches_query("d5_l5", l5_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5.4 — Multi-source diamond
// ═══════════════════════════════════════════════════════════════════════════

/// Two base tables join at L1, L1 fans out to L2a and L2b.
///
/// ```text
/// msd_left ──┐
///             └─ L1 (JOIN) ──┬── L2a (SUM)
/// msd_right ─┘               └── L2b (COUNT)
/// ```
#[tokio::test]
async fn test_multi_source_diamond() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute(
        "CREATE TABLE msd_left (
            id   SERIAL PRIMARY KEY,
            key  INT NOT NULL,
            lval TEXT NOT NULL
        )",
    )
    .await;
    db.execute(
        "CREATE TABLE msd_right (
            id   SERIAL PRIMARY KEY,
            key  INT NOT NULL,
            rval INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO msd_left (key, lval) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    db.execute("INSERT INTO msd_right (key, rval) VALUES (1, 10), (2, 20)")
        .await;

    // L1: inner join of left + right
    db.create_st(
        "msd_l1",
        "SELECT l.key, l.lval, r.rval FROM msd_left l JOIN msd_right r ON l.key = r.key",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // L2a: SUM of rval by lval (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'msd_l2a',
            $$SELECT lval, SUM(rval) AS total FROM msd_l1 GROUP BY lval$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // L2b: COUNT by lval (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'msd_l2b',
            $$SELECT lval, COUNT(*) AS cnt FROM msd_l1 GROUP BY lval$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    let l2a_q = "SELECT l.lval, SUM(r.rval) AS total \
                 FROM msd_left l JOIN msd_right r ON l.key = r.key GROUP BY l.lval";
    let l2b_q = "SELECT l.lval, COUNT(*) AS cnt \
                 FROM msd_left l JOIN msd_right r ON l.key = r.key GROUP BY l.lval";

    db.assert_st_matches_query("msd_l2a", l2a_q).await;
    db.assert_st_matches_query("msd_l2b", l2b_q).await;

    // Mutate left table only
    db.execute("INSERT INTO msd_left (key, lval) VALUES (2, 'b_extra')")
        .await;
    db.refresh_st("msd_l1").await;
    db.refresh_st_with_retry("msd_l2a").await;
    db.refresh_st_with_retry("msd_l2b").await;
    db.assert_st_matches_query("msd_l2a", l2a_q).await;
    db.assert_st_matches_query("msd_l2b", l2b_q).await;

    // Mutate right table only
    db.execute("INSERT INTO msd_right (key, rval) VALUES (3, 30)")
        .await;
    db.refresh_st("msd_l1").await;
    db.refresh_st_with_retry("msd_l2a").await;
    db.refresh_st_with_retry("msd_l2b").await;
    db.assert_st_matches_query("msd_l2a", l2a_q).await;
    db.assert_st_matches_query("msd_l2b", l2b_q).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5.5 — Wide fan-out: deletion isolation
// ═══════════════════════════════════════════════════════════════════════════

/// base → L1..L6 (6 leaf STs with different WHERE filters).
/// Delete data relevant to only L3's query.  Verify L1, L2, L4, L5, L6
/// are unaffected (row counts unchanged).
#[tokio::test]
async fn test_wide_fanout_deletion_isolation() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE wf_src (
            id   SERIAL PRIMARY KEY,
            grp  TEXT NOT NULL,
            val  INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO wf_src (grp, val) VALUES
            ('a', 10), ('b', 20), ('c', 30), ('d', 40), ('e', 50), ('f', 60)",
    )
    .await;

    // 6 leaf STs, each filtering a different group
    let groups = ["a", "b", "c", "d", "e", "f"];
    for (i, g) in groups.iter().enumerate() {
        let name = format!("wf_l{}", i + 1);
        db.create_st(
            &name,
            &format!("SELECT id, grp, val FROM wf_src WHERE grp = '{g}'"),
            "1m",
            "DIFFERENTIAL",
        )
        .await;
    }

    // Record initial counts
    let mut initial_counts = Vec::new();
    for i in 1..=6 {
        let cnt = db.count(&format!("public.wf_l{i}")).await;
        initial_counts.push(cnt);
    }

    // Delete only group 'c' (affects wf_l3 only)
    db.execute("DELETE FROM wf_src WHERE grp = 'c'").await;

    // Refresh all
    for i in 1..=6 {
        db.refresh_st(&format!("wf_l{i}")).await;
    }

    // L3 should have 0 rows now
    assert_eq!(
        db.count("public.wf_l3").await,
        0,
        "wf_l3 should be empty after deleting group 'c'"
    );

    // All others should be unchanged
    for i in [1, 2, 4, 5, 6] {
        let cnt = db.count(&format!("public.wf_l{i}")).await;
        assert_eq!(
            cnt,
            initial_counts[i - 1],
            "wf_l{i} should be unaffected by deleting group 'c'"
        );
    }

    // Verify correctness using ground truth
    for (i, g) in groups.iter().enumerate() {
        let name = format!("wf_l{}", i + 1);
        let q = format!("SELECT id, grp, val FROM wf_src WHERE grp = '{g}'");
        db.assert_st_matches_query(&name, &q).await;
    }
}
// ═══════════════════════════════════════════════════════════════════════════
// Property-Based Invariant Traces
// ═══════════════════════════════════════════════════════════════════════════

const FANOUT_INVARIANTS: [(&str, &str); 4] = [
    (
        "prop_fo_leaf1",
        "SELECT id, val FROM prop_fo_src WHERE val % 2 = 0",
    ),
    (
        "prop_fo_leaf2",
        "SELECT id, val FROM prop_fo_src WHERE val % 2 = 1",
    ),
    (
        "prop_fo_leaf3",
        "SELECT val, COUNT(*) as cnt, SUM(val) as sm FROM prop_fo_src GROUP BY val",
    ),
    (
        "prop_fo_leaf4",
        "SELECT val, MIN(id) as mn FROM prop_fo_src GROUP BY val",
    ),
];

#[tokio::test]
async fn test_prop_fanout_leaf_independence() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xF0F0_0001) {
        run_fanout_trace(seed, &config).await;
    }
}

async fn run_fanout_trace(seed: u64, config: &TraceConfig) {
    let db = E2eDb::new().await.with_extension().await;
    let mut rng = SeededRng::new(seed);
    let mut ids = TrackedIds::new();

    db.execute("CREATE TABLE prop_fo_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;

    // Create leaves
    for (st, query) in FANOUT_INVARIANTS {
        db.create_st(st, query, "1m", "DIFFERENTIAL").await;
    }

    let st_names = [
        "prop_fo_leaf1",
        "prop_fo_leaf2",
        "prop_fo_leaf3",
        "prop_fo_leaf4",
    ];

    // Initial setup
    for _ in 0..config.initial_rows {
        let (id, val) = (ids.alloc(), rng.i32_range(1, 10));
        db.execute(&format!("INSERT INTO prop_fo_src VALUES ({id}, {val})"))
            .await;
    }

    for st in st_names {
        db.refresh_st_with_retry(st).await;
    }

    for cycle in 1..=config.cycles {
        let op = rng.usize_range(0, 100);
        if op < 40 {
            // insert
            let (id, val) = (ids.alloc(), rng.i32_range(1, 10));
            db.execute(&format!("INSERT INTO prop_fo_src VALUES ({id}, {val})"))
                .await;
        } else if op < 70 && !ids.is_empty() {
            // update
            let id = ids.pick(&mut rng).unwrap();
            let new_val = rng.i32_range(1, 10);
            db.execute(&format!(
                "UPDATE prop_fo_src SET val = {new_val} WHERE id = {id}"
            ))
            .await;
        } else if !ids.is_empty() {
            // delete
            let id = ids.remove_random(&mut rng).unwrap();
            db.execute(&format!("DELETE FROM prop_fo_src WHERE id = {id}"))
                .await;
        }

        // Random refresh sequences to test independence and overlapping intervals
        let refresh_order = match rng.usize_range(0, 3) {
            0 => vec![
                "prop_fo_leaf1",
                "prop_fo_leaf2",
                "prop_fo_leaf3",
                "prop_fo_leaf4",
            ],
            1 => vec![
                "prop_fo_leaf4",
                "prop_fo_leaf3",
                "prop_fo_leaf2",
                "prop_fo_leaf1",
            ],
            2 => vec![
                "prop_fo_leaf2",
                "prop_fo_leaf4",
                "prop_fo_leaf1",
                "prop_fo_leaf3",
            ],
            _ => vec![
                "prop_fo_leaf3",
                "prop_fo_leaf1",
                "prop_fo_leaf4",
                "prop_fo_leaf2",
            ],
        };

        for st in &refresh_order {
            db.refresh_st_with_retry(st).await;
        }

        let check_list = if refresh_order.len() == 4 {
            FANOUT_INVARIANTS.to_vec()
        } else {
            vec![]
        };

        if !check_list.is_empty() {
            assert_st_query_invariants(&db, &check_list, seed, cycle, "cycle").await;
        }
    }

    // Final convergence refresh
    for st in st_names {
        db.refresh_st_with_retry(st).await;
    }
    assert_st_query_invariants(&db, &FANOUT_INVARIANTS, seed, config.cycles + 1, "final").await;
}

const DEEP_CHAIN_INVARIANTS: [(&str, &str); 4] = [
    (
        "prop_dc_l1",
        "SELECT id, val FROM prop_dc_src WHERE val > 0",
    ),
    ("prop_dc_l2", "SELECT id, val FROM prop_dc_l1 WHERE val > 1"),
    ("prop_dc_l3", "SELECT id, val FROM prop_dc_l2 WHERE val > 2"),
    ("prop_dc_l4", "SELECT id, val FROM prop_dc_l3 WHERE val > 3"),
];

#[tokio::test]
async fn test_prop_deep_chain_small_trace() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xDCDC_0001) {
        run_deep_chain_trace(seed, &config).await;
    }
}

async fn run_deep_chain_trace(seed: u64, config: &TraceConfig) {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;
    let mut rng = SeededRng::new(seed);
    let mut ids = TrackedIds::new();

    db.execute("CREATE TABLE prop_dc_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;

    for (st, query) in DEEP_CHAIN_INVARIANTS {
        db.create_st(st, query, "1m", "DIFFERENTIAL").await;
    }

    for _ in 0..config.initial_rows {
        let (id, val) = (ids.alloc(), rng.i32_range(0, 5));
        db.execute(&format!("INSERT INTO prop_dc_src VALUES ({id}, {val})"))
            .await;
    }

    for (st, _) in DEEP_CHAIN_INVARIANTS {
        db.refresh_st_with_retry(st).await;
    }

    for cycle in 1..=config.cycles {
        let op = rng.usize_range(0, 100);
        if op < 40 {
            let (id, val) = (ids.alloc(), rng.i32_range(0, 5));
            db.execute(&format!("INSERT INTO prop_dc_src VALUES ({id}, {val})"))
                .await;
        } else if op < 70 && !ids.is_empty() {
            let id = ids.pick(&mut rng).unwrap();
            let new_val = rng.i32_range(0, 5);
            db.execute(&format!(
                "UPDATE prop_dc_src SET val = {new_val} WHERE id = {id}"
            ))
            .await;
        } else if !ids.is_empty() {
            let id = ids.remove_random(&mut rng).unwrap();
            db.execute(&format!("DELETE FROM prop_dc_src WHERE id = {id}"))
                .await;
        }

        for (st, _) in DEEP_CHAIN_INVARIANTS {
            db.refresh_st_with_retry(st).await;
        }

        assert_st_query_invariants(&db, &DEEP_CHAIN_INVARIANTS, seed, cycle, "cycle").await;
    }
}
