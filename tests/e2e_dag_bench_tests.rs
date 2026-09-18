//! DAG topology benchmarks and propagation measurements.
//!
//! Measures end-to-end propagation latency and throughput through multi-level
//! DAG topologies (linear chains, wide DAGs, fan-out trees, diamonds, mixed).
//!
//! These tests are `#[ignore]`d to skip in normal CI. Run explicitly:
//!
//! ```bash
//! cargo test --test e2e_dag_bench_tests --features pg18 -- --ignored --nocapture
//! ```
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use std::time::{Duration, Instant};

// ── Configuration ──────────────────────────────────────────────────────

/// Number of measurement cycles per benchmark (after warm-up).
const CYCLES: usize = 5;

/// Number of throw-away warm-up rounds before measured cycles.
const WARMUP_CYCLES: usize = 2;

/// Rows to INSERT per latency measurement cycle.
const LATENCY_DELTA_ROWS: u32 = 100;

/// Base table size for all topologies.
const BASE_TABLE_SIZE: u32 = 10_000;

/// Number of groups used in source tables.
const NUM_GROUPS: u32 = 10;

/// Delta sizes used for throughput benchmarks.
const THROUGHPUT_DELTA_SIZES: &[u32] = &[10, 100, 1000];

/// Scheduler interval used for latency benchmarks (ms).
const SCHEDULER_INTERVAL_MS: f64 = 200.0;

/// Polling interval for leaf refresh detection (ms).
const POLL_INTERVAL_MS: u64 = 100;

/// Maximum time to wait for a leaf refresh (seconds).
const LEAF_TIMEOUT_SECS: u64 = 120;

// ── Data Structures ────────────────────────────────────────────────────

/// Metadata about a constructed DAG topology.
struct DagTopology {
    /// Base source table names.
    source_tables: Vec<String>,
    /// All ST names in topological (creation) order.
    all_sts: Vec<String>,
    /// Terminal STs (no downstream consumers).
    leaf_sts: Vec<String>,
    /// Longest path from source to leaf.
    depth: u32,
    /// Maximum STs at any single level.
    max_width: u32,
    /// STs grouped by level (for parallel dispatch analysis).
    levels: Vec<Vec<String>>,
}

/// Per-ST timing extracted from `pgt_refresh_history`.
#[derive(Clone, Debug)]
struct StTimingEntry {
    pgt_name: String,
    level: u32,
    refresh_ms: f64,
    action: String,
}

/// A single DAG benchmark measurement.
#[derive(Clone)]
struct DagBenchResult {
    topology: String,
    refresh_mode: String,
    measurement: String,
    dag_depth: u32,
    dag_width: u32,
    total_sts: u32,
    delta_rows: u32,
    cycle: usize,
    propagation_ms: f64,
    per_hop_avg_ms: f64,
    throughput_rows_per_sec: f64,
    theoretical_ms: f64,
    overhead_pct: f64,
    per_st_breakdown: Vec<StTimingEntry>,
}

// ── Topology Builders ──────────────────────────────────────────────────

/// Build a linear chain: src → st_lc_1 → st_lc_2 → ... → st_lc_N
///
/// L1: aggregate (GROUP BY grp, SUM + COUNT)
/// L2+: alternating project (total * 2) and filter (WHERE total > 0)
async fn build_linear_chain(db: &E2eDb, depth: u32) -> DagTopology {
    let src = "lc_src".to_string();
    db.execute(
        "CREATE TABLE lc_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute(&format!(
        "INSERT INTO lc_src (grp, val)
         SELECT 'g' || (i % {NUM_GROUPS}), (random() * 1000)::int
         FROM generate_series(1, {BASE_TABLE_SIZE}) AS s(i)"
    ))
    .await;

    db.execute("ANALYZE lc_src").await;

    let mut all_sts = Vec::new();
    let mut levels: Vec<Vec<String>> = Vec::new();

    // L1: aggregate
    let st1 = "st_lc_1".to_string();
    db.create_st(
        &st1,
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM lc_src GROUP BY grp",
        "calculated",
        "DIFFERENTIAL",
    )
    .await;
    all_sts.push(st1.clone());
    levels.push(vec![st1]);

    // L2+: alternating project / filter on previous ST
    for i in 2..=depth {
        let st_name = format!("st_lc_{i}");
        let prev = format!("st_lc_{}", i - 1);
        let query = if i % 2 == 0 {
            format!("SELECT grp, total * 2 AS total, cnt FROM {prev}")
        } else {
            format!("SELECT grp, total, cnt FROM {prev} WHERE total > 0")
        };
        db.execute(&format!(
            "SELECT pgtrickle.create_stream_table('{st_name}', $${query}$$, 'calculated', 'DIFFERENTIAL')"
        ))
        .await;
        all_sts.push(st_name.clone());
        levels.push(vec![st_name]);
    }

    let leaf = all_sts.last().unwrap().clone();

    DagTopology {
        source_tables: vec![src],
        all_sts,
        leaf_sts: vec![leaf],
        depth,
        max_width: 1,
        levels,
    }
}

/// Build a wide DAG: src → [W parallel chains of depth D]
///
/// L1: `width` independent STs, each filtering a different group from the source.
/// L2+: Each ST depends on the ST directly above (same position index).
async fn build_wide_dag(db: &E2eDb, depth: u32, width: u32) -> DagTopology {
    let src = "wd_src".to_string();
    db.execute(
        "CREATE TABLE wd_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute(&format!(
        "INSERT INTO wd_src (grp, val)
         SELECT 'g' || (i % {width}), (random() * 1000)::int
         FROM generate_series(1, {BASE_TABLE_SIZE}) AS s(i)"
    ))
    .await;

    db.execute("ANALYZE wd_src").await;

    let mut all_sts = Vec::new();
    let mut levels: Vec<Vec<String>> = Vec::new();

    // L1: width independent STs, each filtering a different group
    let mut level1 = Vec::new();
    for w in 0..width {
        let st_name = format!("st_wd_1_{w}");
        db.create_st(
            &st_name,
            &format!(
                "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM wd_src WHERE grp = 'g{w}' GROUP BY grp"
            ),
            "calculated",
            "DIFFERENTIAL",
        )
        .await;
        all_sts.push(st_name.clone());
        level1.push(st_name);
    }
    levels.push(level1);

    // L2+: each ST at level L depends on same-index ST at level L-1
    for d in 2..=depth {
        let mut level = Vec::new();
        for w in 0..width {
            let st_name = format!("st_wd_{d}_{w}");
            let prev = format!("st_wd_{}_{w}", d - 1);
            let query = if d % 2 == 0 {
                format!("SELECT grp, total * 2 AS total, cnt FROM {prev}")
            } else {
                format!("SELECT grp, total, cnt FROM {prev} WHERE total > 0")
            };
            db.execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_name}', $${query}$$, 'calculated', 'DIFFERENTIAL')"
            ))
            .await;
            all_sts.push(st_name.clone());
            level.push(st_name);
        }
        levels.push(level);
    }

    let leaf_sts: Vec<String> = levels.last().unwrap().clone();

    DagTopology {
        source_tables: vec![src],
        all_sts,
        leaf_sts,
        depth,
        max_width: width,
        levels,
    }
}

/// Build a fan-out tree: src → root → [b children] → [b² grandchildren] → ...
///
/// Each node fans out to `branching_factor` children with different filter/project queries.
/// Total STs = (b^d - 1) / (b - 1) where b = branching factor, d = depth.
async fn build_fan_out_tree(db: &E2eDb, depth: u32, branching_factor: u32) -> DagTopology {
    let src = "fo_src".to_string();
    db.execute(
        "CREATE TABLE fo_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute(&format!(
        "INSERT INTO fo_src (grp, val)
         SELECT 'g' || (i % {NUM_GROUPS}), (random() * 1000)::int
         FROM generate_series(1, {BASE_TABLE_SIZE}) AS s(i)"
    ))
    .await;

    db.execute("ANALYZE fo_src").await;

    let mut all_sts = Vec::new();
    let mut levels: Vec<Vec<String>> = Vec::new();
    let mut max_width: u32 = 0;

    // L1: single root aggregate
    let root = "st_fo_1".to_string();
    db.create_st(
        &root,
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM fo_src GROUP BY grp",
        "calculated",
        "DIFFERENTIAL",
    )
    .await;
    all_sts.push(root.clone());
    levels.push(vec![root]);
    max_width = max_width.max(1);

    // L2+: each parent fans out to branching_factor children
    for d in 2..=depth {
        let parent_level = &levels[d as usize - 2];
        let mut level = Vec::new();
        for (pi, parent) in parent_level.iter().enumerate() {
            for b in 0..branching_factor {
                let st_name = format!("st_fo_{d}_{}_{b}", pi);
                let query = if b % 2 == 0 {
                    format!("SELECT grp, total * {} AS total, cnt FROM {parent}", b + 2)
                } else {
                    format!("SELECT grp, total, cnt FROM {parent} WHERE total > {b}")
                };
                db.execute(&format!(
                    "SELECT pgtrickle.create_stream_table('{st_name}', $${query}$$, 'calculated', 'DIFFERENTIAL')"
                ))
                .await;
                all_sts.push(st_name.clone());
                level.push(st_name);
            }
        }
        max_width = max_width.max(level.len() as u32);
        levels.push(level);
    }

    let leaf_sts = levels.last().unwrap().clone();

    DagTopology {
        source_tables: vec![src],
        all_sts,
        leaf_sts,
        depth,
        max_width,
        levels,
    }
}

/// Build a diamond topology: src → [fan_out aggregates] → join ST → [extra_depth layers]
///
/// L1: `fan_out` independent aggregate STs (SUM, COUNT, MAX, MIN, ...)
/// L2: Single ST that JOINs all L1 STs on `grp`
/// L3+: Optional additional layers after the join point
async fn build_diamond(db: &E2eDb, fan_out: u32, extra_depth: u32) -> DagTopology {
    let src = "dm_src".to_string();
    db.execute(
        "CREATE TABLE dm_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute(&format!(
        "INSERT INTO dm_src (grp, val)
         SELECT 'g' || (i % {NUM_GROUPS}), (random() * 1000)::int
         FROM generate_series(1, {BASE_TABLE_SIZE}) AS s(i)"
    ))
    .await;

    db.execute("ANALYZE dm_src").await;

    let mut all_sts = Vec::new();
    let mut levels: Vec<Vec<String>> = Vec::new();

    // L1: fan_out independent aggregate STs
    let agg_fns = ["SUM", "COUNT", "MAX", "MIN", "AVG"];
    let mut l1_names = Vec::new();
    for i in 0..fan_out {
        let st_name = format!("st_dm_{}", (b'a' + i as u8) as char);
        let agg = agg_fns[i as usize % agg_fns.len()];
        let alias = agg.to_lowercase();
        db.create_st(
            &st_name,
            &format!("SELECT grp, {agg}(val) AS {alias}_val FROM dm_src GROUP BY grp"),
            "calculated",
            "DIFFERENTIAL",
        )
        .await;
        all_sts.push(st_name.clone());
        l1_names.push(st_name);
    }
    levels.push(l1_names.clone());

    // L2: join all L1 STs on grp
    let join_name = "st_dm_join".to_string();
    let first = &l1_names[0];
    let first_alias = "t0";
    let mut select_cols = format!("{first_alias}.grp");
    let mut from_clause = format!("{first} {first_alias}");
    for (i, st) in l1_names.iter().enumerate() {
        let alias = format!("t{i}");
        if i == 0 {
            continue;
        }
        // Add the aggregate column from this ST to the select
        let agg = agg_fns[i % agg_fns.len()].to_lowercase();
        select_cols.push_str(&format!(", {alias}.{agg}_val"));
        from_clause.push_str(&format!(
            " JOIN {st} {alias} ON {first_alias}.grp = {alias}.grp"
        ));
    }
    // Add first ST's column too
    let first_agg = agg_fns[0].to_lowercase();
    select_cols = format!("{first_alias}.grp, {first_alias}.{first_agg}_val, {}", {
        let mut cols = Vec::new();
        for i in 1..l1_names.len() {
            let alias = format!("t{i}");
            let agg = agg_fns[i % agg_fns.len()].to_lowercase();
            cols.push(format!("{alias}.{agg}_val"));
        }
        cols.join(", ")
    });

    let join_query = format!("SELECT {select_cols} FROM {from_clause}");
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('{join_name}', $${join_query}$$, 'calculated', 'DIFFERENTIAL')"
    ))
    .await;
    all_sts.push(join_name.clone());
    levels.push(vec![join_name.clone()]);

    // L3+: additional layers after the join
    let mut prev = join_name;
    for i in 0..extra_depth {
        let st_name = format!("st_dm_ext_{}", i + 1);
        let query = if i % 2 == 0 {
            format!("SELECT grp, {first_agg}_val * 2 AS {first_agg}_val FROM {prev}")
        } else {
            format!("SELECT grp, {first_agg}_val FROM {prev} WHERE {first_agg}_val > 0")
        };
        db.execute(&format!(
            "SELECT pgtrickle.create_stream_table('{st_name}', $${query}$$, 'calculated', 'DIFFERENTIAL')"
        ))
        .await;
        all_sts.push(st_name.clone());
        levels.push(vec![st_name.clone()]);
        prev = st_name;
    }

    let leaf = all_sts.last().unwrap().clone();
    let total_depth = 2 + extra_depth;

    DagTopology {
        source_tables: vec![src],
        all_sts,
        leaf_sts: vec![leaf],
        depth: total_depth,
        max_width: fan_out,
        levels,
    }
}

/// Build a mixed topology modeled on an e-commerce scenario.
///
/// Two source tables (orders, products) with ~35 STs across 4 layers:
/// - Layer 1: base aggregates from each source
/// - Layer 2: domain views (region, product, daily)
/// - Layer 3: dashboards (cross-source joins)
/// - Layer 4: alerts / summary
async fn build_mixed(db: &E2eDb) -> DagTopology {
    // ── Source tables ───────────────────────────────────────────────
    db.execute(
        "CREATE TABLE mx_orders (
            id         SERIAL PRIMARY KEY,
            region     TEXT NOT NULL,
            product_id INT NOT NULL,
            amount     INT NOT NULL,
            ts         DATE NOT NULL DEFAULT CURRENT_DATE
        )",
    )
    .await;

    db.execute(&format!(
        "INSERT INTO mx_orders (region, product_id, amount, ts)
         SELECT
             CASE (i % 4) WHEN 0 THEN 'north' WHEN 1 THEN 'south'
                          WHEN 2 THEN 'east' ELSE 'west' END,
             (i % 50) + 1,
             (random() * 1000)::int,
             CURRENT_DATE - (i % 30)
         FROM generate_series(1, {BASE_TABLE_SIZE}) AS s(i)"
    ))
    .await;

    db.execute(
        "CREATE TABLE mx_products (
            id       SERIAL PRIMARY KEY,
            name     TEXT NOT NULL,
            category TEXT NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO mx_products (name, category)
         SELECT 'product_' || i, CASE (i % 5)
             WHEN 0 THEN 'electronics' WHEN 1 THEN 'clothing'
             WHEN 2 THEN 'food' WHEN 3 THEN 'furniture' ELSE 'other' END
         FROM generate_series(1, 50) AS s(i)",
    )
    .await;

    db.execute("ANALYZE mx_orders").await;
    db.execute("ANALYZE mx_products").await;

    let mut all_sts = Vec::new();
    let mut levels: Vec<Vec<String>> = Vec::new();

    // ── Layer 1: base aggregates ────────────────────────────────────
    let mut l1 = Vec::new();

    // Orders aggregates
    db.create_st(
        "mx_orders_daily",
        "SELECT ts, SUM(amount) AS total, COUNT(*) AS cnt FROM mx_orders GROUP BY ts",
        "calculated",
        "DIFFERENTIAL",
    )
    .await;
    all_sts.push("mx_orders_daily".into());
    l1.push("mx_orders_daily".into());

    db.create_st(
        "mx_orders_by_region",
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM mx_orders GROUP BY region",
        "calculated",
        "DIFFERENTIAL",
    )
    .await;
    all_sts.push("mx_orders_by_region".into());
    l1.push("mx_orders_by_region".into());

    db.create_st(
        "mx_orders_by_product",
        "SELECT product_id, SUM(amount) AS total, COUNT(*) AS cnt FROM mx_orders GROUP BY product_id",
        "calculated",
        "DIFFERENTIAL",
    )
    .await;
    all_sts.push("mx_orders_by_product".into());
    l1.push("mx_orders_by_product".into());

    db.create_st(
        "mx_orders_high_value",
        "SELECT id, region, product_id, amount FROM mx_orders WHERE amount > 500",
        "calculated",
        "DIFFERENTIAL",
    )
    .await;
    all_sts.push("mx_orders_high_value".into());
    l1.push("mx_orders_high_value".into());

    // Products aggregates
    db.create_st(
        "mx_product_stats",
        "SELECT category, COUNT(*) AS cnt FROM mx_products GROUP BY category",
        "calculated",
        "DIFFERENTIAL",
    )
    .await;
    all_sts.push("mx_product_stats".into());
    l1.push("mx_product_stats".into());

    levels.push(l1);

    // ── Layer 2: domain views (ST-on-ST) ────────────────────────────
    let mut l2 = Vec::new();

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_orders_summary',
            $$SELECT SUM(total) AS grand_total, SUM(cnt) AS grand_cnt FROM mx_orders_daily$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_orders_summary".into());
    l2.push("mx_orders_summary".into());

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_region_ranked',
            $$SELECT region, total, cnt,
              total * 100 / NULLIF(cnt, 0) AS avg_amount
              FROM mx_orders_by_region$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_region_ranked".into());
    l2.push("mx_region_ranked".into());

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_top_products',
            $$SELECT product_id, total FROM mx_orders_by_product WHERE total > 100$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_top_products".into());
    l2.push("mx_top_products".into());

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_hv_by_region',
            $$SELECT region, COUNT(*) AS hv_cnt, SUM(amount) AS hv_total
              FROM mx_orders_high_value GROUP BY region$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_hv_by_region".into());
    l2.push("mx_hv_by_region".into());

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_product_categories',
            $$SELECT category, cnt * 2 AS weighted_cnt FROM mx_product_stats$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_product_categories".into());
    l2.push("mx_product_categories".into());

    levels.push(l2);

    // ── Layer 3: dashboards (cross-source joins, deeper chains) ─────
    let mut l3 = Vec::new();

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_exec_dashboard',
            $$SELECT r.region, r.total AS region_total, r.avg_amount,
                     h.hv_cnt, h.hv_total
              FROM mx_region_ranked r
              JOIN mx_hv_by_region h ON r.region = h.region$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_exec_dashboard".into());
    l3.push("mx_exec_dashboard".into());

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_summary_extended',
            $$SELECT grand_total, grand_cnt,
                     grand_total * 100 / NULLIF(grand_cnt, 0) AS grand_avg
              FROM mx_orders_summary$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_summary_extended".into());
    l3.push("mx_summary_extended".into());

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_top_products_doubled',
            $$SELECT product_id, total * 2 AS doubled FROM mx_top_products$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_top_products_doubled".into());
    l3.push("mx_top_products_doubled".into());

    levels.push(l3);

    // ── Layer 4: alerts / final summary ─────────────────────────────
    let mut l4 = Vec::new();

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_alert_high_region',
            $$SELECT region, region_total, hv_cnt
              FROM mx_exec_dashboard WHERE hv_cnt > 0$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_alert_high_region".into());
    l4.push("mx_alert_high_region".into());

    db.execute(
        "SELECT pgtrickle.create_stream_table('mx_final_summary',
            $$SELECT grand_total, grand_avg FROM mx_summary_extended WHERE grand_avg > 0$$,
            'calculated', 'DIFFERENTIAL')",
    )
    .await;
    all_sts.push("mx_final_summary".into());
    l4.push("mx_final_summary".into());

    levels.push(l4);

    let leaf_sts = levels.last().unwrap().clone();
    let max_width = levels.iter().map(|l| l.len() as u32).max().unwrap_or(1);

    DagTopology {
        source_tables: vec!["mx_orders".into(), "mx_products".into()],
        all_sts,
        leaf_sts,
        depth: levels.len() as u32,
        max_width,
        levels,
    }
}

// ── Measurement Helpers ────────────────────────────────────────────────

/// Configure scheduler for latency benchmarks with the given refresh mode.
async fn configure_latency_scheduler(db: &E2eDb, mode: &str, concurrency: u32) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 200")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;

    if mode.starts_with("PARALLEL") {
        db.execute("ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'on'")
            .await;
        db.execute(&format!(
            "ALTER SYSTEM SET pg_trickle.max_concurrent_refreshes = {concurrency}"
        ))
        .await;
    } else {
        // CALCULATED mode tests expect sequential (topological) processing
        // within a single scheduler tick.  The default parallel_refresh_mode
        // is 'on', which dispatches work to background workers and changes
        // the execution model.  Explicitly switch to 'off' so the scheduler
        // uses the inline sequential path.
        db.execute("ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'off'")
            .await;
    }

    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "200")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;

    if mode.starts_with("PARALLEL") {
        db.wait_for_setting("pg_trickle.parallel_refresh_mode", "on")
            .await;
        db.wait_for_setting(
            "pg_trickle.max_concurrent_refreshes",
            &concurrency.to_string(),
        )
        .await;
    } else {
        db.wait_for_setting("pg_trickle.parallel_refresh_mode", "off")
            .await;
    }

    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler did not appear within 90 s"
    );
}

/// INSERT delta rows into a source table.
async fn insert_delta(db: &E2eDb, source_table: &str, delta_rows: u32) {
    let sql = if source_table == "mx_orders" {
        format!(
            "INSERT INTO mx_orders (region, product_id, amount, ts)
             SELECT
                 CASE ((random() * 3)::int) WHEN 0 THEN 'north' WHEN 1 THEN 'south'
                                             WHEN 2 THEN 'east' ELSE 'west' END,
                 ((random() * 49)::int) + 1,
                 (random() * 1000)::int,
                 CURRENT_DATE - ((random() * 29)::int)
             FROM generate_series(1, {delta_rows})"
        )
    } else if source_table == "mx_products" {
        format!(
            "INSERT INTO mx_products (name, category)
             SELECT 'product_' || (1000 + g), CASE ((random() * 4)::int)
                 WHEN 0 THEN 'electronics' WHEN 1 THEN 'clothing'
                 WHEN 2 THEN 'food' WHEN 3 THEN 'furniture' ELSE 'other' END
             FROM generate_series(1, {delta_rows}) AS s(g)"
        )
    } else {
        format!(
            "INSERT INTO {source_table} (grp, val)
             SELECT 'g' || (random() * {})::int, (random() * 1000)::int
             FROM generate_series(1, {delta_rows})",
            NUM_GROUPS - 1,
        )
    };
    db.execute(&sql).await;
}

/// Apply a mixed DML workload (70% UPDATE, 15% DELETE, 15% INSERT).
async fn apply_dml_mix(db: &E2eDb, source_table: &str, delta_size: u32) {
    let n_update = (delta_size as f64 * 0.70) as u32;
    let n_delete = (delta_size as f64 * 0.15) as u32;
    let n_insert = delta_size - n_update - n_delete;

    if n_update > 0 {
        let update_col = if source_table == "mx_orders" {
            "amount = amount + 1"
        } else {
            "val = val + 1"
        };
        db.execute(&format!(
            "UPDATE {source_table} SET {update_col}
             WHERE id IN (SELECT id FROM {source_table} ORDER BY random() LIMIT {n_update})"
        ))
        .await;
    }

    if n_delete > 0 {
        db.execute(&format!(
            "DELETE FROM {source_table}
             WHERE id IN (SELECT id FROM {source_table} ORDER BY random() LIMIT {n_delete})"
        ))
        .await;
    }

    if n_insert > 0 {
        insert_delta(db, source_table, n_insert).await;
    }
}

/// Get the count of COMPLETED refresh entries for a given ST name.
async fn completed_count(db: &E2eDb, pgt_name: &str) -> i64 {
    db.query_scalar::<i64>(&format!(
        "SELECT COUNT(*) FROM pgtrickle.pgt_refresh_history h
         JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
         WHERE st.pgt_name = '{pgt_name}' AND h.status = 'COMPLETED'"
    ))
    .await
}

/// Wait for a leaf ST to have a new COMPLETED refresh entry.
/// Returns elapsed time in ms, or None on timeout.
async fn wait_for_leaf_refresh(
    db: &E2eDb,
    leaf_st: &str,
    before_count: i64,
    timeout: Duration,
) -> Option<f64> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;

        let current = completed_count(db, leaf_st).await;
        if current > before_count {
            return Some(start.elapsed().as_secs_f64() * 1000.0);
        }
    }
}

/// Dump diagnostic info when a latency measurement times out.
/// Shows refresh history, frontiers, and change buffer state per ST.
async fn dump_timeout_diagnostics(db: &E2eDb, since_ts: &str, all_sts: &[String], source: &str) {
    eprintln!("[DAG_BENCH_DIAG] === Timeout Diagnostics ===");

    // 1. Show recent refresh history for ALL STs (last 30 entries)
    eprintln!("[DAG_BENCH_DIAG] Recent refresh history since {since_ts}:");
    let history = db
        .query_scalar_opt::<String>(&format!(
            "SELECT string_agg(line, E'\\n' ORDER BY rn) FROM (
                SELECT ROW_NUMBER() OVER (ORDER BY h.start_time DESC) AS rn,
                    st.pgt_name || ' | ' || h.action || ' | ' || h.status ||
                    ' | +' || COALESCE(h.rows_inserted::text, '?') ||
                    ' -' || COALESCE(h.rows_deleted::text, '?') ||
                    ' | ' || to_char(h.start_time, 'HH24:MI:SS.MS') AS line
                FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
                WHERE h.start_time >= '{since_ts}'::timestamptz
                ORDER BY h.start_time DESC
                LIMIT 30
             ) sub"
        ))
        .await;
    match history {
        Some(h) => {
            for line in h.lines() {
                eprintln!("[DAG_BENCH_DIAG]   {line}");
            }
        }
        None => eprintln!("[DAG_BENCH_DIAG]   (no refresh history found)"),
    }

    // 2. Per-ST status: populated, schedule, refresh_mode, completed count, frontier keys
    eprintln!("[DAG_BENCH_DIAG] Per-ST status:");
    for st_name in all_sts {
        let info = db
            .query_scalar_opt::<String>(&format!(
                "SELECT st.pgt_name ||
                    ' pop=' || st.is_populated::text ||
                    ' sched=' || COALESCE(st.schedule, 'CALC') ||
                    ' mode=' || st.refresh_mode ||
                    ' status=' || st.status ||
                    ' completed=' || (
                        SELECT COUNT(*) FROM pgtrickle.pgt_refresh_history h
                        WHERE h.pgt_id = st.pgt_id AND h.status = 'COMPLETED'
                    )::text ||
                    ' frontier_srcs=' || COALESCE(
                        (SELECT string_agg(key, ',' ORDER BY key)
                         FROM jsonb_object_keys(st.frontier -> 'sources') AS key), 'none')
                 FROM pgtrickle.pgt_stream_tables st
                 WHERE st.pgt_name = '{st_name}'"
            ))
            .await;
        if let Some(i) = info {
            eprintln!("[DAG_BENCH_DIAG]   {i}");
        }
    }

    // 3. Source table change buffer row count (v0.32.0+: stable hash name)
    let source_oid = db
        .query_scalar::<i64>(&format!(
            "SELECT oid::bigint FROM pg_class WHERE relname = '{source}'"
        ))
        .await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            source_oid
        ))
        .await;
    let cb_count = db
        .query_scalar_opt::<i64>(&format!(
            "SELECT COUNT(*)::bigint FROM pgtrickle_changes.changes_{stable_name}"
        ))
        .await;
    eprintln!(
        "[DAG_BENCH_DIAG] Change buffer for {source} (oid={source_oid}): {} rows",
        cb_count.unwrap_or(-1)
    );

    // 4. ST change buffer row counts for STs that have downstream consumers
    eprintln!("[DAG_BENCH_DIAG] ST change buffers:");
    let st_ids: Vec<(String, i64)> = {
        let rows = db
            .query_scalar_opt::<String>(
                "SELECT string_agg(pgt_name || ':' || pgt_id::text, ',' ORDER BY pgt_name)
                 FROM pgtrickle.pgt_stream_tables",
            )
            .await;
        rows.unwrap_or_default()
            .split(',')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, ':');
                let name = parts.next()?.to_string();
                let id = parts.next()?.parse::<i64>().ok()?;
                Some((name, id))
            })
            .collect()
    };
    for (name, pgt_id) in &st_ids {
        let buf_exists = db
            .query_scalar_opt::<bool>(&format!(
                "SELECT to_regclass('pgtrickle_changes.changes_pgt_{pgt_id}') IS NOT NULL"
            ))
            .await
            .unwrap_or(false);
        if buf_exists {
            let count = db
                .query_scalar::<i64>(&format!(
                    "SELECT COUNT(*)::bigint FROM pgtrickle_changes.changes_pgt_{pgt_id}"
                ))
                .await;
            eprintln!("[DAG_BENCH_DIAG]   {name} (pgt_id={pgt_id}): {count} rows in ST buffer");
        }
    }

    // 5. Scheduler job table state — key for diagnosing parallel dispatch stalls
    eprintln!("[DAG_BENCH_DIAG] Scheduler jobs:");
    let job_summary = db
        .query_scalar_opt::<String>(
            "SELECT string_agg(line, E'\\n' ORDER BY rn) FROM (
                SELECT ROW_NUMBER() OVER (ORDER BY enqueued_at DESC) AS rn,
                    'job_id=' || job_id || ' key=' || unit_key || ' kind=' || unit_kind ||
                    ' status=' || status ||
                    ' enqueued=' || to_char(enqueued_at, 'HH24:MI:SS.MS') ||
                    COALESCE(' started=' || to_char(started_at, 'HH24:MI:SS.MS'), '') ||
                    COALESCE(' finished=' || to_char(finished_at, 'HH24:MI:SS.MS'), '') ||
                    COALESCE(' worker_pid=' || worker_pid::text, ' worker_pid=NULL') AS line
                FROM pgtrickle.pgt_scheduler_jobs
                ORDER BY enqueued_at DESC
                LIMIT 40
             ) sub",
        )
        .await;
    match job_summary {
        Some(s) => {
            for line in s.lines() {
                eprintln!("[DAG_BENCH_DIAG]   {line}");
            }
        }
        None => eprintln!("[DAG_BENCH_DIAG]   (no scheduler jobs found)"),
    }

    let job_status_counts = db
        .query_scalar_opt::<String>(
            "SELECT string_agg(status || '=' || cnt::text, ', ' ORDER BY status)
             FROM (SELECT status, count(*) AS cnt FROM pgtrickle.pgt_scheduler_jobs GROUP BY status) sub",
        )
        .await;
    eprintln!(
        "[DAG_BENCH_DIAG] Job status counts: {}",
        job_status_counts.unwrap_or_else(|| "(none)".into())
    );

    // 6. Active worker processes and max_worker_processes
    let active_workers = db
        .query_scalar_opt::<i64>(
            "SELECT count(*)::bigint FROM pg_stat_activity WHERE backend_type = 'pg_trickle refresh worker'",
        )
        .await
        .unwrap_or(0);
    let max_workers = db
        .query_scalar_opt::<String>("SELECT current_setting('max_worker_processes')")
        .await
        .unwrap_or_else(|| "?".into());
    let total_bgw = db
        .query_scalar_opt::<i64>(
            "SELECT count(*)::bigint FROM pg_stat_activity WHERE backend_type LIKE 'pg_trickle%'",
        )
        .await
        .unwrap_or(0);
    eprintln!(
        "[DAG_BENCH_DIAG] Active refresh workers: {active_workers}, total pg_trickle BGW: {total_bgw}, max_worker_processes: {max_workers}"
    );

    eprintln!("[DAG_BENCH_DIAG] === End Diagnostics ===");
}

/// Collect per-ST timing from pgt_refresh_history since a given timestamp.
async fn collect_per_st_timing(
    db: &E2eDb,
    since_ts: &str,
    levels: &[Vec<String>],
) -> Vec<StTimingEntry> {
    // Build a name→level map
    let mut entries = Vec::new();
    for (level_idx, level_sts) in levels.iter().enumerate() {
        for st_name in level_sts {
            let row = db
                .query_scalar_opt::<String>(&format!(
                    "SELECT h.action || '|' || EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000.0
                     FROM pgtrickle.pgt_refresh_history h
                     JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
                     WHERE st.pgt_name = '{st_name}'
                       AND h.status = 'COMPLETED'
                       AND h.start_time >= '{since_ts}'::timestamptz
                     ORDER BY h.start_time DESC
                     LIMIT 1"
                ))
                .await;

            if let Some(row) = row {
                let parts: Vec<&str> = row.splitn(2, '|').collect();
                if parts.len() == 2 {
                    entries.push(StTimingEntry {
                        pgt_name: st_name.clone(),
                        level: (level_idx + 1) as u32,
                        refresh_ms: parts[1].parse::<f64>().unwrap_or(0.0),
                        action: parts[0].to_string(),
                    });
                }
            }
        }
    }
    entries
}

/// Compute theoretical latency from PLAN_DAG_PERFORMANCE.md formulas.
fn theoretical_latency_ms(
    mode: &str,
    levels: &[Vec<String>],
    avg_tr_ms: f64,
    scheduler_interval_ms: f64,
    concurrency: u32,
) -> f64 {
    if mode == "CALCULATED" {
        // L = I_s + N × T_r  where N = total number of STs
        let total_sts: usize = levels.iter().map(|l| l.len()).sum();
        scheduler_interval_ms + total_sts as f64 * avg_tr_ms
    } else {
        // PARALLEL: L = Σ ⌈W_l / C⌉ × max(I_p, T_r) for each level l
        let poll_ms = scheduler_interval_ms;
        levels
            .iter()
            .map(|level| {
                let batches = (level.len() as f64 / concurrency as f64).ceil();
                batches * poll_ms.max(avg_tr_ms)
            })
            .sum()
    }
}

/// Run a complete latency benchmark: enable scheduler, INSERT into source,
/// measure propagation time to leaf ST via pgt_refresh_history polling.
async fn measure_latency(
    db: &E2eDb,
    topo: &DagTopology,
    topology_name: &str,
    mode: &str,
    concurrency: u32,
) -> Vec<DagBenchResult> {
    let leaf = &topo.leaf_sts[0];
    let source = &topo.source_tables[0];
    let timeout = Duration::from_secs(LEAF_TIMEOUT_SECS);

    // Ensure initial population by explicitly refreshing all STs in
    // topological order.  The scheduler may not have populated them yet
    // (especially for ST-on-ST diamond/mixed topologies).
    eprintln!(
        "[DAG_BENCH] Populating {} STs for {topology_name}...",
        topo.all_sts.len()
    );
    for st in &topo.all_sts {
        db.refresh_st_with_retry(st).await;
    }
    eprintln!("[DAG_BENCH] Initial population complete for {topology_name}");

    // Warmup: insert deltas and wait for scheduler to propagate to the leaf.
    // Use main timeout — if the scheduler cannot propagate, warmup cycles will
    // time out and measurement cycles will report timeout warnings.
    for warmup in 0..WARMUP_CYCLES {
        eprintln!(
            "[DAG_BENCH] Warmup cycle {}/{WARMUP_CYCLES} for {topology_name}...",
            warmup + 1
        );
        let before = completed_count(db, leaf).await;
        insert_delta(db, source, LATENCY_DELTA_ROWS).await;
        wait_for_leaf_refresh(db, leaf, before, timeout).await;
    }

    // Get timestamp for post-measurement timing queries
    let mut results = Vec::new();

    for cycle in 1..=CYCLES {
        let before = completed_count(db, leaf).await;
        let since_ts: String = db.query_scalar("SELECT now()::text").await;

        let start = Instant::now();
        insert_delta(db, source, LATENCY_DELTA_ROWS).await;

        let propagation_ms = match wait_for_leaf_refresh(db, leaf, before, timeout).await {
            Some(ms) => ms,
            None => {
                eprintln!("[DAG_BENCH] WARN: timeout waiting for leaf '{leaf}' in cycle {cycle}");
                // Dump diagnostic info on first timeout only (avoid spam)
                if cycle == 1 {
                    dump_timeout_diagnostics(db, &since_ts, &topo.all_sts, source).await;
                }
                start.elapsed().as_secs_f64() * 1000.0
            }
        };

        // Collect per-ST timing breakdown
        let breakdown = collect_per_st_timing(db, &since_ts, &topo.levels).await;
        let avg_tr_ms = if breakdown.is_empty() {
            50.0 // fallback estimate
        } else {
            breakdown.iter().map(|e| e.refresh_ms).sum::<f64>() / breakdown.len() as f64
        };

        let theoretical = theoretical_latency_ms(
            mode,
            &topo.levels,
            avg_tr_ms,
            SCHEDULER_INTERVAL_MS,
            concurrency,
        );

        let overhead_pct = if theoretical > 0.0 {
            (propagation_ms - theoretical) / theoretical * 100.0
        } else {
            0.0
        };

        let per_hop_avg = propagation_ms / topo.depth as f64;
        let throughput = LATENCY_DELTA_ROWS as f64 / (propagation_ms / 1000.0);

        // Machine-parseable output
        eprintln!(
            "[DAG_BENCH] topology={topology_name} mode={mode} sts={} depth={} width={} \
             cycle={cycle} actual_ms={propagation_ms:.1} theory_ms={theoretical:.1} \
             overhead_pct={overhead_pct:.1} per_hop_ms={per_hop_avg:.1}",
            topo.all_sts.len(),
            topo.depth,
            topo.max_width,
        );

        results.push(DagBenchResult {
            topology: topology_name.to_string(),
            refresh_mode: mode.to_string(),
            measurement: "latency".to_string(),
            dag_depth: topo.depth,
            dag_width: topo.max_width,
            total_sts: topo.all_sts.len() as u32,
            delta_rows: LATENCY_DELTA_ROWS,
            cycle,
            propagation_ms,
            per_hop_avg_ms: per_hop_avg,
            throughput_rows_per_sec: throughput,
            theoretical_ms: theoretical,
            overhead_pct,
            per_st_breakdown: breakdown,
        });
    }

    results
}

/// Run a throughput benchmark: disable scheduler, manual topological refresh,
/// measure wall-clock time for full DAG refresh.
async fn measure_throughput(
    db: &E2eDb,
    topo: &DagTopology,
    topology_name: &str,
    delta_sizes: &[u32],
) -> Vec<DagBenchResult> {
    let source = &topo.source_tables[0];
    let mut results = Vec::new();

    for &delta_size in delta_sizes {
        for cycle in 1..=CYCLES {
            // Apply mixed DML
            apply_dml_mix(db, source, delta_size).await;

            let since_ts: String = db.query_scalar("SELECT now()::text").await;

            // Refresh all STs in topological order
            let start = Instant::now();
            for st in &topo.all_sts {
                db.refresh_st_with_retry(st).await;
            }
            let total_ms = start.elapsed().as_secs_f64() * 1000.0;

            let breakdown = collect_per_st_timing(db, &since_ts, &topo.levels).await;
            let avg_tr_ms = if breakdown.is_empty() {
                50.0
            } else {
                breakdown.iter().map(|e| e.refresh_ms).sum::<f64>() / breakdown.len() as f64
            };

            let throughput = delta_size as f64 / (total_ms / 1000.0);
            let per_hop_avg = total_ms / topo.depth as f64;

            // For throughput mode, theoretical = N × T_r (no scheduler overhead)
            let theoretical = topo.all_sts.len() as f64 * avg_tr_ms;
            let overhead_pct = if theoretical > 0.0 {
                (total_ms - theoretical) / theoretical * 100.0
            } else {
                0.0
            };

            eprintln!(
                "[DAG_BENCH] topology={topology_name} mode=THROUGHPUT sts={} depth={} \
                 delta={delta_size} cycle={cycle} total_ms={total_ms:.1} \
                 throughput={throughput:.0} per_hop_ms={per_hop_avg:.1}",
                topo.all_sts.len(),
                topo.depth,
            );

            results.push(DagBenchResult {
                topology: topology_name.to_string(),
                refresh_mode: "MANUAL".to_string(),
                measurement: "throughput".to_string(),
                dag_depth: topo.depth,
                dag_width: topo.max_width,
                total_sts: topo.all_sts.len() as u32,
                delta_rows: delta_size,
                cycle,
                propagation_ms: total_ms,
                per_hop_avg_ms: per_hop_avg,
                throughput_rows_per_sec: throughput,
                theoretical_ms: theoretical,
                overhead_pct,
                per_st_breakdown: breakdown,
            });
        }
    }

    results
}

// ── Reporting ──────────────────────────────────────────────────────────

/// Print ASCII summary table for DAG benchmark results.
fn print_dag_results_table(results: &[DagBenchResult]) {
    if results.is_empty() {
        return;
    }

    println!();
    println!(
        "╔══════════════════════════════════════════════════════════════════════════════════════════════════════╗"
    );
    println!(
        "║                         pg_trickle DAG Topology Benchmark Results                                 ║"
    );
    println!(
        "╠═══════════════╤═══════════════╤══════╤═══════╤═══════╤════════════╤════════════╤═══════════════════╣"
    );
    println!(
        "║ Topology      │ Mode          │ STs  │ Depth │ Width │ Actual ms  │ Theory ms  │ Overhead          ║"
    );
    println!(
        "╠═══════════════╪═══════════════╪══════╪═══════╪═══════╪════════════╪════════════╪═══════════════════╣"
    );

    for r in results {
        println!(
            "║ {:13} │ {:13} │ {:>4} │ {:>5} │ {:>5} │ {:>10.1} │ {:>10.1} │ {:>+7.1}%            ║",
            r.topology,
            r.refresh_mode,
            r.total_sts,
            r.dag_depth,
            r.dag_width,
            r.propagation_ms,
            r.theoretical_ms,
            r.overhead_pct,
        );
    }

    println!(
        "╚═══════════════╧═══════════════╧══════╧═══════╧═══════╧════════════╧════════════╧═══════════════════╝"
    );
    println!();

    // Print per-level breakdown for first result with per_st_breakdown data
    print_per_level_breakdown(results);
}

/// Print per-level timing breakdown.
fn print_per_level_breakdown(results: &[DagBenchResult]) {
    // Group by (topology, mode), show breakdown for first cycle
    let mut seen = std::collections::HashSet::new();

    for r in results {
        let key = format!("{}_{}", r.topology, r.refresh_mode);
        if seen.contains(&key) || r.per_st_breakdown.is_empty() {
            continue;
        }
        seen.insert(key);

        println!(
            "  Per-Level Breakdown ({} D={}, {}):",
            r.topology, r.dag_depth, r.refresh_mode,
        );

        // Group breakdown entries by level
        let mut by_level: std::collections::BTreeMap<u32, Vec<&StTimingEntry>> =
            std::collections::BTreeMap::new();
        for entry in &r.per_st_breakdown {
            by_level.entry(entry.level).or_default().push(entry);
        }

        let mut total_refresh = 0.0;
        for (level, entries) in &by_level {
            let avg_ms = entries.iter().map(|e| e.refresh_ms).sum::<f64>() / entries.len() as f64;
            total_refresh += avg_ms * entries.len() as f64;
            let names: Vec<&str> = entries.iter().map(|e| e.pgt_name.as_str()).collect();
            println!(
                "  Level {:>2}: avg {:>7.1}ms  [{}]",
                level,
                avg_ms,
                names.join(", ")
            );
        }

        let scheduler_overhead = r.propagation_ms - total_refresh;
        println!(
            "  Total:     {:>7.1}ms  (scheduler overhead: {:.1}ms)",
            total_refresh, scheduler_overhead,
        );
        println!();
    }
}

/// Print summary table with averages across cycles.
fn print_dag_summary(results: &[DagBenchResult]) {
    // Group by (topology, mode, measurement, delta_rows)
    let mut groups: std::collections::BTreeMap<
        (String, String, String, u32),
        Vec<&DagBenchResult>,
    > = std::collections::BTreeMap::new();

    for r in results {
        let key = (
            r.topology.clone(),
            r.refresh_mode.clone(),
            r.measurement.clone(),
            r.delta_rows,
        );
        groups.entry(key).or_default().push(r);
    }

    println!(
        "┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐"
    );
    println!(
        "│                                  Summary (avg across cycles)                                       │"
    );
    println!(
        "├───────────────┬───────────────┬───────────┬─────────┬──────────┬──────────┬──────────┬─────────────┤"
    );
    println!(
        "│ Topology      │ Mode          │ Measure   │ Delta   │ Avg ms   │ Med ms   │ P95 ms   │ Avg r/s     │"
    );
    println!(
        "├───────────────┼───────────────┼───────────┼─────────┼──────────┼──────────┼──────────┼─────────────┤"
    );

    for ((topo, mode, meas, delta), entries) in &groups {
        let times: Vec<f64> = entries.iter().map(|r| r.propagation_ms).collect();
        let avg = times.iter().sum::<f64>() / times.len() as f64;
        let median = {
            let mut sorted = times.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            if sorted.len().is_multiple_of(2) {
                (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
            } else {
                sorted[sorted.len() / 2]
            }
        };
        let p95 = {
            let mut sorted = times.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let idx = ((sorted.len() as f64 * 0.95) - 1.0).max(0.0) as usize;
            sorted[idx.min(sorted.len() - 1)]
        };
        let avg_throughput = entries
            .iter()
            .map(|r| r.throughput_rows_per_sec)
            .sum::<f64>()
            / entries.len() as f64;

        println!(
            "│ {:13} │ {:13} │ {:9} │ {:>7} │ {:>8.1} │ {:>8.1} │ {:>8.1} │ {:>11.0} │",
            topo, mode, meas, delta, avg, median, p95, avg_throughput,
        );
    }

    println!(
        "└───────────────┴───────────────┴───────────┴─────────┴──────────┴──────────┴──────────┴─────────────┘"
    );
    println!();
}

/// Write results as JSON for cross-run comparison.
fn write_dag_results_json(results: &[DagBenchResult]) {
    use std::io::Write;

    let out_dir = std::env::var("PGS_DAG_BENCH_JSON_DIR")
        .unwrap_or_else(|_| "target/dag_bench_results".to_string());
    let _ = std::fs::create_dir_all(&out_dir);

    let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H%M%S");
    let path = format!("{out_dir}/{timestamp}.json");

    let mut entries = Vec::new();
    for r in results {
        let breakdown_json: Vec<String> = r
            .per_st_breakdown
            .iter()
            .map(|e| {
                format!(
                    r#"{{"pgt_name":"{}","level":{},"refresh_ms":{:.3},"action":"{}"}}"#,
                    e.pgt_name, e.level, e.refresh_ms, e.action,
                )
            })
            .collect();

        entries.push(format!(
            r#"  {{"topology":"{}","refresh_mode":"{}","measurement":"{}","dag_depth":{},"dag_width":{},"total_sts":{},"delta_rows":{},"cycle":{},"propagation_ms":{:.3},"per_hop_avg_ms":{:.3},"throughput_rows_per_sec":{:.1},"theoretical_ms":{:.3},"overhead_pct":{:.1},"per_st_breakdown":[{}]}}"#,
            r.topology,
            r.refresh_mode,
            r.measurement,
            r.dag_depth,
            r.dag_width,
            r.total_sts,
            r.delta_rows,
            r.cycle,
            r.propagation_ms,
            r.per_hop_avg_ms,
            r.throughput_rows_per_sec,
            r.theoretical_ms,
            r.overhead_pct,
            breakdown_json.join(","),
        ));
    }

    let json = format!("[\n{}\n]\n", entries.join(",\n"));
    match std::fs::File::create(&path) {
        Ok(mut f) => {
            let _ = f.write_all(json.as_bytes());
            eprintln!("[DAG_BENCH_JSON] Results written to {path}");
        }
        Err(e) => {
            eprintln!("[DAG_BENCH_JSON] WARN: Could not write {path}: {e}");
        }
    }
}

/// Report all results: ASCII tables, summary, and JSON.
fn report_results(results: &[DagBenchResult]) {
    print_dag_results_table(results);
    print_dag_summary(results);
    write_dag_results_json(results);
}

// ═══════════════════════════════════════════════════════════════════════
// Latency Benchmarks — Linear Chain, CALCULATED mode
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_latency_linear_5_calc() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "CALCULATED", 1).await;
    let topo = build_linear_chain(&db, 5).await;
    let results = measure_latency(&db, &topo, "linear_chain", "CALCULATED", 1).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_latency_linear_10_calc() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "CALCULATED", 1).await;
    let topo = build_linear_chain(&db, 10).await;
    let results = measure_latency(&db, &topo, "linear_chain", "CALCULATED", 1).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_latency_linear_20_calc() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "CALCULATED", 1).await;
    let topo = build_linear_chain(&db, 20).await;
    let results = measure_latency(&db, &topo, "linear_chain", "CALCULATED", 1).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_latency_linear_10_par4() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "PARALLEL_C4", 4).await;
    let topo = build_linear_chain(&db, 10).await;
    let results = measure_latency(&db, &topo, "linear_chain", "PARALLEL_C4", 4).await;
    report_results(&results);
}

// ═══════════════════════════════════════════════════════════════════════
// Throughput Benchmarks — Linear Chain
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_throughput_linear_5() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    // Disable scheduler for throughput (manual refresh)
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = off")
        .await;
    db.reload_config_and_wait().await;
    let topo = build_linear_chain(&db, 5).await;
    // Initial population
    for st in &topo.all_sts {
        db.refresh_st_with_retry(st).await;
    }
    let results = measure_throughput(&db, &topo, "linear_chain", THROUGHPUT_DELTA_SIZES).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_throughput_linear_10() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = off")
        .await;
    db.reload_config_and_wait().await;
    let topo = build_linear_chain(&db, 10).await;
    for st in &topo.all_sts {
        db.refresh_st_with_retry(st).await;
    }
    let results = measure_throughput(&db, &topo, "linear_chain", THROUGHPUT_DELTA_SIZES).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_throughput_linear_20() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = off")
        .await;
    db.reload_config_and_wait().await;
    let topo = build_linear_chain(&db, 20).await;
    for st in &topo.all_sts {
        db.refresh_st_with_retry(st).await;
    }
    let results = measure_throughput(&db, &topo, "linear_chain", THROUGHPUT_DELTA_SIZES).await;
    report_results(&results);
}

// ═══════════════════════════════════════════════════════════════════════
// Latency Benchmarks — Wide DAG
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_latency_wide_3x20_calc() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "CALCULATED", 1).await;
    let topo = build_wide_dag(&db, 3, 20).await;
    let results = measure_latency(&db, &topo, "wide_dag", "CALCULATED", 1).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_latency_wide_3x20_par4() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "PARALLEL_C4", 4).await;
    let topo = build_wide_dag(&db, 3, 20).await;
    let results = measure_latency(&db, &topo, "wide_dag", "PARALLEL_C4", 4).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_latency_wide_3x20_par8() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "PARALLEL_C8", 8).await;
    let topo = build_wide_dag(&db, 3, 20).await;
    let results = measure_latency(&db, &topo, "wide_dag", "PARALLEL_C8", 8).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_latency_wide_5x20_calc() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "CALCULATED", 1).await;
    let topo = build_wide_dag(&db, 5, 20).await;
    let results = measure_latency(&db, &topo, "wide_dag", "CALCULATED", 1).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_latency_wide_5x20_par8() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "PARALLEL_C8", 8).await;
    let topo = build_wide_dag(&db, 5, 20).await;
    let results = measure_latency(&db, &topo, "wide_dag", "PARALLEL_C8", 8).await;
    report_results(&results);
}

// ═══════════════════════════════════════════════════════════════════════
// Latency Benchmarks — Fan-Out Tree
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_latency_fanout_b2d5_calc() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "CALCULATED", 1).await;
    let topo = build_fan_out_tree(&db, 5, 2).await;
    let results = measure_latency(&db, &topo, "fan_out", "CALCULATED", 1).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_latency_fanout_b2d5_par8() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "PARALLEL_C8", 8).await;
    let topo = build_fan_out_tree(&db, 5, 2).await;
    let results = measure_latency(&db, &topo, "fan_out", "PARALLEL_C8", 8).await;
    report_results(&results);
}

// ═══════════════════════════════════════════════════════════════════════
// Latency Benchmarks — Diamond
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_latency_diamond_4_calc() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "CALCULATED", 1).await;
    let topo = build_diamond(&db, 4, 0).await;
    let results = measure_latency(&db, &topo, "diamond", "CALCULATED", 1).await;
    report_results(&results);
}

// ═══════════════════════════════════════════════════════════════════════
// Latency Benchmarks — Mixed Topology
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_latency_mixed_calc() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "CALCULATED", 1).await;
    let topo = build_mixed(&db).await;
    let results = measure_latency(&db, &topo, "mixed", "CALCULATED", 1).await;
    report_results(&results);
}

#[tokio::test]
#[ignore]
async fn bench_latency_mixed_par8() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_latency_scheduler(&db, "PARALLEL_C8", 8).await;
    let topo = build_mixed(&db).await;
    let results = measure_latency(&db, &topo, "mixed", "PARALLEL_C8", 8).await;
    report_results(&results);
}

// ═══════════════════════════════════════════════════════════════════════
// Throughput Benchmarks — Wide DAG
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_throughput_wide_3x20() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = off")
        .await;
    db.reload_config_and_wait().await;
    let topo = build_wide_dag(&db, 3, 20).await;
    for st in &topo.all_sts {
        db.refresh_st_with_retry(st).await;
    }
    let results = measure_throughput(&db, &topo, "wide_dag", THROUGHPUT_DELTA_SIZES).await;
    report_results(&results);
}

// ═══════════════════════════════════════════════════════════════════════
// Throughput Benchmarks — Fan-Out Tree
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_throughput_fanout_b2d5() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = off")
        .await;
    db.reload_config_and_wait().await;
    let topo = build_fan_out_tree(&db, 5, 2).await;
    for st in &topo.all_sts {
        db.refresh_st_with_retry(st).await;
    }
    let results = measure_throughput(&db, &topo, "fan_out", THROUGHPUT_DELTA_SIZES).await;
    report_results(&results);
}

// ═══════════════════════════════════════════════════════════════════════
// Throughput Benchmarks — Diamond
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_throughput_diamond_4() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = off")
        .await;
    db.reload_config_and_wait().await;
    let topo = build_diamond(&db, 4, 0).await;
    for st in &topo.all_sts {
        db.refresh_st_with_retry(st).await;
    }
    let results = measure_throughput(&db, &topo, "diamond", THROUGHPUT_DELTA_SIZES).await;
    report_results(&results);
}

// ═══════════════════════════════════════════════════════════════════════
// Throughput Benchmarks — Mixed Topology
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_throughput_mixed() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = off")
        .await;
    db.reload_config_and_wait().await;
    let topo = build_mixed(&db).await;
    for st in &topo.all_sts {
        db.refresh_st_with_retry(st).await;
    }
    // Mixed topology uses mx_orders as the primary source for DML
    let results = measure_throughput(&db, &topo, "mixed", THROUGHPUT_DELTA_SIZES).await;
    report_results(&results);
}
