//! Database-level benchmarks — PLAN.md §11.4.
//!
//! Measures differential vs full refresh performance across table sizes,
//! change rates, and query complexities.
//!
//! These tests are `#[ignore]`d to skip in normal CI. Run explicitly:
//!
//! ```bash
//! cargo test --test e2e_bench_tests --features pg18 -- --ignored --nocapture
//! ```
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use std::time::Instant;

// ── Configuration ──────────────────────────────────────────────────────

/// Table sizes to benchmark.
const TABLE_SIZES: &[usize] = &[10_000, 100_000];

/// Change rates (fraction of rows mutated per cycle).
const CHANGE_RATES: &[f64] = &[0.01, 0.10, 0.50];

/// Number of refresh cycles per (size, rate, query, mode) combination.
const CYCLES: usize = 10;

/// Number of throw-away warm-up cycles before measured cycles.
/// Eliminates cache-warming effects from the first measured iteration.
const WARMUP_CYCLES: usize = 2;

// ── Data generation helpers ────────────────────────────────────────────

/// Generate SQL to bulk-insert `n` rows into a single source table.
///
/// Schema: `src(id SERIAL PK, region TEXT, category TEXT, amount INT, score INT)`
fn bulk_insert_single(n: usize) -> String {
    // Use generate_series for fast bulk loading
    format!(
        "INSERT INTO src (region, category, amount, score)
         SELECT
             CASE (i % 5)
                 WHEN 0 THEN 'north'
                 WHEN 1 THEN 'south'
                 WHEN 2 THEN 'east'
                 WHEN 3 THEN 'west'
                 ELSE 'central'
             END,
             CASE (i % 4)
                 WHEN 0 THEN 'A'
                 WHEN 1 THEN 'B'
                 WHEN 2 THEN 'C'
                 ELSE 'D'
             END,
             (i * 17 + 13) % 10000,
             (i * 31 + 7) % 100
         FROM generate_series(1, {n}) AS s(i)"
    )
}

/// Generate SQL to create the second source table for join benchmarks.
fn create_join_table() -> &'static str {
    "CREATE TABLE dim (
         id SERIAL PRIMARY KEY,
         region TEXT NOT NULL,
         region_name TEXT NOT NULL,
         multiplier NUMERIC NOT NULL DEFAULT 1.0
     )"
}

/// Populate the dimension table with 5 regions.
fn populate_dim() -> &'static str {
    "INSERT INTO dim (region, region_name, multiplier) VALUES
     ('north', 'Northern Region', 1.1),
     ('south', 'Southern Region', 0.9),
     ('east', 'Eastern Region', 1.0),
     ('west', 'Western Region', 1.2),
     ('central', 'Central Region', 1.05)"
}

/// Apply random changes to `change_pct` fraction of rows.
/// Returns separate SQL statements (sqlx cannot execute multi-statement strings).
fn apply_changes_stmts(table_size: usize, change_pct: f64) -> Vec<String> {
    let n_changes = ((table_size as f64) * change_pct).max(1.0) as usize;
    // Mix of updates (70%), deletes (15%), inserts (15%)
    let n_updates = (n_changes as f64 * 0.70).max(1.0) as usize;
    let n_deletes = (n_changes as f64 * 0.15).max(1.0) as usize;
    let n_inserts = (n_changes as f64 * 0.15).max(1.0) as usize;

    vec![
        format!(
            "UPDATE src SET amount = amount + 1
             WHERE id IN (
                 SELECT id FROM src ORDER BY random() LIMIT {n_updates}
             )"
        ),
        format!(
            "DELETE FROM src
             WHERE id IN (
                 SELECT id FROM src ORDER BY random() LIMIT {n_deletes}
             )"
        ),
        format!(
            "INSERT INTO src (region, category, amount, score)
             SELECT
                 CASE (i % 5)
                     WHEN 0 THEN 'north' WHEN 1 THEN 'south'
                     WHEN 2 THEN 'east' WHEN 3 THEN 'west' ELSE 'central'
                 END,
                 CASE (i % 4) WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' ELSE 'D' END,
                 (random() * 10000)::int,
                 (random() * 100)::int
             FROM generate_series(1, {n_inserts}) AS s(i)"
        ),
    ]
}

/// Execute all change statements for a benchmark cycle.
async fn apply_changes(db: &E2eDb, table_size: usize, change_pct: f64) {
    for stmt in apply_changes_stmts(table_size, change_pct) {
        db.execute(&stmt).await;
    }
}

// ── Query complexity definitions ───────────────────────────────────────

/// Benchmark scenario: a named query template + whether it needs a join table.
///
/// `max_table_size`: when `Some(n)`, the scenario is skipped for table sizes
/// larger than `n` in the full/large matrix benchmarks.  Use this for queries
/// that fall back to FULL refresh and have super-linear (e.g. O(n²)) cost at
/// large scales — notably correlated LATERAL sub-selects and window functions.
struct QueryScenario {
    name: &'static str,
    query: &'static str,
    needs_dim: bool,
    /// Skip in matrix benchmarks when `table_size > max_table_size`.
    max_table_size: Option<usize>,
}

fn query_scenarios() -> Vec<QueryScenario> {
    vec![
        QueryScenario {
            name: "scan",
            query: "SELECT id, region, category, amount, score FROM src",
            needs_dim: false,
            max_table_size: None,
        },
        QueryScenario {
            name: "filter",
            query: "SELECT id, region, amount FROM src WHERE amount > 5000",
            needs_dim: false,
            max_table_size: None,
        },
        QueryScenario {
            name: "aggregate",
            query: "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM src GROUP BY region",
            needs_dim: false,
            max_table_size: None,
        },
        QueryScenario {
            name: "join",
            query: "SELECT s.id, s.region, s.amount, d.region_name \
                    FROM src s INNER JOIN dim d ON s.region = d.region",
            needs_dim: true,
            max_table_size: None,
        },
        QueryScenario {
            name: "join_agg",
            query: "SELECT d.region_name, SUM(s.amount) AS total, COUNT(*) AS cnt \
                    FROM src s INNER JOIN dim d ON s.region = d.region \
                    GROUP BY d.region_name",
            needs_dim: true,
            max_table_size: None,
        },
        // I-7: Window / lateral / CTE / UNION ALL operator scenarios
        //
        // window: falls back to FULL refresh (window functions not differentiable).
        //         O(n log n) FULL refresh becomes expensive at 100K rows.
        // lateral: correlated sub-select is O(n²/regions) per FULL refresh;
        //          at 100K rows this took ~60 min in CI — cap at 10K.
        QueryScenario {
            name: "window",
            query: "SELECT id, region, amount, \
                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC, id) AS rn \
                    FROM src",
            needs_dim: false,
            max_table_size: Some(10_000),
        },
        QueryScenario {
            name: "lateral",
            query: "SELECT s.id, s.region, s.amount, l.top_score \
                    FROM src s, \
                    LATERAL (SELECT MAX(score) AS top_score FROM src s2 \
                             WHERE s2.region = s.region) l",
            needs_dim: false,
            max_table_size: Some(10_000),
        },
        QueryScenario {
            name: "cte",
            query: "WITH regional AS ( \
                        SELECT region, SUM(amount) AS total FROM src GROUP BY region \
                    ) \
                    SELECT s.id, s.region, s.amount, r.total \
                    FROM src s JOIN regional r ON s.region = r.region",
            needs_dim: false,
            max_table_size: None,
        },
        QueryScenario {
            name: "union_all",
            query: "SELECT id, amount, 'high' AS tier FROM src WHERE amount > 5000 \
                    UNION ALL \
                    SELECT id, amount, 'low' AS tier FROM src WHERE amount <= 5000",
            needs_dim: false,
            max_table_size: None,
        },
    ]
}

// ── Result reporting ───────────────────────────────────────────────────

// ProfileData, extract_last_profile, and parse_profile_line are shared
// utilities defined in tests/e2e/mod.rs.
use e2e::{ProfileData, extract_last_profile};

/// A single benchmark measurement.
#[derive(Clone)]
struct BenchResult {
    scenario: String,
    table_size: usize,
    change_pct: f64,
    mode: String,
    cycle: usize,
    refresh_ms: f64,
    st_row_count: i64,
    profile: Option<ProfileData>,
}

/// Compute a percentile from a sorted slice using linear interpolation.
fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = pct / 100.0 * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = (lo + 1).min(sorted.len() - 1);
    let frac = rank - rank.floor();
    sorted[lo] * (1.0 - frac) + sorted[hi] * frac
}

/// I-4: Write benchmark results to a JSON file for cross-run comparison.
///
/// Output path determined by `PGS_BENCH_JSON` env var, or defaults to
/// `target/bench_results/<timestamp>.json`.
fn write_results_json(results: &[BenchResult]) {
    use std::io::Write;

    let out_dir =
        std::env::var("PGS_BENCH_JSON_DIR").unwrap_or_else(|_| "target/bench_results".to_string());
    let _ = std::fs::create_dir_all(&out_dir);

    let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H%M%S");
    let path = format!("{}/{}.json", out_dir, timestamp);

    // Build JSON manually to avoid serde dependency
    let mut entries = Vec::new();
    for r in results {
        let profile_json = if let Some(ref p) = r.profile {
            format!(
                r#"{{"decision_ms":{:.3},"generate_ms":{:.3},"merge_ms":{:.3},"cleanup_ms":{:.3},"total_ms":{:.3},"affected":{},"path":"{}"}}"#,
                p.decision_ms,
                p.generate_ms,
                p.merge_ms,
                p.cleanup_ms,
                p.total_ms,
                p.affected,
                p.path,
            )
        } else {
            "null".to_string()
        };
        entries.push(format!(
            r#"  {{"scenario":"{}","table_size":{},"change_pct":{},"mode":"{}","cycle":{},"refresh_ms":{:.3},"st_row_count":{},"profile":{}}}"#,
            r.scenario, r.table_size, r.change_pct, r.mode, r.cycle,
            r.refresh_ms, r.st_row_count, profile_json,
        ));
    }

    let json = format!("[\n{}\n]\n", entries.join(",\n"));
    match std::fs::File::create(&path) {
        Ok(mut f) => {
            let _ = f.write_all(json.as_bytes());
            eprintln!("[BENCH_JSON] Results written to {}", path);
        }
        Err(e) => {
            eprintln!("[BENCH_JSON] WARN: Could not write {}: {}", path, e);
        }
    }
}

fn print_results_table(results: &[BenchResult]) {
    // ── Per-cycle machine-parseable output (I-2) ───────────────────
    // Format: [BENCH_CYCLE] scenario=X rows=N pct=P cycle=C mode=M ms=T
    // Enables external analysis (histograms, trend detection) without changing
    // the human-readable tables below.
    for r in results {
        eprintln!(
            "[BENCH_CYCLE] scenario={} rows={} pct={:.2} cycle={} mode={} ms={:.3}{}",
            r.scenario,
            r.table_size,
            r.change_pct,
            r.cycle,
            r.mode,
            r.refresh_ms,
            if let Some(ref p) = r.profile {
                format!(
                    " decision={:.2} gen_build={:.2} merge={:.2} cleanup={:.2} path={}",
                    p.decision_ms, p.generate_ms, p.merge_ms, p.cleanup_ms, p.path
                )
            } else {
                String::new()
            },
        );
    }
    eprintln!();

    println!();
    println!(
        "╔══════════════════════════════════════════════════════════════════════════════════════╗"
    );
    println!("║                    pg_trickle Refresh Benchmark Results                      ║");
    println!(
        "╠════════════╤══════════╤════════╤═════════════╤═══════╤════════════╤═════════════════╣"
    );
    println!(
        "║ Scenario   │ Rows     │ Chg %  │ Mode        │ Cycle │ Refresh ms │ ST Rows         ║"
    );
    println!(
        "╠════════════╪══════════╪════════╪═════════════╪═══════╪════════════╪═════════════════╣"
    );

    for r in results {
        println!(
            "║ {:10} │ {:>8} │ {:>5.0}% │ {:11} │ {:>5} │ {:>10.1} │ {:>15} ║",
            r.scenario,
            r.table_size,
            r.change_pct * 100.0,
            r.mode,
            r.cycle,
            r.refresh_ms,
            r.st_row_count,
        );
    }
    println!(
        "╚════════════╧══════════╧════════╧═════════════╧═══════╧════════════╧═════════════════╝"
    );
    println!();

    // Print summary: avg refresh time per (scenario, size, rate, mode)
    print_summary(results);

    // I-4: Write JSON results for cross-run comparison
    write_results_json(results);
}

/// Benchmark summary grouped by scenario key.
type SummaryMap = std::collections::BTreeMap<(String, usize, String), (Vec<f64>, Vec<f64>)>;

fn print_summary(results: &[BenchResult]) {
    println!(
        "┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐"
    );
    println!(
        "│                                    Summary (avg ms per cycle)                                                          │"
    );
    println!(
        "├────────────┬──────────┬────────┬─────────────────┬──────────────────────┬─────────┬─────────┬─────────┬─────────┤"
    );
    println!(
        "│ Scenario   │ Rows     │ Chg %  │ FULL avg ms     │ INCR avg ms          │ INCR c1 │ INCR 2+ │ INCR med│ INCR P95│"
    );
    println!(
        "├────────────┼──────────┼────────┼─────────────────┼──────────────────────┼─────────┼─────────┼─────────┼─────────┤"
    );

    // Group results by (scenario, size, rate)
    let mut groups: SummaryMap = std::collections::BTreeMap::new();

    for r in results {
        let key = (
            r.scenario.clone(),
            r.table_size,
            format!("{:.0}%", r.change_pct * 100.0),
        );
        let entry = groups.entry(key).or_insert_with(|| (vec![], vec![]));
        if r.mode == "FULL" {
            entry.0.push(r.refresh_ms);
        } else {
            entry.1.push(r.refresh_ms);
        }
    }

    for ((scenario, size, rate), (full_times, inc_times)) in &groups {
        let full_avg = if full_times.is_empty() {
            0.0
        } else {
            full_times.iter().sum::<f64>() / full_times.len() as f64
        };
        let inc_avg = if inc_times.is_empty() {
            0.0
        } else {
            inc_times.iter().sum::<f64>() / inc_times.len() as f64
        };
        let speedup = if inc_avg > 0.0 {
            format!("{:.1}x", full_avg / inc_avg)
        } else {
            "N/A".to_string()
        };

        // Compute cycle-1 vs cycle-2+ breakdown for DIFFERENTIAL
        let (c1_str, c2n_str) = if inc_times.len() >= 2 {
            let c1 = inc_times[0];
            let c2n_avg = inc_times[1..].iter().sum::<f64>() / (inc_times.len() - 1) as f64;
            (format!("{c1:>7.1}"), format!("{c2n_avg:>7.1}"))
        } else if inc_times.len() == 1 {
            (format!("{:>7.1}", inc_times[0]), "    N/A".to_string())
        } else {
            ("    N/A".to_string(), "    N/A".to_string())
        };

        // Compute median for DIFFERENTIAL
        let inc_median_str = if !inc_times.is_empty() {
            let mut sorted = inc_times.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let median = if sorted.len() % 2 == 0 {
                (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
            } else {
                sorted[sorted.len() / 2]
            };
            format!("{median:>7.1}")
        } else {
            "    N/A".to_string()
        };

        // Compute P95 for DIFFERENTIAL
        let inc_p95_str = if !inc_times.is_empty() {
            let mut sorted = inc_times.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let p95 = percentile(&sorted, 95.0);
            format!("{p95:>7.1}")
        } else {
            "    N/A".to_string()
        };

        println!(
            "│ {:10} │ {:>8} │ {:>6} │ {:>10.1}       │ {:>10.1} ({:>6}) │ {} │ {} │ {} │ {} │",
            scenario,
            size,
            rate,
            full_avg,
            inc_avg,
            speedup,
            c1_str,
            c2n_str,
            inc_median_str,
            inc_p95_str,
        );
    }

    println!(
        "└────────────┴──────────┴────────┴─────────────────┴──────────────────────┴─────────┴─────────┴─────────┴─────────┘"
    );
    println!();

    // Print per-phase timing breakdown for DIFFERENTIAL results
    print_phase_breakdown(results);
}

/// Per-phase timing breakdown for DIFFERENTIAL refreshes.
///
/// Extracts `[PGS_PROFILE]` data from results and displays average
/// decision / generate+build / merge / cleanup breakdown per scenario.
fn print_phase_breakdown(results: &[BenchResult]) {
    // Collect profile data grouped by (scenario, size, rate)
    let mut groups: std::collections::BTreeMap<(String, usize, String), Vec<&ProfileData>> =
        std::collections::BTreeMap::new();

    for r in results {
        if r.mode == "DIFFERENTIAL"
            && let Some(ref p) = r.profile
        {
            let key = (
                r.scenario.clone(),
                r.table_size,
                format!("{:.0}%", r.change_pct * 100.0),
            );
            groups.entry(key).or_default().push(p);
        }
    }

    if groups.is_empty() {
        return;
    }

    println!(
        "┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐"
    );
    println!(
        "│                     Per-Phase Timing Breakdown (DIFFERENTIAL avg ms)                                 │"
    );
    println!(
        "├────────────┬──────────┬────────┬──────────┬───────────────┬───────────┬──────────┬──────────────────┤"
    );
    println!(
        "│ Scenario   │ Rows     │ Chg %  │ Decision │ Gen+Build     │ Merge     │ Cleanup  │ Path             │"
    );
    println!(
        "├────────────┼──────────┼────────┼──────────┼───────────────┼───────────┼──────────┼──────────────────┤"
    );

    for ((scenario, size, rate), profiles) in &groups {
        let n = profiles.len() as f64;
        let avg =
            |f: fn(&ProfileData) -> f64| -> f64 { profiles.iter().map(|p| f(p)).sum::<f64>() / n };

        let decision = avg(|p| p.decision_ms);
        let generate = avg(|p| p.generate_ms);
        let merge = avg(|p| p.merge_ms);
        let cleanup = avg(|p| p.cleanup_ms);

        // Determine dominant path (most common)
        let hit_count = profiles.iter().filter(|p| p.path == "cache_hit").count();
        let path = if hit_count > profiles.len() / 2 {
            "cache_hit"
        } else {
            "cache_miss"
        };

        println!(
            "│ {:10} │ {:>8} │ {:>6} │ {:>8.2} │ {:>13.2} │ {:>9.2} │ {:>8.2} │ {:16} │",
            scenario, size, rate, decision, generate, merge, cleanup, path,
        );
    }

    println!(
        "└────────────┴──────────┴────────┴──────────┴───────────────┴───────────┴──────────┴──────────────────┘"
    );
    println!();
}

// ── Benchmark runner ───────────────────────────────────────────────────

/// Whether EXPLAIN ANALYZE plan capture is enabled via `PGS_BENCH_EXPLAIN=true`.
fn explain_capture_enabled() -> bool {
    std::env::var("PGS_BENCH_EXPLAIN")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Capture EXPLAIN ANALYZE output for a stream table's defining query.
///
/// Runs `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)` on the defining query
/// and writes the plan to `/tmp/bench_plans/<scenario>_<size>_<pct>.txt`.
async fn capture_explain_plan(
    db: &E2eDb,
    scenario_name: &str,
    table_size: usize,
    change_pct: f64,
    defining_query: &str,
) {
    let plan_dir = "/tmp/bench_plans";
    let _ = std::fs::create_dir_all(plan_dir);
    let plan_file = format!(
        "{}/{}_{}_{}pct.txt",
        plan_dir,
        scenario_name,
        table_size,
        (change_pct * 100.0) as u32
    );

    let explain_sql = format!("EXPLAIN (ANALYZE, BUFFERS) {defining_query}");
    match db.query_text(&explain_sql).await {
        Some(plan_text) => {
            let _ = std::fs::write(&plan_file, &plan_text);
            eprintln!(
                "[BENCH_EXPLAIN] scenario={} rows={} pct={:.2} plan_file={}",
                scenario_name, table_size, change_pct, plan_file
            );
            // Print first 20 lines to console for quick inspection
            for (i, line) in plan_text.lines().take(20).enumerate() {
                eprintln!("[BENCH_EXPLAIN]   {}: {}", i + 1, line);
            }
        }
        None => {
            eprintln!(
                "[BENCH_EXPLAIN] WARN: Could not capture plan for {}",
                scenario_name
            );
        }
    }
}

/// Run one full benchmark for a given (scenario, table_size, change_rate).
///
/// Creates one container with bench-tuned resource constraints, sets up
/// the schema, runs warm-up cycles, and then measures FULL/DIFFERENTIAL
/// refreshes for fair comparison. Captures `[PGS_PROFILE]` data for
/// DIFFERENTIAL cycles.
async fn run_benchmark(
    scenario: &QueryScenario,
    table_size: usize,
    change_pct: f64,
) -> Vec<BenchResult> {
    let db = E2eDb::new_bench().await.with_extension().await;
    let cid = db.container_id().to_string();

    // Create source table
    db.execute(
        "CREATE TABLE src (
             id SERIAL PRIMARY KEY,
             region TEXT NOT NULL,
             category TEXT NOT NULL,
             amount INT NOT NULL,
             score INT NOT NULL
         )",
    )
    .await;

    // Populate source
    db.execute(&bulk_insert_single(table_size)).await;

    // Create dimension table if needed
    if scenario.needs_dim {
        db.execute(create_join_table()).await;
        db.execute(populate_dim()).await;
    }

    // ANALYZE for stable query plans
    db.execute("ANALYZE src").await;
    if scenario.needs_dim {
        db.execute("ANALYZE dim").await;
    }

    let mut results = Vec::new();

    // ── FULL mode benchmark ────────────────────────────────────────
    let full_pgt_name = format!("bench_{}_full", scenario.name);
    db.create_st(&full_pgt_name, scenario.query, "1m", "FULL")
        .await;

    // Warm-up cycles (throw-away, not measured)
    for _ in 0..WARMUP_CYCLES {
        apply_changes(&db, table_size, change_pct).await;
        db.execute("ANALYZE src").await;
        db.refresh_st(&full_pgt_name).await;
    }

    // Measured cycles
    for cycle in 1..=CYCLES {
        apply_changes(&db, table_size, change_pct).await;
        db.execute("ANALYZE src").await;

        let start = Instant::now();
        db.refresh_st(&full_pgt_name).await;
        let elapsed = start.elapsed();

        let row_count = db.count(&format!("public.{full_pgt_name}")).await;

        results.push(BenchResult {
            scenario: scenario.name.to_string(),
            table_size,
            change_pct,
            mode: "FULL".to_string(),
            cycle,
            refresh_ms: elapsed.as_secs_f64() * 1000.0,
            st_row_count: row_count,
            profile: None,
        });
    }

    // Verify smoke correctness of the FULL refresh final state
    db.assert_st_matches_query(&full_pgt_name, scenario.query)
        .await;

    db.drop_st(&full_pgt_name).await;

    // ── Re-populate source for DIFFERENTIAL (to have same starting point) ──
    db.execute("TRUNCATE src RESTART IDENTITY").await;
    db.execute(&bulk_insert_single(table_size)).await;
    db.execute("ANALYZE src").await;

    // ── DIFFERENTIAL mode benchmark ─────────────────────────────────
    let inc_pgt_name = format!("bench_{}_inc", scenario.name);
    db.create_st(&inc_pgt_name, scenario.query, "1m", "DIFFERENTIAL")
        .await;

    // Warm-up cycles (throw-away, not measured)
    for _ in 0..WARMUP_CYCLES {
        apply_changes(&db, table_size, change_pct).await;
        db.execute("ANALYZE src").await;
        db.refresh_st(&inc_pgt_name).await;
    }

    // I-3: Capture EXPLAIN ANALYZE plan on first measured cycle if enabled
    if explain_capture_enabled() {
        apply_changes(&db, table_size, change_pct).await;
        db.execute("ANALYZE src").await;
        capture_explain_plan(&db, scenario.name, table_size, change_pct, scenario.query).await;
        db.refresh_st(&inc_pgt_name).await;
    }

    // Measured cycles with profile capture
    for cycle in 1..=CYCLES {
        apply_changes(&db, table_size, change_pct).await;
        db.execute("ANALYZE src").await;

        let start = Instant::now();
        db.refresh_st(&inc_pgt_name).await;
        let elapsed = start.elapsed();

        let row_count = db.count(&format!("public.{inc_pgt_name}")).await;

        // Capture [PGS_PROFILE] from container logs
        let profile = extract_last_profile(&cid).await;

        results.push(BenchResult {
            scenario: scenario.name.to_string(),
            table_size,
            change_pct,
            mode: "DIFFERENTIAL".to_string(),
            cycle,
            refresh_ms: elapsed.as_secs_f64() * 1000.0,
            st_row_count: row_count,
            profile,
        });
    }

    // Verify smoke correctness of the final ST state
    // Note: since DIFFERENTIAL mode is the last one run, this verifies
    // that after multi-cycle DML, the ST correctly holds the expected multiset.
    db.assert_st_matches_query(&inc_pgt_name, scenario.query)
        .await;

    results
}

// ── Individual benchmark tests ─────────────────────────────────────────
//
// Each test is #[ignore] so it doesn't run in normal CI.
// Run all benches: cargo test --test e2e_bench_tests --features pg18 -- --ignored --nocapture

#[tokio::test]
#[ignore]
async fn bench_scan_10k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[0]; // scan
    let results = run_benchmark(s, 10_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_scan_10k_10pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[0];
    let results = run_benchmark(s, 10_000, 0.10).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_scan_10k_50pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[0];
    let results = run_benchmark(s, 10_000, 0.50).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_filter_10k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[1]; // filter
    let results = run_benchmark(s, 10_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_aggregate_10k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[2]; // aggregate
    let results = run_benchmark(s, 10_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_join_10k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[3]; // join
    let results = run_benchmark(s, 10_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_join_agg_10k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[4]; // join_agg
    let results = run_benchmark(s, 10_000, 0.01).await;
    print_results_table(&results);
}

// ── I-7: Window / lateral / CTE / UNION ALL benchmarks (10K) ──────────

#[tokio::test]
#[ignore]
async fn bench_window_10k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[5]; // window
    let results = run_benchmark(s, 10_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_lateral_10k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[6]; // lateral
    let results = run_benchmark(s, 10_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_lateral_100k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[6]; // lateral
    let results = run_benchmark(s, 100_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_cte_10k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[7]; // cte
    let results = run_benchmark(s, 10_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_union_all_10k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[8]; // union_all
    let results = run_benchmark(s, 10_000, 0.01).await;
    print_results_table(&results);
}

// ── 100K row benchmarks ────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn bench_scan_100k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[0];
    let results = run_benchmark(s, 100_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_scan_100k_10pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[0];
    let results = run_benchmark(s, 100_000, 0.10).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_scan_100k_50pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[0];
    let results = run_benchmark(s, 100_000, 0.50).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_aggregate_100k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[2];
    let results = run_benchmark(s, 100_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_aggregate_100k_10pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[2];
    let results = run_benchmark(s, 100_000, 0.10).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_join_100k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[3]; // join
    let results = run_benchmark(s, 100_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_join_agg_100k_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[4];
    let results = run_benchmark(s, 100_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_join_agg_100k_10pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[4];
    let results = run_benchmark(s, 100_000, 0.10).await;
    print_results_table(&results);
}

// ── 1M row benchmarks (I-6) ───────────────────────────────────────────
//
// Tests at production-scale table sizes. The delta is larger (10K changes
// at 1% of 1M rows) and MERGE touches more stream table rows.
// Run separately from the standard suite due to longer run time.

#[tokio::test]
#[ignore]
async fn bench_scan_1m_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[0]; // scan
    let results = run_benchmark(s, 1_000_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_filter_1m_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[1]; // filter
    let results = run_benchmark(s, 1_000_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_aggregate_1m_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[2]; // aggregate
    let results = run_benchmark(s, 1_000_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_join_1m_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[3]; // join
    let results = run_benchmark(s, 1_000_000, 0.01).await;
    print_results_table(&results);
}

#[tokio::test]
#[ignore]
async fn bench_join_agg_1m_1pct() {
    let scenarios = query_scenarios();
    let s = &scenarios[4]; // join_agg
    let results = run_benchmark(s, 1_000_000, 0.01).await;
    print_results_table(&results);
}

// ── Large matrix benchmark (I-6) ──────────────────────────────────────
//
// Includes the 1M-row tier in addition to 10K and 100K. Uses only 1% change
// rate at 1M to keep run time reasonable. Expect ~45-90 minutes total.

#[tokio::test]
#[ignore]
async fn bench_large_matrix() {
    let scenarios = query_scenarios();
    let mut all_results = Vec::new();
    let large_sizes: &[usize] = &[10_000, 100_000, 1_000_000];
    let large_rates: &[f64] = &[0.01, 0.10];

    for &table_size in large_sizes {
        // At 1M rows, only test 1% change rate to keep run time sane
        let rates: &[f64] = if table_size >= 1_000_000 {
            &[0.01]
        } else {
            large_rates
        };
        for &change_pct in rates {
            for scenario in &scenarios {
                if let Some(max) = scenario.max_table_size
                    && table_size > max
                {
                    eprintln!(
                        "↷ Skipping: {} | {}rows (capped at {}rows — FULL-refresh O(n²))",
                        scenario.name, table_size, max,
                    );
                    continue;
                }
                eprintln!(
                    "▶ Benchmarking: {} | {} rows | {:.0}% changes ...",
                    scenario.name,
                    table_size,
                    change_pct * 100.0,
                );
                let results = run_benchmark(scenario, table_size, change_pct).await;
                all_results.extend(results);
            }
        }
    }

    print_results_table(&all_results);
}

// ── Full matrix benchmark ──────────────────────────────────────────────
//
// Runs ALL combinations of dimensions: 9 queries × 2 sizes × 3 rates,
// skipping scenarios whose `max_table_size` cap would be exceeded.
// Each run does CYCLES full + CYCLES differential refreshes.
// Expect ~30-60 minutes depending on hardware.
//
// Scenarios capped at 10K rows (lateral, window) are excluded at 100K
// because their FULL-refresh cost is O(n²) / prohibitively slow in CI.

#[tokio::test]
#[ignore]
async fn bench_full_matrix() {
    let scenarios = query_scenarios();
    let mut all_results = Vec::new();

    for table_size in TABLE_SIZES {
        for change_pct in CHANGE_RATES {
            for scenario in &scenarios {
                if let Some(max) = scenario.max_table_size
                    && *table_size > max
                {
                    eprintln!(
                        "↷ Skipping: {} | {}rows (capped at {}rows — FULL-refresh O(n²))",
                        scenario.name, table_size, max,
                    );
                    continue;
                }
                eprintln!(
                    "▶ Benchmarking: {} | {}rows | {:.0}% changes ...",
                    scenario.name,
                    table_size,
                    change_pct * 100.0,
                );
                let results = run_benchmark(scenario, *table_size, *change_pct).await;
                all_results.extend(results);
            }
        }
    }

    print_results_table(&all_results);
}

// ── NO_DATA refresh latency benchmark ──────────────────────────────────

#[tokio::test]
#[ignore]
async fn bench_no_data_refresh_latency() {
    let db = E2eDb::new_bench().await.with_extension().await;

    db.execute("CREATE TABLE src_nd (id SERIAL PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO src_nd (val) SELECT i FROM generate_series(1, 10000) AS s(i)")
        .await;

    db.create_st("nd_st", "SELECT id, val FROM src_nd", "1m", "DIFFERENTIAL")
        .await;

    // No changes → refresh should be near-zero cost
    let mut times = Vec::new();
    for _ in 0..10 {
        let start = Instant::now();
        db.refresh_st("nd_st").await;
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    let avg = times.iter().sum::<f64>() / times.len() as f64;
    let max_ms = times.iter().cloned().fold(0.0f64, f64::max);

    println!();
    println!("┌──────────────────────────────────────────────┐");
    println!("│ NO_DATA Refresh Latency (10 iterations)      │");
    println!("├──────────────────────────────────────────────┤");
    println!("│ Avg: {:>8.2} ms                             │", avg);
    println!("│ Max: {:>8.2} ms                             │", max_ms);
    println!("│ Target: < 10 ms                              │");
    println!(
        "│ Status: {}                              │",
        if avg < 10.0 {
            "✅ PASS"
        } else {
            "⚠️ SLOW"
        }
    );
    println!("└──────────────────────────────────────────────┘");
    println!();

    // Verify smoke correctness
    db.assert_st_matches_query("nd_st", "SELECT id, val FROM src_nd")
        .await;
}
// ═══════════════════════════════════════════════════════════════════════
// F50 / G7.3 — Covering index overhead benchmark
//
// Compares change buffer query performance with and without the INCLUDE
// (action) clause on the (lsn, pk_hash, change_id) index.
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_covering_index_overhead() {
    let db = E2eDb::new_bench().await.with_extension().await;

    // Create a source table and stream table to get the change buffer
    db.execute("CREATE TABLE ci_src (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO ci_src (grp, val) \
         SELECT CASE (i % 10) WHEN 0 THEN 'a' WHEN 1 THEN 'b' WHEN 2 THEN 'c' \
                WHEN 3 THEN 'd' WHEN 4 THEN 'e' ELSE 'f' END, \
                (i * 17 + 13) % 10000 \
         FROM generate_series(1, 50000) AS s(i)",
    )
    .await;

    let q = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM ci_src GROUP BY grp";
    db.create_st("ci_st", q, "1m", "DIFFERENTIAL").await;

    // Find the change buffer table OID
    let src_oid: i64 = db
        .query_scalar(
            "SELECT oid::bigint FROM pg_class WHERE relname = 'ci_src' AND relnamespace = 'public'::regnamespace",
        )
        .await;
    let buf_table = format!("pgtrickle_changes.changes_{}", src_oid);

    // Generate a significant number of changes in the buffer
    // (don't refresh — let them accumulate)
    for _round in 0..3 {
        db.execute("UPDATE ci_src SET val = val + 1 WHERE id <= 5000")
            .await;
    }

    let change_count: i64 = db
        .query_scalar(&format!("SELECT COUNT(*) FROM {buf_table}"))
        .await;
    println!();
    println!("Change buffer has {} pending rows", change_count);

    // Typical change buffer query pattern (mirrors what refresh does):
    let bench_query = format!(
        "SELECT pk_hash, action, change_id \
         FROM {buf_table} \
         WHERE lsn > '0/0' \
         ORDER BY pk_hash, change_id"
    );

    // ── Phase 1: WITH covering index (default) ───────────────────

    // Warm up
    for _ in 0..3 {
        db.execute(&format!("SELECT COUNT(*) FROM ({}) sub", bench_query))
            .await;
    }

    let mut with_include_ms = Vec::new();
    for _ in 0..20 {
        let start = Instant::now();
        db.execute(&format!("SELECT COUNT(*) FROM ({}) sub", bench_query))
            .await;
        with_include_ms.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    // ── Phase 2: WITHOUT covering index ──────────────────────────

    // Drop the covering index and create a plain one
    let idx_name = format!("idx_changes_{}_lsn_pk_cid", src_oid);
    db.execute(&format!(
        "DROP INDEX IF EXISTS pgtrickle_changes.{idx_name}"
    ))
    .await;
    db.execute(&format!(
        "CREATE INDEX {idx_name}_plain ON {buf_table} (lsn, pk_hash, change_id)"
    ))
    .await;
    db.execute("ANALYZE").await;

    // Warm up
    for _ in 0..3 {
        db.execute(&format!("SELECT COUNT(*) FROM ({}) sub", bench_query))
            .await;
    }

    let mut without_include_ms = Vec::new();
    for _ in 0..20 {
        let start = Instant::now();
        db.execute(&format!("SELECT COUNT(*) FROM ({}) sub", bench_query))
            .await;
        without_include_ms.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    // ── Results ──────────────────────────────────────────────────

    let avg_with = with_include_ms.iter().sum::<f64>() / with_include_ms.len() as f64;
    let avg_without = without_include_ms.iter().sum::<f64>() / without_include_ms.len() as f64;
    let p95_with = percentile(&with_include_ms, 0.95);
    let p95_without = percentile(&without_include_ms, 0.95);
    let diff_pct = ((avg_with - avg_without) / avg_without) * 100.0;

    println!();
    println!("┌─────────────────────────────────────────────────────────┐");
    println!("│ F50: Covering Index (INCLUDE action) Overhead Benchmark │");
    println!("├─────────────────────────────────────────────────────────┤");
    println!(
        "│ Change buffer rows: {:>8}                            │",
        change_count
    );
    println!("│                                                         │");
    println!("│           WITH INCLUDE    WITHOUT INCLUDE               │");
    println!(
        "│  Avg:     {:>8.2} ms     {:>8.2} ms                   │",
        avg_with, avg_without
    );
    println!(
        "│  P95:     {:>8.2} ms     {:>8.2} ms                   │",
        p95_with, p95_without
    );
    println!("│                                                         │");
    println!(
        "│  Overhead: {:>+.1}%                                       │",
        diff_pct
    );
    println!(
        "│  Verdict:  {}                                      │",
        if diff_pct.abs() < 15.0 {
            "✅ Acceptable"
        } else if diff_pct > 0.0 {
            "⚠️ Significant overhead"
        } else {
            "✅ INCLUDE is faster"
        }
    );
    println!("└─────────────────────────────────────────────────────────┘");
    println!();

    db.assert_st_matches_query(
        "ci_st",
        "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM ci_src GROUP BY grp",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════
// E: CDC Trigger Overhead Benchmark
//
// Measures write-side overhead introduced by row-level AFTER triggers used
// for change-data-capture. Compares INSERT/UPDATE/DELETE throughput on a
// table that is a stream table source (has CDC triggers) versus an
// identical table with no triggers.
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn bench_cdc_trigger_overhead() {
    let db = E2eDb::new_bench().await.with_extension().await;

    let rows = 50_000usize;
    let batch = 5_000usize;
    let iterations = 10usize;

    // ── Setup: two identical tables ──────────────────────────────

    // Table WITH CDC triggers (source of a stream table)
    db.execute("CREATE TABLE cdc_src (id SERIAL PRIMARY KEY, region TEXT, amount INT)")
        .await;
    db.execute(&format!(
        "INSERT INTO cdc_src (region, amount) \
         SELECT CASE (i % 5) WHEN 0 THEN 'n' WHEN 1 THEN 's' WHEN 2 THEN 'e' \
                WHEN 3 THEN 'w' ELSE 'c' END, (i * 17) % 10000 \
         FROM generate_series(1, {rows}) AS s(i)"
    ))
    .await;

    // Create a stream table so CDC triggers are installed on cdc_src
    db.create_st(
        "cdc_bench_st",
        "SELECT id, region, amount FROM cdc_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Table WITHOUT CDC triggers (control)
    db.execute("CREATE TABLE nocdc_src (id SERIAL PRIMARY KEY, region TEXT, amount INT)")
        .await;
    db.execute(&format!(
        "INSERT INTO nocdc_src (region, amount) \
         SELECT CASE (i % 5) WHEN 0 THEN 'n' WHEN 1 THEN 's' WHEN 2 THEN 'e' \
                WHEN 3 THEN 'w' ELSE 'c' END, (i * 17) % 10000 \
         FROM generate_series(1, {rows}) AS s(i)"
    ))
    .await;

    db.execute("ANALYZE cdc_src").await;
    db.execute("ANALYZE nocdc_src").await;

    // Verify CDC trigger exists on the source
    let trig_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_trigger t \
             JOIN pg_class c ON c.oid = t.tgrelid \
                         WHERE c.relname = 'cdc_src' \
                             AND (t.tgname LIKE 'pg_trickle_%' OR t.tgname LIKE 'pgt_%')",
        )
        .await;
    assert!(
        trig_count > 0,
        "CDC triggers should be installed on cdc_src"
    );

    // ── Benchmark: INSERT ────────────────────────────────────────

    let mut cdc_insert_ms = Vec::new();
    let mut nocdc_insert_ms = Vec::new();

    for i in 0..iterations {
        let offset = rows + i * batch;

        let start = Instant::now();
        db.execute(&format!(
            "INSERT INTO cdc_src (region, amount) \
             SELECT 'n', (i * 13) % 10000 \
             FROM generate_series(1, {batch}) AS s(i)"
        ))
        .await;
        cdc_insert_ms.push(start.elapsed().as_secs_f64() * 1000.0);

        let start = Instant::now();
        db.execute(&format!(
            "INSERT INTO nocdc_src (region, amount) \
             SELECT 'n', (i * 13) % 10000 \
             FROM generate_series(1, {batch}) AS s(i)"
        ))
        .await;
        nocdc_insert_ms.push(start.elapsed().as_secs_f64() * 1000.0);

        // Drain the change buffer periodically to avoid bloat
        if i % 3 == 2 {
            db.refresh_st("cdc_bench_st").await;
        }

        let _ = offset;
    }

    // ── Benchmark: UPDATE ────────────────────────────────────────

    let mut cdc_update_ms = Vec::new();
    let mut nocdc_update_ms = Vec::new();

    for _ in 0..iterations {
        let start = Instant::now();
        db.execute(&format!(
            "UPDATE cdc_src SET amount = amount + 1 \
             WHERE id IN (SELECT id FROM cdc_src ORDER BY id LIMIT {batch})"
        ))
        .await;
        cdc_update_ms.push(start.elapsed().as_secs_f64() * 1000.0);

        let start = Instant::now();
        db.execute(&format!(
            "UPDATE nocdc_src SET amount = amount + 1 \
             WHERE id IN (SELECT id FROM nocdc_src ORDER BY id LIMIT {batch})"
        ))
        .await;
        nocdc_update_ms.push(start.elapsed().as_secs_f64() * 1000.0);

        db.refresh_st("cdc_bench_st").await;
    }

    // ── Benchmark: DELETE ────────────────────────────────────────

    let mut cdc_delete_ms = Vec::new();
    let mut nocdc_delete_ms = Vec::new();

    for i in 0..iterations {
        // Delete a batch of rows (from the end to avoid conflicting with updates)
        let start = Instant::now();
        db.execute(&format!(
            "DELETE FROM cdc_src \
             WHERE id IN (SELECT id FROM cdc_src ORDER BY id DESC LIMIT {})",
            batch / 5
        ))
        .await;
        cdc_delete_ms.push(start.elapsed().as_secs_f64() * 1000.0);

        let start = Instant::now();
        db.execute(&format!(
            "DELETE FROM nocdc_src \
             WHERE id IN (SELECT id FROM nocdc_src ORDER BY id DESC LIMIT {})",
            batch / 5
        ))
        .await;
        nocdc_delete_ms.push(start.elapsed().as_secs_f64() * 1000.0);

        if i % 3 == 2 {
            db.refresh_st("cdc_bench_st").await;
        }
    }

    // ── Results ──────────────────────────────────────────────────

    let avg = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
    let overhead = |cdc: &[f64], nocdc: &[f64]| {
        let a = avg(cdc);
        let b = avg(nocdc);
        if b > 0.0 { ((a - b) / b) * 100.0 } else { 0.0 }
    };

    let ins_oh = overhead(&cdc_insert_ms, &nocdc_insert_ms);
    let upd_oh = overhead(&cdc_update_ms, &nocdc_update_ms);
    let del_oh = overhead(&cdc_delete_ms, &nocdc_delete_ms);
    let avg_oh = (ins_oh + upd_oh + del_oh) / 3.0;

    println!();
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ E: CDC Trigger Overhead Benchmark                        │");
    println!("├───────────────────────────────────────────────────────────┤");
    println!(
        "│ Config: {} base rows, {} rows/batch, {} iterations       │",
        rows, batch, iterations
    );
    println!("│                                                           │");
    println!("│ Operation    CDC (avg)    No-CDC (avg)    Overhead        │");
    println!(
        "│ INSERT    {:>8.2} ms    {:>8.2} ms    {:>+6.1}%          │",
        avg(&cdc_insert_ms),
        avg(&nocdc_insert_ms),
        ins_oh,
    );
    println!(
        "│ UPDATE    {:>8.2} ms    {:>8.2} ms    {:>+6.1}%          │",
        avg(&cdc_update_ms),
        avg(&nocdc_update_ms),
        upd_oh,
    );
    println!(
        "│ DELETE    {:>8.2} ms    {:>8.2} ms    {:>+6.1}%          │",
        avg(&cdc_delete_ms),
        avg(&nocdc_delete_ms),
        del_oh,
    );
    println!("│                                                           │");
    println!(
        "│ Average overhead: {:>+.1}%                                 │",
        avg_oh
    );
    println!(
        "│ Verdict: {}                                           │",
        if avg_oh < 20.0 {
            "✅ Acceptable (<20%)"
        } else if avg_oh < 50.0 {
            "⚠️  Moderate (20-50%)"
        } else {
            "❌ High (>50%)       "
        }
    );
    println!("└───────────────────────────────────────────────────────────┘");
    println!();

    // Verify smoke correctness after operations
    db.refresh_st("cdc_bench_st").await;
    db.assert_st_matches_query("cdc_bench_st", "SELECT id, region, amount FROM cdc_src")
        .await;
}

// ═══════════════════════════════════════════════════════════════════════
// B3: Statement-Level vs Row-Level CDC Trigger Write-Side Benchmark
//
// Measures the write-side overhead reduction from statement-level triggers
// (v0.4.0 default) vs the legacy row-level triggers.
//
// Matrix: 3 table widths × 3 DML types × bulk + single-row = 18 cells.
//
// Table widths:
//   narrow  — 3 columns (id PK, grp TEXT, val INT)
//   medium  — 8 columns (id PK, a–f TEXT/INT, val INT)
//   wide    — 20 columns (id PK + 19 data columns)
//
// DML types: bulk INSERT (5 000 rows), bulk UPDATE (5 000 rows), single-row
// INSERT (1 row).
//
// Expected outcome: 50–80% write overhead reduction for bulk DML with
// statement-level triggers; neutral for single-row DML.
//
// Run explicitly:
//   cargo test --test e2e_bench_tests -- --ignored bench_stmt_vs_row_cdc_matrix --nocapture
// ═══════════════════════════════════════════════════════════════════════

/// Schema descriptor for one table-width variant.
struct TableWidth {
    name: &'static str,
    cols: usize,
    create_ddl: &'static str,
    insert_bulk_sql: &'static str,
    /// Stream-table defining query (must be a SELECT on the source).
    st_query: &'static str,
}

fn table_widths() -> Vec<TableWidth> {
    vec![
        TableWidth {
            name: "narrow",
            cols: 3,
            create_ddl: "CREATE TABLE {src} (id SERIAL PRIMARY KEY, grp TEXT NOT NULL, val INT NOT NULL)",
            insert_bulk_sql: "INSERT INTO {src} (grp, val) \
                SELECT CASE (i%5) WHEN 0 THEN 'a' WHEN 1 THEN 'b' WHEN 2 THEN 'c' \
                WHEN 3 THEN 'd' ELSE 'e' END, (i*17)%10000 \
                FROM generate_series(1,{n}) AS s(i)",
            st_query: "SELECT id, grp, val FROM {src}",
        },
        TableWidth {
            name: "medium",
            cols: 8,
            create_ddl: "CREATE TABLE {src} (id SERIAL PRIMARY KEY, \
                a TEXT NOT NULL, b TEXT NOT NULL, c TEXT NOT NULL, \
                d INT NOT NULL, e INT NOT NULL, f INT NOT NULL, val INT NOT NULL)",
            insert_bulk_sql: "INSERT INTO {src} (a,b,c,d,e,f,val) \
                SELECT CASE (i%3) WHEN 0 THEN 'x' WHEN 1 THEN 'y' ELSE 'z' END, \
                       CASE (i%4) WHEN 0 THEN 'p' WHEN 1 THEN 'q' WHEN 2 THEN 'r' ELSE 's' END, \
                       CASE (i%5) WHEN 0 THEN 'a' WHEN 1 THEN 'b' WHEN 2 THEN 'c' WHEN 3 THEN 'd' ELSE 'e' END, \
                       (i*3)%100, (i*7)%100, (i*13)%100, (i*17)%10000 \
                FROM generate_series(1,{n}) AS s(i)",
            st_query: "SELECT id, a, b, c, d, e, f, val FROM {src}",
        },
        TableWidth {
            name: "wide",
            cols: 20,
            create_ddl: "CREATE TABLE {src} (id SERIAL PRIMARY KEY, \
                c1 TEXT, c2 TEXT, c3 TEXT, c4 TEXT, c5 TEXT, \
                c6 INT, c7 INT, c8 INT, c9 INT, c10 INT, \
                c11 TEXT, c12 TEXT, c13 TEXT, c14 TEXT, c15 TEXT, \
                c16 INT, c17 INT, c18 INT, c19 INT)",
            insert_bulk_sql: "INSERT INTO {src} (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19) \
                SELECT CASE (i%3) WHEN 0 THEN 'x' WHEN 1 THEN 'y' ELSE 'z' END, \
                       CASE (i%4) WHEN 0 THEN 'p' WHEN 1 THEN 'q' WHEN 2 THEN 'r' ELSE 's' END, \
                       CASE (i%5) WHEN 0 THEN 'a' WHEN 1 THEN 'b' WHEN 2 THEN 'c' WHEN 3 THEN 'd' ELSE 'e' END, \
                       CASE (i%2) WHEN 0 THEN 'foo' ELSE 'bar' END, \
                       CASE (i%6) WHEN 0 THEN 'alpha' WHEN 1 THEN 'beta' WHEN 2 THEN 'gamma' \
                       WHEN 3 THEN 'delta' WHEN 4 THEN 'epsilon' ELSE 'zeta' END, \
                       (i*2)%100, (i*3)%100, (i*5)%100, (i*7)%100, (i*11)%100, \
                       CASE (i%3) WHEN 0 THEN 'u' WHEN 1 THEN 'v' ELSE 'w' END, \
                       CASE (i%4) WHEN 0 THEN 'i' WHEN 1 THEN 'j' WHEN 2 THEN 'k' ELSE 'l' END, \
                       CASE (i%5) WHEN 0 THEN 'f' WHEN 1 THEN 'g' WHEN 2 THEN 'h' WHEN 3 THEN 'i' ELSE 'j' END, \
                       CASE (i%2) WHEN 0 THEN 'cat' ELSE 'dog' END, \
                       CASE (i%3) WHEN 0 THEN 'red' WHEN 1 THEN 'green' ELSE 'blue' END, \
                       (i*13)%100, (i*17)%100, (i*19)%100, (i*23)%100 \
                FROM generate_series(1,{n}) AS s(i)",
            st_query: "SELECT id, c1, c6, c11, c16 FROM {src}",
        },
    ]
}

/// A single CDC trigger mode benchmark cell result.
#[derive(Clone)]
struct TriggerBenchResult {
    width: String,
    mode: String,
    dml: String,
    batch_size: usize,
    timings_ms: Vec<f64>,
}

impl TriggerBenchResult {
    fn avg(&self) -> f64 {
        self.timings_ms.iter().sum::<f64>() / self.timings_ms.len() as f64
    }
    fn p50(&self) -> f64 {
        let mut v = self.timings_ms.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        percentile(&v, 50.0)
    }
    fn p95(&self) -> f64 {
        let mut v = self.timings_ms.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        percentile(&v, 95.0)
    }
}

fn speedup_str(row_avg: f64, stmt_avg: f64) -> String {
    if row_avg <= 0.0 || stmt_avg <= 0.0 {
        return "   n/a".to_string();
    }
    let ratio = row_avg / stmt_avg;
    if ratio >= 1.0 {
        format!("{:.2}x⬇", ratio)
    } else {
        format!("{:.2}x⬆", ratio)
    }
}

/// Run the full B3 benchmark matrix.
///
/// For each table width, enables row-level mode and statement-level mode in
/// turn, measures bulk INSERT / bulk UPDATE / single INSERT, then prints the
/// comparison table.
async fn run_stmt_vs_row_matrix() {
    const BASE_ROWS: usize = 50_000;
    const BULK_BATCH: usize = 5_000;
    const SINGLE_BATCH: usize = 1;
    const ITERATIONS: usize = 10;
    const WARMUP: usize = 2;

    let mut all_results: Vec<TriggerBenchResult> = Vec::new();

    for width in table_widths() {
        eprintln!(
            "▶ B3: {} table ({} cols) — row vs statement mode ...",
            width.name, width.cols
        );

        for trigger_mode in &["row", "statement"] {
            let db = E2eDb::new_bench().await.with_extension().await;

            // Switch to the requested trigger mode
            db.alter_system_set_and_wait(
                "pg_trickle.cdc_trigger_mode",
                &format!("'{trigger_mode}'"),
                trigger_mode,
            )
            .await;

            let src = format!("cdc_b3_{}", width.name);
            let st_name = format!("b3_st_{}", width.name);

            // Create source table
            let ddl = width.create_ddl.replace("{src}", &src);
            db.execute(&ddl).await;

            // Populate base rows
            let ins = width
                .insert_bulk_sql
                .replace("{src}", &src)
                .replace("{n}", &BASE_ROWS.to_string());
            db.execute(&ins).await;
            db.execute(&format!("ANALYZE {src}")).await;

            // Create stream table (installs CDC trigger in current mode)
            let st_query = width.st_query.replace("{src}", &src);
            db.create_st(&st_name, &st_query, "1m", "DIFFERENTIAL")
                .await;

            // ── DML helper closures ─────────────────────────────────────

            // bulk INSERT
            let bulk_ins = |n: usize| {
                width
                    .insert_bulk_sql
                    .replace("{src}", &src)
                    .replace("{n}", &n.to_string())
            };

            // bulk UPDATE of the first `n` rows — update c6/d/val depending on width
            let bulk_upd_sql = format!(
                "UPDATE {src} SET id = id WHERE id IN \
                 (SELECT id FROM {src} ORDER BY id LIMIT {BULK_BATCH})"
            );

            // ── Warm-up ─────────────────────────────────────────────────
            for _ in 0..WARMUP {
                db.execute(&bulk_ins(BULK_BATCH)).await;
                db.execute(&bulk_upd_sql).await;
                db.refresh_st(&st_name).await;
            }

            // ── Bulk INSERT timings ──────────────────────────────────────
            let mut bulk_insert_ms = Vec::new();
            for _ in 0..ITERATIONS {
                let sql = bulk_ins(BULK_BATCH);
                let start = Instant::now();
                db.execute(&sql).await;
                bulk_insert_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                db.refresh_st(&st_name).await; // drain buffer
            }

            // ── Bulk UPDATE timings ──────────────────────────────────────
            let mut bulk_update_ms = Vec::new();
            for _ in 0..ITERATIONS {
                let start = Instant::now();
                db.execute(&bulk_upd_sql).await;
                bulk_update_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                db.refresh_st(&st_name).await;
            }

            // ── Single-row INSERT timings ────────────────────────────────
            let mut single_insert_ms = Vec::new();
            for _ in 0..ITERATIONS {
                let sql = bulk_ins(SINGLE_BATCH);
                let start = Instant::now();
                db.execute(&sql).await;
                single_insert_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                db.refresh_st(&st_name).await;
            }

            all_results.push(TriggerBenchResult {
                width: width.name.to_string(),
                mode: trigger_mode.to_string(),
                dml: "bulk_insert".to_string(),
                batch_size: BULK_BATCH,
                timings_ms: bulk_insert_ms,
            });
            all_results.push(TriggerBenchResult {
                width: width.name.to_string(),
                mode: trigger_mode.to_string(),
                dml: "bulk_update".to_string(),
                batch_size: BULK_BATCH,
                timings_ms: bulk_update_ms,
            });
            all_results.push(TriggerBenchResult {
                width: width.name.to_string(),
                mode: trigger_mode.to_string(),
                dml: "single_insert".to_string(),
                batch_size: SINGLE_BATCH,
                timings_ms: single_insert_ms,
            });

            // Verify smoke correctness
            db.refresh_st(&st_name).await;
            db.assert_st_matches_query(&st_name, &st_query).await;
        }
    }

    // ── Print comparison table ─────────────────────────────────────────────

    println!();
    println!(
        "┌───────────────────────────────────────────────────────────────────────────────────────────────────┐"
    );
    println!(
        "│  B3: Statement-Level vs Row-Level CDC Trigger — Write-Side Benchmark                              │"
    );
    println!(
        "│  Base rows: 50 000 · Bulk batch: 5 000 · Iterations: 10                                          │"
    );
    println!(
        "├──────────┬───────────────┬───────────────┬──────────────────────────────────────────────────────┤"
    );
    println!(
        "│ Width    │ DML           │ Batch         │ row avg   row p50   row p95  stmt avg  stmt p50  stmt p95  Speedup │"
    );
    println!(
        "├──────────┼───────────────┼───────────────┼──────────────────────────────────────────────────────┤"
    );

    let widths = ["narrow", "medium", "wide"];
    let dmls = ["bulk_insert", "bulk_update", "single_insert"];

    for w in &widths {
        for dml in &dmls {
            let find = |mode: &str| {
                all_results
                    .iter()
                    .find(|r| r.width == *w && r.dml == *dml && r.mode == mode)
            };
            let row_r = find("row");
            let stmt_r = find("statement");

            match (row_r, stmt_r) {
                (Some(row), Some(stmt)) => {
                    let batch = row.batch_size;
                    let speedup = speedup_str(row.avg(), stmt.avg());
                    println!(
                        "│ {:8} │ {:13} │ {:>13} │ {:>8.2}  {:>8.2}  {:>8.2}  {:>8.2}  {:>8.2}  {:>8.2}  {:>7} │",
                        w,
                        dml,
                        batch,
                        row.avg(),
                        row.p50(),
                        row.p95(),
                        stmt.avg(),
                        stmt.p50(),
                        stmt.p95(),
                        speedup,
                    );
                }
                _ => {
                    println!(
                        "│ {:8} │ {:13} │         n/a │ (data missing)                                                       │",
                        w, dml
                    );
                }
            }
        }
        println!(
            "├──────────┼───────────────┼───────────────┼──────────────────────────────────────────────────────┤"
        );
    }

    println!(
        "└──────────┴───────────────┴───────────────┴──────────────────────────────────────────────────────┘"
    );

    // ── Summary verdict ────────────────────────────────────────────────────
    println!();
    println!("Summary (bulk DML speedup):");
    for w in &widths {
        for dml in &["bulk_insert", "bulk_update"] {
            let row_r = all_results
                .iter()
                .find(|r| r.width == *w && r.dml == *dml && r.mode == "row");
            let stmt_r = all_results
                .iter()
                .find(|r| r.width == *w && r.dml == *dml && r.mode == "statement");
            if let (Some(row), Some(stmt)) = (row_r, stmt_r) {
                let reduction_pct = if row.avg() > 0.0 {
                    (row.avg() - stmt.avg()) / row.avg() * 100.0
                } else {
                    0.0
                };
                let verdict = if reduction_pct >= 50.0 {
                    "✅ ≥50% reduction (better than target)"
                } else if reduction_pct >= 20.0 {
                    "✅ 20–50% reduction (within target)"
                } else if reduction_pct >= 0.0 {
                    "⚠️  <20% reduction (below target)"
                } else {
                    "❌ regression (statement slower than row)"
                };
                println!(
                    "  {w:6} × {dml:12} → row {:.2}ms  stmt {:.2}ms  {:+.1}%  {verdict}",
                    row.avg(),
                    stmt.avg(),
                    reduction_pct,
                );
            }
        }
    }
    println!();

    // Single-row DML: statement should be neutral (within ±10%)
    println!("Single-row INSERT neutrality check:");
    for w in &widths {
        let row_r = all_results
            .iter()
            .find(|r| r.width == *w && r.dml == "single_insert" && r.mode == "row");
        let stmt_r = all_results
            .iter()
            .find(|r| r.width == *w && r.dml == "single_insert" && r.mode == "statement");
        if let (Some(row), Some(stmt)) = (row_r, stmt_r) {
            let delta_pct = if row.avg() > 0.0 {
                (stmt.avg() - row.avg()) / row.avg() * 100.0
            } else {
                0.0
            };
            let verdict = if delta_pct.abs() <= 10.0 {
                "✅ neutral (±10%)"
            } else if delta_pct > 0.0 {
                "⚠️  statement slower for single-row"
            } else {
                "✅ statement faster for single-row"
            };
            println!(
                "  {w:6} → row {:.2}ms  stmt {:.2}ms  {:+.1}%  {verdict}",
                row.avg(),
                stmt.avg(),
                delta_pct,
            );
        }
    }
    println!();
}

/// B3: Statement-Level vs Row-Level CDC trigger write-side benchmark matrix.
///
/// Compares write-side overhead of `pg_trickle.cdc_trigger_mode = 'row'`
/// (legacy) vs `'statement'` (v0.4.0 default) across narrow/medium/wide
/// tables and bulk/single-row DML.
///
/// Run explicitly:
/// ```bash
/// cargo test --test e2e_bench_tests -- --ignored bench_stmt_vs_row_cdc_matrix --nocapture
/// ```
#[tokio::test]
#[ignore]
async fn bench_stmt_vs_row_cdc_matrix() {
    run_stmt_vs_row_matrix().await;
}

/// B3 quick: narrowing smoke-check — narrow table, bulk INSERT only.
///
/// Completes in ~60s instead of the full 15–20 min matrix run.
///
/// Run explicitly:
/// ```bash
/// cargo test --test e2e_bench_tests -- --ignored bench_stmt_vs_row_cdc_quick --nocapture
/// ```
#[tokio::test]
#[ignore]
async fn bench_stmt_vs_row_cdc_quick() {
    const BASE_ROWS: usize = 20_000;
    const BULK_BATCH: usize = 2_000;
    const ITERATIONS: usize = 5;
    const WARMUP: usize = 1;

    let widths = table_widths();
    let narrow = &widths[0]; // narrow only

    let mut results: [Option<TriggerBenchResult>; 2] = [None, None];

    for (idx, trigger_mode) in ["row", "statement"].iter().enumerate() {
        let db = E2eDb::new_bench().await.with_extension().await;

        db.alter_system_set_and_wait(
            "pg_trickle.cdc_trigger_mode",
            &format!("'{trigger_mode}'"),
            trigger_mode,
        )
        .await;

        let src = "b3q_src".to_string();
        let st_name = "b3q_st".to_string();
        let ddl = narrow.create_ddl.replace("{src}", &src);
        db.execute(&ddl).await;
        let ins_base = narrow
            .insert_bulk_sql
            .replace("{src}", &src)
            .replace("{n}", &BASE_ROWS.to_string());
        db.execute(&ins_base).await;
        db.execute(&format!("ANALYZE {src}")).await;
        let st_query = narrow.st_query.replace("{src}", &src);
        db.create_st(&st_name, &st_query, "1m", "DIFFERENTIAL")
            .await;

        // warm-up
        for _ in 0..WARMUP {
            let s = narrow
                .insert_bulk_sql
                .replace("{src}", &src)
                .replace("{n}", &BULK_BATCH.to_string());
            db.execute(&s).await;
            db.refresh_st(&st_name).await;
        }

        let mut ms = Vec::new();
        for _ in 0..ITERATIONS {
            let s = narrow
                .insert_bulk_sql
                .replace("{src}", &src)
                .replace("{n}", &BULK_BATCH.to_string());
            let start = Instant::now();
            db.execute(&s).await;
            ms.push(start.elapsed().as_secs_f64() * 1000.0);
            db.refresh_st(&st_name).await;
        }

        results[idx] = Some(TriggerBenchResult {
            width: "narrow".to_string(),
            mode: trigger_mode.to_string(),
            dml: "bulk_insert".to_string(),
            batch_size: BULK_BATCH,
            timings_ms: ms,
        });

        // Verify smoke correctness
        db.refresh_st(&st_name).await;
        db.assert_st_matches_query(&st_name, &st_query).await;
    }

    let row = results[0].as_ref().unwrap();
    let stmt = results[1].as_ref().unwrap();
    let reduction_pct = if row.avg() > 0.0 {
        (row.avg() - stmt.avg()) / row.avg() * 100.0
    } else {
        0.0
    };

    println!();
    println!("┌──────────────────────────────────────────────────────────┐");
    println!("│  B3 Quick: Statement vs Row CDC — Narrow Bulk INSERT      │");
    println!("├──────────────────────────────────────────────────────────┤");
    println!(
        "│  row mode  avg: {:>8.2} ms  p95: {:>8.2} ms              │",
        row.avg(),
        row.p95()
    );
    println!(
        "│  stmt mode avg: {:>8.2} ms  p95: {:>8.2} ms              │",
        stmt.avg(),
        stmt.p95()
    );
    println!(
        "│  Reduction: {:>+.1}%  {}                   │",
        reduction_pct,
        if reduction_pct >= 20.0 {
            "✅"
        } else {
            "⚠️ "
        },
    );
    println!("└──────────────────────────────────────────────────────────┘");
    println!();
}

// ═══════════════════════════════════════════════════════════════════════
// I-5: Concurrent Writer Benchmarks
//
// Measures CDC trigger overhead under concurrent writer load. Multiple
// tokio tasks perform DML simultaneously on the same source table,
// stress-testing BIGSERIAL contention, change buffer index lock
// contention, and WAL write serialization.
//
// Writer concurrency sweep: 1, 2, 4, 8 connections.
// ═══════════════════════════════════════════════════════════════════════

/// Run concurrent writer benchmark for a given writer count.
///
/// Each writer performs `iterations` INSERT batches of `batch_size` rows.
/// Returns (total_elapsed_ms, per_writer_avg_ms).
async fn run_concurrent_writers(
    db: &E2eDb,
    n_writers: usize,
    batch_size: usize,
    iterations: usize,
) -> (f64, Vec<f64>) {
    let conn_str = db.connection_string().to_string();

    let total_start = Instant::now();

    let handles: Vec<_> = (0..n_writers)
        .map(|writer_id| {
            let cs = conn_str.clone();
            tokio::spawn(async move {
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&cs)
                    .await
                    .expect("writer pool connect");

                let mut times_ms = Vec::new();
                for _ in 0..iterations {
                    let sql = format!(
                        "INSERT INTO cw_src (region, amount) \
                         SELECT CASE (i % 5) WHEN 0 THEN 'n' WHEN 1 THEN 's' WHEN 2 THEN 'e' \
                                WHEN 3 THEN 'w' ELSE 'c' END, \
                                (i * {} + 13) % 10000 \
                         FROM generate_series(1, {}) AS s(i)",
                        17 + writer_id,
                        batch_size,
                    );
                    let start = Instant::now();
                    sqlx::query(&sql)
                        .execute(&pool)
                        .await
                        .expect("writer insert");
                    times_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                }
                pool.close().await;
                times_ms
            })
        })
        .collect();

    let mut all_writer_avgs = Vec::new();
    for handle in handles {
        let times = handle.await.expect("writer task");
        let avg = times.iter().sum::<f64>() / times.len() as f64;
        all_writer_avgs.push(avg);
    }

    let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
    (total_ms, all_writer_avgs)
}

/// I-5: Concurrent writer benchmark — sweep 1, 2, 4, 8 writers.
///
/// Run explicitly:
/// ```bash
/// cargo test --test e2e_bench_tests -- --ignored bench_concurrent_writers --nocapture
/// ```
#[tokio::test]
#[ignore]
async fn bench_concurrent_writers() {
    let db = E2eDb::new_bench().await.with_extension().await;

    let base_rows = 50_000usize;
    let batch_size = 1_000usize;
    let iterations = 10usize;
    let writer_counts: &[usize] = &[1, 2, 4, 8];

    // Setup: source table + stream table (installs CDC triggers)
    db.execute(
        "CREATE TABLE cw_src (id SERIAL PRIMARY KEY, region TEXT NOT NULL, amount INT NOT NULL)",
    )
    .await;
    db.execute(&format!(
        "INSERT INTO cw_src (region, amount) \
         SELECT CASE (i % 5) WHEN 0 THEN 'n' WHEN 1 THEN 's' WHEN 2 THEN 'e' \
                WHEN 3 THEN 'w' ELSE 'c' END, (i * 17) % 10000 \
         FROM generate_series(1, {base_rows}) AS s(i)"
    ))
    .await;
    db.create_st(
        "cw_st",
        "SELECT id, region, amount FROM cw_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("ANALYZE cw_src").await;

    println!();
    println!("┌───────────────────────────────────────────────────────────────┐");
    println!("│ I-5: Concurrent Writer CDC Overhead Benchmark                 │");
    println!("├───────────────────────────────────────────────────────────────┤");
    println!(
        "│ Base rows: {} · Batch: {} · Iterations: {}                │",
        base_rows, batch_size, iterations
    );
    println!("│                                                               │");
    println!("│ Writers  Total ms    Per-writer avg ms   Throughput rows/s     │");
    println!("├───────────────────────────────────────────────────────────────┤");

    let mut baseline_throughput = 0.0f64;

    for &n_writers in writer_counts {
        // Drain change buffer before each run
        db.refresh_st("cw_st").await;

        let (total_ms, writer_avgs) =
            run_concurrent_writers(&db, n_writers, batch_size, iterations).await;

        let per_writer_avg = writer_avgs.iter().sum::<f64>() / writer_avgs.len() as f64;
        let total_rows = (n_writers * batch_size * iterations) as f64;
        let throughput = total_rows / (total_ms / 1000.0);

        if n_writers == 1 {
            baseline_throughput = throughput;
        }
        let scaling = if baseline_throughput > 0.0 {
            throughput / baseline_throughput
        } else {
            1.0
        };

        println!(
            "│ {:>7}  {:>10.1}  {:>18.1}  {:>10.0} ({:.2}x)     │",
            n_writers, total_ms, per_writer_avg, throughput, scaling,
        );

        // Drain after each run
        db.refresh_st("cw_st").await;
    }

    println!("└───────────────────────────────────────────────────────────────┘");
    println!();

    // Verify smoke correctness after operations
    db.refresh_st("cw_st").await;
    db.assert_st_matches_query("cw_st", "SELECT id, region, amount FROM cw_src")
        .await;
}

// ── B-2: Predicate Pushdown Performance ────────────────────────────────────

/// B-2: Verify that delta predicate pushdown keeps differential refresh
/// time proportional to delta size (not source table size) for selective
/// queries.
///
/// Setup:
/// - 100,000-row source table with a `category TEXT` column holding 100
///   distinct category values (each category has ~1,000 rows).
/// - Two stream tables:
///   A) `b2_full_st`  — `SELECT * FROM b2_src`           (no WHERE, no pushdown benefit)
///   B) `b2_filt_st`  — `SELECT * FROM b2_src WHERE category = 'cat_001'` (~1,000 rows)
///
/// Per cycle: 10 source rows are mutated (1% of category 'cat_001'),
/// meaning the predicate matches the entire delta.  The pushed-down scan
/// should read **only** the matching change-buffer rows, not all 100,000
/// source rows.
///
/// Assertion: median differential refresh time for `b2_filt_st` must not
/// exceed 3× the time for `b2_full_st` at the same delta rate. In practice,
/// pushdown typically makes filtered refresh *faster*.
///
/// Run:
/// ```bash
/// cargo test --test e2e_bench_tests -- bench_b2_predicate_pushdown --ignored --nocapture
/// ```
#[tokio::test]
#[ignore]
async fn bench_b2_predicate_pushdown() {
    const ROWS: usize = 100_000;
    const CATEGORIES: usize = 100;
    const DELTA_ROWS: usize = 10; // rows changed per cycle
    const WARMUP: usize = 2;
    const CYCLES: usize = 20;

    let db = E2eDb::new().await.with_extension().await;

    // ── Setup ─────────────────────────────────────────────────────────
    db.execute(
        "CREATE TABLE b2_src (
            id       BIGINT PRIMARY KEY,
            category TEXT   NOT NULL,
            value    INT    NOT NULL
        )",
    )
    .await;

    // Insert ROWS rows spread across CATEGORIES categories
    db.execute(&format!(
        "INSERT INTO b2_src \
         SELECT g, 'cat_' || LPAD((g % {cats})::text, 3, '0'), g % 1000 \
         FROM generate_series(1, {rows}) g",
        cats = CATEGORIES,
        rows = ROWS,
    ))
    .await;

    // Unfiltered stream table (no pushdown benefit)
    db.create_st(
        "b2_full_st",
        "SELECT id, category, value FROM b2_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("b2_full_st").await;

    // Filtered stream table (predicate pushdown: only cat_001)
    db.create_st(
        "b2_filt_st",
        "SELECT id, category, value FROM b2_src WHERE category = 'cat_001'",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("b2_filt_st").await;

    // Helper: mutate DELTA_ROWS rows in cat_001
    let mutate = |cycle: usize| {
        let db = &db;
        async move {
            db.execute(&format!(
                "UPDATE b2_src SET value = value + 1 \
                 WHERE id IN (\
                   SELECT id FROM b2_src WHERE category = 'cat_001' \
                   ORDER BY id LIMIT {delta} OFFSET {offset}\
                 )",
                delta = DELTA_ROWS,
                offset = (cycle * DELTA_ROWS) % (ROWS / CATEGORIES),
            ))
            .await;
        }
    };

    // ── Warm-up ───────────────────────────────────────────────────────
    for i in 0..WARMUP {
        mutate(i).await;
        db.refresh_st("b2_full_st").await;
        db.refresh_st("b2_filt_st").await;
    }

    // ── Measured cycles ───────────────────────────────────────────────
    let mut full_times: Vec<f64> = Vec::with_capacity(CYCLES);
    let mut filt_times: Vec<f64> = Vec::with_capacity(CYCLES);

    for i in 0..CYCLES {
        mutate(WARMUP + i).await;

        let t0 = Instant::now();
        db.refresh_st("b2_full_st").await;
        full_times.push(t0.elapsed().as_secs_f64() * 1000.0);

        let t1 = Instant::now();
        db.refresh_st("b2_filt_st").await;
        filt_times.push(t1.elapsed().as_secs_f64() * 1000.0);
    }

    // ── Report ────────────────────────────────────────────────────────
    full_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    filt_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let median_full = full_times[CYCLES / 2];
    let median_filt = filt_times[CYCLES / 2];

    println!();
    println!("┌─────────────────────────────────────────────────────────┐");
    println!("│  B-2: Delta Predicate Pushdown — {ROWS} rows, {DELTA_ROWS} δ/cycle       │");
    println!("├────────────────────────┬────────────────────────────────┤");
    println!("│ Stream table           │  Median refresh (ms)           │");
    println!("├────────────────────────┼────────────────────────────────┤");
    println!(
        "│ b2_full_st (no filter) │  {:>8.1} ms                  │",
        median_full
    );
    println!(
        "│ b2_filt_st (WHERE cat) │  {:>8.1} ms                  │",
        median_filt
    );
    println!(
        "│ Ratio filt/full        │  {:>8.2}×                    │",
        median_filt / median_full
    );
    println!("└────────────────────────┴────────────────────────────────┘");
    println!();

    // Correctness: both STs must match ground truth
    db.assert_st_matches_query(
        "public.b2_full_st",
        "SELECT id, category, value FROM b2_src",
    )
    .await;
    db.assert_st_matches_query(
        "public.b2_filt_st",
        "SELECT id, category, value FROM b2_src WHERE category = 'cat_001'",
    )
    .await;

    // B-2 gate: filtered ST must not be more than 3× slower than unfiltered.
    // In practice pushdown makes filtered refresh faster or equivalent.
    assert!(
        median_filt <= median_full * 3.0,
        "B-2 FAILED: filtered refresh ({:.1}ms) is more than 3× unfiltered ({:.1}ms) — \
         predicate pushdown may not be active",
        median_filt,
        median_full,
    );

    println!(
        "B-2 PASSED: filtered refresh {:.1}ms vs unfiltered {:.1}ms ({:.2}× ratio ≤ 3.0×)",
        median_filt,
        median_full,
        median_filt / median_full
    );
}
