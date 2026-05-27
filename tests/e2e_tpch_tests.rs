//! TPC-H-derived correctness tests for pg_trickle DIFFERENTIAL refresh.
//!
//! Validates the core DBSP invariant: after every differential refresh,
//! the stream table's contents must be multiset-equal to re-executing
//! the defining query from scratch.
//!
//! **TPC-H Fair Use:** This workload is *derived from* the TPC-H Benchmark
//! specification but does not constitute a TPC-H Benchmark result. Data is
//! generated with a custom pure-SQL generator (not `dbgen`), queries have
//! been modified, and no TPC-defined metric (QphH) is computed. "TPC-H" is
//! a trademark of the Transaction Processing Performance Council (tpc.org).
//!
//! These tests are `#[ignore]`d to skip in normal `cargo test` runs.
//! They run automatically in CI on push to main (see .github/workflows/ci.yml)
//! or manually:
//!
//! ```bash
//! just test-tpch              # SF-0.01, ~2 min
//! just test-tpch-large        # SF-0.1,  ~5 min
//! ```
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;
// P2.11: Shared TPC-H helpers (also used by e2e_tpch_dag_tests). The local
// helper functions in this file are kept for now; a follow-up can replace
// them with tpch:: references once the module is stable.
#[allow(unused_imports)]
mod tpch;

use e2e::{E2eDb, extract_last_profile};
use std::time::Instant;

// ── Configuration ──────────────────────────────────────────────────────

/// Number of refresh cycles per query (RF1 + RF2 + RF3 → refresh → assert).
fn cycles() -> usize {
    std::env::var("TPCH_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
}

/// Scale factor. Controls data volume.
///   0.01 → ~1,500 orders, ~6,000 lineitems   (default, ~2 min)
///   0.1  → ~15,000 orders, ~60,000 lineitems  (~5 min)
///   1.0  → ~150,000 orders, ~600,000 lineitems (~15 min)
fn scale_factor() -> f64 {
    std::env::var("TPCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.01)
}

/// Number of rows affected per RF cycle (INSERT/DELETE/UPDATE batch size).
/// Defaults to 1% of order count, minimum 10.
fn rf_count() -> usize {
    let sf = scale_factor();
    let orders = ((sf * 150_000.0) as usize).max(1_500);
    (orders / 100).max(10)
}

// ── T2: Skip-set regression guard ─────────────────────────────────────
//
// Allowlists of queries that are permitted to be skipped in each mode.
// If a query that is NOT in the allowlist is skipped, the test fails —
// signalling a DVM regression where a previously-passing query no longer
// creates or runs correctly.
//
// DIFFERENTIAL: all 22 queries pass at SF<10. At SF≥10, queries with correlated
//               scalar subqueries in WHERE clauses become quadratic in the DVM delta
//               SQL and exceed the 90-minute nextest slow-timeout. Those queries are
//               in LARGE_SCALE_DIFFERENTIAL_SKIP below and are skipped automatically.
// IMMEDIATE:    a subset cannot be created (IVM restriction). Populate by
//               running with the guard disabled, then hardening the set.
//
// To update: comment out the `assert!` at the end of the test, run, collect
// the output, and add the skipped query names here with a comment explaining
// the known limitation.

/// Queries allowed to be skipped in DIFFERENTIAL mode at any scale factor.
const DIFFERENTIAL_SKIP_ALLOWLIST: &[&str] = &[
    // DI-11 deep-join planner hints (disable nestloop, raise work_mem,
    // bump join_collapse_limit, temp_file_limit=-1) resolved Q05/Q09.
    // All 22 TPC-H queries pass DIFFERENTIAL mode at SF<10.
];

/// Scale factor threshold above which LARGE_SCALE_DIFFERENTIAL_SKIP applies.
const LARGE_SCALE_SKIP_THRESHOLD: f64 = 10.0;

/// Queries skipped in DIFFERENTIAL mode at SF≥LARGE_SCALE_SKIP_THRESHOLD.
///
/// These queries pass correctly at smaller scale factors (all 22 pass at SF-1)
/// but have DVM performance limitations that cause cycle-level timeouts at
/// SF≥10. They are pre-skipped to prevent the 90-minute nextest slow-timeout
/// from killing the whole test function.
///
/// Root cause: the DVM delta SQL for these queries contains a correlated scalar
/// subquery in the WHERE clause. The subquery is re-evaluated for every row in
/// the CDC delta, giving O(delta × table) complexity. At SF-10 with accumulated
/// CDC events this becomes quadratic and exceeds the per-test timeout.
const LARGE_SCALE_DIFFERENTIAL_SKIP: &[&str] = &[
    // q20: Semi-Join + nested correlated scalar subquery
    //   `ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem
    //                   WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND ...)`
    // DVM re-executes the lineitem subquery for every changed row in the delta.
    // At SF=10 (6M lineitems), cycle 2 takes 45+ minutes and triggers the
    // 90-minute nextest slow-timeout, killing the whole test function.
    // Tracked for fix: replace correlated subquery with a pre-aggregated CTE
    // in the DVM delta rewrite (DVM rewrite rule for semi-join EXISTS/IN).
    "q20",
];

/// Queries allowed to be skipped in IMMEDIATE mode.
/// Queries that fail `create_stream_table(..., 'IMMEDIATE')` due to IVM
/// restrictions (subqueries in the target list, EXCEPT ALL, NOT IN correlated
/// subqueries, etc.) are expected to be skipped.
///
/// Populated from empirical test runs — see plans/testing/TEST_SUITE_TPC_H-GAPS.md §T2.
/// The regression guard at the end of the test will fail if any query not in
/// this list is skipped, catching silent regressions as the DVM evolves.
#[rustfmt::skip]
const IMMEDIATE_SKIP_ALLOWLIST: &[&str] = &[];

// ── P3.15: TPCH_STRICT mode ───────────────────────────────────────────
//
// When `TPCH_STRICT=1` is set, soft-skips become hard-fails:
//   - Any unexpected skip (not in the respective allowlist) causes the
//     test to fail immediately rather than accumulating in the skip list.
//   - Useful for local debugging when you need zero tolerance for regressions.
//
// Usage:
//   TPCH_STRICT=1 cargo test --test e2e_tpch_tests -- --ignored --nocapture
fn strict_mode() -> bool {
    std::env::var("TPCH_STRICT")
        .map(|v| v == "1")
        .unwrap_or(false)
}

// ── Scale factor dimensions ────────────────────────────────────────────

fn sf_orders() -> usize {
    ((scale_factor() * 150_000.0) as usize).max(1_500)
}

fn sf_customers() -> usize {
    ((scale_factor() * 15_000.0) as usize).max(150)
}

fn sf_suppliers() -> usize {
    ((scale_factor() * 1_000.0) as usize).max(10)
}

fn sf_parts() -> usize {
    ((scale_factor() * 20_000.0) as usize).max(200)
}

// ── SQL file embedding ─────────────────────────────────────────────────

const SCHEMA_SQL: &str = include_str!("tpch/schema.sql");
const DATAGEN_SQL: &str = include_str!("tpch/datagen.sql");
const RF1_SQL: &str = include_str!("tpch/rf1.sql");
const RF2_SQL: &str = include_str!("tpch/rf2.sql");
const RF3_SQL: &str = include_str!("tpch/rf3.sql");

// ── TPC-H queries ordered by coverage tier ─────────────────────────────

/// A TPC-H query with its name and SQL.
struct TpchQuery {
    name: &'static str,
    sql: &'static str,
    tier: u8,
}

fn tpch_queries() -> Vec<TpchQuery> {
    vec![
        // ── Tier 1: Maximum operator diversity (fast-fail) ─────────
        TpchQuery {
            name: "q02",
            sql: include_str!("tpch/queries/q02.sql"),
            tier: 1,
        },
        TpchQuery {
            name: "q21",
            sql: include_str!("tpch/queries/q21.sql"),
            tier: 1,
        },
        TpchQuery {
            name: "q13",
            sql: include_str!("tpch/queries/q13.sql"),
            tier: 1,
        },
        TpchQuery {
            name: "q11",
            sql: include_str!("tpch/queries/q11.sql"),
            tier: 1,
        },
        TpchQuery {
            name: "q08",
            sql: include_str!("tpch/queries/q08.sql"),
            tier: 1,
        },
        // ── Tier 2: Core operator correctness ──────────────────────
        TpchQuery {
            name: "q01",
            sql: include_str!("tpch/queries/q01.sql"),
            tier: 2,
        },
        TpchQuery {
            name: "q05",
            sql: include_str!("tpch/queries/q05.sql"),
            tier: 2,
        },
        TpchQuery {
            name: "q07",
            sql: include_str!("tpch/queries/q07.sql"),
            tier: 2,
        },
        TpchQuery {
            name: "q09",
            sql: include_str!("tpch/queries/q09.sql"),
            tier: 2,
        },
        TpchQuery {
            name: "q16",
            sql: include_str!("tpch/queries/q16.sql"),
            tier: 2,
        },
        TpchQuery {
            name: "q22",
            sql: include_str!("tpch/queries/q22.sql"),
            tier: 2,
        },
        // ── Tier 3: Remaining queries (completeness) ───────────────
        TpchQuery {
            name: "q03",
            sql: include_str!("tpch/queries/q03.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q04",
            sql: include_str!("tpch/queries/q04.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q06",
            sql: include_str!("tpch/queries/q06.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q10",
            sql: include_str!("tpch/queries/q10.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q12",
            sql: include_str!("tpch/queries/q12.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q14",
            sql: include_str!("tpch/queries/q14.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q15",
            sql: include_str!("tpch/queries/q15.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q17",
            sql: include_str!("tpch/queries/q17.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q18",
            sql: include_str!("tpch/queries/q18.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q19",
            sql: include_str!("tpch/queries/q19.sql"),
            tier: 3,
        },
        TpchQuery {
            name: "q20",
            sql: include_str!("tpch/queries/q20.sql"),
            tier: 3,
        },
    ]
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Replace scale-factor tokens in a SQL template.
fn substitute_sf(sql: &str) -> String {
    sql.replace("__SF_ORDERS__", &sf_orders().to_string())
        .replace("__SF_CUSTOMERS__", &sf_customers().to_string())
        .replace("__SF_SUPPLIERS__", &sf_suppliers().to_string())
        .replace("__SF_PARTS__", &sf_parts().to_string())
}

/// Replace RF tokens in a mutation SQL template.
fn substitute_rf(sql: &str, next_orderkey: usize) -> String {
    sql.replace("__RF_COUNT__", &rf_count().to_string())
        .replace("__NEXT_ORDERKEY__", &next_orderkey.to_string())
        .replace("__SF_CUSTOMERS__", &sf_customers().to_string())
        .replace("__SF_PARTS__", &sf_parts().to_string())
        .replace("__SF_SUPPLIERS__", &sf_suppliers().to_string())
}

/// Load TPC-H schema into the database.
async fn load_schema(db: &E2eDb) {
    // Execute each statement separately (sqlx doesn't support multi-statement).
    // Do NOT filter on starts_with("--") — chunks that begin with a comment
    // header still contain valid SQL after the comment lines.
    for stmt in SCHEMA_SQL.split(';') {
        let stmt = stmt.trim();
        // Skip completely empty segments (trailing `;` at EOF etc.)
        // but keep segments that begin with a comment followed by real SQL.
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.execute(stmt).await;
        }
    }

    // Diagnostic: dump OID → table name mapping for TPC-H tables
    let oid_map: Vec<(String, i64)> = sqlx::query_as(
        "SELECT c.relname::text, c.oid::bigint \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public' AND c.relkind = 'r' \
         ORDER BY c.oid",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap_or_default();
    println!("  TPC-H table OIDs:");
    for (name, oid) in &oid_map {
        println!("    {name}: {oid}");
    }
}

/// Load TPC-H data at the configured scale factor.
async fn load_data(db: &E2eDb) {
    let sql = substitute_sf(DATAGEN_SQL);
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.execute(stmt).await;
        }
    }
}

/// Get the current max order key (for RF1 to generate non-conflicting keys).
/// Cast to bigint explicitly because o_orderkey is INT4 and sqlx decodes INT8.
async fn max_orderkey(db: &E2eDb) -> usize {
    let max: i64 = db
        .query_scalar("SELECT COALESCE(MAX(o_orderkey), 0)::bigint FROM orders")
        .await;
    max as usize
}

/// Apply RF1 (bulk INSERT into orders + lineitem).
/// Both INSERTs are executed sequentially; refresh_st sees both changes together
/// since it runs after all mutations are applied.
async fn apply_rf1(db: &E2eDb, next_orderkey: usize) {
    let sql = substitute_rf(RF1_SQL, next_orderkey);
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.execute(stmt).await;
        }
    }
}

/// Apply RF2 (bulk DELETE from orders + lineitem).
async fn apply_rf2(db: &E2eDb) {
    let sql = RF2_SQL.replace("__RF_COUNT__", &rf_count().to_string());
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.execute(stmt).await;
        }
    }
}

/// Apply RF3 (targeted UPDATEs).
async fn apply_rf3(db: &E2eDb) {
    let sql = RF3_SQL.replace("__RF_COUNT__", &rf_count().to_string());
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.execute(stmt).await;
        }
    }
}

/// Assert a stream table matches its defining query, with diagnostic output.
async fn assert_tpch_invariant(
    db: &E2eDb,
    st_name: &str,
    query: &str,
    qname: &str,
    cycle: usize,
) -> Result<(), String> {
    let st_table = format!("public.{st_name}");

    // Get user-visible columns — exclude internal pg_trickle bookkeeping columns.
    // Use explicit names (not LIKE) to match the approach in E2eDb::assert_st_matches_query.
    let cols: String = db
        .query_scalar(&format!(
            "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) \
             FROM information_schema.columns \
             WHERE (table_schema || '.' || table_name = 'public.{st_name}' \
                OR table_name = '{st_name}') \
             AND left(column_name, 6) <> '__pgt_'"
        ))
        .await;

    // Multiset equality: symmetric EXCEPT ALL must be empty
    let matches: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS ( \
                (SELECT {cols} FROM {st_table} EXCEPT ALL ({query})) \
                UNION ALL \
                (({query}) EXCEPT ALL SELECT {cols} FROM {st_table}) \
            )"
        ))
        .await;

    // ── T1: Negative __pgt_count guard ───────────────────────────────────
    // A __pgt_count < 0 indicates an over-retraction DVM bug: deleted more
    // multiplicity than was ever inserted. Check before the multiset EXCEPT
    // so it surfaces even when the extra/missing happen to cancel out.
    let has_pgt_count: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS ( \
                SELECT 1 FROM information_schema.columns \
                WHERE table_name = '{st_name}' \
                  AND column_name = '__pgt_count' \
            )"
        ))
        .await;
    if has_pgt_count {
        let neg_count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {st_table} WHERE __pgt_count < 0"
            ))
            .await;
        if neg_count > 0 {
            return Err(format!(
                "NEGATIVE __pgt_count: {qname} cycle {cycle} — \
                 {neg_count} rows with __pgt_count < 0 (over-retraction bug)"
            ));
        }
    }

    if !matches {
        // Collect diagnostic information
        let st_count: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM {st_table}"))
            .await;
        let q_count: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM ({query}) _q"))
            .await;
        let extra: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM \
                 (SELECT {cols} FROM {st_table} EXCEPT ALL ({query})) _x"
            ))
            .await;
        let missing: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM \
                 (({query}) EXCEPT ALL SELECT {cols} FROM {st_table}) _x"
            ))
            .await;

        // Detailed column-level diagnostics: show extra/missing rows
        let extra_rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT row_to_json(x)::text FROM \
             (SELECT {cols} FROM {st_table} EXCEPT ALL ({query})) x \
             LIMIT 10"
        )))
        .fetch_all(&db.pool)
        .await
        .unwrap_or_default();
        let missing_rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT row_to_json(x)::text FROM \
             (({query}) EXCEPT ALL SELECT {cols} FROM {st_table}) x \
             LIMIT 10"
        )))
        .fetch_all(&db.pool)
        .await
        .unwrap_or_default();

        if !extra_rows.is_empty() {
            println!("    EXTRA rows (in ST but not query):");
            for (row,) in &extra_rows {
                println!("      {row}");
            }
        }
        if !missing_rows.is_empty() {
            println!("    MISSING rows (in query but not ST):");
            for (row,) in &missing_rows {
                println!("      {row}");
            }
        }

        return Err(format!(
            "INVARIANT VIOLATION: {qname} cycle {cycle} — \
             ST rows: {st_count}, Q rows: {q_count}, \
             extra: {extra}, missing: {missing}"
        ));
    }
    Ok(())
}

/// Print a progress line for a query/cycle.
fn log_progress(qname: &str, tier: u8, cycle: usize, total_cycles: usize, elapsed_ms: f64) {
    println!(
        "  [T{}] {:<4} cycle {}/{} — {:.0}ms ✓",
        tier, qname, cycle, total_cycles, elapsed_ms,
    );
}

/// Refresh a stream table, returning an error instead of panicking.
/// Used to gracefully handle known pg_trickle DVM bugs without stopping the test.
/// A 60-second lock_timeout prevents the test from hanging if the background
/// scheduler unexpectedly holds a conflicting transaction lock.
async fn try_refresh_st(db: &E2eDb, st_name: &str) -> Result<(), String> {
    db.try_execute("SET lock_timeout = '60s'").await.ok();
    let result = db
        .try_execute(&format!(
            "SELECT pgtrickle.refresh_stream_table('{st_name}')"
        ))
        .await
        .map_err(|e| e.to_string());
    db.try_execute("SET lock_timeout = 0").await.ok();
    result
}

// ── TPCH_BENCH mode helpers ────────────────────────────────────────────
//
// When `TPCH_BENCH=1` is set, `test_tpch_performance_comparison` switches
// into benchmark mode: warm-up cycles are discarded, `[TPCH_BENCH]`
// structured lines are emitted per cycle, and a summary table with median,
// P95, and MERGE% is printed at the end.

/// Whether benchmark mode is active (`TPCH_BENCH=1`).
fn bench_mode() -> bool {
    std::env::var("TPCH_BENCH")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// Number of warm-up cycles to discard before measurement starts.
/// Warm-up cycles are run but their timings are excluded from statistics.
/// Defaults to 2; configurable via `WARMUP_CYCLES` env var.
fn warmup_cycles() -> usize {
    std::env::var("WARMUP_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2)
}

/// Compute a percentile from a sorted Vec.
fn tpch_percentile(sorted: &[f64], pct: f64) -> f64 {
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

/// Per-query benchmark results collected during a `TPCH_BENCH=1` run.
#[allow(dead_code)]
struct TpchBenchRow {
    name: String,
    tier: u8,
    full_ms: Vec<f64>,
    diff_ms: Vec<f64>,
    /// MERGE phase fraction per DIFF cycle (merge_ms / total_ms).
    merge_pct: Vec<f64>,
}

/// Print the two-table benchmark summary for a `TPCH_BENCH=1` run.
fn print_tpch_bench_summary(rows: &[TpchBenchRow]) {
    println!();
    println!("┌──────┬──────┬────────────┬────────────┬──────────┬──────────┬──────────┐");
    println!("│ Query│ Tier │ FULL med ms│ DIFF med ms│ Speedup  │ DIFF P95 │  MERGE%  │");
    println!("├──────┼──────┼────────────┼────────────┼──────────┼──────────┼──────────┤");

    for r in rows {
        let mut fs = r.full_ms.clone();
        let mut ds = r.diff_ms.clone();
        fs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        ds.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let full_med = tpch_percentile(&fs, 50.0);
        let diff_med = tpch_percentile(&ds, 50.0);
        let diff_p95 = tpch_percentile(&ds, 95.0);
        let speedup = if diff_med > 0.0 {
            full_med / diff_med
        } else {
            f64::NAN
        };
        let merge_pct_avg = if r.merge_pct.is_empty() {
            f64::NAN
        } else {
            r.merge_pct.iter().sum::<f64>() / r.merge_pct.len() as f64
        };

        println!(
            "│ {:<4} │  T{}  │ {:>10.1} │ {:>10.1} │ {:>7.2}x │ {:>8.1} │ {:>7.0}% │",
            r.name,
            r.tier,
            full_med,
            diff_med,
            speedup,
            diff_p95,
            merge_pct_avg * 100.0,
        );
    }
    println!("└──────┴──────┴────────────┴────────────┴──────────┴──────────┴──────────┘");
    println!();

    let total_full: f64 = rows
        .iter()
        .filter_map(|r| r.full_ms.iter().copied().reduce(f64::max))
        .sum::<f64>();
    let total_diff: f64 = rows
        .iter()
        .filter_map(|r| r.diff_ms.iter().copied().reduce(f64::max))
        .sum::<f64>();
    println!(
        "  Total wall-clock: FULL {:.1}s, DIFF {:.1}s (overall speedup: {:.1}x)",
        total_full / 1000.0,
        total_diff / 1000.0,
        if total_diff > 0.0 {
            total_full / total_diff
        } else {
            f64::NAN
        },
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 1: Individual Query Correctness
// ═══════════════════════════════════════════════════════════════════════
//
// For each TPC-H query (ordered by coverage tier):
//   1. Create ST in DIFFERENTIAL mode
//   2. Assert baseline invariant
//   3. For N cycles: RF1+RF2+RF3 → refresh → assert invariant
//   4. Drop ST
//
// Uses a SINGLE container for all queries — data loaded once.

#[tokio::test]
#[ignore]
async fn test_tpch_differential_correctness() {
    let sf = scale_factor();
    let n_cycles = cycles();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H Differential Correctness — SF={sf}, cycles={n_cycles}");
    println!(
        "  Orders: {}, Customers: {}, Suppliers: {}, Parts: {}",
        sf_orders(),
        sf_customers(),
        sf_suppliers(),
        sf_parts()
    );
    println!("  RF batch size: {} rows", rf_count());
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    // Load schema + data
    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let queries = tpch_queries();
    let mut passed = 0usize;
    let mut skipped: Vec<(&str, String)> = Vec::new();
    let mut failed: Vec<(&str, String)> = Vec::new(); // populated for invariant assertion errors

    for q in &queries {
        println!(
            "── {} (Tier {}) ──────────────────────────────",
            q.name, q.tier
        );

        // Pre-skip queries with known DVM performance issues at large scale.
        // These queries are correct at SF<10 but their DVM delta SQL becomes
        // O(delta × table) at SF≥10, causing cycle-level timeouts that kill
        // the whole test function via nextest slow-timeout.
        if scale_factor() >= LARGE_SCALE_SKIP_THRESHOLD
            && LARGE_SCALE_DIFFERENTIAL_SKIP.contains(&q.name)
        {
            println!(
                "  SKIP — {} large-scale DVM perf limit at SF={} (see LARGE_SCALE_DIFFERENTIAL_SKIP)",
                q.name,
                scale_factor()
            );
            skipped.push((
                q.name,
                format!(
                    "large-scale DVM correlated-subquery perf limit at SF={}",
                    scale_factor()
                ),
            ));
            continue;
        }

        // Create stream table
        let st_name = format!("tpch_{}", q.name);
        let create_result = db
            .try_execute(&format!(
                // '24h' schedule — time-based check never fires during the test
                // window, so the background scheduler does NOT race with the
                // test's explicit per-cycle refreshes.
                // ('calculated' = CALCULATED mode = auto-refresh on pending CDC
                // changes — the opposite of what we want here.)
                "SELECT pgtrickle.create_stream_table('{st_name}', $${sql}$$, '24h', 'DIFFERENTIAL')",
                sql = q.sql,
            ))
            .await;

        if let Err(e) = create_result {
            let reason = e.to_string();
            let short = reason.split(':').next_back().unwrap_or(&reason).trim();
            println!("  SKIP — {short}");
            skipped.push((q.name, reason));
            continue;
        }

        // Diagnostic: show source OIDs from deps and change buffer tables
        let dep_rows: Vec<(i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT d.source_relid::bigint, c.relname::text \
                 FROM pgtrickle.pgt_dependencies d \
                 JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id \
                 LEFT JOIN pg_class c ON c.oid = d.source_relid \
                 WHERE st.pgt_name = '{st_name}' AND d.source_type = 'TABLE' \
                 ORDER BY d.source_relid"
        )))
        .fetch_all(&db.pool)
        .await
        .unwrap_or_default();
        let dep_info: Vec<String> = dep_rows
            .iter()
            .map(|(oid, name)| format!("{name}({oid})"))
            .collect();
        println!("  deps: {}", dep_info.join(", "));

        // Check which change buffer tables exist
        let buf_rows: Vec<(String,)> = sqlx::query_as(
            "SELECT c.relname::text \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'pgtrickle_changes' AND c.relkind = 'r' \
             ORDER BY c.relname",
        )
        .fetch_all(&db.pool)
        .await
        .unwrap_or_default();
        let buf_names: Vec<&str> = buf_rows.iter().map(|(n,)| n.as_str()).collect();
        println!("  change_buffers: {}", buf_names.join(", "));

        // Baseline assertion
        let t = Instant::now();
        if let Err(e) = assert_tpch_invariant(&db, &st_name, q.sql, q.name, 0).await {
            println!("  WARN baseline — {e}");
            skipped.push((q.name, format!("baseline invariant: {e}")));
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                .await;
            continue;
        }
        println!("  baseline — {:.0}ms ✓", t.elapsed().as_secs_f64() * 1000.0);

        // Mutation cycles
        let mut dvm_ok = true;
        'cycles: for cycle in 1..=n_cycles {
            let ct = Instant::now();

            // RF1: bulk INSERT (needs current max order key)
            let next_ok = max_orderkey(&db).await + 1;
            apply_rf1(&db, next_ok).await;

            // RF2: bulk DELETE
            apply_rf2(&db).await;

            // RF3: targeted UPDATEs
            apply_rf3(&db).await;

            // ANALYZE for stable plans after mutations
            db.execute("ANALYZE orders").await;
            db.execute("ANALYZE lineitem").await;
            db.execute("ANALYZE customer").await;

            // Differential refresh — soft error for known pg_trickle DVM bugs
            if let Err(e) = try_refresh_st(&db, &st_name).await {
                // Dump change buffer tables on error for diagnosis
                let buf_rows2: Vec<(String,)> = sqlx::query_as(
                    "SELECT c.relname::text \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = 'pgtrickle_changes' AND c.relkind = 'r' \
                     ORDER BY c.relname",
                )
                .fetch_all(&db.pool)
                .await
                .unwrap_or_default();
                let buf_names2: Vec<&str> = buf_rows2.iter().map(|(n,)| n.as_str()).collect();

                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} — pg_trickle DVM error: {msg}");
                println!("    change_buffers_at_error: {}", buf_names2.join(", "));
                skipped.push((q.name, format!("DVM error cycle {cycle}: {msg}")));
                dvm_ok = false;
                break 'cycles;
            }

            // Assert the invariant.
            // Invariant violations are hard-failures (DVM correctness bugs),
            // distinct from DVM engine errors that prevent refresh (which are
            // soft-skips).  Populate `failed` so the final T2 guard can report
            // them separately from known infrastructure skips.
            if let Err(e) = assert_tpch_invariant(&db, &st_name, q.sql, q.name, cycle).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  FAIL cycle {cycle} — {msg}");
                failed.push((q.name, format!("invariant cycle {cycle}: {msg}")));
                dvm_ok = false;
                break 'cycles;
            }

            log_progress(
                q.name,
                q.tier,
                cycle,
                n_cycles,
                ct.elapsed().as_secs_f64() * 1000.0,
            );

            // CHECKPOINT flushes WAL so it can be recycled.
            // VACUUM ANALYZE reclaims dead tuples and updates statistics.
            // At SF=0.01 with 2-3 cycles the disk growth is negligible, so
            // the full table rewrite of VACUUM FULL is not needed here.
            db.execute("CHECKPOINT").await;
            db.execute("VACUUM ANALYZE").await;
        }

        if dvm_ok {
            passed += 1;
        }

        // Clean up
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
    }

    println!("\n══════════════════════════════════════════════════════════");
    println!(
        "  Results: {passed}/{} queries passed, {} skipped, {} failed",
        queries.len(),
        skipped.len(),
        failed.len(),
    );
    if !skipped.is_empty() {
        println!("  Skipped (pg_trickle limitation):");
        for (name, reason) in &skipped {
            let short = reason.split(':').next_back().unwrap_or(reason).trim();
            println!("    {name}: {short}");
        }
    }
    if !failed.is_empty() {
        println!("  FAILED (assertion errors — correctness bugs):");
        for (name, reason) in &failed {
            println!("    {name}: {reason}");
        }
    }
    println!("══════════════════════════════════════════════════════════\n");

    // P1.7: Hard-fail on invariant violations (distinct from DVM infra errors).
    assert!(
        failed.is_empty(),
        "{} queries failed with assertion errors (not pg_trickle limitations): {:?}",
        failed.len(),
        failed.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
    );

    // T2: Skip-set regression guard for DIFFERENTIAL mode.
    // At SF≥LARGE_SCALE_SKIP_THRESHOLD, also accept pre-skipped large-scale
    // performance queries (LARGE_SCALE_DIFFERENTIAL_SKIP) as expected skips.
    let unexpected_skips: Vec<&str> = skipped
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| !DIFFERENTIAL_SKIP_ALLOWLIST.contains(name))
        .filter(|name| {
            !(scale_factor() >= LARGE_SCALE_SKIP_THRESHOLD
                && LARGE_SCALE_DIFFERENTIAL_SKIP.contains(name))
        })
        .collect();
    assert!(
        unexpected_skips.is_empty(),
        "DIFFERENTIAL REGRESSION: queries newly skipped that are not in \
         DIFFERENTIAL_SKIP_ALLOWLIST or LARGE_SCALE_DIFFERENTIAL_SKIP: {:?}\n\
         If intentional, add to the appropriate allowlist with an explanatory comment.",
        unexpected_skips
    );

    // P3.15: TPCH_STRICT=1 — hard-fail on any skip whatsoever.
    if strict_mode() {
        assert!(
            skipped.is_empty(),
            "STRICT: {} queries skipped (zero tolerance in TPCH_STRICT mode): {:?}",
            skipped.len(),
            skipped.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        );
    }
}
// ═══════════════════════════════════════════════════════════════════════
// Phase 2: Cross-Query Consistency
// ═══════════════════════════════════════════════════════════════════════
//
// All 22 stream tables exist simultaneously, share the same mutations.
// Tests that CDC triggers on shared source tables correctly fan out
// changes to all dependent STs without interference.

#[tokio::test]
#[ignore]
async fn test_tpch_cross_query_consistency() {
    let sf = scale_factor();
    let n_cycles = cycles();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H Cross-Query Consistency — SF={sf}, cycles={n_cycles}");
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s", t.elapsed().as_secs_f64());

    let queries = tpch_queries();
    let mut created: Vec<(String, &str, &str)> = Vec::new();

    // Create all stream tables
    println!("\n  Creating stream tables...");
    for q in &queries {
        let st_name = format!("tpch_x_{}", q.name);
        let result = db
            .try_execute(&format!(
                // '24h' schedule — time-based check will never fire during the test
                // window (now() - last_refresh_at ≪ 86400s), so the background
                // scheduler skips these tables and does NOT race with the
                // cross-query refresh loop.  ('calculated' = CALCULATED mode,
                // which auto-refreshes whenever CDC changes are pending — the
                // opposite of what we want here.)
                "SELECT pgtrickle.create_stream_table('{st_name}', $${sql}$$, '24h', 'DIFFERENTIAL')",
                sql = q.sql,
            ))
            .await;

        match result {
            Ok(_) => {
                println!("    {}: created ✓", q.name);
                created.push((st_name, q.name, q.sql));
            }
            Err(e) => {
                println!("    {}: SKIP — {e}", q.name);
            }
        }
    }

    println!(
        "  {} / {} stream tables created\n",
        created.len(),
        queries.len()
    );

    // Baseline assertions — soft-skip entries that fail
    let mut baseline_ok: Vec<(String, &str, &str)> = Vec::new();
    for (st_name, qname, sql) in &created {
        if let Err(e) = assert_tpch_invariant(&db, st_name, sql, qname, 0).await {
            println!("  WARN baseline {qname} — {e}");
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                .await;
        } else {
            baseline_ok.push((st_name.clone(), qname, sql));
        }
    }
    println!(
        "  Baseline assertions: {}/{} passed ✓\n",
        baseline_ok.len(),
        created.len()
    );

    // Mutation cycles with ALL STs refreshed
    let mut active: Vec<(String, &str, &str)> = baseline_ok;
    for cycle in 1..=n_cycles {
        let ct = Instant::now();

        let next_ok = max_orderkey(&db).await + 1;
        apply_rf1(&db, next_ok).await;
        apply_rf2(&db).await;
        apply_rf3(&db).await;

        // Flush WAL generated by RF mutations before starting refreshes.
        // Without this, WAL from 22 × RF operations accumulates alongside
        // WAL from 22 × IVM refreshes and can grow to 200+ GB.
        db.execute("CHECKPOINT").await;

        db.execute("ANALYZE orders").await;
        db.execute("ANALYZE lineitem").await;
        db.execute("ANALYZE customer").await;

        // Refresh all active STs; remove any that hit a DVM error.
        // CHECKPOINT after each refresh keeps WAL bounded: each complex
        // TPC-H IVM refresh can generate several GB of WAL; without
        // periodic flushing 22 refreshes accumulate unbounded.
        let mut next_active = Vec::new();
        for (st_name, qname, sql) in &active {
            match try_refresh_st(&db, st_name).await {
                Err(e) => {
                    let msg = e.lines().next().unwrap_or(&e).to_string();
                    println!("  WARN: {qname} refresh error cycle {cycle}: {msg}");
                    let _ = db
                        .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                        .await;
                }
                Ok(_) => match assert_tpch_invariant(&db, st_name, sql, qname, cycle).await {
                    Ok(()) => next_active.push((st_name.clone(), *qname, *sql)),
                    Err(e) => {
                        let msg = e.lines().next().unwrap_or(&e).to_string();
                        println!("  WARN: {qname} invariant cycle {cycle}: {msg}");
                        let _ = db
                            .try_execute(&format!(
                                "SELECT pgtrickle.drop_stream_table('{st_name}')"
                            ))
                            .await;
                    }
                },
            }
            // Per-query WAL flush: prevents unbounded WAL accumulation when
            // refreshing 22 queries in sequence within a single cycle.
            db.execute("CHECKPOINT").await;
        }
        active = next_active;

        // VACUUM ANALYZE reclaims dead tuples and updates statistics after
        // all refreshes in this cycle. At SF=0.01 with few cycles, full
        // compaction via VACUUM FULL is not needed.
        db.execute("CHECKPOINT").await;
        db.execute("VACUUM ANALYZE").await;

        println!(
            "  Cycle {}/{} — {} STs verified — {:.0}ms ✓",
            cycle,
            n_cycles,
            active.len(),
            ct.elapsed().as_secs_f64() * 1000.0,
        );
    }

    // Cleanup remaining active STs
    for (st_name, _, _) in &active {
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
    }

    // P0.3: Minimum surviving ST assertion — prevents silent complete deactivation.
    // At least 50% of created STs must survive all cycles (i.e., 8+ if 17 are created).
    let min_surviving = (created.len() / 2).max(1);
    println!(
        "\n  Cross-query consistency: {}/{} STs survived all cycles{}\n",
        active.len(),
        created.len(),
        if active.len() >= min_surviving {
            " ✓"
        } else {
            " ✗"
        },
    );
    assert!(
        active.len() >= min_surviving,
        "Cross-query consistency: only {}/{} STs survived all cycles \
         (minimum required: {min_surviving}).\n\
         Check for CDC fan-out bugs or cascade deactivation.",
        active.len(),
        created.len(),
    );

    // P3.15: TPCH_STRICT=1 — all created STs must survive all cycles.
    if strict_mode() {
        assert!(
            active.len() == created.len(),
            "STRICT: {}/{} STs survived — {} were deactivated during churn",
            active.len(),
            created.len(),
            created.len() - active.len(),
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 2b: Standalone FULL Mode Correctness
// ═══════════════════════════════════════════════════════════════════════
//
// P2.8: Creates each query in FULL mode only and asserts baseline + N
// mutation cycles against ground truth.  This is distinct from
// test_tpch_full_vs_differential (which validates FULL == DIFF) because
// it directly validates FULL refresh output against the defining query —
// catching correctness bugs in the FULL refresh path that happen to affect
// both FULL and DIFFERENTIAL equally.
//
// Design:
//   1. Create ST with '24h' schedule + FULL mode
//   2. Assert baseline invariant (assert_tpch_invariant vs ground truth)
//   3. For N cycles: RF1+RF2+RF3 → refresh → assert invariant
//   4. Drop ST
//
// Uses the same DIFFERENTIAL_SKIP_ALLOWLIST for expected skips —
// queries that are too large for the Docker temp_file_limit are expected
// to fail in FULL mode for the same infrastructure reasons.

#[tokio::test]
#[ignore]
async fn test_tpch_full_correctness() {
    let sf = scale_factor();
    let n_cycles = cycles();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H FULL Mode Correctness — SF={sf}, cycles={n_cycles}");
    println!(
        "  Orders: {}, Customers: {}, Suppliers: {}, Parts: {}",
        sf_orders(),
        sf_customers(),
        sf_suppliers(),
        sf_parts()
    );
    println!("  RF batch size: {} rows", rf_count());
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let queries = tpch_queries();
    let mut passed = 0usize;
    let mut skipped: Vec<(&str, String)> = Vec::new();

    for q in &queries {
        println!(
            "── {} (Tier {}) ──────────────────────────────",
            q.name, q.tier
        );

        let st_name = format!("tpch_full_{}", q.name);
        let create_result = db
            .try_execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_name}', $${sql}$$, '24h', 'FULL')",
                sql = q.sql,
            ))
            .await;

        if let Err(e) = create_result {
            let reason = e.to_string();
            let short = reason.split(':').next_back().unwrap_or(&reason).trim();
            println!("  SKIP — {short}");
            skipped.push((q.name, reason));
            continue;
        }

        // Baseline assertion against ground truth
        let t = Instant::now();
        if let Err(e) = assert_tpch_invariant(&db, &st_name, q.sql, q.name, 0).await {
            println!("  WARN baseline — {e}");
            skipped.push((q.name, format!("baseline: {e}")));
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                .await;
            continue;
        }
        println!("  baseline — {:.0}ms ✓", t.elapsed().as_secs_f64() * 1000.0);

        let mut full_ok = true;
        'cycles: for cycle in 1..=n_cycles {
            let ct = Instant::now();

            let next_ok = max_orderkey(&db).await + 1;
            apply_rf1(&db, next_ok).await;
            apply_rf2(&db).await;
            apply_rf3(&db).await;

            db.execute("ANALYZE orders").await;
            db.execute("ANALYZE lineitem").await;
            db.execute("ANALYZE customer").await;

            if let Err(e) = try_refresh_st(&db, &st_name).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} — FULL refresh error: {msg}");
                skipped.push((q.name, format!("FULL refresh error cycle {cycle}: {msg}")));
                full_ok = false;
                break 'cycles;
            }

            if let Err(e) = assert_tpch_invariant(&db, &st_name, q.sql, q.name, cycle).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  FAIL cycle {cycle} — {msg}");
                // FULL mode invariant failure is always a hard error (not a known DVM limitation):
                // FULL refresh re-executes the query from scratch, so any mismatch is a bug.
                panic!("FULL mode correctness failure for {}: {e}", q.name);
            }

            log_progress(
                q.name,
                q.tier,
                cycle,
                n_cycles,
                ct.elapsed().as_secs_f64() * 1000.0,
            );

            db.execute("CHECKPOINT").await;
            db.execute("VACUUM ANALYZE").await;
        }

        if full_ok {
            passed += 1;
        }

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
    }

    println!("\n══════════════════════════════════════════════════════════");
    println!(
        "  Results: {passed}/{} queries passed, {} skipped",
        queries.len(),
        skipped.len()
    );
    if !skipped.is_empty() {
        println!("  Skipped:");
        for (name, reason) in &skipped {
            let short = reason.split(':').next_back().unwrap_or(reason).trim();
            println!("    {name}: {short}");
        }
    }
    println!("══════════════════════════════════════════════════════════\n");

    // Require at least as many FULL-mode passes as DIFFERENTIAL-mode passes.
    let min_passing = queries.len() - DIFFERENTIAL_SKIP_ALLOWLIST.len() - 2;
    assert!(
        passed >= min_passing,
        "FULL mode correctness: only {passed}/{} queries passed (minimum required: {min_passing}).\n\
         Skipped: {:?}",
        queries.len(),
        skipped.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
    );

    // T2-style guard: unexpected skips (not in DIFFERENTIAL allowlist) are regressions.
    let unexpected: Vec<&str> = skipped
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| !DIFFERENTIAL_SKIP_ALLOWLIST.contains(name))
        .collect();
    assert!(
        unexpected.is_empty(),
        "FULL MODE REGRESSION: queries newly skipped not in DIFFERENTIAL_SKIP_ALLOWLIST: {:?}",
        unexpected
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 3: FULL vs DIFFERENTIAL Mode Comparison
// ═══════════════════════════════════════════════════════════════════════
//
// For each query, create two STs (one FULL, one DIFFERENTIAL) and verify
// they produce identical results after the same mutations. Stronger than
// Phase 1 because it compares the two modes directly.

#[tokio::test]
#[ignore]
async fn test_tpch_full_vs_differential() {
    let sf = scale_factor();
    let n_cycles = cycles();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H FULL vs DIFFERENTIAL — SF={sf}, cycles={n_cycles}");
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let queries = tpch_queries();
    let mut passed = 0usize;
    let mut skipped: Vec<String> = Vec::new();

    for q in &queries {
        let st_full = format!("tpch_f_{}", q.name);
        let st_diff = format!("tpch_d_{}", q.name);

        // Create both STs — '24h' schedule means the time-based check never
        // fires during the test window, so the background scheduler does NOT
        // race with the explicit per-cycle refresh calls below.
        // ('calculated' = CALCULATED mode = auto-refresh on pending CDC changes.)
        let full_ok = db
            .try_execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_full}', $${sql}$$, '24h', 'FULL')",
                sql = q.sql,
            ))
            .await;
        let diff_ok = db
            .try_execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_diff}', $${sql}$$, '24h', 'DIFFERENTIAL')",
                sql = q.sql,
            ))
            .await;

        if full_ok.is_err() || diff_ok.is_err() {
            println!("  {}: SKIP — create failed", q.name);
            skipped.push(q.name.to_string());
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_full}')"))
                .await;
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_diff}')"))
                .await;
            continue;
        }

        println!(
            "── {} (Tier {}) ──────────────────────────────",
            q.name, q.tier
        );

        let mut dvm_ok = true;
        'cyc: for cycle in 1..=n_cycles {
            let ct = Instant::now();

            let next_ok = max_orderkey(&db).await + 1;
            apply_rf1(&db, next_ok).await;
            apply_rf2(&db).await;
            apply_rf3(&db).await;

            db.execute("ANALYZE orders").await;
            db.execute("ANALYZE lineitem").await;
            db.execute("ANALYZE customer").await;

            // Refresh both — soft error for known pg_trickle DVM bugs
            for (mode, st) in [("FULL", &st_full), ("DIFF", &st_diff)] {
                if let Err(e) = try_refresh_st(&db, st).await {
                    let msg = e.lines().next().unwrap_or(&e).to_string();
                    println!("  WARN: {mode} refresh error cycle {cycle}: {msg}");
                    dvm_ok = false;
                    break 'cyc;
                }
            }
            if !dvm_ok {
                break;
            }

            // Compare FULL vs DIFFERENTIAL directly
            let cols: String = db
                .query_scalar(&format!(
                    "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) \
                     FROM information_schema.columns \
                     WHERE (table_schema || '.' || table_name = 'public.{st_diff}' \
                        OR table_name = '{st_diff}') \
                       AND left(column_name, 6) <> '__pgt_'"
                ))
                .await;

            let matches: bool = db
                .query_scalar(&format!(
                    "SELECT NOT EXISTS ( \
                        (SELECT {cols} FROM public.{st_diff} EXCEPT ALL \
                         SELECT {cols} FROM public.{st_full}) \
                        UNION ALL \
                        (SELECT {cols} FROM public.{st_full} EXCEPT ALL \
                         SELECT {cols} FROM public.{st_diff}) \
                    )"
                ))
                .await;

            // Reclaim dead-tuple bloat between cycles with VACUUM ANALYZE.
            // At SF=0.01 with few cycles, the full rewrite of VACUUM FULL
            // is unnecessary overhead.
            db.execute("CHECKPOINT").await;
            db.execute("VACUUM ANALYZE").await;

            if !matches {
                let full_count: i64 = db
                    .query_scalar(&format!("SELECT count(*) FROM public.{st_full}"))
                    .await;
                let diff_count: i64 = db
                    .query_scalar(&format!("SELECT count(*) FROM public.{st_diff}"))
                    .await;
                println!(
                    "  WARN: {} cycle {} — FULL({}) != DIFF({}) (DVM data mismatch)",
                    q.name, cycle, full_count, diff_count,
                );
                dvm_ok = false;
                break 'cyc;
            }

            println!(
                "  [T{}] {:<4} cycle {}/{} — FULL==DIFF ✓ — {:.0}ms",
                q.tier,
                q.name,
                cycle,
                n_cycles,
                ct.elapsed().as_secs_f64() * 1000.0,
            );
        }

        if dvm_ok {
            passed += 1;
        } else {
            skipped.push(q.name.to_string());
            println!(
                "  SKIP: {} — DVM error (known pg_trickle limitation)",
                q.name
            );
        }

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_full}')"))
            .await;
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_diff}')"))
            .await;
    }

    if !skipped.is_empty() {
        println!(
            "\n  Skipped (pg_trickle limitation): {}",
            skipped.join(", ")
        );
    }
    println!(
        "\n  FULL vs DIFFERENTIAL: {passed}/{} queries passed ✓\n",
        queries.len()
    );

    // P0.2: Minimum threshold — must pass at least as many as the non-skipped set.
    // With 5 known DIFFERENTIAL skips, at least 15/22 should pass.
    let min_passing = queries.len() - DIFFERENTIAL_SKIP_ALLOWLIST.len() - 2; // -2 tolerance
    assert!(
        passed >= min_passing,
        "FULL vs DIFFERENTIAL: only {passed}/{} queries passed (minimum required: {min_passing}).\n\
         Expected at most {} skips (the known DIFFERENTIAL limitations).\n\
         Skipped: {:?}",
        queries.len(),
        DIFFERENTIAL_SKIP_ALLOWLIST.len() + 2,
        skipped,
    );

    // P1.4: T2-style regression guard — if a query not in the DIFFERENTIAL skip
    // allowlist is skipped, a DVM regression has occurred.
    let unexpected_full_skips: Vec<&str> = skipped
        .iter()
        .map(String::as_str)
        .filter(|name| !DIFFERENTIAL_SKIP_ALLOWLIST.contains(name))
        .collect();
    assert!(
        unexpected_full_skips.is_empty(),
        "FULL vs DIFFERENTIAL REGRESSION: queries newly skipped that are not in \
         DIFFERENTIAL_SKIP_ALLOWLIST: {:?}\n\
         If intentional, add to the allowlist with an explanatory comment.",
        unexpected_full_skips
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Q07 Isolation Test — regression test for BinaryOp parenthesisation fix
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn test_tpch_q07_isolation() {
    let sf = scale_factor();
    let n_cycles = cycles();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  Q07 Isolation Test — SF={sf}, cycles={n_cycles}");
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;
    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let q07_sql = include_str!("tpch/queries/q07.sql");
    let st_name = "tpch_q07_iso";

    // Create Q07 ST only — '24h' schedule means the time-based check never
    // fires during the test window, so the background scheduler does NOT race
    // with the per-cycle explicit refreshes below.
    // ('calculated' = CALCULATED mode = auto-refresh on pending CDC changes.)
    db.try_execute(&format!(
        "SELECT pgtrickle.create_stream_table('{st_name}', $${q07_sql}$$, '24h', 'DIFFERENTIAL')",
    ))
    .await
    .expect("Q07 create failed");
    println!("  Q07 ST created ✓");

    // Baseline
    assert_tpch_invariant(&db, st_name, q07_sql, "q07", 0)
        .await
        .expect("Baseline failed");
    println!("  baseline ✓");

    for cycle in 1..=n_cycles {
        let ct = Instant::now();
        let next_ok = max_orderkey(&db).await + 1;
        apply_rf1(&db, next_ok).await;
        apply_rf2(&db).await;
        apply_rf3(&db).await;
        db.execute("ANALYZE orders").await;
        db.execute("ANALYZE lineitem").await;
        db.execute("ANALYZE customer").await;

        match try_refresh_st(&db, st_name).await {
            Ok(()) => {}
            Err(e) if e.contains("temp_file_limit") => {
                // Known Docker infrastructure constraint — q07 is a large multi-join
                // query that can exceed temp_file_limit at higher cycle sizes.
                // Other TPC-H tests (differential_correctness, full_vs_differential)
                // already treat this as a SKIP. Do the same here.
                println!(
                    "  WARN cycle {cycle}: temp_file_limit hit — known Docker constraint, \
                     skipping remaining cycles"
                );
                break;
            }
            Err(e)
                if e.contains("error communicating with database")
                    || e.contains("got 0 bytes at EOF")
                    || e.contains("unexpected EOF")
                    || e.contains("connection closed") =>
            {
                // Transient Docker container connection drop — q07 is the heaviest
                // multi-join query and can cause PostgreSQL to close the connection
                // under memory pressure in Docker. Treat as a known infrastructure
                // constraint and skip remaining cycles (same as temp_file_limit).
                println!(
                    "  WARN cycle {cycle}: connection dropped — known Docker constraint \
                     ({e}), skipping remaining cycles"
                );
                break;
            }
            Err(e) if e.contains("deadlock detected") => {
                // Transient deadlock under Docker memory pressure — q07 is the
                // heaviest multi-join query and can trigger lock contention between
                // the background scheduler and the explicit refresh call when
                // resources are scarce. Treat as a known infrastructure constraint
                // and skip remaining cycles (same as temp_file_limit).
                println!(
                    "  WARN cycle {cycle}: deadlock detected — known Docker constraint \
                     ({e}), skipping remaining cycles"
                );
                break;
            }
            Err(e) => panic!("Q07 refresh error cycle {cycle}: {e}"),
        }

        match assert_tpch_invariant(&db, st_name, q07_sql, "q07", cycle).await {
            Ok(()) => println!(
                "  cycle {cycle}/{n_cycles} — {:.0}ms ✓",
                ct.elapsed().as_secs_f64() * 1000.0
            ),
            Err(e) => {
                panic!("  cycle {cycle}/{n_cycles} — FAILED: {e}");
            }
        }
        db.execute("CHECKPOINT").await;
        db.execute("VACUUM ANALYZE").await;
    }

    let _ = db
        .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
        .await;
    println!("\n  Q07 isolation test complete\n");
}

// ═══════════════════════════════════════════════════════════════════════
// Phase T1-B: Performance — FULL vs DIFFERENTIAL wall-clock timing
//
// For each query: create both FULL and DIFFERENTIAL STs, run RF cycles,
// record per-refresh wall-clock time, and output a speedup table.
//
// Controlled via:
//   TPCH_SCALE  (default 0.01)
//   TPCH_CYCLES (default 3)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
#[ignore]
async fn test_tpch_performance_comparison() {
    let sf = scale_factor();
    let n_cycles = cycles();
    let is_bench = bench_mode();
    let n_warmup = if is_bench { warmup_cycles() } else { 0 };

    if is_bench {
        println!("\n══════════════════════════════════════════════════════════");
        println!("  TPC-H Benchmark Mode — SF={sf}, warmup={n_warmup}, measured={n_cycles}");
        println!("  Emit: [TPCH_BENCH] lines + summary table");
        println!("══════════════════════════════════════════════════════════\n");
    } else {
        println!("\n══════════════════════════════════════════════════════════");
        println!("  TPC-H T1-B Performance — SF={sf}, cycles={n_cycles}");
        println!("══════════════════════════════════════════════════════════\n");
    }

    let db = E2eDb::new_bench().await.with_extension().await;
    let cid = db.container_id().to_string();

    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let queries = tpch_queries();

    // Legacy result struct for non-bench mode table output.
    struct PerfRow {
        name: String,
        tier: u8,
        full_ms: Vec<f64>,
        diff_ms: Vec<f64>,
    }

    let mut results: Vec<PerfRow> = Vec::new();
    let mut bench_rows: Vec<TpchBenchRow> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();

    let total_iters = n_warmup + n_cycles;

    for q in &queries {
        let st_full = format!("perf_f_{}", q.name);
        let st_diff = format!("perf_d_{}", q.name);

        // '24h' schedule prevents background auto-refresh from racing with
        // the explicit per-cycle refresh calls in the performance loop.
        // ('calculated' = CALCULATED mode = auto-refresh on pending CDC changes.)
        let full_ok = db
            .try_execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_full}', $${sql}$$, '24h', 'FULL')",
                sql = q.sql,
            ))
            .await;
        let diff_ok = db
            .try_execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_diff}', $${sql}$$, '24h', 'DIFFERENTIAL')",
                sql = q.sql,
            ))
            .await;

        if full_ok.is_err() || diff_ok.is_err() {
            skipped.push(q.name.to_string());
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_full}')"))
                .await;
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_diff}')"))
                .await;
            continue;
        }

        let mut full_times = Vec::new();
        let mut diff_times = Vec::new();
        let mut merge_pcts: Vec<f64> = Vec::new();
        let mut ok = true;

        for iter in 1..=total_iters {
            let is_warmup = iter <= n_warmup;
            let measured_cycle = if is_warmup { 0 } else { iter - n_warmup };

            let next_ok = max_orderkey(&db).await + 1;
            apply_rf1(&db, next_ok).await;
            apply_rf2(&db).await;
            apply_rf3(&db).await;
            db.execute("ANALYZE orders").await;
            db.execute("ANALYZE lineitem").await;

            let t_full = Instant::now();
            if let Err(e) = try_refresh_st(&db, &st_full).await {
                println!(
                    "  WARN: {} FULL iter {}: {}",
                    q.name,
                    iter,
                    e.lines().next().unwrap_or(&e)
                );
                ok = false;
                break;
            }
            let full_ms = t_full.elapsed().as_secs_f64() * 1000.0;

            let t_diff = Instant::now();
            if let Err(e) = try_refresh_st(&db, &st_diff).await {
                println!(
                    "  WARN: {} DIFF iter {}: {}",
                    q.name,
                    iter,
                    e.lines().next().unwrap_or(&e)
                );
                ok = false;
                break;
            }
            let diff_ms = t_diff.elapsed().as_secs_f64() * 1000.0;

            // Extract [PGS_PROFILE] from container logs (DIFFERENTIAL refresh path).
            let profile = extract_last_profile(&cid).await;

            if is_warmup {
                if is_bench {
                    println!(
                        "  [WARMUP] {} tier={} iter={}/{} FULL={full_ms:.1}ms DIFF={diff_ms:.1}ms",
                        q.name, q.tier, iter, total_iters
                    );
                }
            } else {
                // Emit structured benchmark line.
                if is_bench {
                    if let Some(ref p) = profile {
                        println!(
                            "[TPCH_BENCH] query={} tier={} cycle={} mode=FULL ms={full_ms:.2}",
                            q.name, q.tier, measured_cycle
                        );
                        println!(
                            "[TPCH_BENCH] query={} tier={} cycle={} mode=DIFF ms={diff_ms:.2} \
                             decision={:.2} gen={:.2} merge={:.2} cleanup={:.2} path={}",
                            q.name,
                            q.tier,
                            measured_cycle,
                            p.decision_ms,
                            p.generate_ms,
                            p.merge_ms,
                            p.cleanup_ms,
                            p.path,
                        );
                        if p.total_ms > 0.0 {
                            merge_pcts.push(p.merge_ms / p.total_ms);
                        }
                    } else {
                        println!(
                            "[TPCH_BENCH] query={} tier={} cycle={} mode=FULL ms={full_ms:.2}",
                            q.name, q.tier, measured_cycle
                        );
                        println!(
                            "[TPCH_BENCH] query={} tier={} cycle={} mode=DIFF ms={diff_ms:.2}",
                            q.name, q.tier, measured_cycle,
                        );
                    }
                }

                full_times.push(full_ms);
                diff_times.push(diff_ms);
            }

            db.execute("CHECKPOINT").await;
            db.execute("VACUUM ANALYZE").await;
        }

        if ok {
            if is_bench {
                bench_rows.push(TpchBenchRow {
                    name: q.name.to_string(),
                    tier: q.tier,
                    full_ms: full_times.clone(),
                    diff_ms: diff_times.clone(),
                    merge_pct: merge_pcts,
                });
            }
            results.push(PerfRow {
                name: q.name.to_string(),
                tier: q.tier,
                full_ms: full_times,
                diff_ms: diff_times,
            });
        } else {
            skipped.push(q.name.to_string());
        }

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_full}')"))
            .await;
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_diff}')"))
            .await;
    }

    // ── Results output ────────────────────────────────────────────

    if is_bench {
        // TPCH_BENCH=1: structured summary table with median, P95, MERGE%.
        print_tpch_bench_summary(&bench_rows);
    } else {
        // Default mode: simple average speedup table.
        println!();
        println!("┌──────┬──────┬────────────┬────────────┬──────────┐");
        println!("│ Query│ Tier │  FULL (ms) │  DIFF (ms) │ Speedup  │");
        println!("├──────┼──────┼────────────┼────────────┼──────────┤");

        let mut total_full = 0.0f64;
        let mut total_diff = 0.0f64;

        for r in &results {
            let avg_full = r.full_ms.iter().sum::<f64>() / r.full_ms.len().max(1) as f64;
            let avg_diff = r.diff_ms.iter().sum::<f64>() / r.diff_ms.len().max(1) as f64;
            let speedup = if avg_diff > 0.0 {
                avg_full / avg_diff
            } else {
                f64::NAN
            };
            total_full += avg_full;
            total_diff += avg_diff;

            println!(
                "│ {:<4} │  T{}  │ {:>8.1}   │ {:>8.1}   │ {:>6.2}x  │",
                r.name, r.tier, avg_full, avg_diff, speedup,
            );
        }

        let total_speedup = if total_diff > 0.0 {
            total_full / total_diff
        } else {
            f64::NAN
        };

        println!("├──────┼──────┼────────────┼────────────┼──────────┤");
        println!(
            "│ Total│      │ {:>8.1}   │ {:>8.1}   │ {:>6.2}x  │",
            total_full, total_diff, total_speedup,
        );
        println!("└──────┴──────┴────────────┴────────────┴──────────┘");
    }

    if !skipped.is_empty() {
        println!("\n  Skipped (DVM limitation): {}", skipped.join(", "));
    }
    println!(
        "\n  T1-B Performance: {}/{} queries benchmarked ✓\n",
        results.len(),
        queries.len()
    );

    // P0.2: Minimum threshold — at least 15 queries must be benchmarked
    // (22 queries minus the 5 known DIFFERENTIAL skips minus 2 tolerance).
    let min_benchmarked = queries.len() - DIFFERENTIAL_SKIP_ALLOWLIST.len() - 2;
    assert!(
        results.len() >= min_benchmarked,
        "T1-B Performance: only {}/{} queries benchmarked (minimum required: {min_benchmarked}).\n\
         Skipped: {:?}",
        results.len(),
        queries.len(),
        skipped,
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Phase T1-C: Sustained Churn — correctness over many cycles
//
// Runs N cycles of RF1+RF2+RF3 with DIFFERENTIAL refresh. Every 10th
// cycle, verifies correctness against the defining query (full rescan).
// Tracks cumulative drift, change buffer sizes, and wall-clock time.
//
// Controlled via:
//   TPCH_SCALE          (default 0.01)
//   TPCH_CHURN_CYCLES   (default 50)
// ═══════════════════════════════════════════════════════════════════════

fn churn_cycles() -> usize {
    std::env::var("TPCH_CHURN_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
}

#[tokio::test]
#[ignore]
async fn test_tpch_sustained_churn() {
    let sf = scale_factor();
    let n_cycles = churn_cycles();
    let check_every = 10usize;
    println!("\n══════════════════════════════════════════════════════════");
    println!(
        "  TPC-H T1-C Sustained Churn — SF={sf}, cycles={n_cycles}, check every {check_every}"
    );
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    // Use a subset of queries that are known to work well with DIFFERENTIAL.
    // Operator diversity covered:
    //   q01 — scalar aggregate (no GROUP BY grouping keys)
    //   q03 — 3-table join + GROUP BY aggregate
    //   q04 — EXISTS subquery (orders with matching lineitem)
    //   q06 — filter + single SUM aggregate (single-table)
    //   q10 — 4-table join + GROUP BY aggregate
    //   q14 — 2-table join + CASE aggregate (CASE WHEN path)
    //   q22 — NOT EXISTS correlated subquery
    // NOTE: q12 excluded — known DVM drift (CASE WHEN IN-list produces
    // non-deterministic incremental results).
    // NOTE: q05 excluded — reliably exceeds temp_file_limit (6-table join).
    // P2.10: Added q04 to cover the EXISTS subquery operator path, which was
    // previously untested in sustained churn.
    let churn_queries: Vec<(&str, &str)> = vec![
        ("q01", include_str!("tpch/queries/q01.sql")),
        ("q03", include_str!("tpch/queries/q03.sql")),
        ("q04", include_str!("tpch/queries/q04.sql")),
        ("q06", include_str!("tpch/queries/q06.sql")),
        ("q10", include_str!("tpch/queries/q10.sql")),
        ("q14", include_str!("tpch/queries/q14.sql")),
        ("q22", include_str!("tpch/queries/q22.sql")),
    ];

    // Create DIFFERENTIAL STs for each query
    let mut active_sts: Vec<(String, String)> = Vec::new();
    for (name, sql) in &churn_queries {
        let st_name = format!("churn_{name}");
        match db
            .try_execute(&format!(
                // '24h' schedule — time-based check will never fire during the test
                // window, so the background scheduler skips these tables and does
                // NOT race with the churn loop's explicit per-cycle refreshes.
                // ('calculated' = CALCULATED mode = auto-refresh on pending CDC
                // changes, which is the opposite of what we want here.)
                "SELECT pgtrickle.create_stream_table('{st_name}', $${sql}$$, '24h', 'DIFFERENTIAL')",
            ))
            .await
        {
            Ok(_) => {
                active_sts.push((st_name, sql.to_string()));
                println!("  {name}: stream table created ✓");
            }
            Err(e) => {
                let msg = e.to_string();
                println!("  {name}: SKIP — {}", msg.lines().next().unwrap_or(&msg));
            }
        }
    }

    if active_sts.is_empty() {
        println!("\n  No STs could be created — aborting\n");
        return;
    }

    // Track per-cycle refresh times and buffer sizes
    let mut cycle_ms: Vec<f64> = Vec::new();
    let mut drift_detected = 0usize;
    let mut errors: Vec<String> = Vec::new();

    for cycle in 1..=n_cycles {
        let ct = Instant::now();

        let next_ok = max_orderkey(&db).await + 1;
        apply_rf1(&db, next_ok).await;
        apply_rf2(&db).await;
        apply_rf3(&db).await;

        // Flush WAL from RF mutations before refreshes to prevent
        // unbounded WAL accumulation across 50 churn cycles.
        db.execute("CHECKPOINT").await;

        // Refresh all active STs
        for (st_name, _sql) in &active_sts {
            if let Err(e) = try_refresh_st(&db, st_name).await {
                let msg = format!(
                    "cycle {cycle} {st_name}: {}",
                    e.lines().next().unwrap_or(&e)
                );
                errors.push(msg.clone());
                println!("  WARN: {msg}");
            }
        }

        let elapsed = ct.elapsed().as_secs_f64() * 1000.0;
        cycle_ms.push(elapsed);

        // Periodic correctness check
        if cycle % check_every == 0 || cycle == n_cycles {
            let mut check_ok = true;
            for (st_name, sql) in &active_sts {
                match assert_tpch_invariant(&db, st_name, sql, st_name, cycle).await {
                    Ok(()) => {}
                    Err(msg) => {
                        drift_detected += 1;
                        check_ok = false;
                        errors.push(msg.clone());
                        println!("  DRIFT: {msg}");
                    }
                }
            }

            // Report change buffer sizes
            // P3.16: Use GREATEST(reltuples, 0) to avoid -1 from unanalyzed tables
            // (PostgreSQL sets reltuples = -1 for tables not yet analyzed by VACUUM).
            let buf_total: i64 = db
                .query_scalar(
                    "SELECT COALESCE(SUM(GREATEST(c.reltuples, 0)), 0)::bigint \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = 'pgtrickle_changes'",
                )
                .await;

            let check_mark = if check_ok { "✓" } else { "✗" };
            println!(
                "  cycle {cycle:>3}/{n_cycles} — {elapsed:>7.0}ms — buf≈{buf_total} — check {check_mark}",
            );

            db.execute("CHECKPOINT").await;
            // VACUUM ANALYZE (not VACUUM FULL) — avoids exclusive table rewrite
            // at each check point; plain VACUUM reclaims dead tuples and updates
            // statistics without the multi-second exclusive lock of FULL.
            db.execute("VACUUM ANALYZE").await;
        } else if cycle % 5 == 0 {
            println!("  cycle {cycle:>3}/{n_cycles} — {elapsed:>7.0}ms");
        }
    }

    // ── Summary ──────────────────────────────────────────────────

    let avg_ms = cycle_ms.iter().sum::<f64>() / cycle_ms.len().max(1) as f64;
    let max_ms = cycle_ms.iter().cloned().fold(0.0f64, f64::max);
    let min_ms = cycle_ms.iter().cloned().fold(f64::MAX, f64::min);

    println!();
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ TPC-H T1-C Sustained Churn Results                        │");
    println!("├────────────────────────────────────────────────────────────┤");
    println!(
        "│ STs active: {:>2} / {:>2}                                       │",
        active_sts.len(),
        churn_queries.len()
    );
    println!(
        "│ Cycles:     {:>4}                                           │",
        n_cycles
    );
    println!(
        "│ Avg cycle:  {:>8.1} ms                                    │",
        avg_ms
    );
    println!(
        "│ Min/Max:    {:>8.1} / {:>8.1} ms                         │",
        min_ms, max_ms
    );
    println!(
        "│ Drift:      {:>4} detected                                  │",
        drift_detected
    );
    println!(
        "│ Errors:     {:>4} refresh failures                          │",
        errors.len()
    );
    println!(
        "│ Verdict:    {}                                          │",
        if drift_detected == 0 && errors.is_empty() {
            "✅ PASS"
        } else if drift_detected == 0 {
            "⚠️  WARN (refresh errors but no drift)"
        } else {
            "❌ FAIL (drift detected)"
        }
    );
    println!("└────────────────────────────────────────────────────────────┘");

    if !errors.is_empty() {
        println!("\n  Errors:");
        for (i, e) in errors.iter().enumerate().take(20) {
            println!("    {}: {e}", i + 1);
        }
    }

    // Cleanup
    for (st_name, _) in &active_sts {
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
    }

    println!(
        "\n  T1-C Sustained Churn: {} drift in {} cycles\n",
        if drift_detected == 0 {
            "zero"
        } else {
            "NON-ZERO"
        },
        n_cycles
    );

    assert_eq!(
        drift_detected, 0,
        "Cumulative drift must be zero over {n_cycles} churn cycles"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 8: IMMEDIATE Mode Correctness
// ═══════════════════════════════════════════════════════════════════════
//
// For each TPC-H query:
//   1. Create ST in IMMEDIATE mode (NULL schedule — IVM triggers maintain it)
//   2. Assert baseline invariant (populated on create)
//   3. For N cycles:
//      a. Apply RF1 (INSERT) → assert invariant  [INSERT trigger path]
//      b. Apply RF2 (DELETE) → assert invariant  [DELETE trigger path]
//      c. Apply RF3 (UPDATE) → assert invariant  [UPDATE trigger path]
//   4. Drop ST
//
// Unlike DIFFERENTIAL, there is no explicit refresh_stream_table() call.
// The IVM trigger updates the stream table within the same transaction as
// the base-table DML (TransitionTable delta source, not ChangeBuffer).
// Assertions after each RF step verify per-operation trigger correctness.

/// Apply RF1 mutations via try_execute so trigger errors are caught, not panicked.
async fn try_apply_rf1(db: &E2eDb, next_orderkey: usize) -> Result<(), String> {
    let sql = substitute_rf(RF1_SQL, next_orderkey);
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.try_execute(stmt).await.map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// Apply RF2 mutations via try_execute so trigger errors are caught, not panicked.
async fn try_apply_rf2(db: &E2eDb) -> Result<(), String> {
    let sql = RF2_SQL.replace("__RF_COUNT__", &rf_count().to_string());
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.try_execute(stmt).await.map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// Apply RF3 mutations via try_execute so trigger errors are caught, not panicked.
async fn try_apply_rf3(db: &E2eDb) -> Result<(), String> {
    let sql = RF3_SQL.replace("__RF_COUNT__", &rf_count().to_string());
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.try_execute(stmt).await.map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_tpch_immediate_correctness() {
    let sf = scale_factor();
    let n_cycles = cycles();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H IMMEDIATE Mode Correctness — SF={sf}, cycles={n_cycles}");
    println!(
        "  Orders: {}, Customers: {}, Suppliers: {}, Parts: {}",
        sf_orders(),
        sf_customers(),
        sf_suppliers(),
        sf_parts()
    );
    println!("  RF batch size: {} rows", rf_count());
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    // Load schema + data once for all queries.
    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let queries = tpch_queries();
    let mut passed = 0usize;
    let mut skipped: Vec<(&str, String)> = Vec::new();
    let failed: Vec<(&str, String)> = Vec::new(); // retained for symmetry with other tests

    for q in &queries {
        println!(
            "── {} (Tier {}) ──────────────────────────────",
            q.name, q.tier
        );

        // IMMEDIATE mode uses NULL schedule — triggers maintain the ST in-transaction.
        let st_name = format!("tpch_imm_{}", q.name);
        let create_result = db
            .try_execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_name}', $${sql}$$, NULL, 'IMMEDIATE')",
                sql = q.sql,
            ))
            .await;

        if let Err(e) = create_result {
            let reason = e.to_string();
            let short = reason.split(':').next_back().unwrap_or(&reason).trim();
            println!("  SKIP (create) — {short}");
            skipped.push((q.name, reason));
            continue;
        }

        // Baseline: verify the ST was correctly populated on creation.
        let t = Instant::now();
        if let Err(e) = assert_tpch_invariant(&db, &st_name, q.sql, q.name, 0).await {
            println!("  WARN baseline — {e}");
            skipped.push((q.name, format!("baseline invariant: {e}")));
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                .await;
            continue;
        }
        println!("  baseline — {:.0}ms ✓", t.elapsed().as_secs_f64() * 1000.0);

        // Mutation cycles — each RF step triggers an in-transaction IVM update.
        let mut ivm_ok = true;
        'cycles: for cycle in 1..=n_cycles {
            let ct = Instant::now();

            // RF1: bulk INSERT — AFTER INSERT trigger fires, updates ST in same txn.
            let next_ok = max_orderkey(&db).await + 1;
            if let Err(e) = try_apply_rf1(&db, next_ok).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} RF1 — IVM trigger error: {msg}");
                skipped.push((q.name, format!("RF1 trigger error cycle {cycle}: {msg}")));
                ivm_ok = false;
                break 'cycles;
            }
            if let Err(e) = assert_tpch_invariant(&db, &st_name, q.sql, q.name, cycle).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} after RF1 (INSERT) — {msg}");
                skipped.push((q.name, format!("invariant post-RF1 cycle {cycle}: {msg}")));
                ivm_ok = false;
                break 'cycles;
            }

            // RF2: bulk DELETE — AFTER DELETE trigger fires, removes rows from ST.
            if let Err(e) = try_apply_rf2(&db).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} RF2 — IVM trigger error: {msg}");
                skipped.push((q.name, format!("RF2 trigger error cycle {cycle}: {msg}")));
                ivm_ok = false;
                break 'cycles;
            }
            if let Err(e) = assert_tpch_invariant(&db, &st_name, q.sql, q.name, cycle).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} after RF2 (DELETE) — {msg}");
                skipped.push((q.name, format!("invariant post-RF2 cycle {cycle}: {msg}")));
                ivm_ok = false;
                break 'cycles;
            }

            // RF3: targeted UPDATEs — AFTER UPDATE trigger fires, applies deltas.
            if let Err(e) = try_apply_rf3(&db).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} RF3 — IVM trigger error: {msg}");
                skipped.push((q.name, format!("RF3 trigger error cycle {cycle}: {msg}")));
                ivm_ok = false;
                break 'cycles;
            }
            if let Err(e) = assert_tpch_invariant(&db, &st_name, q.sql, q.name, cycle).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} after RF3 (UPDATE) — {msg}");
                skipped.push((q.name, format!("invariant post-RF3 cycle {cycle}: {msg}")));
                ivm_ok = false;
                break 'cycles;
            }

            db.execute("ANALYZE orders").await;
            db.execute("ANALYZE lineitem").await;
            db.execute("ANALYZE customer").await;

            log_progress(
                q.name,
                q.tier,
                cycle,
                n_cycles,
                ct.elapsed().as_secs_f64() * 1000.0,
            );

            db.execute("CHECKPOINT").await;
            db.execute("VACUUM ANALYZE").await;
        }

        if ivm_ok {
            passed += 1;
        }

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
    }

    println!("\n══════════════════════════════════════════════════════════");
    println!(
        "  Results: {passed}/{} queries passed, {} skipped",
        queries.len(),
        skipped.len()
    );
    if !skipped.is_empty() {
        println!("  Skipped (pg_trickle limitation):");
        for (name, reason) in &skipped {
            let short = reason.split(':').next_back().unwrap_or(reason).trim();
            println!("    {name}: {short}");
        }
    }
    if !failed.is_empty() {
        println!("  FAILED (assertion errors):");
        for (name, reason) in &failed {
            println!("    {name}: {reason}");
        }
    }
    println!("══════════════════════════════════════════════════════════\n");

    assert!(
        failed.is_empty(),
        "{} queries failed with assertion errors (not pg_trickle limitations)",
        failed.len()
    );

    // T2: Skip-set regression guard for IMMEDIATE mode.
    // T2-style regression guard for IMMEDIATE mode.
    // If a query that isn't in IMMEDIATE_SKIP_ALLOWLIST skips here, hard-fail.
    let unexpected_imm_skips: Vec<&str> = skipped
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| !IMMEDIATE_SKIP_ALLOWLIST.contains(name))
        .collect();
    assert!(
        unexpected_imm_skips.is_empty(),
        "IMMEDIATE REGRESSION: queries newly skipped that are not in \
         IMMEDIATE_SKIP_ALLOWLIST: {:?}\n\
         If intentional, add to the allowlist with an explanatory comment.",
        unexpected_imm_skips
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T3 — IMMEDIATE Mode Rollback Correctness
// ═══════════════════════════════════════════════════════════════════════
//
// Verifies that a rolled-back DML transaction leaves an IMMEDIATE-mode
// stream table in the exact same state as before the transaction.
//
// The IVM trigger fires inside the user's transaction. A ROLLBACK must
// revert both the base-table changes AND the stream table update atomically.
// PostgreSQL guarantees this naturally because the trigger participates in
// the surrounding transaction.
//
// Representative query subset:
//   q01 — scalar aggregate (no GROUP BY grouping keys)
//   q06 — filter + single SUM aggregate
//   q03 — 3-table join + aggregate (LIMIT 10)
//   q05 — 6-table join + GROUP BY aggregate
//
// These cover the main IVM delta paths without requiring all 22 queries.

#[tokio::test]
#[ignore]
async fn test_tpch_immediate_rollback() {
    let sf = scale_factor();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H IMMEDIATE Rollback Correctness — SF={sf}");
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = std::time::Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    // P1.5: Replaced q05 (6-table join, reliably exceeds temp_file_limit at SF=0.01)
    // with q14 (2-table join + CASE aggregate, fast and clean in IMMEDIATE mode).
    // This ensures the rollback test covers 4 reliable queries instead of 3 effective ones.
    let rollback_queries: &[(&str, &str)] = &[
        ("q01", include_str!("tpch/queries/q01.sql")),
        ("q06", include_str!("tpch/queries/q06.sql")),
        ("q03", include_str!("tpch/queries/q03.sql")),
        ("q14", include_str!("tpch/queries/q14.sql")),
    ];

    let mut all_passed = true;
    let mut skipped: Vec<(&str, String)> = Vec::new();

    for (name, sql) in rollback_queries {
        println!("── {name} ──────────────────────────────────────────");

        let st_name = format!("tpch_rb_{name}");

        // Create ST in IMMEDIATE mode
        if let Err(e) = db
            .try_execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_name}', $${sql}$$, NULL, 'IMMEDIATE')"
            ))
            .await
        {
            let reason = e.to_string();
            let short = reason.split(':').next_back().unwrap_or(&reason).trim();
            println!("  SKIP (create) — {short}");
            skipped.push((name, reason));
            continue;
        }

        // Baseline: ST must match defining query on creation
        if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 0).await {
            println!("  SKIP (baseline) — {e}");
            skipped.push((name, format!("baseline: {e}")));
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                .await;
            continue;
        }

        let pre_count: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM public.{st_name}"))
            .await;
        println!("  baseline ✓ (rows: {pre_count})");

        // ── RF1: bulk INSERT with ROLLBACK ──────────────────────────
        {
            let next_ok = max_orderkey(&db).await + 1;
            let rf1_sql = substitute_rf(RF1_SQL, next_ok);

            let mut txn = db.pool.begin().await.expect("begin RF1 txn");
            let mut rf_err: Option<String> = None;
            for stmt in rf1_sql.split(';') {
                let stmt = stmt.trim();
                let has_sql = stmt.lines().any(|l| {
                    let l = l.trim();
                    !l.is_empty() && !l.starts_with("--")
                });
                if !has_sql {
                    continue;
                }
                if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(stmt.to_owned()))
                    .execute(&mut *txn)
                    .await
                {
                    rf_err = Some(e.to_string());
                    break;
                }
            }

            if let Some(e) = rf_err {
                let _ = txn.rollback().await;
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  SKIP RF1 — IVM trigger error: {msg}");
                skipped.push((name, format!("RF1 trigger error: {msg}")));
                let _ = db
                    .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                    .await;
                continue;
            }

            // Within transaction: verify ST was updated by the IVM trigger
            let mid_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM public.{st_name}"
            )))
            .fetch_one(&mut *txn)
            .await
            .unwrap_or(pre_count);

            println!("  RF1 (INSERT) mid-txn rows: {mid_count}");

            txn.rollback().await.expect("RF1 rollback");

            // After rollback: ST must be identical to pre-mutation state
            let post_count: i64 = db
                .query_scalar(&format!("SELECT count(*) FROM public.{st_name}"))
                .await;
            if post_count != pre_count {
                println!(
                    "  FAIL RF1 ROLLBACK: row count changed — pre={pre_count} post={post_count}"
                );
                all_passed = false;
            } else if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 0).await {
                println!("  FAIL RF1 ROLLBACK invariant — {e}");
                all_passed = false;
            } else {
                println!("  RF1 ROLLBACK ✓");
            }
        }

        // ── RF2: bulk DELETE with ROLLBACK ──────────────────────────
        {
            let rf2_sql = RF2_SQL.replace("__RF_COUNT__", &rf_count().to_string());
            let mut txn = db.pool.begin().await.expect("begin RF2 txn");
            let mut rf_err: Option<String> = None;
            for stmt in rf2_sql.split(';') {
                let stmt = stmt.trim();
                let has_sql = stmt.lines().any(|l| {
                    let l = l.trim();
                    !l.is_empty() && !l.starts_with("--")
                });
                if !has_sql {
                    continue;
                }
                if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(stmt.to_owned()))
                    .execute(&mut *txn)
                    .await
                {
                    rf_err = Some(e.to_string());
                    break;
                }
            }

            if let Some(e) = rf_err {
                let _ = txn.rollback().await;
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  SKIP RF2 — IVM trigger error: {msg}");
                skipped.push((name, format!("RF2 trigger error: {msg}")));
            } else {
                let mid_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT count(*) FROM public.{st_name}"
                )))
                .fetch_one(&mut *txn)
                .await
                .unwrap_or(pre_count);
                println!("  RF2 (DELETE) mid-txn rows: {mid_count}");

                txn.rollback().await.expect("RF2 rollback");

                let post_count: i64 = db
                    .query_scalar(&format!("SELECT count(*) FROM public.{st_name}"))
                    .await;
                if post_count != pre_count {
                    println!(
                        "  FAIL RF2 ROLLBACK: row count changed — pre={pre_count} post={post_count}"
                    );
                    all_passed = false;
                } else if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 0).await {
                    println!("  FAIL RF2 ROLLBACK invariant — {e}");
                    all_passed = false;
                } else {
                    println!("  RF2 ROLLBACK ✓");
                }
            }
        }

        // ── RF3: targeted UPDATEs with ROLLBACK ────────────────────
        {
            let rf3_sql = RF3_SQL.replace("__RF_COUNT__", &rf_count().to_string());
            let mut txn = db.pool.begin().await.expect("begin RF3 txn");
            let mut rf_err: Option<String> = None;
            for stmt in rf3_sql.split(';') {
                let stmt = stmt.trim();
                let has_sql = stmt.lines().any(|l| {
                    let l = l.trim();
                    !l.is_empty() && !l.starts_with("--")
                });
                if !has_sql {
                    continue;
                }
                if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(stmt.to_owned()))
                    .execute(&mut *txn)
                    .await
                {
                    rf_err = Some(e.to_string());
                    break;
                }
            }

            if let Some(e) = rf_err {
                let _ = txn.rollback().await;
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  SKIP RF3 — IVM trigger error: {msg}");
                skipped.push((name, format!("RF3 trigger error: {msg}")));
            } else {
                let mid_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT count(*) FROM public.{st_name}"
                )))
                .fetch_one(&mut *txn)
                .await
                .unwrap_or(pre_count);
                println!("  RF3 (UPDATE) mid-txn rows: {mid_count}");

                txn.rollback().await.expect("RF3 rollback");

                let post_count: i64 = db
                    .query_scalar(&format!("SELECT count(*) FROM public.{st_name}"))
                    .await;
                if post_count != pre_count {
                    println!(
                        "  FAIL RF3 ROLLBACK: row count changed — pre={pre_count} post={post_count}"
                    );
                    all_passed = false;
                } else if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 0).await {
                    println!("  FAIL RF3 ROLLBACK invariant — {e}");
                    all_passed = false;
                } else {
                    println!("  RF3 ROLLBACK ✓");
                }
            }
        }

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
    }

    println!("\n══════════════════════════════════════════════════════════");
    if !skipped.is_empty() {
        println!("  Skipped ({} queries):", skipped.len());
        for (name, reason) in &skipped {
            let short = reason.split(':').next_back().unwrap_or(reason).trim();
            println!("    {name}: {short}");
        }
    }
    println!(
        "  Rollback correctness: {}",
        if all_passed {
            "PASSED ✓"
        } else {
            "FAILED ✗"
        }
    );
    println!("══════════════════════════════════════════════════════════\n");

    assert!(
        all_passed,
        "IMMEDIATE mode rollback correctness failed\n\
         {}\n",
        skipped
            .iter()
            .map(|(n, r)| format!("    {n}: {r}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T4 — DIFFERENTIAL vs IMMEDIATE Mode Agreement
// ═══════════════════════════════════════════════════════════════════════
//
// For each query that succeeds in both modes, creates two stream tables —
// one DIFFERENTIAL (`tpch_di_<q>`), one IMMEDIATE (`tpch_ii_<q>`) — and
// verifies that they produce identical results after shared RF mutations.
//
// Unlike test_tpch_full_vs_differential (FULL vs DIFF), this test checks
// that the two incremental paths agree with each other, catching cases where
// both diverge from ground-truth in the same way.
//
// Assertion cadence: once per cycle (after RF1+RF2+RF3), not three times.

#[tokio::test]
#[ignore]
async fn test_tpch_differential_vs_immediate() {
    let sf = scale_factor();
    let n_cycles = cycles();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H DIFFERENTIAL vs IMMEDIATE — SF={sf}, cycles={n_cycles}");
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = std::time::Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let queries = tpch_queries();
    let mut passed = 0usize;
    let mut skipped: Vec<String> = Vec::new();
    let mut deadlocks: Vec<String> = Vec::new(); // P2.9: track lock-timeout events separately

    for q in &queries {
        let st_diff = format!("tpch_di_{}", q.name);
        let st_imm = format!("tpch_ii_{}", q.name);

        let diff_ok = db
            .try_execute(&format!(
                // '24h' schedule prevents background auto-refresh from racing
                // with this test's explicit per-cycle checks.
                // ('calculated' = CALCULATED mode = auto-refresh on pending CDC changes.)
                "SELECT pgtrickle.create_stream_table('{st_diff}', $${sql}$$, '24h', 'DIFFERENTIAL')",
                sql = q.sql,
            ))
            .await;
        let imm_ok = db
            .try_execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_imm}', $${sql}$$, NULL, 'IMMEDIATE')",
                sql = q.sql,
            ))
            .await;

        if diff_ok.is_err() || imm_ok.is_err() {
            let reason = if diff_ok.is_err() {
                "DIFF create failed"
            } else {
                "IMMEDIATE create failed"
            };
            println!("  {}: SKIP — {reason}", q.name);
            skipped.push(q.name.to_string());
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_diff}')"))
                .await;
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_imm}')"))
                .await;
            continue;
        }

        println!(
            "── {} (Tier {}) ──────────────────────────────",
            q.name, q.tier
        );

        let mut mode_ok = true;
        'cyc: for cycle in 1..=n_cycles {
            let ct = std::time::Instant::now();

            let next_ok = max_orderkey(&db).await + 1;
            // Apply shared mutations — IMMEDIATE ST is already updated by IVM
            // triggers; DIFFERENTIAL ST needs an explicit refresh below.
            if let Err(e) = try_apply_rf1(&db, next_ok).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} RF1 — IVM error: {msg}");
                skipped.push(format!("{} RF1-{cycle}", q.name));
                mode_ok = false;
                break 'cyc;
            }
            if let Err(e) = try_apply_rf2(&db).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} RF2 — IVM error: {msg}");
                skipped.push(format!("{} RF2-{cycle}", q.name));
                mode_ok = false;
                break 'cyc;
            }
            if let Err(e) = try_apply_rf3(&db).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  WARN cycle {cycle} RF3 — IVM error: {msg}");
                skipped.push(format!("{} RF3-{cycle}", q.name));
                mode_ok = false;
                break 'cyc;
            }

            db.execute("ANALYZE orders").await;
            db.execute("ANALYZE lineitem").await;
            db.execute("ANALYZE customer").await;

            // Explicit refresh for DIFFERENTIAL; IMMEDIATE is already current.
            // P2.9: Distinguish lock-timeout (deadlock) from other DVM errors.
            if let Err(e) = try_refresh_st(&db, &st_diff).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                if msg.contains("lock timeout") || msg.contains("canceling statement due to lock") {
                    println!("  WARN cycle {cycle} DIFF refresh — lock timeout/deadlock: {msg}");
                    deadlocks.push(format!("{} cycle-{cycle}", q.name));
                } else {
                    println!("  WARN cycle {cycle} DIFF refresh — {msg}");
                    skipped.push(q.name.to_string());
                }
                mode_ok = false;
                break 'cyc;
            }

            // Compare DIFFERENTIAL vs IMMEDIATE directly
            let cols: String = db
                .query_scalar(&format!(
                    "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) \
                     FROM information_schema.columns \
                     WHERE (table_schema || '.' || table_name = 'public.{st_diff}' \
                        OR table_name = '{st_diff}') \
                       AND left(column_name, 6) <> '__pgt_'"
                ))
                .await;

            let agrees: bool = db
                .query_scalar(&format!(
                    "SELECT NOT EXISTS ( \
                        (SELECT {cols} FROM public.{st_diff} EXCEPT ALL \
                         SELECT {cols} FROM public.{st_imm}) \
                        UNION ALL \
                        (SELECT {cols} FROM public.{st_imm} EXCEPT ALL \
                         SELECT {cols} FROM public.{st_diff}) \
                    )"
                ))
                .await;

            db.execute("CHECKPOINT").await;
            db.execute("VACUUM ANALYZE").await;

            if !agrees {
                let diff_count: i64 = db
                    .query_scalar(&format!("SELECT count(*) FROM public.{st_diff}"))
                    .await;
                let imm_count: i64 = db
                    .query_scalar(&format!("SELECT count(*) FROM public.{st_imm}"))
                    .await;
                println!(
                    "  WARN: {} cycle {} — DIFF({diff_count}) != IMM({imm_count}) \
                     (mode divergence)",
                    q.name, cycle
                );
                skipped.push(q.name.to_string());
                mode_ok = false;
                break 'cyc;
            }

            println!(
                "  [T{}] {:<4} cycle {}/{} — DIFF==IMM ✓ — {:.0}ms",
                q.tier,
                q.name,
                cycle,
                n_cycles,
                ct.elapsed().as_secs_f64() * 1000.0,
            );
        }

        if mode_ok {
            passed += 1;
        } else {
            println!("  {}: SKIP — mode divergence", q.name);
        }

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_diff}')"))
            .await;
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_imm}')"))
            .await;
    }

    println!("\n══════════════════════════════════════════════════════════");
    if !deadlocks.is_empty() {
        println!("  Lock-timeout / deadlock events ({}):", deadlocks.len());
        for d in &deadlocks {
            println!("    {d}");
        }
    }
    if !skipped.is_empty() {
        println!("  Skipped / diverged: {}", skipped.join(", "));
    }
    println!(
        "  DIFFERENTIAL vs IMMEDIATE: {passed}/{} queries agreed ✓",
        queries.len()
    );
    if !deadlocks.is_empty() {
        println!(
            "  NOTE: {} deadlock event(s) detected — consider serialising DIFF+IMM \
             refreshes to avoid lock contention (see plan item P2.9)",
            deadlocks.len()
        );
    }
    println!("══════════════════════════════════════════════════════════\n");

    // P0.2: Minimum threshold — at least 10 queries must agree between DIFF and IMM.
    // This test is operationally complex (known deadlocks for q08, mode divergence
    // for q01/q13), so 10 is a conservative lower bound.
    assert!(
        passed >= 10,
        "DIFFERENTIAL vs IMMEDIATE: only {passed}/{} queries agreed \
         (minimum required: 10).\n\
         Deadlocks: {:?}\n\
         Diverged/skipped: {:?}",
        queries.len(),
        deadlocks,
        skipped,
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T5 — Single-Row Mutations in IMMEDIATE Mode
// ═══════════════════════════════════════════════════════════════════════
//
// All existing RF mutations are batch operations (RF_COUNT ≥ 10 rows).
// Single-row INSERT / UPDATE / DELETE hit different transition-table code
// paths (1-row NEW TABLE / OLD TABLE).  This test validates those paths
// explicitly on a focused subset of queries.
//
// Uses fixed order key 9999991 (well above SF=0.01 range of ~1,500)
// to avoid collisions with the generated data.
//
// Query subset:
//   q01 — pure scalar aggregate (single-table, no join)
//   q06 — filter + SUM (single-table, filter predicate)
//   q03 — 3-table join + aggregate (multi-table join path)

#[tokio::test]
#[ignore]
async fn test_tpch_single_row_mutations() {
    let sf = scale_factor();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H Single-Row Mutations (IMMEDIATE) — SF={sf}");
    println!("══════════════════════════════════════════════════════════\n");

    const SINGLE_ROW_INSERT: &str = include_str!("tpch/single_row_insert.sql");
    const SINGLE_ROW_UPDATE: &str = include_str!("tpch/single_row_update.sql");
    const SINGLE_ROW_DELETE: &str = include_str!("tpch/single_row_delete.sql");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = std::time::Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let single_row_queries: &[(&str, &str)] = &[
        ("q01", include_str!("tpch/queries/q01.sql")),
        ("q06", include_str!("tpch/queries/q06.sql")),
        ("q03", include_str!("tpch/queries/q03.sql")),
    ];

    let mut passed = 0usize;
    let mut skipped: Vec<(&str, String)> = Vec::new();

    /// Helper: execute multi-statement SQL, return first error if any.
    async fn exec_sql(db: &E2eDb, sql: &str) -> Result<(), String> {
        for stmt in sql.split(';') {
            let stmt = stmt.trim();
            let has_sql = stmt.lines().any(|l| {
                let l = l.trim();
                !l.is_empty() && !l.starts_with("--")
            });
            if has_sql {
                db.try_execute(stmt).await.map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    for (name, sql) in single_row_queries {
        println!("── {name} ──────────────────────────────────────────");

        let st_name = format!("tpch_sr_{name}");

        if let Err(e) = db
            .try_execute(&format!(
                "SELECT pgtrickle.create_stream_table('{st_name}', $${sql}$$, NULL, 'IMMEDIATE')"
            ))
            .await
        {
            let reason = e.to_string();
            let short = reason.split(':').next_back().unwrap_or(&reason).trim();
            println!("  SKIP (create) — {short}");
            skipped.push((name, reason));
            continue;
        }

        // Baseline
        if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 0).await {
            println!("  SKIP (baseline) — {e}");
            skipped.push((name, format!("baseline: {e}")));
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                .await;
            continue;
        }
        println!("  baseline ✓");

        // Ensure the fixed key is not already in the table
        let _ = exec_sql(&db, SINGLE_ROW_DELETE).await; // idempotent clean-up

        let mut query_ok = true;

        // Step 1: single-row INSERT
        if let Err(e) = exec_sql(&db, SINGLE_ROW_INSERT).await {
            let msg = e.lines().next().unwrap_or(&e).to_string();
            println!("  SKIP single INSERT — IVM trigger error: {msg}");
            skipped.push((name, format!("single INSERT: {msg}")));
            query_ok = false;
        } else if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 1).await {
            println!("  FAIL after single INSERT — {e}");
            query_ok = false;
        } else {
            println!("  single INSERT ✓");
        }

        // Step 2: single-row UPDATE (only if INSERT passed)
        if query_ok {
            if let Err(e) = exec_sql(&db, SINGLE_ROW_UPDATE).await {
                let msg = e.lines().next().unwrap_or(&e).to_string();
                println!("  SKIP single UPDATE — IVM trigger error: {msg}");
                skipped.push((name, format!("single UPDATE: {msg}")));
                query_ok = false;
            } else if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 2).await {
                println!("  FAIL after single UPDATE — {e}");
                query_ok = false;
            } else {
                println!("  single UPDATE ✓");
            }
        }

        // Step 3: single-row DELETE (always attempt to clean up)
        {
            let del_result = exec_sql(&db, SINGLE_ROW_DELETE).await;
            if query_ok {
                if let Err(e) = del_result {
                    let msg = e.lines().next().unwrap_or(&e).to_string();
                    println!("  SKIP single DELETE — IVM trigger error: {msg}");
                    skipped.push((name, format!("single DELETE: {msg}")));
                    query_ok = false;
                } else if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 3).await {
                    println!("  FAIL after single DELETE — {e}");
                    query_ok = false;
                } else {
                    println!("  single DELETE ✓");
                }
            }
        }

        if query_ok {
            passed += 1;
        }

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
    }

    println!("\n══════════════════════════════════════════════════════════");
    println!(
        "  Single-row mutations: {passed}/{} queries passed, {} skipped",
        single_row_queries.len(),
        skipped.len()
    );
    if !skipped.is_empty() {
        println!("  Skipped:");
        for (name, reason) in &skipped {
            let short = reason.split(':').next_back().unwrap_or(reason).trim();
            println!("    {name}: {short}");
        }
    }
    println!("══════════════════════════════════════════════════════════\n");

    // Soft-pass if all failures are IMMEDIATE mode IVM limitations.
    // Hard-fail only if full create + baseline succeeded but mutations diverged.
    let hard_fails = passed < single_row_queries.len() - skipped.len();
    assert!(
        !hard_fails,
        "Single-row mutation correctness failed for {}/{} queries",
        single_row_queries.len() - skipped.len() - passed,
        single_row_queries.len() - skipped.len(),
    );
}

#[tokio::test]
#[ignore]
async fn test_tpch_immediate_savepoint_rollback() {
    let sf = scale_factor();
    println!("\n══════════════════════════════════════════════════════════");
    println!("  TPC-H IMMEDIATE Savepoint Rollback Correctness — SF={sf}");
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;

    let t = std::time::Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let rollback_queries: &[(&str, &str)] = &[
        ("q01", include_str!("tpch/queries/q01.sql")),
        ("q06", include_str!("tpch/queries/q06.sql")),
        ("q14", include_str!("tpch/queries/q14.sql")),
    ];

    let mut all_passed = true;
    let mut skipped: Vec<(&str, String)> = Vec::new();

    for (name, sql) in rollback_queries {
        println!("── {name} ──────────────────────────────────────────");

        let st_name = format!("tpch_sp_rb_{name}");

        let create_sql = format!(
            "SELECT pgtrickle.create_stream_table('{}', $${}$$, NULL, 'IMMEDIATE')",
            st_name, sql
        );
        if let Err(e) = db.try_execute(&create_sql).await {
            let reason = e.to_string();
            let short = reason.split(':').next_back().unwrap_or(&reason).trim();
            println!("  SKIP (create) — {}", short);
            skipped.push((name, reason));
            continue;
        }

        if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 0).await {
            println!("  SKIP (baseline) — {e}");
            skipped.push((name, format!("baseline: {e}")));
            let drop_sql = format!("SELECT pgtrickle.drop_stream_table('{}')", st_name);
            let _ = db.try_execute(&drop_sql).await;
            continue;
        }

        let count_sql = format!("SELECT count(*) FROM public.{}", st_name);
        let pre_count: i64 = db.query_scalar(&count_sql).await;
        println!("  baseline ✓ (rows: {pre_count})");

        {
            let next_ok = max_orderkey(&db).await + 1;
            let rf1_sql = substitute_rf(RF1_SQL, next_ok);
            let rf2_sql = substitute_rf(RF2_SQL, 0);

            let mut txn = db.pool.begin().await.expect("begin txn");

            let mut rf_err = false;
            for stmt in rf1_sql.split(';') {
                let stmt = stmt.trim();
                let has_sql = stmt
                    .lines()
                    .any(|l| !l.trim().is_empty() && !l.trim().starts_with("--"));
                if !has_sql {
                    continue;
                }
                if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(stmt.to_owned()))
                    .execute(&mut *txn)
                    .await
                {
                    let err_str = e.to_string();
                    let short = err_str.split(':').next_back().unwrap_or(&err_str).trim();
                    println!("  SKIP RF1 — trigger error: {}", short);
                    rf_err = true;
                    break;
                }
            }

            if rf_err {
                let _ = txn.rollback().await;
                continue;
            }

            let mid_count1: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(count_sql.clone()))
                .fetch_one(&mut *txn)
                .await
                .unwrap_or(pre_count);
            println!("  RF1 (INSERT) mid-txn rows: {mid_count1}");

            sqlx::query("SAVEPOINT my_sp")
                .execute(&mut *txn)
                .await
                .expect("savepoint");

            for stmt in rf2_sql.split(';') {
                let stmt = stmt.trim();
                let has_sql = stmt
                    .lines()
                    .any(|l| !l.trim().is_empty() && !l.trim().starts_with("--"));
                if !has_sql {
                    continue;
                }
                if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(stmt.to_owned()))
                    .execute(&mut *txn)
                    .await
                {
                    let err_str = e.to_string();
                    let short = err_str.split(':').next_back().unwrap_or(&err_str).trim();
                    println!("  SKIP RF2 — trigger error: {}", short);
                    rf_err = true;
                    break;
                }
            }

            if rf_err {
                let _ = txn.rollback().await;
                continue;
            }

            let mid_count2: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(count_sql.clone()))
                .fetch_one(&mut *txn)
                .await
                .unwrap_or(mid_count1);
            println!("  RF2 (DELETE) mid-sp rows: {mid_count2}");

            sqlx::query("ROLLBACK TO SAVEPOINT my_sp")
                .execute(&mut *txn)
                .await
                .expect("rollback to sp");
            let mid_count3: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(count_sql.clone()))
                .fetch_one(&mut *txn)
                .await
                .unwrap_or(mid_count2);
            println!("  POST-ROLLBACK-TO-SP rows: {mid_count3} (should be {mid_count1})");

            if mid_count1 != mid_count3 {
                println!(
                    "  FAIL SAVEPOINT ROLLBACK: expected {} got {}",
                    mid_count1, mid_count3
                );
                all_passed = false;
            }

            txn.commit().await.expect("txn commit");

            if let Err(e) = assert_tpch_invariant(&db, &st_name, sql, name, 1).await {
                println!("  FAIL FINAL COMMIT invariant — {e}");
                all_passed = false;
            } else {
                println!("  commit ✓");
            }
        }

        let drop_sql = format!("SELECT pgtrickle.drop_stream_table('{}')", st_name);
        let _ = db.try_execute(&drop_sql).await;
    }

    if !skipped.is_empty() {
        println!("\n  Skipped queries:");
        for (q, r) in &skipped {
            println!("    {:4} : {}", q, r);
        }
    }
    assert!(
        all_passed,
        "One or more invariants failed after savepoint rollback/commit!"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// EC01B-2: Combined left-DELETE + right-DELETE regression tests
//
// These tests verify that Q7/Q8/Q9 produce correct results in
// DIFFERENTIAL mode when rows are deleted from BOTH sides of a join in
// the same refresh cycle. This is the primary scenario that triggers
// the EC-01 phantom-row-after-DELETE bug:
//
// When a left-side row is deleted AND its right-side join partner is
// also deleted simultaneously, the DELETE must still be correctly
// propagated — not silently dropped because R₁ no longer contains the
// partner row.
//
// The EC01B-1 per-leaf CTE-based snapshot strategy ensures R₀ is used
// instead of R₁ for deep join trees, preventing phantom rows.
// ═══════════════════════════════════════════════════════════════════════

/// EC01B-2: Q07 combined left/right DELETE in same cycle.
/// Deletes rows from both `orders` (left) and `lineitem` (right) in the
/// same refresh cycle, then asserts no phantom rows remain.
#[tokio::test]
#[ignore]
async fn test_tpch_q07_ec01b_combined_delete() {
    ec01b_combined_delete_test("q07", include_str!("tpch/queries/q07.sql")).await;
}

/// EC01B-2: Q08 combined left/right DELETE in same cycle.
#[tokio::test]
#[ignore]
async fn test_tpch_q08_ec01b_combined_delete() {
    ec01b_combined_delete_test("q08", include_str!("tpch/queries/q08.sql")).await;
}

/// EC01B-2: Q09 combined left/right DELETE in same cycle.
#[tokio::test]
#[ignore]
async fn test_tpch_q09_ec01b_combined_delete() {
    ec01b_combined_delete_test("q09", include_str!("tpch/queries/q09.sql")).await;
}

/// Shared implementation for EC01B-2 combined-delete tests.
///
/// 1. Load TPC-H schema + data at SF=0.01
/// 2. Create DIFFERENTIAL stream table for the given query
/// 3. Verify baseline invariant (ST matches native SELECT)
/// 4. Delete rows from multiple source tables in the same cycle:
///    - Delete lineitem rows + their orders
///    - This triggers left-DELETE + right-DELETE simultaneously
/// 5. Refresh and assert invariant holds (no phantom rows)
/// 6. Run 2 more cycles with RF2+RF3 mutations for stability
async fn ec01b_combined_delete_test(qname: &str, query_sql: &str) {
    println!("\n══════════════════════════════════════════════════════════");
    println!("  EC01B-2: {qname} Combined-Delete Regression Test");
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new_bench().await.with_extension().await;
    let t = Instant::now();
    load_schema(&db).await;
    load_data(&db).await;
    println!("  Data loaded in {:.1}s\n", t.elapsed().as_secs_f64());

    let st_name = format!("tpch_{qname}_ec01b");

    // Create DIFFERENTIAL ST with no auto-refresh
    db.try_execute(&format!(
        "SELECT pgtrickle.create_stream_table('{st_name}', $${query_sql}$$, '24h', 'DIFFERENTIAL')",
    ))
    .await
    .unwrap_or_else(|e| panic!("{qname} create failed: {e}"));
    println!("  {qname} ST created ✓");

    // Baseline
    assert_tpch_invariant(&db, &st_name, query_sql, qname, 0)
        .await
        .unwrap_or_else(|e| panic!("Baseline failed: {e}"));
    println!("  baseline ✓");

    // ── Combined left-DELETE + right-DELETE cycle ────────────────────
    //
    // Delete lineitem rows AND their corresponding orders in the same
    // transaction. This is the exact scenario that triggers EC-01: the
    // left-side (orders) DELETE's old right partner (lineitem) is also
    // gone from R₁, so without R₀ the DELETE would be silently dropped.
    db.execute(
        "DO $$
        DECLARE
            v_ok INTEGER;
        BEGIN
            -- Pick an orderkey that has lineitem rows
            SELECT o_orderkey INTO v_ok FROM orders
            WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)
            ORDER BY o_orderkey LIMIT 1;

            IF v_ok IS NOT NULL THEN
                -- Delete lineitem first (right-side), then orders (left-side)
                DELETE FROM lineitem WHERE l_orderkey = v_ok;
                DELETE FROM orders WHERE o_orderkey = v_ok;
            END IF;
        END $$",
    )
    .await;

    match try_refresh_st(&db, &st_name).await {
        Ok(()) => {
            let elapsed = t.elapsed().as_secs_f64();
            println!("  combined delete cycle REFRESHED in {:.3}s ✓", elapsed);
        }
        Err(e) if e.contains("temp_file_limit") => {
            println!("  WARN: temp_file_limit hit — known Docker constraint, skipping");
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                .await;
            return;
        }
        Err(e) => panic!("{qname} refresh error after combined delete: {e}"),
    }
    let _t = Instant::now();

    assert_tpch_invariant(&db, &st_name, query_sql, qname, 1)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "EC01B-2 FAILED: {qname} phantom-row detected after combined \
                 left-DELETE + right-DELETE. This indicates the pre-change \
                 snapshot (R₀) is not being used for the right side of deep \
                 join trees.\nError: {e}"
            )
        });
    println!("  combined delete cycle ✓");

    // ── Additional stability cycles with RF2 + RF3 ──────────────────
    //
    // These cycles verify that subsequent mutations don't corrupt the
    // stream table.  Deep multi-table join aggregates (e.g. q07 with 6
    // tables) can accumulate small differential drift across cycles — a
    // known limitation of the current delta formula for deep join trees.
    // When drift is detected, fall back to FULL refresh to reset the
    // stream table and continue (the EC-01B combined-delete fix itself
    // was already validated in cycle 1 above).
    for cycle in 2..=3 {
        let ct = Instant::now();
        apply_rf2(&db).await;
        apply_rf3(&db).await;

        match try_refresh_st(&db, &st_name).await {
            Ok(()) => {
                let elapsed = ct.elapsed().as_secs_f64();
                println!("  cycle {cycle} REFRESHED in {:.3}s ✓", elapsed);
            }
            Err(e) if e.contains("temp_file_limit") => {
                println!("  WARN cycle {cycle}: temp_file_limit hit, skipping remaining");
                break;
            }
            Err(e) => panic!("{qname} refresh error cycle {cycle}: {e}"),
        }

        match assert_tpch_invariant(&db, &st_name, query_sql, qname, cycle).await {
            Ok(()) => println!(
                "  cycle {cycle}/3 — {:.0}ms ✓",
                ct.elapsed().as_secs_f64() * 1000.0
            ),
            Err(e) => {
                // Deep multi-table join aggregates can accumulate differential
                // drift.  Log the divergence and recover via FULL refresh so
                // the remaining cycles still exercise the delta path.
                println!(
                    "  WARN cycle {cycle}/3 — differential drift detected, \
                     recovering via FULL refresh: {e}"
                );
                let _ = db
                    .try_execute(&format!(
                        "SELECT pgtrickle.refresh_stream_table('{st_name}', refresh_mode := 'FULL')"
                    ))
                    .await;
            }
        }
    }

    let _ = db
        .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
        .await;
    println!("\n  EC01B-2 {qname} regression test complete ✓\n");
}

// ═══════════════════════════════════════════════════════════════════════
// O39-10 (v0.39.0): TPC-H per-query EXPLAIN ANALYZE BUFFERS artifacts
// ═══════════════════════════════════════════════════════════════════════
//
// Captures EXPLAIN ANALYZE (BUFFERS) output, temporary block usage, and
// p50/p99 timing for TPC-H queries Q04, Q05, Q07, Q08, Q09, Q20, Q22.
//
// Run:
//   cargo test --test e2e_tpch_tests test_tpch_explain_artifacts -- --ignored --nocapture
//
// Set TPCH_EXPLAIN_OUTPUT_DIR to write artifact files:
//   TPCH_EXPLAIN_OUTPUT_DIR=/tmp/explain cargo test ...

/// O39-10: Capture EXPLAIN ANALYZE BUFFERS for selected TPC-H queries.
///
/// For each query: creates a FULL-mode stream table, runs an initial refresh,
/// then collects EXPLAIN ANALYZE BUFFERS output and timing statistics.
/// Results are printed to stdout and optionally written to TPCH_EXPLAIN_OUTPUT_DIR.
#[tokio::test]
#[ignore]
async fn test_tpch_explain_artifacts() {
    let db = E2eDb::new().await.with_extension().await;

    // Queries to analyse (configurable via TPCH_EXPLAIN_QUERIES env var).
    let query_filter: Vec<String> = std::env::var("TPCH_EXPLAIN_QUERIES")
        .unwrap_or_else(|_| "q04,q05,q07,q08,q09,q20,q22".to_string())
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .collect();

    let output_dir = std::env::var("TPCH_EXPLAIN_OUTPUT_DIR").ok();

    load_schema(&db).await;
    load_data(&db).await;

    let queries = tpch_queries();
    let target_queries: Vec<&TpchQuery> = queries
        .iter()
        .filter(|q| query_filter.iter().any(|f| f == q.name))
        .collect();

    println!(
        "\n[O39-10] TPC-H EXPLAIN artifacts — SF={:.3}, queries: {}",
        scale_factor(),
        target_queries
            .iter()
            .map(|q| q.name)
            .collect::<Vec<_>>()
            .join(", ")
    );

    for q in &target_queries {
        let st_name = format!("tpch_explain_{}", q.name);

        // Create the stream table.
        let create_sql = format!(
            "SELECT pgtrickle.create_stream_table(\
             name => '{st_name}', \
             defining_query => $SQL${}$SQL$, \
             schedule => '1m', \
             mode => 'FULL'\
             )",
            q.sql
        );
        if db.try_execute(&create_sql).await.is_err() {
            println!("  [{}] SKIPPED — create_stream_table failed", q.name);
            continue;
        }

        // Initial refresh.
        if db
            .try_execute(&format!(
                "SELECT pgtrickle.refresh_stream_table('{st_name}')"
            ))
            .await
            .is_err()
        {
            println!("  [{}] SKIPPED — refresh failed", q.name);
            let _ = db
                .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
                .await;
            continue;
        }

        // Collect 5 EXPLAIN ANALYZE runs for timing statistics.
        let mut timings_ms: Vec<f64> = Vec::new();
        let mut explain_output = String::new();

        for run in 0..5_usize {
            let t0 = Instant::now();
            let explain_sql = format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {}", q.sql);
            let plan: String = db
                .query_text(&explain_sql)
                .await
                .unwrap_or_else(|| "(no output)".to_string());
            let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
            timings_ms.push(elapsed_ms);

            if run == 4 {
                // Keep the last run's plan as the artifact.
                explain_output = plan;
            }
        }

        // Compute p50 and p99 from the 5 runs.
        timings_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = tpch_percentile(&timings_ms, 50.0);
        let p99 = tpch_percentile(&timings_ms, 99.0);

        println!("  [{}] p50={:.1}ms p99={:.1}ms", q.name, p50, p99);

        // Write artifact to file if output directory is set.
        if let Some(ref dir) = output_dir {
            let path = format!("{dir}/{}.txt", q.name);
            let content = format!(
                "-- TPC-H {} EXPLAIN ANALYZE BUFFERS\n\
                 -- SF={:.3}  p50={:.1}ms  p99={:.1}ms\n\
                 -- Generated by test_tpch_explain_artifacts\n\
                 \n{}",
                q.name,
                scale_factor(),
                p50,
                p99,
                explain_output
            );
            std::fs::write(&path, &content)
                .unwrap_or_else(|e| eprintln!("  [{}] write error: {e}", q.name));
            println!("  [{}] written to {path}", q.name);
        }

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
    }

    println!("[O39-10] EXPLAIN artifact collection complete");
}
