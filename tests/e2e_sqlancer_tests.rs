//! SQLancer-style differential fuzzing tests (Phase 4 — SQLANCER-1 & SQLANCER-2).
//!
//! # What this implements
//!
//! **SQLANCER-1 — Crash oracle:** For each randomly generated query Q,
//! `pgtrickle.create_stream_table()` + `pgtrickle.refresh_stream_table()` must
//! not crash the backend. Any structured SQL error (unsupported query, invalid
//! argument) is acceptable; a lost connection or PostgreSQL PANIC is a failure.
//!
//! **SQLANCER-2 — Equivalence oracle:** For queries that successfully create and
//! populate a stream table, the stream table contents must be identical (multiset
//! equality) to the result of executing the original SELECT directly.
//!
//! # Running
//!
//! These tests are marked `#[ignore]` and are not included in the normal `just
//! test-e2e` run. They are executed by the `weekly-sqlancer` CI job or locally:
//!
//! ```bash
//! just sqlancer          # rebuild Docker image + run all sqlancer tests
//! just sqlancer-fast     # skip Docker image rebuild
//!
//! # Control the number of generated queries:
//! SQLANCER_CASES=500 just sqlancer-fast
//! ```
//!
//! # Proptest regression seeds
//!
//! When the equivalence oracle detects a mismatch it panics with the seed value.
//! Save the seed to `proptest-regressions/e2e_sqlancer/corpus.txt` (one hex
//! seed per line) to replay the failure on subsequent runs:
//!
//! ```text
//! # proptest-regressions/e2e_sqlancer/corpus.txt
//! 0xdeadbeef01234567
//! ```

mod e2e;

use e2e::E2eDb;
use e2e::oracle::{self, CaseOutcome};

// ── Query generator ────────────────────────────────────────────────────────

/// Seeded LCG random number generator (deterministic, no external dependency).
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x6c62272e07bb0142,
        }
    }

    fn next(&mut self) -> u64 {
        // Knuth multiplicative LCG (64-bit)
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    fn choice<T: Clone>(&mut self, options: &[T]) -> T {
        options[(self.next() as usize) % options.len()].clone()
    }

    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + (self.next() % (hi - lo + 1))
    }
}

/// Description of a generated test table.
#[derive(Clone, Debug)]
struct TestTable {
    name: String,
    cols: Vec<(String, &'static str)>, // (column_name, sql_type)
    row_count: usize,
}

impl TestTable {
    fn ddl(&self) -> String {
        let col_defs: Vec<String> = std::iter::once("id BIGINT PRIMARY KEY".to_string())
            .chain(self.cols.iter().map(|(n, t)| format!("{n} {t}")))
            .collect();
        format!("CREATE TABLE {} ({})", self.name, col_defs.join(", "))
    }

    fn insert_dml(&self, rng: &mut Lcg) -> String {
        let mut rows = Vec::new();
        for i in 1..=(self.row_count) {
            let mut vals: Vec<String> = vec![i.to_string()];
            for (_, t) in &self.cols {
                let v = match *t {
                    "INT" => (rng.range(1, 100) as i64).to_string(),
                    "BIGINT" => (rng.range(1, 1000) as i64).to_string(),
                    "NUMERIC" => format!("{}", rng.range(1, 500)),
                    "TEXT" => {
                        let choices = ["alpha", "beta", "gamma", "delta", "epsilon"];
                        format!("'{}'", rng.choice(&choices))
                    }
                    _ => "0".to_string(),
                };
                vals.push(v);
            }
            rows.push(format!("({})", vals.join(", ")));
        }
        format!("INSERT INTO {} VALUES {}", self.name, rows.join(", "))
    }
}

/// A generated test query and the tables it references.
#[derive(Clone, Debug)]
struct GeneratedQuery {
    query: String,
    tables: Vec<TestTable>,
    /// Human-readable description for failure messages.
    description: String,
    seed: u64,
}

/// Generate a batch of queries using a seeded random number generator.
fn generate_queries(base_seed: u64, count: usize) -> Vec<GeneratedQuery> {
    let mut queries = Vec::with_capacity(count);

    for idx in 0..count {
        let seed = base_seed.wrapping_add((idx as u64).wrapping_mul(0x9e3779b97f4a7c15));
        let mut rng = Lcg::new(seed);

        let query = generate_one_query(&mut rng, idx);
        queries.push(GeneratedQuery { seed, ..query });
    }

    queries
}

fn generate_one_query(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let variant = rng.range(0, 5);
    match variant {
        0 => gen_simple_select(rng, idx),
        1 => gen_aggregate_query(rng, idx),
        2 => gen_filter_query(rng, idx),
        3 => gen_join_query(rng, idx),
        _ => gen_multi_aggregate(rng, idx),
    }
}

fn make_table(name: &str, rng: &mut Lcg, row_count: usize) -> TestTable {
    let col_types = ["INT", "BIGINT", "NUMERIC", "TEXT"];
    let num_cols = rng.range(2, 5) as usize;
    let cols: Vec<(String, &'static str)> = (0..num_cols)
        .map(|i| {
            let t = rng.choice(&col_types);
            (format!("col{i}"), t)
        })
        .collect();
    TestTable {
        name: name.to_string(),
        cols,
        row_count,
    }
}

fn gen_simple_select(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count = rng.range(5, 30) as usize;
    let tbl = make_table(&format!("t_ss_{idx}"), rng, row_count);
    let non_text_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t != "TEXT")
        .map(|(n, _)| n.clone())
        .collect();
    let select_col = if non_text_cols.is_empty() {
        "col0".to_string()
    } else {
        rng.choice(&non_text_cols)
    };
    let query = format!("SELECT id, {select_col} FROM {}", tbl.name);
    GeneratedQuery {
        query,
        tables: vec![tbl.clone()],
        description: format!("simple SELECT (idx={idx})"),
        seed: 0,
    }
}

fn gen_aggregate_query(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count = rng.range(8, 40) as usize;
    let tbl = make_table(&format!("t_ag_{idx}"), rng, row_count);
    let text_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t == "TEXT")
        .map(|(n, _)| n.clone())
        .collect();
    let num_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t != "TEXT")
        .map(|(n, _)| n.clone())
        .collect();

    let group_col = if text_cols.is_empty() {
        "id"
    } else {
        &text_cols[0]
    };
    let agg_func = rng.choice(&["SUM", "COUNT", "MAX", "MIN"]);

    let (agg_expr, agg_alias) = if agg_func == "COUNT" || num_cols.is_empty() {
        ("COUNT(*)".to_string(), "cnt".to_string())
    } else {
        let c = rng.choice(&num_cols);
        (format!("{agg_func}({c})"), "agg_result".to_string())
    };

    let query = format!(
        "SELECT {group_col}, {agg_expr} AS {agg_alias} FROM {} GROUP BY {group_col}",
        tbl.name
    );
    GeneratedQuery {
        query,
        tables: vec![tbl],
        description: format!("aggregate {agg_func} GROUP BY (idx={idx})"),
        seed: 0,
    }
}

fn gen_filter_query(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count = rng.range(10, 50) as usize;
    let tbl = make_table(&format!("t_fl_{idx}"), rng, row_count);
    let num_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t != "TEXT")
        .map(|(n, _)| n.clone())
        .collect();

    let (where_clause, select_cols) = if num_cols.is_empty() {
        ("id > 0".to_string(), "id".to_string())
    } else {
        let c = rng.choice(&num_cols);
        let threshold = rng.range(1, 50);
        (format!("{c} > {threshold}"), format!("id, {c}"))
    };

    let query = format!(
        "SELECT {select_cols} FROM {} WHERE {where_clause}",
        tbl.name
    );
    GeneratedQuery {
        query,
        tables: vec![tbl],
        description: format!("filter query (idx={idx})"),
        seed: 0,
    }
}

fn gen_join_query(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count_1 = rng.range(5, 20) as usize;
    let t1 = make_table(&format!("t_j1_{idx}"), rng, row_count_1);
    let row_count_2 = rng.range(5, 20) as usize;
    let t2 = make_table(&format!("t_j2_{idx}"), rng, row_count_2);
    let query = format!(
        "SELECT a.id, a.col0, b.col0 AS b_col0 \
         FROM {t1} a JOIN {t2} b ON a.id = b.id",
        t1 = t1.name,
        t2 = t2.name,
    );
    GeneratedQuery {
        query,
        tables: vec![t1, t2],
        description: format!("inner JOIN (idx={idx})"),
        seed: 0,
    }
}

fn gen_multi_aggregate(rng: &mut Lcg, idx: usize) -> GeneratedQuery {
    let row_count = rng.range(8, 40) as usize;
    let tbl = make_table(&format!("t_ma_{idx}"), rng, row_count);
    let text_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t == "TEXT")
        .map(|(n, _)| n.clone())
        .collect();
    let num_cols: Vec<_> = tbl
        .cols
        .iter()
        .filter(|(_, t)| *t != "TEXT")
        .map(|(n, _)| n.clone())
        .collect();

    let group_col = if text_cols.is_empty() {
        "id"
    } else {
        &text_cols[0]
    };

    let agg_clauses: Vec<String> = if num_cols.is_empty() {
        vec!["COUNT(*) AS cnt".to_string()]
    } else {
        let c1 = rng.choice(&num_cols);
        let c2 = rng.choice(&num_cols);
        vec![
            format!("SUM({c1}) AS sum_col"),
            format!("MAX({c2}) AS max_col"),
            "COUNT(*) AS cnt".to_string(),
        ]
    };

    let query = format!(
        "SELECT {group_col}, {} FROM {} GROUP BY {group_col}",
        agg_clauses.join(", "),
        tbl.name,
    );
    GeneratedQuery {
        query,
        tables: vec![tbl],
        description: format!("multi-aggregate (idx={idx})"),
        seed: 0,
    }
}

// ── Test helpers ───────────────────────────────────────────────────────────

fn sqlancer_cases() -> usize {
    std::env::var("SQLANCER_CASES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(200)
}

fn base_seed() -> u64 {
    std::env::var("SQLANCER_SEED")
        .ok()
        .and_then(|v| {
            let s = v.trim();
            if s.starts_with("0x") || s.starts_with("0X") {
                u64::from_str_radix(&s[2..], 16).ok()
            } else {
                s.parse::<u64>().ok()
            }
        })
        .unwrap_or(0xdeadbeef_cafebabe)
}

// ── SQLANCER-1: Crash oracle ───────────────────────────────────────────────

/// **SQLANCER-1 — Crash oracle.**
///
/// Generates `SQLANCER_CASES` (default 200) random `create_stream_table` calls
/// and verifies that none crash the PostgreSQL backend. Any structured SQL error
/// (unsupported query, constraint violation) is acceptable; a lost connection or
/// server PANIC is a failure.
///
/// Run with: `just sqlancer-fast` or `SQLANCER_CASES=10000 just sqlancer-fast`.
/// Run the crash oracle logic.
///
/// Accepts a shared `E2eDb` so that the combined CI test (`test_sqlancer_ci_combined`)
/// can reuse a single database across all three oracle phases, keeping the number of
/// active PostgreSQL databases — and thus pg_trickle background workers consuming
/// DSM — bounded to one.  Each iteration cleans up its tables so the database stays
/// lean throughout a long run.
async fn run_crash_oracle(db: &E2eDb) {
    let cases = sqlancer_cases();
    let seed = base_seed();

    println!("[sqlancer] crash oracle: {cases} cases, base_seed=0x{seed:016x}");

    let queries = generate_queries(seed, cases);
    let mut crashes = 0usize;
    let mut structured_errors = 0usize;
    let mut successes = 0usize;

    for (i, gq) in queries.iter().enumerate() {
        // Create tables and insert data.
        for tbl in &gq.tables {
            db.execute(&tbl.ddl()).await;
            let mut rng = Lcg::new(gq.seed ^ (i as u64).wrapping_mul(0x1234567890abcdef));
            db.execute(&tbl.insert_dml(&mut rng)).await;
        }

        // Try to create the stream table.
        let st_name = format!("sqlancer_st_{i}");
        let create_sql = format!(
            "SELECT pgtrickle.create_stream_table('{st_name}', $SQL${}$SQL$, '1m', 'FULL')",
            gq.query
        );

        let create_result = db.try_execute(&create_sql).await;
        match create_result {
            Ok(_) => {
                successes += 1;
                // Also attempt a refresh to trigger the execution path.
                let refresh_sql = format!("SELECT pgtrickle.refresh_stream_table('{st_name}')");
                let refresh_result = db.try_execute(&refresh_sql).await;
                if let Err(e) = refresh_result {
                    // Distinguish a lost backend from a structured error using
                    // sqlx's error type and PostgreSQL SQLSTATE.
                    if oracle::is_infrastructure_sqlx_error(&e) {
                        crashes += 1;
                        eprintln!(
                            "[sqlancer] CRASH detected (seed=0x{:016x}, case={i}): {e}\n  query: {}",
                            gq.seed, gq.query
                        );
                    } else {
                        structured_errors += 1;
                    }
                }
            }
            Err(e) => {
                if oracle::is_infrastructure_sqlx_error(&e) {
                    crashes += 1;
                    eprintln!(
                        "[sqlancer] CRASH on create (seed=0x{:016x}, case={i}): {e}\n  query: {}",
                        gq.seed, gq.query
                    );
                } else {
                    // Structured error (unsupported query, etc.) — expected for fuzzing.
                    structured_errors += 1;
                }
            }
        }

        if (i + 1) % 50 == 0 {
            println!(
                "[sqlancer] progress: {}/{cases} — ok={successes} errs={structured_errors} crashes={crashes}",
                i + 1
            );
        }

        // Cleanup: drop the stream table (if it was created) and all source tables
        // so they do not accumulate across iterations.  Without this, thousands of
        // tables build up in the shared database, eventually exhausting the
        // container's /dev/shm via pg_trickle background-worker DSM usage.
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
        for tbl in &gq.tables {
            let _ = db
                .try_execute(&format!("DROP TABLE IF EXISTS {}", tbl.name))
                .await;
        }
    }

    println!(
        "[sqlancer] crash oracle done: {cases} cases — \
         ok={successes}, structured_errors={structured_errors}, crashes={crashes}"
    );

    assert_eq!(
        crashes, 0,
        "SQLANCER-1 crash oracle: {crashes} backend crash(es) detected out of {cases} cases.\n\
         Re-run with SQLANCER_SEED=0x{seed:016x} SQLANCER_CASES={cases} to reproduce.",
    );
}

#[tokio::test]
#[ignore]
async fn test_sqlancer_crash_oracle() {
    let db = E2eDb::new().await.with_extension().await;
    run_crash_oracle(&db).await;
}

/// Run the equivalence oracle logic.
///
/// See [`run_crash_oracle`] for the rationale behind the shared-database approach
/// and per-iteration cleanup.
async fn run_equivalence_oracle(db: &E2eDb) {
    let cases = sqlancer_cases();
    let seed = base_seed();

    println!("[sqlancer] equivalence oracle: {cases} cases, base_seed=0x{seed:016x}");

    let queries = generate_queries(seed, cases);
    let mut mismatches = Vec::<(u64, String, String)>::new();
    let mut skipped = 0usize;
    let mut checked = 0usize;

    for (i, gq) in queries.iter().enumerate() {
        // Create source tables.
        for tbl in &gq.tables {
            db.execute(&tbl.ddl()).await;
            let mut rng = Lcg::new(gq.seed ^ (i as u64).wrapping_mul(0x1234567890abcdef));
            db.execute(&tbl.insert_dml(&mut rng)).await;
        }

        // Attempt to create + populate a FULL-mode stream table, then compare.
        // Use an inner async block so cleanup always runs regardless of the exit path.
        let st_name = format!("sqlancer_eq_{i}");
        let create_sql = format!(
            "SELECT pgtrickle.create_stream_table('{st_name}', $SQL${}$SQL$, '1m', 'FULL')",
            gq.query
        );
        let refresh_sql = format!("SELECT pgtrickle.refresh_stream_table('{st_name}')");

        let outcome: CaseOutcome = async {
            if let Err(e) = db.try_execute(&create_sql).await {
                return oracle::classify_admission_sqlx_error(&e);
            }
            if let Err(e) = db.try_execute(&refresh_sql).await {
                return CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("refresh failed after creation: {e}"),
                    details: Some(gq.query.clone()),
                });
            }
            match oracle::compare_st_to_query(db, &st_name, &gq.query).await {
                Ok(()) => CaseOutcome::Passed(oracle::PassReport {
                    effective_mode: "FULL".to_string(),
                    row_count: 0,
                }),
                Err(diff) => CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("Multiset equivalence mismatch:\n{diff}"),
                    details: Some(gq.query.clone()),
                }),
            }
        }
        .await;

        // Cleanup: always drop the stream table and source tables so they do not
        // accumulate across iterations (see run_crash_oracle for rationale).
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
        for tbl in &gq.tables {
            let _ = db
                .try_execute(&format!("DROP TABLE IF EXISTS {}", tbl.name))
                .await;
        }

        match outcome {
            CaseOutcome::UnsupportedAtAdmission(_) => {
                skipped += 1;
            }
            CaseOutcome::Passed(_) => {
                checked += 1;
            }
            CaseOutcome::ProductFailure(pf) => {
                checked += 1;
                mismatches.push((gq.seed, gq.description.clone(), pf.reason));
            }
            CaseOutcome::GeneratorInvalid(ge) => {
                panic!(
                    "Generator error (seed=0x{:016x}): {}: {}",
                    gq.seed, ge.stage, ge.message
                );
            }
            CaseOutcome::InfrastructureFailure(inf) => {
                panic!(
                    "Infrastructure failure (seed=0x{:016x}): {}",
                    gq.seed, inf.message
                );
            }
        }

        if (i + 1) % 50 == 0 {
            println!(
                "[sqlancer] progress: {}/{cases} — checked={checked} skipped={skipped} mismatches={}",
                i + 1,
                mismatches.len()
            );
        }
    }

    println!(
        "[sqlancer] equivalence oracle done: {cases} cases — \
         checked={checked}, skipped={skipped}, mismatches={}",
        mismatches.len()
    );

    if !mismatches.is_empty() {
        eprintln!("\n[sqlancer] EQUIVALENCE FAILURES:");
        for (seed, desc, msg) in &mismatches {
            eprintln!("  seed=0x{seed:016x}  [{desc}]  {msg}");
        }
        eprintln!(
            "\nTo replay: SQLANCER_SEED=0x{seed:016x} SQLANCER_CASES={cases} just sqlancer-fast",
        );
        panic!(
            "SQLANCER-2 equivalence oracle: {} mismatch(es) out of {checked} checked queries.",
            mismatches.len()
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_sqlancer_equivalence_oracle() {
    let db = E2eDb::new().await.with_extension().await;
    run_equivalence_oracle(&db).await;
}

// ── O39-11 (v0.39.0): Light PR-mode SQLancer tests ─────────────────────────
//
// These tests run a bounded subset (50 cases, fixed seed) and are NOT marked
// #[ignore] so they execute on every PR via the `light-e2e` CI job.  They use
// the same crash and equivalence oracles as the full tests but with a much
// smaller case count to keep PR CI time low (~30 s per test).
//
// The fixed seed ensures reproducibility: any query that triggers a crash or
// mismatch can be replayed with:
//   SQLANCER_SEED=0xc0ffee42dead1234 SQLANCER_CASES=50 just sqlancer-fast
//
// To run locally:
//   cargo test --test e2e_sqlancer_tests test_sqlancer_light -- --nocapture

/// SQLANCER-LIGHT-1: Crash oracle — 50 randomly generated queries must not crash.
#[tokio::test]
async fn test_sqlancer_crash_oracle_light() {
    let db = E2eDb::new().await.with_extension().await;

    // Fixed seed for deterministic PR-gate behaviour.
    let seed = std::env::var("SQLANCER_LIGHT_SEED")
        .ok()
        .and_then(|v| {
            let s = v.trim();
            if s.starts_with("0x") || s.starts_with("0X") {
                u64::from_str_radix(&s[2..], 16).ok()
            } else {
                s.parse::<u64>().ok()
            }
        })
        .unwrap_or(0xc0ffee42_dead1234_u64);

    let cases: usize = std::env::var("SQLANCER_LIGHT_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);

    let queries = generate_queries(seed, cases);
    let mut crashes = 0usize;

    for (i, gq) in queries.iter().enumerate() {
        for tbl in &gq.tables {
            db.execute(&tbl.ddl()).await;
            let mut rng = Lcg::new(gq.seed ^ (i as u64).wrapping_mul(0x9e3779b97f4a7c15));
            db.execute(&tbl.insert_dml(&mut rng)).await;
        }

        let st_name = format!("sqlancer_light1_{i}");
        let create_sql = format!(
            "SELECT pgtrickle.create_stream_table('{st_name}', $SQL${}$SQL$, '1m', 'FULL')",
            gq.query
        );
        let refresh_sql = format!("SELECT pgtrickle.refresh_stream_table('{st_name}')");

        let _ = db.try_execute(&create_sql).await;
        let r = db.try_execute(&refresh_sql).await;
        if let Err(e) = &r
            && oracle::is_infrastructure_sqlx_error(e)
        {
            crashes += 1;
        }

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
        for tbl in &gq.tables {
            let _ = db
                .try_execute(&format!("DROP TABLE IF EXISTS {} CASCADE", tbl.name))
                .await;
        }
    }

    assert_eq!(
        crashes, 0,
        "SQLANCER-LIGHT-1 crash oracle: {crashes} crash(es) in {cases} cases (seed=0x{seed:016x}).\n\
         Replay: SQLANCER_LIGHT_SEED=0x{seed:016x} SQLANCER_LIGHT_CASES={cases} just sqlancer-fast",
    );
}

/// SQLANCER-LIGHT-2: Equivalence oracle — 50 randomly generated queries must
/// match the direct SELECT as exact multisets.
#[tokio::test]
async fn test_sqlancer_equivalence_oracle_light() {
    let db = E2eDb::new().await.with_extension().await;

    let seed = std::env::var("SQLANCER_LIGHT_SEED")
        .ok()
        .and_then(|v| {
            let s = v.trim();
            if s.starts_with("0x") || s.starts_with("0X") {
                u64::from_str_radix(&s[2..], 16).ok()
            } else {
                s.parse::<u64>().ok()
            }
        })
        .unwrap_or(0xc0ffee42_dead1234_u64);

    let cases: usize = std::env::var("SQLANCER_LIGHT_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);

    let queries = generate_queries(seed, cases);
    let mut mismatches = Vec::<(u64, String, String)>::new();
    let mut checked = 0usize;
    let mut skipped = 0usize;

    for (i, gq) in queries.iter().enumerate() {
        for tbl in &gq.tables {
            db.execute(&tbl.ddl()).await;
            let mut rng = Lcg::new(gq.seed ^ (i as u64).wrapping_mul(0x6c62272e07bb0142));
            db.execute(&tbl.insert_dml(&mut rng)).await;
        }

        let st_name = format!("sqlancer_light2_{i}");
        let create_sql = format!(
            "SELECT pgtrickle.create_stream_table('{st_name}', $SQL${}$SQL$, '1m', 'FULL')",
            gq.query
        );
        let refresh_sql = format!("SELECT pgtrickle.refresh_stream_table('{st_name}')");

        let outcome: CaseOutcome = async {
            if let Err(e) = db.try_execute(&create_sql).await {
                return oracle::classify_admission_sqlx_error(&e);
            }
            if let Err(e) = db.try_execute(&refresh_sql).await {
                return CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("refresh failed: {e}"),
                    details: Some(gq.query.clone()),
                });
            }
            match oracle::compare_st_to_query(&db, &st_name, &gq.query).await {
                Ok(()) => CaseOutcome::Passed(oracle::PassReport {
                    effective_mode: "FULL".to_string(),
                    row_count: 0,
                }),
                Err(diff) => CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("exact oracle mismatch:\n{diff}"),
                    details: Some(gq.query.clone()),
                }),
            }
        }
        .await;

        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_name}')"))
            .await;
        for tbl in &gq.tables {
            let _ = db
                .try_execute(&format!("DROP TABLE IF EXISTS {} CASCADE", tbl.name))
                .await;
        }

        match outcome {
            CaseOutcome::UnsupportedAtAdmission(_) => {
                skipped += 1;
            }
            CaseOutcome::Passed(_) => {
                checked += 1;
            }
            CaseOutcome::ProductFailure(pf) => {
                checked += 1;
                mismatches.push((gq.seed, gq.query.clone(), pf.reason));
            }
            CaseOutcome::GeneratorInvalid(ge) => {
                panic!("Generator error: {}: {}", ge.stage, ge.message);
            }
            CaseOutcome::InfrastructureFailure(inf) => {
                panic!("Infrastructure failure: {}", inf.message);
            }
        }
    }

    println!(
        "[sqlancer-light] equivalence: checked={checked}, skipped={skipped}, mismatches={}",
        mismatches.len()
    );

    if !mismatches.is_empty() {
        for (seed, q, diff) in &mismatches {
            eprintln!("MISMATCH seed=0x{seed:016x}: {diff}\n  query: {q}");
        }
        panic!(
            "SQLANCER-LIGHT-2: {} mismatch(es) in {checked} queries (seed=0x{seed:016x}).\n\
             Replay: SQLANCER_LIGHT_SEED=0x{seed:016x} SQLANCER_LIGHT_CASES={cases} just sqlancer-fast",
            mismatches.len()
        );
    }
}

// ── DML mutation helpers ───────────────────────────────────────────────────

/// Read the number of stateful DML mutations from `SQLANCER_MUTATIONS`
/// (default: 100; set to 10000+ for nightly soak runs).
fn stateful_dml_mutations() -> usize {
    std::env::var("SQLANCER_MUTATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v: &usize| v > 0)
        .unwrap_or(100)
}

fn gen_insert_dml(rng: &mut Lcg, tbl: &TestTable, id: u64) -> String {
    let mut vals: Vec<String> = vec![id.to_string()];
    for (_, t) in &tbl.cols {
        let v = match *t {
            "INT" => (rng.range(1, 100) as i64).to_string(),
            "BIGINT" => (rng.range(1, 1000) as i64).to_string(),
            "NUMERIC" => format!("{}", rng.range(1, 500)),
            "TEXT" => {
                let choices = ["alpha", "beta", "gamma", "delta", "epsilon"];
                format!("'{}'", rng.choice(&choices))
            }
            _ => "0".to_string(),
        };
        vals.push(v);
    }
    format!("INSERT INTO {} VALUES ({})", tbl.name, vals.join(", "))
}

fn gen_update_dml(rng: &mut Lcg, tbl: &TestTable) -> Option<String> {
    let num_cols: Vec<_> = tbl.cols.iter().filter(|(_, t)| *t != "TEXT").collect();
    if num_cols.is_empty() {
        return None;
    }
    let (col, _) = rng.choice(&num_cols);
    let new_val = rng.range(1, 100);
    Some(format!(
        "UPDATE {name} SET {col} = {new_val} WHERE id = (SELECT id FROM {name} LIMIT 1)",
        name = tbl.name,
        col = col,
        new_val = new_val,
    ))
}

fn gen_delete_dml(tbl: &TestTable) -> String {
    format!(
        "DELETE FROM {name} WHERE id = (SELECT id FROM {name} LIMIT 1)",
        name = tbl.name,
    )
}

/// Apply one random INSERT / UPDATE / DELETE to `tbl`.
fn apply_random_mutation(rng: &mut Lcg, tbl: &TestTable, next_id: &mut u64) -> String {
    match rng.range(0, 2) {
        0 => {
            let sql = gen_insert_dml(rng, tbl, *next_id);
            *next_id += 1;
            sql
        }
        1 => gen_update_dml(rng, tbl).unwrap_or_else(|| gen_delete_dml(tbl)),
        _ => gen_delete_dml(tbl),
    }
}

/// Execute a planned mutation and reject ineffective DML. A correctness run
/// cannot continue after silently changing zero rows.
async fn execute_effective_dml(db: &E2eDb, sql: &str) -> Result<(), String> {
    let result = sqlx::query(sqlx::AssertSqlSafe(sql.to_string()))
        .execute(&db.pool)
        .await
        .map_err(|error| format!("{error}\nSQL: {sql}"))?;
    if result.rows_affected() == 0 {
        return Err(format!("mutation affected zero rows\nSQL: {sql}"));
    }
    Ok(())
}

// ── SQLANCER-3: DIFFERENTIAL ≡ FULL oracle after DML ─────────────────────

/// **SQLANCER-3 — Differential ≡ Full equivalence oracle after DML.**
///
/// For each generated query, creates two stream tables — one DIFFERENTIAL,
/// one FULL — applies a short random DML sequence, refreshes both, and
/// asserts that their exact multisets match.  Catches semantic bugs that only
/// surface after an UPDATE or DELETE (e.g. incorrect delta computation).
///
/// Run via `just sqlancer-fast` (combines SQLANCER-1 through SQLANCER-3).
///
/// See [`run_crash_oracle`] for the rationale behind the shared-database approach
/// and per-iteration cleanup.
async fn run_diff_vs_full_oracle(db: &E2eDb) {
    let cases = sqlancer_cases();
    let seed = base_seed();
    println!("[sqlancer-3] diff-vs-full oracle: {cases} cases, seed=0x{seed:016x}");

    let queries = generate_queries(seed, cases);
    let mut mismatches: Vec<(u64, String, String)> = Vec::new();
    let mut skipped = 0usize;
    let mut checked = 0usize;

    for (i, gq) in queries.iter().enumerate() {
        for tbl in &gq.tables {
            db.execute(&tbl.ddl()).await;
            let mut rng = Lcg::new(gq.seed ^ (i as u64).wrapping_mul(0x1234567890abcdef));
            db.execute(&tbl.insert_dml(&mut rng)).await;
        }

        let st_diff = format!("sqlancer_s3_diff_{i}");
        let st_full = format!("sqlancer_s3_full_{i}");

        let create_diff = format!(
            "SELECT pgtrickle.create_stream_table('{st_diff}', $SQL${}$SQL$, '1m', 'DIFFERENTIAL')",
            gq.query
        );
        let create_full = format!(
            "SELECT pgtrickle.create_stream_table('{st_full}', $SQL${}$SQL$, '1m', 'FULL')",
            gq.query
        );

        let refresh_diff_sql = format!("SELECT pgtrickle.refresh_stream_table('{st_diff}')");
        let refresh_full_sql = format!("SELECT pgtrickle.refresh_stream_table('{st_full}')");

        let outcome: CaseOutcome = async {
            // Skip if DIFFERENTIAL mode is not supported for this query.
            if let Err(e) = db.try_execute(&create_diff).await {
                return oracle::classify_admission_sqlx_error(&e);
            }
            if let Err(e) = db.try_execute(&create_full).await {
                return CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("FULL baseline admission failed: {e}"),
                    details: Some(gq.query.clone()),
                });
            }

            if let Err(e) = db.try_execute(&refresh_diff_sql).await {
                return CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("initial refresh_diff failed: {e}"),
                    details: Some(gq.query.clone()),
                });
            }
            if let Err(e) = db.try_execute(&refresh_full_sql).await {
                return CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("initial refresh_full failed: {e}"),
                    details: Some(gq.query.clone()),
                });
            }

            if let Err(pf) =
                oracle::assert_effective_refresh_mode(db, &st_diff, "DIFFERENTIAL").await
            {
                return CaseOutcome::ProductFailure(pf);
            }
            if let Err(pf) = oracle::assert_effective_refresh_mode(db, &st_full, "FULL").await {
                return CaseOutcome::ProductFailure(pf);
            }

            // Apply a short DML sequence across every source leaf.
            let mut rng = Lcg::new(gq.seed ^ 0xabcdef1234567890);
            let mut next_id = 10_000u64 + (i as u64 * 500);
            for mutation_index in 0..4 {
                let tbl = &gq.tables[mutation_index % gq.tables.len()];
                let sql = apply_random_mutation(&mut rng, tbl, &mut next_id);
                if let Err(e) = execute_effective_dml(db, &sql).await {
                    return CaseOutcome::GeneratorInvalid(oracle::GeneratorError {
                        stage: "DML mutation".to_string(),
                        message: e,
                    });
                }
            }

            if let Err(e) = db.try_execute(&refresh_diff_sql).await {
                return CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("post-DML refresh_diff failed: {e}"),
                    details: Some(gq.query.clone()),
                });
            }
            if let Err(e) = db.try_execute(&refresh_full_sql).await {
                return CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("post-DML refresh_full failed: {e}"),
                    details: Some(gq.query.clone()),
                });
            }

            if let Err(pf) =
                oracle::assert_effective_refresh_mode(db, &st_diff, "DIFFERENTIAL").await
            {
                return CaseOutcome::ProductFailure(pf);
            }
            if let Err(pf) = oracle::assert_effective_refresh_mode(db, &st_full, "FULL").await {
                return CaseOutcome::ProductFailure(pf);
            }

            // Compare DIFFERENTIAL vs FULL as exact multisets
            if let Err(diff) = oracle::compare_sts(db, &st_diff, &st_full).await {
                return CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("DIFF vs FULL multiset mismatch after DML:\n{diff}"),
                    details: Some(gq.query.clone()),
                });
            }

            // Compare DIFFERENTIAL vs Direct Query as exact multisets
            if let Err(diff) = oracle::compare_st_to_query(db, &st_diff, &gq.query).await {
                return CaseOutcome::ProductFailure(oracle::ProductFailure {
                    reason: format!("DIFF vs Direct Query multiset mismatch after DML:\n{diff}"),
                    details: Some(gq.query.clone()),
                });
            }

            CaseOutcome::Passed(oracle::PassReport {
                effective_mode: "DIFFERENTIAL".to_string(),
                row_count: 0,
            })
        }
        .await;

        // Cleanup: always drop both stream tables and all source tables so they do
        // not accumulate across iterations (see run_crash_oracle for rationale).
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_diff}')"))
            .await;
        let _ = db
            .try_execute(&format!("SELECT pgtrickle.drop_stream_table('{st_full}')"))
            .await;
        for tbl in &gq.tables {
            let _ = db
                .try_execute(&format!("DROP TABLE IF EXISTS {}", tbl.name))
                .await;
        }

        match outcome {
            CaseOutcome::UnsupportedAtAdmission(_) => {
                skipped += 1;
            }
            CaseOutcome::Passed(_) => {
                checked += 1;
            }
            CaseOutcome::ProductFailure(pf) => {
                checked += 1;
                mismatches.push((gq.seed, gq.description.clone(), pf.reason));
            }
            CaseOutcome::GeneratorInvalid(ge) => {
                panic!("Generator error: {}: {}", ge.stage, ge.message);
            }
            CaseOutcome::InfrastructureFailure(inf) => {
                panic!("Infrastructure failure: {}", inf.message);
            }
        }

        if (i + 1) % 25 == 0 {
            println!(
                "[sqlancer-3] progress: {}/{cases} — \
                 checked={checked} skipped={skipped} mismatches={}",
                i + 1,
                mismatches.len()
            );
        }
    }

    println!(
        "[sqlancer-3] diff-vs-full oracle done: {cases} cases — \
         checked={checked}, skipped={skipped}, mismatches={}",
        mismatches.len()
    );

    if !mismatches.is_empty() {
        eprintln!("\n[sqlancer-3] DIFF-vs-FULL FAILURES:");
        for (seed, desc, msg) in &mismatches {
            eprintln!("  seed=0x{seed:016x}  [{desc}]  {msg}");
        }
        eprintln!(
            "\nTo replay: SQLANCER_SEED=0x{seed:016x} SQLANCER_CASES={cases} just sqlancer-fast",
        );
        panic!(
            "SQLANCER-3 diff-vs-full oracle: {} mismatch(es) out of {checked} checked queries.",
            mismatches.len()
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_sqlancer_diff_vs_full_oracle() {
    let db = E2eDb::new().await.with_extension().await;
    run_diff_vs_full_oracle(&db).await;
}

// ── SQLANCER-4: Stateful DML fuzzing ──────────────────────────────────────

/// **SQLANCER-4 — Stateful DML fuzzing.**
///
/// Finds the first generated query that pg_trickle supports in DIFFERENTIAL
/// mode, then runs `SQLANCER_MUTATIONS` (default 100, set to 10 000 for
/// nightly) random INSERT/UPDATE/DELETE operations across every source leaf.
/// Every `SQLANCER_CHECKPOINT_INTERVAL` (50) mutations it refreshes both
/// a DIFFERENTIAL stream table and a FULL-mode baseline and asserts that
/// their exact multisets and direct-query result agree. Catches state-dependent bugs that only manifest
/// after specific mutation histories.
///
/// Run via `just sqlancer-stateful-fast` or in the `weekly-sqlancer-stateful`
/// CI job with `SQLANCER_MUTATIONS=10000`.
async fn run_stateful_dml_fuzzing() {
    let mutations = stateful_dml_mutations();
    let seed = base_seed();
    const CHECKPOINT_INTERVAL: usize = 50;

    println!(
        "[sqlancer-4] stateful DML fuzzing: {mutations} mutations, \
         checkpoint every {CHECKPOINT_INTERVAL}, seed=0x{seed:016x}"
    );

    // ── Find a query supported by DIFFERENTIAL mode ───────────────────
    let probe_queries = generate_queries(seed, 50);
    let db = E2eDb::new().await.with_extension().await;
    let mut chosen: Option<GeneratedQuery> = None;

    for (i, gq) in probe_queries.iter().enumerate() {
        let mut rng = Lcg::new(gq.seed ^ (i as u64).wrapping_mul(0x1234567890abcdef));
        for tbl in &gq.tables {
            db.execute(&tbl.ddl()).await;
            db.execute(&tbl.insert_dml(&mut rng)).await;
        }

        let create_sql = format!(
            "SELECT pgtrickle.create_stream_table('soak_probe', $SQL${}$SQL$, '1m', 'DIFFERENTIAL')",
            gq.query
        );

        let differential_supported = db.try_execute(&create_sql).await.is_ok()
            && db
                .try_execute("SELECT pgtrickle.refresh_stream_table('soak_probe')")
                .await
                .is_ok()
            && oracle::assert_effective_refresh_mode(&db, "soak_probe", "DIFFERENTIAL")
                .await
                .is_ok();
        if differential_supported {
            let _ = db
                .try_execute("SELECT pgtrickle.drop_stream_table('soak_probe')")
                .await;
            chosen = Some(gq.clone());
            break;
        }

        let _ = db
            .try_execute("SELECT pgtrickle.drop_stream_table('soak_probe')")
            .await;
        for tbl in &gq.tables {
            let _ = db
                .try_execute(&format!("DROP TABLE IF EXISTS {}", tbl.name))
                .await;
        }
    }

    let Some(gq) = chosen else {
        println!("[sqlancer-4] SKIP: no DIFFERENTIAL-supported query found in probe corpus");
        return;
    };

    println!("[sqlancer-4] running soak on: {}", gq.description);

    // ── Create the soak stream tables ─────────────────────────────────
    let st_diff = "sqlancer_soak_diff";
    let st_full = "sqlancer_soak_full";

    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('{st_diff}', $SQL${}$SQL$, '1m', 'DIFFERENTIAL')",
        gq.query
    ))
    .await;

    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('{st_full}', $SQL${}$SQL$, '1m', 'FULL')",
        gq.query
    ))
    .await;

    db.execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('{st_diff}')"
    ))
    .await;
    db.execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('{st_full}')"
    ))
    .await;
    oracle::assert_effective_refresh_mode(&db, st_diff, "DIFFERENTIAL")
        .await
        .unwrap_or_else(|failure| panic!("[sqlancer-4] {failure:?}"));
    oracle::assert_effective_refresh_mode(&db, st_full, "FULL")
        .await
        .unwrap_or_else(|failure| panic!("[sqlancer-4] {failure:?}"));

    // ── Mutation loop ─────────────────────────────────────────────────
    let mut rng = Lcg::new(seed ^ 0xfeedfacecafebeef);
    let mut next_id = 50_000u64;
    let mut applied = 0usize;
    let mut mismatches: Vec<(usize, String)> = Vec::new();

    for m in 0..mutations {
        let source_tbl = &gq.tables[m % gq.tables.len()];
        let sql = apply_random_mutation(&mut rng, source_tbl, &mut next_id);
        if let Err(error) = execute_effective_dml(&db, &sql).await {
            panic!(
                "[sqlancer-4] Generator failure at mutation {m}: {error}\nquery: {}",
                gq.query
            );
        }
        applied += 1;

        if (m + 1) % CHECKPOINT_INTERVAL == 0 {
            if let Err(e) = db
                .try_execute(&format!(
                    "SELECT pgtrickle.refresh_stream_table('{st_diff}')"
                ))
                .await
            {
                panic!(
                    "[sqlancer-4] Product failure: refresh_diff failed at mutation {m}: {e}\n  query: {}",
                    gq.query
                );
            }
            db.execute(&format!(
                "SELECT pgtrickle.refresh_stream_table('{st_full}')"
            ))
            .await;

            oracle::assert_effective_refresh_mode(&db, st_diff, "DIFFERENTIAL")
                .await
                .unwrap_or_else(|failure| panic!("[sqlancer-4] {failure:?}"));
            oracle::assert_effective_refresh_mode(&db, st_full, "FULL")
                .await
                .unwrap_or_else(|failure| panic!("[sqlancer-4] {failure:?}"));

            if let Err(diff) = oracle::compare_sts(&db, st_diff, st_full).await {
                mismatches.push((m + 1, format!("DIFF vs FULL mismatch:\n{diff}")));
                eprintln!("[sqlancer-4] MISMATCH at mutation {}:\n{diff}", m + 1);
            } else if let Err(diff) = oracle::compare_st_to_query(&db, st_diff, &gq.query).await {
                mismatches.push((m + 1, format!("DIFF vs Query mismatch:\n{diff}")));
                eprintln!("[sqlancer-4] MISMATCH at mutation {}:\n{diff}", m + 1);
            } else {
                println!(
                    "[sqlancer-4] checkpoint {}/{mutations}: ok (applied={applied})",
                    m + 1
                );
            }
        }
    }

    println!(
        "[sqlancer-4] stateful DML done: {mutations} mutations, \
         {applied} applied, {} checkpoints, {} mismatches",
        mutations / CHECKPOINT_INTERVAL,
        mismatches.len()
    );

    assert!(
        mismatches.is_empty(),
        "SQLANCER-4: {} mismatch(es) in stateful DML fuzzing over {mutations} mutations \
         (query: {}, seed=0x{seed:016x})",
        mismatches.len(),
        gq.description,
    );
}

#[tokio::test]
#[ignore]
async fn test_sqlancer_stateful_dml() {
    run_stateful_dml_fuzzing().await;
}

// ── SQLANCER: stress + crash combined (CI entry point) ────────────────────

/// Combined crash + equivalence + diff-vs-full oracle for CI (SQLANCER-1–3).
///
/// Runs in the `weekly-sqlancer` CI job. Uses `SQLANCER_CASES` to control
/// case count (default 200 for quick CI runs; 2 000 for nightly).
/// The stateful DML soak test (SQLANCER-4) runs separately via
/// `test_sqlancer_stateful_dml` with `SQLANCER_MUTATIONS=10000`.
///
/// All three oracle phases share a **single** `E2eDb` (and therefore a single
/// PostgreSQL database).  The previous approach created one database per oracle
/// phase; since pg_trickle background workers allocate POSIX shared memory
/// (DSM) for each active database, three live databases could exhaust the
/// container's `/dev/shm` (512 MB) before the equivalence oracle even started.
/// Sharing one database keeps DSM usage bounded.  Each oracle function cleans
/// up its tables after every iteration so the database stays lean.
#[tokio::test]
#[ignore]
async fn test_sqlancer_ci_combined() {
    let db = E2eDb::new().await.with_extension().await;
    run_crash_oracle(&db).await;
    run_equivalence_oracle(&db).await;
    run_diff_vs_full_oracle(&db).await;
}
