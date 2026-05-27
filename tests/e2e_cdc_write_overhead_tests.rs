//! BENCH-W1 — CDC write-side overhead benchmark.
//!
//! Measures the DML throughput overhead introduced by pg_trickle's CDC
//! triggers on source tables. Compares "source-only" (no stream table)
//! vs "source + stream table" (CDC triggers active) across five scenarios:
//!
//! 1. **Single-row INSERT** — one INSERT per statement
//! 2. **Bulk INSERT** — 10,000 rows per statement
//! 3. **Bulk UPDATE** — 10,000 rows per statement
//! 4. **Bulk DELETE** — 10,000 rows per statement
//! 5. **Concurrent writers** — 4 parallel sessions doing bulk INSERTs
//!
//! For each scenario, the benchmark reports:
//! - **Baseline** (no trigger): wall-clock time
//! - **With CDC trigger**: wall-clock time
//! - **Write amplification factor**: CDC time / baseline time
//!
//! All tests are `#[ignore]`d and require the E2E Docker image.
//!
//! ```bash
//! cargo test --test e2e_cdc_write_overhead_tests --features pg18 -- --ignored --nocapture
//! ```

mod e2e;

use e2e::E2eDb;
use std::time::Instant;

// ── Configuration ──────────────────────────────────────────────────────

/// Number of single-row INSERTs per measurement cycle.
const SINGLE_ROW_COUNT: usize = 1_000;

/// Number of rows per bulk DML statement.
const BULK_ROWS: usize = 10_000;

/// Number of measurement cycles per scenario.
const CYCLES: usize = 5;

/// Number of warm-up cycles (discarded).
const WARMUP_CYCLES: usize = 1;

/// Number of concurrent writer sessions for scenario 5.
const CONCURRENT_WRITERS: usize = 4;

/// Rows per writer in the concurrent scenario.
const ROWS_PER_WRITER: usize = 5_000;

// ── Result types ───────────────────────────────────────────────────────

struct ScenarioResult {
    name: &'static str,
    baseline_ms: Vec<f64>,
    with_cdc_ms: Vec<f64>,
}

impl ScenarioResult {
    fn avg_baseline(&self) -> f64 {
        self.baseline_ms.iter().sum::<f64>() / self.baseline_ms.len() as f64
    }

    fn avg_cdc(&self) -> f64 {
        self.with_cdc_ms.iter().sum::<f64>() / self.with_cdc_ms.len() as f64
    }

    fn write_amplification(&self) -> f64 {
        self.avg_cdc() / self.avg_baseline()
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Create the source table used by all scenarios.
async fn create_source_table(db: &E2eDb) {
    db.execute(
        "CREATE TABLE IF NOT EXISTS bench_src (
            id SERIAL PRIMARY KEY,
            region TEXT NOT NULL DEFAULT 'north',
            category TEXT NOT NULL DEFAULT 'A',
            amount INT NOT NULL DEFAULT 100,
            score INT NOT NULL DEFAULT 50
         )",
    )
    .await;
}

/// Populate the source table with `n` rows.
async fn populate_source(db: &E2eDb, n: usize) {
    db.execute(&format!(
        "INSERT INTO bench_src (region, category, amount, score)
         SELECT
             CASE (i % 5)
                 WHEN 0 THEN 'north' WHEN 1 THEN 'south' WHEN 2 THEN 'east'
                 WHEN 3 THEN 'west'  ELSE 'central'
             END,
             CASE (i % 4) WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' ELSE 'D' END,
             (i * 17 + 13) % 10000,
             (i * 31 + 7) % 100
         FROM generate_series(1, {n}) AS s(i)"
    ))
    .await;
}

/// Reset the source table (TRUNCATE + repopulate).
async fn reset_source(db: &E2eDb, n: usize) {
    db.execute("TRUNCATE bench_src RESTART IDENTITY CASCADE")
        .await;
    populate_source(db, n).await;
}

/// Create a stream table on the source (activates CDC triggers).
async fn create_stream_table(db: &E2eDb) {
    db.create_st(
        "bench_st",
        "SELECT id, region, category, amount, score FROM bench_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("bench_st").await;
}

/// Cleanup the stream table.
async fn drop_stream_table(db: &E2eDb) {
    db.drop_st("bench_st").await;
}

/// Measure wall-clock time of a SQL execution in milliseconds.
async fn timed_execute(db: &E2eDb, sql: &str) -> f64 {
    let start = Instant::now();
    db.execute(sql).await;
    start.elapsed().as_secs_f64() * 1000.0
}

// ── Print helpers ──────────────────────────────────────────────────────

fn print_results(results: &[ScenarioResult]) {
    eprintln!();
    eprintln!(
        "╔═══════════════════════════════════════════════════════════════════════════════════╗"
    );
    eprintln!(
        "║               pg_trickle CDC Write-Side Overhead Benchmark                       ║"
    );
    eprintln!(
        "╠═══════════════════════╤═══════════════╤═══════════════╤═════════════════════════╣"
    );
    eprintln!(
        "║ Scenario              │ Baseline (ms) │ With CDC (ms) │ Write Amplification     ║"
    );
    eprintln!(
        "╠═══════════════════════╪═══════════════╪═══════════════╪═════════════════════════╣"
    );
    for r in results {
        eprintln!(
            "║ {:<21} │ {:>13.1} │ {:>13.1} │ {:>10.2}×             ║",
            r.name,
            r.avg_baseline(),
            r.avg_cdc(),
            r.write_amplification(),
        );
    }
    eprintln!(
        "╚═══════════════════════╧═══════════════╧═══════════════╧═════════════════════════╝"
    );
    eprintln!();
}

fn print_machine_readable(results: &[ScenarioResult]) {
    for r in results {
        eprintln!(
            "[CDC_BENCH] scenario={} baseline_avg_ms={:.1} cdc_avg_ms={:.1} write_amplification={:.2}",
            r.name.replace(' ', "_"),
            r.avg_baseline(),
            r.avg_cdc(),
            r.write_amplification(),
        );
    }
}

// ── Scenario 1: Single-Row INSERT ──────────────────────────────────────

async fn bench_single_row_insert(db: &E2eDb) -> ScenarioResult {
    let mut baseline_ms = Vec::new();
    let mut with_cdc_ms = Vec::new();

    // ── Baseline (no stream table) ──
    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        db.execute("TRUNCATE bench_src RESTART IDENTITY").await;
        let start = Instant::now();
        for i in 1..=SINGLE_ROW_COUNT {
            db.execute(&format!(
                "INSERT INTO bench_src (region, amount) VALUES ('north', {i})"
            ))
            .await;
        }
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if cycle >= WARMUP_CYCLES {
            baseline_ms.push(elapsed);
        }
    }

    // ── With CDC triggers ──
    db.execute("TRUNCATE bench_src RESTART IDENTITY").await;
    populate_source(db, 100).await; // need some data for ST creation
    create_stream_table(db).await;

    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        // Clear source but keep the ST triggers
        db.execute("DELETE FROM bench_src").await;
        db.refresh_st("bench_st").await;

        let start = Instant::now();
        for i in 1..=SINGLE_ROW_COUNT {
            db.execute(&format!(
                "INSERT INTO bench_src (region, amount) VALUES ('north', {i})"
            ))
            .await;
        }
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if cycle >= WARMUP_CYCLES {
            with_cdc_ms.push(elapsed);
        }
    }

    drop_stream_table(db).await;

    ScenarioResult {
        name: "single-row INSERT",
        baseline_ms,
        with_cdc_ms,
    }
}

// ── Scenario 2: Bulk INSERT ────────────────────────────────────────────

async fn bench_bulk_insert(db: &E2eDb) -> ScenarioResult {
    let mut baseline_ms = Vec::new();
    let mut with_cdc_ms = Vec::new();

    let insert_sql = format!(
        "INSERT INTO bench_src (region, category, amount, score)
         SELECT
             CASE (i % 5) WHEN 0 THEN 'north' WHEN 1 THEN 'south' WHEN 2 THEN 'east'
                          WHEN 3 THEN 'west'  ELSE 'central' END,
             CASE (i % 4) WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' ELSE 'D' END,
             (i * 17 + 13) % 10000,
             (i * 31 + 7) % 100
         FROM generate_series(1, {BULK_ROWS}) AS s(i)"
    );

    // ── Baseline ──
    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        db.execute("TRUNCATE bench_src RESTART IDENTITY").await;
        let ms = timed_execute(db, &insert_sql).await;
        if cycle >= WARMUP_CYCLES {
            baseline_ms.push(ms);
        }
    }

    // ── With CDC ──
    db.execute("TRUNCATE bench_src RESTART IDENTITY").await;
    populate_source(db, 100).await;
    create_stream_table(db).await;

    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        db.execute("DELETE FROM bench_src").await;
        db.refresh_st("bench_st").await;

        let ms = timed_execute(db, &insert_sql).await;
        if cycle >= WARMUP_CYCLES {
            with_cdc_ms.push(ms);
        }
    }

    drop_stream_table(db).await;

    ScenarioResult {
        name: "bulk INSERT (10K)",
        baseline_ms,
        with_cdc_ms,
    }
}

// ── Scenario 3: Bulk UPDATE ────────────────────────────────────────────

async fn bench_bulk_update(db: &E2eDb) -> ScenarioResult {
    let mut baseline_ms = Vec::new();
    let mut with_cdc_ms = Vec::new();

    let update_sql = format!("UPDATE bench_src SET amount = amount + 1 WHERE id <= {BULK_ROWS}");

    // ── Baseline ──
    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        reset_source(db, BULK_ROWS).await;
        let ms = timed_execute(db, &update_sql).await;
        if cycle >= WARMUP_CYCLES {
            baseline_ms.push(ms);
        }
    }

    // ── With CDC ──
    reset_source(db, BULK_ROWS).await;
    create_stream_table(db).await;

    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        // Reset source data to a known state
        db.execute(&format!(
            "UPDATE bench_src SET amount = (id * 17 + 13) % 10000 WHERE id <= {BULK_ROWS}"
        ))
        .await;
        db.refresh_st("bench_st").await;

        let ms = timed_execute(db, &update_sql).await;
        if cycle >= WARMUP_CYCLES {
            with_cdc_ms.push(ms);
        }
    }

    drop_stream_table(db).await;

    ScenarioResult {
        name: "bulk UPDATE (10K)",
        baseline_ms,
        with_cdc_ms,
    }
}

// ── Scenario 4: Bulk DELETE ────────────────────────────────────────────

async fn bench_bulk_delete(db: &E2eDb) -> ScenarioResult {
    let mut baseline_ms = Vec::new();
    let mut with_cdc_ms = Vec::new();

    // ── Baseline ──
    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        reset_source(db, BULK_ROWS).await;
        let ms = timed_execute(
            db,
            &format!("DELETE FROM bench_src WHERE id <= {BULK_ROWS}"),
        )
        .await;
        if cycle >= WARMUP_CYCLES {
            baseline_ms.push(ms);
        }
    }

    // ── With CDC ──
    reset_source(db, BULK_ROWS).await;
    create_stream_table(db).await;

    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        // Refill the table for a fresh DELETE measurement
        db.execute("DELETE FROM bench_src").await;
        db.refresh_st("bench_st").await;
        populate_source(db, BULK_ROWS).await;
        db.refresh_st("bench_st").await;

        let ms = timed_execute(
            db,
            &format!("DELETE FROM bench_src WHERE id <= {BULK_ROWS}"),
        )
        .await;
        if cycle >= WARMUP_CYCLES {
            with_cdc_ms.push(ms);
        }
    }

    drop_stream_table(db).await;

    ScenarioResult {
        name: "bulk DELETE (10K)",
        baseline_ms,
        with_cdc_ms,
    }
}

// ── Scenario 5: Concurrent Writers ─────────────────────────────────────

async fn bench_concurrent_writers(db: &E2eDb) -> ScenarioResult {
    let mut baseline_ms = Vec::new();
    let mut with_cdc_ms = Vec::new();

    let conn_str = db.connection_string().to_string();

    // ── Baseline ──
    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        db.execute("TRUNCATE bench_src RESTART IDENTITY").await;

        let start = Instant::now();
        let mut handles = Vec::new();
        for w in 0..CONCURRENT_WRITERS {
            let cs = conn_str.clone();
            handles.push(tokio::spawn(async move {
                let pool = sqlx::PgPool::connect(&cs).await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO bench_src (region, category, amount, score)
                     SELECT
                         CASE (i % 5) WHEN 0 THEN 'north' WHEN 1 THEN 'south'
                                      WHEN 2 THEN 'east'  WHEN 3 THEN 'west'
                                      ELSE 'central' END,
                         'W{w}',
                         (i * 17 + 13) % 10000,
                         (i * 31 + 7) % 100
                     FROM generate_series(1, {ROWS_PER_WRITER}) AS s(i)"
                )))
                .execute(&pool)
                .await
                .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if cycle >= WARMUP_CYCLES {
            baseline_ms.push(elapsed);
        }
    }

    // ── With CDC ──
    db.execute("TRUNCATE bench_src RESTART IDENTITY").await;
    populate_source(db, 100).await;
    create_stream_table(db).await;

    for cycle in 0..(WARMUP_CYCLES + CYCLES) {
        db.execute("DELETE FROM bench_src").await;
        db.refresh_st("bench_st").await;

        let start = Instant::now();
        let mut handles = Vec::new();
        for w in 0..CONCURRENT_WRITERS {
            let cs = conn_str.clone();
            handles.push(tokio::spawn(async move {
                let pool = sqlx::PgPool::connect(&cs).await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO bench_src (region, category, amount, score)
                     SELECT
                         CASE (i % 5) WHEN 0 THEN 'north' WHEN 1 THEN 'south'
                                      WHEN 2 THEN 'east'  WHEN 3 THEN 'west'
                                      ELSE 'central' END,
                         'W{w}',
                         (i * 17 + 13) % 10000,
                         (i * 31 + 7) % 100
                     FROM generate_series(1, {ROWS_PER_WRITER}) AS s(i)"
                )))
                .execute(&pool)
                .await
                .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if cycle >= WARMUP_CYCLES {
            with_cdc_ms.push(elapsed);
        }
    }

    drop_stream_table(db).await;

    ScenarioResult {
        name: "concurrent (4×5K)",
        baseline_ms,
        with_cdc_ms,
    }
}

// ── Full Suite ─────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn bench_cdc_write_overhead_full() {
    let db = E2eDb::new().await.with_extension().await;
    create_source_table(&db).await;

    let mut results = Vec::new();
    results.push(bench_single_row_insert(&db).await);
    results.push(bench_bulk_insert(&db).await);
    results.push(bench_bulk_update(&db).await);
    results.push(bench_bulk_delete(&db).await);
    results.push(bench_concurrent_writers(&db).await);

    print_results(&results);
    print_machine_readable(&results);

    // Sanity: write amplification should be > 1.0 (triggers add overhead)
    // but not astronomically high (> 10× would indicate a problem).
    for r in &results {
        assert!(
            r.write_amplification() > 0.5,
            "{}: write amplification {:.2}× is suspiciously low",
            r.name,
            r.write_amplification(),
        );
        assert!(
            r.write_amplification() < 20.0,
            "{}: write amplification {:.2}× is too high — investigate trigger overhead",
            r.name,
            r.write_amplification(),
        );
    }
}

// ── Individual scenario tests ──────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn bench_cdc_single_row_insert() {
    let db = E2eDb::new().await.with_extension().await;
    create_source_table(&db).await;
    let result = bench_single_row_insert(&db).await;
    print_results(&[result]);
}

#[tokio::test]
#[ignore]
async fn bench_cdc_bulk_insert() {
    let db = E2eDb::new().await.with_extension().await;
    create_source_table(&db).await;
    let result = bench_bulk_insert(&db).await;
    print_results(&[result]);
}

#[tokio::test]
#[ignore]
async fn bench_cdc_bulk_update() {
    let db = E2eDb::new().await.with_extension().await;
    create_source_table(&db).await;
    let result = bench_bulk_update(&db).await;
    print_results(&[result]);
}

#[tokio::test]
#[ignore]
async fn bench_cdc_bulk_delete() {
    let db = E2eDb::new().await.with_extension().await;
    create_source_table(&db).await;
    let result = bench_bulk_delete(&db).await;
    print_results(&[result]);
}

#[tokio::test]
#[ignore]
async fn bench_cdc_concurrent_writers() {
    let db = E2eDb::new().await.with_extension().await;
    create_source_table(&db).await;
    let result = bench_concurrent_writers(&db).await;
    print_results(&[result]);
}
