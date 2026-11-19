//! G17-SOAK: Long-running stability soak test for pg_trickle.
//!
//! Validates that the extension operates correctly under sustained mixed DML
//! workload over an extended period. Checks:
//!   - Zero background worker crashes
//!   - Zero stream tables in zombie/ERROR state
//!   - Stable memory usage (RSS growth < 20% over the test duration)
//!   - Correctness: stream table contents match defining queries
//!
//! The long soak is `#[ignore]`d; bounded correctness regression tests run in normal E2E suites.
//! Run manually or via CI:
//!
//! ```bash
//! just test-soak          # Default: 10 minutes (configurable via SOAK_DURATION_SECS)
//! just test-soak-short    # Quick: 2 minutes
//! ```
//!
//! Environment variables:
//!   SOAK_DURATION_SECS  — Total soak duration in seconds (default: 600 = 10 min)
//!   SOAK_CYCLE_MS       — Milliseconds between DML batches (default: 500)
//!   SOAK_BATCH_SIZE     — Rows per INSERT/UPDATE/DELETE batch (default: 50)
//!   SOAK_NUM_SOURCES    — Number of source tables (default: 5)
//!
//! Prerequisites: E2E Docker image (`just build-e2e-image`)

mod e2e;

use e2e::{E2eDb, oracle};
use std::time::{Duration, Instant};

// ── Configuration ──────────────────────────────────────────────────────

fn soak_duration_secs() -> u64 {
    std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600)
}

fn cycle_delay_ms() -> u64 {
    std::env::var("SOAK_CYCLE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500)
}

fn batch_size() -> usize {
    std::env::var("SOAK_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
}

fn num_sources() -> usize {
    std::env::var("SOAK_NUM_SOURCES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

// ── Schema Setup ───────────────────────────────────────────────────────

/// Create N source tables with varied schemas to exercise different DVM paths.
async fn create_source_tables(db: &E2eDb, n: usize) {
    for i in 1..=n {
        db.execute(&format!(
            "CREATE TABLE source_{i} (
                id SERIAL PRIMARY KEY,
                category INT NOT NULL DEFAULT (random() * 10)::int,
                value NUMERIC(12,2) NOT NULL DEFAULT (random() * 1000)::numeric(12,2),
                label TEXT NOT NULL DEFAULT 'row_' || gen_random_uuid()::text,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ))
        .await;

        // Seed with initial data
        db.execute(&format!(
            "INSERT INTO source_{i} (category, value, label)
             SELECT (random() * 10)::int,
                    (random() * 1000)::numeric(12,2),
                    'seed_' || g
             FROM generate_series(1, 200) g"
        ))
        .await;
    }
}

/// Create stream tables covering different DVM operator paths.
async fn create_stream_tables(db: &E2eDb, n: usize) {
    // ST1: Simple aggregation (SUM/COUNT)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'soak_agg',
            'SELECT category, SUM(value) AS total, COUNT(*) AS cnt
             FROM source_1 GROUP BY category',
            schedule => '1s',
            refresh_mode => 'DIFFERENTIAL'
        )",
    )
    .await;

    // ST2: Two-table join
    if n >= 2 {
        db.execute(
            "SELECT pgtrickle.create_stream_table(
                'soak_join',
                'SELECT s1.id, s1.category, s1.value, s2.label AS label_2
                 FROM source_1 s1
                 JOIN source_2 s2 ON s1.category = s2.category',
                schedule => '1s',
                refresh_mode => 'DIFFERENTIAL'
            )",
        )
        .await;
    }

    // ST3: UNION ALL of multiple sources
    if n >= 3 {
        db.execute(
            "SELECT pgtrickle.create_stream_table(
                'soak_union',
                'SELECT id, category, value FROM source_1
                 UNION ALL
                 SELECT id, category, value FROM source_2
                 UNION ALL
                 SELECT id, category, value FROM source_3',
                schedule => '1s',
                refresh_mode => 'DIFFERENTIAL'
            )",
        )
        .await;
    }

    // ST4: LEFT JOIN with aggregate
    if n >= 4 {
        db.execute(
            "SELECT pgtrickle.create_stream_table(
                'soak_left_agg',
                'SELECT s1.category,
                        COUNT(s2.id) AS matched_count,
                        COALESCE(SUM(s2.value), 0) AS matched_total
                 FROM source_1 s1
                 LEFT JOIN source_4 s2 ON s1.category = s2.category
                 GROUP BY s1.category',
                schedule => '1s',
                refresh_mode => 'DIFFERENTIAL'
            )",
        )
        .await;
    }

    // ST5: Filter + projection (simple scan path)
    if n >= 5 {
        db.execute(
            "SELECT pgtrickle.create_stream_table(
                'soak_filter',
                'SELECT id, category, value, label
                 FROM source_5
                 WHERE category >= 5',
                schedule => '1s',
                refresh_mode => 'DIFFERENTIAL'
            )",
        )
        .await;
    }
}

// ── DML Operations ─────────────────────────────────────────────────────

/// Apply a mixed DML batch to a random source table.
async fn apply_dml_batch(db: &E2eDb, source_idx: usize, batch: usize, cycle: u64) {
    let table = format!("source_{source_idx}");

    // INSERT batch — labels include cycle number to ensure uniqueness.
    // Without this, repeated 'batch_{batch}_N' labels across cycles create
    // duplicate (category, label) pairs in source tables.  For soak_join
    // (which projects label but not s2.id), duplicate output rows share the
    // same __pgt_row_id, violating the UNIQUE constraint and causing
    // monotonic row-count drift during differential refresh.
    db.execute(&format!(
        "INSERT INTO {table} (category, value, label)
         SELECT (random() * 10)::int,
                (random() * 1000)::numeric(12,2),
                'c{cycle}_' || g
         FROM generate_series(1, {bs}) g",
        bs = batch
    ))
    .await;

    // UPDATE ~30% of the batch size
    let update_count = (batch as f64 * 0.3).max(1.0) as usize;
    db.execute(&format!(
        "UPDATE {table}
         SET value = value + (random() * 100)::numeric(12,2),
             label = label || '_upd'
         WHERE id IN (
             SELECT id FROM {table} ORDER BY random() LIMIT {update_count}
         )"
    ))
    .await;

    // DELETE ~10% of the batch size
    let delete_count = (batch as f64 * 0.1).max(1.0) as usize;
    db.execute(&format!(
        "DELETE FROM {table}
         WHERE id IN (
             SELECT id FROM {table} ORDER BY random() LIMIT {delete_count}
         )"
    ))
    .await;
}

// ── Health Checks ──────────────────────────────────────────────────────

/// Check that no stream tables are in ERROR or SUSPENDED state.
async fn check_no_error_states(db: &E2eDb) -> Result<(), String> {
    let error_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_stream_tables
             WHERE status IN ('ERROR', 'SUSPENDED')",
        )
        .await;

    if error_count > 0 {
        let errors: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT pgt_name::text, status::text, last_error_message
             FROM pgtrickle.pgt_stream_tables
             WHERE status IN ('ERROR', 'SUSPENDED')",
        )
        .fetch_all(&db.pool)
        .await
        .unwrap_or_default();

        let details: Vec<String> = errors
            .iter()
            .map(|(name, status, msg)| {
                format!(
                    "  {} ({}): {}",
                    name,
                    status,
                    msg.as_deref().unwrap_or("(no message)")
                )
            })
            .collect();

        return Err(format!(
            "{} stream table(s) in error state:\n{}",
            error_count,
            details.join("\n")
        ));
    }
    Ok(())
}

/// Check that the background worker is running (not crashed).
async fn check_worker_alive(db: &E2eDb) -> Result<(), String> {
    let alive: bool = db
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE backend_type = 'pg_trickle scheduler'
            )",
        )
        .await;

    if !alive {
        return Err("pg_trickle scheduler background worker not found in pg_stat_activity".into());
    }
    Ok(())
}

/// Get the current RSS of the PostgreSQL backend (in KB).
/// Returns None if the platform doesn't support this.
async fn get_rss_kb(db: &E2eDb) -> Option<i64> {
    // Use /proc/self/status on Linux (Docker container)
    db.try_execute(
        "CREATE OR REPLACE FUNCTION pg_temp.get_rss_kb()
         RETURNS BIGINT LANGUAGE plpgsql AS $$
         DECLARE
             line TEXT;
             rss_kb BIGINT;
         BEGIN
             FOR line IN SELECT pg_read_file('/proc/self/status') LOOP
                 IF line LIKE 'VmRSS:%' THEN
                     rss_kb := regexp_replace(line, '[^0-9]', '', 'g')::bigint;
                     RETURN rss_kb;
                 END IF;
             END LOOP;
             RETURN NULL;
         EXCEPTION WHEN OTHERS THEN
             RETURN NULL;
         END; $$",
    )
    .await
    .ok()?;

    let rss: Option<i64> = sqlx::query_scalar("SELECT pg_temp.get_rss_kb()")
        .fetch_one(&db.pool)
        .await
        .ok()?;

    rss
}

/// Evidence from a completed manual refresh at a quiescent checkpoint.
#[derive(Debug)]
struct RefreshWitness {
    action: String,
    affected_rows: i64,
}

/// Catch up a stream with a manual refresh and compare its *maintained* public
/// result against its defining query. The scheduler must already be paused and
/// drained. In particular, never switch to FULL or repair a mismatched result.
///
/// A manual refresh can return Ok when it was actually skipped. Insist on a
/// new, completed history record and reject implicit FULL fallbacks so this
/// checkpoint cannot certify correctness by rebuilding the stream table.
async fn verify_correctness(db: &E2eDb, st_name: &str) -> Result<RefreshWitness, String> {
    let before: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(h.refresh_id), 0)
         FROM pgtrickle.pgt_refresh_history h
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
         WHERE st.pgt_schema = 'public' AND st.pgt_name = $1",
    )
    .bind(st_name)
    .fetch_one(&db.pool)
    .await
    .map_err(|e| format!("refresh history lookup failed for {st_name}: {e}"))?;

    db.try_execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('public.{st_name}')"
    ))
    .await
    .map_err(|e| format!("refresh failed for {st_name}: {e}"))?;

    let record: Option<(String, String, bool, Option<String>, i64, i64, i64)> =
        sqlx::query_as(
            "SELECT h.action, h.status, h.was_full_fallback, h.merge_strategy_used,
                    COALESCE(h.rows_inserted, 0), COALESCE(h.rows_updated, 0),
                    COALESCE(h.rows_deleted, 0)
             FROM pgtrickle.pgt_refresh_history h
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
             WHERE st.pgt_schema = 'public' AND st.pgt_name = $1
               AND h.refresh_id > $2
             ORDER BY h.refresh_id DESC LIMIT 1",
        )
        .bind(st_name)
        .bind(before)
        .fetch_optional(&db.pool)
        .await
        .map_err(|e| format!("refresh witness lookup failed for {st_name}: {e}"))?;
    let (action, status, full_fallback, merge_strategy, inserted, updated, deleted) =
        record.ok_or_else(|| {
            format!("refresh for {st_name} returned without a new history record (skipped?)")
        })?;
    if status != "COMPLETED"
        || !matches!(action.as_str(), "DIFFERENTIAL" | "NO_DATA")
        || full_fallback
        || merge_strategy.as_deref() == Some("FULL")
    {
        return Err(format!(
            "unexpected refresh path for {st_name}: action={action}, status={status}, \
             was_full_fallback={full_fallback}, merge_strategy={merge_strategy:?}"
        ));
    }

    let defining_query: String = sqlx::query_scalar(
        "SELECT defining_query FROM pgtrickle.pgt_stream_tables
         WHERE pgt_schema = 'public' AND pgt_name = $1",
    )
    .bind(st_name)
    .fetch_one(&db.pool)
    .await
    .map_err(|e| format!("defining query lookup failed for {st_name}: {e}"))?;

    // Exact bag comparison, including duplicate rows, and concrete extra/missing
    // row samples on failure. All writes are quiesced for the entire comparison.
    oracle::compare_st_to_query(db, &format!("public.{st_name}"), &defining_query)
        .await
        .map_err(|e| format!("CORRECTNESS VIOLATION for {st_name}: {e}"))?;

    Ok(RefreshWitness {
        action,
        affected_rows: inserted + updated + deleted,
    })
}

/// Every correctness checkpoint uses the same drain/refresh/compare path.
/// Keep failures in the caller-owned accumulator: later checks, or an optional
/// manual recovery outside this harness, must never erase an earlier mismatch.
async fn run_correctness_checkpoint(
    db: &E2eDb,
    active_sts: &[&str],
    label: &str,
    failures: &mut Vec<String>,
    resume_after: bool,
) {
    let target_list = active_sts
        .iter()
        .map(|name| format!("'public.{name}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let pause = format!("SELECT pgtrickle.pause_scheduler(ARRAY[{target_list}])");
    let resume = format!("SELECT pgtrickle.resume_scheduler(ARRAY[{target_list}])");

    if let Err(e) = db.try_execute(&pause).await {
        let failure = format!("[{label}] scheduler drain failed for {active_sts:?}: {e}");
        println!("    FAIL — {failure}");
        failures.push(failure);
    } else {
        for st in active_sts {
            match verify_correctness(db, st).await {
                Ok(witness) => println!(
                    "    {st}: ✓ (action={}, affected_rows={})",
                    witness.action, witness.affected_rows
                ),
                Err(e) => {
                    let failure = format!("[{label}] {e}");
                    println!("    {st}: FAIL — {failure}");
                    failures.push(failure);
                }
            }
        }
    }

    if resume_after {
        if let Err(e) = db.try_execute(&resume).await {
            let failure = format!("[{label}] scheduler resume failed: {e}");
            println!("    FAIL — {failure}");
            failures.push(failure);
        }
    }
}

/// This is the final-path entry point used by the long soak and the bounded
/// regression: it runs even if the duration never reached the periodic timer.
async fn run_final_correctness(db: &E2eDb, active_sts: &[&str], failures: &mut Vec<String>) {
    println!("\n  Final correctness verification (DIFFERENTIAL, no rebuild)...");
    run_correctness_checkpoint(db, active_sts, "final", failures, false).await;
}

// ── Main Soak Test ─────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_soak_stability() {
    let duration = Duration::from_secs(soak_duration_secs());
    let cycle_delay = Duration::from_millis(cycle_delay_ms());
    let batch = batch_size();
    let n_sources = num_sources();
    assert!(
        (1..=5).contains(&n_sources),
        "SOAK_NUM_SOURCES must be between 1 and 5"
    );

    println!("\n══════════════════════════════════════════════════════════");
    println!("  G17-SOAK: Long-Running Stability Soak Test");
    println!(
        "  Duration: {}s, Cycle: {}ms, Batch: {}, Sources: {}",
        duration.as_secs(),
        cycle_delay.as_millis(),
        batch,
        n_sources,
    );
    println!("══════════════════════════════════════════════════════════\n");

    let db = E2eDb::new().await.with_extension().await;

    // ── Setup ──────────────────────────────────────────────────────────
    println!("  Setting up source tables and stream tables...");
    create_source_tables(&db, n_sources).await;
    create_stream_tables(&db, n_sources).await;

    // Initial refresh to populate all STs
    let st_names = vec![
        "soak_agg",
        "soak_join",
        "soak_union",
        "soak_left_agg",
        "soak_filter",
    ];
    let active_sts: Vec<&str> = st_names.into_iter().take(n_sources).collect();

    for st in &active_sts {
        db.execute(&format!("SELECT pgtrickle.refresh_stream_table('{st}')"))
            .await;
    }
    println!(
        "  Initial refresh complete for {} stream tables",
        active_sts.len()
    );

    // Record initial RSS
    let initial_rss = get_rss_kb(&db).await;
    if let Some(rss) = initial_rss {
        println!("  Initial RSS: {} KB", rss);
    }

    // ── Soak Loop ──────────────────────────────────────────────────────
    let start = Instant::now();
    let mut cycle = 0u64;
    let mut total_dml_ops = 0u64;
    let mut total_refreshes = 0u64;
    let mut health_check_failures: Vec<String> = Vec::new();
    let check_interval = Duration::from_secs(30);
    let mut last_check = Instant::now();
    let correctness_interval = Duration::from_secs(120);
    let mut last_correctness = Instant::now();

    println!("  Starting soak loop...\n");

    while start.elapsed() < duration {
        cycle += 1;

        // Pick a source table (round-robin)
        let source_idx = ((cycle as usize - 1) % n_sources) + 1;

        // Apply mixed DML
        apply_dml_batch(&db, source_idx, batch, cycle).await;
        total_dml_ops += 3; // INSERT + UPDATE + DELETE

        // Manual refresh on a rotating stream table.
        // Retry once on transient deadlock before recording a failure.
        let st_idx = (cycle as usize - 1) % active_sts.len();
        let st = active_sts[st_idx];
        let refresh_result = {
            let r = db
                .try_execute(&format!("SELECT pgtrickle.refresh_stream_table('{st}')"))
                .await;
            if r.is_err() {
                tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
                db.try_execute(&format!("SELECT pgtrickle.refresh_stream_table('{st}')"))
                    .await
            } else {
                r
            }
        };
        if let Err(e) = refresh_result {
            health_check_failures.push(format!("Cycle {cycle}: refresh failed for {st}: {e}"));
        } else {
            total_refreshes += 1;
        }

        // Periodic health checks (every 30s)
        if last_check.elapsed() >= check_interval {
            let elapsed = start.elapsed().as_secs();
            print!("  [{elapsed}s] cycle {cycle}: ");

            // Check error states
            if let Err(e) = check_no_error_states(&db).await {
                println!("FAIL — {e}");
                health_check_failures.push(format!("[{elapsed}s] {e}"));
            } else {
                print!("states OK, ");
            }

            // Check worker alive
            if let Err(e) = check_worker_alive(&db).await {
                println!("FAIL — {e}");
                health_check_failures.push(format!("[{elapsed}s] {e}"));
            } else {
                print!("worker OK");
            }

            // RSS check
            if let Some(rss) = get_rss_kb(&db).await {
                print!(", RSS={rss}KB");
                if let Some(initial) = initial_rss {
                    let growth_pct =
                        ((rss as f64 - initial as f64) / initial as f64 * 100.0).max(0.0);
                    print!(" (+{growth_pct:.1}%)");
                }
            }

            println!(", refreshes={total_refreshes}, dml_ops={total_dml_ops}");
            last_check = Instant::now();
        }

        // Periodic correctness checks (every 2 min), after a quiescent
        // scheduler boundary. JOIN violations have no special exemption.
        if last_correctness.elapsed() >= correctness_interval {
            let elapsed = start.elapsed().as_secs();
            println!("  [{elapsed}s] Running correctness check...");
            run_correctness_checkpoint(
                &db,
                &active_sts,
                &format!("{elapsed}s"),
                &mut health_check_failures,
                true,
            )
            .await;
            last_correctness = Instant::now();
        }

        tokio::time::sleep(cycle_delay).await;
    }

    // ── Final Checks ───────────────────────────────────────────────────
    let total_elapsed = start.elapsed();
    println!(
        "\n  Soak loop complete: {cycle} cycles in {:.1}s",
        total_elapsed.as_secs_f64()
    );
    println!("  Total DML operations: {total_dml_ops}");
    println!("  Total refreshes: {total_refreshes}");

    // Final correctness is unconditional, even for runs shorter than the
    // periodic interval. Drain the scheduler and compare before any repair.
    // Leave the scheduler paused so subsequent final health checks cannot
    // change the result we just certified.
    run_final_correctness(&db, &active_sts, &mut health_check_failures).await;

    // Final RSS check
    if let (Some(initial), Some(final_rss)) = (initial_rss, get_rss_kb(&db).await) {
        let growth_pct = ((final_rss as f64 - initial as f64) / initial as f64 * 100.0).max(0.0);
        println!("\n  RSS: initial={initial}KB, final={final_rss}KB, growth={growth_pct:.1}%");
        assert!(
            growth_pct < 20.0,
            "RSS grew by {growth_pct:.1}% (threshold: 20%): initial={initial}KB, final={final_rss}KB"
        );
    }

    // Final error state check
    if let Err(e) = check_no_error_states(&db).await {
        health_check_failures.push(format!("[final] {e}"));
    }
    if let Err(e) = check_worker_alive(&db).await {
        health_check_failures.push(format!("[final] {e}"));
    }

    // Report
    if !health_check_failures.is_empty() {
        println!("\n  ⚠ Health check failures during soak:");
        for f in &health_check_failures {
            println!("    - {f}");
        }
        panic!(
            "Soak test had {} health check failure(s)",
            health_check_failures.len()
        );
    }

    println!("\n  ✓ Soak test PASSED: {cycle} cycles, {total_refreshes} refreshes, zero failures");
}

// ── Bounded fail-closed correctness regressions (#1430) ────────────────

/// Small deterministic JOIN fixture. Pause/drain scheduler dispatch before
/// the first manual refresh; every later checkpoint has a stable source cut.
async fn setup_bounded_join(db: &E2eDb) {
    db.execute(
        "CREATE TABLE source_1 (id INT PRIMARY KEY, category INT NOT NULL, value NUMERIC(12,2))",
    )
    .await;
    db.execute("CREATE TABLE source_2 (id INT PRIMARY KEY, category INT NOT NULL, label TEXT)")
        .await;
    db.execute("INSERT INTO source_1 VALUES (1, 1, 10), (2, 2, 20)")
        .await;
    db.execute("INSERT INTO source_2 VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c')")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'soak_join',
            'SELECT s1.id, s1.category, s1.value, s2.label AS label_2
             FROM source_1 s1
             JOIN source_2 s2 ON s1.category = s2.category',
            schedule => '1s',
            refresh_mode => 'DIFFERENTIAL'
        )",
    )
    .await;
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['soak_join'])")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('soak_join')")
        .await;
}

/// Bypass only the stream-table DML guard in a test transaction, without
/// changing source data. A subsequent NO_DATA refresh must not hide drift.
async fn corrupt_one_public_join_row(db: &E2eDb) {
    let mut tx = db.pool.begin().await.expect("begin corruption transaction");
    sqlx::query("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .execute(&mut *tx)
        .await
        .expect("set test-only DML guard override");
    let changed = sqlx::query(
        "UPDATE public.soak_join SET value = value + 1
         WHERE ctid = (SELECT ctid FROM public.soak_join LIMIT 1)",
    )
    .execute(&mut *tx)
    .await
    .expect("inject one incorrect public JOIN row")
    .rows_affected();
    assert_eq!(changed, 1, "fixture must contain a JOIN row to corrupt");
    tx.commit().await.expect("commit injected JOIN drift");
}

#[tokio::test]
async fn test_soak_join_dml_completes_nonempty_differential_and_matches_exactly() {
    let db = E2eDb::new().await.with_extension().await;
    setup_bounded_join(&db).await;
    db.execute("INSERT INTO source_1 VALUES (3, 1, 30)")
        .await;
    let witness = verify_correctness(&db, "soak_join")
        .await
        .expect("healthy JOIN must match after DML");
    assert_eq!(witness.action, "DIFFERENTIAL");
    assert!(
        witness.affected_rows > 0,
        "a completed, nonempty differential refresh is required: {witness:?}"
    );
}

#[tokio::test]
async fn test_soak_periodic_checkpoint_reports_join_row_drift() {
    let db = E2eDb::new().await.with_extension().await;
    setup_bounded_join(&db).await;
    corrupt_one_public_join_row(&db).await;
    let mut failures = Vec::new();
    run_correctness_checkpoint(&db, &["soak_join"], "120s", &mut failures, true).await;
    assert_eq!(failures.len(), 1, "JOIN mismatch must fail periodic checks");
    assert!(failures[0].contains("CORRECTNESS VIOLATION for soak_join"));
    assert!(failures[0].contains("Extra rows in ST"));
    assert!(failures[0].contains("Missing rows from ST"));
}

#[tokio::test]
async fn test_soak_join_refresh_error_is_not_suppressed() {
    let db = E2eDb::new().await.with_extension().await;
    setup_bounded_join(&db).await;
    // An ordinary refresh error, not a row mismatch. This previously went
    // was previously ignored for JOINs by the periodic check.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables
         SET status = 'ERROR' WHERE pgt_name = 'soak_join'",
    )
    .await;
    let mut failures = Vec::new();
    run_correctness_checkpoint(&db, &["soak_join"], "120s", &mut failures, false).await;
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("refresh failed for soak_join"));
    assert!(failures[0].contains("error state"));
}

#[tokio::test]
async fn test_soak_short_final_checkpoint_keeps_failure_after_full_recovery() {
    let db = E2eDb::new().await.with_extension().await;
    setup_bounded_join(&db).await;
    // No 120-second periodic checkpoint has fired. Final verification is
    // mandatory even for a shorter soak, *before* any FULL repair.
    corrupt_one_public_join_row(&db).await;
    let mut failures = Vec::new();
    run_final_correctness(&db, &["soak_join"], &mut failures).await;
    assert_eq!(failures.len(), 1, "short soak must detect final JOIN drift");
    assert!(failures[0].contains("[final] CORRECTNESS VIOLATION for soak_join"));

    // Recovery may make the public rows equal, but cannot retroactively
    // erase the earlier failure from the original incremental state.
    db.execute(
        "SELECT pgtrickle.alter_stream_table('soak_join', refresh_mode => 'FULL')",
    )
    .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('soak_join')")
        .await;
    let defining_query: String = db
        .query_scalar(
            "SELECT defining_query FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'soak_join'",
        )
        .await;
    oracle::compare_st_to_query(&db, "public.soak_join", &defining_query)
        .await
        .expect("FULL recovery should restore exact equality");
    assert_eq!(failures.len(), 1, "recovery must not clear the failing verdict");
}
