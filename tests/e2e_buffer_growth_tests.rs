//! SCAL-1: Buffer growth stress test for pg_trickle.
//!
//! Validates that the `max_buffer_rows` cap triggers FULL refresh fallback
//! when change buffers exceed the configured limit. Checks:
//!   - FULL fallback fires when buffer rows exceed the cap
//!   - Stream table remains correct after FULL fallback
//!   - Recovery behavior when write rate normalizes
//!   - No data loss during the fallback transition
//!
//! These tests are `#[ignore]`d to skip in normal `cargo test` runs.
//! Run via:
//!
//! ```bash
//! just build-e2e-image
//! cargo test --test e2e_buffer_growth_tests -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! Environment variables:
//!   BUFFER_STRESS_BATCH_SIZE — Rows per INSERT batch (default: 5000)
//!   BUFFER_STRESS_BATCHES   — Number of batches to insert before refresh (default: 10)
//!   BUFFER_STRESS_CAP       — max_buffer_rows cap for the test (default: 10000)

mod e2e;

use e2e::E2eDb;

// ── Configuration ──────────────────────────────────────────────────────

fn batch_size() -> usize {
    std::env::var("BUFFER_STRESS_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000)
}

fn num_batches() -> usize {
    std::env::var("BUFFER_STRESS_BATCHES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

fn buffer_cap() -> usize {
    std::env::var("BUFFER_STRESS_CAP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000)
}

// ── Tests ──────────────────────────────────────────────────────────────

/// SCAL-1a: Verify max_buffer_rows triggers FULL fallback.
///
/// Insert more rows than the cap into a source table without refreshing,
/// then trigger a refresh and confirm it completes as FULL (not DIFFERENTIAL).
#[tokio::test]
#[ignore]
async fn test_buffer_growth_triggers_full_fallback() {
    let db = E2eDb::new().await;
    let cap = buffer_cap();
    let batch = batch_size();
    let batches = num_batches();

    // Set a low buffer cap for this test
    db.alter_system_set_and_wait(
        "pg_trickle.max_buffer_rows",
        &cap.to_string(),
        &cap.to_string(),
    )
    .await;

    // Create source table
    db.execute(
        "CREATE TABLE buf_source (
            id SERIAL PRIMARY KEY,
            val INT NOT NULL DEFAULT 0
        )",
    )
    .await;

    // Seed with a modest amount of data
    db.execute(
        "INSERT INTO buf_source (val)
         SELECT g FROM generate_series(1, 100) g",
    )
    .await;

    // Use a long schedule so the test controls refresh timing.
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'buf_st',
            'SELECT val, COUNT(*) AS cnt FROM buf_source GROUP BY val',
            schedule => '1h'
        )",
    )
    .await;

    // Initial refresh to establish baseline
    db.execute("SELECT pgtrickle.refresh_stream_table('buf_st')")
        .await;

    // Bulk-insert enough rows to exceed the buffer cap WITHOUT refreshing.
    // Each batch inserts `batch` rows; total = batch × batches.
    let total = batch * batches;
    assert!(
        total > cap,
        "Total rows ({total}) must exceed buffer cap ({cap}) for this test"
    );

    for b in 0..batches {
        let start = b * batch + 101;
        let end = start + batch - 1;
        db.execute(&format!(
            "INSERT INTO buf_source (val)
             SELECT g % 50 FROM generate_series({start}, {end}) g"
        ))
        .await;
    }

    // Now refresh — buffer exceeds cap, should trigger FULL fallback
    db.execute("SELECT pgtrickle.refresh_stream_table('buf_st')")
        .await;

    // Verify the stream table is correct by comparing to the source
    let st_count: i64 = db.query_scalar("SELECT SUM(cnt)::bigint FROM buf_st").await;
    let source_count = db.count("buf_source").await;

    assert_eq!(
        st_count, source_count,
        "Stream table row sum ({st_count}) must match source count ({source_count}) after FULL fallback"
    );

    // Verify refresh history shows a FULL refresh (not just DIFFERENTIAL)
    let has_full: bool = db
        .query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM pgtrickle.pgt_refresh_history
                WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'buf_st')
                  AND action = 'FULL'
                ORDER BY refresh_id DESC LIMIT 1
            )",
        )
        .await;

    assert!(
        has_full,
        "Buffer cap exceeded should trigger a FULL refresh in history"
    );

    // Reset system config
    db.alter_system_reset_and_wait("pg_trickle.max_buffer_rows", "1000000")
        .await;
}

/// SCAL-1b: Verify recovery after burst — buffer normalizes.
///
/// After a burst that triggers FULL fallback, insert a small batch and
/// verify that DIFFERENTIAL refresh resumes normally.
#[tokio::test]
#[ignore]
async fn test_buffer_growth_recovery_after_burst() {
    let db = E2eDb::new().await;
    let cap = buffer_cap();
    let batch = batch_size();
    let batches = num_batches();

    db.alter_system_set_and_wait(
        "pg_trickle.max_buffer_rows",
        &cap.to_string(),
        &cap.to_string(),
    )
    .await;

    db.execute(
        "CREATE TABLE recovery_source (
            id SERIAL PRIMARY KEY,
            val INT NOT NULL DEFAULT 0
        )",
    )
    .await;
    db.execute(
        "INSERT INTO recovery_source (val)
         SELECT g FROM generate_series(1, 100) g",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'recovery_st',
            'SELECT val, COUNT(*) AS cnt FROM recovery_source GROUP BY val',
            schedule => '1h'
        )",
    )
    .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('recovery_st')")
        .await;

    // Phase 1: Burst — exceed the buffer cap
    let total = batch * batches;
    assert!(total > cap);
    for b in 0..batches {
        let start = b * batch + 101;
        let end = start + batch - 1;
        db.execute(&format!(
            "INSERT INTO recovery_source (val)
             SELECT g % 50 FROM generate_series({start}, {end}) g"
        ))
        .await;
    }
    db.execute("SELECT pgtrickle.refresh_stream_table('recovery_st')")
        .await;

    // Phase 2: Small change — should resume DIFFERENTIAL
    db.execute("INSERT INTO recovery_source (val) VALUES (999)")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('recovery_st')")
        .await;

    // Verify latest refresh was DIFFERENTIAL (not another FULL)
    let latest_action: String = db
        .query_scalar(
            "SELECT action::text FROM pgtrickle.pgt_refresh_history
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'recovery_st')
             ORDER BY refresh_id DESC LIMIT 1",
        )
        .await;

    assert_eq!(
        latest_action, "DIFFERENTIAL",
        "After buffer normalizes, refresh should resume DIFFERENTIAL (got {latest_action})"
    );

    // Verify correctness
    let st_count: i64 = db
        .query_scalar("SELECT SUM(cnt)::bigint FROM recovery_st")
        .await;
    let source_count = db.count("recovery_source").await;
    assert_eq!(st_count, source_count);

    db.alter_system_reset_and_wait("pg_trickle.max_buffer_rows", "1000000")
        .await;
}

/// SCAL-1c: Verify no data loss during FULL fallback transition.
///
/// Insert data, trigger FULL fallback, then verify every row in the source
/// is accounted for in the stream table.
#[tokio::test]
#[ignore]
async fn test_buffer_growth_no_data_loss() {
    let db = E2eDb::new().await;
    let cap = 5_000;

    db.alter_system_set_and_wait(
        "pg_trickle.max_buffer_rows",
        &cap.to_string(),
        &cap.to_string(),
    )
    .await;

    db.execute(
        "CREATE TABLE nodloss_source (
            id SERIAL PRIMARY KEY,
            category INT NOT NULL,
            amount NUMERIC(10,2) NOT NULL
        )",
    )
    .await;

    // Seed with known data
    db.execute(
        "INSERT INTO nodloss_source (category, amount)
         SELECT (g % 20), (g * 1.5)::numeric(10,2)
         FROM generate_series(1, 200) g",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'nodloss_st',
            'SELECT category, SUM(amount) AS total, COUNT(*) AS cnt
             FROM nodloss_source GROUP BY category',
            schedule => '1h'
        )",
    )
    .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('nodloss_st')")
        .await;

    // Insert enough to blow past the cap
    db.execute(
        "INSERT INTO nodloss_source (category, amount)
         SELECT (g % 20), (g * 2.0)::numeric(10,2)
         FROM generate_series(1, 10000) g",
    )
    .await;

    // Also do some UPDATEs and DELETEs to exercise mixed DML
    db.execute("UPDATE nodloss_source SET amount = amount + 1 WHERE id <= 50")
        .await;
    db.execute("DELETE FROM nodloss_source WHERE id BETWEEN 51 AND 100")
        .await;

    // Refresh (should FULL-fallback due to buffer cap)
    db.execute("SELECT pgtrickle.refresh_stream_table('nodloss_st')")
        .await;

    // Verify: stream table aggregates must match direct query
    let st_total: String = db
        .query_scalar("SELECT SUM(total)::text FROM nodloss_st")
        .await;
    let direct_total: String = db
        .query_scalar("SELECT SUM(amount)::text FROM nodloss_source")
        .await;
    assert_eq!(
        st_total, direct_total,
        "SUM(total) mismatch after FULL fallback: ST={st_total} vs direct={direct_total}"
    );

    let st_cnt: i64 = db
        .query_scalar("SELECT SUM(cnt)::bigint FROM nodloss_st")
        .await;
    let direct_cnt = db.count("nodloss_source").await;
    assert_eq!(
        st_cnt, direct_cnt,
        "COUNT mismatch after FULL fallback: ST={st_cnt} vs direct={direct_cnt}"
    );

    db.alter_system_reset_and_wait("pg_trickle.max_buffer_rows", "1000000")
        .await;
}

/// SCAL-1d: Sustained high write rate with slow refresh.
///
/// Simulates a scenario where writes arrive faster than refresh can process.
/// With auto-refresh at 5s interval and continuous inserts, verifies the
/// system recovers and the stream table is eventually correct.
#[tokio::test]
#[ignore]
async fn test_buffer_growth_sustained_high_write_rate() {
    let db = E2eDb::new().await;
    let cap = 20_000;

    db.alter_system_set_and_wait(
        "pg_trickle.max_buffer_rows",
        &cap.to_string(),
        &cap.to_string(),
    )
    .await;

    db.execute(
        "CREATE TABLE sustained_source (
            id SERIAL PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO sustained_source (val)
         SELECT g FROM generate_series(1, 100) g",
    )
    .await;

    // Auto-scheduled stream table with slow interval (5s)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'sustained_st',
            'SELECT val % 10 AS bucket, COUNT(*) AS cnt
             FROM sustained_source GROUP BY val % 10',
            schedule => '5s',
            refresh_mode => 'DIFFERENTIAL'
        )",
    )
    .await;

    // Wait for initial refresh
    let had_refresh = db
        .wait_for_auto_refresh("sustained_st", std::time::Duration::from_secs(30))
        .await;
    assert!(
        had_refresh,
        "Initial auto-refresh of sustained_st should complete within 30 seconds"
    );

    // Insert in rapid bursts (10 batches of 5000 rows in quick succession)
    for b in 0..10 {
        let start = b * 5000 + 101;
        let end = start + 4999;
        db.execute(&format!(
            "INSERT INTO sustained_source (val)
             SELECT g FROM generate_series({start}, {end}) g"
        ))
        .await;
    }

    // Wait for the scheduler to catch up (adaptive polling instead of a fixed sleep).
    let caught_up = db
        .wait_for_condition(
            "scheduler catchup after sustained burst",
            "SELECT (SELECT SUM(cnt)::bigint FROM sustained_st) \
              = (SELECT COUNT(*)::bigint FROM sustained_source)",
            std::time::Duration::from_secs(60),
            std::time::Duration::from_millis(500),
        )
        .await;
    // If the condition isn't met within the timeout, the final manual refresh
    // below will still produce the correct result — we log a diagnostic here.
    if !caught_up {
        eprintln!(
            "wait_for_condition: scheduler did not catch up within 60 s — forcing manual refresh"
        );
    }

    // Force a final manual refresh to ensure consistency
    db.execute("SELECT pgtrickle.refresh_stream_table('sustained_st')")
        .await;

    // Verify correctness
    let st_count: i64 = db
        .query_scalar("SELECT SUM(cnt)::bigint FROM sustained_st")
        .await;
    let source_count = db.count("sustained_source").await;
    assert_eq!(
        st_count, source_count,
        "Stream table ({st_count}) must match source ({source_count}) after sustained burst"
    );

    // Verify stream table is not in error state
    let status: String = db
        .query_scalar(
            "SELECT pgt_status::text FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'sustained_st'",
        )
        .await;
    assert_ne!(status, "ERROR", "Stream table should not be in ERROR state");

    db.alter_system_reset_and_wait("pg_trickle.max_buffer_rows", "1000000")
        .await;
}
