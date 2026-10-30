//! C-1d: Full E2E tests for tiered scheduling — scheduler-dependent behaviour.
//!
//! These tests require the background scheduler (`shared_preload_libraries`)
//! and can only run in the full E2E harness (just test-e2e).
//! They are NOT in the light-E2E allowlist.

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helpers ────────────────────────────────────────────────────────────────

async fn configure_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.tiered_scheduling = on")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.tiered_scheduling", "on")
        .await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(
        sched_running,
        "pg_trickle scheduler did not appear within 90 s"
    );
}

/// Wait until a ST has at least `min_count` COMPLETED refresh history records.
async fn wait_for_n_refreshes(
    db: &E2eDb,
    pgt_name: &str,
    min_count: i64,
    timeout: Duration,
) -> i64 {
    let start = std::time::Instant::now();
    loop {
        let count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
                 WHERE d.pgt_name = '{pgt_name}' AND h.status = 'COMPLETED'"
            ))
            .await;
        if count >= min_count {
            return count;
        }
        if start.elapsed() > timeout {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// C-1d: Frozen ST never refreshes while frozen, but resumes after promotion.
#[tokio::test]
async fn test_tier_frozen_skips_refresh_until_promoted() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE tier_sched_src1 (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO tier_sched_src1 VALUES (1, 10)")
        .await;
    db.create_st(
        "tier_sched_st1",
        "SELECT id, val FROM tier_sched_src1",
        "1s",
        "FULL",
    )
    .await;

    // Wait for initial population (the create does an initial refresh)
    assert_eq!(db.count("public.tier_sched_st1").await, 1);

    // Freeze the ST
    db.alter_st("tier_sched_st1", "tier => 'frozen'").await;

    // Record current refresh count
    let baseline: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'tier_sched_st1' AND h.status = 'COMPLETED'",
        )
        .await;

    // Insert new data and wait — frozen ST should NOT refresh
    db.execute("INSERT INTO tier_sched_src1 VALUES (2, 20)")
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let after_freeze: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'tier_sched_st1' AND h.status = 'COMPLETED'",
        )
        .await;

    assert_eq!(
        after_freeze, baseline,
        "Frozen ST should not receive any new refreshes (baseline={baseline}, after={after_freeze})"
    );

    // ST data should still be stale
    assert_eq!(
        db.count("public.tier_sched_st1").await,
        1,
        "Frozen ST data should be stale (still 1 row)"
    );

    // Promote back to hot
    // Capture the frontier first so a fast scheduler tick cannot complete
    // between promotion and the wait helper's baseline read.
    let before_promotion_ts: Option<String> = db
        .query_scalar_opt(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'tier_sched_st1'",
        )
        .await;
    db.alter_st("tier_sched_st1", "tier => 'hot'").await;

    // Now it should refresh and pick up the new row
    let refreshed = db
        .wait_for_auto_refresh_since(
            "tier_sched_st1",
            before_promotion_ts,
            Duration::from_secs(30),
        )
        .await;
    assert!(refreshed, "Promoted ST should resume refreshing");

    let final_count = db.count("public.tier_sched_st1").await;
    assert_eq!(final_count, 2, "After promotion, ST should have both rows");
}

/// C-1d: Cold ST refreshes less frequently than Hot ST.
///
/// With a 1s schedule:
///   - Hot: refreshes every ~1s (1× multiplier)
///   - Cold: refreshes every ~10s (10× multiplier)
///
/// Over 15 seconds, Hot should have significantly more refreshes than Cold.
#[tokio::test]
async fn test_tier_cold_refreshes_less_than_hot() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Create two source tables with steady DML
    db.execute("CREATE TABLE tier_hot_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO tier_hot_src VALUES (1, 1)").await;
    db.create_st(
        "tier_hot_st",
        "SELECT id, val FROM tier_hot_src",
        "1s",
        "FULL",
    )
    .await;

    db.execute("CREATE TABLE tier_cold_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO tier_cold_src VALUES (1, 1)").await;
    db.create_st(
        "tier_cold_st",
        "SELECT id, val FROM tier_cold_src",
        "1s",
        "FULL",
    )
    .await;

    // Set tier for cold
    db.alter_st("tier_cold_st", "tier => 'cold'").await;

    // Wait for initial refresh of both
    let _ = wait_for_n_refreshes(&db, "tier_hot_st", 1, Duration::from_secs(30)).await;
    let _ = wait_for_n_refreshes(&db, "tier_cold_st", 1, Duration::from_secs(30)).await;

    // Record baselines
    let hot_baseline: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'tier_hot_st' AND h.status = 'COMPLETED'",
        )
        .await;
    let cold_baseline: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'tier_cold_st' AND h.status = 'COMPLETED'",
        )
        .await;

    // Keep inserting data to trigger staleness over 15 seconds
    for i in 2..=15 {
        db.execute(&format!("INSERT INTO tier_hot_src VALUES ({i}, {i})"))
            .await;
        db.execute(&format!("INSERT INTO tier_cold_src VALUES ({i}, {i})"))
            .await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let hot_total = wait_for_n_refreshes(
        &db,
        "tier_hot_st",
        hot_baseline + 3,
        Duration::from_secs(10),
    )
    .await;
    let hot_count = hot_total - hot_baseline;
    let cold_total: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'tier_cold_st' AND h.status = 'COMPLETED'",
        )
        .await;
    let cold_count = cold_total - cold_baseline;

    assert!(
        hot_count > cold_count,
        "Hot ST should have more refreshes than Cold ST \
         (hot={hot_count}, cold={cold_count}) over the test window"
    );
}
