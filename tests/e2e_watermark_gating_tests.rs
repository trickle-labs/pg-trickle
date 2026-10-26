//! E2E tests for Watermark Gating (v0.7.0, WM-6).
//!
//! Validates:
//! - WM-2: `advance_watermark()` monotonicity, idempotency, and transactional semantics
//! - WM-3: `create_watermark_group()` / `drop_watermark_group()` CRUD
//! - WM-4: Scheduler skips misaligned stream tables, allows aligned ones
//! - WM-5: `watermarks()`, `watermark_groups()`, `watermark_status()` introspection
//! - Edge cases: tolerance, multiple groups, mixed external+internal sources
//!
//! Prerequisites: `just build-e2e-image` (for scheduler tests)

mod e2e;

use e2e::E2eDb;
#[cfg(not(feature = "light-e2e"))]
use std::time::Duration;

// ── advance_watermark() tests (light E2E) ──────────────────────────────────

/// WM-2: advance_watermark() stores the watermark for a source.
#[tokio::test]
async fn test_advance_watermark_stores_value() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE wm_src (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.advance_watermark('wm_src', '2026-03-01 12:00:00+00')")
        .await;

    let wm: String = db
        .query_scalar(
            "SELECT watermark::text FROM pgtrickle.watermarks() \
             WHERE source_table = 'wm_src'",
        )
        .await;

    assert!(
        wm.contains("2026-03-01"),
        "watermark should contain the date, got: {}",
        wm
    );
}

/// WM-2: advance_watermark() rejects backward watermarks.
#[tokio::test]
async fn test_advance_watermark_monotonic() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mono_src (id INT PRIMARY KEY)")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('mono_src', '2026-03-01 12:00:00+00')")
        .await;

    let result = db
        .try_execute("SELECT pgtrickle.advance_watermark('mono_src', '2026-03-01 11:00:00+00')")
        .await;

    assert!(result.is_err(), "advancing watermark backward should fail");
}

/// WM-2: advance_watermark() with the same value is an idempotent no-op.
#[tokio::test]
async fn test_advance_watermark_idempotent() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE idem_wm (id INT PRIMARY KEY)")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('idem_wm', '2026-03-01 12:00:00+00')")
        .await;
    // Same value again — should not error.
    db.execute("SELECT pgtrickle.advance_watermark('idem_wm', '2026-03-01 12:00:00+00')")
        .await;

    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.watermarks() \
             WHERE source_table = 'idem_wm'",
        )
        .await;

    assert_eq!(count, 1, "idempotent advance should not create duplicates");
}

/// WM-2: advance_watermark() stores the WAL LSN alongside the watermark.
#[tokio::test]
async fn test_advance_watermark_stores_wal_lsn() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lsn_wm (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.advance_watermark('lsn_wm', '2026-03-01 12:00:00+00')")
        .await;

    let has_lsn: bool = db
        .query_scalar(
            "SELECT wal_lsn IS NOT NULL FROM pgtrickle.watermarks() \
             WHERE source_table = 'lsn_wm'",
        )
        .await;

    assert!(has_lsn, "advance_watermark should record the WAL LSN");
}

/// WM-2: advance_watermark() on non-existent table returns an error.
#[tokio::test]
async fn test_advance_watermark_nonexistent_table_errors() {
    let db = E2eDb::new().await.with_extension().await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.advance_watermark('no_such_table', '2026-03-01 12:00:00+00')",
        )
        .await;

    assert!(
        result.is_err(),
        "advancing watermark on non-existent table should fail"
    );
}

/// WM-2: advance_watermark() records the current_user as advanced_by.
#[tokio::test]
async fn test_advance_watermark_records_user() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE usr_wm (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.advance_watermark('usr_wm', '2026-03-01 12:00:00+00')")
        .await;

    let has_user: bool = db
        .query_scalar(
            "SELECT advanced_by IS NOT NULL FROM pgtrickle.watermarks() \
             WHERE source_table = 'usr_wm'",
        )
        .await;

    assert!(has_user, "advance_watermark should record advanced_by");
}

// ── create_watermark_group() / drop_watermark_group() tests ────────────────

/// WM-3: create_watermark_group() stores a group definition.
#[tokio::test]
async fn test_create_watermark_group() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE grp_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE grp_b (id INT PRIMARY KEY)").await;

    db.execute("SELECT pgtrickle.create_watermark_group('test_grp', ARRAY['grp_a', 'grp_b'], 0)")
        .await;

    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.watermark_groups() \
             WHERE group_name = 'test_grp'",
        )
        .await;

    assert_eq!(count, 1, "watermark group should be created");
}

/// WM-3: create_watermark_group() returns the group_id.
#[tokio::test]
async fn test_create_watermark_group_returns_id() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE gid_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE gid_b (id INT PRIMARY KEY)").await;

    let group_id: i32 = db
        .query_scalar(
            "SELECT pgtrickle.create_watermark_group('gid_grp', ARRAY['gid_a', 'gid_b'], 0)",
        )
        .await;

    assert!(
        group_id > 0,
        "create_watermark_group should return a positive group_id"
    );
}

/// WM-3: create_watermark_group() rejects fewer than 2 sources.
#[tokio::test]
async fn test_create_watermark_group_requires_two_sources() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE solo_src (id INT PRIMARY KEY)")
        .await;

    let result = db
        .try_execute("SELECT pgtrickle.create_watermark_group('solo_grp', ARRAY['solo_src'], 0)")
        .await;

    assert!(
        result.is_err(),
        "watermark group with <2 sources should fail"
    );
}

/// WM-3: create_watermark_group() rejects negative tolerance.
#[tokio::test]
async fn test_create_watermark_group_rejects_negative_tolerance() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE neg_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE neg_b (id INT PRIMARY KEY)").await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_watermark_group('neg_grp', ARRAY['neg_a', 'neg_b'], -5)",
        )
        .await;

    assert!(result.is_err(), "negative tolerance should be rejected");
}

/// WM-3: create_watermark_group() rejects duplicate group name.
#[tokio::test]
async fn test_create_watermark_group_rejects_duplicate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE dup_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE dup_b (id INT PRIMARY KEY)").await;

    db.execute("SELECT pgtrickle.create_watermark_group('dup_grp', ARRAY['dup_a', 'dup_b'], 0)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_watermark_group('dup_grp', ARRAY['dup_a', 'dup_b'], 0)",
        )
        .await;

    assert!(
        result.is_err(),
        "duplicate watermark group name should fail"
    );
}

/// WM-3: drop_watermark_group() removes the group.
#[tokio::test]
async fn test_drop_watermark_group() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE drp_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE drp_b (id INT PRIMARY KEY)").await;

    db.execute("SELECT pgtrickle.create_watermark_group('drp_grp', ARRAY['drp_a', 'drp_b'], 0)")
        .await;

    db.execute("SELECT pgtrickle.drop_watermark_group('drp_grp')")
        .await;

    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.watermark_groups() \
             WHERE group_name = 'drp_grp'",
        )
        .await;

    assert_eq!(count, 0, "dropped watermark group should not be visible");
}

/// WM-3: drop_watermark_group() on non-existent group returns an error.
#[tokio::test]
async fn test_drop_watermark_group_nonexistent_errors() {
    let db = E2eDb::new().await.with_extension().await;

    let result = db
        .try_execute("SELECT pgtrickle.drop_watermark_group('no_such_group')")
        .await;

    assert!(
        result.is_err(),
        "dropping non-existent watermark group should fail"
    );
}

/// WM-3: create_watermark_group() stores the tolerance value.
#[tokio::test]
async fn test_create_watermark_group_with_tolerance() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE tol_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE tol_b (id INT PRIMARY KEY)").await;

    db.execute("SELECT pgtrickle.create_watermark_group('tol_grp', ARRAY['tol_a', 'tol_b'], 30)")
        .await;

    let tolerance: f64 = db
        .query_scalar(
            "SELECT tolerance_secs FROM pgtrickle.watermark_groups() \
             WHERE group_name = 'tol_grp'",
        )
        .await;

    assert!(
        (tolerance - 30.0).abs() < 0.01,
        "tolerance should be stored as 30.0, got: {}",
        tolerance
    );
}

// ── Introspection function tests (light E2E) ──────────────────────────────

/// WM-5: watermarks() returns empty set initially.
#[tokio::test]
async fn test_watermarks_empty_by_default() {
    let db = E2eDb::new().await.with_extension().await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.watermarks()")
        .await;

    assert_eq!(count, 0, "watermarks() should be empty initially");
}

/// WM-5: watermark_groups() returns empty set initially.
#[tokio::test]
async fn test_watermark_groups_empty_by_default() {
    let db = E2eDb::new().await.with_extension().await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.watermark_groups()")
        .await;

    assert_eq!(count, 0, "watermark_groups() should be empty initially");
}

/// WM-5: watermark_status() returns one row per group with alignment info.
#[tokio::test]
async fn test_watermark_status_shows_alignment() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ws_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE ws_b (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.create_watermark_group('ws_grp', ARRAY['ws_a', 'ws_b'], 0)")
        .await;

    // Before any watermarks are advanced, group should be trivially aligned.
    let aligned: bool = db
        .query_scalar(
            "SELECT aligned FROM pgtrickle.watermark_status() \
             WHERE group_name = 'ws_grp'",
        )
        .await;

    assert!(
        aligned,
        "group with no watermarks should be trivially aligned"
    );
}

/// WM-5: watermark_status() shows misalignment when watermarks diverge.
#[tokio::test]
async fn test_watermark_status_shows_misalignment() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mis_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE mis_b (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.create_watermark_group('mis_grp', ARRAY['mis_a', 'mis_b'], 0)")
        .await;

    // Advance watermarks to different times (10 min apart).
    db.execute("SELECT pgtrickle.advance_watermark('mis_a', '2026-03-01 12:05:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('mis_b', '2026-03-01 11:55:00+00')")
        .await;

    let aligned: bool = db
        .query_scalar(
            "SELECT aligned FROM pgtrickle.watermark_status() \
             WHERE group_name = 'mis_grp'",
        )
        .await;

    assert!(
        !aligned,
        "group with 10-min lag and 0 tolerance should be misaligned"
    );

    let lag: f64 = db
        .query_scalar(
            "SELECT lag_secs FROM pgtrickle.watermark_status() \
             WHERE group_name = 'mis_grp'",
        )
        .await;

    assert!(
        (lag - 600.0).abs() < 1.0,
        "lag should be ~600s (10 min), got: {}",
        lag
    );
}

/// WM-5: watermark_status() shows alignment when within tolerance.
#[tokio::test]
async fn test_watermark_status_alignment_with_tolerance() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE t_b (id INT PRIMARY KEY)").await;
    // 60-second tolerance, watermarks 30 seconds apart → aligned.
    db.execute("SELECT pgtrickle.create_watermark_group('t_grp', ARRAY['t_a', 't_b'], 60)")
        .await;

    db.execute("SELECT pgtrickle.advance_watermark('t_a', '2026-03-01 12:00:30+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('t_b', '2026-03-01 12:00:00+00')")
        .await;

    let aligned: bool = db
        .query_scalar(
            "SELECT aligned FROM pgtrickle.watermark_status() \
             WHERE group_name = 't_grp'",
        )
        .await;

    assert!(aligned, "30s lag within 60s tolerance should be aligned");
}

/// WM-5: watermark_status() tracks sources_with_watermark and sources_total.
#[tokio::test]
async fn test_watermark_status_source_counts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sc_a (id INT PRIMARY KEY)").await;
    db.execute("CREATE TABLE sc_b (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.create_watermark_group('sc_grp', ARRAY['sc_a', 'sc_b'], 0)")
        .await;

    // Only advance one source.
    db.execute("SELECT pgtrickle.advance_watermark('sc_a', '2026-03-01 12:00:00+00')")
        .await;

    let total: i32 = db
        .query_scalar(
            "SELECT sources_total FROM pgtrickle.watermark_status() \
             WHERE group_name = 'sc_grp'",
        )
        .await;
    let with_wm: i32 = db
        .query_scalar(
            "SELECT sources_with_watermark FROM pgtrickle.watermark_status() \
             WHERE group_name = 'sc_grp'",
        )
        .await;

    assert_eq!(total, 2, "sources_total should be 2");
    assert_eq!(with_wm, 1, "sources_with_watermark should be 1");
}

// ── Watermark forward-advancement tests ────────────────────────────────────

/// WM-2: advance_watermark() allows forward advancement.
#[tokio::test]
async fn test_advance_watermark_forward() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE fwd_wm (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.advance_watermark('fwd_wm', '2026-03-01 12:00:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('fwd_wm', '2026-03-01 13:00:00+00')")
        .await;

    let wm: String = db
        .query_scalar(
            "SELECT watermark::text FROM pgtrickle.watermarks() \
             WHERE source_table = 'fwd_wm'",
        )
        .await;

    assert!(
        wm.contains("13:00:00"),
        "watermark should have advanced to 13:00, got: {}",
        wm
    );
}

// ── Watermark gating integration with stream tables (light E2E) ────────────

/// WM-4: Stream table with all sources aligned refreshes normally.
#[tokio::test]
async fn test_watermark_aligned_st_refreshes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE al_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE al_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO al_a VALUES (1, 'a')").await;
    db.execute("INSERT INTO al_b VALUES (1, 'b')").await;

    db.create_st(
        "al_report",
        "SELECT a.id, a.val AS a_val, b.val AS b_val FROM al_a a JOIN al_b b ON a.id = b.id",
        "5m",
        "FULL",
    )
    .await;

    // Create group and align both sources.
    db.execute("SELECT pgtrickle.create_watermark_group('al_grp', ARRAY['al_a', 'al_b'], 0)")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('al_a', '2026-03-01 12:00:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('al_b', '2026-03-01 12:00:00+00')")
        .await;

    // Manual refresh should succeed since sources are aligned.
    db.refresh_st("al_report").await;

    let count: i64 = db.count("public.al_report").await;
    assert_eq!(count, 1, "aligned ST should refresh successfully");
}

/// WM-4: Manual refresh always works regardless of watermark state.
///
/// This mirrors the bootstrap gating behavior: manual refreshes are not
/// blocked by watermark gating (only the scheduler is).
#[tokio::test]
async fn test_manual_refresh_not_blocked_by_watermark() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE man_wm_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE man_wm_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO man_wm_a VALUES (1, 'hello')").await;
    db.execute("INSERT INTO man_wm_b VALUES (1, 'world')").await;

    db.create_st(
        "man_wm_st",
        "SELECT a.id, a.val AS a_val, b.val AS b_val FROM man_wm_a a JOIN man_wm_b b ON a.id = b.id",
        "5m",
        "FULL",
    )
    .await;

    // Create group with misaligned watermarks (10 min apart, 0 tolerance).
    db.execute(
        "SELECT pgtrickle.create_watermark_group('man_wm_grp', ARRAY['man_wm_a', 'man_wm_b'], 0)",
    )
    .await;
    db.execute("SELECT pgtrickle.advance_watermark('man_wm_a', '2026-03-01 12:05:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('man_wm_b', '2026-03-01 11:55:00+00')")
        .await;

    // Manual refresh should still work (gating only affects scheduler).
    db.refresh_st("man_wm_st").await;

    let count: i64 = db.count("public.man_wm_st").await;
    assert_eq!(
        count, 1,
        "manual refresh must succeed even when watermarks are misaligned"
    );
}

/// WM-4: Stream table without watermark groups refreshes freely.
#[tokio::test]
async fn test_st_without_watermark_group_refreshes_freely() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE free_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO free_src VALUES (1, 'ok')").await;

    db.create_st("free_st", "SELECT id, val FROM free_src", "5m", "FULL")
        .await;

    // No watermark group — refresh should work normally.
    db.refresh_st("free_st").await;

    let count: i64 = db.count("public.free_st").await;
    assert_eq!(count, 1, "ST without watermark group should refresh freely");
}

/// WM-4: Intermediate ST (one source in group) refreshes freely.
///
/// A stream table that only depends on one source in a 2-source group has
/// fewer than 2 overlapping sources → the group is irrelevant → no gating.
#[tokio::test]
async fn test_watermark_intermediate_refreshes_freely() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE int_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE int_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO int_a VALUES (1, 'data')").await;

    // This ST only depends on int_a, not int_b.
    db.create_st("int_summary", "SELECT id, val FROM int_a", "5m", "FULL")
        .await;

    // Create a group with both sources, but misaligned.
    db.execute("SELECT pgtrickle.create_watermark_group('int_grp', ARRAY['int_a', 'int_b'], 0)")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('int_a', '2026-03-01 12:05:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('int_b', '2026-03-01 11:55:00+00')")
        .await;

    // int_summary only touches int_a → group overlap is <2 → no gating.
    db.refresh_st("int_summary").await;

    let count: i64 = db.count("public.int_summary").await;
    assert_eq!(
        count, 1,
        "single-source ST should not be gated by a multi-source group"
    );
}

// ── Scheduler SKIP tests (full E2E — requires bgworker) ───────────────────

/// WM-4: Scheduler logs SKIP when watermarks are misaligned.
///
/// Procedure:
/// 1. Configure fast scheduler.
/// 2. Create two sources + a stream table joining them.
/// 3. Create a watermark group with strict tolerance.
/// 4. Advance watermarks to misaligned timestamps.
/// 5. Wait for scheduler to log SKIPPED in pgt_refresh_history.
#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_scheduler_skips_misaligned_watermark() {
    let db = E2eDb::new_on_postgres_db().await;

    db.execute("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
        .await;

    // Configure fast scheduler.
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

    let sched_ok = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(sched_ok, "pg_trickle scheduler must be running");

    // Create sources and stream table.
    db.execute("CREATE TABLE sched_wm_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE sched_wm_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sched_wm_a VALUES (1, 'a')").await;
    db.execute("INSERT INTO sched_wm_b VALUES (1, 'b')").await;

    db.create_st(
        "sched_wm_report",
        "SELECT a.id, a.val AS a_val, b.val AS b_val \
         FROM sched_wm_a a JOIN sched_wm_b b ON a.id = b.id",
        "1s",
        "FULL",
    )
    .await;

    // Trigger CDC so the scheduler has changes to process for the auto-refresh.
    db.execute("INSERT INTO sched_wm_a VALUES (99, 'a99')")
        .await;

    // Wait for initial COMPLETED refresh.
    let refreshed = db
        .wait_for_auto_refresh("sched_wm_report", Duration::from_secs(60))
        .await;
    assert!(refreshed, "scheduler should auto-refresh before gating");

    // Create watermark group with strict tolerance, then misalign.
    db.execute(
        "SELECT pgtrickle.create_watermark_group(\
             'sched_wm_grp', ARRAY['sched_wm_a', 'sched_wm_b'], 0\
         )",
    )
    .await;
    db.execute("SELECT pgtrickle.advance_watermark('sched_wm_a', '2026-03-01 12:05:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('sched_wm_b', '2026-03-01 11:55:00+00')")
        .await;

    // Insert data to trigger scheduler attention.
    db.execute("INSERT INTO sched_wm_a VALUES (2, 'trigger')")
        .await;

    // Wait for a SKIPPED record.
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'sched_wm_report'",
        )
        .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut got_skip = false;
    while std::time::Instant::now() < deadline {
        let skip_count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} \
                   AND status = 'SKIPPED' \
                   AND action = 'SKIP' \
                   AND error_message LIKE '%watermark%'"
            ))
            .await;
        if skip_count > 0 {
            got_skip = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        got_skip,
        "scheduler should write SKIPPED/SKIP with watermark reason in pgt_refresh_history"
    );
}

/// WM-4: Scheduler resumes refreshes after watermarks align.
///
/// Procedure:
/// 1. Set up misaligned watermarks (scheduler starts skipping).
/// 2. Align watermarks.
/// 3. Wait for a new COMPLETED refresh.
#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_scheduler_resumes_after_watermark_alignment() {
    let db = E2eDb::new_on_postgres_db().await;

    db.execute("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
        .await;

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

    let sched_ok = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(sched_ok, "pg_trickle scheduler must be running");

    db.execute("CREATE TABLE res_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE res_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO res_a VALUES (1, 'a')").await;
    db.execute("INSERT INTO res_b VALUES (1, 'b')").await;

    db.create_st(
        "res_report",
        "SELECT a.id, a.val AS a_val, b.val AS b_val \
         FROM res_a a JOIN res_b b ON a.id = b.id",
        "1s",
        "FULL",
    )
    .await;

    // Capture the timestamp before the source write. The scheduler can
    // complete its refresh before we enter the wait on a fast runner.
    let baseline_timestamp: Option<String> = db
        .query_scalar_opt(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables \\
             WHERE pgt_name = 'res_report'",
        )
        .await;
    db.execute("INSERT INTO res_a VALUES (99, 'a99')").await;

    let refreshed = db
        .wait_for_auto_refresh_since(
            "res_report",
            baseline_timestamp,
            Duration::from_secs(60),
        )
        .await;
    assert!(refreshed, "initial auto-refresh should complete");

    // Create misaligned watermark group.
    db.execute("SELECT pgtrickle.create_watermark_group('res_grp', ARRAY['res_a', 'res_b'], 0)")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('res_a', '2026-03-01 12:05:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('res_b', '2026-03-01 11:55:00+00')")
        .await;

    // Count completed refreshes before alignment.
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'res_report'",
        )
        .await;
    let before_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = {pgt_id} AND status = 'COMPLETED'"
        ))
        .await;

    // Now align watermarks.
    db.execute("SELECT pgtrickle.advance_watermark('res_b', '2026-03-01 12:05:00+00')")
        .await;

    // Insert data so the scheduler has reason to refresh.
    db.execute("INSERT INTO res_a VALUES (2, 'new_data')").await;

    // Wait for another COMPLETED refresh after alignment.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut resumed = false;
    while std::time::Instant::now() < deadline {
        let after_count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} AND status = 'COMPLETED'"
            ))
            .await;
        if after_count > before_count {
            resumed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        resumed,
        "scheduler should resume refreshes after watermarks align"
    );
}

/// WM-4: Tolerance allows refresh when lag is within bounds.
///
/// Creates a group with 120s tolerance and 60s lag → aligned → refreshes.
#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_scheduler_respects_tolerance() {
    let db = E2eDb::new_on_postgres_db().await;

    db.execute("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
        .await;

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

    let sched_ok = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(sched_ok, "pg_trickle scheduler must be running");

    db.execute("CREATE TABLE tol_sa (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE tol_sb (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO tol_sa VALUES (1, 'a')").await;
    db.execute("INSERT INTO tol_sb VALUES (1, 'b')").await;

    db.create_st(
        "tol_report",
        "SELECT a.id, a.val AS a_val, b.val AS b_val \
         FROM tol_sa a JOIN tol_sb b ON a.id = b.id",
        "1s",
        "FULL",
    )
    .await;

    // Trigger CDC so the scheduler has changes to process for the auto-refresh.
    db.execute("INSERT INTO tol_sa VALUES (99, 'a99')").await;

    // Wait for initial refresh.
    let refreshed = db
        .wait_for_auto_refresh("tol_report", Duration::from_secs(60))
        .await;
    assert!(refreshed, "initial auto-refresh should complete");

    // Create group with 120s tolerance. Watermarks 60s apart → within tolerance.
    db.execute(
        "SELECT pgtrickle.create_watermark_group('tol_sgrp', ARRAY['tol_sa', 'tol_sb'], 120)",
    )
    .await;
    db.execute("SELECT pgtrickle.advance_watermark('tol_sa', '2026-03-01 12:01:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('tol_sb', '2026-03-01 12:00:00+00')")
        .await;

    // Insert data to trigger scheduler.
    db.execute("INSERT INTO tol_sa VALUES (2, 'trigger')").await;

    // The scheduler should still produce COMPLETED refreshes (within tolerance).
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'tol_report'",
        )
        .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut got_completed = false;
    while std::time::Instant::now() < deadline {
        let completed: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} AND status = 'COMPLETED'"
            ))
            .await;
        // We expect at least 2 completions (initial + post-watermark).
        if completed >= 2 {
            got_completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        got_completed,
        "scheduler should still refresh when watermark lag is within tolerance"
    );
}

// ── WM-7: Stuck Watermark Hold-Back tests ──────────────────────────────────

/// WM-7: Scheduler skips refresh when a watermark is stuck (not advanced
/// within holdback timeout). Uses a very short timeout (1s) so the test
/// completes quickly.
#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_scheduler_skips_stuck_watermark() {
    let db = E2eDb::new_on_postgres_db().await;

    db.execute("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
        .await;

    // Configure fast scheduler + holdback timeout.
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.watermark_holdback_timeout = 1")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;
    db.wait_for_setting("pg_trickle.watermark_holdback_timeout", "1")
        .await;

    let sched_ok = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(sched_ok, "pg_trickle scheduler must be running");

    // Create sources and stream table.
    db.execute("CREATE TABLE stuck_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE stuck_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO stuck_a VALUES (1, 'a')").await;
    db.execute("INSERT INTO stuck_b VALUES (1, 'b')").await;

    db.create_st(
        "stuck_report",
        "SELECT a.id, a.val AS a_val, b.val AS b_val \
         FROM stuck_a a JOIN stuck_b b ON a.id = b.id",
        "1s",
        "FULL",
    )
    .await;

    // Trigger initial refresh.
    db.execute("INSERT INTO stuck_a VALUES (99, 'trigger')")
        .await;
    let refreshed = db
        .wait_for_auto_refresh("stuck_report", Duration::from_secs(60))
        .await;
    assert!(refreshed, "scheduler should auto-refresh before gating");

    // Create watermark group and advance both sources to the SAME timestamp
    // (aligned, but will become stuck).
    db.execute(
        "SELECT pgtrickle.create_watermark_group(\
             'stuck_grp', ARRAY['stuck_a', 'stuck_b'], 0\
         )",
    )
    .await;
    db.execute("SELECT pgtrickle.advance_watermark('stuck_a', '2026-03-01 12:00:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('stuck_b', '2026-03-01 12:00:00+00')")
        .await;

    // Wait so the watermarks become stale (timeout is 1s).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Insert data to trigger scheduler.
    db.execute("INSERT INTO stuck_a VALUES (2, 'post_stuck')")
        .await;

    // Wait for a SKIPPED record mentioning "stuck".
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'stuck_report'",
        )
        .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut got_skip = false;
    while std::time::Instant::now() < deadline {
        let skip_count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} \
                   AND status = 'SKIPPED' \
                   AND action = 'SKIP' \
                   AND error_message LIKE '%stuck%'"
            ))
            .await;
        if skip_count > 0 {
            got_skip = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        got_skip,
        "scheduler should SKIP refresh when watermark is stuck"
    );
}

/// WM-7: Scheduler resumes refresh after a stuck watermark is advanced
/// (auto-resume).
#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_scheduler_resumes_after_watermark_unstuck() {
    let db = E2eDb::new_on_postgres_db().await;

    db.execute("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
        .await;

    // Configure fast scheduler + holdback timeout.
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.watermark_holdback_timeout = 1")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;
    db.wait_for_setting("pg_trickle.watermark_holdback_timeout", "1")
        .await;

    let sched_ok = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(sched_ok, "pg_trickle scheduler must be running");

    // Create sources and stream table.
    db.execute("CREATE TABLE resume_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE resume_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO resume_a VALUES (1, 'a')").await;
    db.execute("INSERT INTO resume_b VALUES (1, 'b')").await;

    db.create_st(
        "resume_report",
        "SELECT a.id, a.val AS a_val, b.val AS b_val \
         FROM resume_a a JOIN resume_b b ON a.id = b.id",
        "1s",
        "FULL",
    )
    .await;

    // Trigger initial refresh.
    db.execute("INSERT INTO resume_a VALUES (99, 'trigger')")
        .await;
    let refreshed = db
        .wait_for_auto_refresh("resume_report", Duration::from_secs(60))
        .await;
    assert!(refreshed, "scheduler should auto-refresh before gating");

    // Create watermark group and advance both sources.
    db.execute(
        "SELECT pgtrickle.create_watermark_group(\
             'resume_grp', ARRAY['resume_a', 'resume_b'], 0\
         )",
    )
    .await;
    db.execute("SELECT pgtrickle.advance_watermark('resume_a', '2026-03-01 12:00:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('resume_b', '2026-03-01 12:00:00+00')")
        .await;

    // Wait for watermarks to become stale (timeout is 1s).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Record the completed refresh count before unsticking.
    let completed_before: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                             WHERE pgt_name = 'resume_report') \
               AND status = 'COMPLETED'",
        )
        .await;

    // Re-advance watermarks to "unstick" them.
    db.execute("SELECT pgtrickle.advance_watermark('resume_a', '2026-03-02 12:00:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('resume_b', '2026-03-02 12:00:00+00')")
        .await;

    // Insert data so the scheduler has changes to process.
    db.execute("INSERT INTO resume_a VALUES (3, 'unstuck')")
        .await;

    // Wait for a new COMPLETED refresh after unsticking.
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'resume_report'",
        )
        .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut resumed = false;
    while std::time::Instant::now() < deadline {
        let completed_after: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} AND status = 'COMPLETED'"
            ))
            .await;
        if completed_after > completed_before {
            resumed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(
        resumed,
        "scheduler should resume refreshing after stuck watermarks are advanced"
    );
}

/// WM-7: Holdback timeout of 0 (disabled) does not cause stuck detection.
#[tokio::test]
async fn test_holdback_timeout_zero_disabled() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE hb0_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE hb0_b (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO hb0_a VALUES (1, 'a')").await;
    db.execute("INSERT INTO hb0_b VALUES (1, 'b')").await;

    db.create_st(
        "hb0_report",
        "SELECT a.id, a.val AS a_val, b.val AS b_val \
         FROM hb0_a a JOIN hb0_b b ON a.id = b.id",
        "5m",
        "FULL",
    )
    .await;

    // Create aligned group — with default holdback_timeout = 0.
    db.execute("SELECT pgtrickle.create_watermark_group('hb0_grp', ARRAY['hb0_a', 'hb0_b'], 0)")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('hb0_a', '2026-03-01 12:00:00+00')")
        .await;
    db.execute("SELECT pgtrickle.advance_watermark('hb0_b', '2026-03-01 12:00:00+00')")
        .await;

    // Manual refresh should succeed — holdback disabled.
    db.refresh_st("hb0_report").await;

    let count: i64 = db.count("public.hb0_report").await;
    assert_eq!(
        count, 1,
        "with holdback_timeout=0, refresh should succeed regardless of watermark age"
    );
}
