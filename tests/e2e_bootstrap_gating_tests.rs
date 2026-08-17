//! E2E tests for Bootstrap Source Gating (v0.5.0, Phase 3) and Follow-Up (v0.6.0).
//!
//! Validates:
//! - BOOT-2: `gate_source()` inserts into `pgt_source_gates`
//! - BOOT-3: `ungate_source()` marks gated=false; `source_gates()` returns current status
//! - BOOT-4: Scheduler logs SKIP in `pgt_refresh_history` for gated sources;
//!   manual refresh is NOT blocked by gates
//! - BOOT-F1: Idempotent `gate_source()` refreshes timestamp and preserves state
//! - BOOT-F2: Full gate → ungate → re-gate lifecycle with manual refresh
//! - BOOT-F3: `bootstrap_gate_status()` introspection function
//! - Edge cases: idempotent gating, re-gating after ungate, non-existent table
//!
//! Prerequisites: `just build-e2e-image` (for scheduler tests)

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Catalog API tests (light E2E) ──────────────────────────────────────────

/// BOOT-2: gate_source() inserts a row into pgt_source_gates with gated=true.
#[tokio::test]
async fn test_gate_source_inserts_gate_record() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE gated_src (id INT PRIMARY KEY, val TEXT)")
        .await;

    db.execute("SELECT pgtrickle.gate_source('gated_src')")
        .await;

    let gated: bool = db
        .query_scalar(
            "SELECT g.gated \
             FROM pgtrickle.pgt_source_gates g \
             JOIN pg_class c ON c.oid = g.source_relid \
             WHERE c.relname = 'gated_src'",
        )
        .await;

    assert!(
        gated,
        "gate_source() should set gated=true in pgt_source_gates"
    );
}

/// BOOT-3: source_gates() returns rows with the correct fields after gating.
#[tokio::test]
async fn test_source_gates_returns_gated_source() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sg_src (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.gate_source('sg_src')").await;

    let gated: bool = db
        .query_scalar(
            "SELECT gated FROM pgtrickle.source_gates() \
             WHERE source_table = 'sg_src'",
        )
        .await;

    assert!(gated, "source_gates() should report the source as gated");
}

/// BOOT-3: ungate_source() sets gated=false and records ungated_at.
#[tokio::test]
async fn test_ungate_source_clears_gate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ug_src (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.gate_source('ug_src')").await;
    db.execute("SELECT pgtrickle.ungate_source('ug_src')").await;

    let gated: bool = db
        .query_scalar(
            "SELECT gated FROM pgtrickle.source_gates() \
             WHERE source_table = 'ug_src'",
        )
        .await;

    let ungated_at_not_null: bool = db
        .query_scalar(
            "SELECT ungated_at IS NOT NULL \
             FROM pgtrickle.source_gates() \
             WHERE source_table = 'ug_src'",
        )
        .await;

    assert!(!gated, "ungate_source() should set gated=false");
    assert!(ungated_at_not_null, "ungate_source() should set ungated_at");
}

/// Idempotent gating: calling gate_source() twice is safe.
#[tokio::test]
async fn test_gate_source_is_idempotent() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE idem_src (id INT PRIMARY KEY)")
        .await;
    db.execute("SELECT pgtrickle.gate_source('idem_src')").await;
    // Second call must not error.
    db.execute("SELECT pgtrickle.gate_source('idem_src')").await;

    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_source_gates g \
             JOIN pg_class c ON c.oid = g.source_relid \
             WHERE c.relname = 'idem_src'",
        )
        .await;

    assert_eq!(
        count, 1,
        "idempotent gate_source() must produce exactly one row"
    );
}

/// Re-gating after ungate works correctly.
#[tokio::test]
async fn test_regate_after_ungate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rg_src (id INT PRIMARY KEY)").await;
    db.execute("SELECT pgtrickle.gate_source('rg_src')").await;
    db.execute("SELECT pgtrickle.ungate_source('rg_src')").await;
    // Re-gate: should set gated=true and clear ungated_at.
    db.execute("SELECT pgtrickle.gate_source('rg_src')").await;

    let gated: bool = db
        .query_scalar(
            "SELECT gated FROM pgtrickle.source_gates() \
             WHERE source_table = 'rg_src'",
        )
        .await;

    assert!(gated, "re-gating after ungate should set gated=true again");
}

/// gate_source() on a non-existent relation returns an error.
#[tokio::test]
async fn test_gate_source_nonexistent_table_errors() {
    let db = E2eDb::new().await.with_extension().await;

    let result = db
        .try_execute("SELECT pgtrickle.gate_source('does_not_exist_table_xyz')")
        .await;

    assert!(
        result.is_err(),
        "gate_source() on a non-existent table should return an error"
    );
}

/// source_gates() returns an empty result set when no gates have been registered.
#[tokio::test]
async fn test_source_gates_empty_by_default() {
    let db = E2eDb::new().await.with_extension().await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.source_gates()")
        .await;

    assert_eq!(
        count, 0,
        "source_gates() should be empty when nothing is gated"
    );
}

/// Multiple sources can be gated simultaneously.
#[tokio::test]
async fn test_multiple_sources_gated() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE multi_src1 (id INT PRIMARY KEY)")
        .await;
    db.execute("CREATE TABLE multi_src2 (id INT PRIMARY KEY)")
        .await;
    db.execute("SELECT pgtrickle.gate_source('multi_src1')")
        .await;
    db.execute("SELECT pgtrickle.gate_source('multi_src2')")
        .await;

    let gated_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.source_gates() WHERE gated = true")
        .await;

    assert_eq!(gated_count, 2, "both sources should appear as gated");
}

// ── BOOT-F1: Enhanced idempotency tests ────────────────────────────────────

/// BOOT-F1: Double-gating refreshes gated_at timestamp and gated_by.
///
/// When gate_source() is called twice, the second call should update
/// gated_at to a newer timestamp (proving it's an UPSERT, not a skip).
#[tokio::test]
async fn test_idempotent_gate_refreshes_timestamp() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE idem_ts_src (id INT PRIMARY KEY)")
        .await;

    db.execute("SELECT pgtrickle.gate_source('idem_ts_src')")
        .await;

    let first_gated_at: String = db
        .query_scalar(
            "SELECT g.gated_at::text \
             FROM pgtrickle.pgt_source_gates g \
             JOIN pg_class c ON c.oid = g.source_relid \
             WHERE c.relname = 'idem_ts_src'",
        )
        .await;

    // Small delay so that now() advances.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second gate call — should update gated_at.
    db.execute("SELECT pgtrickle.gate_source('idem_ts_src')")
        .await;

    let second_gated_at: String = db
        .query_scalar(
            "SELECT g.gated_at::text \
             FROM pgtrickle.pgt_source_gates g \
             JOIN pg_class c ON c.oid = g.source_relid \
             WHERE c.relname = 'idem_ts_src'",
        )
        .await;

    assert_ne!(
        first_gated_at, second_gated_at,
        "idempotent gate_source() should refresh gated_at timestamp"
    );
}

/// BOOT-F1: Double-gating preserves exactly one row (no duplicates).
///
/// Extends the existing test_gate_source_is_idempotent with additional verifications:
/// gated remains true, ungated_at stays NULL, gated_by is preserved.
#[tokio::test]
async fn test_idempotent_gate_preserves_state() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE idem_st_src (id INT PRIMARY KEY)")
        .await;

    db.execute("SELECT pgtrickle.gate_source('idem_st_src')")
        .await;
    db.execute("SELECT pgtrickle.gate_source('idem_st_src')")
        .await;

    let gated: bool = db
        .query_scalar(
            "SELECT g.gated \
             FROM pgtrickle.pgt_source_gates g \
             JOIN pg_class c ON c.oid = g.source_relid \
             WHERE c.relname = 'idem_st_src'",
        )
        .await;

    let ungated_at_is_null: bool = db
        .query_scalar(
            "SELECT g.ungated_at IS NULL \
             FROM pgtrickle.pgt_source_gates g \
             JOIN pg_class c ON c.oid = g.source_relid \
             WHERE c.relname = 'idem_st_src'",
        )
        .await;

    let gated_by: String = db
        .query_scalar(
            "SELECT g.gated_by \
             FROM pgtrickle.pgt_source_gates g \
             JOIN pg_class c ON c.oid = g.source_relid \
             WHERE c.relname = 'idem_st_src'",
        )
        .await;

    assert!(gated, "double-gated source should still be gated");
    assert!(
        ungated_at_is_null,
        "double-gated source should have NULL ungated_at"
    );
    assert_eq!(gated_by, "gate_source", "gated_by should be 'gate_source'");
}

// ── BOOT-F2: Enhanced lifecycle tests ──────────────────────────────────────

/// BOOT-F2: Full lifecycle — gate → ungate → re-gate preserves correct state.
///
/// Verifies that after re-gating, ungated_at is NULL again (cleared by the
/// UPSERT) and gated_at is updated.
#[tokio::test]
async fn test_regate_lifecycle_clears_ungated_at() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lc_src (id INT PRIMARY KEY)").await;

    // Gate
    db.execute("SELECT pgtrickle.gate_source('lc_src')").await;

    // Ungate
    db.execute("SELECT pgtrickle.ungate_source('lc_src')").await;

    let ungated_at_after_ungate: bool = db
        .query_scalar(
            "SELECT ungated_at IS NOT NULL \
             FROM pgtrickle.source_gates() WHERE source_table = 'lc_src'",
        )
        .await;
    assert!(
        ungated_at_after_ungate,
        "ungated_at should be set after ungate"
    );

    // Re-gate: ungated_at should be cleared
    db.execute("SELECT pgtrickle.gate_source('lc_src')").await;

    let gated: bool = db
        .query_scalar(
            "SELECT gated FROM pgtrickle.source_gates() \
             WHERE source_table = 'lc_src'",
        )
        .await;
    let ungated_at_is_null: bool = db
        .query_scalar(
            "SELECT ungated_at IS NULL \
             FROM pgtrickle.source_gates() WHERE source_table = 'lc_src'",
        )
        .await;

    assert!(gated, "re-gated source should be gated");
    assert!(
        ungated_at_is_null,
        "re-gating should clear ungated_at back to NULL"
    );
}

/// BOOT-F2: Manual refresh works correctly throughout the full gate lifecycle.
///
/// Verifies: gated → manual refresh OK → ungate → manual refresh OK →
/// re-gate → manual refresh still OK. Manual refresh must never be blocked.
#[tokio::test]
async fn test_manual_refresh_works_through_full_lifecycle() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lc_man_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO lc_man_src VALUES (1, 'a')").await;
    db.create_st("lc_man_st", "SELECT id, val FROM lc_man_src", "5m", "FULL")
        .await;

    // Gate → manual refresh should still work.
    db.execute("SELECT pgtrickle.gate_source('lc_man_src')")
        .await;
    db.refresh_st("lc_man_st").await;
    let c1: i64 = db.count("public.lc_man_st").await;
    assert_eq!(c1, 1, "manual refresh while gated should work");

    // Ungate → insert/update → manual refresh.
    db.execute("SELECT pgtrickle.ungate_source('lc_man_src')")
        .await;
    db.execute("INSERT INTO lc_man_src VALUES (2, 'b')").await;
    db.execute("UPDATE lc_man_src SET val = 'b_mod' WHERE id = 2")
        .await;
    db.refresh_st("lc_man_st").await;
    let c2: i64 = db.count("public.lc_man_st").await;
    assert_eq!(c2, 2, "manual refresh after ungate should work");

    // Re-gate → insert/delete → manual refresh still works.
    db.execute("SELECT pgtrickle.gate_source('lc_man_src')")
        .await;
    db.execute("INSERT INTO lc_man_src VALUES (3, 'c')").await;
    db.execute("DELETE FROM lc_man_src WHERE id = 1").await;
    db.refresh_st("lc_man_st").await;
    let c3: i64 = db.count("public.lc_man_st").await;
    assert_eq!(
        c3, 2,
        "manual refresh after re-gate should work (1 del, 1 ins)"
    );

    // Verify full data correctness at the end of the gate lifecycle
    db.assert_st_matches_query("public.lc_man_st", "SELECT id, val FROM lc_man_src")
        .await;
}

// ── BOOT-F3: bootstrap_gate_status() tests ─────────────────────────────────

/// BOOT-F3: bootstrap_gate_status() returns the expected columns.
#[tokio::test]
async fn test_bootstrap_gate_status_returns_expected_columns() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE bgs_src (id INT PRIMARY KEY)")
        .await;
    db.execute("SELECT pgtrickle.gate_source('bgs_src')").await;

    let gated: bool = db
        .query_scalar(
            "SELECT gated FROM pgtrickle.bootstrap_gate_status() \
             WHERE source_table = 'bgs_src'",
        )
        .await;
    assert!(gated, "bootstrap_gate_status() should show source as gated");

    let has_duration: bool = db
        .query_scalar(
            "SELECT gate_duration IS NOT NULL \
             FROM pgtrickle.bootstrap_gate_status() \
             WHERE source_table = 'bgs_src'",
        )
        .await;
    assert!(
        has_duration,
        "bootstrap_gate_status() should compute gate_duration for gated source"
    );
}

/// BOOT-F3: bootstrap_gate_status() shows gate_duration for ungated sources.
#[tokio::test]
async fn test_bootstrap_gate_status_ungated_duration() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE bgs_ug_src (id INT PRIMARY KEY)")
        .await;
    db.execute("SELECT pgtrickle.gate_source('bgs_ug_src')")
        .await;
    // Small delay so duration is nonzero.
    tokio::time::sleep(Duration::from_millis(50)).await;
    db.execute("SELECT pgtrickle.ungate_source('bgs_ug_src')")
        .await;

    let gated: bool = db
        .query_scalar(
            "SELECT gated FROM pgtrickle.bootstrap_gate_status() \
             WHERE source_table = 'bgs_ug_src'",
        )
        .await;
    assert!(
        !gated,
        "bootstrap_gate_status() should show source as ungated"
    );

    let has_duration: bool = db
        .query_scalar(
            "SELECT gate_duration IS NOT NULL \
             FROM pgtrickle.bootstrap_gate_status() \
             WHERE source_table = 'bgs_ug_src'",
        )
        .await;
    assert!(
        has_duration,
        "bootstrap_gate_status() should show historic gate_duration for ungated source"
    );
}

/// BOOT-F3: bootstrap_gate_status() shows affected_stream_tables.
#[tokio::test]
async fn test_bootstrap_gate_status_affected_stream_tables() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE bgs_dep_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO bgs_dep_src VALUES (1, 'x')").await;
    db.create_st(
        "bgs_dep_st",
        "SELECT id, val FROM bgs_dep_src",
        "5m",
        "FULL",
    )
    .await;

    db.execute("SELECT pgtrickle.gate_source('bgs_dep_src')")
        .await;

    let affected: String = db
        .query_scalar(
            "SELECT affected_stream_tables \
             FROM pgtrickle.bootstrap_gate_status() \
             WHERE source_table = 'bgs_dep_src'",
        )
        .await;

    assert!(
        affected.contains("bgs_dep_st"),
        "affected_stream_tables should list the dependent stream table, got: {}",
        affected
    );
}

/// BOOT-F3: bootstrap_gate_status() returns empty when no gates registered.
#[tokio::test]
async fn test_bootstrap_gate_status_empty_by_default() {
    let db = E2eDb::new().await.with_extension().await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.bootstrap_gate_status()")
        .await;

    assert_eq!(
        count, 0,
        "bootstrap_gate_status() should be empty when nothing is gated"
    );
}

// ── Manual refresh not blocked (BOOT-4 boundary) ──────────────────────────

/// BOOT-4 boundary: manual refresh_stream_table() is NOT blocked by gates.
///
/// Gates only suppress scheduler-initiated refreshes.  Manual refreshes
/// must always succeed so operators can unblock out-of-band.
#[tokio::test]
async fn test_manual_refresh_not_blocked_by_gate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE man_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO man_src VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st("man_st", "SELECT id, val FROM man_src", "5m", "FULL")
        .await;

    // Gate the source table.
    db.execute("SELECT pgtrickle.gate_source('man_src')").await;

    // Manual refresh must succeed even though the source is gated.
    db.refresh_st("man_st").await;

    let count: i64 = db.count("public.man_st").await;
    assert_eq!(
        count, 2,
        "manual refresh must succeed even when source is gated"
    );

    // Verify data correctness — both rows must be present with correct values
    db.assert_st_matches_query("public.man_st", "SELECT id, val FROM man_src")
        .await;
}

// ── Scheduler SKIP tests (full E2E — requires bgworker) ───────────────────

/// BOOT-4: When a source is gated the scheduler logs SKIP+SKIPPED in history.
///
/// Procedure:
/// 1. Configure fast scheduler.
/// 2. Create source + stream table.
/// 3. Wait for at least one successful COMPLETED refresh to confirm the
///    scheduler is working.
/// 4. Gate the source.
/// 5. Insert more rows (so the scheduler would normally fire again).
/// 6. Wait for a SKIPPED record to appear in pgt_refresh_history.
#[tokio::test]
async fn test_scheduler_logs_skip_when_source_gated() {
    let db = E2eDb::new_on_postgres_db().await;

    // Install the extension so the launcher can discover this database
    // and spawn a scheduler background worker for it.
    db.execute("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
        .await;

    // Lower scheduler cadence to speed up the test (matching bgworker tests).
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

    db.execute("CREATE TABLE sched_gate_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sched_gate_src VALUES (1, 'hello')")
        .await;
    db.create_st(
        "sched_gate_st",
        "SELECT id, val FROM sched_gate_src",
        "1s",
        "FULL",
    )
    .await;

    // Insert after ST creation so CDC captures a change and the scheduler
    // has a reason to auto-refresh (advancing data_timestamp).
    db.execute("INSERT INTO sched_gate_src VALUES (2, 'trigger')")
        .await;

    // Wait for the scheduler to apply the CDC change. Checking the materialized
    // row count avoids relying on data_timestamp precision when fast refreshes
    // happen within the same timestamp value.
    let refreshed = db
        .wait_for_condition(
            "sched_gate_st contains the CDC row",
            "SELECT count(*) >= 2 FROM public.sched_gate_st",
            Duration::from_secs(60),
            Duration::from_millis(200),
        )
        .await;
    assert!(
        refreshed,
        "scheduler should auto-refresh sched_gate_st before gate is set"
    );

    // Now gate the source.
    db.execute("SELECT pgtrickle.gate_source('sched_gate_src')")
        .await;

    // Insert a row so the scheduler has a reason to fire (change detected).
    db.execute("INSERT INTO sched_gate_src VALUES (3, 'world')")
        .await;

    // Wait up to 30 s for a SKIPPED record in pgt_refresh_history.
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'sched_gate_st'",
        )
        .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut got_skip = false;
    while std::time::Instant::now() < deadline {
        let skip_count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = {pgt_id} AND status = 'SKIPPED' AND action = 'SKIP'"
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
        "scheduler should write a SKIPPED/SKIP record to pgt_refresh_history when source is gated"
    );

    // After ungating the source, the scheduler should resume normal refreshes.
    db.execute("SELECT pgtrickle.ungate_source('sched_gate_src')")
        .await;

    let resumed = db
        .wait_for_auto_refresh("sched_gate_st", Duration::from_secs(60))
        .await;
    assert!(
        resumed,
        "scheduler should resume refreshes after ungate_source()"
    );
}
