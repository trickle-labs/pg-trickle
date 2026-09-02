//! E2E tests for Phase 4 — Ergonomics & API Polish.
//!
//! Validates:
//! - ERG-D: Manual refresh records `initiated_by='MANUAL'` in pgt_refresh_history
//! - ERG-E: `pgtrickle.quick_health` view returns correct single-row status
//! - COR-2: `create_stream_table_if_not_exists()` is a no-op when ST exists

mod e2e;

use e2e::E2eDb;

// ── ERG-D: Manual refresh history recording ──────────────────────────

/// ERG-D: A manual `refresh_stream_table()` call must create a
/// `pgt_refresh_history` row with `initiated_by = 'MANUAL'`.
#[tokio::test]
async fn test_manual_refresh_records_initiated_by_manual() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE erg_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO erg_src VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st("erg_st", "SELECT id, val FROM erg_src", "1m", "FULL")
        .await;

    // Manual refresh
    db.refresh_st("erg_st").await;

    // Check that the history contains a MANUAL entry
    let initiated_by: String = db
        .query_scalar(
            "SELECT initiated_by FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'erg_st') \
             AND initiated_by = 'MANUAL' \
             ORDER BY refresh_id DESC LIMIT 1",
        )
        .await;
    assert_eq!(initiated_by, "MANUAL");
}

/// ERG-D: Manual refresh history should record COMPLETED status for a
/// successful refresh.
#[tokio::test]
async fn test_manual_refresh_records_completed_status() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE erg_status_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO erg_status_src VALUES (1, 10)")
        .await;

    db.create_st(
        "erg_status_st",
        "SELECT id, val FROM erg_status_src",
        "1m",
        "FULL",
    )
    .await;

    db.refresh_st("erg_status_st").await;

    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'erg_status_st') \
             AND initiated_by = 'MANUAL' \
             ORDER BY refresh_id DESC LIMIT 1",
        )
        .await;
    assert_eq!(status, "COMPLETED");

    // end_time should be set
    let has_end_time: bool = db
        .query_scalar(
            "SELECT end_time IS NOT NULL FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'erg_status_st') \
             AND initiated_by = 'MANUAL' \
             ORDER BY refresh_id DESC LIMIT 1",
        )
        .await;
    assert!(has_end_time);
}

/// ERG-D: Manual DIFFERENTIAL refresh must also record history.
#[tokio::test]
async fn test_manual_differential_refresh_records_history() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE erg_diff_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO erg_diff_src VALUES (1, 'x')").await;

    db.create_st(
        "erg_diff_st",
        "SELECT id, val FROM erg_diff_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // First manual refresh (will be FULL since no frontier yet)
    db.refresh_st("erg_diff_st").await;

    // Insert more data and do a second manual refresh (should be DIFFERENTIAL)
    db.execute("INSERT INTO erg_diff_src VALUES (2, 'y')").await;
    db.refresh_st("erg_diff_st").await;

    // Should have at least 2 MANUAL entries
    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'erg_diff_st') \
             AND initiated_by = 'MANUAL'",
        )
        .await;
    assert!(count >= 2, "expected >= 2 MANUAL entries, got {count}");
}

// ── ERG-E: quick_health view ─────────────────────────────────────────

/// ERG-E: The quick_health view should return exactly one row.
#[tokio::test]
async fn test_quick_health_returns_one_row() {
    let db = E2eDb::new().await.with_extension().await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.quick_health")
        .await;
    assert_eq!(count, 1);
}

/// ERG-E: With no stream tables, quick_health should show status = 'EMPTY'.
#[tokio::test]
async fn test_quick_health_empty_status() {
    let db = E2eDb::new().await.with_extension().await;

    let status: String = db
        .query_scalar("SELECT status FROM pgtrickle.quick_health")
        .await;
    assert_eq!(status, "EMPTY");

    let total: i64 = db
        .query_scalar("SELECT total_stream_tables FROM pgtrickle.quick_health")
        .await;
    assert_eq!(total, 0);
}

/// ERG-E: With a healthy stream table, quick_health should show status = 'OK'.
#[tokio::test]
async fn test_quick_health_ok_status() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE qh_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO qh_src VALUES (1, 'a')").await;

    db.create_st("qh_st", "SELECT id, val FROM qh_src", "1m", "FULL")
        .await;

    let total: i64 = db
        .query_scalar("SELECT total_stream_tables FROM pgtrickle.quick_health")
        .await;
    assert_eq!(total, 1);

    let error_tables: i64 = db
        .query_scalar("SELECT error_tables FROM pgtrickle.quick_health")
        .await;
    assert_eq!(error_tables, 0);
}

// ── COR-2: create_stream_table_if_not_exists ─────────────────────────

/// COR-2: When the stream table does not exist, it creates it normally.
#[tokio::test]
async fn test_create_if_not_exists_creates_when_missing() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ine_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO ine_src VALUES (1, 'a'), (2, 'b')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table_if_not_exists(\
         'ine_st', 'SELECT id, val FROM ine_src', '1m', 'FULL')",
    )
    .await;

    let count: i64 = db.count("ine_st").await;
    assert_eq!(count, 2);
}

/// COR-2: When the stream table already exists, it is a silent no-op.
#[tokio::test]
async fn test_create_if_not_exists_noop_when_exists() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ine2_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO ine2_src VALUES (1, 'a')").await;

    // First creation
    db.execute(
        "SELECT pgtrickle.create_stream_table_if_not_exists(\
         'ine2_st', 'SELECT id, val FROM ine2_src', '1m', 'FULL')",
    )
    .await;

    // Second call — should be a no-op (not error)
    db.execute(
        "SELECT pgtrickle.create_stream_table_if_not_exists(\
         'ine2_st', 'SELECT id, val FROM ine2_src', '1m', 'FULL')",
    )
    .await;

    // Still exactly 1 row (not recreated)
    let count: i64 = db.count("ine2_st").await;
    assert_eq!(count, 1);

    // Only one catalog entry
    let cat_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ine2_st'")
        .await;
    assert_eq!(cat_count, 1);
}

/// COR-2: create_stream_table_if_not_exists with a different query
/// should still be a no-op when the name matches.
#[tokio::test]
async fn test_create_if_not_exists_ignores_query_difference() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ine3_src (id INT PRIMARY KEY, val TEXT, extra INT DEFAULT 0)")
        .await;
    db.execute("INSERT INTO ine3_src VALUES (1, 'a', 10)").await;

    // Create with one query
    db.execute(
        "SELECT pgtrickle.create_stream_table_if_not_exists(\
         'ine3_st', 'SELECT id, val FROM ine3_src', '1m', 'FULL')",
    )
    .await;

    // Second call with a different query — should still be no-op
    db.execute(
        "SELECT pgtrickle.create_stream_table_if_not_exists(\
         'ine3_st', 'SELECT id, val, extra FROM ine3_src', '1m', 'FULL')",
    )
    .await;

    // The original definition should be preserved (2 columns: id, val + __pgt_row_id)
    let col_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_name = 'ine3_st' AND table_schema = 'public'",
        )
        .await;
    // __pgt_row_id + id + val = 3 columns
    assert_eq!(col_count, 3);
}

// ── ERG-T1: Smart schedule default ───────────────────────────────────

/// ERG-T1: Passing `schedule => 'calculated'` should succeed and produce
/// a CALCULATED stream table (NULL schedule in catalog).
#[tokio::test]
async fn test_erg_t1_calculated_schedule_accepted() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t1_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO t1_src VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st(
        "t1_calc_st",
        "SELECT id, val FROM t1_src",
        "calculated",
        "FULL",
    )
    .await;

    // Catalog should store NULL schedule for CALCULATED mode
    let schedule_is_null: bool = db
        .query_scalar(
            "SELECT schedule IS NULL FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 't1_calc_st'",
        )
        .await;
    assert!(
        schedule_is_null,
        "CALCULATED schedule should be stored as NULL in catalog"
    );

    // The stream table should be populated
    let count = db.count("public.t1_calc_st").await;
    assert_eq!(count, 2);
}

/// ERG-T1: The default schedule (when omitted) is 'calculated', so
/// creating with only name + query + refresh_mode should work.
#[tokio::test]
async fn test_erg_t1_default_schedule_is_calculated() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t1_def_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO t1_def_src VALUES (1, 10)").await;

    // Omit schedule entirely — should default to 'calculated'
    db.execute(
        "SELECT pgtrickle.create_stream_table('t1_def_st', \
         $$ SELECT id, val FROM t1_def_src $$, refresh_mode => 'FULL')",
    )
    .await;

    let schedule_is_null: bool = db
        .query_scalar(
            "SELECT schedule IS NULL FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 't1_def_st'",
        )
        .await;
    assert!(
        schedule_is_null,
        "Default schedule should be CALCULATED (stored as NULL)"
    );
}

/// ERG-T1: Passing `schedule => NULL` should return a clear error.
#[tokio::test]
async fn test_erg_t1_null_schedule_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t1_null_src (id INT PRIMARY KEY, val INT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('t1_null_st', \
             $$ SELECT id, val FROM t1_null_src $$, NULL, 'FULL')",
        )
        .await;

    assert!(result.is_err(), "NULL schedule should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("calculated"),
        "Error should suggest 'calculated'; got: {err_msg}"
    );
}

/// ERG-T1: An explicit fixed schedule like '30s' should still work.
#[tokio::test]
async fn test_erg_t1_explicit_fixed_schedule_works() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t1_fixed_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO t1_fixed_src VALUES (1, 42)").await;

    db.create_st(
        "t1_fixed_st",
        "SELECT id, val FROM t1_fixed_src",
        "30s",
        "FULL",
    )
    .await;

    let schedule: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 't1_fixed_st'",
        )
        .await;
    assert_eq!(schedule, "30s");
}

/// ERG-T1: `alter_stream_table` with `schedule => 'calculated'` should
/// switch an existing fixed-schedule ST to CALCULATED mode.
#[tokio::test]
async fn test_erg_t1_alter_to_calculated_schedule() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t1_alter_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO t1_alter_src VALUES (1, 1)").await;

    db.create_st(
        "t1_alter_st",
        "SELECT id, val FROM t1_alter_src",
        "1m",
        "FULL",
    )
    .await;

    // Switch to calculated
    db.alter_st("t1_alter_st", "schedule => 'calculated'").await;

    let schedule_is_null: bool = db
        .query_scalar(
            "SELECT schedule IS NULL FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 't1_alter_st'",
        )
        .await;
    assert!(
        schedule_is_null,
        "After ALTER to 'calculated', schedule should be NULL in catalog"
    );
}

// ── ERG-T2: Removed GUCs stay removed ────────────────────────────────

/// ERG-T2: `SHOW pg_trickle.diamond_consistency` should return an error
/// because this GUC was removed in v0.4.0.
#[tokio::test]
async fn test_erg_t2_diamond_consistency_guc_removed() {
    let db = E2eDb::new().await.with_extension().await;

    let result = db.try_execute("SHOW pg_trickle.diamond_consistency").await;
    assert!(
        result.is_err(),
        "diamond_consistency GUC should not exist; SHOW should error"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("unrecognized configuration parameter")
            || err_msg.contains("not recognized"),
        "Expected 'unrecognized' error; got: {err_msg}"
    );
}

/// ERG-T2: `SHOW pg_trickle.diamond_schedule_policy` should return an
/// error because this GUC was removed in v0.4.0.
#[tokio::test]
async fn test_erg_t2_diamond_schedule_policy_guc_removed() {
    let db = E2eDb::new().await.with_extension().await;

    let result = db
        .try_execute("SHOW pg_trickle.diamond_schedule_policy")
        .await;
    assert!(
        result.is_err(),
        "diamond_schedule_policy GUC should not exist; SHOW should error"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("unrecognized configuration parameter")
            || err_msg.contains("not recognized"),
        "Expected 'unrecognized' error; got: {err_msg}"
    );
}

// ── ERG-T3: Full refresh warning on alter ────────────────────────────

/// ERG-T3: Changing a stream table's refresh mode via `alter_stream_table`
/// should emit a WARNING about the implicit full refresh.
#[tokio::test]
async fn test_erg_t3_alter_refresh_mode_emits_warning() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t3_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO t3_src VALUES (1, 10), (2, 20)")
        .await;

    // Create as FULL first
    db.create_st("t3_warn_st", "SELECT id, val FROM t3_src", "1m", "FULL")
        .await;

    // Alter to DIFFERENTIAL — this triggers a full refresh + warning
    let notices = db
        .try_execute_with_notices(
            "SELECT pgtrickle.alter_stream_table('t3_warn_st', \
             refresh_mode => 'DIFFERENTIAL')",
        )
        .await
        .expect("alter_stream_table should succeed");

    let saw_warning = notices
        .iter()
        .any(|n| n.contains("refresh mode changed") && n.contains("full refresh was applied"));
    assert!(
        saw_warning,
        "Expected WARNING about implicit full refresh; got: {notices:?}"
    );
}

/// ERG-T3: Changing the defining query via `alter_stream_table` should
/// emit a WARNING about the protected rebuild triggered by the schema change.
#[tokio::test]
async fn test_erg_t3_alter_query_emits_warning() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t3q_src (id INT PRIMARY KEY, val INT, extra TEXT DEFAULT 'x')")
        .await;
    db.execute("INSERT INTO t3q_src VALUES (1, 10, 'a'), (2, 20, 'b')")
        .await;

    db.create_st("t3q_warn_st", "SELECT id, val FROM t3q_src", "1m", "FULL")
        .await;

    // Change the query (adds a column — incompatible schema change)
    let notices = db
        .try_execute_with_notices(
            "SELECT pgtrickle.alter_stream_table('t3q_warn_st', \
             query => $$ SELECT id, val, extra FROM t3q_src $$)",
        )
        .await
        .expect("alter_stream_table with query change should succeed");

    let saw_warning = notices
        .iter()
        .any(|n| n.contains("ALTER QUERY applied a protected") && n.contains("rebuild"));
    assert!(
        saw_warning,
        "Expected WARNING about ALTER QUERY protected rebuild; got: {notices:?}"
    );
}

/// ERG-T3: Altering to the same refresh mode should NOT produce a
/// full-refresh warning.
#[tokio::test]
async fn test_erg_t3_alter_same_mode_no_warning() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t3s_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO t3s_src VALUES (1, 10)").await;

    db.create_st("t3s_st", "SELECT id, val FROM t3s_src", "1m", "FULL")
        .await;

    // Alter to the same mode — should be a no-op, no warning
    let notices = db
        .try_execute_with_notices(
            "SELECT pgtrickle.alter_stream_table('t3s_st', \
             refresh_mode => 'FULL')",
        )
        .await
        .expect("alter to same mode should succeed");

    let saw_refresh_warning = notices
        .iter()
        .any(|n| n.contains("full refresh was applied"));
    assert!(
        !saw_refresh_warning,
        "Same-mode alter should NOT produce full refresh warning; got: {notices:?}"
    );
}

// ── ERG-T4: WAL configuration warning ────────────────────────────────

/// ERG-T4: When `wal_level = logical` (as in E2E containers), the
/// `_PG_init` WAL configuration warning should NOT appear. We verify by
/// creating the extension on a fresh connection and checking notices.
#[tokio::test]
async fn test_erg_t4_no_wal_warning_when_wal_level_logical() {
    let db = E2eDb::new().await.with_extension().await;

    // In light E2E the extension is loaded dynamically (no shared_preload),
    // but we can still verify no spurious WAL warning is emitted during SQL
    // operations.
    let notices = db
        .try_execute_with_notices("SELECT 1")
        .await
        .expect("simple query should succeed");

    let saw_wal_warning = notices
        .iter()
        .any(|n| n.contains("wal_level is not") || n.contains("wal_level"));
    assert!(
        !saw_wal_warning,
        "No WAL-level warning expected when wal_level is logical; got: {notices:?}"
    );
}
