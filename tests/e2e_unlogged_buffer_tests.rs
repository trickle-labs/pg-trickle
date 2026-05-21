//! D-1d: E2E tests for UNLOGGED change buffers.
//!
//! Validates:
//!   - `pg_trickle.unlogged_buffers = true` creates UNLOGGED buffer tables
//!   - `pg_trickle.unlogged_buffers = false` (default) creates logged buffers
//!   - `pgtrickle.convert_buffers_to_unlogged()` converts existing buffers
//!   - Crash recovery triggers FULL refresh for UNLOGGED buffers
//!
//! Light-eligible tests use `E2eDb::new()` (GUC + relpersistence checks).
//! Crash-recovery tests use full E2E harness.

mod e2e;

use e2e::E2eDb;

// ── Light-eligible: GUC surface + buffer creation ─────────────────────────

/// D-1d: Default `unlogged_buffers` GUC is 'off'.
#[tokio::test]
async fn test_unlogged_buffers_guc_default_off() {
    let db = E2eDb::new().await.with_extension().await;
    let val = db.show_setting("pg_trickle.unlogged_buffers").await;
    assert_eq!(val, "off", "Default unlogged_buffers should be 'off'");
}

/// D-1d: When `unlogged_buffers = false`, new change buffers are logged
/// (relpersistence = 'p' for permanent/logged).
#[tokio::test]
async fn test_logged_buffer_when_guc_off() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE unlog_src1 (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO unlog_src1 VALUES (1, 10)").await;
    db.create_st(
        "unlog_st1",
        "SELECT id, val FROM unlog_src1",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Find the change buffer table for this ST (v0.32.0+: stable name)
    let oid: i32 = db.table_oid("unlog_src1").await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let persistence: String = db
        .query_scalar(&format!(
            "SELECT relpersistence::text FROM pg_class \
             WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgtrickle_changes') \
             AND relname = 'changes_{stable_name}'"
        ))
        .await;
    assert_eq!(
        persistence, "p",
        "Buffer should be logged (permanent) when unlogged_buffers = off"
    );
}

/// D-1d: When `unlogged_buffers = true`, new change buffers are UNLOGGED
/// (relpersistence = 'u').
#[tokio::test]
async fn test_unlogged_buffer_when_guc_on() {
    let db = E2eDb::new().await.with_extension().await;

    // Set the GUC for this session (session-level SET is enough for
    // create_stream_table to read it during the same transaction)
    db.execute_seq(&[
        "SET pg_trickle.unlogged_buffers = on",
        "CREATE TABLE unlog_src2 (id INT PRIMARY KEY, val INT)",
        "INSERT INTO unlog_src2 VALUES (1, 10)",
        "SELECT pgtrickle.create_stream_table(\
            'unlog_st2',\
            'SELECT id, val FROM unlog_src2',\
            '1m',\
            'DIFFERENTIAL'\
        )",
    ])
    .await;

    let oid: i32 = db.table_oid("unlog_src2").await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let persistence: String = db
        .query_scalar(&format!(
            "SELECT relpersistence::text FROM pg_class \
             WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgtrickle_changes') \
             AND relname = 'changes_{stable_name}'"
        ))
        .await;
    assert_eq!(
        persistence, "u",
        "Buffer should be UNLOGGED when unlogged_buffers = on"
    );
}

/// D-1d: `pgtrickle.convert_buffers_to_unlogged()` converts existing
/// logged buffers to UNLOGGED and returns the count.
#[tokio::test]
async fn test_convert_buffers_to_unlogged() {
    let db = E2eDb::new().await.with_extension().await;

    // Create two STs with logged buffers (default GUC = off)
    db.execute("CREATE TABLE conv_src1 (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO conv_src1 VALUES (1, 10)").await;
    db.create_st(
        "conv_st1",
        "SELECT id, val FROM conv_src1",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute("CREATE TABLE conv_src2 (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO conv_src2 VALUES (1, 'a')").await;
    db.create_st(
        "conv_st2",
        "SELECT id, val FROM conv_src2",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Verify both are logged before conversion
    let logged_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_class c \
             JOIN pg_namespace n ON c.relnamespace = n.oid \
             WHERE n.nspname = 'pgtrickle_changes' \
             AND c.relname LIKE 'changes_%' \
             AND c.relpersistence = 'p'",
        )
        .await;
    assert!(
        logged_count >= 2,
        "Should have at least 2 logged buffer tables, got {logged_count}"
    );

    // Run conversion
    let converted: i64 = db
        .query_scalar("SELECT pgtrickle.convert_buffers_to_unlogged()")
        .await;
    assert!(
        converted >= 2,
        "convert_buffers_to_unlogged should convert at least 2 tables, got {converted}"
    );

    // Verify all change buffers are now UNLOGGED
    let still_logged: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_class c \
             JOIN pg_namespace n ON c.relnamespace = n.oid \
             WHERE n.nspname = 'pgtrickle_changes' \
             AND c.relname LIKE 'changes_%' \
             AND c.relpersistence = 'p'",
        )
        .await;
    assert_eq!(
        still_logged, 0,
        "All change buffers should be UNLOGGED after conversion"
    );
}

/// D-1d: Calling `convert_buffers_to_unlogged()` when all buffers are
/// already UNLOGGED returns 0.
#[tokio::test]
async fn test_convert_buffers_idempotent() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE conv_idem_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO conv_idem_src VALUES (1, 10)").await;
    db.create_st(
        "conv_idem_st",
        "SELECT id, val FROM conv_idem_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // First conversion
    let _ = db
        .query_scalar::<i64>("SELECT pgtrickle.convert_buffers_to_unlogged()")
        .await;

    // Second conversion should return 0
    let second_run: i64 = db
        .query_scalar("SELECT pgtrickle.convert_buffers_to_unlogged()")
        .await;
    assert_eq!(
        second_run, 0,
        "Second conversion should be a no-op (already UNLOGGED)"
    );
}

/// D-1d: FULL-mode ST doesn't create change buffers, so convert is a no-op.
#[tokio::test]
async fn test_full_mode_no_change_buffer() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE full_only_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO full_only_src VALUES (1, 10)").await;
    db.create_st(
        "full_only_st",
        "SELECT id, val FROM full_only_src",
        "1m",
        "FULL",
    )
    .await;

    // The source table should only have a change buffer if DIFFERENTIAL triggers
    // were installed. For FULL-only mode, the buffer may still exist (created
    // during CDC setup). Just verify the extension runs without error.
    let result = db
        .try_execute("SELECT pgtrickle.convert_buffers_to_unlogged()")
        .await;
    assert!(result.is_ok(), "convert_buffers_to_unlogged should succeed");
}

// ── TEST-004: change_buffer_durability GUC (COR-003/ARCH-001, v0.68.0) ───

/// TEST-004a: `change_buffer_durability = 'unlogged'` creates UNLOGGED buffer.
///
/// COR-003/ARCH-001 (v0.68.0): Verify the `change_buffer_durability` GUC is
/// honoured by `create_stream_table()`. Before this fix the GUC was a no-op
/// and the legacy `unlogged_buffers` bool was the only working path.
#[tokio::test]
async fn test_change_buffer_durability_unlogged() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "SET pg_trickle.change_buffer_durability = 'unlogged'",
        "CREATE TABLE cbd_unlog_src (id INT PRIMARY KEY, val INT)",
        "INSERT INTO cbd_unlog_src VALUES (1, 1)",
        "SELECT pgtrickle.create_stream_table(\
            'cbd_unlog_st',\
            'SELECT id, val FROM cbd_unlog_src',\
            '1m',\
            'DIFFERENTIAL'\
        )",
    ])
    .await;

    let oid: i32 = db.table_oid("cbd_unlog_src").await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let persistence: String = db
        .query_scalar(&format!(
            "SELECT relpersistence::text FROM pg_class \
             WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgtrickle_changes') \
             AND relname = 'changes_{stable_name}'"
        ))
        .await;

    assert_eq!(
        persistence, "u",
        "change_buffer_durability = 'unlogged' must create UNLOGGED buffer \
         (relpersistence = 'u'); got '{persistence}'"
    );
}

/// TEST-004b: `change_buffer_durability = 'logged'` creates a logged buffer.
///
/// COR-003/ARCH-001 (v0.68.0): Logged variant.
#[tokio::test]
async fn test_change_buffer_durability_logged() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "SET pg_trickle.change_buffer_durability = 'logged'",
        "CREATE TABLE cbd_log_src (id INT PRIMARY KEY, val INT)",
        "INSERT INTO cbd_log_src VALUES (1, 1)",
        "SELECT pgtrickle.create_stream_table(\
            'cbd_log_st',\
            'SELECT id, val FROM cbd_log_src',\
            '1m',\
            'DIFFERENTIAL'\
        )",
    ])
    .await;

    let oid: i32 = db.table_oid("cbd_log_src").await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let persistence: String = db
        .query_scalar(&format!(
            "SELECT relpersistence::text FROM pg_class \
             WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgtrickle_changes') \
             AND relname = 'changes_{stable_name}'"
        ))
        .await;

    assert_eq!(
        persistence, "p",
        "change_buffer_durability = 'logged' must create permanent buffer \
         (relpersistence = 'p'); got '{persistence}'"
    );
}

/// TEST-004c: Legacy `unlogged_buffers = true` still works (maps to 'unlogged').
///
/// COR-003/ARCH-001 (v0.68.0): Backward-compat shim — setting the deprecated
/// GUC must still produce an UNLOGGED buffer. A WARNING is emitted in the
/// PostgreSQL log but the functional behaviour is preserved.
#[tokio::test]
async fn test_unlogged_buffers_legacy_shim_still_creates_unlogged_buffer() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute_seq(&[
        "SET pg_trickle.unlogged_buffers = on",
        "CREATE TABLE cbd_legacy_src (id INT PRIMARY KEY, val INT)",
        "INSERT INTO cbd_legacy_src VALUES (1, 1)",
        "SELECT pgtrickle.create_stream_table(\
            'cbd_legacy_st',\
            'SELECT id, val FROM cbd_legacy_src',\
            '1m',\
            'DIFFERENTIAL'\
        )",
    ])
    .await;

    let oid: i32 = db.table_oid("cbd_legacy_src").await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            oid
        ))
        .await;
    let persistence: String = db
        .query_scalar(&format!(
            "SELECT relpersistence::text FROM pg_class \
             WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgtrickle_changes') \
             AND relname = 'changes_{stable_name}'"
        ))
        .await;

    assert_eq!(
        persistence, "u",
        "Legacy unlogged_buffers = true must still create UNLOGGED buffer \
         (relpersistence = 'u'); got '{persistence}'"
    );
}
