//! Integration tests for user-trigger detection logic.
//!
//! Validates the SQL queries used by `has_user_triggers()` and the trigger
//! detection plumbing against a real PostgreSQL 18.6 container. These tests
//! do NOT require the pg_trickle extension — they exercise the raw SQL
//! patterns used internally.
//!
//! Prerequisites: Testcontainers + Docker

mod common;

use common::TestDb;

// ── Trigger detection query ────────────────────────────────────────────
//
// Mirrors the query in src/cdc.rs `has_user_triggers()`:
//   SELECT EXISTS(
//     SELECT 1 FROM pg_trigger
//     WHERE tgrelid = $OID
//       AND NOT tgisinternal
//       AND tgname NOT LIKE 'pgt_%'
//       AND tgtype & 1 = 1  -- ROW-level trigger
//   )

const HAS_USER_TRIGGERS_SQL: &str = r#"
    SELECT EXISTS(
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = $1::oid
          AND NOT tgisinternal
          AND tgname NOT LIKE 'pgt_%'
          AND tgname NOT LIKE 'pg_trickle_%'
          AND tgtype & 1 = 1
    )
"#;

#[tokio::test]
async fn test_trigger_detection_no_triggers() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_empty (id INT PRIMARY KEY, val TEXT)")
        .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_empty'::regclass::oid::int")
        .await;

    let has_triggers: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("trigger detection query failed");

    assert!(
        !has_triggers,
        "Table with no user triggers should return false"
    );
}

#[tokio::test]
async fn test_trigger_detection_with_user_trigger() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_trig (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    db.execute(
        "CREATE TRIGGER user_trig AFTER INSERT ON detect_trig
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_trig'::regclass::oid::int")
        .await;

    let has_triggers: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("trigger detection query failed");

    assert!(has_triggers, "Table with user trigger should return true");
}

#[tokio::test]
async fn test_trigger_detection_ignores_pgt_prefix() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_pgs (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    // Trigger with pgt_ prefix should be ignored (internal pg_trickle triggers)
    db.execute(
        "CREATE TRIGGER pgt_cdc_trigger AFTER INSERT ON detect_pgs
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_pgs'::regclass::oid::int")
        .await;

    let has_triggers: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("trigger detection query failed");

    assert!(
        !has_triggers,
        "pgt_-prefixed triggers should be excluded from detection"
    );
}

#[tokio::test]
async fn test_trigger_detection_ignores_statement_level() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_stmt (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_stmt_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql",
    )
    .await;
    // Statement-level trigger (FOR EACH STATEMENT) should NOT be detected
    // because we check tgtype & 1 = 1 (ROW-level flag)
    db.execute(
        "CREATE TRIGGER stmt_trig AFTER INSERT ON detect_stmt
         FOR EACH STATEMENT EXECUTE FUNCTION noop_stmt_fn()",
    )
    .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_stmt'::regclass::oid::int")
        .await;

    let has_triggers: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("trigger detection query failed");

    assert!(
        !has_triggers,
        "Statement-level triggers should not be detected as user row-level triggers"
    );
}

#[tokio::test]
async fn test_trigger_detection_mixed_triggers() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_mixed (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_stmt_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql",
    )
    .await;

    // Internal pg_trickle trigger (should be ignored)
    db.execute(
        "CREATE TRIGGER pgt_internal AFTER INSERT ON detect_mixed
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;
    // Statement-level trigger (should be ignored)
    db.execute(
        "CREATE TRIGGER stmt_trig AFTER INSERT ON detect_mixed
         FOR EACH STATEMENT EXECUTE FUNCTION noop_stmt_fn()",
    )
    .await;
    // User row-level trigger (should be detected)
    db.execute(
        "CREATE TRIGGER user_row_trig AFTER UPDATE ON detect_mixed
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_mixed'::regclass::oid::int")
        .await;

    let has_triggers: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("trigger detection query failed");

    assert!(
        has_triggers,
        "Should detect the user row-level trigger among mixed triggers"
    );
}

#[tokio::test]
async fn test_trigger_detection_before_trigger() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_before (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    // BEFORE triggers should also be detected (they are row-level)
    db.execute(
        "CREATE TRIGGER before_trig BEFORE UPDATE ON detect_before
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_before'::regclass::oid::int")
        .await;

    let has_triggers: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("trigger detection query failed");

    assert!(has_triggers, "BEFORE row-level triggers should be detected");
}

#[tokio::test]
async fn test_trigger_detection_after_drop() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_drop (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    db.execute(
        "CREATE TRIGGER drop_trig AFTER INSERT ON detect_drop
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_drop'::regclass::oid::int")
        .await;

    // Should detect trigger
    let before: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("query failed");
    assert!(before, "Should detect trigger before drop");

    // Drop the trigger
    db.execute("DROP TRIGGER drop_trig ON detect_drop").await;

    // Should no longer detect trigger
    let after: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("query failed");
    assert!(!after, "Should not detect trigger after drop");
}

/// Regression: `pg_trickle_%` prefixed triggers (CDC triggers installed by
/// the extension) must NOT be counted as user triggers. Before the fix,
/// only `pgt_%` was excluded, so `pg_trickle_cdc_<oid>` triggers were
/// incorrectly detected as user triggers, forcing the slower explicit-DML
/// refresh path.
#[tokio::test]
async fn test_trigger_detection_ignores_pg_trickle_prefix() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_trickle (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    // Simulate the CDC trigger name pattern used by pg_trickle
    db.execute(
        "CREATE TRIGGER pg_trickle_cdc_12345 AFTER INSERT ON detect_trickle
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_trickle'::regclass::oid::int")
        .await;

    let has_triggers: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("trigger detection query failed");

    assert!(
        !has_triggers,
        "pg_trickle_-prefixed triggers should be excluded from user trigger detection"
    );
}

/// Regression: Both `pgt_%` AND `pg_trickle_%` triggers should be excluded,
/// while a real user trigger alongside them is still detected.
#[tokio::test]
async fn test_trigger_detection_mixed_internal_and_user() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_all (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    // Internal: pgt_* prefix
    db.execute(
        "CREATE TRIGGER pgt_change_buffer AFTER INSERT ON detect_all
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;
    // Internal: pg_trickle_* prefix (CDC)
    db.execute(
        "CREATE TRIGGER pg_trickle_cdc_99999 AFTER INSERT ON detect_all
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_all'::regclass::oid::int")
        .await;

    // With only internal triggers, should be false
    let has_triggers: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("query failed");
    assert!(
        !has_triggers,
        "Only internal triggers present — should not detect user triggers"
    );

    // Add a real user trigger
    db.execute(
        "CREATE TRIGGER audit_log_trigger AFTER UPDATE ON detect_all
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;

    let has_triggers_with_user: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("query failed");
    assert!(
        has_triggers_with_user,
        "Real user trigger should be detected alongside internal triggers"
    );
}
