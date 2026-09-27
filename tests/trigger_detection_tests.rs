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
// Mirrors the query in src/cdc/rebuild.rs `has_user_triggers()`:
//   SELECT EXISTS(
//     SELECT 1 FROM pg_trigger
//     WHERE tgrelid = $OID AND NOT tgisinternal
//       AND NOT EXISTS (... pgtrickle IVM maintenance function identity ...)
//   )

const HAS_USER_TRIGGERS_SQL: &str = r#"
    SELECT EXISTS(
        SELECT 1 FROM pg_catalog.pg_trigger tr
        WHERE tr.tgrelid = $1::oid
          AND NOT tr.tgisinternal
          AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc fn
            JOIN pg_catalog.pg_namespace ns ON ns.oid = fn.pronamespace
            WHERE fn.oid = tr.tgfoid
              AND ns.nspname = 'pgtrickle'
              AND fn.proname::text ~
                '^(pgt_ivm_before|pgt_ivm_after_(ins|upd|del|trunc))_fn_[0-9]+_[0-9]+$'
          )
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
async fn test_trigger_detection_detects_pgt_prefixed_application_trigger() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_pgs (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    // A name prefix alone does not make an application trigger internal.
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
        has_triggers,
        "pgt_-prefixed application trigger should be detected"
    );
}

#[tokio::test]
async fn test_trigger_detection_detects_statement_level() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_stmt (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_stmt_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql",
    )
    .await;
    // Application statement triggers also select the explicit DML path.
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
        has_triggers,
        "statement-level application trigger should be detected"
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

    // A prefix alone does not identify a pg_trickle maintenance trigger.
    db.execute(
        "CREATE TRIGGER pgt_internal AFTER INSERT ON detect_mixed
         FOR EACH ROW EXECUTE FUNCTION noop_fn()",
    )
    .await;
    // Statement-level application triggers are included too.
    db.execute(
        "CREATE TRIGGER stmt_trig AFTER INSERT ON detect_mixed
         FOR EACH STATEMENT EXECUTE FUNCTION noop_stmt_fn()",
    )
    .await;
    // User row-level trigger should also be detected.
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
        "Should detect application triggers among mixed triggers"
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

/// Application triggers can use extension-looking names and must still count.
#[tokio::test]
async fn test_trigger_detection_detects_pg_trickle_prefixed_application_trigger() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_trickle (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    // A matching trigger name attached to an application function is user code.
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
        has_triggers,
        "pg_trickle_-prefixed application trigger should be detected"
    );
}

/// Only actual maintenance function identity, not its trigger name, is excluded.
#[tokio::test]
async fn test_trigger_detection_excludes_ivm_function_identity() {
    let db = TestDb::new().await;

    db.execute("CREATE TABLE detect_all (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION pgtrickle.pgt_ivm_after_ins_fn_1_99999() RETURNS trigger AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    db.execute(
        "CREATE TRIGGER pgt_ivm_after_ins_1 AFTER INSERT ON detect_all
         FOR EACH ROW EXECUTE FUNCTION pgtrickle.pgt_ivm_after_ins_fn_1_99999()",
    )
    .await;

    let oid: i32 = db
        .query_scalar("SELECT 'detect_all'::regclass::oid::int")
        .await;

    let has_triggers: bool = sqlx::query_scalar(HAS_USER_TRIGGERS_SQL)
        .bind(oid)
        .fetch_one(&db.pool)
        .await
        .expect("query failed");
    assert!(
        !has_triggers,
        "the IVM maintenance function should be excluded"
    );

    db.execute(
        "CREATE OR REPLACE FUNCTION noop_fn() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    // Even an extension-looking application trigger is detected.
    db.execute(
        "CREATE TRIGGER pgt_change_buffer AFTER UPDATE ON detect_all
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
        "application function identity should be detected despite trigger name"
    );
}
