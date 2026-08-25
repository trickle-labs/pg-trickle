//! Integration tests that replace the former `#[pg_test]` tests.
//!
//! These tests verify that the pg_trickle catalog schema, tables, views, and
//! hash function logic work correctly inside a real PostgreSQL 18.3
//! container managed by Testcontainers.
//!
//! Replaces the previous `#[pg_test]` tests from `lib.rs`:
//! - test_extension_loads          → test_pg_trickle_schema_exists
//! - test_pg_trickle_hash_function_exists → test_xxhash_deterministic
//! - test_pg_trickle_hash_deterministic  → test_xxhash_deterministic
//! - test_catalog_tables_exist     → test_all_catalog_objects_exist

mod common;

use common::TestDb;

// ── Schema / Catalog Existence ─────────────────────────────────────────────

/// Equivalent of the old `test_extension_loads` pg_test.
/// Verifies the pg_trickle schema and core infrastructure are created.
#[tokio::test]
async fn test_pg_trickle_schema_exists() {
    let db = TestDb::with_catalog().await;

    let pg_trickle_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
        )
        .await;
    assert!(pg_trickle_exists, "pg_trickle schema should exist");

    let changes_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle_changes')",
        )
        .await;
    assert!(changes_exists, "pgtrickle_changes schema should exist");
}

/// Equivalent of the old `test_catalog_tables_exist` pg_test.
/// Verifies all catalog tables, indexes, and views are present.
#[tokio::test]
async fn test_all_catalog_objects_exist() {
    let db = TestDb::with_catalog().await;

    // Tables
    let tables = [
        ("pgtrickle", "pgt_stream_tables"),
        ("pgtrickle", "pgt_dependencies"),
        ("pgtrickle", "pgt_refresh_history"),
        ("pgtrickle", "pgt_change_tracking"),
    ];

    for (schema, table) in tables {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = '{}' AND table_name = '{}')",
                schema, table
            ))
            .await;
        assert!(exists, "Table {}.{} should exist", schema, table);
    }

    // View
    let view_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.views \
             WHERE table_schema = 'pgtrickle' AND table_name = 'stream_tables_info')",
        )
        .await;
    assert!(
        view_exists,
        "pgtrickle.stream_tables_info view should exist"
    );

    // Indexes (check key ones via pg_indexes)
    let idx_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_indexes \
             WHERE schemaname = 'pgtrickle' AND tablename = 'pgt_stream_tables'",
        )
        .await;
    // At least: PK, idx_pgt_status, idx_pgt_name
    assert!(
        idx_count >= 3,
        "Expected at least 3 indexes on pgtrickle.pgt_stream_tables, found {}",
        idx_count
    );
}

// ── Hash Function Logic ────────────────────────────────────────────────────

/// Equivalent of `test_pg_trickle_hash_function_exists` and
/// `test_pg_trickle_hash_deterministic` pg_tests.
///
/// Since the `pgtrickle.pg_trickle_hash()` SQL function is a pgrx-generated wrapper
/// around `xxhash_rust::xxh64`, we test the PostgreSQL-native `hashtext()`
/// as the hash mechanism used in our storage tables, and independently
/// verify that xxh64 is deterministic.
#[tokio::test]
async fn test_xxhash_deterministic() {
    // Verify xxh64 directly (the Rust implementation used by pg_trickle_hash)
    use xxhash_rust::xxh64;
    let h1 = xxh64::xxh64(b"hello", 0) as i64;
    let h2 = xxh64::xxh64(b"hello", 0) as i64;
    assert_eq!(h1, h2, "xxh64 should be deterministic");
    assert_ne!(h1, 0, "xxh64 should produce a non-zero hash for 'hello'");

    // Different inputs should produce different hashes
    let h3 = xxh64::xxh64(b"world", 0) as i64;
    assert_ne!(h1, h3, "Different inputs should produce different hashes");
}

/// Verify that PostgreSQL's hashtext (used in the refresh SQL) also
/// works as a fallback hashing mechanism inside a real container.
#[tokio::test]
async fn test_pg_hashtext_works_in_container() {
    let db = TestDb::new().await;

    let h1: i32 = db.query_scalar("SELECT hashtext('hello')").await;
    let h2: i32 = db.query_scalar("SELECT hashtext('hello')").await;
    assert_eq!(h1, h2, "hashtext should be deterministic");

    let h3: i32 = db.query_scalar("SELECT hashtext('world')").await;
    assert_ne!(h1, h3, "Different inputs should produce different hashes");
}

// ── Column & Constraint Verification ───────────────────────────────────────

/// Verify the pgt_stream_tables table has all expected columns including
/// the Phase 8 frontier column.
#[tokio::test]
async fn test_stream_tables_columns() {
    let db = TestDb::with_catalog().await;

    let expected_columns = [
        "pgt_id",
        "pgt_relid",
        "pgt_name",
        "pgt_schema",
        "defining_query",
        "schedule",
        "refresh_mode",
        "status",
        "is_populated",
        "data_timestamp",
        "frontier",
        "last_refresh_at",
        "consecutive_errors",
        "needs_reinit",
        "created_at",
        "updated_at",
    ];

    for col in expected_columns {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'pgtrickle' AND table_name = 'pgt_stream_tables' \
                 AND column_name = '{}')",
                col
            ))
            .await;
        assert!(
            exists,
            "Column pgtrickle.pgt_stream_tables.{} should exist",
            col
        );
    }
}

/// Verify that the frontier column accepts valid JSONB and stores/retrieves it.
#[tokio::test]
async fn test_frontier_jsonb_column() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE fsrc (id INT PRIMARY KEY)").await;
    let oid: i32 = db.query_scalar("SELECT 'fsrc'::regclass::oid::int").await;

    // Insert a ST with a frontier
    let frontier_json = r#"{"sources":{"12345":{"lsn":"0/1A2B","snapshot_ts":"2026-02-17T10:00:00Z"}},"data_timestamp":"2026-02-17T10:00:00Z"}"#;
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables \
         (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, refresh_mode, frontier) \
         VALUES ({}, 'frontier_st', 'public', 'SELECT 1', 'public', 'FULL', '{}'::jsonb)",
        oid,
        frontier_json.replace('\'', "''")
    ))
    .await;

    // Read it back
    let stored: serde_json::Value = db
        .query_scalar(
            "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'frontier_st'",
        )
        .await;

    assert_eq!(
        stored["data_timestamp"].as_str(),
        Some("2026-02-17T10:00:00Z")
    );
    assert_eq!(stored["sources"]["12345"]["lsn"].as_str(), Some("0/1A2B"));

    // Update the frontier
    let new_frontier = r#"{"sources":{"12345":{"lsn":"0/3C4D","snapshot_ts":"2026-02-17T11:00:00Z"}},"data_timestamp":"2026-02-17T11:00:00Z"}"#;
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = '{}'::jsonb WHERE pgt_name = 'frontier_st'",
        new_frontier.replace('\'', "''")
    ))
    .await;

    let updated: serde_json::Value = db
        .query_scalar(
            "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'frontier_st'",
        )
        .await;
    assert_eq!(updated["sources"]["12345"]["lsn"].as_str(), Some("0/3C4D"));
}

// ── Logical Decoding Prerequisites ─────────────────────────────────────────

/// Verify the container supports pg_lsn type (needed for change buffer tables).
#[tokio::test]
async fn test_pg_lsn_type_works() {
    let db = TestDb::new().await;

    let lsn: String = db.query_scalar("SELECT '0/1A2B3C4D'::pg_lsn::text").await;
    assert_eq!(lsn, "0/1A2B3C4D");

    // LSN comparison
    let gt: bool = db
        .query_scalar("SELECT '0/2'::pg_lsn > '0/1'::pg_lsn")
        .await;
    assert!(gt, "LSN 0/2 should be > 0/1");
}

// ── Column Type Verification ───────────────────────────────────────────────

/// Verify that the catalog table columns have the expected data types.
///
/// Column existence tests (test_stream_tables_columns etc.) only check that
/// a column *exists* — a column named `pgt_status` of type `integer` instead
/// of `text` would still pass. This test pins both the name and the data type
/// to catch schema drift between the mock DDL and expectations.
#[tokio::test]
async fn test_stream_tables_column_types() {
    let db = TestDb::with_catalog().await;

    // Fetch (column_name, data_type) pairs from information_schema
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, data_type
         FROM information_schema.columns
         WHERE table_schema = 'pgtrickle' AND table_name = 'pgt_stream_tables'
         ORDER BY ordinal_position",
    )
    .fetch_all(&db.pool)
    .await
    .expect("information_schema query failed");

    let col_map: std::collections::HashMap<String, String> = rows.into_iter().collect();

    // Key columns and their expected information_schema data_type values
    let expected: &[(&str, &str)] = &[
        ("pgt_name", "text"),
        ("pgt_schema", "text"),
        ("defining_query", "text"),
        ("original_query", "text"),
        ("schedule", "text"),
        ("refresh_mode", "text"),
        ("status", "text"),
        ("is_populated", "boolean"),
        ("consecutive_errors", "integer"),
        ("needs_reinit", "boolean"),
        ("frontier", "jsonb"),
        ("topk_limit", "integer"),
        ("topk_order_by", "text"),
        ("requested_cdc_mode", "text"),
    ];

    for (col_name, expected_type) in expected {
        let actual = col_map
            .get(*col_name)
            .unwrap_or_else(|| panic!("column '{}' missing from pgt_stream_tables", col_name));
        assert_eq!(
            actual.as_str(),
            *expected_type,
            "column '{}': expected type '{}', got '{}'",
            col_name,
            expected_type,
            actual
        );
    }
}

/// Verify that pgt_refresh_history columns have the expected data types.
#[tokio::test]
async fn test_refresh_history_column_types() {
    let db = TestDb::with_catalog().await;

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, data_type
         FROM information_schema.columns
         WHERE table_schema = 'pgtrickle' AND table_name = 'pgt_refresh_history'
         ORDER BY ordinal_position",
    )
    .fetch_all(&db.pool)
    .await
    .expect("information_schema query failed");

    let col_map: std::collections::HashMap<String, String> = rows.into_iter().collect();

    let expected: &[(&str, &str)] = &[
        ("action", "text"),
        ("status", "text"),
        ("rows_inserted", "bigint"),
        ("rows_deleted", "bigint"),
        ("error_message", "text"),
        ("initiated_by", "text"),
    ];

    for (col_name, expected_type) in expected {
        let actual = col_map
            .get(*col_name)
            .unwrap_or_else(|| panic!("column '{}' missing from pgt_refresh_history", col_name));
        assert_eq!(
            actual.as_str(),
            *expected_type,
            "column '{}': expected type '{}', got '{}'",
            col_name,
            expected_type,
            actual
        );
    }
}
