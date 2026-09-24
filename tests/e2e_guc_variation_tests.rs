//! E2E tests for GUC-variation differential correctness (F23: G8.1).
//!
//! Validates that differential refresh produces correct results under
//! different GUC configurations: block_source_ddl, use_prepared_statements,
//! merge_planner_hints, cleanup_use_truncate, merge_work_mem_mb,
//! max_grouping_set_branches, foreign_table_polling.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use std::sync::{Arc, LazyLock};

/// Mutex that serialises tests touching the `foreign_table_polling` GUC.
///
/// `ALTER SYSTEM` changes are cluster-wide (all databases in the shared
/// container), so two tests that each modify this GUC must not run in
/// parallel — one could observe the other's interim value.
static FOREIGN_TABLE_POLLING_LOCK: LazyLock<Arc<tokio::sync::Mutex<()>>> =
    LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(())));

/// Mutex that serialises tests touching the `max_grouping_set_branches` GUC.
static GROUPING_SET_BRANCHES_LOCK: LazyLock<Arc<tokio::sync::Mutex<()>>> =
    LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(())));

/// Helper: create a standard table, seed data, create ST, verify.
async fn setup_guc_test(db: &E2eDb) {
    db.execute("CREATE TABLE guc_src (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO guc_src (grp, val) VALUES \
         ('a', 10), ('a', 20), ('b', 30), ('b', 40), ('c', 50)",
    )
    .await;
}

const GUC_QUERY: &str = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM guc_src GROUP BY grp";

/// Helper: mutate then refresh and verify.
async fn mutate_and_verify(db: &E2eDb) {
    db.execute("INSERT INTO guc_src (grp, val) VALUES ('a', 5), ('d', 99)")
        .await;
    db.execute("UPDATE guc_src SET val = 100 WHERE grp = 'b' AND val = 30")
        .await;
    db.execute("DELETE FROM guc_src WHERE grp = 'c'").await;
    db.refresh_st("guc_st").await;
    db.assert_st_matches_query("guc_st", GUC_QUERY).await;
}

// ═══════════════════════════════════════════════════════════════════════
// use_prepared_statements = off
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guc_prepared_statements_off() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("SET pg_trickle.use_prepared_statements = off")
        .await;
    setup_guc_test(&db).await;
    db.create_st("guc_st", GUC_QUERY, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("guc_st", GUC_QUERY).await;
    mutate_and_verify(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════
// merge_planner_hints = off
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guc_merge_planner_hints_off() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("SET pg_trickle.merge_planner_hints = off").await;
    setup_guc_test(&db).await;
    db.create_st("guc_st", GUC_QUERY, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("guc_st", GUC_QUERY).await;
    mutate_and_verify(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════
// cleanup_use_truncate = off
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guc_cleanup_use_truncate_off() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("SET pg_trickle.cleanup_use_truncate = off")
        .await;
    setup_guc_test(&db).await;
    db.create_st("guc_st", GUC_QUERY, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("guc_st", GUC_QUERY).await;
    mutate_and_verify(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════
// merge_work_mem_mb non-default
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guc_merge_work_mem_mb_custom() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("SET pg_trickle.merge_work_mem_mb = 16").await;
    setup_guc_test(&db).await;
    db.create_st("guc_st", GUC_QUERY, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("guc_st", GUC_QUERY).await;
    mutate_and_verify(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════
// block_source_ddl = on — DDL blocked after ST creation
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guc_block_source_ddl_on() {
    let db = E2eDb::new().await.with_extension().await;
    setup_guc_test(&db).await;
    db.create_st("guc_st", GUC_QUERY, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("guc_st", GUC_QUERY).await;

    // The GUC defaults to true (on), so column-altering DDL should be
    // blocked out of the box.
    let result = db
        .try_execute("ALTER TABLE guc_src ADD COLUMN new_col TEXT")
        .await;
    assert!(
        result.is_err(),
        "DDL should be blocked when block_source_ddl=on (default)"
    );

    // Data DML should still work
    mutate_and_verify(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════
// differential_max_change_ratio = 0.0 (never fall back to FULL)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guc_differential_max_change_ratio_zero() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("SET pg_trickle.differential_max_change_ratio = 0.0")
        .await;
    setup_guc_test(&db).await;
    db.create_st("guc_st", GUC_QUERY, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("guc_st", GUC_QUERY).await;
    mutate_and_verify(&db).await;

    // Verify mode is still differential
    let (_, mode, _, _) = db.pgt_status("guc_st").await;
    assert_eq!(mode, "DIFFERENTIAL");
}

// ═══════════════════════════════════════════════════════════════════════
// Combined: multiple GUCs changed at once
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guc_combined_non_default() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("SET pg_trickle.use_prepared_statements = off")
        .await;
    db.execute("SET pg_trickle.merge_planner_hints = off").await;
    db.execute("SET pg_trickle.cleanup_use_truncate = off")
        .await;
    db.execute("SET pg_trickle.merge_work_mem_mb = 8").await;
    setup_guc_test(&db).await;
    db.create_st("guc_st", GUC_QUERY, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("guc_st", GUC_QUERY).await;
    mutate_and_verify(&db).await;
}

// ═══════════════════════════════════════════════════════════════════════
// EC-02: max_grouping_set_branches — CUBE/ROLLUP branch limit
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guc_max_grouping_set_branches_rejects_over_limit() {
    // CUBE(a, b, c) produces 2^3 = 8 branches.  Setting the GUC to 4
    // should cause creation to fail with a clear error.
    let _gs_lock = GROUPING_SET_BRANCHES_LOCK.lock().await;
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE gs_limit_src (a TEXT, b TEXT, c TEXT, val INT)")
        .await;

    // Use session-level SET on the same connection as create_stream_table so
    // the GUC is visible even in the Light-E2E environment (no
    // shared_preload_libraries — ALTER SYSTEM + reload is unreliable there).
    let result = db
        .try_execute_with_config(
            &["SET pg_trickle.max_grouping_set_branches = 4"],
            "SELECT pgtrickle.create_stream_table('gs_limit_st', \
             $$ SELECT a, b, c, SUM(val) FROM gs_limit_src GROUP BY CUBE (a, b, c) $$, \
             '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_err(),
        "CUBE(3) = 8 branches should exceed limit of 4"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("exceeds the limit"),
        "Error should mention branch limit, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_guc_max_grouping_set_branches_allows_within_limit() {
    // CUBE(a, b) produces 2^2 = 4 branches.  Limit of 4 should pass.
    let _gs_lock = GROUPING_SET_BRANCHES_LOCK.lock().await;
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE gs_ok_src (id SERIAL PRIMARY KEY, a TEXT, b TEXT, val INT)")
        .await;

    let result = db
        .try_execute_with_config(
            &["SET pg_trickle.max_grouping_set_branches = 4"],
            "SELECT pgtrickle.create_stream_table('gs_ok_st', \
             $$ SELECT a, b, SUM(val) FROM gs_ok_src GROUP BY CUBE (a, b) $$, \
             '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "CUBE(2) = 4 branches should be within limit of 4, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_guc_max_grouping_set_branches_raised_allows_large_cube() {
    // Raise the limit to 128 so CUBE(a, b, c, d, e) = 32 branches passes.
    let _gs_lock = GROUPING_SET_BRANCHES_LOCK.lock().await;
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE gs_big_src (id SERIAL PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, val INT)")
        .await;

    let result = db
        .try_execute_with_config(
            &["SET pg_trickle.max_grouping_set_branches = 128"],
            "SELECT pgtrickle.create_stream_table('gs_big_st', \
             $$ SELECT a, b, c, d, e, SUM(val) FROM gs_big_src \
             GROUP BY CUBE (a, b, c, d, e) $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "CUBE(5) = 32 branches should be within limit of 128, got: {:?}",
        result.err()
    );
}

// ═══════════════════════════════════════════════════════════════════════
// EC-05: foreign_table_polling — polling-based CDC for foreign tables
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guc_foreign_table_polling_off_rejects_differential() {
    // With polling disabled, foreign tables should be rejected
    // in DIFFERENTIAL mode.
    //
    // Acquire the cluster-wide GUC lock to prevent this test from running
    // concurrently with test_guc_foreign_table_polling_on_allows_differential,
    // which sets foreign_table_polling = on via ALTER SYSTEM.
    let _polling_lock = FOREIGN_TABLE_POLLING_LOCK.lock().await;
    let db = E2eDb::new().await.with_extension().await;
    let db_name: String = db.query_scalar("SELECT current_database()").await;

    // Set up a loopback foreign server via postgres_fdw.
    db.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .await;
    db.execute("CREATE TABLE ft_local_src (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute(&format!(
        "CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw \
         OPTIONS (dbname '{db_name}', host '127.0.0.1', port '5432')"
    ))
    .await;
    db.execute(
        "CREATE USER MAPPING FOR CURRENT_USER SERVER loopback \
         OPTIONS (user 'postgres')",
    )
    .await;
    db.execute(
        "CREATE FOREIGN TABLE ft_remote_src (id INT, val INT) \
         SERVER loopback OPTIONS (table_name 'ft_local_src')",
    )
    .await;

    // Pin the session setting on the connection that creates the stream table.
    let result = db
        .try_execute_with_config(
            &["SET pg_trickle.foreign_table_polling = off"],
            "SELECT pgtrickle.create_stream_table('ft_diff_st', \
             $$ SELECT id, val FROM ft_remote_src $$, '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        result.is_err(),
        "Foreign table in DIFFERENTIAL mode should be rejected when polling=off"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("foreign_table_polling"),
        "Error should suggest enabling polling GUC, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_guc_foreign_table_polling_full_mode_no_guc_needed() {
    // In FULL mode, foreign tables should always be accepted (no polling needed).
    let db = E2eDb::new().await.with_extension().await;
    let db_name: String = db.query_scalar("SELECT current_database()").await;

    db.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .await;
    db.execute("CREATE TABLE ft_full_src (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO ft_full_src (val) VALUES (10), (20), (30)")
        .await;
    db.execute(&format!(
        "CREATE SERVER loopback_full FOREIGN DATA WRAPPER postgres_fdw \
         OPTIONS (dbname '{db_name}', host '127.0.0.1', port '5432')"
    ))
    .await;
    db.execute(
        "CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_full \
         OPTIONS (user 'postgres')",
    )
    .await;
    db.execute(
        "CREATE FOREIGN TABLE ft_full_remote (id INT, val INT) \
         SERVER loopback_full OPTIONS (table_name 'ft_full_src')",
    )
    .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('ft_full_st', \
             $$ SELECT id, val FROM ft_full_remote $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "Foreign table in FULL mode should be accepted without polling GUC, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_guc_foreign_table_polling_on_allows_differential() {
    // With polling enabled, foreign tables should be accepted in DIFFERENTIAL mode
    // and the snapshot-based CDC should produce correct results.
    //
    // Acquire the cluster-wide GUC lock to prevent this test from running
    // concurrently with test_guc_foreign_table_polling_off_rejects_differential,
    // which relies on foreign_table_polling being off.
    let _polling_lock = FOREIGN_TABLE_POLLING_LOCK.lock().await;
    let db = E2eDb::new().await.with_extension().await;
    let db_name: String = db.query_scalar("SELECT current_database()").await;

    db.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .await;
    db.execute("CREATE TABLE ft_poll_src (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO ft_poll_src (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)")
        .await;
    db.execute(&format!(
        "CREATE SERVER loopback_poll FOREIGN DATA WRAPPER postgres_fdw \
         OPTIONS (dbname '{db_name}', host '127.0.0.1', port '5432')"
    ))
    .await;
    db.execute(
        "CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_poll \
         OPTIONS (user 'postgres')",
    )
    .await;
    db.execute(
        "CREATE FOREIGN TABLE ft_poll_remote (id INT, grp TEXT, val INT) \
         SERVER loopback_poll OPTIONS (table_name 'ft_poll_src')",
    )
    .await;

    // Enable polling-based CDC (cluster-wide so it applies across pool connections).
    db.alter_system_set_and_wait("pg_trickle.foreign_table_polling", "on", "on")
        .await;

    let query = "SELECT grp, SUM(val) AS total FROM ft_poll_remote GROUP BY grp";
    db.create_st("ft_poll_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("ft_poll_st", query).await;

    // Mutate the underlying local table (which the foreign table points to).
    db.execute("INSERT INTO ft_poll_src (grp, val) VALUES ('c', 50), ('a', 5)")
        .await;
    db.execute("DELETE FROM ft_poll_src WHERE grp = 'b'").await;
    db.refresh_st("ft_poll_st").await;
    db.assert_st_matches_query("ft_poll_st", query).await;

    db.alter_system_reset_and_wait("pg_trickle.foreign_table_polling", "off")
        .await;
}
