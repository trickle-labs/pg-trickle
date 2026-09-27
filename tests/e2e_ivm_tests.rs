//! E2E tests for IMMEDIATE-mode (Transactional IVM) stream tables.
//!
//! Validates that IMMEDIATE stream tables:
//! - Are maintained synchronously within the same transaction as DML.
//! - Handle INSERT, UPDATE, DELETE, and TRUNCATE correctly.
//! - Support window functions and scalar subqueries.
//! - Reject unsupported features (TopK, recursive CTEs).
//! - Cascade through dependent IMMEDIATE stream tables.
//! - Handle concurrent inserts correctly.
//! - Clean up properly on DROP.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use sqlx::PgPool;
use std::time::Duration;

// ── Helper ─────────────────────────────────────────────────────────────

/// Create an IMMEDIATE-mode stream table (schedule = NULL).
async fn create_immediate_st(db: &E2eDb, name: &str, query: &str) {
    let sql = format!(
        "SELECT pgtrickle.create_stream_table('{name}', $${query}$$, \
         NULL, 'IMMEDIATE')"
    );
    db.execute(&sql).await;
}

async fn table_rows_json(pool: &PgPool, relation: &str) -> String {
    let query = format!(
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text), \
         '[]'::jsonb)::text FROM {relation} AS t"
    );
    sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(query))
        .fetch_one(pool)
        .await
        .expect("snapshot table rows")
}

async fn table_snapshots(pool: &PgPool, relations: &[String]) -> Vec<String> {
    let mut snapshots = Vec::with_capacity(relations.len());
    for relation in relations {
        snapshots.push(table_rows_json(pool, relation).await);
    }
    snapshots
}

async fn fresh_verification_pool(db: &E2eDb) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(db.connection_string())
        .await
        .expect("open a fresh connection for rollback-state verification")
}

async fn frontier_snapshot(pool: &PgPool, streams: &[&str]) -> String {
    let streams: Vec<String> = streams.iter().map(|stream| (*stream).to_owned()).collect();
    sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(jsonb_build_array(pgt_name::text, frontier::text) \
         ORDER BY pgt_name), '[]'::jsonb)::text \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name::text = ANY($1)",
    )
    .bind(streams)
    .fetch_one(pool)
    .await
    .expect("snapshot stream frontiers")
}

async fn trigger_state_snapshot(pool: &PgPool, relation: &str) -> String {
    sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(jsonb_build_array(tgname::text, tgenabled::text) \
         ORDER BY tgname), '[]'::jsonb)::text \
         FROM pg_catalog.pg_trigger WHERE tgrelid = $1::regclass AND NOT tgisinternal",
    )
    .bind(relation)
    .fetch_one(pool)
    .await
    .expect("snapshot trigger modes")
}

fn assert_check_constraint_error(error: sqlx::Error, constraint: &str) {
    let database_error = error
        .as_database_error()
        .expect("injected capture failure must be a PostgreSQL error");
    assert_eq!(
        database_error.code().as_deref(),
        Some("23514"),
        "expected CHECK violation {constraint}, got: {}",
        database_error.message()
    );
    assert!(
        database_error.constraint() == Some(constraint)
            || database_error.message().contains(constraint),
        "expected CHECK constraint {constraint}, got: {}",
        database_error.message()
    );
}

fn assert_st_matches_sql(st_name: &str, columns: &str, expected_query: &str) -> String {
    format!(
        r#"DO $assert$
        DECLARE same_rows boolean;
        BEGIN
            WITH expected AS MATERIALIZED ({expected_query}),
                 actual AS MATERIALIZED (SELECT {columns} FROM public.{st_name})
            SELECT NOT EXISTS (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
               AND NOT EXISTS (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
              INTO same_rows;
            IF NOT same_rows THEN
                RAISE EXCEPTION 'public.{st_name} does not match its defining query';
            END IF;
        END
        $assert$"#
    )
}

async fn assert_st_matches_after_reconnect(
    db: &E2eDb,
    st_name: &str,
    columns: &str,
    expected_query: &str,
) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(db.connection_string())
        .await
        .expect("open a fresh connection for committed-state check");
    let query = format!(
        "WITH expected AS MATERIALIZED ({expected_query}), \
         actual AS MATERIALIZED (SELECT {columns} FROM public.{st_name}) \
         SELECT NOT EXISTS (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) \
            AND NOT EXISTS (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)"
    );
    let same_rows = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(query))
        .fetch_one(&pool)
        .await
        .expect("compare committed stream table contents");
    assert!(
        same_rows,
        "public.{st_name} differs after a fresh connection"
    );
    pool.close().await;
}

// ── Basic Creation ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_create_simple_select() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO orders VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
        .await;

    let query = "SELECT id, customer, amount FROM orders";
    create_immediate_st(&db, "order_imm", query).await;
    db.assert_st_matches_query("order_imm", query).await;

    // Verify catalog entry
    let (status, mode, populated, errors) = db.pgt_status("order_imm").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated, "ST should be populated after create");
    assert_eq!(errors, 0);

    // Verify initial data
    let count = db.count("public.order_imm").await;
    assert_eq!(count, 2, "ST should contain 2 rows after initial populate");

    // Check schedule is NULL for IMMEDIATE
    let schedule_is_null: bool = db
        .query_scalar(
            "SELECT schedule IS NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'order_imm'",
        )
        .await;
    assert!(schedule_is_null, "IMMEDIATE ST should have NULL schedule");
}

// ── INSERT Propagation ─────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_insert_propagates_immediately() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price NUMERIC)")
        .await;
    db.execute("INSERT INTO products VALUES (1, 'Widget', 10.00)")
        .await;

    let query = "SELECT id, name, price FROM products";
    create_immediate_st(&db, "product_imm", query).await;
    db.assert_st_matches_query("product_imm", query).await;

    let count_before = db.count("public.product_imm").await;
    assert_eq!(count_before, 1);

    // Insert a new row — should immediately appear in the ST.
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 25.00)")
        .await;

    let count_after = db.count("public.product_imm").await;
    assert_eq!(
        count_after, 2,
        "ST should have 2 rows after INSERT on base table"
    );

    // Verify the new value
    let gadget_price: String = db
        .query_scalar("SELECT price::text FROM public.product_imm WHERE name = 'Gadget'")
        .await;
    assert_eq!(gadget_price, "25.00");
    db.assert_st_matches_query("product_imm", query).await;
}

#[tokio::test]
async fn test_ivm_multi_row_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE items (id INT PRIMARY KEY, val TEXT)")
        .await;

    let query = "SELECT id, val FROM items";
    create_immediate_st(&db, "items_imm", query).await;
    db.assert_st_matches_query("items_imm", query).await;

    // Insert multiple rows in one statement.
    db.execute("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    let count = db.count("public.items_imm").await;
    assert_eq!(count, 3, "ST should have 3 rows after multi-row INSERT");
}

// ── UPDATE Propagation ─────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_update_propagates_immediately() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE inventory (id INT PRIMARY KEY, product TEXT, qty INT)")
        .await;
    db.execute("INSERT INTO inventory VALUES (1, 'Bolts', 100), (2, 'Nuts', 200)")
        .await;

    let query = "SELECT id, product, qty FROM inventory";
    create_immediate_st(&db, "inv_imm", query).await;
    db.assert_st_matches_query("inv_imm", query).await;

    // Update a row.
    db.execute("UPDATE inventory SET qty = 150 WHERE id = 1")
        .await;

    let new_qty: i32 = db
        .query_scalar("SELECT qty FROM public.inv_imm WHERE product = 'Bolts'")
        .await;
    assert_eq!(new_qty, 150, "ST should reflect UPDATE immediately");

    // Unchanged row should remain.
    let nuts_qty: i32 = db
        .query_scalar("SELECT qty FROM public.inv_imm WHERE product = 'Nuts'")
        .await;
    assert_eq!(nuts_qty, 200, "Non-updated row should be unchanged");
}

// ── DELETE Propagation ─────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_delete_propagates_immediately() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE tasks (id INT PRIMARY KEY, title TEXT)")
        .await;
    db.execute("INSERT INTO tasks VALUES (1, 'Task A'), (2, 'Task B'), (3, 'Task C')")
        .await;

    let query = "SELECT id, title FROM tasks";
    create_immediate_st(&db, "tasks_imm", query).await;
    db.assert_st_matches_query("tasks_imm", query).await;

    let count_before = db.count("public.tasks_imm").await;
    assert_eq!(count_before, 3);

    // Delete a row.
    db.execute("DELETE FROM tasks WHERE id = 2").await;

    let count_after = db.count("public.tasks_imm").await;
    assert_eq!(
        count_after, 2,
        "ST should have 2 rows after DELETE on base table"
    );

    // Verify the deleted row is gone.
    let has_b: i64 = db
        .query_scalar("SELECT count(*) FROM public.tasks_imm WHERE title = 'Task B'")
        .await;
    assert_eq!(has_b, 0, "Deleted row should not be in ST");
}

// ── TRUNCATE Handling ──────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_truncate_clears_and_repopulates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE logs (id INT PRIMARY KEY, msg TEXT)")
        .await;
    db.execute("INSERT INTO logs VALUES (1, 'Entry 1'), (2, 'Entry 2')")
        .await;

    let query = "SELECT id, msg FROM logs";
    create_immediate_st(&db, "logs_imm", query).await;
    db.assert_st_matches_query("logs_imm", query).await;
    assert_eq!(db.count("public.logs_imm").await, 2);

    // TRUNCATE the base table — ST should be emptied.
    db.execute("TRUNCATE logs").await;

    let count = db.count("public.logs_imm").await;
    assert_eq!(count, 0, "ST should be empty after base table TRUNCATE");
}

#[tokio::test]
async fn test_ivm_suspended_writes_preserve_state_and_resume_rebuilds() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ivm_paused_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO ivm_paused_src VALUES (1, 'one')")
        .await;
    let query = "SELECT id, val FROM ivm_paused_src";
    create_immediate_st(&db, "ivm_paused_st", query).await;

    db.execute("SELECT pgtrickle.pause_stream_table('ivm_paused_st')")
        .await;
    db.execute("INSERT INTO ivm_paused_src VALUES (2, 'two')")
        .await;
    db.execute("UPDATE ivm_paused_src SET val = 'changed' WHERE id = 1")
        .await;
    db.execute("DELETE FROM ivm_paused_src WHERE id = 2").await;
    db.execute("TRUNCATE ivm_paused_src").await;
    db.execute("INSERT INTO ivm_paused_src VALUES (3, 'three')")
        .await;

    let (status, _, _, _) = db.pgt_status("ivm_paused_st").await;
    assert_eq!(status, "SUSPENDED");
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ivm_paused_st'",
        )
        .await;
    assert!(needs_reinit);
    assert_eq!(db.count("public.ivm_paused_st").await, 1);

    db.execute("SELECT pgtrickle.resume_stream_table('ivm_paused_st')")
        .await;
    db.assert_st_matches_query("ivm_paused_st", query).await;
    let (status, _, _, _) = db.pgt_status("ivm_paused_st").await;
    assert_eq!(status, "ACTIVE");
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ivm_paused_st'",
        )
        .await;
    assert!(!needs_reinit);

    db.execute("INSERT INTO ivm_paused_src VALUES (4, 'four')")
        .await;
    db.assert_st_matches_query("ivm_paused_st", query).await;
}

#[tokio::test]
async fn test_ivm_ambiguous_suspension_survives_delta_and_alter_resume() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ivm_ambiguous_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO ivm_ambiguous_src VALUES (1)").await;
    let query = "SELECT id FROM ivm_ambiguous_src";
    create_immediate_st(&db, "ivm_ambiguous_st", query).await;

    db.alter_st("ivm_ambiguous_st", "status => 'SUSPENDED'")
        .await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET refresh_reason = 'SOURCE_DEPENDENCY_AMBIGUOUS', last_error_message = 'dependency changed' WHERE pgt_name = 'ivm_ambiguous_st'")
        .await;
    db.execute("INSERT INTO ivm_ambiguous_src VALUES (2)").await;

    let (status, _, _, _) = db.pgt_status("ivm_ambiguous_st").await;
    assert_eq!(status, "SUSPENDED");
    let needs_reinit: bool = db
        .query_scalar("SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ivm_ambiguous_st'")
        .await;
    assert!(needs_reinit);
    let reason: String = db
        .query_scalar("SELECT refresh_reason::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ivm_ambiguous_st'")
        .await;
    assert_eq!(reason, "SOURCE_DEPENDENCY_AMBIGUOUS");
    let error: String = db
        .query_scalar("SELECT last_error_message FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ivm_ambiguous_st'")
        .await;
    assert_eq!(error, "dependency changed");

    db.alter_st("ivm_ambiguous_st", "status => 'ACTIVE'").await;
    db.assert_st_matches_query("ivm_ambiguous_st", query).await;
}

#[tokio::test]
async fn test_ivm_legacy_delta_skips_suspended_table() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ivm_legacy_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO ivm_legacy_src VALUES (1)").await;
    db.try_execute_with_config(
        &["SET pg_trickle.ivm_use_enr = off"],
        "SELECT pgtrickle.create_stream_table('ivm_legacy_st', 'SELECT id FROM ivm_legacy_src', NULL, 'IMMEDIATE')",
    )
    .await
    .expect("create legacy IVM stream table");

    db.execute("SELECT pgtrickle.pause_stream_table('ivm_legacy_st')")
        .await;
    db.execute("INSERT INTO ivm_legacy_src VALUES (2)").await;
    let (status, _, _, _) = db.pgt_status("ivm_legacy_st").await;
    assert_eq!(status, "SUSPENDED");
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ivm_legacy_st'",
        )
        .await;
    assert!(needs_reinit);

    db.execute("SELECT pgtrickle.resume_stream_table('ivm_legacy_st')")
        .await;
    db.assert_st_matches_query("ivm_legacy_st", "SELECT id FROM ivm_legacy_src")
        .await;
}

// ── DROP Cleanup ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_drop_cleans_up_triggers() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cleanup_test (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cleanup_test VALUES (1, 'x')").await;

    let query = "SELECT id, val FROM cleanup_test";
    create_immediate_st(&db, "cleanup_imm", query).await;
    db.assert_st_matches_query("cleanup_imm", query).await;

    // Drop the stream table.
    db.drop_st("cleanup_imm").await;

    // Verify catalog entry removed.
    let cat_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'cleanup_imm'",
        )
        .await;
    assert_eq!(cat_count, 0, "Catalog entry should be removed after DROP");

    // Verify IVM triggers are cleaned up — regular DML should work fine.
    db.execute("INSERT INTO cleanup_test VALUES (2, 'y')").await;
    let base_count: i64 = db.query_scalar("SELECT count(*) FROM cleanup_test").await;
    assert_eq!(base_count, 2, "Base table DML should work after ST drop");
}

// ── Validation Errors ──────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_topk_immediate_within_threshold() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE scores (id INT PRIMARY KEY, name TEXT, score INT)")
        .await;
    db.execute("INSERT INTO scores VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Carol', 70)")
        .await;

    // TopK (ORDER BY + LIMIT 10) is allowed in IMMEDIATE mode when within the
    // ivm_topk_max_limit threshold (default 1000). Uses inline micro-refresh.
    create_immediate_st(
        &db,
        "top_scores",
        "SELECT name, score FROM scores ORDER BY score DESC LIMIT 10",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("top_scores").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.top_scores").await, 3);
}

#[tokio::test]
async fn test_ivm_topk_captures_changes_for_deferred_child() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE topk_chain_src (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO topk_chain_src VALUES (1, 30), (2, 20), (3, 10)")
        .await;
    create_immediate_st(
        &db,
        "topk_chain_upstream",
        "SELECT id, score FROM topk_chain_src ORDER BY score DESC LIMIT 2",
    )
    .await;
    db.create_st(
        "topk_chain_child",
        "SELECT id, score FROM topk_chain_upstream",
        "5m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute("INSERT INTO topk_chain_src VALUES (4, 40)")
        .await;
    db.refresh_st_with_retry("topk_chain_child").await;
    db.assert_st_matches_query(
        "topk_chain_child",
        "SELECT id, score FROM topk_chain_src ORDER BY score DESC LIMIT 2",
    )
    .await;
}

// ── Manual Refresh ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_manual_refresh_does_full_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE refresh_test (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO refresh_test VALUES (1, 10), (2, 20)")
        .await;

    let query = "SELECT id, val FROM refresh_test";
    create_immediate_st(&db, "refresh_imm", query).await;
    db.assert_st_matches_query("refresh_imm", query).await;
    assert_eq!(db.count("public.refresh_imm").await, 2);

    // Manual refresh should work (does a full refresh)
    db.refresh_st("refresh_imm").await;

    let count = db.count("public.refresh_imm").await;
    assert_eq!(count, 2, "ST should still have 2 rows after manual refresh");
}

// ── Mixed Operations ───────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_mixed_operations_in_sequence() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, balance NUMERIC)")
        .await;

    let query = "SELECT id, name, balance FROM accounts";
    create_immediate_st(&db, "acct_imm", query).await;
    db.assert_st_matches_query("acct_imm", query).await;
    assert_eq!(db.count("public.acct_imm").await, 0);

    // INSERT
    db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000), (2, 'Bob', 2000)")
        .await;
    assert_eq!(db.count("public.acct_imm").await, 2);

    // UPDATE
    db.execute("UPDATE accounts SET balance = balance + 500 WHERE id = 1")
        .await;
    let alice_bal: String = db
        .query_scalar("SELECT balance::text FROM public.acct_imm WHERE name = 'Alice'")
        .await;
    assert_eq!(alice_bal, "1500");

    // DELETE
    db.execute("DELETE FROM accounts WHERE id = 2").await;
    assert_eq!(db.count("public.acct_imm").await, 1);

    // INSERT again
    db.execute("INSERT INTO accounts VALUES (3, 'Charlie', 3000)")
        .await;
    assert_eq!(db.count("public.acct_imm").await, 2);
}

// ── Mode Switching (alter_stream_table) ────────────────────────────────

#[tokio::test]
async fn test_ivm_alter_differential_to_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_d2i (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sw_d2i VALUES (1, 'a'), (2, 'b')")
        .await;

    // Start as DIFFERENTIAL.
    db.execute(
        "SELECT pgtrickle.create_stream_table('sw_d2i_st', \
         $$SELECT id, val FROM sw_d2i$$, '5m', 'DIFFERENTIAL')",
    )
    .await;

    let (_, mode, _, _) = db.pgt_status("sw_d2i_st").await;
    assert_eq!(mode, "DIFFERENTIAL");

    // Switch to IMMEDIATE.
    db.alter_st("sw_d2i_st", "refresh_mode => 'IMMEDIATE'")
        .await;

    let (status, mode, populated, _) = db.pgt_status("sw_d2i_st").await;
    assert_eq!(mode, "IMMEDIATE");
    assert_eq!(status, "ACTIVE");
    assert!(populated, "ST should be populated after mode switch");

    // Verify existing data is intact.
    assert_eq!(db.count("public.sw_d2i_st").await, 2);

    // Verify IVM triggers are active — INSERT should propagate immediately.
    db.execute("INSERT INTO sw_d2i VALUES (3, 'c')").await;
    assert_eq!(
        db.count("public.sw_d2i_st").await,
        3,
        "INSERT should propagate immediately after switch to IMMEDIATE"
    );
}

#[tokio::test]
async fn test_ivm_alter_stream_table_source_to_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_st_base (id INT PRIMARY KEY, val INT)")
        .await;
    create_immediate_st(&db, "sw_st_upstream", "SELECT id, val FROM sw_st_base").await;
    db.create_st(
        "sw_st_explicit",
        "SELECT id, val * 2 AS doubled FROM sw_st_upstream",
        "5m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "sw_st_target",
        "SELECT id, val * 3 AS tripled FROM sw_st_upstream",
        "5m",
        "DIFFERENTIAL",
    )
    .await;
    let upstream_pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'sw_st_upstream'",
        )
        .await;
    let buffer_name = format!("pgtrickle_changes.changes_pgt_{upstream_pgt_id}");
    let buffer_exists: bool = db
        .query_scalar(&format!("SELECT to_regclass('{buffer_name}') IS NOT NULL"))
        .await;
    assert!(buffer_exists);

    db.alter_st("sw_st_explicit", "refresh_mode => 'IMMEDIATE'")
        .await;
    db.alter_st("sw_st_target", "target_freshness => 'on_commit'")
        .await;

    let (_, explicit_mode, _, _) = db.pgt_status("sw_st_explicit").await;
    let (_, target_mode, _, _) = db.pgt_status("sw_st_target").await;
    assert_eq!(explicit_mode, "IMMEDIATE");
    assert_eq!(target_mode, "IMMEDIATE");
    let buffer_exists: bool = db
        .query_scalar(&format!("SELECT to_regclass('{buffer_name}') IS NOT NULL"))
        .await;
    assert!(
        !buffer_exists,
        "an upstream buffer with no deferred consumers should be removed"
    );

    db.execute("INSERT INTO sw_st_base VALUES (1, 10)").await;
    let doubled: i32 = db
        .query_scalar("SELECT doubled FROM sw_st_explicit WHERE id = 1")
        .await;
    let tripled: i32 = db
        .query_scalar("SELECT tripled FROM sw_st_target WHERE id = 1")
        .await;
    assert_eq!(doubled, 20);
    assert_eq!(tripled, 30);

    db.alter_st(
        "sw_st_explicit",
        "refresh_mode => 'DIFFERENTIAL', schedule => '10m'",
    )
    .await;
    let explicit_pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'sw_st_explicit'",
        )
        .await;
    let trigger_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pg_trigger \
             WHERE tgrelid = 'sw_st_upstream'::regclass \
               AND tgname LIKE 'pgt_ivm_%_{explicit_pgt_id}'"
        ))
        .await;
    assert_eq!(
        trigger_count, 0,
        "switching back to DIFFERENTIAL should remove downstream IVM triggers"
    );
    let buffer_exists: bool = db
        .query_scalar(&format!("SELECT to_regclass('{buffer_name}') IS NOT NULL"))
        .await;
    assert!(
        buffer_exists,
        "switching to DIFFERENTIAL should restore the upstream buffer"
    );
    db.execute("INSERT INTO sw_st_base VALUES (2, 20)").await;
    db.refresh_st_with_retry("sw_st_explicit").await;
    let doubled: i32 = db
        .query_scalar("SELECT doubled FROM sw_st_explicit WHERE id = 2")
        .await;
    assert_eq!(doubled, 40);
}

#[tokio::test]
async fn test_full_upstream_refresh_propagates_to_immediate_child_without_buffer() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE full_imm_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.create_st(
        "full_imm_upstream",
        "SELECT id, val FROM full_imm_src",
        "5m",
        "FULL",
    )
    .await;
    create_immediate_st(
        &db,
        "full_imm_child",
        "SELECT id, val * 2 AS doubled FROM full_imm_upstream",
    )
    .await;

    db.execute("INSERT INTO full_imm_src VALUES (1, 10)").await;
    db.refresh_st_with_retry("full_imm_upstream").await;
    let doubled: i32 = db
        .query_scalar("SELECT doubled FROM full_imm_child WHERE id = 1")
        .await;
    assert_eq!(doubled, 20);

    let upstream_pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'full_imm_upstream'",
        )
        .await;
    let buffer_exists: bool = db
        .query_scalar(&format!(
            "SELECT to_regclass('pgtrickle_changes.changes_pgt_{upstream_pgt_id}') IS NOT NULL"
        ))
        .await;
    assert!(
        !buffer_exists,
        "IMMEDIATE children must not allocate a CDC buffer"
    );
}

#[tokio::test]
async fn test_ivm_full_refresh_preserves_immediate_descendants() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE full_chain_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO full_chain_src VALUES (1, 10), (2, 20)")
        .await;
    db.create_st(
        "full_chain_upstream",
        "SELECT id, val FROM full_chain_src",
        "5m",
        "FULL",
    )
    .await;
    db.refresh_st("full_chain_upstream").await;
    create_immediate_st(
        &db,
        "full_chain_child",
        "SELECT id, val * 2 AS doubled FROM full_chain_upstream",
    )
    .await;
    create_immediate_st(
        &db,
        "full_chain_grandchild",
        "SELECT id, doubled + 1 AS result FROM full_chain_child",
    )
    .await;
    db.execute(
        "CREATE FUNCTION full_chain_noop() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
    )
    .await;
    db.execute(
        "CREATE TRIGGER full_chain_app BEFORE INSERT ON full_chain_upstream \
         FOR EACH ROW EXECUTE FUNCTION full_chain_noop()",
    )
    .await;

    let child_check = assert_st_matches_sql(
        "full_chain_child",
        "id, doubled",
        "SELECT id, val * 2 AS doubled FROM full_chain_src",
    );
    let grandchild_check = assert_st_matches_sql(
        "full_chain_grandchild",
        "id, result",
        "SELECT id, val * 2 + 1 AS result FROM full_chain_src",
    );
    db.execute_seq(&[
        "BEGIN",
        "DELETE FROM full_chain_src",
        "INSERT INTO full_chain_src VALUES (3, 15), (4, 25)",
        "SELECT pgtrickle.refresh_stream_table('full_chain_upstream')",
        &child_check,
        &grandchild_check,
        "COMMIT",
    ])
    .await;
    assert_st_matches_after_reconnect(
        &db,
        "full_chain_child",
        "id, doubled",
        "SELECT id, val * 2 AS doubled FROM full_chain_src",
    )
    .await;
    assert_st_matches_after_reconnect(
        &db,
        "full_chain_grandchild",
        "id, result",
        "SELECT id, val * 2 + 1 AS result FROM full_chain_src",
    )
    .await;

    let empty_child_check = assert_st_matches_sql(
        "full_chain_child",
        "id, doubled",
        "SELECT id, val * 2 AS doubled FROM full_chain_src",
    );
    let empty_grandchild_check = assert_st_matches_sql(
        "full_chain_grandchild",
        "id, result",
        "SELECT id, val * 2 + 1 AS result FROM full_chain_src",
    );
    db.execute_seq(&[
        "BEGIN",
        "DELETE FROM full_chain_src",
        "SELECT pgtrickle.refresh_stream_table('full_chain_upstream')",
        &empty_child_check,
        &empty_grandchild_check,
        "COMMIT",
    ])
    .await;
    assert_st_matches_after_reconnect(
        &db,
        "full_chain_child",
        "id, doubled",
        "SELECT id, val * 2 AS doubled FROM full_chain_src",
    )
    .await;
    assert_st_matches_after_reconnect(
        &db,
        "full_chain_grandchild",
        "id, result",
        "SELECT id, val * 2 + 1 AS result FROM full_chain_src",
    )
    .await;
}

#[tokio::test]
async fn test_ivm_scheduled_full_refresh_preserves_immediate_descendants() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "scheduler should be running"
    );

    db.execute("CREATE TABLE scheduled_full_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO scheduled_full_src VALUES (1, 10)")
        .await;
    db.create_st(
        "scheduled_full_upstream",
        "SELECT id, val FROM scheduled_full_src",
        "1s",
        "FULL",
    )
    .await;
    db.refresh_st("scheduled_full_upstream").await;
    create_immediate_st(
        &db,
        "scheduled_full_child",
        "SELECT id, val * 2 AS doubled FROM scheduled_full_upstream",
    )
    .await;
    create_immediate_st(
        &db,
        "scheduled_full_grandchild",
        "SELECT id, doubled + 1 AS result FROM scheduled_full_child",
    )
    .await;
    db.execute(
        "CREATE FUNCTION scheduled_full_noop() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
    )
    .await;
    db.execute(
        "CREATE TRIGGER scheduled_full_app BEFORE INSERT ON scheduled_full_upstream \
         FOR EACH ROW EXECUTE FUNCTION scheduled_full_noop()",
    )
    .await;
    db.execute("CREATE TABLE scheduled_full_audit (event TEXT NOT NULL)")
        .await;
    db.execute(
        "CREATE FUNCTION scheduled_full_audit_trigger() RETURNS trigger \
         LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO scheduled_full_audit VALUES (TG_OP);
           IF TG_LEVEL = 'ROW' THEN RETURN NEW; END IF;
           RETURN NULL;
         END
         $$",
    )
    .await;
    db.execute(
        "CREATE TRIGGER scheduled_full_audit_insert AFTER INSERT ON scheduled_full_upstream \
         FOR EACH ROW EXECUTE FUNCTION scheduled_full_audit_trigger()",
    )
    .await;
    db.execute(
        "CREATE TRIGGER scheduled_full_audit_truncate AFTER TRUNCATE ON scheduled_full_upstream \
         FOR EACH STATEMENT EXECUTE FUNCTION scheduled_full_audit_trigger()",
    )
    .await;

    let initial_ts: Option<String> = db
        .query_scalar_opt(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'scheduled_full_upstream'",
        )
        .await;
    db.execute("UPDATE scheduled_full_src SET val = 25 WHERE id = 1")
        .await;
    assert!(
        db.wait_for_auto_refresh_since(
            "scheduled_full_upstream",
            initial_ts,
            Duration::from_secs(45),
        )
        .await,
        "scheduler should complete the FULL replacement"
    );
    let (action, status, initiated_by): (String, String, Option<String>) = sqlx::query_as(
        "SELECT h.action, h.status, h.initiated_by \
         FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE st.pgt_name = 'scheduled_full_upstream' \
         ORDER BY refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read scheduled FULL history");
    assert_eq!(action, "FULL");
    assert_eq!(status, "COMPLETED");
    assert_eq!(initiated_by.as_deref(), Some("SCHEDULER"));
    assert_eq!(
        db.count("scheduled_full_audit").await,
        0,
        "scheduled FULL must suppress INSERT and TRUNCATE application triggers"
    );
    db.assert_st_matches_query(
        "scheduled_full_child",
        "SELECT id, val * 2 AS doubled FROM scheduled_full_src",
    )
    .await;
    db.assert_st_matches_query(
        "scheduled_full_grandchild",
        "SELECT id, val * 2 + 1 AS result FROM scheduled_full_src",
    )
    .await;
}

#[tokio::test]
async fn test_ivm_truncate_captures_deferred_changes() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.refresh_strategy = 'differential'")
        .await;
    db.reload_config_and_wait().await;
    db.execute("CREATE TABLE ivm_trunc_src (grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO ivm_trunc_src VALUES ('a', 1), ('b', NULL)")
        .await;
    create_immediate_st(&db, "ivm_trunc_imm", "SELECT grp, val FROM ivm_trunc_src").await;
    db.create_st(
        "ivm_trunc_diff_one",
        "SELECT grp, val FROM ivm_trunc_imm",
        "5m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "ivm_trunc_diff_two",
        "SELECT grp, val FROM ivm_trunc_imm",
        "5m",
        "DIFFERENTIAL",
    )
    .await;
    create_immediate_st(
        &db,
        "ivm_trunc_agg",
        "SELECT count(*)::bigint AS row_count, sum(val)::bigint AS total \
         FROM ivm_trunc_src",
    )
    .await;
    db.create_st(
        "ivm_trunc_agg_diff",
        "SELECT row_count, total FROM ivm_trunc_agg",
        "5m",
        "DIFFERENTIAL",
    )
    .await;
    let configured_mode: String = db
        .query_scalar(
            "SELECT refresh_mode::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ivm_trunc_diff_one'",
        )
        .await;
    assert_eq!(configured_mode, "DIFFERENTIAL");
    for child in [
        "ivm_trunc_diff_one",
        "ivm_trunc_diff_two",
        "ivm_trunc_agg_diff",
    ] {
        db.refresh_st_with_retry(child).await;
    }

    db.create_st(
        "ivm_trunc_full",
        "SELECT grp, val FROM ivm_trunc_src",
        "5m",
        "FULL",
    )
    .await;
    db.refresh_st_with_retry("ivm_trunc_full").await;
    db.create_st(
        "ivm_trunc_full_diff",
        "SELECT grp, val FROM ivm_trunc_full",
        "5m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st_with_retry("ivm_trunc_full_diff").await;

    // A keyless content identity stays present when an identical row is
    // inserted or removed. Both IVM and FULL capture must preserve the count.
    db.execute("INSERT INTO ivm_trunc_src VALUES ('a', 1)")
        .await;
    db.assert_st_matches_query("ivm_trunc_imm", "SELECT grp, val FROM ivm_trunc_src")
        .await;
    db.refresh_st_with_retry("ivm_trunc_diff_one").await;
    db.assert_st_matches_query("ivm_trunc_diff_one", "SELECT grp, val FROM ivm_trunc_src")
        .await;
    db.refresh_st_with_retry("ivm_trunc_agg_diff").await;
    db.assert_st_matches_query(
        "ivm_trunc_agg_diff",
        "SELECT count(*)::bigint AS row_count, sum(val)::bigint AS total \
         FROM ivm_trunc_src",
    )
    .await;
    db.refresh_st_with_retry("ivm_trunc_full").await;
    db.refresh_st_with_retry("ivm_trunc_full_diff").await;
    db.assert_st_matches_query("ivm_trunc_full_diff", "SELECT grp, val FROM ivm_trunc_src")
        .await;
    let agg_pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ivm_trunc_agg'",
        )
        .await;
    let agg_buffer = format!("pgtrickle_changes.changes_pgt_{agg_pgt_id}");
    let agg_buffer_watermark: i64 = db
        .query_scalar(&format!(
            "SELECT COALESCE(MAX(change_id), 0) FROM {agg_buffer}"
        ))
        .await;

    db.execute("TRUNCATE ivm_trunc_src").await;
    assert_eq!(db.count("ivm_trunc_imm").await, 0);
    let zero_row: bool = db
        .query_scalar("SELECT row_count = 0 AND total IS NULL FROM ivm_trunc_agg")
        .await;
    assert!(zero_row, "scalar aggregate must retain its zero-count row");
    let agg_buffer_changes = sqlx::query_as::<_, (String, Option<i64>, Option<i64>, String)>(
        sqlx::AssertSqlSafe(format!(
            "SELECT action::text, row_count, total, __pgt_row_id::text \
             FROM {agg_buffer} \
             WHERE action IN ('I', 'D') \
               AND change_id > {agg_buffer_watermark} \
             ORDER BY change_id"
        )),
    )
    .fetch_all(&db.pool)
    .await
    .expect("read immediate aggregate change buffer");
    assert_eq!(
        agg_buffer_changes.len(),
        2,
        "capture the aggregate replacement"
    );
    assert_eq!(agg_buffer_changes[0].0, "D");
    assert_eq!(agg_buffer_changes[0].1, Some(3));
    assert_eq!(agg_buffer_changes[0].2, Some(2));
    assert_eq!(agg_buffer_changes[1].0, "I");
    assert_eq!(agg_buffer_changes[1].1, Some(0));
    assert_eq!(agg_buffer_changes[1].2, None);
    assert_eq!(agg_buffer_changes[0].3, agg_buffer_changes[1].3);

    // Consume the empty replacement, then truncate the already-empty source
    // again to exercise a no-change capture and refresh.
    db.refresh_st_with_retry("ivm_trunc_diff_one").await;
    db.assert_st_matches_query("ivm_trunc_diff_one", "SELECT grp, val FROM ivm_trunc_src")
        .await;
    let (truncate_action, truncate_fallback): (String, bool) = sqlx::query_as(
        "SELECT h.action, h.was_full_fallback \
         FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE st.pgt_name = 'ivm_trunc_diff_one' AND h.status = 'COMPLETED' \
         ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read refresh history for the truncate capture");
    assert_eq!(
        truncate_action, "DIFFERENTIAL",
        "the refresh consuming TRUNCATE capture must be differential"
    );
    assert!(
        !truncate_fallback,
        "the refresh consuming TRUNCATE capture must not fall back to FULL"
    );
    db.refresh_st_with_retry("ivm_trunc_agg_diff").await;
    db.assert_st_matches_query(
        "ivm_trunc_agg_diff",
        "SELECT count(*)::bigint AS row_count, sum(val)::bigint AS total \
         FROM ivm_trunc_src",
    )
    .await;
    db.execute("TRUNCATE ivm_trunc_src").await;
    db.refresh_st_with_retry("ivm_trunc_diff_one").await;
    db.assert_st_matches_query("ivm_trunc_diff_one", "SELECT grp, val FROM ivm_trunc_src")
        .await;

    db.execute("INSERT INTO ivm_trunc_src VALUES ('c', 3), ('c', 3), ('d', NULL)")
        .await;

    db.refresh_st_with_retry("ivm_trunc_diff_one").await;
    db.assert_st_matches_query("ivm_trunc_diff_one", "SELECT grp, val FROM ivm_trunc_src")
        .await;
    let (action, strategy, full_fallback, reason, detail, delta_rows): (
        String,
        Option<String>,
        bool,
        Option<String>,
        Option<String>,
        i64,
    ) = sqlx::query_as(
        "SELECT h.action, h.merge_strategy_used, h.was_full_fallback, \
                h.refresh_reason, h.refresh_reason_detail, h.delta_row_count \
         FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE st.pgt_name = 'ivm_trunc_diff_one' AND h.status = 'COMPLETED' \
         ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read downstream refresh history");
    assert_ne!(
        strategy.as_deref(),
        Some("FULL"),
        "latest refresh: action={action}, fallback={full_fallback}, \
         reason={reason:?}, detail={detail:?}, delta_rows={delta_rows}"
    );
    assert!(
        !full_fallback,
        "deferred consumer must use captured differences"
    );
    assert_eq!(
        action, "DIFFERENTIAL",
        "deferred consumer must execute differential refresh, not just avoid a full fallback"
    );
    db.refresh_st_with_retry("ivm_trunc_diff_one").await;
    db.assert_st_matches_query("ivm_trunc_diff_one", "SELECT grp, val FROM ivm_trunc_src")
        .await;

    // A second consumer catches up later from its own copy of the capture.
    db.refresh_st_with_retry("ivm_trunc_diff_two").await;
    db.assert_st_matches_query("ivm_trunc_diff_two", "SELECT grp, val FROM ivm_trunc_src")
        .await;
    db.refresh_st_with_retry("ivm_trunc_agg_diff").await;
    db.assert_st_matches_query(
        "ivm_trunc_agg_diff",
        "SELECT count(*)::bigint AS row_count, sum(val)::bigint AS total \
         FROM ivm_trunc_src",
    )
    .await;
    let aggregate: (i64, Option<i64>) =
        sqlx::query_as("SELECT row_count, total FROM ivm_trunc_agg_diff")
            .fetch_one(&db.pool)
            .await
            .expect("read refreshed scalar aggregate");
    assert_eq!(aggregate, (3, Some(6)));
}

#[tokio::test]
async fn test_ivm_replacement_failure_rolls_back_and_recovers() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE full_fail_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO full_fail_src VALUES (1, 10)").await;
    db.create_st(
        "full_fail_upstream",
        "SELECT id, val FROM full_fail_src",
        "5m",
        "FULL",
    )
    .await;
    db.refresh_st("full_fail_upstream").await;
    create_immediate_st(
        &db,
        "full_fail_imm",
        "SELECT id, val * 2 AS doubled FROM full_fail_upstream",
    )
    .await;
    db.create_st(
        "full_fail_deferred",
        "SELECT id, val FROM full_fail_upstream",
        "5m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st_with_retry("full_fail_deferred").await;
    db.execute(
        "CREATE FUNCTION full_fail_noop() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
    )
    .await;
    db.execute(
        "CREATE TRIGGER full_fail_app BEFORE INSERT ON full_fail_upstream \
         FOR EACH ROW EXECUTE FUNCTION full_fail_noop()",
    )
    .await;
    let full_pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'full_fail_upstream'",
        )
        .await;
    let full_buffer = format!("pgtrickle_changes.changes_pgt_{full_pgt_id}");
    let full_relations = [
        "public.full_fail_src".to_owned(),
        "public.full_fail_upstream".to_owned(),
        "public.full_fail_imm".to_owned(),
        "public.full_fail_deferred".to_owned(),
        full_buffer.clone(),
    ];
    let full_rollback_rows_before = table_snapshots(&db.pool, &full_relations).await;
    let full_rollback_frontiers_before = frontier_snapshot(
        &db.pool,
        &["full_fail_upstream", "full_fail_imm", "full_fail_deferred"],
    )
    .await;
    let full_rollback_triggers_before =
        trigger_state_snapshot(&db.pool, "public.full_fail_upstream").await;

    db.execute_seq(&[
        "BEGIN",
        "UPDATE full_fail_src SET val = 11 WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('full_fail_upstream')",
        "DO $$ BEGIN IF (SELECT doubled FROM full_fail_imm WHERE id = 1) <> 22 \
             THEN RAISE EXCEPTION 'IMMEDIATE child missed transactional replacement'; END IF; END $$",
        "ROLLBACK",
    ])
    .await;
    let full_rollback_pool = fresh_verification_pool(&db).await;
    assert_eq!(
        table_snapshots(&full_rollback_pool, &full_relations).await,
        full_rollback_rows_before,
        "explicit FULL rollback must retain every base, stream, and buffer row"
    );
    assert_eq!(
        frontier_snapshot(
            &full_rollback_pool,
            &["full_fail_upstream", "full_fail_imm", "full_fail_deferred"],
        )
        .await,
        full_rollback_frontiers_before,
        "explicit FULL rollback must retain every refresh frontier"
    );
    assert_eq!(
        trigger_state_snapshot(&full_rollback_pool, "public.full_fail_upstream").await,
        full_rollback_triggers_before,
        "explicit FULL rollback must retain every trigger mode"
    );
    full_rollback_pool.close().await;

    db.execute(&format!(
        "ALTER TABLE {full_buffer} ADD CONSTRAINT reject_full_capture \
         CHECK (val <> 99)"
    ))
    .await;
    db.execute("UPDATE full_fail_src SET val = 99 WHERE id = 1")
        .await;
    let full_rows_before_failure = table_snapshots(&db.pool, &full_relations).await;
    let full_frontiers_before_failure = frontier_snapshot(
        &db.pool,
        &["full_fail_upstream", "full_fail_imm", "full_fail_deferred"],
    )
    .await;
    let full_triggers_before_failure =
        trigger_state_snapshot(&db.pool, "public.full_fail_upstream").await;
    let failed_full = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('full_fail_upstream')")
        .await;
    assert_check_constraint_error(
        failed_full.expect_err("a rejected downstream capture must fail the FULL replacement"),
        "reject_full_capture",
    );
    let full_failure_pool = fresh_verification_pool(&db).await;
    assert_eq!(
        table_snapshots(&full_failure_pool, &full_relations).await,
        full_rows_before_failure,
        "failed FULL capture must retain every base, stream, and buffer row"
    );
    assert_eq!(
        frontier_snapshot(
            &full_failure_pool,
            &["full_fail_upstream", "full_fail_imm", "full_fail_deferred"],
        )
        .await,
        full_frontiers_before_failure,
        "failed FULL capture must retain every refresh frontier"
    );
    assert_eq!(
        trigger_state_snapshot(&full_failure_pool, "public.full_fail_upstream").await,
        full_triggers_before_failure,
        "failed FULL capture must restore every trigger mode"
    );
    full_failure_pool.close().await;

    db.execute(&format!(
        "ALTER TABLE {full_buffer} DROP CONSTRAINT reject_full_capture"
    ))
    .await;
    db.refresh_st_with_retry("full_fail_upstream").await;
    db.refresh_st_with_retry("full_fail_deferred").await;
    assert_st_matches_after_reconnect(
        &db,
        "full_fail_upstream",
        "id, val",
        "SELECT id, val FROM full_fail_src",
    )
    .await;
    assert_st_matches_after_reconnect(
        &db,
        "full_fail_imm",
        "id, doubled",
        "SELECT id, val * 2 AS doubled FROM full_fail_src",
    )
    .await;
    assert_st_matches_after_reconnect(
        &db,
        "full_fail_deferred",
        "id, val",
        "SELECT id, val FROM full_fail_src",
    )
    .await;

    db.execute("CREATE TABLE trunc_fail_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO trunc_fail_src VALUES (1, 10)")
        .await;
    create_immediate_st(&db, "trunc_fail_imm", "SELECT id, val FROM trunc_fail_src").await;
    db.create_st(
        "trunc_fail_deferred",
        "SELECT id, val FROM trunc_fail_imm",
        "5m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st_with_retry("trunc_fail_deferred").await;
    db.execute("INSERT INTO trunc_fail_src VALUES (2, 20)")
        .await;
    let trunc_pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'trunc_fail_imm'",
        )
        .await;
    let trunc_buffer = format!("pgtrickle_changes.changes_pgt_{trunc_pgt_id}");
    let trunc_relations = [
        "public.trunc_fail_src".to_owned(),
        "public.trunc_fail_imm".to_owned(),
        "public.trunc_fail_deferred".to_owned(),
        trunc_buffer.clone(),
    ];
    let trunc_rollback_rows_before = table_snapshots(&db.pool, &trunc_relations).await;
    let trunc_rollback_frontiers_before =
        frontier_snapshot(&db.pool, &["trunc_fail_imm", "trunc_fail_deferred"]).await;
    let trunc_rollback_triggers_before =
        trigger_state_snapshot(&db.pool, "public.trunc_fail_src").await;

    db.execute_seq(&[
        "BEGIN",
        "TRUNCATE trunc_fail_src",
        "DO $$ BEGIN IF (SELECT count(*) FROM trunc_fail_imm) <> 0 \
             THEN RAISE EXCEPTION 'IMMEDIATE truncate did not run'; END IF; END $$",
        "ROLLBACK",
    ])
    .await;
    let trunc_rollback_pool = fresh_verification_pool(&db).await;
    assert_eq!(
        table_snapshots(&trunc_rollback_pool, &trunc_relations).await,
        trunc_rollback_rows_before,
        "explicit TRUNCATE rollback must retain every base, stream, and buffer row"
    );
    assert_eq!(
        frontier_snapshot(
            &trunc_rollback_pool,
            &["trunc_fail_imm", "trunc_fail_deferred"]
        )
        .await,
        trunc_rollback_frontiers_before,
        "explicit TRUNCATE rollback must retain every refresh frontier"
    );
    assert_eq!(
        trigger_state_snapshot(&trunc_rollback_pool, "public.trunc_fail_src").await,
        trunc_rollback_triggers_before,
        "explicit TRUNCATE rollback must retain every trigger mode"
    );
    trunc_rollback_pool.close().await;

    db.execute(&format!(
        "ALTER TABLE {trunc_buffer} ADD CONSTRAINT reject_truncate_capture \
         CHECK (val IS DISTINCT FROM 10)"
    ))
    .await;
    let trunc_rows_before_failure = table_snapshots(&db.pool, &trunc_relations).await;
    let trunc_frontiers_before_failure =
        frontier_snapshot(&db.pool, &["trunc_fail_imm", "trunc_fail_deferred"]).await;
    let trunc_triggers_before_failure =
        trigger_state_snapshot(&db.pool, "public.trunc_fail_src").await;
    let failed_truncate = db.try_execute("TRUNCATE trunc_fail_src").await;
    assert_check_constraint_error(
        failed_truncate.expect_err("a rejected truncate capture must fail the source TRUNCATE"),
        "reject_truncate_capture",
    );
    let trunc_failure_pool = fresh_verification_pool(&db).await;
    assert_eq!(
        table_snapshots(&trunc_failure_pool, &trunc_relations).await,
        trunc_rows_before_failure,
        "failed TRUNCATE capture must retain every base, stream, and buffer row"
    );
    assert_eq!(
        frontier_snapshot(
            &trunc_failure_pool,
            &["trunc_fail_imm", "trunc_fail_deferred"]
        )
        .await,
        trunc_frontiers_before_failure,
        "failed TRUNCATE capture must retain every refresh frontier"
    );
    assert_eq!(
        trigger_state_snapshot(&trunc_failure_pool, "public.trunc_fail_src").await,
        trunc_triggers_before_failure,
        "failed TRUNCATE capture must retain every trigger mode"
    );
    trunc_failure_pool.close().await;
    db.execute(&format!(
        "ALTER TABLE {trunc_buffer} DROP CONSTRAINT reject_truncate_capture"
    ))
    .await;
    db.execute("TRUNCATE trunc_fail_src").await;
    db.execute("INSERT INTO trunc_fail_src VALUES (3, 30)")
        .await;
    db.refresh_st_with_retry("trunc_fail_deferred").await;
    assert_st_matches_after_reconnect(
        &db,
        "trunc_fail_imm",
        "id, val",
        "SELECT id, val FROM trunc_fail_src",
    )
    .await;
    assert_st_matches_after_reconnect(
        &db,
        "trunc_fail_deferred",
        "id, val",
        "SELECT id, val FROM trunc_fail_src",
    )
    .await;
}

#[tokio::test]
async fn test_immediate_aggregate_captures_public_columns_for_deferred_child() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE imm_diff_src (id INT PRIMARY KEY, grp TEXT, amount INT)")
        .await;
    db.execute("INSERT INTO imm_diff_src VALUES (1, 'a', 10)")
        .await;
    create_immediate_st(
        &db,
        "imm_diff_upstream",
        "SELECT grp, SUM(amount) AS total FROM imm_diff_src GROUP BY grp",
    )
    .await;
    db.create_st(
        "imm_diff_child",
        "SELECT grp, total FROM imm_diff_upstream",
        "5m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute("INSERT INTO imm_diff_src VALUES (2, 'a', 5)")
        .await;
    db.refresh_st_with_retry("imm_diff_child").await;
    let total: i64 = db
        .query_scalar("SELECT total::bigint FROM imm_diff_child WHERE grp = 'a'")
        .await;
    assert_eq!(total, 15);
}

#[tokio::test]
async fn test_ivm_alter_immediate_to_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_i2d (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sw_i2d VALUES (1, 'x')").await;

    let query = "SELECT id, val FROM sw_i2d";
    create_immediate_st(&db, "sw_i2d_st", query).await;
    db.assert_st_matches_query("sw_i2d_st", query).await;

    let (_, mode, _, _) = db.pgt_status("sw_i2d_st").await;
    assert_eq!(mode, "IMMEDIATE");

    // Switch to DIFFERENTIAL with a schedule.
    db.execute(
        "SELECT pgtrickle.alter_stream_table('sw_i2d_st', \
         refresh_mode => 'DIFFERENTIAL', schedule => '10m')",
    )
    .await;

    let (status, mode, populated, _) = db.pgt_status("sw_i2d_st").await;
    assert_eq!(mode, "DIFFERENTIAL");
    assert_eq!(status, "ACTIVE");
    assert!(populated, "ST should remain populated after mode switch");
    assert_eq!(db.count("public.sw_i2d_st").await, 1);

    // IVM triggers should be gone — INSERT should NOT propagate immediately.
    db.execute("INSERT INTO sw_i2d VALUES (2, 'y')").await;
    assert_eq!(
        db.count("public.sw_i2d_st").await,
        1,
        "INSERT should NOT propagate in DIFFERENTIAL mode"
    );
    // Refresh should synchronize it
    db.refresh_st("sw_i2d_st").await;
    db.assert_st_matches_query("sw_i2d_st", query).await;
}

#[tokio::test]
async fn test_ivm_alter_full_to_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_f2i (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sw_f2i VALUES (1, 'p'), (2, 'q')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table('sw_f2i_st', \
         $$SELECT id, val FROM sw_f2i$$, '5m', 'FULL')",
    )
    .await;

    let (_, mode, _, _) = db.pgt_status("sw_f2i_st").await;
    assert_eq!(mode, "FULL");

    // Switch to IMMEDIATE.
    db.alter_st("sw_f2i_st", "refresh_mode => 'IMMEDIATE'")
        .await;

    let (_, mode, populated, _) = db.pgt_status("sw_f2i_st").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.sw_f2i_st").await, 2);

    // Verify IVM triggers are active.
    db.execute("INSERT INTO sw_f2i VALUES (3, 'r')").await;
    assert_eq!(db.count("public.sw_f2i_st").await, 3);
}

#[tokio::test]
async fn test_ivm_alter_immediate_to_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_i2f (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO sw_i2f VALUES (1, 'z')").await;

    let query = "SELECT id, val FROM sw_i2f";
    create_immediate_st(&db, "sw_i2f_st", query).await;
    db.assert_st_matches_query("sw_i2f_st", query).await;

    // Switch to FULL.
    db.execute(
        "SELECT pgtrickle.alter_stream_table('sw_i2f_st', \
         refresh_mode => 'FULL', schedule => '5m')",
    )
    .await;

    let (_, mode, _, _) = db.pgt_status("sw_i2f_st").await;
    assert_eq!(mode, "FULL");

    // IVM triggers should be removed — manual INSERT shouldn't propagate.
    db.execute("INSERT INTO sw_i2f VALUES (2, 'w')").await;
    assert_eq!(
        db.count("public.sw_i2f_st").await,
        1,
        "INSERT should NOT propagate in FULL mode"
    );
}

// ── IMMEDIATE Query Restriction Validation ─────────────────────────────

#[tokio::test]
async fn test_ivm_recursive_cte_immediate_allowed() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rc_src (id INT PRIMARY KEY, parent_id INT, name TEXT)")
        .await;
    db.execute("INSERT INTO rc_src VALUES (1, NULL, 'root'), (2, 1, 'child1'), (3, 1, 'child2')")
        .await;

    // Recursive CTEs are now allowed in IMMEDIATE mode (Task 5.1).
    // Semi-naive evaluation inside the trigger uses transition tables.
    create_immediate_st(
        &db,
        "rc_imm",
        "WITH RECURSIVE tree AS ( \
           SELECT id, parent_id, name FROM rc_src WHERE parent_id IS NULL \
           UNION ALL \
           SELECT c.id, c.parent_id, c.name FROM rc_src c \
           INNER JOIN tree t ON c.parent_id = t.id \
         ) SELECT id, parent_id, name FROM tree",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("rc_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.rc_imm").await, 3);
}

// ── Window Functions in IMMEDIATE Mode ─────────────────────────────────

#[tokio::test]
async fn test_ivm_window_function_create_succeeds() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE win_src (id INT PRIMARY KEY, val INT, grp TEXT)")
        .await;
    db.execute("INSERT INTO win_src VALUES (1, 10, 'A'), (2, 20, 'A'), (3, 30, 'B')")
        .await;

    // Window functions should now be accepted in IMMEDIATE mode.
    create_immediate_st(
        &db,
        "win_imm",
        "SELECT id, val, grp, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM public.win_src",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("win_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.win_imm").await, 3);
}

#[tokio::test]
async fn test_ivm_window_insert_propagates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE win_prop (id INT PRIMARY KEY, val INT, grp TEXT)")
        .await;
    db.execute("INSERT INTO win_prop VALUES (1, 10, 'X'), (2, 20, 'X')")
        .await;

    create_immediate_st(
        &db,
        "win_prop_imm",
        "SELECT id, val, grp, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM public.win_prop",
    )
    .await;
    assert_eq!(db.count("public.win_prop_imm").await, 2);

    // INSERT into the same partition should propagate and recompute row_number.
    db.execute("INSERT INTO win_prop VALUES (3, 5, 'X')").await;

    assert_eq!(
        db.count("public.win_prop_imm").await,
        3,
        "Window ST should have 3 rows after INSERT"
    );
}

// ── LATERAL Subqueries in IMMEDIATE Mode ───────────────────────────────

#[tokio::test]
async fn test_ivm_lateral_join_with_mutable_inner_source_is_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_parent (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE lat_child (id INT PRIMARY KEY, parent_id INT, score INT)")
        .await;
    db.execute("INSERT INTO lat_parent VALUES (1, 100), (2, 200)")
        .await;
    db.execute("INSERT INTO lat_child VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('lat_imm', \
             $$SELECT p.id, t.score FROM lat_parent p, \
               LATERAL (SELECT score FROM lat_child c WHERE c.parent_id = p.id \
                        ORDER BY score DESC LIMIT 1) t$$, NULL, 'IMMEDIATE')",
        )
        .await;
    assert!(
        result.is_err(),
        "mutable LATERAL inner sources are not IMMEDIATE-safe"
    );
}

#[tokio::test]
async fn test_ivm_lateral_insert_with_mutable_inner_source_is_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_ins_p (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE lat_ins_c (id INT PRIMARY KEY, parent_id INT, amount INT)")
        .await;
    db.execute("INSERT INTO lat_ins_p VALUES (1, 'Alice')")
        .await;
    db.execute("INSERT INTO lat_ins_c VALUES (1, 1, 100)").await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('lat_ins_imm', \
             $$SELECT p.id, p.name, t.amount FROM lat_ins_p p, \
               LATERAL (SELECT amount FROM lat_ins_c c WHERE c.parent_id = p.id \
                        ORDER BY amount DESC LIMIT 1) t$$, NULL, 'IMMEDIATE')",
        )
        .await;
    assert!(
        result.is_err(),
        "mutable LATERAL inner sources are not IMMEDIATE-safe"
    );
}

// ── Scalar Subqueries in IMMEDIATE Mode ────────────────────────────────

#[tokio::test]
async fn test_ivm_scalar_subquery_create_succeeds() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ssq_main (id INT PRIMARY KEY, cat TEXT)")
        .await;
    db.execute("CREATE TABLE ssq_counts (cat TEXT PRIMARY KEY, cnt INT)")
        .await;
    db.execute("INSERT INTO ssq_main VALUES (1, 'A'), (2, 'B')")
        .await;
    db.execute("INSERT INTO ssq_counts VALUES ('A', 10), ('B', 20)")
        .await;

    // Scalar subqueries should now be accepted in IMMEDIATE mode.
    create_immediate_st(
        &db,
        "ssq_imm",
        "SELECT id, cat, (SELECT cnt FROM ssq_counts sc WHERE sc.cat = m.cat) AS cat_count FROM ssq_main m",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("ssq_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.ssq_imm").await, 2);
}

#[tokio::test]
async fn test_ivm_allow_aggregate_in_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE agg_src (id INT PRIMARY KEY, category TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO agg_src VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30)")
        .await;

    // Aggregates should be allowed in IMMEDIATE mode.
    create_immediate_st(
        &db,
        "agg_imm",
        "SELECT category, SUM(amount) AS total FROM agg_src GROUP BY category",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("agg_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);

    let count = db.count("public.agg_imm").await;
    assert_eq!(count, 2, "Should have 2 groups (A, B)");

    // INSERT should propagate and update aggregate.
    db.execute("INSERT INTO agg_src VALUES (4, 'A', 40)").await;

    let total_a: String = db
        .query_scalar("SELECT total::text FROM public.agg_imm WHERE category = 'A'")
        .await;
    assert_eq!(total_a, "80", "SUM for category A should be 10+30+40=80");
}

#[tokio::test]
async fn test_ivm_allow_join_in_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE join_left (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE join_right (id INT PRIMARY KEY, left_id INT, val INT)")
        .await;
    db.execute("INSERT INTO join_left VALUES (1, 'Alpha'), (2, 'Beta')")
        .await;
    db.execute("INSERT INTO join_right VALUES (1, 1, 100), (2, 2, 200)")
        .await;

    // Joins should be allowed in IMMEDIATE mode.
    create_immediate_st(
        &db,
        "join_imm",
        "SELECT l.id, l.name, r.val FROM join_left l INNER JOIN join_right r ON r.left_id = l.id",
    )
    .await;

    let (_, mode, populated, _) = db.pgt_status("join_imm").await;
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);
    assert_eq!(db.count("public.join_imm").await, 2);

    // INSERT into right table should propagate.
    db.execute("INSERT INTO join_right VALUES (3, 1, 300)")
        .await;
    assert_eq!(
        db.count("public.join_imm").await,
        3,
        "Join ST should have 3 rows after INSERT into right table"
    );
}

// ── Alter Mode Switching Validation ────────────────────────────────────

#[tokio::test]
async fn test_ivm_alter_to_immediate_allows_recursive_cte() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_rc (id INT PRIMARY KEY, parent_id INT, name TEXT)")
        .await;

    // Create as DIFFERENTIAL with a recursive CTE query.
    db.execute(
        "SELECT pgtrickle.create_stream_table('sw_rc_st', \
         $$WITH RECURSIVE tree AS ( \
           SELECT id, parent_id, name FROM sw_rc WHERE parent_id IS NULL \
           UNION ALL \
           SELECT c.id, c.parent_id, c.name FROM sw_rc c \
           INNER JOIN tree t ON c.parent_id = t.id \
         ) SELECT id, parent_id, name FROM tree$$, \
         '5m', 'DIFFERENTIAL')",
    )
    .await;

    // Recursive CTEs are now allowed in IMMEDIATE mode (Task 5.1).
    // Switching a recursive-CTE ST to IMMEDIATE should succeed.
    db.alter_st("sw_rc_st", "refresh_mode => 'IMMEDIATE'").await;

    // Verify mode changed to IMMEDIATE.
    let (_, mode, _, _) = db.pgt_status("sw_rc_st").await;
    assert_eq!(mode, "IMMEDIATE", "Mode should switch to IMMEDIATE");
}

#[tokio::test]
async fn test_ivm_alter_to_immediate_allows_window() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sw_win (id INT PRIMARY KEY, val INT, grp TEXT)")
        .await;
    db.execute("INSERT INTO sw_win VALUES (1, 10, 'A'), (2, 20, 'B')")
        .await;

    // Create as DIFFERENTIAL with a window function query.
    db.execute(
        "SELECT pgtrickle.create_stream_table('sw_win_st', \
         $$SELECT id, val, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM public.sw_win$$, \
         '5m', 'DIFFERENTIAL')",
    )
    .await;

    // Switch to IMMEDIATE should now succeed for window functions.
    db.alter_st("sw_win_st", "refresh_mode => 'IMMEDIATE'")
        .await;

    let (_, mode, _, _) = db.pgt_status("sw_win_st").await;
    assert_eq!(
        mode, "IMMEDIATE",
        "Mode should switch to IMMEDIATE for window function query"
    );
}

// ── Cascading IMMEDIATE Stream Tables ──────────────────────────────────

#[tokio::test]
async fn test_ivm_cascading_immediate_sts() {
    let db = E2eDb::new().await.with_extension().await;

    for (suffix, use_enr) in [("temp", false), ("enr", true)] {
        let base = format!("cascade_base_{suffix}");
        let a = format!("cascade_a_{suffix}");
        let b = format!("cascade_b_{suffix}");
        let c = format!("cascade_c_{suffix}");
        let set_enr = format!("SET pg_trickle.ivm_use_enr = {use_enr}");
        let create_base = format!("CREATE TABLE {base} (id INT PRIMARY KEY, val INT)");
        let seed_base = format!("INSERT INTO {base} VALUES (1, 10), (2, 20)");
        let create_a = format!(
            "SELECT pgtrickle.create_stream_table('{a}', \
             $$SELECT id, val FROM {base} WHERE val > 0$$, NULL, 'IMMEDIATE')"
        );
        let create_b = format!(
            "SELECT pgtrickle.create_stream_table('{b}', \
             $$SELECT id, val * 10 AS val10 FROM {a}$$, NULL, 'IMMEDIATE')"
        );
        let create_c = format!(
            "SELECT pgtrickle.create_stream_table('{c}', \
             $$SELECT id, val10 + 1 AS val11 FROM {b}$$, NULL, 'IMMEDIATE')"
        );
        db.execute_seq(&[
            &set_enr,
            &create_base,
            &seed_base,
            &create_a,
            &create_b,
            &create_c,
            "RESET pg_trickle.ivm_use_enr",
        ])
        .await;

        for (source, downstream) in [(&a, &b), (&b, &c)] {
            let trigger_count: i64 = db
                .query_scalar(&format!(
                    "SELECT count(*) FROM pg_trigger t \
                     JOIN pgtrickle.pgt_stream_tables st \
                       ON st.pgt_name = '{downstream}' \
                     WHERE t.tgrelid = '{source}'::regclass \
                       AND t.tgname LIKE 'pgt_ivm_%_' || st.pgt_id"
                ))
                .await;
            assert_eq!(
                trigger_count, 8,
                "{downstream} should have all IVM triggers installed on {source}"
            );
        }

        let insert = format!("INSERT INTO {base} VALUES (3, 30)");
        let check_insert = format!(
            "DO $check$ BEGIN \
               ASSERT (SELECT val11 FROM {c} WHERE id = 3) = 301; \
             END $check$"
        );
        let update = format!("UPDATE {base} SET val = 40 WHERE id = 3");
        let check_update = format!(
            "DO $check$ BEGIN \
               ASSERT (SELECT val11 FROM {c} WHERE id = 3) = 401; \
             END $check$"
        );
        let delete = format!("DELETE FROM {base} WHERE id = 3");
        let check_delete = format!(
            "DO $check$ BEGIN \
               ASSERT NOT EXISTS (SELECT FROM {c} WHERE id = 3); \
             END $check$"
        );
        let truncate = format!("TRUNCATE {base}");
        let check_truncate = format!(
            "DO $check$ BEGIN \
               ASSERT NOT EXISTS (SELECT FROM {a}); \
               ASSERT NOT EXISTS (SELECT FROM {b}); \
               ASSERT NOT EXISTS (SELECT FROM {c}); \
             END $check$"
        );
        db.execute_seq(&[
            "BEGIN",
            &insert,
            &check_insert,
            &update,
            &check_update,
            &delete,
            &check_delete,
            &truncate,
            &check_truncate,
            "ROLLBACK",
        ])
        .await;
    }
}

// ── Concurrent IMMEDIATE Mode Tests ────────────────────────────────────

#[tokio::test]
async fn test_ivm_concurrent_inserts_immediate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE conc_src (id INT PRIMARY KEY, val INT)")
        .await;

    let query = "SELECT id, val FROM conc_src";
    create_immediate_st(&db, "conc_imm", query).await;
    db.assert_st_matches_query("conc_imm", query).await;

    // Perform concurrent inserts using separate connections from the pool.
    let pool = db.pool.clone();
    let mut handles = Vec::new();

    for batch in 0..5 {
        let p = pool.clone();
        let handle = tokio::spawn(async move {
            let base = batch * 10 + 1;
            for i in 0..10 {
                let id = base + i;
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO conc_src VALUES ({id}, {id})"
                )))
                .execute(&p)
                .await
                .expect("concurrent INSERT should succeed");
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.expect("task should not panic");
    }

    // All 50 rows should be reflected in the IMMEDIATE ST.
    assert_eq!(
        db.count("public.conc_imm").await,
        50,
        "IMMEDIATE ST should have 50 rows after concurrent inserts"
    );
    db.assert_st_matches_query("conc_imm", query).await;
}

// ── SEC-002: IVM AFTER trigger search_path shadowing ────────────────────

/// SEC-002: Verify that a user-created schema named `pgtrickle` cannot shadow
/// the real pg_trickle functions during AFTER trigger execution.
///
/// Creates a public function `pgtrickle.pg_trickle_capture_change` in a test
/// schema and inserts into a source table.  The IVM trigger must call the
/// actual extension function — not the impersonator — and the ST must be
/// correctly maintained.
#[tokio::test]
async fn test_sec002_ivm_trigger_not_shadowed_by_public_pgtrickle() {
    let db = e2e::E2eDb::new().await.with_extension().await;

    // Set up source table and IMMEDIATE stream table.
    db.execute("CREATE TABLE sec002_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    // schedule=NULL, mode='IMMEDIATE' — matches how other IVM tests create
    // IMMEDIATE stream tables (see create_immediate_st helper above).
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'sec002_st', \
            $$SELECT id, val FROM sec002_src$$, \
            NULL, 'IMMEDIATE'\
        )",
    )
    .await;

    // Create an impersonator schema and function that would corrupt data
    // if the trigger called it instead of the real extension function.
    // The function raises an error so we can clearly detect if it's called.
    db.execute("CREATE SCHEMA IF NOT EXISTS test_shadow_pgtrickle")
        .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION test_shadow_pgtrickle.pg_trickle_capture_change() \
         RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
         BEGIN \
           RAISE EXCEPTION 'SHADOWED: pg_trickle_capture_change called from wrong schema'; \
         END; \
         $$",
    )
    .await;

    // Insert data — the AFTER trigger should call the REAL pgtrickle function,
    // not the impostor. If the impostor is called, the INSERT will fail with
    // our sentinel exception.
    db.execute("INSERT INTO sec002_src VALUES (1, 'hello')")
        .await;
    db.execute("INSERT INTO sec002_src VALUES (2, 'world')")
        .await;

    // If we reach here, the real trigger fired (not the impostor).
    // Verify the IMMEDIATE ST was correctly maintained.
    assert_eq!(
        db.count("public.sec002_st").await,
        2,
        "IMMEDIATE ST should have 2 rows — trigger was NOT shadowed"
    );

    let val: String = db
        .query_scalar("SELECT val FROM public.sec002_st WHERE id = 1")
        .await;
    assert_eq!(val, "hello", "Row value should be correct after trigger");

    // Cleanup
    db.execute("DROP SCHEMA IF EXISTS test_shadow_pgtrickle CASCADE")
        .await;
}
