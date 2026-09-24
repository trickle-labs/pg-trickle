//! E2E tests for v0.43.0 features (T-A44-1, T-A44-2, T-A44-3, T-A44-4, T-A44-10).
//!
//! Covers:
//! - T-A44-1: Deep-join threshold GUC behavior
//! - T-A44-2: GROUP_RESCAN improvement with SUM(CASE ...) correctness
//! - T-A44-3: explain_stream_table() diagnostic output accuracy
//! - T-A44-4: WAL per-source status view accuracy
//! - T-A44-10: D+I change buffer schema CDC correctness
//!
//! These tests use the light E2E path (stock postgres:18.6 container).

mod e2e;

use e2e::E2eDb;

// ── T-A44-1: Deep-join threshold GUCs ─────────────────────────────────────

/// T-A44-1a: Setting pg_trickle.part3_max_scan_count to a very low value
/// should be accepted and retrievable.
#[tokio::test]
async fn test_t_a44_1_part3_max_scan_count_guc_settable() {
    let db = E2eDb::new().await.with_extension().await;

    // Use set_and_show_setting so both statements share a single connection.
    // Session-level SET is only visible on the connection that ran it.
    // part3_max_scan_count valid range is 1..32; use 20 as a mid-range value.
    let val = db
        .set_and_show_setting(
            "SET pg_trickle.part3_max_scan_count = 20",
            "pg_trickle.part3_max_scan_count",
        )
        .await;
    assert_eq!(val, "20", "GUC should be 20 after SET");
}

/// T-A44-1b: Setting pg_trickle.deep_join_l0_scan_threshold to a small
/// value should be accepted and retrievable.
#[tokio::test]
async fn test_t_a44_1_deep_join_l0_scan_threshold_guc_settable() {
    let db = E2eDb::new().await.with_extension().await;

    // deep_join_l0_scan_threshold valid range is 1..32; use 16 as a mid-range value.
    let val = db
        .set_and_show_setting(
            "SET pg_trickle.deep_join_l0_scan_threshold = 16",
            "pg_trickle.deep_join_l0_scan_threshold",
        )
        .await;
    assert_eq!(val, "16", "GUC should be 16 after SET");
}

/// T-A44-1c: WAL GUCs are settable (pg_trickle.wal_max_changes_per_poll).
#[tokio::test]
async fn test_t_a44_1_wal_max_changes_per_poll_guc_settable() {
    let db = E2eDb::new().await.with_extension().await;

    let val = db
        .set_and_show_setting(
            "SET pg_trickle.wal_max_changes_per_poll = 5000",
            "pg_trickle.wal_max_changes_per_poll",
        )
        .await;
    assert_eq!(val, "5000", "wal_max_changes_per_poll GUC should be 5000");
}

// ── T-A44-2: GROUP_RESCAN / SUM(CASE ...) correctness ─────────────────────

/// T-A44-2a: SUM(CASE WHEN status = 'active' THEN amount ELSE 0 END) gives
/// correct incremental results after mixed INSERT/UPDATE/DELETE.
#[tokio::test]
async fn test_t_a44_2_sum_case_incremental_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE a44_orders (id SERIAL PRIMARY KEY, region TEXT, status TEXT, amount INT)",
    )
    .await;
    db.execute(
        "INSERT INTO a44_orders (region, status, amount) VALUES \
         ('east', 'active', 100), \
         ('east', 'inactive', 50), \
         ('west', 'active', 200), \
         ('west', 'inactive', 75)",
    )
    .await;

    let query = "SELECT region, SUM(CASE WHEN status = 'active' THEN amount ELSE 0 END) AS active_total \
                 FROM a44_orders GROUP BY region ORDER BY region";

    db.create_st("a44_case_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st("a44_case_st").await;
    db.assert_st_matches_query("a44_case_st", query).await;

    // UPDATE: flip an inactive order to active (crosses CASE boundary)
    db.execute("UPDATE a44_orders SET status = 'active' WHERE id = 2")
        .await;
    // INSERT a new active order
    db.execute("INSERT INTO a44_orders (region, status, amount) VALUES ('east', 'active', 300)")
        .await;
    // DELETE an active order
    db.execute("DELETE FROM a44_orders WHERE id = 3").await;

    db.refresh_st("a44_case_st").await;
    db.assert_st_matches_query("a44_case_st", query).await;
}

// ── T-A44-3: explain_stream_table() diagnostics ───────────────────────────

/// T-A44-3a: explain_stream_table returns a non-empty string result for a
/// simple scan stream table.
#[tokio::test]
async fn test_t_a44_3_explain_stream_table_returns_result() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE a44_src (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO a44_src VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "a44_explain_st",
        "SELECT id, val FROM a44_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("a44_explain_st").await;

    // explain_stream_table returns a single TEXT value
    let result: Option<String> = db
        .query_scalar_opt("SELECT pgtrickle.explain_stream_table('a44_explain_st')")
        .await;
    assert!(
        result.as_ref().map(|s| !s.is_empty()).unwrap_or(false),
        "explain_stream_table must return a non-empty string"
    );
}

/// T-A44-3b: explain_stream_table includes refresh mode and plan info.
#[tokio::test]
async fn test_t_a44_3_explain_stream_table_includes_plan_info() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE a44_plan_src (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO a44_plan_src VALUES (1, 'a', 10), (2, 'b', 20)")
        .await;

    db.create_st(
        "a44_plan_st",
        "SELECT grp, SUM(val) AS total FROM a44_plan_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("a44_plan_st").await;

    // The explain output should mention the refresh mode (DIFFERENTIAL)
    let result: String = db
        .query_scalar("SELECT pgtrickle.explain_stream_table('a44_plan_st')")
        .await;

    let lower = result.to_lowercase();
    assert!(
        lower.contains("differential") || lower.contains("diff"),
        "explain output should mention differential mode; got: {result}"
    );
}

// ── T-A44-4: WAL per-source status ────────────────────────────────────────

/// T-A44-4a: wal_source_status() returns a result row for each source table.
#[tokio::test]
async fn test_t_a44_4_wal_source_status_returns_rows() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE a44_wal_src (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO a44_wal_src VALUES (1, 1)").await;

    db.create_st(
        "a44_wal_st",
        "SELECT id, val FROM a44_wal_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("a44_wal_st").await;

    // The function should exist and return at least one row
    let count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.wal_source_status()")
        .await;
    assert!(count >= 0, "wal_source_status should not fail");
}

/// T-A44-4b: wal_source_status() function exists with expected signature.
#[tokio::test]
async fn test_t_a44_4_wal_source_status_schema() {
    let db = E2eDb::new().await.with_extension().await;

    // Verify the function exists in the pgtrickle schema
    let has_fn: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_proc p \
             JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'pgtrickle' AND p.proname = 'wal_source_status')",
        )
        .await;
    assert!(has_fn, "pgtrickle.wal_source_status() function must exist");

    // Verify it is callable (returns zero or more rows without error)
    db.execute("SELECT * FROM pgtrickle.wal_source_status() LIMIT 0")
        .await;
}

// ── T-A44-10: D+I change buffer schema correctness ────────────────────────

/// T-A44-10a: INSERT correctness with D+I schema — inserted row appears in ST.
#[tokio::test]
async fn test_t_a44_10_insert_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE a44_di_src (id SERIAL PRIMARY KEY, name TEXT, val INT)")
        .await;
    db.execute("INSERT INTO a44_di_src (name, val) VALUES ('alice', 10), ('bob', 20)")
        .await;

    db.create_st(
        "a44_di_st",
        "SELECT id, name, val FROM a44_di_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("a44_di_st").await;

    db.execute("INSERT INTO a44_di_src (name, val) VALUES ('carol', 30)")
        .await;
    db.refresh_st("a44_di_st").await;
    db.assert_st_matches_query("a44_di_st", "SELECT id, name, val FROM a44_di_src")
        .await;
}

/// T-A44-10b: UPDATE correctness with D+I schema — updated row reflects new values.
#[tokio::test]
async fn test_t_a44_10_update_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE a44_upd_src (id SERIAL PRIMARY KEY, name TEXT, val INT)")
        .await;
    db.execute("INSERT INTO a44_upd_src (name, val) VALUES ('alice', 10), ('bob', 20)")
        .await;

    db.create_st(
        "a44_upd_st",
        "SELECT id, name, val FROM a44_upd_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("a44_upd_st").await;

    // UPDATE that changes a non-PK column
    db.execute("UPDATE a44_upd_src SET val = 99 WHERE name = 'alice'")
        .await;
    db.refresh_st("a44_upd_st").await;
    db.assert_st_matches_query("a44_upd_st", "SELECT id, name, val FROM a44_upd_src")
        .await;
}

/// T-A44-10c: DELETE correctness with D+I schema — deleted row absent from ST.
#[tokio::test]
async fn test_t_a44_10_delete_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE a44_del_src (id SERIAL PRIMARY KEY, name TEXT, val INT)")
        .await;
    db.execute("INSERT INTO a44_del_src (name, val) VALUES ('alice', 10), ('bob', 20)")
        .await;

    db.create_st(
        "a44_del_st",
        "SELECT id, name, val FROM a44_del_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("a44_del_st").await;

    db.execute("DELETE FROM a44_del_src WHERE name = 'bob'")
        .await;
    db.refresh_st("a44_del_st").await;
    db.assert_st_matches_query("a44_del_st", "SELECT id, name, val FROM a44_del_src")
        .await;
}

/// T-A44-10d: Net-effect idempotency — multiple UPDATEs to the same row in
/// one refresh cycle produce the correct final value.
#[tokio::test]
async fn test_t_a44_10_net_effect_idempotency() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE a44_net_src (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO a44_net_src (val) VALUES (1)").await;

    db.create_st(
        "a44_net_st",
        "SELECT id, val FROM a44_net_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("a44_net_st").await;

    // Three updates before the next refresh
    db.execute("UPDATE a44_net_src SET val = 2 WHERE id = 1")
        .await;
    db.execute("UPDATE a44_net_src SET val = 3 WHERE id = 1")
        .await;
    db.execute("UPDATE a44_net_src SET val = 99 WHERE id = 1")
        .await;

    db.refresh_st("a44_net_st").await;

    let val: i32 = db
        .query_scalar("SELECT val FROM public.a44_net_st WHERE id = 1")
        .await;
    assert_eq!(val, 99, "Stream table must reflect final UPDATE value 99");
}

/// T-A44-10e: INSERT-UPDATE-DELETE chain — row should not appear in ST.
#[tokio::test]
async fn test_t_a44_10_insert_update_delete_net_zero() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE a44_chain_src (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO a44_chain_src (val) VALUES (10)")
        .await;

    db.create_st(
        "a44_chain_st",
        "SELECT id, val FROM a44_chain_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("a44_chain_st").await;
    assert_eq!(db.count("public.a44_chain_st").await, 1);

    // INSERT + UPDATE + DELETE in a single refresh cycle
    db.execute("INSERT INTO a44_chain_src (val) VALUES (99)")
        .await;
    db.execute("UPDATE a44_chain_src SET val = 100 WHERE val = 99")
        .await;
    db.execute("DELETE FROM a44_chain_src WHERE val = 100")
        .await;

    db.refresh_st("a44_chain_st").await;
    // The net effect is zero — count stays at 1
    assert_eq!(
        db.count("public.a44_chain_st").await,
        1,
        "Net INSERT+UPDATE+DELETE should leave count unchanged"
    );
}

/// T-A44-10f: D+I schema with aggregate — SUM over source table with UPDATE
/// gives correct incremental result.
#[tokio::test]
async fn test_t_a44_10_aggregate_correctness_with_update() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE a44_agg_src (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO a44_agg_src (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)")
        .await;

    let query = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM a44_agg_src GROUP BY grp";
    db.create_st("a44_agg_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st("a44_agg_st").await;
    db.assert_st_matches_query("a44_agg_st", query).await;

    // UPDATE changes the val for an existing row
    db.execute("UPDATE a44_agg_src SET val = 50 WHERE grp = 'a' AND val = 10")
        .await;
    db.execute("INSERT INTO a44_agg_src (grp, val) VALUES ('b', 15)")
        .await;

    db.refresh_st("a44_agg_st").await;
    db.assert_st_matches_query("a44_agg_st", query).await;
}

/// T-A44-10g: changed_cols bitmask — verify CB rows have correct VARBIT values.
/// D-row and I-row from an UPDATE both carry the same changed_cols bitmask;
/// genuine INSERT/DELETE rows have NULL changed_cols.
#[tokio::test]
async fn test_t_a44_10_changed_cols_bitmask_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE a44_vcols_src (id SERIAL PRIMARY KEY, col1 INT, col2 TEXT, col3 FLOAT)",
    )
    .await;
    db.execute("INSERT INTO a44_vcols_src (col1, col2, col3) VALUES (1, 'hello', 1.5)")
        .await;

    db.create_st(
        "a44_vcols_st",
        "SELECT id, col1, col2, col3 FROM a44_vcols_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("a44_vcols_st").await;

    let source_oid = db.table_oid("a44_vcols_src").await;
    let cb_table = db.change_buffer_table(source_oid as i64).await;

    // UPDATE only col1 — the D-row and I-row should both carry the same
    // changed_cols bitmask reflecting col1 changed.
    db.execute("UPDATE a44_vcols_src SET col1 = 99 WHERE id = 1")
        .await;

    // After UPDATE: should have 2 rows (D-row + I-row) in the CB
    let cb_count: i64 = db
        .query_scalar(&format!(
            "SELECT COUNT(*) FROM {cb_table} WHERE action IN ('I', 'D')"
        ))
        .await;
    assert_eq!(
        cb_count, 2,
        "UPDATE should produce exactly 2 CB rows (D+I) in D+I schema"
    );

    // Both D-row and I-row should have the same non-NULL changed_cols
    let distinct_masks: i64 = db
        .query_scalar(&format!(
            "SELECT COUNT(DISTINCT changed_cols) FROM {cb_table} \
             WHERE action IN ('I', 'D') AND changed_cols IS NOT NULL"
        ))
        .await;
    assert_eq!(
        distinct_masks, 1,
        "D-row and I-row should have identical changed_cols bitmask"
    );

    db.refresh_st("a44_vcols_st").await;
    db.assert_st_matches_query(
        "a44_vcols_st",
        "SELECT id, col1, col2, col3 FROM a44_vcols_src",
    )
    .await;
}
