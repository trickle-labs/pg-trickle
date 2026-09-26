//! TEST-3 (v0.31.0): ENR compatibility-setting parity tests.
//!
//! Verifies that IMMEDIATE-mode stream tables produce identical results
//! whether `pg_trickle.ivm_use_enr = true` or false. The setting is retained
//! for compatibility while both values use the safe temp-table approach.
//!
//! These tests exercise INSERT, UPDATE, and DELETE on stream tables created
//! in both modes and assert row-level correctness parity.

mod e2e;

use e2e::E2eDb;

/// Helper: create an IMMEDIATE-mode stream table with the given ENR mode setting.
async fn create_immediate_st_with_enr(db: &E2eDb, name: &str, query: &str, use_enr: bool) {
    let set_enr = format!(
        "SET pg_trickle.ivm_use_enr = {}",
        if use_enr { "true" } else { "false" }
    );
    let create = format!(
        "SELECT pgtrickle.create_stream_table('{name}', $${query}$$, \
         NULL, 'IMMEDIATE')"
    );
    db.execute_seq(&[&set_enr, &create, "RESET pg_trickle.ivm_use_enr"])
        .await;
}

// ── INSERT parity ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_enr_parity_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t_enr_ins (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO t_enr_ins VALUES (1, 'a'), (2, 'b')")
        .await;

    let query = "SELECT id, val FROM t_enr_ins";

    // Create one ST with ENR mode, one with legacy temp-table mode.
    create_immediate_st_with_enr(&db, "st_enr", query, true).await;
    create_immediate_st_with_enr(&db, "st_tmp", query, false).await;

    // Both should start equal.
    db.assert_st_matches_query("st_enr", query).await;
    db.assert_st_matches_query("st_tmp", query).await;

    // INSERT a new row — both STs should update synchronously.
    db.execute("INSERT INTO t_enr_ins VALUES (3, 'c')").await;

    db.assert_st_matches_query("st_enr", query).await;
    db.assert_st_matches_query("st_tmp", query).await;

    // Both should contain the same rows.
    let count_enr = db.count("public.st_enr").await;
    let count_tmp = db.count("public.st_tmp").await;
    assert_eq!(
        count_enr, count_tmp,
        "ENR and temp-table modes should produce identical row counts after INSERT"
    );
    assert_eq!(count_enr, 3);
}

// ── UPDATE parity ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_enr_parity_update() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t_enr_upd (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO t_enr_upd VALUES (1, 'old'), (2, 'keep')")
        .await;

    let query = "SELECT id, val FROM t_enr_upd";

    create_immediate_st_with_enr(&db, "st_enr_upd", query, true).await;
    create_immediate_st_with_enr(&db, "st_tmp_upd", query, false).await;

    db.assert_st_matches_query("st_enr_upd", query).await;
    db.assert_st_matches_query("st_tmp_upd", query).await;

    // UPDATE row 1 — both STs should reflect the change.
    db.execute("UPDATE t_enr_upd SET val = 'new' WHERE id = 1")
        .await;

    db.assert_st_matches_query("st_enr_upd", query).await;
    db.assert_st_matches_query("st_tmp_upd", query).await;

    let enr_val: String = db
        .query_scalar("SELECT val FROM public.st_enr_upd WHERE id = 1")
        .await;
    let tmp_val: String = db
        .query_scalar("SELECT val FROM public.st_tmp_upd WHERE id = 1")
        .await;
    assert_eq!(
        enr_val, tmp_val,
        "ENR and temp-table modes should produce identical values after UPDATE"
    );
    assert_eq!(enr_val, "new");
}

// ── DELETE parity ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_enr_parity_delete() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t_enr_del (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO t_enr_del VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .await;

    let query = "SELECT id, val FROM t_enr_del";

    create_immediate_st_with_enr(&db, "st_enr_del", query, true).await;
    create_immediate_st_with_enr(&db, "st_tmp_del", query, false).await;

    db.assert_st_matches_query("st_enr_del", query).await;
    db.assert_st_matches_query("st_tmp_del", query).await;

    // DELETE row 2 — both STs should remove the row.
    db.execute("DELETE FROM t_enr_del WHERE id = 2").await;

    db.assert_st_matches_query("st_enr_del", query).await;
    db.assert_st_matches_query("st_tmp_del", query).await;

    let count_enr = db.count("public.st_enr_del").await;
    let count_tmp = db.count("public.st_tmp_del").await;
    assert_eq!(
        count_enr, count_tmp,
        "ENR and temp-table modes should produce identical row counts after DELETE"
    );
    assert_eq!(count_enr, 2);
}

#[tokio::test]
async fn test_legacy_enr_entry_point_falls_back_to_full_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE enr_legacy_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO enr_legacy_src VALUES (1, 10)")
        .await;
    db.create_st(
        "enr_legacy_st",
        "SELECT id, val FROM enr_legacy_src",
        "1m",
        "IMMEDIATE",
    )
    .await;
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'enr_legacy_st'",
        )
        .await;
    let source_oid: i32 = db
        .query_scalar("SELECT 'enr_legacy_src'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "DROP TRIGGER pgt_ivm_after_upd_{pgt_id} ON enr_legacy_src"
    ))
    .await;
    db.execute("UPDATE enr_legacy_src SET val = 20 WHERE id = 1")
        .await;
    db.execute(&format!(
        "SELECT pgtrickle.pgt_ivm_apply_delta_enr({pgt_id}, {source_oid}, true, true)"
    ))
    .await;

    db.assert_st_matches_query("enr_legacy_st", "SELECT id, val FROM enr_legacy_src")
        .await;
}

// ── Aggregate parity ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivm_enr_parity_aggregate() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t_enr_agg (id INT PRIMARY KEY, grp TEXT, amt NUMERIC)")
        .await;
    db.execute("INSERT INTO t_enr_agg VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)")
        .await;

    let query = "SELECT grp, SUM(amt) AS total FROM t_enr_agg GROUP BY grp";

    create_immediate_st_with_enr(&db, "st_enr_agg", query, true).await;
    create_immediate_st_with_enr(&db, "st_tmp_agg", query, false).await;

    db.assert_st_matches_query("st_enr_agg", query).await;
    db.assert_st_matches_query("st_tmp_agg", query).await;

    // Insert a new row into group A.
    db.execute("INSERT INTO t_enr_agg VALUES (4, 'A', 5)").await;

    db.assert_st_matches_query("st_enr_agg", query).await;
    db.assert_st_matches_query("st_tmp_agg", query).await;

    // Both should show the same totals.
    let enr_total_a: String = db
        .query_scalar("SELECT total::text FROM public.st_enr_agg WHERE grp = 'A'")
        .await;
    let tmp_total_a: String = db
        .query_scalar("SELECT total::text FROM public.st_tmp_agg WHERE grp = 'A'")
        .await;
    assert_eq!(
        enr_total_a, tmp_total_a,
        "ENR and temp-table modes should produce identical aggregate values"
    );
    assert_eq!(enr_total_a, "35");
}
