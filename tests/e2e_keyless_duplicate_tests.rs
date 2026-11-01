//! E2E tests for keyless / duplicate-row table differential correctness (EC-06).
//!
//! Validates that tables without primary keys (using net-counting delta
//! via `has_keyless_source`) handle duplicate rows correctly under
//! differential refresh: identical rows, delete-one-of-duplicates,
//! update-one-of-duplicates, mixed DML stress.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Basic keyless table with duplicates
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_keyless_duplicate_rows_basic() {
    let db = E2eDb::new().await.with_extension().await;
    // Keyless: no PRIMARY KEY — pgtrickle uses ctid-based row identity
    db.execute("CREATE TABLE kl_dup (val INT, label TEXT)")
        .await;
    db.execute("ALTER TABLE kl_dup REPLICA IDENTITY FULL").await;
    db.execute("INSERT INTO kl_dup VALUES (1, 'a'), (1, 'a'), (1, 'a'), (2, 'b')")
        .await;

    let q = "SELECT val, label FROM kl_dup";
    db.create_st("kl_dup_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("kl_dup_st", q).await;

    // Insert another exact duplicate
    db.execute("INSERT INTO kl_dup VALUES (1, 'a')").await;
    db.refresh_st_until_converged("kl_dup_st").await;
    db.assert_st_matches_query("kl_dup_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Delete one of identical rows
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_keyless_delete_one_duplicate() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE kl_del (val INT, label TEXT)")
        .await;
    db.execute("ALTER TABLE kl_del REPLICA IDENTITY FULL").await;
    db.execute("INSERT INTO kl_del VALUES (1, 'x'), (1, 'x'), (2, 'y')")
        .await;

    let q = "SELECT val, label FROM kl_del";
    db.create_st("kl_del_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("kl_del_st", q).await;

    // Delete exactly one of the duplicates using ctid
    db.execute("DELETE FROM kl_del WHERE ctid = (SELECT MIN(ctid) FROM kl_del WHERE val = 1)")
        .await;
    db.refresh_st("kl_del_st").await;
    db.assert_st_matches_query("kl_del_st", q).await;

    // Verify count is correct (originally 3, deleted 1 → 2)
    let cnt: i64 = db.query_scalar("SELECT COUNT(*) FROM kl_del_st").await;
    assert_eq!(cnt, 2);
}

// ═══════════════════════════════════════════════════════════════════════
// Update one of identical rows
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_keyless_update_one_duplicate() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE kl_upd (val INT, label TEXT)")
        .await;
    db.execute("ALTER TABLE kl_upd REPLICA IDENTITY FULL").await;
    db.execute("INSERT INTO kl_upd VALUES (1, 'x'), (1, 'x'), (1, 'x')")
        .await;

    let q = "SELECT val, label FROM kl_upd";
    db.create_st("kl_upd_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("kl_upd_st", q).await;

    // Update exactly one duplicate
    db.execute(
        "UPDATE kl_upd SET label = 'CHANGED' \
         WHERE ctid = (SELECT MIN(ctid) FROM kl_upd WHERE val = 1)",
    )
    .await;
    db.refresh_st("kl_upd_st").await;
    db.assert_st_matches_query("kl_upd_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Keyless with aggregate (GROUP BY on duplicate-rich data)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_keyless_aggregate_with_duplicates() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE kl_agg (cat TEXT, val INT)").await;
    db.execute("ALTER TABLE kl_agg REPLICA IDENTITY FULL").await;
    db.execute("INSERT INTO kl_agg VALUES ('a', 1), ('a', 1), ('a', 2), ('b', 1)")
        .await;

    let q = "SELECT cat, SUM(val) AS total, COUNT(*) AS cnt FROM kl_agg GROUP BY cat";
    db.create_st("kl_agg_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("kl_agg_st", q).await;

    // Delete one duplicate
    db.execute(
        "DELETE FROM kl_agg WHERE ctid = (SELECT MIN(ctid) FROM kl_agg WHERE cat = 'a' AND val = 1)",
    )
    .await;
    db.refresh_st("kl_agg_st").await;
    db.assert_st_matches_query("kl_agg_st", q).await;

    // Insert more duplicates
    db.execute("INSERT INTO kl_agg VALUES ('a', 1), ('a', 1), ('b', 1)")
        .await;
    db.refresh_st("kl_agg_st").await;
    db.assert_st_matches_query("kl_agg_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Keyless with unique content (no duplicates — baseline)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_keyless_unique_content() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE kl_uniq (val INT, label TEXT)")
        .await;
    db.execute("ALTER TABLE kl_uniq REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO kl_uniq VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    let q = "SELECT val, label FROM kl_uniq";
    db.create_st("kl_uniq_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("kl_uniq_st", q).await;

    db.execute("INSERT INTO kl_uniq VALUES (4, 'd')").await;
    db.refresh_st("kl_uniq_st").await;
    db.assert_st_matches_query("kl_uniq_st", q).await;

    db.execute("DELETE FROM kl_uniq WHERE val = 2").await;
    db.refresh_st("kl_uniq_st").await;
    db.assert_st_matches_query("kl_uniq_st", q).await;

    db.execute("UPDATE kl_uniq SET label = 'updated' WHERE val = 1")
        .await;
    db.refresh_st("kl_uniq_st").await;
    db.assert_st_matches_query("kl_uniq_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Keyless: delete all duplicates then re-insert
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_keyless_delete_all_then_reinsert() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE kl_cycle (val INT)").await;
    db.execute("ALTER TABLE kl_cycle REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO kl_cycle VALUES (1), (1), (1)")
        .await;

    let q = "SELECT val FROM kl_cycle";
    db.create_st("kl_cycle_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("kl_cycle_st", q).await;

    // Delete all
    db.execute("DELETE FROM kl_cycle").await;
    db.refresh_st("kl_cycle_st").await;
    db.assert_st_matches_query("kl_cycle_st", q).await;

    // Re-insert
    db.execute("INSERT INTO kl_cycle VALUES (1), (2)").await;
    db.refresh_st("kl_cycle_st").await;
    db.assert_st_matches_query("kl_cycle_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Keyless: mixed DML stress with many identical rows
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_keyless_mixed_dml_stress() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE kl_stress (cat TEXT, val INT)")
        .await;
    db.execute("ALTER TABLE kl_stress REPLICA IDENTITY FULL")
        .await;
    db.execute(
        "INSERT INTO kl_stress \
         SELECT 'a', 1 FROM generate_series(1, 10) \
         UNION ALL \
         SELECT 'b', 2 FROM generate_series(1, 5)",
    )
    .await;

    let q = "SELECT cat, val FROM kl_stress";
    db.create_st("kl_stress_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("kl_stress_st", q).await;

    // Delete half of 'a' rows
    db.execute(
        "DELETE FROM kl_stress WHERE ctid IN \
         (SELECT ctid FROM kl_stress WHERE cat = 'a' LIMIT 5)",
    )
    .await;
    db.refresh_st("kl_stress_st").await;
    db.assert_st_matches_query("kl_stress_st", q).await;

    // Update remaining 'a' rows
    db.execute("UPDATE kl_stress SET val = 99 WHERE cat = 'a'")
        .await;
    db.refresh_st("kl_stress_st").await;
    db.assert_st_matches_query("kl_stress_st", q).await;

    // Insert more duplicates
    db.execute("INSERT INTO kl_stress SELECT 'c', 3 FROM generate_series(1, 8)")
        .await;
    db.refresh_st("kl_stress_st").await;
    db.assert_st_matches_query("kl_stress_st", q).await;
}
