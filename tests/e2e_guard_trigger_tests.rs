//! E2E tests for stream table guard triggers (EC-25 + EC-26).
//!
//! Validates that direct DML (INSERT/UPDATE/DELETE) and TRUNCATE on
//! stream tables is blocked by the guard triggers installed at creation.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// EC-26: Direct INSERT on stream table is blocked
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guard_trigger_blocks_direct_insert() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE guard_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO guard_src VALUES (1, 'a'), (2, 'b')")
        .await;

    let q = "SELECT id, val FROM guard_src";
    db.create_st("guard_ins_st", q, "1m", "DIFFERENTIAL").await;

    // Direct INSERT should be rejected
    let result = db
        .try_execute("INSERT INTO guard_ins_st (__pgt_row_id, id, val) VALUES (pgtrickle.encode_row_id_v2('SCAN_KEY', ROW(99::int)), 99, 'bad')")
        .await;
    assert!(
        result.is_err(),
        "Direct INSERT on stream table should be blocked"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Direct DML on stream table"),
        "Error should mention guard: {err_msg}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// EC-26: Direct UPDATE on stream table is blocked
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guard_trigger_blocks_direct_update() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE guard_upd_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO guard_upd_src VALUES (1, 'a')")
        .await;

    let q = "SELECT id, val FROM guard_upd_src";
    db.create_st("guard_upd_st", q, "1m", "DIFFERENTIAL").await;

    // Direct UPDATE should be rejected
    let result = db
        .try_execute("UPDATE guard_upd_st SET val = 'hacked'")
        .await;
    assert!(
        result.is_err(),
        "Direct UPDATE on stream table should be blocked"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Direct DML on stream table"),
        "Error should mention guard: {err_msg}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// EC-26: Direct DELETE on stream table is blocked
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guard_trigger_blocks_direct_delete() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE guard_del_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO guard_del_src VALUES (1, 'a')")
        .await;

    let q = "SELECT id, val FROM guard_del_src";
    db.create_st("guard_del_st", q, "1m", "DIFFERENTIAL").await;

    // Direct DELETE should be rejected
    let result = db.try_execute("DELETE FROM guard_del_st").await;
    assert!(
        result.is_err(),
        "Direct DELETE on stream table should be blocked"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Direct DML on stream table"),
        "Error should mention guard: {err_msg}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// EC-25: TRUNCATE on stream table is blocked
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guard_trigger_blocks_truncate() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE guard_trunc_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO guard_trunc_src VALUES (1, 'a'), (2, 'b')")
        .await;

    let q = "SELECT id, val FROM guard_trunc_src";
    db.create_st("guard_trunc_st", q, "1m", "DIFFERENTIAL")
        .await;

    // TRUNCATE should be rejected
    let result = db.try_execute("TRUNCATE guard_trunc_st").await;
    assert!(
        result.is_err(),
        "TRUNCATE on stream table should be blocked"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Direct DML on stream table"),
        "Error should mention guard: {err_msg}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// EC-26: Managed refresh still works (guard bypassed internally)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_guard_trigger_allows_managed_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE guard_ok_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO guard_ok_src VALUES (1, 10), (2, 20)")
        .await;

    let q = "SELECT id, val FROM guard_ok_src";
    db.create_st("guard_ok_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("guard_ok_st", q).await;

    // Make a change and refresh — should succeed despite guard
    db.execute("INSERT INTO guard_ok_src VALUES (3, 30)").await;
    db.refresh_st("guard_ok_st").await;
    db.assert_st_matches_query("guard_ok_st", q).await;
}
