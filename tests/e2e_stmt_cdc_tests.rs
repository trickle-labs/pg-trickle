//! E2E tests for B1/B2: Statement-Level CDC Triggers.
//!
//! Validates that:
//! - The default `pg_trickle.cdc_trigger_mode = 'statement'` creates a
//!   `FOR EACH STATEMENT REFERENCING NEW TABLE AS __pgt_new OLD TABLE AS
//!   __pgt_old` trigger (not a per-row trigger).
//! - Bulk DML (INSERT/UPDATE/DELETE) is captured correctly in a single pass.
//! - Keyless table UPDATE is modelled as D + I rows (no PK join possible).
//! - Setting the GUC to `'row'` creates the legacy row-level trigger.
//! - `pgtrickle.rebuild_cdc_triggers()` migrates an existing row-level trigger
//!   to statement-level when the GUC is switched.

mod e2e;

use e2e::E2eDb;

/// Disable the background scheduler for the current test database.
async fn disable_scheduler(db: &E2eDb) {
    db.execute(
        "DO $$ BEGIN \
           EXECUTE format('ALTER DATABASE %I SET pg_trickle.enabled = off', current_database()); \
         END $$",
    )
    .await;
    db.execute(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = current_database() \
           AND backend_type = 'pg_trickle scheduler' \
           AND pid <> pg_backend_pid()",
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

// ── Helper ────────────────────────────────────────────────────────────────

/// Return the `tgtype & 1` value for the named trigger (0 = STATEMENT, 1 = ROW).
async fn trigger_row_bit(db: &E2eDb, trigger_name: &str) -> i32 {
    db.query_scalar(&format!(
        "SELECT (tgtype & 1)::int FROM pg_trigger WHERE tgname = '{trigger_name}'"
    ))
    .await
}

/// Return the transition NEW TABLE name recorded in `pg_trigger`, or `""`.
async fn trigger_new_table(db: &E2eDb, trigger_name: &str) -> String {
    db.query_scalar(&format!(
        "SELECT COALESCE(tgnewtable::text, '') \
         FROM pg_trigger WHERE tgname = '{trigger_name}'"
    ))
    .await
}

/// Return the transition OLD TABLE name recorded in `pg_trigger`, or `""`.
async fn trigger_old_table(db: &E2eDb, trigger_name: &str) -> String {
    db.query_scalar(&format!(
        "SELECT COALESCE(tgoldtable::text, '') \
         FROM pg_trigger WHERE tgname = '{trigger_name}'"
    ))
    .await
}

// ── Tests ─────────────────────────────────────────────────────────────────

/// Default GUC creates a `FOR EACH STATEMENT` trigger with transition table
/// references (`__pgt_new` / `__pgt_old`).
#[tokio::test]
async fn test_stmt_cdc_default_trigger_is_statement_level() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE stmt_type_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.create_st(
        "stmt_type_st",
        "SELECT id, val FROM stmt_type_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("stmt_type_src").await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT source_stable_name FROM pgtrickle.pgt_change_tracking \
             WHERE source_relid = {}",
            source_oid
        ))
        .await;
    let ins_trigger = format!("pg_trickle_cdc_ins_{}", stable_name);
    let upd_trigger = format!("pg_trickle_cdc_upd_{}", stable_name);
    let del_trigger = format!("pg_trickle_cdc_del_{}", stable_name);

    // All three per-event triggers must be statement-level (tgtype & 1 = 0).
    assert_eq!(
        trigger_row_bit(&db, &ins_trigger).await,
        0,
        "INSERT CDC trigger should be statement-level (tgtype & 1 = 0)"
    );
    assert_eq!(
        trigger_row_bit(&db, &upd_trigger).await,
        0,
        "UPDATE CDC trigger should be statement-level (tgtype & 1 = 0)"
    );
    assert_eq!(
        trigger_row_bit(&db, &del_trigger).await,
        0,
        "DELETE CDC trigger should be statement-level (tgtype & 1 = 0)"
    );

    // INSERT trigger references only __pgt_new.
    assert_eq!(
        trigger_new_table(&db, &ins_trigger).await,
        "__pgt_new",
        "INSERT trigger: tgnewtable should be __pgt_new"
    );
    assert_eq!(
        trigger_old_table(&db, &ins_trigger).await,
        "",
        "INSERT trigger should not reference __pgt_old"
    );

    // UPDATE trigger references both __pgt_new and __pgt_old.
    assert_eq!(
        trigger_new_table(&db, &upd_trigger).await,
        "__pgt_new",
        "UPDATE trigger: tgnewtable should be __pgt_new"
    );
    assert_eq!(
        trigger_old_table(&db, &upd_trigger).await,
        "__pgt_old",
        "UPDATE trigger: tgoldtable should be __pgt_old"
    );

    // DELETE trigger references only __pgt_old.
    assert_eq!(
        trigger_new_table(&db, &del_trigger).await,
        "",
        "DELETE trigger should not reference __pgt_new"
    );
    assert_eq!(
        trigger_old_table(&db, &del_trigger).await,
        "__pgt_old",
        "DELETE trigger: tgoldtable should be __pgt_old"
    );
}

/// A bulk `INSERT … SELECT` is captured in a single trigger firing: the change
/// buffer receives exactly one row per inserted source row.
#[tokio::test]
async fn test_stmt_cdc_bulk_insert_all_rows_captured() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE bulk_ins (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.create_st(
        "bulk_ins_st",
        "SELECT id, val FROM bulk_ins",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("bulk_ins_st").await;

    let source_oid = db.table_oid("bulk_ins").await;
    let buf = db.change_buffer_table(source_oid as i64).await;

    // Bulk-insert 100 rows in one SQL statement.
    db.execute(
        "INSERT INTO bulk_ins \
         SELECT i, 'val' || i::text FROM generate_series(1, 100) AS i",
    )
    .await;

    let change_count: i64 = db
        .query_scalar(&format!(
            "SELECT COUNT(*) FROM {buf} WHERE action IN ('I', 'D')"
        ))
        .await;
    assert_eq!(
        change_count, 100,
        "All 100 rows should appear in the change buffer"
    );

    let action: String = db
        .query_scalar(&format!(
            "SELECT DISTINCT action FROM {buf} WHERE action IN ('I', 'D')"
        ))
        .await;
    assert_eq!(action, "I", "All change rows should have action='I'");

    // Differential refresh should materialise all 100 rows.
    db.refresh_st("bulk_ins_st").await;
    assert_eq!(
        db.count("public.bulk_ins_st").await,
        100,
        "Stream table should have all 100 rows after refresh"
    );
}

/// A bulk `UPDATE` on a keyed table captures `'U'` rows with both `new_*` and
/// `old_*` columns populated.  Differential refresh converges to the source.
#[tokio::test]
async fn test_stmt_cdc_bulk_update_keyed_table() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE bulk_upd (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "INSERT INTO bulk_upd \
         SELECT i, 'initial_' || i::text FROM generate_series(1, 50) AS i",
    )
    .await;
    db.create_st(
        "bulk_upd_st",
        "SELECT id, val FROM bulk_upd",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("bulk_upd_st").await;

    let source_oid = db.table_oid("bulk_upd").await;
    let buf = db.change_buffer_table(source_oid as i64).await;
    // Clear post-create changes from the buffer.
    db.execute(&format!("DELETE FROM {buf} WHERE action <> 'S'"))
        .await;

    // Bulk-update all 50 rows in one statement.
    db.execute("UPDATE bulk_upd SET val = 'updated_' || id::text")
        .await;

    let change_count: i64 = db
        .query_scalar(&format!(
            "SELECT COUNT(*) FROM {buf} WHERE action IN ('I', 'D')"
        ))
        .await;
    // A44-10 D+I: keyed UPDATE of 50 rows → 50 D-rows + 50 I-rows = 100 total.
    assert_eq!(
        change_count, 100,
        "50 updated rows → 100 change rows (50 D + 50 I) in D+I schema"
    );

    let d_count: i64 = db
        .query_scalar(&format!("SELECT COUNT(*) FROM {buf} WHERE action = 'D'"))
        .await;
    let i_count: i64 = db
        .query_scalar(&format!("SELECT COUNT(*) FROM {buf} WHERE action = 'I'"))
        .await;
    assert_eq!(d_count, 50, "50 D-rows from keyed bulk UPDATE");
    assert_eq!(i_count, 50, "50 I-rows from keyed bulk UPDATE");

    // D-row for id=25 carries the OLD value; I-row carries the NEW value.
    let old_val: String = db
        .query_scalar(&format!(
            "SELECT val FROM {buf} WHERE id = 25 AND action = 'D'"
        ))
        .await;
    assert_eq!(
        old_val, "initial_25",
        "D-row val should contain the pre-update value"
    );

    let new_val: String = db
        .query_scalar(&format!(
            "SELECT val FROM {buf} WHERE id = 25 AND action = 'I'"
        ))
        .await;
    assert_eq!(
        new_val, "updated_25",
        "I-row val should reflect the updated value"
    );

    // Differential refresh must converge to the source.
    db.refresh_st("bulk_upd_st").await;
    db.assert_st_matches_query("public.bulk_upd_st", "SELECT id, val FROM bulk_upd")
        .await;
}

/// A bulk `DELETE` produces `'D'` rows with `old_*` columns populated.
#[tokio::test]
async fn test_stmt_cdc_bulk_delete_keyed_table() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE bulk_del (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "INSERT INTO bulk_del \
         SELECT i, 'v' || i::text FROM generate_series(1, 30) AS i",
    )
    .await;
    db.create_st(
        "bulk_del_st",
        "SELECT id, val FROM bulk_del",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("bulk_del_st").await;

    let source_oid = db.table_oid("bulk_del").await;
    let buf = db.change_buffer_table(source_oid as i64).await;
    db.execute(&format!("DELETE FROM {buf} WHERE action <> 'S'"))
        .await;

    // Delete all rows where id is odd (15 rows).
    db.execute("DELETE FROM bulk_del WHERE id % 2 = 1").await;

    let change_count: i64 = db
        .query_scalar(&format!(
            "SELECT COUNT(*) FROM {buf} WHERE action IN ('I', 'D')"
        ))
        .await;
    assert_eq!(
        change_count, 15,
        "All 15 deleted rows should appear in the change buffer"
    );

    let action: String = db
        .query_scalar(&format!(
            "SELECT DISTINCT action FROM {buf} WHERE action IN ('I', 'D')"
        ))
        .await;
    assert_eq!(action, "D", "All change rows should have action='D'");

    // Differential refresh on a FULL-refresh ST: final count = 15.
    db.refresh_st("bulk_del_st").await;
    assert_eq!(
        db.count("public.bulk_del_st").await,
        15,
        "Stream table should have 15 remaining rows after refresh"
    );
}

/// For a keyless table (no PRIMARY KEY), statement-level triggers cannot JOIN
/// `__pgt_new` with `__pgt_old` — UPDATE is instead modelled as a DELETE from
/// `__pgt_old` followed by INSERT from `__pgt_new`.
#[tokio::test]
async fn test_stmt_cdc_keyless_update_captured_as_delete_plus_insert() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    // Keyless table — no PRIMARY KEY.
    db.execute("CREATE TABLE keyless_upd (val TEXT)").await;
    db.execute("INSERT INTO keyless_upd VALUES ('a'), ('b'), ('c')")
        .await;
    db.create_st(
        "keyless_upd_st",
        "SELECT val FROM keyless_upd",
        "1m",
        "FULL",
    )
    .await;
    db.refresh_st("keyless_upd_st").await;

    let source_oid = db.table_oid("keyless_upd").await;
    let buf = db.change_buffer_table(source_oid as i64).await;
    db.execute(&format!("DELETE FROM {buf} WHERE action <> 'S'"))
        .await;

    // Update all 3 rows in a single statement.
    db.execute("UPDATE keyless_upd SET val = val || '_upd'")
        .await;

    let total: i64 = db
        .query_scalar(&format!(
            "SELECT COUNT(*) FROM {buf} WHERE action IN ('I', 'D')"
        ))
        .await;
    assert_eq!(
        total, 6,
        "Keyless UPDATE of 3 rows should produce 3 D + 3 I = 6 change rows"
    );

    let d_count: i64 = db
        .query_scalar(&format!("SELECT COUNT(*) FROM {buf} WHERE action = 'D'"))
        .await;
    let i_count: i64 = db
        .query_scalar(&format!("SELECT COUNT(*) FROM {buf} WHERE action = 'I'"))
        .await;
    assert_eq!(d_count, 3, "3 DELETE rows expected for keyless UPDATE");
    assert_eq!(i_count, 3, "3 INSERT rows expected for keyless UPDATE");

    // Full refresh should still see the updated values.
    db.refresh_st("keyless_upd_st").await;
    db.assert_st_matches_query("public.keyless_upd_st", "SELECT val FROM keyless_upd")
        .await;
}

/// When `pg_trickle.cdc_trigger_mode = 'row'`, newly created stream tables
/// receive a `FOR EACH ROW` trigger with no transition table references.
#[tokio::test]
async fn test_stmt_cdc_row_mode_guc_creates_row_level_trigger() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE row_mode_src (id INT PRIMARY KEY, val TEXT)")
        .await;

    // Switch to row-level trigger mode for this session and create the ST
    // on the same connection so the GUC is visible to create_stream_table.
    db.execute_seq(&[
        "SET pg_trickle.cdc_trigger_mode = 'row'",
        "SELECT pgtrickle.create_stream_table('row_mode_st', $$SELECT id, val FROM row_mode_src$$, '1m', 'DIFFERENTIAL')",
    ])
    .await;

    let source_oid = db.table_oid("row_mode_src").await;
    // v0.32.0+: trigger names use stable hash. Row-level trigger has no ins/upd/del suffix.
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT source_stable_name FROM pgtrickle.pgt_change_tracking \
             WHERE source_relid = {}",
            source_oid
        ))
        .await;
    let trigger_name = format!("pg_trickle_cdc_{}", stable_name);

    // tgtype bit 0 = 1 → FOR EACH ROW
    assert_eq!(
        trigger_row_bit(&db, &trigger_name).await,
        1,
        "CDC trigger should be row-level (tgtype & 1 = 1) when GUC = 'row'"
    );

    // Row-level triggers have no transition table references.
    assert_eq!(
        trigger_new_table(&db, &trigger_name).await,
        "",
        "Row-level trigger should not have a tgnewtable reference"
    );
    assert_eq!(
        trigger_old_table(&db, &trigger_name).await,
        "",
        "Row-level trigger should not have a tgoldtable reference"
    );

    // DML still works correctly in row mode.
    db.execute("INSERT INTO row_mode_src VALUES (1, 'x'), (2, 'y')")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.cdc_trigger_mode = 'row'",
        "SELECT pgtrickle.refresh_stream_table('row_mode_st')",
    ])
    .await;
    assert_eq!(
        db.count("public.row_mode_st").await,
        2,
        "Row-level trigger should still capture DML correctly"
    );
}

/// `pgtrickle.rebuild_cdc_triggers()` replaces an existing row-level trigger
/// with a statement-level trigger when `pg_trickle.cdc_trigger_mode` is
/// switched to `'statement'`.  DML continues to be captured correctly after
/// the rebuild.
#[tokio::test]
async fn test_stmt_cdc_rebuild_cdc_triggers_migrates_to_statement() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    // 1. Initially create the stream table with row-level triggers.
    db.execute("CREATE TABLE rebuild_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.cdc_trigger_mode = 'row'",
        "SELECT pgtrickle.create_stream_table('rebuild_st', 'SELECT id, val FROM rebuild_src', schedule := '1m', refresh_mode := 'DIFFERENTIAL')",
    ])
    .await;

    let source_oid = db.table_oid("rebuild_src").await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT source_stable_name FROM pgtrickle.pgt_change_tracking \
             WHERE source_relid = {}",
            source_oid
        ))
        .await;
    let row_trigger = format!("pg_trickle_cdc_{}", stable_name);
    let ins_trigger = format!("pg_trickle_cdc_ins_{}", stable_name);
    let upd_trigger = format!("pg_trickle_cdc_upd_{}", stable_name);

    // Confirm row-level trigger before migration.
    assert_eq!(
        trigger_row_bit(&db, &row_trigger).await,
        1,
        "Should start as row-level trigger (bit = 1)"
    );

    // 2. Switch to statement mode and call rebuild_cdc_triggers().
    db.execute_seq(&[
        "SET pg_trickle.cdc_trigger_mode = 'statement'",
        "SELECT pgtrickle.rebuild_cdc_triggers()",
    ])
    .await;

    // 3. Per-event statement-level triggers should now exist.
    assert_eq!(
        trigger_row_bit(&db, &ins_trigger).await,
        0,
        "After rebuild_cdc_triggers() INSERT trigger should be statement-level (bit = 0)"
    );
    assert_eq!(
        trigger_new_table(&db, &ins_trigger).await,
        "__pgt_new",
        "Rebuilt INSERT trigger should reference __pgt_new transition table"
    );
    assert_eq!(
        trigger_new_table(&db, &upd_trigger).await,
        "__pgt_new",
        "Rebuilt UPDATE trigger should reference __pgt_new transition table"
    );
    assert_eq!(
        trigger_old_table(&db, &upd_trigger).await,
        "__pgt_old",
        "Rebuilt UPDATE trigger should reference __pgt_old transition table"
    );

    // 4. DML is still captured correctly after the trigger rebuild.
    db.execute("INSERT INTO rebuild_src VALUES (1, 'alpha'), (2, 'beta')")
        .await;
    db.refresh_st("rebuild_st").await;
    assert_eq!(
        db.count("public.rebuild_st").await,
        2,
        "Rebuilt statement-level trigger should capture DML correctly"
    );
    db.assert_st_matches_query("public.rebuild_st", "SELECT id, val FROM rebuild_src")
        .await;
}

/// Statement-level trigger correctly captures a mix of INSERT, UPDATE, and
/// DELETE in the same transaction, and differential refresh converges to the
/// source.
#[tokio::test]
async fn test_stmt_cdc_mixed_dml_in_transaction() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE mixed_dml (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO mixed_dml SELECT i, 'v' || i::text FROM generate_series(1, 10) AS i")
        .await;
    db.create_st(
        "mixed_dml_st",
        "SELECT id, val FROM mixed_dml",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("mixed_dml_st").await;

    let source_oid = db.table_oid("mixed_dml").await;
    let buf = db.change_buffer_table(source_oid as i64).await;
    db.execute(&format!("DELETE FROM {buf} WHERE action <> 'S'"))
        .await;

    // Mix of DML in a single transaction.
    // Use a dedicated connection so BEGIN/DML/COMMIT all run on the same
    // session — pool.execute() could dispatch each call to a different
    // connection, leaving some trigger change-buffer rows uncommitted.
    {
        let mut conn = db.pool.acquire().await.expect("acquire connection");
        sqlx::query("BEGIN").execute(&mut *conn).await.unwrap();
        sqlx::query("DELETE FROM mixed_dml WHERE id IN (1, 2, 3)")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("UPDATE mixed_dml SET val = 'updated' WHERE id IN (4, 5)")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO mixed_dml VALUES (11, 'new11'), (12, 'new12')")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("COMMIT").execute(&mut *conn).await.unwrap();
    }

    // A44-10 D+I: 3 DEL + 2 UPD (→ 2 D + 2 I) + 2 INS = 9 change rows.
    let total: i64 = db
        .query_scalar(&format!(
            "SELECT COUNT(*) FROM {buf} WHERE action IN ('I', 'D')"
        ))
        .await;
    assert_eq!(
        total, 9,
        "9 change rows expected: 5 D + 4 I (UPDs decomposed)"
    );

    let d_count: i64 = db
        .query_scalar(&format!("SELECT COUNT(*) FROM {buf} WHERE action = 'D'"))
        .await;
    let i_count: i64 = db
        .query_scalar(&format!("SELECT COUNT(*) FROM {buf} WHERE action = 'I'"))
        .await;
    assert_eq!(d_count, 5, "5 D-rows: 3 from DELETE + 2 from UPDATE D+I");
    assert_eq!(i_count, 4, "4 I-rows: 2 from UPDATE D+I + 2 from INSERT");

    // Differential refresh must converge to the source.
    db.refresh_st("mixed_dml_st").await;
    db.assert_st_matches_query("public.mixed_dml_st", "SELECT id, val FROM mixed_dml")
        .await;
}
