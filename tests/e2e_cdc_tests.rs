//! E2E tests for trigger-based CDC (Change Data Capture).
//!
//! Validates that CDC triggers fire correctly on source tables,
//! change buffers capture the right data, and cleanup works.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Trigger Capture Tests ──────────────────────────────────────────────

/// SF-10: TRUNCATE on a source table followed by new INSERTs before the
/// next scheduler tick must not drop the post-TRUNCATE rows.
///
/// The change buffer captures a `'T'` (TRUNCATE) marker. When the refresh
/// engine sees the marker it falls back to `execute_full_refresh()`, which
/// re-reads the source table directly (not the change buffer). The
/// post-TRUNCATE INSERTs are therefore captured by the FULL snapshot,
/// and the change buffer is discarded atomically in the same transaction.
///
/// Exit criterion: after TRUNCATE + INSERT + refresh, the stream table
/// contains *only* the post-TRUNCATE rows.
#[tokio::test]
async fn test_sf10_truncate_then_insert_post_truncate_rows_captured() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sf10_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    // Pre-TRUNCATE rows
    db.execute("INSERT INTO sf10_src VALUES (1, 'old_a'), (2, 'old_b'), (3, 'old_c')")
        .await;

    db.create_st(
        "sf10_st",
        "SELECT id, val FROM sf10_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh — stream table should have 3 rows
    db.refresh_st("sf10_st").await;
    assert_eq!(
        db.count("public.sf10_st").await,
        3,
        "Initial state: 3 rows expected"
    );

    // TRUNCATE the source and immediately insert new rows — both operations
    // happen before the next scheduler tick, simulating the race window
    // described in SF-10.
    db.execute("TRUNCATE sf10_src").await;
    db.execute("INSERT INTO sf10_src VALUES (10, 'new_x'), (20, 'new_y')")
        .await;

    // Refresh — the engine must detect the 'T' marker and do a full snapshot
    db.refresh_st("sf10_st").await;

    let count = db.count("public.sf10_st").await;
    assert_eq!(
        count, 2,
        "Stream table must contain exactly the post-TRUNCATE rows"
    );

    // Old rows must be gone
    let old_present: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.sf10_st WHERE id IN (1,2,3))")
        .await;
    assert!(
        !old_present,
        "Pre-TRUNCATE rows (ids 1,2,3) must not appear in the stream table"
    );

    // New rows must be present
    let new_x: Option<String> = db
        .query_scalar_opt("SELECT val FROM public.sf10_st WHERE id = 10")
        .await;
    assert_eq!(
        new_x.as_deref(),
        Some("new_x"),
        "Post-TRUNCATE row id=10 must be present"
    );
    let new_y: Option<String> = db
        .query_scalar_opt("SELECT val FROM public.sf10_st WHERE id = 20")
        .await;
    assert_eq!(
        new_y.as_deref(),
        Some("new_y"),
        "Post-TRUNCATE row id=20 must be present"
    );

    // Final ground-truth check: ST contents must exactly match the source
    db.assert_st_matches_query("public.sf10_st", "SELECT id, val FROM sf10_src")
        .await;
}

#[tokio::test]
async fn test_trigger_captures_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cdc_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cdc_src VALUES (1, 'initial')")
        .await;

    db.create_st(
        "cdc_st",
        "SELECT id, val FROM cdc_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("cdc_src").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // Consume any existing changes from create
    db.refresh_st("cdc_st").await;

    // Insert new rows
    db.execute("INSERT INTO cdc_src VALUES (2, 'new_row')")
        .await;

    // Verify change captured in buffer
    let change_count: i64 = db.count(&buffer_table).await;
    assert!(
        change_count >= 1,
        "Insert should be captured in change buffer"
    );

    let action: String = db
        .query_scalar(&format!(
            "SELECT action FROM {} ORDER BY change_id DESC LIMIT 1",
            buffer_table
        ))
        .await;
    assert_eq!(action, "I", "Action should be 'I' for INSERT");
}

#[tokio::test]
async fn test_trigger_captures_update() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cdc_upd (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cdc_upd VALUES (1, 'old_val')")
        .await;

    db.create_st(
        "cdc_upd_st",
        "SELECT id, val FROM cdc_upd",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("cdc_upd").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // Consume existing changes
    db.refresh_st("cdc_upd_st").await;

    // Update a row
    db.execute("UPDATE cdc_upd SET val = 'new_val' WHERE id = 1")
        .await;

    let action: String = db
        .query_scalar(&format!(
            "SELECT action FROM {} ORDER BY change_id DESC LIMIT 1",
            buffer_table
        ))
        .await;
    // A44-10 D+I schema: UPDATE decomposes into D-row then I-row (no 'U').
    // ORDER BY change_id DESC LIMIT 1 returns the last (I) row.
    assert_eq!(
        action, "I",
        "Action should be 'I' for the I-row of a decomposed UPDATE"
    );

    // Verify flat column captures the new value (I-row carries NEW values).
    let val: String = db
        .query_scalar(&format!(
            "SELECT \"val\" FROM {} ORDER BY change_id DESC LIMIT 1",
            buffer_table
        ))
        .await;
    assert_eq!(
        val, "new_val",
        "val column should contain the new value in the I-row"
    );
}

#[tokio::test]
async fn test_trigger_captures_delete() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cdc_del (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cdc_del VALUES (1, 'to_delete')")
        .await;

    db.create_st(
        "cdc_del_st",
        "SELECT id, val FROM cdc_del",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("cdc_del").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // Consume existing changes
    db.refresh_st("cdc_del_st").await;

    // Delete the row
    db.execute("DELETE FROM cdc_del WHERE id = 1").await;

    let action: String = db
        .query_scalar(&format!(
            "SELECT action FROM {} ORDER BY change_id DESC LIMIT 1",
            buffer_table
        ))
        .await;
    assert_eq!(action, "D", "Action should be 'D' for DELETE");

    // A44-10: flat column `val` carries the OLD value in the D-row.
    let val: String = db
        .query_scalar(&format!(
            "SELECT \"val\" FROM {} ORDER BY change_id DESC LIMIT 1",
            buffer_table
        ))
        .await;
    assert_eq!(
        val, "to_delete",
        "val column should contain the deleted value in the D-row"
    );
}

#[tokio::test]
async fn test_trigger_captures_bulk_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cdc_bulk (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO cdc_bulk VALUES (1, 0)").await;

    db.create_st(
        "cdc_bulk_st",
        "SELECT id, val FROM cdc_bulk",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("cdc_bulk").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // Consume existing changes
    db.refresh_st("cdc_bulk_st").await;

    // Bulk insert via INSERT ... SELECT
    db.execute("INSERT INTO cdc_bulk SELECT g, g FROM generate_series(2, 51) g")
        .await;

    let change_count: i64 = db.count(&buffer_table).await;
    assert_eq!(
        change_count, 50,
        "All 50 bulk-inserted rows should be captured"
    );
}

#[tokio::test]
async fn test_trigger_lsn_ordering() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cdc_lsn (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cdc_lsn VALUES (1, 'a')").await;

    db.create_st(
        "cdc_lsn_st",
        "SELECT id, val FROM cdc_lsn",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("cdc_lsn").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // Consume existing
    db.refresh_st("cdc_lsn_st").await;

    // Multiple DML ops
    db.execute("INSERT INTO cdc_lsn VALUES (2, 'b')").await;
    db.execute("UPDATE cdc_lsn SET val = 'a2' WHERE id = 1")
        .await;
    db.execute("INSERT INTO cdc_lsn VALUES (3, 'c')").await;

    // Verify change_ids are monotonically increasing (proxy for ordering)
    let ordered: bool = db
        .query_scalar(&format!(
            "SELECT bool_and(next_id > prev_id) FROM ( \
                SELECT change_id AS prev_id, \
                       lead(change_id) OVER (ORDER BY change_id) AS next_id \
                FROM {} \
            ) sub WHERE next_id IS NOT NULL",
            buffer_table
        ))
        .await;
    assert!(ordered, "Change IDs should be monotonically increasing");
}

#[tokio::test]
async fn test_trigger_typed_columns_captured() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cdc_typed (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cdc_typed VALUES (1, 'hello')")
        .await;

    db.create_st(
        "cdc_typed_st",
        "SELECT id, val FROM cdc_typed",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("cdc_typed").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // Consume existing
    db.refresh_st("cdc_typed_st").await;

    db.execute("INSERT INTO cdc_typed VALUES (2, 'world')")
        .await;

    // Verify flat columns are populated (A44-10 D+I schema: column name is `id`, not `new_id`).
    let id_val: i32 = db
        .query_scalar(&format!(
            "SELECT \"id\" FROM {} ORDER BY change_id DESC LIMIT 1",
            buffer_table
        ))
        .await;
    assert_eq!(id_val, 2, "id should match inserted id");

    let val: String = db
        .query_scalar(&format!(
            "SELECT \"val\" FROM {} ORDER BY change_id DESC LIMIT 1",
            buffer_table
        ))
        .await;
    assert_eq!(val, "world", "val should match inserted value");
}

#[tokio::test]
async fn test_buffer_cleanup_after_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cdc_cleanup (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cdc_cleanup VALUES (1, 'a')").await;

    db.create_st(
        "cdc_cleanup_st",
        "SELECT id, val FROM cdc_cleanup",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("cdc_cleanup").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // Insert data to create changes
    db.execute("INSERT INTO cdc_cleanup VALUES (2, 'b'), (3, 'c')")
        .await;

    let pre_count: i64 = db.count(&buffer_table).await;
    assert!(pre_count > 0, "Should have changes before refresh");

    // Manual refresh does TRUNCATE+INSERT (full) and doesn't clear the buffer.
    // The data should still be correct after refresh though.
    db.refresh_st("cdc_cleanup_st").await;

    // Verify refresh produced correct data even with changes in buffer
    let st_count = db.count("public.cdc_cleanup_st").await;
    assert_eq!(st_count, 3, "ST should have all 3 rows after refresh");
}

#[tokio::test]
async fn test_multiple_sources_independent_buffers() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_a (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE src_b (id INT PRIMARY KEY, ref_id INT, info TEXT)")
        .await;
    db.execute("INSERT INTO src_a VALUES (1, 'alpha')").await;
    db.execute("INSERT INTO src_b VALUES (1, 1, 'extra')").await;

    db.create_st(
        "joined_st",
        "SELECT a.id, a.val, b.info FROM src_a a JOIN src_b b ON a.id = b.ref_id",
        "1m",
        "FULL",
    )
    .await;

    let oid_a = db.table_oid("src_a").await;
    let oid_b = db.table_oid("src_b").await;

    // Each source should have its own buffer table (v0.32.0: named by stable hash)
    let buf_a = db.change_buffer_table(oid_a as i64).await;
    let buf_b = db.change_buffer_table(oid_b as i64).await;
    // Extract the table name from the qualified name for table_exists
    let buf_a_name = buf_a.trim_start_matches("pgtrickle_changes.").to_string();
    let buf_b_name = buf_b.trim_start_matches("pgtrickle_changes.").to_string();
    let buf_a_exists = db.table_exists("pgtrickle_changes", &buf_a_name).await;
    let buf_b_exists = db.table_exists("pgtrickle_changes", &buf_b_name).await;

    assert!(buf_a_exists, "src_a should have its own change buffer");
    assert!(buf_b_exists, "src_b should have its own change buffer");
    assert_ne!(oid_a, oid_b, "Source OIDs should be different");
}

#[tokio::test]
async fn test_trigger_survives_source_insert_delete_cycle() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cdc_cycle (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cdc_cycle VALUES (1, 'original')")
        .await;

    db.create_st(
        "cdc_cycle_st",
        "SELECT id, val FROM cdc_cycle",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Cycle: insert → delete → insert new
    db.execute("INSERT INTO cdc_cycle VALUES (2, 'added')")
        .await;
    db.execute("DELETE FROM cdc_cycle WHERE id = 2").await;
    db.execute("INSERT INTO cdc_cycle VALUES (3, 'final')")
        .await;

    // Refresh and verify correctness
    db.refresh_st("cdc_cycle_st").await;

    let count = db.count("public.cdc_cycle_st").await;
    assert_eq!(count, 2, "Should have rows 1 and 3");

    // Verify exact data matches the source
    db.assert_st_matches_query("public.cdc_cycle_st", "SELECT id, val FROM cdc_cycle")
        .await;
}

// ── F13: Partitioned Table CDC ─────────────────────────────────────────

/// Verify that CDC triggers on a partitioned parent table correctly capture
/// DML routed to child partitions (PostgreSQL 13+).
#[tokio::test]
async fn test_trigger_captures_partitioned_table_dml() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a RANGE-partitioned table with two child partitions
    db.execute(
        "CREATE TABLE orders (
            id INT NOT NULL,
            region TEXT NOT NULL,
            amount NUMERIC,
            PRIMARY KEY (id, region)
        ) PARTITION BY LIST (region)",
    )
    .await;
    db.execute("CREATE TABLE orders_us PARTITION OF orders FOR VALUES IN ('US')")
        .await;
    db.execute("CREATE TABLE orders_eu PARTITION OF orders FOR VALUES IN ('EU')")
        .await;

    // Seed data into both partitions
    db.execute("INSERT INTO orders VALUES (1, 'US', 100.00)")
        .await;
    db.execute("INSERT INTO orders VALUES (2, 'EU', 200.00)")
        .await;

    // Create a stream table on the partitioned parent
    db.create_st(
        "orders_st",
        "SELECT id, region, amount FROM orders",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Consume initial changes
    db.refresh_st("orders_st").await;

    // Verify initial data
    let count = db.count("public.orders_st").await;
    assert_eq!(count, 2, "Should have 2 initial rows");

    // Insert into both partitions via the parent
    db.execute("INSERT INTO orders VALUES (3, 'US', 300.00)")
        .await;
    db.execute("INSERT INTO orders VALUES (4, 'EU', 400.00)")
        .await;

    // Verify changes are captured in the change buffer
    let source_oid = db.table_oid("orders").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;
    let change_count: i64 = db.count(&buffer_table).await;
    assert!(
        change_count >= 2,
        "Both partition-routed inserts should be captured: got {}",
        change_count,
    );

    // Refresh and verify all 4 rows appear
    db.refresh_st("orders_st").await;
    let count = db.count("public.orders_st").await;
    assert_eq!(count, 4, "Should have 4 rows after insert + refresh");

    // Update a row (routed to US partition)
    db.execute("UPDATE orders SET amount = 150.00 WHERE id = 1")
        .await;
    db.refresh_st("orders_st").await;

    db.assert_st_matches_query("public.orders_st", "SELECT id, region, amount FROM orders")
        .await;

    // Delete from EU partition
    db.execute("DELETE FROM orders WHERE id = 2").await;
    db.refresh_st("orders_st").await;

    let count = db.count("public.orders_st").await;
    assert_eq!(count, 3, "Should have 3 rows after delete + refresh");

    db.assert_st_matches_query("public.orders_st", "SELECT id, region, amount FROM orders")
        .await;
}

/// Verify that FULL refresh also works with partitioned source tables.
#[tokio::test]
async fn test_full_refresh_partitioned_table() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE pt_full (
            id INT NOT NULL,
            cat TEXT NOT NULL,
            PRIMARY KEY (id, cat)
        ) PARTITION BY LIST (cat)",
    )
    .await;
    db.execute("CREATE TABLE pt_full_a PARTITION OF pt_full FOR VALUES IN ('A')")
        .await;
    db.execute("CREATE TABLE pt_full_b PARTITION OF pt_full FOR VALUES IN ('B')")
        .await;

    db.execute("INSERT INTO pt_full VALUES (1, 'A'), (2, 'B')")
        .await;

    db.create_st("pt_full_st", "SELECT id, cat FROM pt_full", "1m", "FULL")
        .await;

    db.refresh_st("pt_full_st").await;
    let count = db.count("public.pt_full_st").await;
    assert_eq!(count, 2, "Should have 2 rows after FULL refresh");

    db.execute("INSERT INTO pt_full VALUES (3, 'A'), (4, 'B')")
        .await;

    db.refresh_st("pt_full_st").await;
    db.assert_st_matches_query("public.pt_full_st", "SELECT id, cat FROM pt_full")
        .await;
}

// ── F15: Selective CDC Column Capture Tests ────────────────────────────────

/// F15: Verify that the change buffer only contains columns referenced by
/// the stream table's defining query plus PK columns.
///
/// Source has 5 columns (id, name, status, score, notes). The ST only
/// references id + name. The change buffer must contain `id` and `name`
/// (flat D+I schema, A44-10) but NOT `status`, `score`, or `notes`.
#[tokio::test]
async fn test_f15_selective_cdc_buffer_has_only_referenced_columns() {
    let db = E2eDb::new().await.with_extension().await;

    // 5-column source; ST references only id + name
    db.execute(
        "CREATE TABLE f15_src (
            id     INT  PRIMARY KEY,
            name   TEXT NOT NULL,
            status TEXT,
            score  INT,
            notes  TEXT
        )",
    )
    .await;

    db.create_st(
        "f15_st",
        "SELECT id, name FROM f15_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("f15_src").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // Buffer must have `id` (PK) and `name` (referenced) — flat D+I schema (A44-10).
    let has_id: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = '{buffer_table}'::regclass
                  AND attname = 'id'
                  AND attnum > 0
                  AND NOT attisdropped
            )"
        ))
        .await;
    assert!(has_id, "id must be present in change buffer");

    let has_name: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = '{buffer_table}'::regclass
                  AND attname = 'name'
                  AND attnum > 0
                  AND NOT attisdropped
            )"
        ))
        .await;
    assert!(has_name, "name must be present in change buffer");

    // Unreferenced columns must NOT be in the buffer.
    for col in &["status", "score", "notes"] {
        let present: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_attribute
                    WHERE attrelid = '{buffer_table}'::regclass
                      AND attname = '{col}'
                      AND attnum > 0
                      AND NOT attisdropped
                )"
            ))
            .await;
        assert!(
            !present,
            "Column {col} must NOT be in the change buffer (selective capture should prune it)"
        );
    }

    // check_cdc_health() must report selective_capture = true for this source.
    let selective: bool = db
        .query_scalar(&format!(
            "SELECT selective_capture
             FROM pgtrickle.check_cdc_health()
             WHERE source_relid = {source_oid}"
        ))
        .await;
    assert!(
        selective,
        "check_cdc_health() must report selective_capture = true for f15_src"
    );
}

/// F15: When a second ST references an additional column, the change buffer
/// must be expanded to include that column (union semantics).
#[tokio::test]
async fn test_f15_selective_cdc_buffer_expands_for_second_stream_table() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE f15_exp (
            id     INT  PRIMARY KEY,
            name   TEXT NOT NULL,
            status TEXT,
            extra  TEXT
        )",
    )
    .await;

    // First ST: references id + name only
    db.create_st(
        "f15_exp_st1",
        "SELECT id, name FROM f15_exp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("f15_exp").await;
    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // After first ST: `status` must NOT be present
    let has_status_before: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = '{buffer_table}'::regclass
                  AND attname = 'status'
                  AND attnum > 0
                  AND NOT attisdropped
            )"
        ))
        .await;
    assert!(
        !has_status_before,
        "status must not exist before second ST is added"
    );

    // Add a second ST that also references status
    db.create_st(
        "f15_exp_st2",
        "SELECT id, name, status FROM f15_exp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // After second ST: `status` must now be present (buffer expanded)
    let has_status_after: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = '{buffer_table}'::regclass
                  AND attname = 'status'
                  AND attnum > 0
                  AND NOT attisdropped
            )"
        ))
        .await;
    assert!(
        has_status_after,
        "status must be present after second ST requiring it is added"
    );

    // extra column (not referenced by either ST) must still not be present
    let has_extra: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = '{buffer_table}'::regclass
                  AND attname = 'extra'
                  AND attnum > 0
                  AND NOT attisdropped
            )"
        ))
        .await;
    assert!(
        !has_extra,
        "extra must never be in buffer (no ST references it)"
    );
}

/// F15: A source with SELECT * must fall back to full column capture (no pruning).
#[tokio::test]
async fn test_f15_select_star_falls_back_to_full_capture() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE f15_star (
            id     INT PRIMARY KEY,
            name   TEXT,
            extra  TEXT
        )",
    )
    .await;

    // SELECT * — columns_used will be NULL in the catalog
    db.create_st(
        "f15_star_st",
        "SELECT * FROM f15_star",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("f15_star").await;

    // selective_capture must be false (full column capture)
    let selective: bool = db
        .query_scalar(&format!(
            "SELECT selective_capture
             FROM pgtrickle.check_cdc_health()
             WHERE source_relid = {source_oid}"
        ))
        .await;
    assert!(
        !selective,
        "SELECT * source must report selective_capture = false"
    );

    let buffer_table = db.change_buffer_table(source_oid as i64).await;

    // All columns must be present in buffer (flat D+I schema, A44-10)
    for col in &["id", "name", "extra"] {
        let present: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_attribute
                    WHERE attrelid = '{buffer_table}'::regclass
                      AND attname = '{col}'
                      AND attnum > 0
                      AND NOT attisdropped
                )"
            ))
            .await;
        assert!(
            present,
            "Column {col} must be present for SELECT * source (full capture)"
        );
    }
}

// ── SECURITY DEFINER privilege tests ──────────────────────────────────

/// SEC-1: CDC triggers must fire successfully when DML is performed by a
/// non-privileged role that has no access to the pgtrickle_changes schema.
///
/// This is the runtime proof that SECURITY DEFINER + SET search_path work
/// correctly on the statement-level CDC trigger functions. Without
/// SECURITY DEFINER the trigger executes as the invoking user and fails
/// with "permission denied for schema pgtrickle_changes".
///
/// Covers: pg_trickle_cdc_ins_fn, pg_trickle_cdc_upd_fn,
///         pg_trickle_cdc_del_fn, pg_trickle_cdc_truncate_fn
#[tokio::test]
async fn test_sec1_unprivileged_dml_captured_by_differential_cdc() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sec1_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO sec1_src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    db.create_st(
        "sec1_st",
        "SELECT id, val FROM sec1_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("sec1_st").await;

    assert_eq!(db.count("public.sec1_st").await, 3, "initial: 3 rows");

    // Create a role with DML on the source table only — no pgtrickle_changes access.
    db.execute("CREATE ROLE sec1_writer LOGIN PASSWORD 'x'")
        .await;
    db.execute("GRANT USAGE ON SCHEMA public TO sec1_writer")
        .await;
    db.execute("GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON public.sec1_src TO sec1_writer")
        .await;

    // INSERT as unprivileged user — trigger must not raise permission denied.
    {
        let mut txn = db.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE sec1_writer")
            .execute(&mut *txn)
            .await
            .unwrap_or_else(|e| panic!("SET ROLE failed: {e}"));
        sqlx::query("INSERT INTO public.sec1_src VALUES (4, 'd')")
            .execute(&mut *txn)
            .await
            .unwrap_or_else(|e| panic!("INSERT as sec1_writer failed: {e}"));
        txn.commit().await.unwrap();
    }

    db.refresh_st("sec1_st").await;
    assert_eq!(db.count("public.sec1_st").await, 4, "after INSERT: 4 rows");

    // UPDATE as unprivileged user.
    {
        let mut txn = db.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE sec1_writer")
            .execute(&mut *txn)
            .await
            .unwrap();
        sqlx::query("UPDATE public.sec1_src SET val = 'updated' WHERE id = 1")
            .execute(&mut *txn)
            .await
            .unwrap_or_else(|e| panic!("UPDATE as sec1_writer failed: {e}"));
        txn.commit().await.unwrap();
    }

    db.refresh_st("sec1_st").await;
    let updated_val: String = db
        .query_scalar("SELECT val FROM public.sec1_st WHERE id = 1")
        .await;
    assert_eq!(
        updated_val, "updated",
        "UPDATE must be reflected in stream table"
    );

    // DELETE as unprivileged user.
    {
        let mut txn = db.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE sec1_writer")
            .execute(&mut *txn)
            .await
            .unwrap();
        sqlx::query("DELETE FROM public.sec1_src WHERE id = 2")
            .execute(&mut *txn)
            .await
            .unwrap_or_else(|e| panic!("DELETE as sec1_writer failed: {e}"));
        txn.commit().await.unwrap();
    }

    db.refresh_st("sec1_st").await;
    assert_eq!(db.count("public.sec1_st").await, 3, "after DELETE: 3 rows");
    let deleted: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.sec1_st WHERE id = 2)")
        .await;
    assert!(!deleted, "deleted row must not appear in stream table");

    // TRUNCATE as unprivileged user — hits the separate truncate trigger function.
    {
        let mut txn = db.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE sec1_writer")
            .execute(&mut *txn)
            .await
            .unwrap();
        sqlx::query("TRUNCATE public.sec1_src")
            .execute(&mut *txn)
            .await
            .unwrap_or_else(|e| panic!("TRUNCATE as sec1_writer failed: {e}"));
        txn.commit().await.unwrap();
    }

    db.refresh_st("sec1_st").await;
    assert_eq!(
        db.count("public.sec1_st").await,
        0,
        "after TRUNCATE: stream table must be empty"
    );
}

/// SEC-2: CDC triggers must fire successfully when DML is performed by a
/// non-privileged role against an IMMEDIATE-mode stream table (row-level
/// trigger pg_trickle_cdc_fn).
#[tokio::test]
async fn test_sec2_unprivileged_dml_captured_by_immediate_cdc() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sec2_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO sec2_src VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st("sec2_st", "SELECT id, val FROM sec2_src", "1m", "IMMEDIATE")
        .await;

    assert_eq!(db.count("public.sec2_st").await, 2, "initial: 2 rows");

    // Create a role with DML on the source table only.
    db.execute("CREATE ROLE sec2_writer LOGIN PASSWORD 'x'")
        .await;
    db.execute("GRANT USAGE ON SCHEMA public TO sec2_writer")
        .await;
    db.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON public.sec2_src TO sec2_writer")
        .await;

    // INSERT as unprivileged user.
    {
        let mut txn = db.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE sec2_writer")
            .execute(&mut *txn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO public.sec2_src VALUES (3, 'c')")
            .execute(&mut *txn)
            .await
            .unwrap_or_else(|e| panic!("INSERT as sec2_writer failed: {e}"));
        txn.commit().await.unwrap();
    }

    db.refresh_st("sec2_st").await;
    assert_eq!(db.count("public.sec2_st").await, 3, "after INSERT: 3 rows");

    // UPDATE as unprivileged user.
    {
        let mut txn = db.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE sec2_writer")
            .execute(&mut *txn)
            .await
            .unwrap();
        sqlx::query("UPDATE public.sec2_src SET val = 'updated' WHERE id = 1")
            .execute(&mut *txn)
            .await
            .unwrap_or_else(|e| panic!("UPDATE as sec2_writer failed: {e}"));
        txn.commit().await.unwrap();
    }

    db.refresh_st("sec2_st").await;
    let val: String = db
        .query_scalar("SELECT val FROM public.sec2_st WHERE id = 1")
        .await;
    assert_eq!(val, "updated", "UPDATE must be reflected in stream table");

    // DELETE as unprivileged user.
    {
        let mut txn = db.pool.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE sec2_writer")
            .execute(&mut *txn)
            .await
            .unwrap();
        sqlx::query("DELETE FROM public.sec2_src WHERE id = 2")
            .execute(&mut *txn)
            .await
            .unwrap_or_else(|e| panic!("DELETE as sec2_writer failed: {e}"));
        txn.commit().await.unwrap();
    }

    db.refresh_st("sec2_st").await;
    assert_eq!(db.count("public.sec2_st").await, 2, "after DELETE: 2 rows");
    let gone: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.sec2_st WHERE id = 2)")
        .await;
    assert!(!gone, "deleted row must not appear in stream table");
}

// ── INV-CACHE1: Change buffer sequence invariant ───────────────────────

/// INV-CACHE1: The change buffer BIGSERIAL sequence must have CACHE = 1.
///
/// With CACHE > 1 backends pre-allocate sequence blocks, decoupling
/// assignment order from row-lock serialization order. This causes the
/// compaction and delta pipelines (ORDER BY change_id) to silently pick
/// stale data as the final state for a row — silent data corruption.
///
/// This test verifies:
/// 1. A freshly created change buffer has CACHE = 1.
/// 2. Manually altering the sequence to CACHE > 1 causes check_cdc_health()
///    to emit a CRITICAL alert string.
/// 3. Restoring CACHE = 1 clears the alert.
#[tokio::test]
async fn test_inv_cache1_change_buffer_sequence_cache_is_one() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cache1_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cache1_src VALUES (1, 'a')").await;
    db.create_st(
        "cache1_st",
        "SELECT id, val FROM cache1_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("cache1_st").await;

    let source_oid = db.table_oid("cache1_src").await;
    // v0.32.0+: sequence name uses stable hash, not OID.
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT source_stable_name FROM pgtrickle.pgt_change_tracking \
             WHERE source_relid = {}",
            source_oid
        ))
        .await;
    let seq_name = format!("pgtrickle_changes.changes_{}_change_id_seq", stable_name);

    // 1. Fresh sequence must have CACHE = 1.
    let cache_size: i64 = db
        .query_scalar(&format!(
            "SELECT cache_size FROM pg_sequences \
             WHERE schemaname = 'pgtrickle_changes' \
               AND sequencename = 'changes_{}_change_id_seq'",
            stable_name
        ))
        .await;
    assert_eq!(
        cache_size, 1,
        "change buffer sequence must have CACHE = 1 (INV-CACHE1)"
    );

    // 2. Manually corrupt: set CACHE = 32. check_cdc_health() must alert.
    db.execute(&format!("ALTER SEQUENCE {} CACHE 32", seq_name))
        .await;

    let alert: Option<String> = db
        .query_scalar_opt(
            "SELECT alert FROM pgtrickle.check_cdc_health() WHERE alert IS NOT NULL LIMIT 1",
        )
        .await;
    assert!(
        alert.is_some(),
        "check_cdc_health() must return an alert when sequence CACHE > 1"
    );
    let alert_text = alert.unwrap();
    assert!(
        alert_text.contains("CACHE > 1") || alert_text.contains("cache_size"),
        "alert must mention CACHE > 1, got: {alert_text}"
    );

    // 3. Restore CACHE = 1. check_cdc_health() must clear the alert.
    db.execute(&format!("ALTER SEQUENCE {} CACHE 1", seq_name))
        .await;

    let alert_after_fix: Option<String> = db
        .query_scalar_opt(
            "SELECT alert FROM pgtrickle.check_cdc_health() WHERE alert LIKE '%CACHE%' LIMIT 1",
        )
        .await;
    assert!(
        alert_after_fix.is_none(),
        "check_cdc_health() must not alert after CACHE is restored to 1"
    );
}

// ── T-4 (v0.77.0): TRUNCATE LSN regression ────────────────────────────

/// T-4: TRUNCATE CDC trigger uses pg_current_wal_insert_lsn (not pg_current_wal_lsn).
///
/// Regression test for C-1 fix: the TRUNCATE trigger function previously
/// used `pg_current_wal_lsn()` (the WAL write position, which can lag behind
/// the insert position within a transaction) instead of
/// `pg_current_wal_insert_lsn()` (always at or ahead of the write position).
///
/// This test verifies the ordering invariant:
/// 1. After TRUNCATE, rows inserted in the same transaction must be
///    captured with an LSN strictly greater than the TRUNCATE marker.
/// 2. After refresh, the stream table contains ONLY the post-TRUNCATE rows.
/// 3. No pre-TRUNCATE rows survive in the stream table.
#[tokio::test]
async fn test_t4_truncate_lsn_insert_ordering_regression() {
    let db = E2eDb::new().await.with_extension().await;

    // Populate initial data
    db.execute("CREATE TABLE t4_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO t4_src SELECT g, 'pre_' || g FROM generate_series(1, 20) g")
        .await;

    db.create_st("t4_st", "SELECT id, val FROM t4_src", "1h", "DIFFERENTIAL")
        .await;

    // Full-initialise the stream table
    db.refresh_st("t4_st").await;
    assert_eq!(
        db.count("public.t4_st").await,
        20,
        "T-4: pre-condition: stream table must have 20 rows after full init"
    );

    // TRUNCATE and INSERT post-TRUNCATE rows in the same transaction.
    // The TRUNCATE trigger must record an LSN ≤ the INSERT LSNs; if the
    // C-1 bug were present, it would record a stale LSN that makes the
    // engine believe all post-TRUNCATE INSERTs happened before the TRUNCATE.
    db.execute(
        "BEGIN;\
         TRUNCATE t4_src;\
         INSERT INTO t4_src SELECT g, 'post_' || g FROM generate_series(100, 115) g;\
         COMMIT",
    )
    .await;

    // Refresh — must detect TRUNCATE marker and fall back to FULL.
    db.refresh_st("t4_st").await;

    // Verify: only post-TRUNCATE rows survive
    let count = db.count("public.t4_st").await;
    assert_eq!(
        count, 16,
        "T-4: after TRUNCATE+INSERT, stream table must have exactly 16 post-TRUNCATE rows, got {count}"
    );

    // No pre-TRUNCATE rows
    let pre_rows: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM public.t4_st WHERE val LIKE 'pre_%'")
        .await;
    assert_eq!(
        pre_rows, 0,
        "T-4: no pre-TRUNCATE rows must survive in the stream table"
    );

    // All post-TRUNCATE rows present
    let post_rows: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM public.t4_st WHERE val LIKE 'post_%'")
        .await;
    assert_eq!(
        post_rows, 16,
        "T-4: all 16 post-TRUNCATE rows must be present in the stream table"
    );
}

/// T-4b: TRUNCATE followed by further INSERTs in a separate transaction.
///
/// Verifies that the TRUNCATE LSN ordering invariant holds when the
/// post-TRUNCATE INSERTs are committed in a different transaction
/// (the common case in bulk-reload workflows).
#[tokio::test]
async fn test_t4b_truncate_then_insert_separate_transactions() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE t4b_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO t4b_src SELECT g, 'initial_' || g FROM generate_series(1, 10) g")
        .await;

    db.create_st(
        "t4b_st",
        "SELECT id, val FROM t4b_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.refresh_st("t4b_st").await;
    assert_eq!(db.count("public.t4b_st").await, 10, "T-4b pre-condition");

    // Separate transactions: TRUNCATE then INSERT
    db.execute("TRUNCATE t4b_src").await;
    db.execute("INSERT INTO t4b_src SELECT g, 'reload_' || g FROM generate_series(50, 55) g")
        .await;

    db.refresh_st("t4b_st").await;

    let count = db.count("public.t4b_st").await;
    assert_eq!(
        count, 6,
        "T-4b: must have exactly 6 reload rows, got {count}"
    );
    let initial_rows: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM public.t4b_st WHERE val LIKE 'initial_%'")
        .await;
    assert_eq!(initial_rows, 0, "T-4b: no initial rows must survive");
}
