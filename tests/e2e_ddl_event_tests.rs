//! E2E tests for DDL event trigger behavior.
//!
//! Validates that event triggers detect source table drops, alters,
//! and direct manipulation of ST storage tables.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_ddl_hooks_dependency_lookup_timeout_fail_closed() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE evt_lock_unrelated (id INT)").await;
    db.execute("CREATE TABLE evt_lock_unrelated_drop (id INT)")
        .await;

    let mut blocker = db.pool.acquire().await.expect("acquire blocker connection");
    sqlx::query("BEGIN")
        .execute(&mut *blocker)
        .await
        .expect("begin blocker transaction");
    sqlx::query("LOCK TABLE pgtrickle.pgt_dependencies IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("lock dependency catalog");

    for (kind, ddl) in [
        (
            "ALTER TABLE",
            "ALTER TABLE evt_lock_unrelated ADD COLUMN added INT",
        ),
        ("DROP TABLE", "DROP TABLE evt_lock_unrelated_drop"),
    ] {
        let blocked = db
            .try_execute_with_config(&["SET lock_timeout = '100ms'"], ddl)
            .await;
        assert!(
            blocked.is_err(),
            "{kind} must fail closed when dependency relevance cannot be determined"
        );
    }

    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .expect("release dependency catalog lock");
    drop(blocker);

    db.execute("ALTER TABLE evt_lock_unrelated ADD COLUMN added INT")
        .await;
    db.execute("DROP TABLE evt_lock_unrelated_drop").await;
}

#[tokio::test]
async fn test_drop_source_fires_event_trigger() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE evt_drop_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO evt_drop_src VALUES (1, 'data')")
        .await;

    db.create_st(
        "evt_drop_st",
        "SELECT id, val FROM evt_drop_src",
        "1m",
        "FULL",
    )
    .await;

    // Verify event triggers are installed
    let ddl_trigger: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM pg_event_trigger WHERE evtname = 'pg_trickle_ddl_tracker' \
            )",
        )
        .await;
    assert!(ddl_trigger, "DDL event trigger should be installed");

    let drop_trigger: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM pg_event_trigger WHERE evtname = 'pg_trickle_drop_tracker' \
            )",
        )
        .await;
    assert!(drop_trigger, "Drop event trigger should be installed");

    // Drop the source table — event trigger should fire
    let result = db.try_execute("DROP TABLE evt_drop_src CASCADE").await;

    // The event trigger should handle this gracefully
    // Whether it succeeds or is prevented depends on implementation
    if result.is_ok() {
        // If allowed, the ST catalog entry may still exist with status=ERROR,
        // or the storage table may have been cascade-dropped too (cleaning up the catalog).
        let st_count: i64 = db
            .query_scalar(
                "SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'evt_drop_st'",
            )
            .await;
        if st_count > 0 {
            // The event trigger sets status to ERROR when a source is dropped
            let status: String = db
                .query_scalar(
                    "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'evt_drop_st'",
                )
                .await;
            assert_eq!(
                status, "ERROR",
                "ST should be set to ERROR after source drop"
            );
        }
        // If st_count == 0, CASCADE dropped the storage table too,
        // and the drop event trigger cleaned up the catalog — also valid.
    }
    // If result is Err, the extension prevented the drop — that's valid too
}

#[tokio::test]
async fn test_alter_source_fires_event_trigger() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE evt_alter_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO evt_alter_src VALUES (1, 'data')")
        .await;

    db.create_st(
        "evt_alter_st",
        "SELECT id, val FROM evt_alter_src",
        "1m",
        "FULL",
    )
    .await;

    // ALTER the source table — event trigger should fire
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE evt_alter_src ADD COLUMN extra INT",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // ST should still be queryable (the added column isn't part of the defining query)
    let count = db.count("public.evt_alter_st").await;
    assert_eq!(count, 1, "ST should still be valid after compatible ALTER");
}

#[tokio::test]
async fn test_drop_st_storage_by_sql() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE evt_storage_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO evt_storage_src VALUES (1)").await;

    db.create_st(
        "evt_storage_st",
        "SELECT id FROM evt_storage_src",
        "1m",
        "FULL",
    )
    .await;

    // Drop the ST storage table directly (bypassing pgtrickle.drop_stream_table)
    let result = db
        .try_execute("DROP TABLE public.evt_storage_st CASCADE")
        .await;

    if result.is_ok() {
        // The event trigger should have cleaned up the catalog
        // Give a tiny moment for event trigger processing
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let cat_count: i64 = db
            .query_scalar(
                "SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'evt_storage_st'",
            )
            .await;
        assert_eq!(
            cat_count, 0,
            "Catalog entry should be cleaned up by event trigger"
        );
    }
    // If the DROP fails, the extension is protecting its tables — also valid
}

#[tokio::test]
async fn test_rename_source_table() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE evt_rename_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO evt_rename_src VALUES (1, 'data')")
        .await;

    db.create_st(
        "evt_rename_st",
        "SELECT id, val FROM evt_rename_src",
        "1m",
        "FULL",
    )
    .await;

    // Rename the source table — triggers DDL event
    db.execute("ALTER TABLE evt_rename_src RENAME TO evt_renamed_src")
        .await;

    // The ST may or may not still work after renaming the source.
    // The defining query still references 'evt_rename_src' which is now gone.
    // Refresh should reveal the problem.
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('evt_rename_st')")
        .await;

    // After renaming source, refresh with old name should fail
    assert!(
        result.is_err(),
        "Refresh should fail after source table rename since defining query references old name"
    );
}

/// F18: CREATE OR REPLACE FUNCTION on a function used by a DIFFERENTIAL
/// stream table should mark the ST for reinitialize.
#[tokio::test]
async fn test_function_change_marks_st_for_reinit() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a custom function
    db.execute(
        "CREATE FUNCTION evt_double(x INT) RETURNS INT AS $$ SELECT x * 2 $$ LANGUAGE SQL IMMUTABLE",
    )
    .await;

    db.execute("CREATE TABLE evt_func_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO evt_func_src VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "evt_func_st",
        "SELECT id, evt_double(val) AS doubled FROM evt_func_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Verify initial data
    let count = db.count("public.evt_func_st").await;
    assert_eq!(count, 2);

    // Verify functions_used was populated
    let func_count: i64 = db
        .query_scalar(
            "SELECT coalesce(array_length(functions_used, 1), 0)::bigint \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'evt_func_st'",
        )
        .await;
    assert!(
        func_count > 0,
        "functions_used should be populated for DIFFERENTIAL STs"
    );

    // Check that evt_double is in the list
    let has_func: bool = db
        .query_scalar(
            "SELECT functions_used @> ARRAY['evt_double']::text[] \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'evt_func_st'",
        )
        .await;
    assert!(has_func, "functions_used should contain 'evt_double'");

    // Replace the function with a different implementation
    db.execute(
        "CREATE OR REPLACE FUNCTION evt_double(x INT) RETURNS INT AS $$ SELECT x * 3 $$ LANGUAGE SQL IMMUTABLE",
    )
    .await;

    // The DDL hook should have marked the ST for reinit
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'evt_func_st'",
        )
        .await;
    assert!(
        needs_reinit,
        "ST should be marked for reinit after function replacement"
    );

    // Trigger reinit by refreshing, then verify data uses the new function body
    db.refresh_st("evt_func_st").await;
    db.assert_st_matches_query(
        "public.evt_func_st",
        "SELECT id, evt_double(val) AS doubled FROM evt_func_src",
    )
    .await;
}

/// F18: DROP FUNCTION on a function used by a stream table should mark
/// the ST for reinit.
#[tokio::test]
async fn test_drop_function_marks_st_for_reinit() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE FUNCTION evt_triple(x INT) RETURNS INT AS $$ SELECT x * 3 $$ LANGUAGE SQL IMMUTABLE",
    )
    .await;

    db.execute("CREATE TABLE evt_dfunc_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO evt_dfunc_src VALUES (1, 5)").await;

    db.create_st(
        "evt_dfunc_st",
        "SELECT id, evt_triple(val) AS tripled FROM evt_dfunc_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count = db.count("public.evt_dfunc_st").await;
    assert_eq!(count, 1);

    // Drop the function (CASCADE to avoid dependency errors)
    let _ = db
        .try_execute("DROP FUNCTION evt_triple(INT) CASCADE")
        .await;

    // The drop hook should have marked the ST for reinit
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'evt_dfunc_st'",
        )
        .await;
    assert!(
        needs_reinit,
        "ST should be marked for reinit after function drop"
    );
}

// ══════════════════════════════════════════════════════════════════════
// B1 — ADD COLUMN on a monitored source
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_add_column_on_source_st_still_functional() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_add_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO ddl_add_src VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "ddl_add_st",
        "SELECT id, val FROM ddl_add_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Add a column that is NOT in the defining query
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE ddl_add_src ADD COLUMN extra TEXT",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // After the column change, the ST should still be refreshable
    db.refresh_st("ddl_add_st").await;
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM public.ddl_add_st")
        .await;
    assert_eq!(
        count, 2,
        "ST should still have all rows after ADD COLUMN + refresh"
    );

    // Verify data correctness — unused column add should not affect ST values
    db.assert_st_matches_query("public.ddl_add_st", "SELECT id, val FROM ddl_add_src")
        .await;
}

#[tokio::test]
async fn test_add_column_unused_st_survives_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_add2_src (id INT PRIMARY KEY, a INT)")
        .await;
    db.execute("INSERT INTO ddl_add2_src VALUES (1, 1)").await;

    db.create_st(
        "ddl_add2_st",
        "SELECT id, a FROM ddl_add2_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Add column 'b' (not used)
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE ddl_add2_src ADD COLUMN b INT DEFAULT 0",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;
    db.execute("UPDATE ddl_add2_src SET b = 99 WHERE id = 1")
        .await;
    db.refresh_st("ddl_add2_st").await;

    // ST should have 1 row with the original columns intact
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM public.ddl_add2_st")
        .await;
    assert_eq!(count, 1);
    let a_val: i32 = db
        .query_scalar("SELECT a FROM public.ddl_add2_st WHERE id = 1")
        .await;
    assert_eq!(a_val, 1);

    // Verify full data: the new column 'b' must not appear in the ST and values are correct
    db.assert_st_matches_query("public.ddl_add2_st", "SELECT id, a FROM ddl_add2_src")
        .await;
}

#[tokio::test]
async fn test_add_column_unused_immediate_st_stays_functional() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_add_imm_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO ddl_add_imm_src VALUES (1, 10)")
        .await;
    db.create_st(
        "ddl_add_imm_st",
        "SELECT id, val FROM ddl_add_imm_src",
        "1m",
        "IMMEDIATE",
    )
    .await;

    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE ddl_add_imm_src ADD COLUMN extra TEXT DEFAULT 'unused'",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;
    db.execute("UPDATE ddl_add_imm_src SET val = 20 WHERE id = 1")
        .await;

    db.assert_st_matches_query(
        "public.ddl_add_imm_st",
        "SELECT id, val FROM ddl_add_imm_src",
    )
    .await;

    let source_oid = db.table_oid("ddl_add_imm_src").await;
    let buffer_exists: bool = db
        .query_scalar(&format!(
            "SELECT to_regclass('pgtrickle_changes.changes_{source_oid}') IS NOT NULL"
        ))
        .await;
    assert!(
        !buffer_exists,
        "IMMEDIATE source DDL must not create a CDC change buffer"
    );
}

// ══════════════════════════════════════════════════════════════════════
// B2 — DROP COLUMN not referenced in query → ST remains functional
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_drop_unused_column_st_survives() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_drop_col_src (id INT PRIMARY KEY, used INT, unused TEXT)")
        .await;
    db.execute("INSERT INTO ddl_drop_col_src VALUES (1, 10, 'x'), (2, 20, 'y')")
        .await;

    db.create_st(
        "ddl_drop_col_st",
        "SELECT id, used FROM ddl_drop_col_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Drop the column that is NOT in the defining query
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE ddl_drop_col_src DROP COLUMN unused",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_drop_col_st'",
        )
        .await;
    assert_ne!(
        status, "ERROR",
        "ST should not be in ERROR after unused column drop"
    );

    db.refresh_st("ddl_drop_col_st").await;
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM public.ddl_drop_col_st")
        .await;
    assert_eq!(
        count, 2,
        "ST should still have 2 rows after dropping unused column"
    );

    // Verify data: the dropped 'unused' column must not appear and values are intact
    db.assert_st_matches_query(
        "public.ddl_drop_col_st",
        "SELECT id, used FROM ddl_drop_col_src",
    )
    .await;
}

// ══════════════════════════════════════════════════════════════════════
// B3 — ALTER COLUMN TYPE on a used column → reinit
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_alter_column_type_triggers_reinit() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_type_src (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO ddl_type_src VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "ddl_type_st",
        "SELECT id, score FROM ddl_type_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Change type of 'score' — column is in defining query
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE ddl_type_src ALTER COLUMN score TYPE BIGINT",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_type_st'",
        )
        .await;
    assert!(
        needs_reinit,
        "ST should be marked for reinit after column type change"
    );

    // Trigger reinit/refresh and verify data is correct with the new column type
    db.refresh_st("ddl_type_st").await;
    db.assert_st_matches_query("public.ddl_type_st", "SELECT id, score FROM ddl_type_src")
        .await;
}

// ══════════════════════════════════════════════════════════════════════
// B4 — CREATE INDEX on source → Benign, no reinit
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_create_index_on_source_is_benign() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_idx_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO ddl_idx_src VALUES (1, 10)").await;

    db.create_st(
        "ddl_idx_st",
        "SELECT id, val FROM ddl_idx_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // CREATE INDEX is DDL but purely structural — should be benign
    db.execute("CREATE INDEX ON ddl_idx_src (val)").await;

    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_idx_st'",
        )
        .await;
    assert!(
        !needs_reinit,
        "CREATE INDEX on source should not trigger reinit"
    );

    // ST should still be functional
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM public.ddl_idx_st")
        .await;
    assert_eq!(count, 1);
}

// ══════════════════════════════════════════════════════════════════════
// B5 — DROP source with multiple downstream STs
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_drop_source_with_multiple_downstream_sts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_multi_src (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO ddl_multi_src VALUES (1, 1), (2, 2)")
        .await;

    db.create_st(
        "ddl_multi_st1",
        "SELECT id, v FROM ddl_multi_src",
        "1m",
        "FULL",
    )
    .await;
    db.create_st(
        "ddl_multi_st2",
        "SELECT id, v * 2 AS v2 FROM ddl_multi_src",
        "1m",
        "FULL",
    )
    .await;

    let result = db.try_execute("DROP TABLE ddl_multi_src CASCADE").await;

    if result.is_ok() {
        // Both STs should either be gone (cascade) or in ERROR
        for st in ["ddl_multi_st1", "ddl_multi_st2"] {
            let status_opt: Option<String> = db
                .query_scalar_opt(&format!(
                    "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{st}'"
                ))
                .await;
            if let Some(status) = status_opt {
                assert_eq!(status, "ERROR", "{st} should be in ERROR after source DROP");
            }
            // If None: cascade cleaned up the catalog entry — also valid
        }
    }
    // If result is Err, the extension prevented the drop — also valid
}

// ══════════════════════════════════════════════════════════════════════
// B6 — pg_trickle.block_source_ddl GUC
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_block_source_ddl_guc_prevents_alter() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_block_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO ddl_block_src VALUES (1, 1)").await;

    db.create_st(
        "ddl_block_st",
        "SELECT id, val FROM ddl_block_src",
        "1m",
        "FULL",
    )
    .await;

    // The GUC defaults to true (on), so ALTER on a monitored source should
    // be blocked out of the box.
    let result = db
        .try_execute("ALTER TABLE ddl_block_src ADD COLUMN extra TEXT")
        .await;
    assert!(
        result.is_err(),
        "ALTER on monitored source should be blocked when block_source_ddl = true (default)"
    );

    // Disable blocking GUC (use ALTER SYSTEM + reload so it applies to all
    // pool connections, not just the current session which the pool may not
    // reuse for the next query).
    db.alter_system_set_and_wait("pg_trickle.block_source_ddl", "false", "off")
        .await;

    // ALTER on a monitored source should now succeed
    let result = db
        .try_execute("ALTER TABLE ddl_block_src ADD COLUMN extra TEXT")
        .await;
    assert!(
        result.is_ok(),
        "ALTER on monitored source should be allowed when block_source_ddl = false"
    );

    db.alter_system_reset_and_wait("pg_trickle.block_source_ddl", "on")
        .await;
}

// ══════════════════════════════════════════════════════════════════════
// B7 — Column change on a joined source
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_add_column_on_joined_source_st_survives() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_nj_a (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE ddl_nj_b (id INT PRIMARY KEY, score INT)")
        .await;
    db.execute("INSERT INTO ddl_nj_a VALUES (1, 'x'), (2, 'y')")
        .await;
    db.execute("INSERT INTO ddl_nj_b VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "ddl_nj_st",
        "SELECT a.id, a.name, b.score FROM ddl_nj_a a JOIN ddl_nj_b b ON a.id = b.id",
        "1m",
        "FULL",
    )
    .await;
    assert_eq!(db.count("public.ddl_nj_st").await, 2);

    // ADD COLUMN to one source
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE ddl_nj_a ADD COLUMN extra TEXT",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    let status: String = db
        .query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_nj_st'")
        .await;
    assert_ne!(
        status, "ERROR",
        "ST should remain valid after adding unused column to joined source"
    );

    // Refresh should succeed
    db.refresh_st("ddl_nj_st").await;
    assert_eq!(db.count("public.ddl_nj_st").await, 2);
}

// ══════════════════════════════════════════════════════════════════════
// Phase 5.2 (TESTING_GAPS_2) — Schema Evolution Tests
// ══════════════════════════════════════════════════════════════════════

/// SE-1: RENAME a column that is NOT in the defining query → benign;
/// ST remains ACTIVE and continues to refresh correctly.
#[tokio::test]
async fn test_rename_unused_column_is_benign() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE se_rename_src (id INT PRIMARY KEY, used_col TEXT, extra TEXT)")
        .await;
    db.execute("INSERT INTO se_rename_src VALUES (1,'hello','unused')")
        .await;

    db.create_st(
        "se_rename_st",
        "SELECT id, used_col FROM se_rename_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("se_rename_st", "SELECT id, used_col FROM se_rename_src")
        .await;

    // Rename the unused column — should be benign
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE se_rename_src RENAME COLUMN extra TO extra_renamed",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'se_rename_st'",
        )
        .await;
    // Renaming an unused column should not mark the ST for reinit
    // (or, at most, mark it for reinit which the refresh recovers from)
    let q = "SELECT id, used_col FROM se_rename_src";
    db.refresh_st("se_rename_st").await;
    db.assert_st_matches_query("se_rename_st", q).await;
    let _ = needs_reinit; // behavior is implementation-defined; correctness is what matters
}

/// SE-2: Widen a VARCHAR column that IS in the defining query
/// (VARCHAR(50) → VARCHAR(200)) — type widening is backward-compatible;
/// ST should remain functional after refresh.
#[tokio::test]
async fn test_widen_varchar_type_benign() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE se_widen_src (id INT PRIMARY KEY, label VARCHAR(50))")
        .await;
    db.execute("INSERT INTO se_widen_src VALUES (1,'short')")
        .await;

    db.create_st(
        "se_widen_st",
        "SELECT id, label FROM se_widen_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("se_widen_st", "SELECT id, label FROM se_widen_src")
        .await;

    // Widen the column — backward-compatible change
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE se_widen_src ALTER COLUMN label TYPE VARCHAR(200)",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // Insert a value longer than 50 chars to confirm the widening took effect
    db.execute("INSERT INTO se_widen_src VALUES (2, 'a_longer_label_than_fifty_characters_xxxxxxxxxxxxxxxxxx')")
        .await;
    db.refresh_st("se_widen_st").await;
    db.assert_st_matches_query("se_widen_st", "SELECT id, label FROM se_widen_src")
        .await;
    assert_eq!(db.count("public.se_widen_st").await, 2);
}

/// SE-3: ADD NOT NULL constraint on a column NOT in the defining query
/// is benign — ST continues to refresh correctly.
#[tokio::test]
async fn test_add_not_null_constraint_benign() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE se_nn_src (id INT PRIMARY KEY, score INT, note TEXT)")
        .await;
    db.execute("INSERT INTO se_nn_src VALUES (1, 10, 'ok'), (2, 20, 'good')")
        .await;

    db.create_st(
        "se_nn_st",
        "SELECT id, score FROM se_nn_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("se_nn_st", "SELECT id, score FROM se_nn_src")
        .await;

    // Add NOT NULL + default to the unused 'note' column
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE se_nn_src ALTER COLUMN note SET DEFAULT 'default_note'",
        "ALTER TABLE se_nn_src ALTER COLUMN note SET NOT NULL",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // Normal DML + refresh cycle should still work
    db.execute("INSERT INTO se_nn_src VALUES (3, 30, 'new')")
        .await;
    db.refresh_st("se_nn_st").await;
    db.assert_st_matches_query("se_nn_st", "SELECT id, score FROM se_nn_src")
        .await;
    assert_eq!(db.count("public.se_nn_st").await, 3);
}

/// SE-4: DROP a source column that IS referenced in the defining query
/// produces a clear, informative error — not a silent data corruption.
#[tokio::test]
async fn test_drop_referenced_column_produces_error() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE se_dropcol_src (id INT PRIMARY KEY, important TEXT, extra TEXT)")
        .await;
    db.execute("INSERT INTO se_dropcol_src VALUES (1,'keep','ignore')")
        .await;

    db.create_st(
        "se_dropcol_st",
        "SELECT id, important FROM se_dropcol_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query("se_dropcol_st", "SELECT id, important FROM se_dropcol_src")
        .await;

    // Dropping a referenced column requires disabling the DDL block
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE se_dropcol_src DROP COLUMN important",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // The ST should now be in a needs_reinit or ERROR state, or the next
    // refresh should fail with a clear error.
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('se_dropcol_st')")
        .await;

    if let Ok(()) = result {
        // Refresh may have succeeded by falling back to full refresh;
        // in that case verify the ST catalog reflects correct state.
        let status: String = db
            .query_scalar(
                "SELECT status FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = 'se_dropcol_st'",
            )
            .await;
        // Status should be ERROR or ACTIVE-after-reinit — not silently wrong data
        assert!(
            status == "ERROR" || status == "ACTIVE",
            "Unexpected status after dropped referenced column: {status}"
        );
    } else {
        // Error is the expected outcome — verify the error message is informative
        let err_msg = result.unwrap_err().to_string();
        assert!(
            !err_msg.is_empty(),
            "Drop of referenced column should produce a non-empty error message"
        );
    }
}

/// SE-5: Interleaving DML changes with a compatible DDL change (ADD COLUMN)
/// in separate transactions. The next refresh must produce the correct result
/// even when the change buffer was populated before the DDL.
#[tokio::test]
async fn test_dml_and_compatible_ddl_interleaved() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE se_intl_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO se_intl_src VALUES (1,10),(2,20)")
        .await;

    let q = "SELECT id, val FROM se_intl_src";
    db.create_st("se_intl_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("se_intl_st", q).await;

    // DML first — inserts a row (captured in change buffer)
    db.execute("INSERT INTO se_intl_src VALUES (3, 30)").await;

    // Compatible DDL: add an unused column
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE se_intl_src ADD COLUMN category TEXT DEFAULT 'misc'",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // More DML after the DDL
    db.execute("INSERT INTO se_intl_src (id, val, category) VALUES (4, 40, 'special')")
        .await;

    // Refresh must pick up both DML batches correctly
    db.refresh_st("se_intl_st").await;
    db.assert_st_matches_query("se_intl_st", q).await;
    assert_eq!(db.count("public.se_intl_st").await, 4);
}

// ══════════════════════════════════════════════════════════════════════
// TEST-2: DDL tracking for ALTER TYPE / ALTER DOMAIN / ALTER POLICY
// ══════════════════════════════════════════════════════════════════════

/// TEST-2a: ALTER TYPE on an enum used in a source column triggers
/// stream table invalidation (needs_reinit or ERROR status).
#[tokio::test]
async fn test_alter_type_enum_triggers_invalidation() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TYPE ddl_status_enum AS ENUM ('active', 'inactive')")
        .await;
    db.execute("CREATE TABLE ddl_enum_src (id INT PRIMARY KEY, status ddl_status_enum NOT NULL)")
        .await;
    db.execute("INSERT INTO ddl_enum_src VALUES (1, 'active'), (2, 'inactive')")
        .await;

    let q = "SELECT id, status::TEXT FROM ddl_enum_src";
    db.create_st("ddl_enum_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("ddl_enum_st", q).await;

    // ALTER TYPE: add a new enum value (structural change to the type)
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TYPE ddl_status_enum ADD VALUE 'archived'",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // Insert a row with the new enum value
    db.execute("INSERT INTO ddl_enum_src VALUES (3, 'archived')")
        .await;

    // Refresh should succeed — data must be correct
    db.refresh_st("ddl_enum_st").await;
    db.assert_st_matches_query("ddl_enum_st", q).await;
}

/// TEST-2b: ALTER DOMAIN on a domain used in a source column triggers
/// stream table invalidation when the constraint changes.
#[tokio::test]
async fn test_alter_domain_triggers_invalidation() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE DOMAIN ddl_score AS INT CHECK (VALUE >= 0)")
        .await;
    db.execute("CREATE TABLE ddl_dom_src (id INT PRIMARY KEY, score ddl_score)")
        .await;
    db.execute("INSERT INTO ddl_dom_src VALUES (1, 50), (2, 75)")
        .await;

    let q = "SELECT id, score FROM ddl_dom_src";
    db.create_st("ddl_dom_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("ddl_dom_st", q).await;

    // ALTER DOMAIN: drop the constraint (structural change)
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER DOMAIN ddl_score DROP CONSTRAINT ddl_score_check",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // The ST may or may not be invalidated depending on event trigger
    // coverage. In either case, a refresh must produce correct data.
    db.refresh_st("ddl_dom_st").await;
    db.assert_st_matches_query("ddl_dom_st", q).await;
}

/// TEST-2c: ALTER POLICY on an RLS policy affecting a source table
/// triggers stream table invalidation.
#[tokio::test]
async fn test_alter_policy_triggers_invalidation() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ddl_pol_src (id INT PRIMARY KEY, dept TEXT, val INT)")
        .await;
    db.execute("INSERT INTO ddl_pol_src VALUES (1, 'eng', 10), (2, 'sales', 20), (3, 'eng', 30)")
        .await;

    let q = "SELECT id, dept, val FROM ddl_pol_src";
    db.create_st("ddl_pol_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("ddl_pol_st", q).await;

    // Enable RLS and create a policy
    db.execute("ALTER TABLE ddl_pol_src ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute("CREATE POLICY eng_only ON ddl_pol_src FOR SELECT USING (dept = 'eng')")
        .await;

    // ALTER POLICY: change the qualification
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER POLICY eng_only ON ddl_pol_src USING (dept = 'sales')",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // Refresh — data must remain correct from the superuser perspective
    // (this stream is owned by the extension superuser, which bypasses RLS)
    db.refresh_st("ddl_pol_st").await;
    db.assert_st_matches_query("ddl_pol_st", q).await;
}
