//! TG2-SCHEMA: Source-table schema evolution E2E tests.
//!
//! Validates that pg_trickle handles DDL changes on source tables gracefully:
//!
//! | Test | DDL Operation | Expected |
//! |------|---------------|----------|
//! | SE-1 | Column rename (not in defining query) | Suspended with reason |
//! | SE-2 | Column rename (used in defining query) | ST detects and suspends |
//! | SE-3 | Column added to source | No impact |
//! | SE-4 | Column type change (INT → BIGINT, compatible) | Explicit repair |
//!
//! These tests use manual `refresh_stream_table()` to keep DDL detection
//! deterministic.

mod e2e;

use e2e::E2eDb;

// ── SE-1: Rename unused column — explicit suspension ───────────────────────

/// Renaming a source column that is NOT referenced in the defining query
/// is treated conservatively as destructive source DDL.
#[tokio::test]
async fn test_schema_evolution_rename_unused_column_suspends() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE se1_src (id SERIAL PRIMARY KEY, used_col INT, unused_col TEXT)")
        .await;
    db.execute("INSERT INTO se1_src (used_col, unused_col) VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st(
        "se1_st",
        "SELECT id, used_col FROM se1_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.se1_st").await, 2);

    // Rename the unused column
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE se1_src RENAME COLUMN unused_col TO other_col",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    let status: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'se1_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED:SOURCE_DESTRUCTIVE_SCHEMA");

    let _: String = db
        .query_scalar("SELECT pgtrickle.reinitialize_stream_table('se1_st')")
        .await;
    db.execute("INSERT INTO se1_src (used_col, other_col) VALUES (3, 'c')")
        .await;
    db.refresh_st("se1_st").await;
    assert_eq!(db.count("public.se1_st").await, 3);
}

// ── SE-2: Rename used column — ST detects mismatch ─────────────────────────

/// Renaming a source column that IS referenced in the defining query
/// should suspend the stream table with a stable reason code.
#[tokio::test]
async fn test_schema_evolution_rename_used_column_detected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE se2_src (id SERIAL PRIMARY KEY, amount INT)")
        .await;
    db.execute("INSERT INTO se2_src (amount) VALUES (10), (20)")
        .await;

    db.create_st(
        "se2_st",
        "SELECT id, amount FROM se2_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.se2_st").await, 2);

    // Rename the column used in the defining query.
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE se2_src RENAME COLUMN amount TO total",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    let status: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'se2_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED:SOURCE_DESTRUCTIVE_SCHEMA");

    // The next refresh should fail because 'amount' no longer exists
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('se2_st')")
        .await;
    assert!(
        result.is_err(),
        "Refresh should fail after renaming a column used in the defining query"
    );
}

// ── SE-3: Add column to source — no impact ─────────────────────────────────

/// Adding a new column to the source table should have no effect on
/// stream tables that don't reference it.
#[tokio::test]
async fn test_schema_evolution_add_column_no_impact() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE se3_src (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO se3_src (val) VALUES (100), (200)")
        .await;

    db.create_st(
        "se3_st",
        "SELECT id, val FROM se3_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.se3_st").await, 2);

    // Add a new column. pg_trickle blocks ADD COLUMN by default
    // (block_source_ddl = true / SchemaChangeKind::AddColumnOnly), so we
    // temporarily disable the guard around the DDL.
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE se3_src ADD COLUMN extra TEXT DEFAULT 'x'",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // Insert using the new column and refresh — ST should be fine
    db.execute("INSERT INTO se3_src (val, extra) VALUES (300, 'y')")
        .await;
    db.refresh_st("se3_st").await;
    assert_eq!(db.count("public.se3_st").await, 3);
}

// ── SE-4: Compatible type change — explicit repair ─────────────────────────

/// Widening a column type (INT → BIGINT) on the source table is suspended
/// until the operator explicitly repairs the captured source contract.
#[tokio::test]
async fn test_schema_evolution_compatible_type_change() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE se4_src (id SERIAL PRIMARY KEY, amount INT)")
        .await;
    db.execute("INSERT INTO se4_src (amount) VALUES (10), (20)")
        .await;

    db.create_st(
        "se4_st",
        "SELECT id, amount FROM se4_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.se4_st").await, 2);

    // Widen the column type. pg_trickle blocks column type changes by default
    // (block_source_ddl = true / SchemaChangeKind::ColumnChange), so we
    // temporarily disable the guard around the DDL.
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE se4_src ALTER COLUMN amount TYPE BIGINT",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    let status: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'se4_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED:SOURCE_DESTRUCTIVE_SCHEMA");

    let _: String = db
        .query_scalar("SELECT pgtrickle.reinitialize_stream_table('se4_st')")
        .await;

    // Insert a value and refresh after the explicit repair.
    db.execute("INSERT INTO se4_src (amount) VALUES (300)")
        .await;
    db.refresh_st("se4_st").await;
    assert_eq!(db.count("public.se4_st").await, 3);
}

#[tokio::test]
async fn test_schema_evolution_recreated_source_uses_new_oid() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE se5_src (id INT PRIMARY KEY, value TEXT)")
        .await;
    db.execute("INSERT INTO se5_src VALUES (1, 'old')").await;
    db.create_st("se5_st", "SELECT id, value FROM se5_src", "1m", "FULL")
        .await;
    let old_oid = db.table_oid("se5_src").await;

    db.execute("DROP TABLE se5_src").await;
    db.execute("CREATE TABLE se5_src (id INT PRIMARY KEY, value TEXT)")
        .await;
    let new_oid = db.table_oid("se5_src").await;
    assert_ne!(old_oid, new_oid);

    let expected_stable: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({new_oid}::oid)"
        ))
        .await;
    let stored_stable: Option<String> = db
        .query_scalar_opt(
            "SELECT d.source_stable_name::text \
             FROM pgtrickle.pgt_dependencies d \
             JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id \
             WHERE st.pgt_name = 'se5_st' AND d.source_type = 'TABLE' \
             LIMIT 1",
        )
        .await;
    assert_eq!(
        stored_stable.as_deref(),
        Some(expected_stable.as_str()),
        "recreated source must retain its stable identity (stored={stored_stable:?}, expected={expected_stable})"
    );

    let status: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'se5_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED:SOURCE_RECREATED");
}

#[tokio::test]
async fn test_cdc_recreated_source_does_not_replay_previous_incarnation() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    db.execute("CREATE TABLE se6_src (id INT PRIMARY KEY, value TEXT)")
        .await;
    db.execute("INSERT INTO se6_src VALUES (1, 'old')").await;
    db.try_execute_with_config(
        &["SET pg_trickle.cdc_mode = 'trigger'"],
        "SELECT pgtrickle.create_stream_table(\
            'se6_st', $$SELECT id, value FROM se6_src$$, '24h', 'DIFFERENTIAL')",
    )
    .await
    .unwrap();

    let old_oid = db.table_oid("se6_src").await;
    db.execute("UPDATE se6_src SET value = 'pending old change' WHERE id = 1")
        .await;
    let old_buffer = db.change_buffer_table(i64::from(old_oid)).await;
    let old_events: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {old_buffer} WHERE action IN ('D', 'I')"
        ))
        .await;
    assert!(
        old_events >= 2,
        "old incarnation must have pending CDC images"
    );

    db.execute("DROP TABLE se6_src").await;
    db.execute("CREATE TABLE se6_src (id INT PRIMARY KEY, value TEXT)")
        .await;
    db.execute("INSERT INTO se6_src VALUES (2, 'new incarnation')")
        .await;
    let new_oid = db.table_oid("se6_src").await;
    assert_ne!(old_oid, new_oid);

    let status: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'se6_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED:SOURCE_RECREATED");

    db.try_execute(
        "SELECT pgtrickle.alter_stream_table(\
        'se6_st', query => $$SELECT id, value FROM se6_src$$)",
    )
    .await
    .unwrap();
    let dependency_oid: i64 = db
        .query_scalar(
            "SELECT d.source_relid::bigint FROM pgtrickle.pgt_dependencies d \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = 'se6_st' AND d.source_type = 'TABLE'",
        )
        .await;
    assert_eq!(dependency_oid, i64::from(new_oid));
    e2e::oracle::assert_st_query_exact(
        &db,
        "se6_st",
        "SELECT id, value FROM se6_src",
        "reinitialization starts from the recreated source",
    )
    .await;

    db.execute("UPDATE se6_src SET value = 'new change' WHERE id = 2")
        .await;
    let before_refresh: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
            "SET pg_trickle.compact_threshold = 1",
        ],
        "SELECT pgtrickle.refresh_stream_table('se6_st')",
    )
    .await
    .unwrap();
    let refreshed_differentially: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = 'se6_st' AND h.refresh_id > {before_refresh} \
               AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' \
               AND NOT h.was_full_fallback)"
        ))
        .await;
    assert!(
        refreshed_differentially,
        "the new incarnation must resume through differential CDC"
    );
    e2e::oracle::assert_st_query_exact(
        &db,
        "se6_st",
        "SELECT id, value FROM se6_src",
        "differential refresh excludes the previous source incarnation",
    )
    .await;
}
