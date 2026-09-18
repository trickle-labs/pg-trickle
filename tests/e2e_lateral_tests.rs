//! E2E tests for LATERAL set-returning functions (SRFs) in FROM clauses.
//!
//! Tests `jsonb_array_elements`, `jsonb_each`, `unnest`, and other SRFs
//! with FULL refresh mode. AUTO falls back to FULL when an SRF body cannot be
//! inspected for incremental maintenance.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_lateral_immutable_table_function_uses_declared_out_columns_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA mdm_graph").await;
    db.execute(
        "CREATE FUNCTION mdm_graph.normalize_text(\
             raw_value text, field_kind text, normalizer_version int, \
             source_state text, options jsonb\
         ) RETURNS TABLE(normalized_value text, normalized_state text) \
         LANGUAGE sql IMMUTABLE PARALLEL SAFE AS \
         'SELECT lower(raw_value), source_state WHERE source_state <> ''DELETED'''",
    )
    .await;
    db.execute(
        "CREATE FUNCTION mdm_graph.normalize_text(\
             raw_value int, field_kind text, normalizer_version int, \
             source_state text, options jsonb\
         ) RETURNS TABLE(wrong_overload_value int) \
         LANGUAGE sql IMMUTABLE PARALLEL SAFE AS 'SELECT raw_value'",
    )
    .await;
    db.execute(
        "CREATE TABLE normalization_records (\
             id int PRIMARY KEY, raw_value text, source_state text NOT NULL\
         )",
    )
    .await;
    db.execute("INSERT INTO normalization_records VALUES (1, 'A@EXAMPLE.COM', 'ACTIVE')")
        .await;
    let defining_query = "SELECT r.id, n.normalized_value, n.normalized_state \
         FROM public.normalization_records AS r \
         CROSS JOIN LATERAL mdm_graph.normalize_text(\
             r.raw_value, 'email', 1, r.source_state, '{}'::jsonb\
         ) AS n";
    db.create_st("normalized_email", defining_query, "1m", "DIFFERENTIAL")
        .await;
    assert_eq!(db.pgt_status("normalized_email").await.1, "DIFFERENTIAL");

    for mutation in [
        "INSERT INTO normalization_records VALUES (2, 'B@EXAMPLE.COM', 'ACTIVE')",
        "UPDATE normalization_records SET raw_value = 'NEW@EXAMPLE.COM' WHERE id = 1",
        "UPDATE normalization_records SET source_state = 'DELETED' WHERE id = 2",
        "UPDATE normalization_records SET source_state = 'ACTIVE' WHERE id = 2",
        "DELETE FROM normalization_records WHERE id = 1",
    ] {
        db.execute(mutation).await;
        db.refresh_st("normalized_email").await;
        db.assert_st_matches_query("public.normalized_email", defining_query)
            .await;
    }
    let mut transaction = db.pool.begin().await.expect("begin rollback probe");
    sqlx::query("INSERT INTO normalization_records VALUES (3, 'ROLLBACK@EXAMPLE.COM', 'ACTIVE')")
        .execute(&mut *transaction)
        .await
        .expect("insert rollback probe");
    transaction.rollback().await.expect("rollback probe");
    db.refresh_st_with_retry("normalized_email").await;
    db.assert_st_matches_query("public.normalized_email", defining_query)
        .await;

    db.execute(
        "CREATE FUNCTION mdm_graph.unsafe_normalize(text) RETURNS TABLE(value text) \
         LANGUAGE sql IMMUTABLE PARALLEL UNSAFE AS 'SELECT $1'",
    )
    .await;
    let unsafe_function = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                 'unsafe_normalized', \
                 $$SELECT r.id, n.value FROM normalization_records r \
                   CROSS JOIN LATERAL mdm_graph.unsafe_normalize(r.raw_value) n$$, \
                 '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        unsafe_function.is_err(),
        "PARALLEL UNSAFE LATERAL function must be rejected"
    );
}

#[tokio::test]
async fn test_lateral_group_projection_preserves_constant_on_insert() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TABLE token_records ( \
             source_record_id integer PRIMARY KEY, field_name text NOT NULL, \
             normalized text NOT NULL, source_sort_key bytea NOT NULL)",
        "INSERT INTO token_records VALUES \
             (1, 'name', 'alice example', decode('01', 'hex'))",
    ])
    .await;
    let defining_query = "SELECT source_record_id, field_name, 'token_name'::text AS channel_id, \
                token AS block_key, source_sort_key \
           FROM token_records \
           CROSS JOIN LATERAL pg_catalog.regexp_split_to_table( \
               normalized, '[[:space:]]+') AS token(token) \
          WHERE field_name = 'name' AND pg_catalog.char_length(token) >= 3 \
          GROUP BY source_record_id, field_name, token, source_sort_key";
    db.create_st("token_blocks", defining_query, "1m", "DIFFERENTIAL")
        .await;

    db.execute(
        "INSERT INTO token_records VALUES \
             (2, 'name', 'alice third', decode('02', 'hex'))",
    )
    .await;
    db.refresh_st("token_blocks").await;
    db.assert_st_matches_query("public.token_blocks", defining_query)
        .await;
    let null_channels: i64 = db
        .query_scalar("SELECT count(*) FROM token_blocks WHERE channel_id IS NULL")
        .await;
    assert_eq!(null_channels, 0);
}

// ═══════════════════════════════════════════════════════════════════════════
//  FULL Refresh Mode
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_lateral_jsonb_array_elements_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_parent (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute(
        "INSERT INTO lat_parent VALUES \
         (1, '{\"children\": [10, 20, 30]}'), \
         (2, '{\"children\": [40, 50]}')",
    )
    .await;

    db.create_st(
        "lat_flat_full",
        "SELECT p.id, child.value AS val \
         FROM lat_parent p, \
         jsonb_array_elements(p.data->'children') AS child",
        "1m",
        "FULL",
    )
    .await;

    let (status, mode, populated, errors) = db.pgt_status("lat_flat_full").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);
    assert_eq!(errors, 0);

    // 3 children for id=1 + 2 children for id=2 = 5 rows
    assert_eq!(db.count("public.lat_flat_full").await, 5);
}

#[tokio::test]
async fn test_lateral_jsonb_each_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_kv (id INT PRIMARY KEY, props JSONB)")
        .await;
    db.execute(
        "INSERT INTO lat_kv VALUES \
         (1, '{\"color\": \"red\", \"size\": \"large\"}'), \
         (2, '{\"color\": \"blue\"}')",
    )
    .await;

    db.create_st(
        "lat_kv_full",
        "SELECT d.id, kv.key, kv.value \
         FROM lat_kv d, \
         jsonb_each(d.props) AS kv",
        "1m",
        "FULL",
    )
    .await;

    // 2 keys for id=1 + 1 key for id=2 = 3 rows
    assert_eq!(db.count("public.lat_kv_full").await, 3);
}

#[tokio::test]
async fn test_lateral_unnest_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_tags (id INT PRIMARY KEY, tags TEXT[])")
        .await;
    db.execute(
        "INSERT INTO lat_tags VALUES \
         (1, ARRAY['rust', 'postgres']), \
         (2, ARRAY['sql'])",
    )
    .await;

    db.create_st(
        "lat_tags_full",
        "SELECT t.id, tag.tag \
         FROM lat_tags t, \
         unnest(t.tags) AS tag(tag)",
        "1m",
        "FULL",
    )
    .await;

    // 2 tags for id=1 + 1 tag for id=2 = 3 rows
    assert_eq!(db.count("public.lat_tags_full").await, 3);
}

#[tokio::test]
async fn test_lateral_with_where_clause_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_arr (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute("INSERT INTO lat_arr VALUES (1, '[10, 20, 30, 5]')")
        .await;

    db.create_st(
        "lat_filtered_full",
        "SELECT a.id, (e.value)::int AS val \
         FROM lat_arr a, \
         jsonb_array_elements(a.data) AS e \
         WHERE (e.value)::int > 15",
        "1m",
        "FULL",
    )
    .await;

    // Only 20 and 30 pass the filter
    assert_eq!(db.count("public.lat_filtered_full").await, 2);
}

#[tokio::test]
async fn test_lateral_full_refresh_picks_up_changes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_fr (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute("INSERT INTO lat_fr VALUES (1, '[1, 2]')").await;

    db.create_st(
        "lat_fr_st",
        "SELECT f.id, e.value AS val \
         FROM lat_fr f, \
         jsonb_array_elements(f.data) AS e",
        "1m",
        "FULL",
    )
    .await;
    assert_eq!(db.count("public.lat_fr_st").await, 2);

    // Add more elements
    db.execute("UPDATE lat_fr SET data = '[1, 2, 3, 4]' WHERE id = 1")
        .await;
    db.refresh_st("lat_fr_st").await;

    assert_eq!(db.count("public.lat_fr_st").await, 4);
}

// ═══════════════════════════════════════════════════════════════════════════
//  AUTO Refresh Mode (FULL fallback for unsupported SRF bodies)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_lateral_jsonb_array_elements_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_diff (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute(
        "INSERT INTO lat_diff VALUES \
         (1, '[10, 20]'), \
         (2, '[30]')",
    )
    .await;

    db.create_st(
        "lat_diff_st",
        "SELECT d.id, e.value AS val \
         FROM public.lat_diff d, \
         jsonb_array_elements(d.data) AS e",
        "1m",
        "AUTO",
    )
    .await;

    let (status, mode, populated, errors) = db.pgt_status("lat_diff_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);
    assert_eq!(errors, 0);

    // 2 elements for id=1 + 1 for id=2 = 3 rows
    assert_eq!(db.count("public.lat_diff_st").await, 3);
}

#[tokio::test]
async fn test_lateral_differential_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_dins (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute("INSERT INTO lat_dins VALUES (1, '[1, 2]')")
        .await;

    db.create_st(
        "lat_dins_st",
        "SELECT d.id, e.value AS val \
         FROM lat_dins d, \
         jsonb_array_elements(d.data) AS e",
        "1m",
        "AUTO",
    )
    .await;
    assert_eq!(db.count("public.lat_dins_st").await, 2);

    // Insert a new source row with 3 elements
    db.execute("INSERT INTO lat_dins VALUES (2, '[10, 20, 30]')")
        .await;
    db.refresh_st("lat_dins_st").await;

    // Should now have 2 + 3 = 5 rows
    assert_eq!(db.count("public.lat_dins_st").await, 5);

    // Verify data matches the defining query
    db.assert_st_matches_query(
        "public.lat_dins_st",
        "SELECT d.id, e.value AS val FROM lat_dins d, jsonb_array_elements(d.data) AS e",
    )
    .await;
}

#[tokio::test]
async fn test_lateral_differential_delete() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_ddel (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute(
        "INSERT INTO lat_ddel VALUES \
         (1, '[1, 2]'), \
         (2, '[3, 4, 5]')",
    )
    .await;

    db.create_st(
        "lat_ddel_st",
        "SELECT d.id, e.value AS val \
         FROM lat_ddel d, \
         jsonb_array_elements(d.data) AS e",
        "1m",
        "AUTO",
    )
    .await;
    assert_eq!(db.count("public.lat_ddel_st").await, 5);

    // Delete source row id=2 → its 3 expanded rows should disappear
    db.execute("DELETE FROM lat_ddel WHERE id = 2").await;
    db.refresh_st("lat_ddel_st").await;

    assert_eq!(db.count("public.lat_ddel_st").await, 2);

    db.assert_st_matches_query(
        "public.lat_ddel_st",
        "SELECT d.id, e.value AS val FROM lat_ddel d, jsonb_array_elements(d.data) AS e",
    )
    .await;
}

#[tokio::test]
async fn test_lateral_differential_update_array() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_dupd (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute("INSERT INTO lat_dupd VALUES (1, '[1, 2, 3]')")
        .await;

    db.create_st(
        "lat_dupd_st",
        "SELECT d.id, e.value AS val \
         FROM lat_dupd d, \
         jsonb_array_elements(d.data) AS e",
        "1m",
        "AUTO",
    )
    .await;
    assert_eq!(db.count("public.lat_dupd_st").await, 3);

    // Update the array: old [1,2,3] → new [10, 20]
    db.execute("UPDATE lat_dupd SET data = '[10, 20]' WHERE id = 1")
        .await;
    db.refresh_st("lat_dupd_st").await;

    // Should now have 2 rows instead of 3
    assert_eq!(db.count("public.lat_dupd_st").await, 2);

    db.assert_st_matches_query(
        "public.lat_dupd_st",
        "SELECT d.id, e.value AS val FROM lat_dupd d, jsonb_array_elements(d.data) AS e",
    )
    .await;
}

#[tokio::test]
async fn test_lateral_differential_mixed_dml() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_dmix (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute(
        "INSERT INTO lat_dmix VALUES \
         (1, '[1, 2]'), \
         (2, '[3]'), \
         (3, '[4, 5, 6]')",
    )
    .await;

    db.create_st(
        "lat_dmix_st",
        "SELECT d.id, e.value AS val \
         FROM lat_dmix d, \
         jsonb_array_elements(d.data) AS e",
        "1m",
        "AUTO",
    )
    .await;
    assert_eq!(db.count("public.lat_dmix_st").await, 6);

    // Mixed DML in one batch
    db.execute("INSERT INTO lat_dmix VALUES (4, '[7, 8]')")
        .await;
    db.execute("UPDATE lat_dmix SET data = '[10]' WHERE id = 1")
        .await;
    db.execute("DELETE FROM lat_dmix WHERE id = 2").await;

    db.refresh_st("lat_dmix_st").await;

    // id=1: was 2 elements → now 1 element (10)
    // id=2: deleted → 0 elements
    // id=3: unchanged → 3 elements
    // id=4: new → 2 elements (7, 8)
    // Total: 1 + 0 + 3 + 2 = 6
    assert_eq!(db.count("public.lat_dmix_st").await, 6);

    db.assert_st_matches_query(
        "public.lat_dmix_st",
        "SELECT d.id, e.value AS val FROM lat_dmix d, jsonb_array_elements(d.data) AS e",
    )
    .await;
}

#[tokio::test]
async fn test_lateral_differential_empty_array() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_empty (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute(
        "INSERT INTO lat_empty VALUES \
         (1, '[1, 2]'), \
         (2, '[]')",
    )
    .await;

    db.create_st(
        "lat_empty_st",
        "SELECT d.id, e.value AS val \
         FROM lat_empty d, \
         jsonb_array_elements(d.data) AS e",
        "1m",
        "AUTO",
    )
    .await;

    // id=2 has empty array → produces no rows
    assert_eq!(db.count("public.lat_empty_st").await, 2);

    // Update id=2 to have elements
    db.execute("UPDATE lat_empty SET data = '[3, 4]' WHERE id = 2")
        .await;
    db.refresh_st("lat_empty_st").await;

    assert_eq!(db.count("public.lat_empty_st").await, 4);

    db.assert_st_matches_query(
        "public.lat_empty_st",
        "SELECT d.id, e.value AS val FROM lat_empty d, jsonb_array_elements(d.data) AS e",
    )
    .await;
}

#[tokio::test]
async fn test_lateral_unnest_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_utags (id INT PRIMARY KEY, tags TEXT[])")
        .await;
    db.execute(
        "INSERT INTO lat_utags VALUES \
         (1, ARRAY['rust', 'postgres']), \
         (2, ARRAY['sql'])",
    )
    .await;

    db.create_st(
        "lat_utags_st",
        "SELECT t.id, tag.tag \
         FROM lat_utags t, \
         unnest(t.tags) AS tag(tag)",
        "1m",
        "AUTO",
    )
    .await;
    assert_eq!(db.count("public.lat_utags_st").await, 3);

    // Add more tags
    db.execute("UPDATE lat_utags SET tags = ARRAY['rust', 'postgres', 'pgrx'] WHERE id = 1")
        .await;
    db.refresh_st("lat_utags_st").await;

    assert_eq!(db.count("public.lat_utags_st").await, 4);

    db.assert_st_matches_query(
        "public.lat_utags_st",
        "SELECT t.id, tag.tag FROM lat_utags t, unnest(t.tags) AS tag(tag)",
    )
    .await;
}

#[tokio::test]
async fn test_lateral_jsonb_each_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_dkv (id INT PRIMARY KEY, props JSONB)")
        .await;
    db.execute(
        "INSERT INTO lat_dkv VALUES \
         (1, '{\"color\": \"red\", \"size\": \"large\"}'), \
         (2, '{\"shape\": \"round\"}')",
    )
    .await;

    db.create_st(
        "lat_dkv_st",
        "SELECT d.id, kv.key, kv.value \
         FROM lat_dkv d, \
         jsonb_each(d.props) AS kv",
        "1m",
        "AUTO",
    )
    .await;
    assert_eq!(db.count("public.lat_dkv_st").await, 3);

    // Add a property to id=2
    db.execute(
        "UPDATE lat_dkv SET props = '{\"shape\": \"round\", \"weight\": \"heavy\"}' WHERE id = 2",
    )
    .await;
    db.refresh_st("lat_dkv_st").await;

    assert_eq!(db.count("public.lat_dkv_st").await, 4);

    db.assert_st_matches_query(
        "public.lat_dkv_st",
        "SELECT d.id, kv.key, kv.value FROM lat_dkv d, jsonb_each(d.props) AS kv",
    )
    .await;
}

#[tokio::test]
async fn test_lateral_with_where_clause_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_filt (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute("INSERT INTO lat_filt VALUES (1, '[5, 15, 25]'), (2, '[10, 30]')")
        .await;

    db.create_st(
        "lat_filt_st",
        "SELECT f.id, (e.value)::int AS val \
         FROM lat_filt f, \
         jsonb_array_elements(f.data) AS e \
         WHERE (e.value)::int > 12",
        "1m",
        "AUTO",
    )
    .await;

    // id=1: 15, 25 pass filter → 2 rows; id=2: 30 passes → 1 row; total 3
    assert_eq!(db.count("public.lat_filt_st").await, 3);

    // Update id=1 to have higher values
    db.execute("UPDATE lat_filt SET data = '[20, 30, 40]' WHERE id = 1")
        .await;
    db.refresh_st("lat_filt_st").await;

    // id=1: all pass → 3 rows; id=2: unchanged → 1 row; total 4
    assert_eq!(db.count("public.lat_filt_st").await, 4);

    db.assert_st_matches_query(
        "public.lat_filt_st",
        "SELECT f.id, (e.value)::int AS val FROM lat_filt f, jsonb_array_elements(f.data) AS e WHERE (e.value)::int > 12",
    )
    .await;
}

#[tokio::test]
async fn test_lateral_with_aggregation_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_agg (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute(
        "INSERT INTO lat_agg VALUES \
         (1, '[10, 20, 30]'), \
         (2, '[5, 15]')",
    )
    .await;

    // SRF expansion + aggregation: count elements per parent
    db.create_st(
        "lat_agg_st",
        "SELECT a.id, count(*) AS elem_count \
         FROM lat_agg a, \
         jsonb_array_elements(a.data) AS e \
         GROUP BY a.id",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.lat_agg_st").await, 2);

    let count_1: i64 = db
        .query_scalar("SELECT elem_count FROM public.lat_agg_st WHERE id = 1")
        .await;
    assert_eq!(count_1, 3);

    let count_2: i64 = db
        .query_scalar("SELECT elem_count FROM public.lat_agg_st WHERE id = 2")
        .await;
    assert_eq!(count_2, 2);
}

#[tokio::test]
async fn test_lateral_multiple_refreshes_converge() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_conv (id INT PRIMARY KEY, data JSONB)")
        .await;
    db.execute("INSERT INTO lat_conv VALUES (1, '[1]')").await;

    db.create_st(
        "lat_conv_st",
        "SELECT d.id, e.value AS val \
         FROM lat_conv d, \
         jsonb_array_elements(d.data) AS e",
        "1m",
        "AUTO",
    )
    .await;
    assert_eq!(db.count("public.lat_conv_st").await, 1);

    // Multiple mutations + refreshes
    db.execute("UPDATE lat_conv SET data = '[1, 2]' WHERE id = 1")
        .await;
    db.refresh_st("lat_conv_st").await;
    assert_eq!(db.count("public.lat_conv_st").await, 2);

    db.execute("INSERT INTO lat_conv VALUES (2, '[3, 4, 5]')")
        .await;
    db.refresh_st("lat_conv_st").await;
    assert_eq!(db.count("public.lat_conv_st").await, 5);

    db.execute("DELETE FROM lat_conv WHERE id = 1").await;
    db.refresh_st("lat_conv_st").await;
    assert_eq!(db.count("public.lat_conv_st").await, 3);

    db.assert_st_matches_query(
        "public.lat_conv_st",
        "SELECT d.id, e.value AS val FROM lat_conv d, jsonb_array_elements(d.data) AS e",
    )
    .await;
}
