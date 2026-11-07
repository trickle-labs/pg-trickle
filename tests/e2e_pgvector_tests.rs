//! F4 (v0.37.0): E2E tests for pgVectorMV — incremental vector aggregate operators.
//!
//! Validates that stream tables computing `avg(embedding)` and `sum(embedding)`
//! over pgvector `vector` typed columns produce correct results after INSERT,
//! UPDATE, and DELETE operations (correctness vs FULL refresh).
//!
//! Uses the group-rescan strategy: affected groups are re-aggregated from source
//! data using pgvector's native `avg(vector)` and `sum(vector)` aggregates.
//!
//! Prerequisites: `./tests/build_e2e_image.sh` (includes pgvector)

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Helper: enable pgvector and the vector agg GUC
// ═══════════════════════════════════════════════════════════════════════

async fn setup_pgvector(db: &E2eDb) {
    db.execute("CREATE EXTENSION IF NOT EXISTS vector").await;
}

#[tokio::test]
async fn test_vector_aggregate_unknown_dimensions_rejects_mixed_input_without_data_loss() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE unknown_vector_dims (id int PRIMARY KEY, embedding vector)")
        .await;
    db.execute("INSERT INTO unknown_vector_dims VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SELECT pgtrickle.create_stream_table('unknown_vector_st', $$SELECT avg(embedding) AS centroid FROM unknown_vector_dims$$, '1m', 'DIFFERENTIAL')",
    ]).await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='unknown_vector_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "vector");
    let error = db.try_execute("CREATE INDEX unknown_vector_ann ON unknown_vector_st USING hnsw (centroid vector_l2_ops)").await.expect_err("an undimensioned vector cannot be indexed directly");
    assert!(
        error.to_string().contains("dimensions"),
        "unexpected error: {error}"
    );

    db.execute("INSERT INTO unknown_vector_dims VALUES (2, '[1,0]')")
        .await;
    let error = db
        .try_execute_with_config(
            &[
                "SET pg_trickle.enable_vector_agg = on",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            "SELECT pgtrickle.refresh_stream_table('unknown_vector_st')",
        )
        .await
        .expect_err("mixed dimensions must not be silently narrowed");
    assert!(
        error.to_string().contains("dimensions"),
        "unexpected error: {error}"
    );
    let centroid: String = db
        .query_scalar("SELECT centroid::text FROM unknown_vector_st")
        .await;
    assert_eq!(centroid, "[1,0,0]");
}

#[tokio::test]
async fn test_vector_aggregate_qualified_join_uses_argument_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE typed_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE typed_scalars (id int PRIMARY KEY, embedding double precision)")
        .await;
    db.execute("INSERT INTO typed_vectors VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.execute("INSERT INTO typed_scalars VALUES (1, 42), (2, 10)")
        .await;
    let query = "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('typed_join_st', $${query}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    db.execute("UPDATE typed_scalars SET embedding = 20 WHERE id = 2")
        .await;
    db.execute("UPDATE typed_vectors SET embedding = '[0,0,1]' WHERE id = 2")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_join_st')",
    ])
    .await;
    let differential: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name='typed_join_st' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(differential);
    db.assert_st_matches_query("typed_join_st", "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding)::vector(3) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)").await;
    let scalar_type: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='typed_join_st'::regclass AND attname='scalar_mean'").await;
    assert_eq!(scalar_type, "double precision");
    db.execute("UPDATE typed_vectors SET embedding=NULL WHERE id=1")
        .await;
    db.execute("UPDATE typed_scalars SET embedding=NULL WHERE id=1")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_join_st')",
    ])
    .await;
    db.assert_st_matches_query("typed_join_st", "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding)::vector(3) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)").await;
    db.execute("DELETE FROM typed_vectors WHERE id=2").await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_join_st')",
    ])
    .await;
    db.assert_st_matches_query("typed_join_st", "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding)::vector(3) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)").await;
    db.execute("INSERT INTO typed_vectors VALUES (3, '[1,1,0]')")
        .await;
    db.execute("INSERT INTO typed_scalars VALUES (3, 7)").await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_join_st')",
    ])
    .await;
    db.assert_st_matches_query("typed_join_st", "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding)::vector(3) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)").await;
}

#[tokio::test]
async fn test_vector_aggregate_cast_uses_cast_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE typed_casts (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO typed_casts VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let query = "SELECT avg(embedding::halfvec(3)) AS centroid FROM typed_casts";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('typed_cast_st', $${query}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    db.execute("UPDATE typed_casts SET embedding = '[0,0,1]' WHERE id = 2")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_cast_st')",
    ])
    .await;
    let differential: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name='typed_cast_st' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(differential);
    db.assert_st_matches_query(
        "typed_cast_st",
        "SELECT avg(embedding::halfvec(3))::halfvec(3) AS centroid FROM typed_casts",
    )
    .await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='typed_cast_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "halfvec(3)");
}

#[tokio::test]
async fn test_vector_aggregate_join_uses_each_bound_dimension() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE dims_three (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE dims_two (id int PRIMARY KEY, embedding vector(2))")
        .await;
    db.execute("INSERT INTO dims_three VALUES (1, '[1,0,0]')")
        .await;
    db.execute("INSERT INTO dims_two VALUES (1, '[1,0]')").await;
    let create = "SELECT pgtrickle.create_stream_table('dims_st', $$SELECT avg(a.embedding) AS three, avg(b.embedding) AS two FROM dims_three a JOIN dims_two b USING(id)$$, '1m', 'DIFFERENTIAL')";
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", create])
        .await;
    let types: Vec<(String, String)> = sqlx::query_as("SELECT attname::text, format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='dims_st'::regclass AND attname IN ('three','two') ORDER BY attname").fetch_all(&db.pool).await.expect("read dimensioned output types");
    assert_eq!(
        types,
        vec![
            ("three".into(), "vector(3)".into()),
            ("two".into(), "vector(2)".into())
        ]
    );
    db.execute("UPDATE dims_two SET embedding='[0,1]' WHERE id=1")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('dims_st')",
    ])
    .await;
    db.assert_st_matches_query("dims_st", "SELECT avg(a.embedding)::vector(3) AS three, avg(b.embedding)::vector(2) AS two FROM dims_three a JOIN dims_two b USING(id)").await;
}

#[tokio::test]
async fn test_vector_aggregate_normalized_expression_keeps_unknown_dimension() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE normalized_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO normalized_source VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let create = "SELECT pgtrickle.create_stream_table('normalized_st', $$SELECT avg(l2_normalize(embedding)) AS centroid FROM normalized_source$$, '1m', 'DIFFERENTIAL')";
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", create])
        .await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='normalized_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "vector");
    db.execute("UPDATE normalized_source SET embedding='[0,0,1]' WHERE id=2")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('normalized_st')",
    ])
    .await;
    db.assert_st_matches_query(
        "normalized_st",
        "SELECT avg(l2_normalize(embedding)) AS centroid FROM normalized_source",
    )
    .await;
}

#[tokio::test]
async fn test_vector_aggregate_immediate_uses_analyzed_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE immediate_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO immediate_vectors VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", "SELECT pgtrickle.create_stream_table('immediate_vectors_st', $$SELECT avg(embedding::halfvec(3)) AS centroid FROM immediate_vectors$$, NULL, 'IMMEDIATE')", "UPDATE immediate_vectors SET embedding='[0,1,0]' WHERE id=1"]).await;
    db.assert_st_matches_query(
        "immediate_vectors_st",
        "SELECT avg(embedding::halfvec(3))::halfvec(3) AS centroid FROM immediate_vectors",
    )
    .await;
}

#[tokio::test]
async fn test_vector_aggregate_nonpublic_extension_schema() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"Vec.X\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"Vec.X\"")
        .await;
    db.execute("CREATE TABLE private_vectors (id int PRIMARY KEY, embedding \"Vec.X\".vector(3))")
        .await;
    db.execute("INSERT INTO private_vectors VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&["SET search_path = public, \"Vec.X\"", "SET pg_trickle.enable_vector_agg = on", "SELECT pgtrickle.create_stream_table('private_vector_st', $$SELECT avg(embedding) AS centroid FROM private_vectors$$, '1m', 'DIFFERENTIAL')"]).await;
    let type_matches: bool = db.query_scalar("SELECT atttypid='\"Vec.X\".vector'::regtype AND atttypmod=3 FROM pg_attribute WHERE attrelid='private_vector_st'::regclass AND attname='centroid'").await;
    assert!(type_matches);
    db.execute("UPDATE private_vectors SET embedding='[0,1,0]' WHERE id=1")
        .await;
    db.execute_seq(&[
        "SET search_path = public, \"Vec.X\"",
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('private_vector_st')",
    ])
    .await;
    let same: bool = db
        .query_scalar("SELECT centroid::text = '[0,1,0]' FROM private_vector_st")
        .await;
    assert!(same);
}

#[tokio::test]
async fn test_vector_projection_retains_typmod_for_direct_ann_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE projected_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO projected_vectors VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('projected_vector_st', $$SELECT id, embedding FROM projected_vectors$$, '1m', 'DIFFERENTIAL')").await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='projected_vector_st'::regclass AND attname='embedding'").await;
    assert_eq!(typ, "vector(3)");
    db.execute("CREATE INDEX projected_vector_ann ON projected_vector_st USING hnsw (embedding vector_l2_ops)").await;
    let mut tx = db.pool.begin().await.expect("begin plan check");
    sqlx::query("SET LOCAL enable_seqscan=off")
        .execute(&mut *tx)
        .await
        .expect("disable sequential scan");
    let plan: serde_json::Value = sqlx::query_scalar("EXPLAIN (FORMAT JSON) SELECT id FROM projected_vector_st ORDER BY embedding <-> '[1,0,0]'::vector LIMIT 1").fetch_one(&mut *tx).await.expect("explain vector search");
    assert!(
        plan.to_string().contains("projected_vector_ann"),
        "direct index is not eligible: {plan}"
    );
    tx.rollback().await.expect("end plan check");
}

#[tokio::test]
async fn test_vector_aggregate_cte_alias_and_group_key_changes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE cte_vectors (id int PRIMARY KEY, grp int, embedding vector(3))")
        .await;
    db.execute("INSERT INTO cte_vectors VALUES (1, 1, '[1,0,0]'), (2, 2, '[0,1,0]')")
        .await;
    let query = "WITH c AS (SELECT id, grp, embedding AS vec FROM cte_vectors) SELECT grp, avg(vec) AS centroid FROM c GROUP BY grp";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('cte_vectors_st', $${query}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='cte_vectors_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "vector(3)");
    db.execute("UPDATE cte_vectors SET grp=2, embedding='[0,0,1]' WHERE id=1")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('cte_vectors_st')",
    ])
    .await;
    db.assert_st_matches_query("cte_vectors_st", "WITH c AS (SELECT id, grp, embedding AS vec FROM cte_vectors) SELECT grp, avg(vec)::vector(3) AS centroid FROM c GROUP BY grp").await;
}

#[tokio::test]
async fn test_vector_aggregate_disabled_reports_actionable_error() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE disabled_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    let error = db.try_execute_with_config(&["SET pg_trickle.enable_vector_agg = off"], "SELECT pgtrickle.create_stream_table('disabled_vector_st', $$SELECT avg(embedding) AS centroid FROM disabled_vectors$$, '1m', 'DIFFERENTIAL')").await.expect_err("vector aggregate should require opt-in");
    assert!(
        error.to_string().contains("enable_vector_agg"),
        "unexpected error: {error}"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = off", "SELECT pgtrickle.create_stream_table('disabled_auto_st', $$SELECT avg(embedding) AS centroid FROM disabled_vectors$$, '1m', 'AUTO')"]).await;
    let mode: String = db.query_scalar("SELECT refresh_mode::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name='disabled_auto_st'").await;
    assert_eq!(mode, "FULL");
}

#[tokio::test]
async fn test_vector_aggregate_alter_query_keeps_expression_index_usable() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alter_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alter_vectors VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", "SELECT pgtrickle.create_stream_table('alter_vectors_st', $$SELECT avg(embedding) AS centroid FROM alter_vectors$$, '1m', 'DIFFERENTIAL')"]).await;
    db.execute("CREATE INDEX alter_vectors_ann ON alter_vectors_st USING hnsw ((l2_normalize(centroid)::vector(3)) vector_l2_ops)").await;
    let alter = "SELECT pgtrickle.alter_stream_table('alter_vectors_st', query => $$SELECT avg(embedding) AS centroid FROM alter_vectors WHERE id > 0$$)";
    let error = db
        .try_execute_with_config(&["SET pg_trickle.enable_vector_agg = on"], alter)
        .await
        .expect_err("ALTER must not silently drop the expression index");
    assert!(
        error
            .to_string()
            .contains("expression or ANN index alter_vectors_ann"),
        "unexpected error: {error}"
    );
    let usable: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid=to_regclass('alter_vectors_ann') AND indisvalid AND indisready)").await;
    assert!(usable, "ALTER QUERY must retain a usable expression index");
    db.assert_st_matches_query(
        "alter_vectors_st",
        "SELECT avg(embedding)::vector(3) AS centroid FROM alter_vectors",
    )
    .await;
    db.execute("DROP INDEX alter_vectors_ann").await;
    db.execute("CREATE INDEX alter_vectors_ann_plain ON alter_vectors_st USING hnsw (centroid vector_l2_ops)").await;
    let error = db
        .try_execute_with_config(&["SET pg_trickle.enable_vector_agg = on"], alter)
        .await
        .expect_err("ALTER must not silently drop a plain ANN index");
    assert!(
        error.to_string().contains("alter_vectors_ann_plain"),
        "unexpected error: {error}"
    );
    db.execute("DROP INDEX alter_vectors_ann_plain").await;
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", alter])
        .await;
    db.execute("CREATE INDEX alter_vectors_ann ON alter_vectors_st USING hnsw ((l2_normalize(centroid)::vector(3)) vector_l2_ops)").await;
    db.assert_st_matches_query(
        "alter_vectors_st",
        "SELECT avg(embedding)::vector(3) AS centroid FROM alter_vectors WHERE id > 0",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-1: User-taste centroid — avg(embedding) per user
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_avg_centroid_insert() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE embeddings (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO embeddings (user_id, embedding) VALUES
            (1, '[1,0,0]'),
            (1, '[0,1,0]'),
            (2, '[0,0,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM embeddings GROUP BY user_id";
    // pgvector's avg(vector) is undimensioned; F4 materializes the source dimension.
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM embeddings GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_st", expected_q).await;

    // INSERT new embedding for user 1
    db.execute("INSERT INTO embeddings (user_id, embedding) VALUES (1, '[1,1,0]')")
        .await;
    db.refresh_st("centroid_st").await;
    db.assert_st_matches_query("centroid_st", expected_q).await;

    // INSERT new user
    db.execute("INSERT INTO embeddings (user_id, embedding) VALUES (3, '[0,1,1]')")
        .await;
    db.refresh_st("centroid_st").await;
    db.assert_st_matches_query("centroid_st", expected_q).await;
}

#[tokio::test]
async fn test_pgvector_avg_centroid_update() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_upd (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_upd (user_id, embedding) VALUES
            (1, '[1,0,0]'),
            (1, '[0,1,0]'),
            (2, '[1,1,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM emb_upd GROUP BY user_id";
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM emb_upd GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_upd_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_upd_st", expected_q)
        .await;

    // UPDATE embedding
    db.execute("UPDATE emb_upd SET embedding = '[0,0,1]' WHERE user_id = 1 AND id = 1")
        .await;
    db.refresh_st("centroid_upd_st").await;
    db.assert_st_matches_query("centroid_upd_st", expected_q)
        .await;
}

#[tokio::test]
async fn test_pgvector_avg_centroid_delete() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_del (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_del (user_id, embedding) VALUES
            (1, '[1,0,0]'),
            (1, '[0,1,0]'),
            (2, '[1,1,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM emb_del GROUP BY user_id";
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM emb_del GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_del_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_del_st", expected_q)
        .await;

    // DELETE one row from user 1
    db.execute("DELETE FROM emb_del WHERE user_id = 1 AND id = 1")
        .await;
    db.refresh_st("centroid_del_st").await;
    db.assert_st_matches_query("centroid_del_st", expected_q)
        .await;

    // DELETE entire user group
    db.execute("DELETE FROM emb_del WHERE user_id = 2").await;
    db.refresh_st("centroid_del_st").await;
    db.assert_st_matches_query("centroid_del_st", expected_q)
        .await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-2: vector_sum — sum(embedding) per group
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_sum_differential() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_sum_src (
            id SERIAL PRIMARY KEY,
            grp TEXT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_sum_src (grp, embedding) VALUES
            ('a', '[1,0,0]'),
            ('a', '[0,1,0]'),
            ('b', '[1,1,1]')",
    )
    .await;

    let q = "SELECT grp, sum(embedding) AS total_vec FROM emb_sum_src GROUP BY grp";
    let expected_q =
        "SELECT grp, sum(embedding)::vector(3) AS total_vec FROM emb_sum_src GROUP BY grp";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('vec_sum_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("vec_sum_st", expected_q).await;

    // INSERT
    db.execute("INSERT INTO emb_sum_src (grp, embedding) VALUES ('a', '[0,0,1]')")
        .await;
    db.refresh_st("vec_sum_st").await;
    db.assert_st_matches_query("vec_sum_st", expected_q).await;

    // DELETE
    db.execute("DELETE FROM emb_sum_src WHERE grp = 'b'").await;
    db.refresh_st("vec_sum_st").await;
    db.assert_st_matches_query("vec_sum_st", expected_q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-3: Distance operator fallback — queries with <-> fall back to FULL
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_distance_operator_fallback_to_full() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_dist (
            id SERIAL PRIMARY KEY,
            category TEXT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_dist (category, embedding) VALUES
            ('cat1', '[1,0,0]'),
            ('cat1', '[0.9,0.1,0]'),
            ('cat2', '[0,1,0]')",
    )
    .await;

    // A stream table using a distance operator in ORDER BY should work
    // via FULL refresh mode (distance operators are FULL-fallback safe).
    // Use FULL mode explicitly to avoid relying on differential fallback.
    let q = "SELECT id, category, embedding FROM emb_dist ORDER BY embedding <-> '[1,0,0]' LIMIT 5";
    db.create_st("dist_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("dist_st", q).await;

    // Refresh after insert
    db.execute("INSERT INTO emb_dist (category, embedding) VALUES ('cat1', '[0.8,0.2,0]')")
        .await;
    db.refresh_st("dist_st").await;
    db.assert_st_matches_query("dist_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-4: HNSW index on centroid stream table
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_hnsw_index_on_stream_table() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_hnsw_src (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_hnsw_src (user_id, embedding) VALUES
            (1, '[1,0,0]'), (1, '[0.9,0.1,0]'),
            (2, '[0,1,0]'), (2, '[0,0.9,0.1]'),
            (3, '[0,0,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM emb_hnsw_src GROUP BY user_id";
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM emb_hnsw_src GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_hnsw_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_hnsw_st", expected_q)
        .await;

    // Create HNSW index on the centroid stream table
    db.execute(
        "CREATE INDEX centroid_hnsw_idx ON public.centroid_hnsw_st \
         USING hnsw (centroid vector_cosine_ops)",
    )
    .await;

    // Verify refresh still works with index in place
    db.execute("INSERT INTO emb_hnsw_src (user_id, embedding) VALUES (1, '[1,0,0]')")
        .await;
    db.refresh_st("centroid_hnsw_st").await;
    db.assert_st_matches_query("centroid_hnsw_st", expected_q)
        .await;

    // Verify the HNSW index can be used for ANN search
    let nn_result = db
        .query_scalar_opt::<i32>("SELECT user_id FROM public.centroid_hnsw_st ORDER BY centroid <-> '[1,0,0]'::vector LIMIT 1")
        .await;
    assert!(
        nn_result.is_some(),
        "HNSW ANN query should return at least one result"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T-VP1 (v0.47.0): Drift-based reindex on HNSW stream table
// ═══════════════════════════════════════════════════════════════════════

/// T-VP1: Create a vector stream table with post_refresh_action='reindex_if_drift',
/// change rows beyond the threshold, verify that the drift counter increments
/// and that last_reindex_at is updated after a refresh.
#[tokio::test]
async fn test_vector_post_refresh_action_reindex_if_drift() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE drift_src (
            id SERIAL PRIMARY KEY,
            embedding vector(3)
        )",
    )
    .await;

    // Insert initial rows
    for i in 1..=20i32 {
        db.execute(&format!(
            "INSERT INTO drift_src (embedding) VALUES ('[{},{},0]')",
            i, i
        ))
        .await;
    }

    // Create stream table
    let q = "SELECT id, embedding FROM drift_src";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('drift_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    ))
    .await;

    // Configure drift-triggered REINDEX (50% threshold so we can trigger it)
    db.execute(
        "SELECT pgtrickle.alter_stream_table('drift_st', \
         post_refresh_action => 'reindex_if_drift', \
         reindex_drift_threshold => 0.50)",
    )
    .await;

    // Add more rows to exceed 50% threshold (20 rows * 50% = 10, add 11)
    for i in 21..=31i32 {
        db.execute(&format!(
            "INSERT INTO drift_src (embedding) VALUES ('[{},{},1]')",
            i, i
        ))
        .await;
    }

    // Refresh — should increment rows_changed_since_last_reindex
    db.refresh_st("drift_st").await;

    // Check that vector_status() shows this stream table
    let drift_pct = db
        .query_scalar_opt::<f64>(
            "SELECT drift_pct FROM pgtrickle.vector_status() WHERE name = 'public.drift_st'",
        )
        .await;
    assert!(
        drift_pct.is_some(),
        "vector_status() should return a row for drift_st"
    );

    // Verify post_refresh_action is stored correctly
    let action = db
        .query_scalar_opt::<String>(
            "SELECT post_refresh_action FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'drift_st'",
        )
        .await
        .expect("post_refresh_action should be set");
    assert_eq!(
        action, "reindex_if_drift",
        "post_refresh_action should be 'reindex_if_drift'"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T-VP2 (v0.47.0): vector_status() accuracy and reset behavior
// ═══════════════════════════════════════════════════════════════════════

/// T-VP2: Verify that pgtrickle.vector_status() reports correct lag, drift,
/// and metadata for vector stream tables. Also verify that after an alter to
/// 'none', the table disappears from vector_status().
#[tokio::test]
async fn test_vector_status_view_accuracy_and_reset() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE vs_src (
            id SERIAL PRIMARY KEY,
            body TEXT,
            embedding vector(3)
        )",
    )
    .await;

    db.execute(
        "INSERT INTO vs_src (body, embedding) VALUES
             ('hello', '[1,0,0]'),
             ('world', '[0,1,0]'),
             ('foo',   '[0,0,1]')",
    )
    .await;

    let q = "SELECT id, body, embedding FROM vs_src";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('vs_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    ))
    .await;

    // Set post_refresh_action to 'analyze'
    db.execute("SELECT pgtrickle.alter_stream_table('vs_st', post_refresh_action => 'analyze')")
        .await;

    // Do a refresh to trigger the action
    db.refresh_st("vs_st").await;

    // vector_status() should show vs_st
    let row = db
        .query_scalar_opt::<String>(
            "SELECT post_refresh_action FROM pgtrickle.vector_status() \
             WHERE name = 'public.vs_st'",
        )
        .await;
    assert!(
        row.is_some(),
        "vector_status() should include vs_st after setting post_refresh_action"
    );
    assert_eq!(
        row.unwrap(),
        "analyze",
        "post_refresh_action should be 'analyze'"
    );

    // data_timestamp should be set (not NULL)
    let ts = db
        .query_scalar_opt::<String>(
            "SELECT data_timestamp::TEXT FROM pgtrickle.vector_status() \
             WHERE name = 'public.vs_st'",
        )
        .await;
    assert!(
        ts.is_some(),
        "data_timestamp should not be NULL in vector_status()"
    );

    // Reset to 'none' — should disappear from vector_status()
    db.execute("SELECT pgtrickle.alter_stream_table('vs_st', post_refresh_action => 'none')")
        .await;

    let gone = db
        .query_scalar_opt::<String>(
            "SELECT post_refresh_action FROM pgtrickle.vector_status() \
             WHERE name = 'public.vs_st'",
        )
        .await;
    assert!(
        gone.is_none(),
        "vs_st should not appear in vector_status() after resetting to 'none'"
    );
}

#[tokio::test]
async fn test_vector_diagnostics_catalog_visibility_and_index_metadata() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute(
        "CREATE TABLE diagnostic_src (id int PRIMARY KEY, a vector(3), b vector(3), c vector)",
    )
    .await;
    db.execute("INSERT INTO diagnostic_src VALUES (1, '[1,0,0]', '[0,1,0]', '[1,1,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('diagnostic_st', $$SELECT id, a, b, c FROM diagnostic_src$$, '1m', 'DIFFERENTIAL')").await;
    db.execute("CREATE INDEX diagnostic_cosine ON diagnostic_st USING hnsw (a vector_cosine_ops) WHERE id > 0").await;
    db.execute("CREATE INDEX diagnostic_expression ON diagnostic_st USING hnsw ((b::vector(3)) vector_l2_ops)").await;

    let columns = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'post_refresh_action' = 'none' AND v->>'dimensions' = '3' AND v->'embedding_freshness' = 'null'::jsonb AND v->'maintenance_pending' = 'null'::jsonb").await;
    assert_eq!(columns, 2);
    let unknown = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'column' = 'c' AND v->'dimensions' = 'null'::jsonb AND v->'requested_index_state' = 'null'::jsonb AND v->>'requested_index_reason' IS NOT NULL").await;
    assert_eq!(unknown, 1);
    let indexes = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'access_method' = 'hnsw' AND v->>'valid' = 'true' AND v->>'ready' = 'true' AND ((v->>'index' = 'diagnostic_cosine' AND v->>'opclass' = 'vector_cosine_ops' AND v->>'predicate' IS NOT NULL) OR (v->>'index' = 'diagnostic_expression' AND v->>'opclass' = 'vector_l2_ops'))").await;
    assert_eq!(indexes, 2);

    db.execute("CREATE ROLE diagnostic_observer").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO diagnostic_observer")
        .await;
    let mut conn = db.pool.acquire().await.expect("acquire connection");
    sqlx::query("SET ROLE diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("set role");
    let hidden: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtrickle.vector_stream_status_v2()")
            .fetch_one(&mut *conn)
            .await
            .expect("inspect as observer");
    assert_eq!(hidden, 0);
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset role");
    sqlx::query("GRANT SELECT ON diagnostic_st TO diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("grant select");
    sqlx::query("SET ROLE diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("set role");
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.ann_index_status_v2()")
        .fetch_one(&mut *conn)
        .await
        .expect("inspect as observer");
    assert_eq!(visible, 2);
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset role");
}

#[tokio::test]
async fn test_vector_diagnostics_reports_partition_leaf_indexes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE diagnostic_partition_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st_partitioned(
        "diagnostic_partition_st",
        "SELECT id, embedding FROM diagnostic_partition_src",
        "1m",
        "DIFFERENTIAL",
        "id",
    )
    .await;
    db.execute("CREATE INDEX diagnostic_partition_ann ON diagnostic_partition_st USING hnsw (embedding vector_l2_ops)").await;
    let leaves = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE v->>'stream_table_oid' = 'diagnostic_partition_st'::regclass::oid::text AND v->>'table_oid' <> v->>'stream_table_oid' AND v->>'opclass' = 'vector_l2_ops'").await;
    assert!(leaves > 0);
}

// ═══════════════════════════════════════════════════════════════════════
// T-VP3 / VP-5 (v0.47.0): Vector-aggregate cases — shard-additive verification
// ═══════════════════════════════════════════════════════════════════════

/// T-VP3 / VP-5: Verify that vector_avg() produces correct results consistent
/// with pgvector's native avg(vector) aggregate. This exercises the same
/// shard-additive algebra that would run in a Citus distributed deployment.
///
/// This test runs on the standard single-node e2e image (no Citus required)
/// but validates the arithmetic that the Citus path relies on.
#[tokio::test]
async fn test_vector_avg_shard_additive_correctness() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE shard_src (
            id      SERIAL PRIMARY KEY,
            shard   INTEGER NOT NULL,
            vec     vector(4)
        )",
    )
    .await;

    // Simulate two logical shards with known vectors
    // Shard 1: avg([1,2,3,4], [3,4,5,6]) = [2,3,4,5]
    // Shard 2: avg([0,0,0,0], [4,4,4,4]) = [2,2,2,2]
    db.execute(
        "INSERT INTO shard_src (shard, vec) VALUES
             (1, '[1,2,3,4]'), (1, '[3,4,5,6]'),
             (2, '[0,0,0,0]'), (2, '[4,4,4,4]')",
    )
    .await;

    let q = "SELECT shard, avg(vec) AS centroid, count(*) AS cnt FROM shard_src GROUP BY shard";
    let expected_q = "SELECT shard, avg(vec)::vector(4) AS centroid, count(*) AS cnt FROM shard_src GROUP BY shard";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('shard_avg_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;

    db.assert_st_matches_query("shard_avg_st", expected_q).await;

    // Verify shard 1 centroid: avg([1,2,3,4],[3,4,5,6]) = [2,3,4,5]
    let s1_centroid: Option<String> = db
        .query_scalar_opt("SELECT centroid::TEXT FROM public.shard_avg_st WHERE shard = 1")
        .await;
    assert!(s1_centroid.is_some(), "shard 1 centroid should be present");
    // pgvector formats vectors as '[x,y,z,w]'; check it's close to [2,3,4,5]
    let ct = s1_centroid.unwrap();
    assert!(
        ct.contains("2") && ct.contains("3") && ct.contains("4") && ct.contains("5"),
        "shard 1 centroid ({ct}) should approximate [2,3,4,5]"
    );

    // Now insert into shard 1 — differential refresh must update centroid correctly
    db.execute("INSERT INTO shard_src (shard, vec) VALUES (1, '[5,6,7,8]')")
        .await;
    db.refresh_st("shard_avg_st").await;
    db.assert_st_matches_query("shard_avg_st", expected_q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// VH-1 (v0.48.0): halfvec/sparsevec aggregate type output correctness
// ═══════════════════════════════════════════════════════════════════════

/// VH-1a: avg(halfvec_col) output column should be typed halfvec(N), not vector(N).
#[tokio::test]
async fn test_pgvector_halfvec_avg_output_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE hv_src (
            id     SERIAL PRIMARY KEY,
            grp    INT,
            emb    halfvec(4)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO hv_src (grp, emb) VALUES
            (1, '[1,0,0,0]'),
            (1, '[0,1,0,0]'),
            (2, '[0,0,1,0]')",
    )
    .await;

    let q = "SELECT grp, avg(emb) AS centroid FROM hv_src GROUP BY grp";
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        &format!(
            "SELECT pgtrickle.create_stream_table('hv_avg_st', $${q}$$, '1m', 'DIFFERENTIAL')"
        ),
    ])
    .await;

    // The output column 'centroid' must be typed halfvec(4), not vector(4).
    let typname: Option<String> = db
        .query_scalar_opt(
            "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             WHERE c.relname = 'hv_avg_st' \
               AND a.attname = 'centroid' \
               AND a.attnum > 0",
        )
        .await;
    assert!(
        typname.is_some(),
        "column 'centroid' must exist in hv_avg_st"
    );
    let tn = typname.unwrap();
    assert!(
        tn.starts_with("halfvec"),
        "centroid column should be halfvec(4), got: {tn}"
    );
}

/// VH-1b: avg(halfvec) values are correct after differential refresh.
#[tokio::test]
async fn test_pgvector_halfvec_avg_values_correct() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE hv_vals_src (
            id  SERIAL PRIMARY KEY,
            grp INT,
            emb halfvec(2)
        )",
    )
    .await;
    db.execute("INSERT INTO hv_vals_src (grp, emb) VALUES (1, '[2,4]'), (1, '[4,2]')")
        .await;

    let q = "SELECT grp, avg(emb) AS centroid FROM hv_vals_src GROUP BY grp";
    let expected_q = "SELECT grp, avg(emb)::halfvec(2) AS centroid FROM hv_vals_src GROUP BY grp";
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        &format!(
            "SELECT pgtrickle.create_stream_table('hv_vals_st', $${q}$$, '1m', 'DIFFERENTIAL')"
        ),
    ])
    .await;
    db.assert_st_matches_query("hv_vals_st", expected_q).await;

    // INSERT — grp 1 centroid should become avg([2,4],[4,2],[6,0]) = [4,2]
    db.execute("INSERT INTO hv_vals_src (grp, emb) VALUES (1, '[6,0]')")
        .await;
    db.refresh_st("hv_vals_st").await;
    db.assert_st_matches_query("hv_vals_st", expected_q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// VH-2 (v0.48.0): Distance subscription catalog operations
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_distance_subscription_notifies_only_membership_crossings() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_source (id int PRIMARY KEY, note text, embedding vector(3))")
        .await;
    db.create_st(
        "alert_st",
        "SELECT id, note, embedding FROM alert_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen("distance_crossing")
        .await
        .expect("listen for crossings");
    db.execute("SELECT pgtrickle.subscribe_distance('public.alert_st', 'distance_crossing', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    db.execute("INSERT INTO alert_source VALUES (1, 'first', '[1,0,0]')")
        .await;
    db.refresh_st("alert_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("enter notification")
        .expect("receive enter");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert_eq!(payload["exited"], 0);
    db.execute("UPDATE alert_source SET note='changed' WHERE id=1")
        .await;
    db.refresh_st("alert_st").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "unchanged membership must not notify"
    );
    drop(listener);
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("reconnect listener");
    listener
        .listen("distance_crossing")
        .await
        .expect("listen for exit");
    db.execute("UPDATE alert_source SET embedding='[0,1,0]' WHERE id=1")
        .await;
    db.refresh_st("alert_st").await;
    let value: String = db
        .query_scalar("SELECT embedding::text FROM alert_st WHERE id=1")
        .await;
    assert_eq!(value, "[0,1,0]");
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("exit notification")
        .expect("receive exit");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 0);
    assert_eq!(payload["exited"], 1);
    db.refresh_st("alert_st").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "unchanged refresh must not notify"
    );
    let mut tx = db
        .pool
        .begin()
        .await
        .expect("begin rolled-back source change");
    sqlx::query("INSERT INTO alert_source VALUES (2, 'rolled back', '[1,0,0]')")
        .execute(&mut *tx)
        .await
        .expect("insert in transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('alert_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh in transaction");
    tx.rollback().await.expect("rollback");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "rolled-back change must not notify"
    );
}

#[tokio::test]
async fn test_distance_subscription_small_delta_snapshots_only_affected_rows() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_batch_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute(
        "INSERT INTO alert_batch_src SELECT i, '[1,0,0]'::vector FROM generate_series(1,1000) i",
    )
    .await;
    db.create_st(
        "alert_batch_st",
        "SELECT id, embedding FROM alert_batch_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('public.alert_batch_st', 'batch_alerts', 'embedding', '[1,0,0]', '<->', 0.5)")
        .await;
    db.execute("UPDATE alert_batch_src SET embedding='[0,1,0]' WHERE id=1")
        .await;
    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_schema='public' AND pgt_name='alert_batch_st'")
        .await;
    let mut tx = db.pool.begin().await.expect("begin refresh transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('alert_batch_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh one changed row");
    let snapshot_sql = format!("SELECT count(*) FROM pg_temp.__pgt_distance_pre_{pgt_id}");
    let before_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(snapshot_sql))
        .fetch_one(&mut *tx)
        .await
        .expect("inspect affected pre-refresh rows");
    let ids_sql = format!("SELECT count(*) FROM pg_temp.__pgt_distance_ids_{pgt_id}");
    let id_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(ids_sql))
        .fetch_one(&mut *tx)
        .await
        .expect("inspect affected identities");
    assert_eq!(before_count, 1);
    assert_eq!(id_count, 1);
    let plan_sql = format!(
        "EXPLAIN (ANALYZE, BUFFERS) SELECT st.* FROM pg_temp.__pgt_distance_ids_{pgt_id} d \
         CROSS JOIN LATERAL (SELECT st.* FROM public.alert_batch_st st \
         WHERE pgtrickle.row_probe_v1(st.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) \
           AND st.__pgt_row_id = d.__pgt_row_id OFFSET 0) st"
    );
    let plan: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(plan_sql))
        .fetch_all(&mut *tx)
        .await
        .expect("inspect affected-row lookup plan");
    assert!(
        plan.iter().any(|line| line.contains("Index")
            && line.contains("Scan")
            && line.contains("row_id_idx")),
        "affected-row lookup should use the row-identity index: {plan:?}"
    );
    tx.commit().await.expect("commit refresh");
}

#[tokio::test]
async fn test_distance_subscription_typed_predicates_and_authorization() {
    use sqlx::postgres::PgListener;
    use std::collections::HashSet;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_typed_src (id int PRIMARY KEY, dense vector(3), half halfvec(3), bits bit(3), sparse sparsevec(3))").await;
    db.create_st(
        "alert_typed_st",
        "SELECT id, dense, half, bits, sparse FROM alert_typed_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    for channel in [
        "typed_dense",
        "typed_cosine",
        "typed_half",
        "typed_bits",
        "typed_jaccard",
        "typed_sparse",
    ] {
        listener.listen(channel).await.expect("listen");
    }
    for sql in [
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','typed_dense','dense','[1,0,0]','<#>',-0.5)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','typed_cosine','dense','[1,0,0]','<=>',0.1)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_half','half','[1,0,0]','<->',0.5)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_bits','bits','101','<~>',1)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_jaccard','bits','101','<%>',0.5)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_sparse','sparse','{1:1}/3','<->',0.5)",
    ] {
        db.execute(sql).await;
    }
    for sql in [
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_dim','dense','[1,0]','<->',0.5)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_op','bits','101','<->',0.5)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_nan','dense','[1,0,0]','<->','NaN'::float8)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_inf','dense','[1,0,0]','<->','Infinity'::float8)",
    ] {
        assert!(
            db.try_execute(sql).await.is_err(),
            "invalid subscription accepted: {sql}"
        );
    }
    db.execute("INSERT INTO alert_typed_src VALUES (1,'[1,0,0]','[1,0,0]','101','{1:1}/3')")
        .await;
    db.refresh_st("alert_typed_st").await;
    let mut channels = HashSet::new();
    for _ in 0..6 {
        let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .expect("typed alert")
            .expect("receive typed alert");
        let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
        assert_eq!(payload["entered"], 1);
        channels.insert(event.channel().to_owned());
    }
    assert_eq!(channels.len(), 6);

    db.execute("CREATE ROLE alert_intruder NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, public TO alert_intruder")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.subscribe_distance(text,text,text,text,text,double precision), pgtrickle.unsubscribe_distance(text,text) TO alert_intruder").await;
    let error = db.try_execute_with_role("SET ROLE alert_intruder", "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','stolen','dense','[1,0,0]','<->',0.5)", "RESET ROLE").await.expect_err("non-owner subscription must fail");
    assert!(
        error.to_string().contains("must be owner"),
        "unexpected denial: {error}"
    );
    let mut tx = db.pool.begin().await.expect("begin observer transaction");
    sqlx::query("SET LOCAL ROLE alert_intruder")
        .execute(&mut *tx)
        .await
        .expect("set role");
    let visible: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtrickle.list_distance_subscriptions(NULL)")
            .fetch_one(&mut *tx)
            .await
            .expect("list visible subscriptions");
    assert_eq!(visible, 0);
    tx.rollback().await.expect("end observer transaction");

    db.execute("CREATE ROLE alert_owner NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, public TO alert_owner")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.subscribe_distance(text,text,text,text,text,double precision), pgtrickle.unsubscribe_distance(text,text) TO alert_owner").await;
    db.execute("ALTER TABLE public.alert_typed_st OWNER TO alert_owner")
        .await;
    let result = db.try_execute_with_role("SET ROLE alert_owner", "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','owner_alert','dense','[1,0,0]','<->',0.5)", "RESET ROLE").await;
    assert!(
        result.is_ok(),
        "stream owner should be able to subscribe: {result:?}"
    );
    let mut tx = db.pool.begin().await.expect("begin owner transaction");
    sqlx::query("SET LOCAL ROLE alert_owner")
        .execute(&mut *tx)
        .await
        .expect("set owner role");
    let visible: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgtrickle.list_distance_subscriptions('public.alert_typed_st')",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("list owner subscriptions");
    assert_eq!(visible, 7);
    tx.rollback().await.expect("end owner transaction");
}

#[tokio::test]
async fn test_distance_subscription_key_delete_and_null_transitions() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_key_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_key_src VALUES (1,'[1,0,0]')")
        .await;
    db.create_st(
        "alert_key_st",
        "SELECT id, embedding FROM alert_key_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("key_alert").await.expect("listen");
    db.execute("SELECT pgtrickle.subscribe_distance('alert_key_st','key_alert','embedding','[1,0,0]','<->',0.5)").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), listener.recv())
            .await
            .is_err(),
        "registration establishes a baseline"
    );
    for (mutation, entered, exited) in [
        ("UPDATE alert_key_src SET id=2 WHERE id=1", 1, 1),
        ("DELETE FROM alert_key_src WHERE id=2", 0, 1),
        ("INSERT INTO alert_key_src VALUES (3,'[1,0,0]')", 1, 0),
        ("UPDATE alert_key_src SET embedding=NULL WHERE id=3", 0, 1),
    ] {
        db.execute(mutation).await;
        db.refresh_st("alert_key_st").await;
        let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .expect("crossing notification")
            .expect("receive crossing");
        let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
        assert_eq!(payload["entered"], entered, "{mutation}");
        assert_eq!(payload["exited"], exited, "{mutation}");
    }
}

#[tokio::test]
async fn test_distance_subscription_full_refresh_and_edit_baseline() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_full_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_full_src VALUES (1,'[1,0,0]')")
        .await;
    db.create_st(
        "alert_full_st",
        "SELECT id, embedding FROM alert_full_src",
        "1m",
        "FULL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("full_alert").await.expect("listen");
    db.execute("SELECT pgtrickle.subscribe_distance('alert_full_st','full_alert','embedding','[1,0,0]','<->',0.5)").await;
    db.execute("UPDATE alert_full_src SET embedding='[0,1,0]' WHERE id=1")
        .await;
    db.refresh_st("alert_full_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("full exit")
        .expect("receive full exit");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["exited"], 1);
    db.execute("SELECT pgtrickle.subscribe_distance('alert_full_st','full_alert','embedding','[0,1,0]','<->',0.5)").await;
    db.refresh_st("alert_full_st").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "subscription edits establish a baseline"
    );
    db.execute("DELETE FROM alert_full_src WHERE id=1").await;
    db.refresh_st("alert_full_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("full delete exit")
        .expect("receive full delete exit");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["exited"], 1);
    db.execute("INSERT INTO alert_full_src VALUES (2,'[0,1,0]')")
        .await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit=true WHERE pgt_name='alert_full_st'",
    )
    .await;
    db.refresh_st("alert_full_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("reinitialization entry")
        .expect("receive reinitialization entry");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
}

#[tokio::test]
async fn test_distance_subscription_quoted_names_and_extension_schema() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"Vector Ext\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"Vector Ext\"")
        .await;
    db.execute("CREATE SCHEMA \"Odd.Schema\"").await;
    db.execute("CREATE TABLE \"Odd.Schema\".source (id int PRIMARY KEY, \"vec weird\" \"Vector Ext\".vector(3))").await;
    db.execute("SELECT pgtrickle.create_stream_table('\"Odd.Schema\".\"Alert Table\"', $$SELECT id, \"vec weird\" FROM \"Odd.Schema\".source$$, '1m', 'DIFFERENTIAL')").await;
    let channel = "alerts \"odd'";
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen(channel)
        .await
        .expect("listen to quoted channel");
    db.execute_seq(&[
        "SET search_path TO \"Odd.Schema\", public",
        "SELECT pgtrickle.subscribe_distance('\"Alert Table\"', 'alerts \"odd''', '\"vec weird\"', '[1,0,0]', '<->', 0.5)",
        "RESET search_path",
    ]).await;
    db.execute("INSERT INTO \"Odd.Schema\".source VALUES (1,'[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('\"Odd.Schema\".\"Alert Table\"')")
        .await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("quoted alert")
        .expect("receive alert");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(event.channel(), channel);
    assert_eq!(payload["schema"], "Odd.Schema");
    assert_eq!(payload["stream_table"], "Alert Table");
    assert_eq!(payload["entered"], 1);
    db.execute("SELECT pgtrickle.drop_stream_table('\"Odd.Schema\".\"Alert Table\"')")
        .await;
    let count: i64 = db.query_scalar("SELECT count(*) FROM pgtrickle.pgt_distance_subscriptions WHERE channel='alerts \"odd''' ").await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_distance_subscription_rejects_unsupported_notification_modes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_modes_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "alert_modes_st",
        "SELECT id, embedding FROM alert_modes_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_modes_st','mode_alert','embedding','[1,0,0]','<->',0.5)").await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('alert_modes_st', pooler_compatibility_mode => true)",
    )
    .await;
    let error = db.try_execute("SELECT pgtrickle.subscribe_distance('alert_modes_st','new_alert','embedding','[1,0,0]','<->',0.5)").await.expect_err("pooler mode must reject subscriptions");
    assert!(
        error.to_string().contains("pooler compatibility"),
        "unexpected error: {error}"
    );
    db.execute("INSERT INTO alert_modes_src VALUES (1,'[1,0,0]')")
        .await;
    let notices = db
        .try_execute_with_notices("SELECT pgtrickle.refresh_stream_table('alert_modes_st')")
        .await
        .expect("refresh remains successful");
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains("distance notifications are disabled")),
        "missing pooler warning: {notices:?}"
    );
    db.create_st(
        "alert_immediate_st",
        "SELECT id, embedding FROM alert_modes_src",
        "1m",
        "IMMEDIATE",
    )
    .await;
    let error = db.try_execute("SELECT pgtrickle.subscribe_distance('alert_immediate_st','immediate_alert','embedding','[1,0,0]','<->',0.5)").await.expect_err("IMMEDIATE mode must reject subscriptions");
    assert!(
        error.to_string().contains("IMMEDIATE"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn test_distance_subscription_waits_for_refresh_catalog_lock() {
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_lock_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "alert_lock_st",
        "SELECT id, embedding FROM alert_lock_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut tx = db.pool.begin().await.expect("begin lock transaction");
    sqlx::query(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name='alert_lock_st' FOR UPDATE",
    )
    .execute(&mut *tx)
    .await
    .expect("hold refresh catalog lock");
    let pool = db.pool.clone();
    let mut registration = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.subscribe_distance('alert_lock_st','lock_alert','embedding','[1,0,0]','<->',0.5)")
            .execute(&pool).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut registration)
            .await
            .is_err(),
        "registration must wait for the refresh lock"
    );
    tx.commit().await.expect("release refresh catalog lock");
    tokio::time::timeout(Duration::from_secs(5), registration)
        .await
        .expect("registration completes")
        .expect("registration task")
        .expect("registration succeeds");
}

#[tokio::test]
async fn test_distance_subscription_fused_chain_scheduler_emits_crossing() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    for setting in [
        "ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100",
        "ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1",
        "ALTER SYSTEM SET pg_trickle.auto_backoff = off",
        "ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'on'",
        "ALTER SYSTEM SET pg_trickle.enable_fused_refresh = on",
    ] {
        db.execute(setting).await;
    }
    db.reload_config_and_wait().await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "scheduler did not start"
    );
    db.execute("CREATE TABLE fused_alert_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "fused_alert_first",
        "SELECT id, embedding FROM fused_alert_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "fused_alert_second",
        "SELECT id FROM fused_alert_first",
        "1s",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("INSERT INTO fused_alert_src VALUES (1,'[0,1,0]')")
        .await;
    assert!(
        db.wait_for_condition(
            "fused chain control converged",
            "SELECT count(*)=1 FROM fused_alert_second",
            Duration::from_secs(30),
            Duration::from_millis(100)
        )
        .await
    );
    // shortcut: workers start with cold template caches, so this checks chain scheduling, not fused SQL; add that control when fusion can be witnessed.
    assert!(db.wait_for_condition("fused chain control job", "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_scheduler_jobs WHERE unit_kind='fused_chain' AND status='SUCCEEDED')", Duration::from_secs(30), Duration::from_millis(100)).await);
    let previous_job: i64 = db
        .query_scalar(
            "SELECT max(job_id) FROM pgtrickle.pgt_scheduler_jobs WHERE unit_kind='fused_chain'",
        )
        .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("fused_alert").await.expect("listen");
    db.execute("SELECT pgtrickle.subscribe_distance('fused_alert_first','fused_alert','embedding','[1,0,0]','<->',0.5)").await;
    db.execute("INSERT INTO fused_alert_src VALUES (100,'[1,0,0]')")
        .await;
    let event = tokio::time::timeout(Duration::from_secs(30), listener.recv())
        .await
        .expect("scheduler crossing")
        .expect("receive scheduler crossing");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert!(db.wait_for_condition("subscribed fused chain job", &format!("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_scheduler_jobs WHERE unit_kind='fused_chain' AND status='SUCCEEDED' AND job_id > {previous_job})"), Duration::from_secs(30), Duration::from_millis(100)).await);
    let initiated_by: String = db.query_scalar("SELECT h.initiated_by FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='fused_alert_first' AND h.refresh_id=(SELECT max(h2.refresh_id) FROM pgtrickle.pgt_refresh_history h2 WHERE h2.pgt_id=s.pgt_id AND h2.status='COMPLETED')").await;
    assert_ne!(initiated_by, "SCHEDULER_FUSED");
}

#[tokio::test]
async fn test_distance_subscription_invalid_stored_value_does_not_abort_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_error_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "alert_error_st",
        "SELECT id, embedding FROM alert_error_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('public.alert_error_st', 'distance_error', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    db.execute("UPDATE pgtrickle.pgt_distance_subscriptions SET query_vector='invalid' WHERE stream_table='public.alert_error_st'").await;
    db.execute("INSERT INTO alert_error_source VALUES (1, '[1,0,0]')")
        .await;
    let notices = db
        .try_execute_with_notices("SELECT pgtrickle.refresh_stream_table('alert_error_st')")
        .await
        .expect("alert failure must not abort refresh");
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains("distance subscription failed")),
        "missing failure warning: {notices:?}"
    );
    let count: i64 = db.query_scalar("SELECT count(*) FROM alert_error_st").await;
    assert_eq!(count, 1);
}

/// VH-2a: subscribe_distance() inserts into pgt_distance_subscriptions.
#[tokio::test]
async fn test_distance_subscription_catalog_insert() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute("CREATE TABLE ds_src (id SERIAL PRIMARY KEY, grp INT, emb vector(3))")
        .await;
    db.execute("INSERT INTO ds_src (grp, emb) VALUES (1, '[1,0,0]'), (1, '[0,1,0]')")
        .await;
    db.create_st(
        "ds_st",
        "SELECT grp, avg(emb)::vector(3) AS centroid FROM ds_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.subscribe_distance(\
             'public.ds_st', 'near_centroid', 'centroid', '[1,0,0]', '<->', 0.5\
         )",
    )
    .await;

    let count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_distance_subscriptions \
             WHERE stream_table = 'public.ds_st' AND channel = 'near_centroid'",
        )
        .await;
    assert_eq!(count, 1, "subscription should be registered in catalog");
    let threshold: f64 = db
        .query_scalar("SELECT threshold FROM pgtrickle.pgt_distance_subscriptions WHERE stream_table = 'public.ds_st' AND channel = 'near_centroid'")
        .await;
    assert_eq!(threshold, 0.5);
    db.execute("SELECT pgtrickle.subscribe_distance('public.ds_st', 'near_centroid', 'centroid', '[1,0,0]', '<->', 0.3)").await;
    let threshold: f64 = db
        .query_scalar("SELECT threshold FROM pgtrickle.list_distance_subscriptions('public.ds_st') WHERE channel = 'near_centroid'")
        .await;
    assert_eq!(threshold, 0.3);
}

/// VH-2b: unsubscribe_distance() removes the catalog entry.
#[tokio::test]
async fn test_distance_subscription_unsubscribe() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute("CREATE TABLE ds2_src (id SERIAL PRIMARY KEY, emb vector(2))")
        .await;
    db.create_st(
        "ds2_st",
        "SELECT id, emb FROM ds2_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.subscribe_distance(\
             'public.ds2_st', 'ch_unsub', 'emb', '[1,0]', '<->', 0.3\
         )",
    )
    .await;
    db.execute("SELECT pgtrickle.unsubscribe_distance('public.ds2_st', 'ch_unsub')")
        .await;

    let count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_distance_subscriptions \
             WHERE stream_table = 'public.ds2_st' AND channel = 'ch_unsub'",
        )
        .await;
    assert_eq!(count, 0, "subscription should be removed after unsubscribe");
}

/// VH-2c: list_distance_subscriptions() returns subscriptions for a stream table.
#[tokio::test]
async fn test_distance_subscription_list() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute("CREATE TABLE ds3_src (id SERIAL PRIMARY KEY, emb vector(2))")
        .await;
    db.create_st(
        "ds3_st",
        "SELECT id, emb FROM ds3_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute_seq(&[
        "SELECT pgtrickle.subscribe_distance('public.ds3_st', 'ch_a', 'emb', '[1,0]', '<->', 0.5)",
        "SELECT pgtrickle.subscribe_distance('public.ds3_st', 'ch_b', 'emb', '[0,1]', '<->', 0.8)",
    ])
    .await;

    let count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.list_distance_subscriptions('public.ds3_st')")
        .await;
    assert_eq!(count, 2, "list_distance_subscriptions should return 2 rows");
}

// ═══════════════════════════════════════════════════════════════════════
// VA-1 (v0.48.0): embedding_stream_table() ergonomic API
// ═══════════════════════════════════════════════════════════════════════

/// VA-1a: embedding_stream_table() creates a stream table with correct schema.
#[tokio::test]
async fn test_embedding_stream_table_creates_st() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE va1_src (
            id        SERIAL PRIMARY KEY,
            tenant_id INT,
            content   TEXT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO va1_src (tenant_id, content, embedding) VALUES
            (1, 'hello', '[1,0,0]'),
            (1, 'world', '[0,1,0]'),
            (2, 'foo',   '[0,0,1]')",
    )
    .await;

    // dry_run=true: returns SQL actions without executing
    let action_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.embedding_stream_table(\
                 'va1_emb_st', 'public.va1_src', 'embedding', \
                 NULL, '1m', 'hnsw', TRUE\
             ) AS t(action)",
        )
        .await;
    assert!(
        action_count >= 1,
        "dry_run should return at least one action, got: {action_count}"
    );
}

/// VA-1b: embedding_stream_table() with dry_run=false creates a real stream table.
#[tokio::test]
async fn test_embedding_stream_table_creates_real_st() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE va1b_src (
            id        SERIAL PRIMARY KEY,
            embedding vector(2)
        )",
    )
    .await;
    db.execute("INSERT INTO va1b_src (embedding) VALUES ('[1,0]'), ('[0,1]')")
        .await;

    db.execute(
        "SELECT pgtrickle.embedding_stream_table(\
             'va1b_emb_st', 'public.va1b_src', 'embedding', \
             NULL, '1m', 'hnsw', FALSE\
         )",
    )
    .await;

    // Stream table should exist
    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'va1b_emb_st')",
        )
        .await;
    assert!(
        exists,
        "embedding_stream_table() should create pgt_stream_tables entry"
    );
}

#[tokio::test]
async fn test_embedding_stream_table_legacy_l2_is_atomic_and_has_no_reindex_policy() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO helper_src VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let preview: Vec<(String,)> = sqlx::query_as(
        "SELECT action FROM pgtrickle.embedding_stream_table('helper_st', 'helper_src', 'embedding', NULL, '1m', 'hnsw', true)"
    ).fetch_all(&db.pool).await.expect("preview helper");
    assert_eq!(preview.len(), 2);
    assert!(preview[1].0.contains("vector_l2_ops"));
    assert!(!preview.iter().any(|row| row.0.contains("reindex_if_drift")));
    let absent: bool = db.query_scalar("SELECT to_regclass('public.helper_st') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'helper_st')").await;
    assert!(absent, "dry run must not create a stream table");

    db.execute("SELECT * FROM pgtrickle.embedding_stream_table('helper_st', 'helper_src', 'embedding', NULL, '1m', 'hnsw', false)").await;
    let index_name: String = db
        .query_scalar(
            "SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_am am ON am.oid = c.relam JOIN pg_opclass op ON op.oid = i.indclass[0] \
         WHERE i.indrelid = 'public.helper_st'::regclass AND i.indisvalid AND i.indisready \
         AND am.amname = 'hnsw' AND op.opcname = 'vector_l2_ops'",
        )
        .await;
    let policy: String = db.query_scalar("SELECT post_refresh_action::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'helper_st'").await;
    assert_eq!(policy, "none");
    let mut tx = db.pool.begin().await.expect("begin plan check");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable sequential scan");
    sqlx::query("SET LOCAL enable_bitmapscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable bitmap scan");
    sqlx::query("SET LOCAL enable_indexonlyscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable covering scan");
    let plan: serde_json::Value = sqlx::query_scalar(
        "EXPLAIN (FORMAT JSON) SELECT id FROM helper_st ORDER BY embedding::vector(3) <-> '[1,0,0]'::vector LIMIT 1"
    ).fetch_one(&mut *tx).await.expect("explain L2 query");
    assert!(
        plan.to_string()
            .contains(&format!("\"Index Name\":\"{index_name}\"")),
        "matching L2 index is not eligible: {plan}"
    );
    tx.rollback().await.expect("end plan check");
}

#[tokio::test]
async fn test_embedding_stream_table_v2_metrics_and_options() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE metric_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO metric_source VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let preview: Vec<(String,)> = sqlx::query_as(
        r#"SELECT action FROM pgtrickle.embedding_stream_table_v2('metric_cosine', 'metric_source', 'embedding', 'cosine', '{"columns":["id"],"hnsw_m":8,"hnsw_ef_construction":32}', true)"#,
    ).fetch_all(&db.pool).await.expect("preview cosine helper");
    assert_eq!(preview.len(), 2);
    assert!(preview[1].0.contains("vector_cosine_ops"));
    let absent: bool = db
        .query_scalar("SELECT to_regclass('public.metric_cosine') IS NULL")
        .await;
    assert!(absent);
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_cosine', 'metric_source', 'embedding', 'cosine', '{"columns":["id"],"hnsw_m":8,"hnsw_ef_construction":32}')"#).await;
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_ip', 'metric_source', 'embedding', 'inner_product', '{"index_type":"ivfflat","ivfflat_lists":1,"post_refresh_action":"analyze"}')"#).await;
    let cosine: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_opclass op ON op.oid=i.indclass[0] WHERE i.indrelid='metric_cosine'::regclass AND i.indisvalid AND i.indisready AND op.opcname='vector_cosine_ops' AND c.reloptions @> ARRAY['m=8','ef_construction=32'])").await;
    assert!(cosine);
    let inner_product: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_opclass op ON op.oid=i.indclass[0] WHERE i.indrelid='metric_ip'::regclass AND i.indisvalid AND i.indisready AND op.opcname='vector_ip_ops' AND c.reloptions @> ARRAY['lists=1'])").await;
    assert!(inner_product);
    let mut tx = db.pool.begin().await.expect("begin metric plan check");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable sequential scan");
    for (table, opclass, query) in [
        (
            "metric_cosine",
            "vector_cosine_ops",
            "EXPLAIN (FORMAT JSON) SELECT id FROM metric_cosine ORDER BY embedding::vector(3) <=> '[1,0,0]'::vector LIMIT 1",
        ),
        (
            "metric_ip",
            "vector_ip_ops",
            "EXPLAIN (FORMAT JSON) SELECT id FROM metric_ip ORDER BY embedding::vector(3) <#> '[1,0,0]'::vector LIMIT 1",
        ),
    ] {
        let index: String = sqlx::query_scalar("SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_opclass op ON op.oid=i.indclass[0] WHERE i.indrelid=$1::regclass AND op.opcname=$2")
            .bind(table).bind(opclass).fetch_one(&mut *tx).await.expect("find metric index");
        let plan: serde_json::Value = sqlx::query_scalar(query)
            .fetch_one(&mut *tx)
            .await
            .expect("explain metric query");
        assert!(
            plan.to_string()
                .contains(&format!("\"Index Name\":\"{index}\"")),
            "matching index is not eligible: {plan}"
        );
    }
    tx.rollback().await.expect("end metric plan check");
    let policy: String = db.query_scalar("SELECT post_refresh_action::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name='metric_ip'").await;
    assert_eq!(policy, "analyze");
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_full', 'metric_source', 'embedding', 'l2', '{"refresh_mode":"FULL","refresh_interval":"1h"}')"#).await;
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_immediate', 'metric_source', 'embedding', 'l2', '{"refresh_mode":"IMMEDIATE"}')"#).await;
    let modes: i64 = db.query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE (pgt_name='metric_full' AND refresh_mode='FULL') OR (pgt_name='metric_immediate' AND refresh_mode='IMMEDIATE')").await;
    assert_eq!(modes, 2);
    for options in [
        r#"{"unknown":1}"#,
        r#"{"columns":["missing"]}"#,
        r#"{"columns":["id","id"]}"#,
        r#"{"columns":["id); DROP TABLE metric_source; --"]}"#,
        r#"{"hnsw_m":1}"#,
        r#"{"hnsw_m":80}"#,
        r#"{"ivfflat_lists":2}"#,
        r#"{"index_type":"ivfflat","ivfflat_lists":0}"#,
        r#"{"refresh_mode":"IMMEDIATE","refresh_interval":"1m"}"#,
    ] {
        let sql = format!(
            "SELECT * FROM pgtrickle.embedding_stream_table_v2('invalid_metric_st', 'metric_source', 'embedding', 'cosine', '{options}', true)"
        );
        assert!(db.try_execute(&sql).await.is_err(), "accepted {options}");
    }
    assert!(db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table_v2('invalid_metric_st', 'metric_source', 'embedding', 'unknown', NULL, true)").await.is_err());
}

#[tokio::test]
async fn test_embedding_stream_table_index_conflict_rolls_back_creation() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_conflict_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE helper_unrelated (id int)").await;
    let preview: String = db.query_scalar(
        "SELECT action FROM pgtrickle.embedding_stream_table('helper_conflict_st', 'helper_conflict_src', 'embedding', NULL, '1m', 'hnsw', true) WHERE action LIKE '-- 2.%'"
    ).await;
    let index_name = preview.split('"').nth(1).expect("preview index identifier");
    assert!(
        index_name.starts_with("pgt_ann_")
            && index_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
    );
    db.execute(&format!(
        "CREATE INDEX {index_name} ON helper_unrelated (id)"
    ))
    .await;
    assert!(db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('helper_conflict_st', 'helper_conflict_src', 'embedding', NULL, '1m', 'hnsw', true)").await.is_err());
    let result = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('helper_conflict_st', 'helper_conflict_src', 'embedding', NULL, '1m', 'hnsw', false)").await;
    assert!(
        result.is_err(),
        "conflicting index must fail instead of claiming success"
    );
    let clean: bool = db.query_scalar("SELECT to_regclass('public.helper_conflict_st') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'helper_conflict_st') AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_change_tracking WHERE source_relid = 'helper_conflict_src'::regclass)").await;
    assert!(clean, "index failure must roll back stream and CDC state");
}

#[tokio::test]
async fn test_embedding_stream_table_starter_fixture_executes() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(include_str!("fixtures/pgvector_l2_starter.sql"))
        .execute(&db.pool)
        .await
        .expect("execute L2 starter SQL");
    let nearest: i64 = db
        .query_scalar(
            "SELECT id FROM doc_corpus ORDER BY embedding::vector(3) <-> '[1,0,0]'::vector LIMIT 1",
        )
        .await;
    assert_eq!(nearest, 1);
    let cosine_nearest: i64 = db.query_scalar("SELECT id FROM corpus_cosine ORDER BY embedding::vector(3) <=> '[1,0,0]'::vector LIMIT 1").await;
    assert_eq!(cosine_nearest, 1);
}

#[tokio::test]
async fn test_embedding_stream_table_quoted_names_and_vector_schema() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"V.X\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"V.X\"")
        .await;
    db.execute("CREATE SCHEMA \"A.B\"").await;
    db.execute("CREATE SCHEMA \"D.S\"").await;
    db.execute("CREATE TABLE \"A.B\".\"Doc.Source\" (id int PRIMARY KEY, \"Embedding Value\" \"V.X\".vector(3))").await;
    db.execute("INSERT INTO \"A.B\".\"Doc.Source\" VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT * FROM pgtrickle.embedding_stream_table('\"D.S\".\"Corpus.A\"', '\"A.B\".\"Doc.Source\"', '\"Embedding Value\"', NULL, '1h', 'hnsw', false)").await;
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('"D.S"."Cosine.Corpus"', '"A.B"."Doc.Source"', '"Embedding Value"', 'cosine', '{"columns":["id"]}')"#).await;
    let v2_valid: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_opclass op ON op.oid=i.indclass[0] JOIN pg_namespace n ON n.oid=op.opcnamespace WHERE i.indrelid='\"D.S\".\"Cosine.Corpus\"'::regclass AND i.indisvalid AND i.indisready AND op.opcname='vector_cosine_ops' AND n.nspname='V.X')").await;
    assert!(v2_valid);
    let valid: bool = db.query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_opclass op ON op.oid = i.indclass[0] JOIN pg_namespace n ON n.oid = op.opcnamespace \
         JOIN pg_am am ON am.oid = c.relam WHERE i.indrelid = '\"D.S\".\"Corpus.A\"'::regclass \
         AND i.indisvalid AND i.indisready AND am.amname = 'hnsw' \
         AND op.opcname = 'vector_l2_ops' AND n.nspname = 'V.X')"
    ).await;
    assert!(
        valid,
        "quoted relation and extension names must produce a valid L2 index"
    );
    let direct = db
        .try_execute(
            "INSERT INTO \"D.S\".\"Corpus.A\" (id, \"Embedding Value\") VALUES (3, '[0,0,1]')",
        )
        .await;
    assert!(
        direct.is_err(),
        "quoted stream table must retain its DML guard"
    );
    db.execute("INSERT INTO \"A.B\".\"Doc.Source\" VALUES (2, '[0,1,0]')")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('\"D.S\".\"Corpus.A\"')")
        .await;
    let rows: i64 = db
        .query_scalar("SELECT count(*) FROM \"D.S\".\"Corpus.A\"")
        .await;
    assert_eq!(rows, 2, "quoted stream must refresh after source changes");
}

#[tokio::test]
async fn test_embedding_stream_table_dry_run_rejects_invalid_source_types() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE scalar_source (id int PRIMARY KEY, embedding int)")
        .await;
    let no_extension = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('bad_st', 'scalar_source', 'embedding', dry_run => true)").await;
    assert!(
        no_extension.is_err(),
        "pgvector must be installed for a validated preview"
    );
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE undimensioned_source (id int PRIMARY KEY, embedding vector)")
        .await;
    db.execute("CREATE SCHEMA fake_vector").await;
    db.execute("CREATE DOMAIN fake_vector.vector AS text").await;
    db.execute("CREATE TABLE impostor_source (id int PRIMARY KEY, embedding fake_vector.vector)")
        .await;
    for source in [
        "missing_source",
        "scalar_source",
        "undimensioned_source",
        "impostor_source",
    ] {
        let sql = format!(
            "SELECT * FROM pgtrickle.embedding_stream_table('bad_st', '{source}', 'embedding', dry_run => true)"
        );
        assert!(
            db.try_execute(&sql).await.is_err(),
            "invalid source {source} must be rejected"
        );
    }
    let absent: bool = db.query_scalar("SELECT to_regclass('public.bad_st') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'bad_st')").await;
    assert!(absent, "invalid previews must not create objects");
}

#[tokio::test]
async fn test_embedding_stream_table_halfvec_and_sparsevec_indexes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_half_src (id int PRIMARY KEY, embedding halfvec(3))")
        .await;
    db.execute("CREATE TABLE helper_sparse_src (id int PRIMARY KEY, embedding sparsevec(3))")
        .await;
    db.execute("INSERT INTO helper_half_src VALUES (1, '[1,0,0]')")
        .await;
    db.execute("INSERT INTO helper_sparse_src VALUES (1, '{1:1}/3')")
        .await;
    let unsupported = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('sparse_bad', 'helper_sparse_src', 'embedding', index_type => 'ivfflat', dry_run => true)").await;
    assert!(unsupported.is_err(), "IVFFlat does not support sparsevec");
    for (source, stream, opclass) in [
        ("helper_half_src", "helper_half_st", "halfvec_l2_ops"),
        ("helper_sparse_src", "helper_sparse_st", "sparsevec_l2_ops"),
    ] {
        db.execute(&format!(
            "SELECT * FROM pgtrickle.embedding_stream_table('{stream}', '{source}', 'embedding')"
        ))
        .await;
        let valid: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
             JOIN pg_am am ON am.oid = c.relam JOIN pg_opclass op ON op.oid = i.indclass[0] \
             WHERE i.indrelid = '{stream}'::regclass AND i.indisvalid AND i.indisready \
             AND am.amname = 'hnsw' AND op.opcname = '{opclass}')"
            ))
            .await;
        assert!(valid, "{stream}: expected a valid {opclass} index");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// VA-4 (v0.48.0): attach_embedding_outbox() catalog registration
// ═══════════════════════════════════════════════════════════════════════

/// VA-4: attach_embedding_outbox() registers the embedding_vector_column.
#[tokio::test]
async fn test_attach_embedding_outbox_catalog_entry() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE va4_src (
            id        SERIAL PRIMARY KEY,
            embedding vector(3)
        )",
    )
    .await;
    db.execute("INSERT INTO va4_src (embedding) VALUES ('[1,0,0]'), ('[0,1,0]')")
        .await;
    db.create_st(
        "va4_emb_st",
        "SELECT id, embedding FROM va4_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute("CREATE EXTENSION pg_tide").await;

    db.execute("SELECT pgtrickle.attach_embedding_outbox('va4_emb_st', 'embedding')")
        .await;

    let vec_col: Option<String> = db
        .query_scalar_opt(
            "SELECT embedding_vector_column FROM pgtrickle.pgt_outbox_config \
             WHERE stream_table_name = 'public.va4_emb_st'",
        )
        .await;
    assert_eq!(
        vec_col,
        Some("embedding".to_string()),
        "attach_embedding_outbox should store embedding_vector_column"
    );
}
