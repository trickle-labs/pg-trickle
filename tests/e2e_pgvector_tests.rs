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
async fn test_wide_vectors_refresh_without_oversized_covering_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE bounded_index_source (id int PRIMARY KEY, value int)")
        .await;
    db.create_st(
        "bounded_index_stream",
        "SELECT id, value FROM bounded_index_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let covered: bool = db.query_scalar("SELECT indnatts > indnkeyatts FROM pg_index WHERE indexrelid = 'bounded_index_stream_row_id_idx'::regclass").await;
    assert!(covered, "small fixed-width outputs should remain covered");
    for dimensions in [768, 1536] {
        let source = format!("wide_vectors_{dimensions}");
        let stream = format!("wide_vector_st_{dimensions}");
        db.execute(&format!(
            "CREATE TABLE {source} (id int PRIMARY KEY, embedding vector({dimensions}))"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} SELECT 1, ARRAY(SELECT sin(n::float8)::real \
            FROM generate_series(1, {dimensions}) n)::vector({dimensions})"
        ))
        .await;
        db.create_st(
            &stream,
            &format!("SELECT id, embedding FROM {source}"),
            "1h",
            "DIFFERENTIAL",
        )
        .await;
        let no_includes: bool = db
            .query_scalar(&format!(
                "SELECT indnatts = indnkeyatts FROM pg_index \
            WHERE indexrelid = '{stream}_row_id_idx'::regclass"
            ))
            .await;
        assert!(
            no_includes,
            "unbounded vectors must not enter the B-tree index"
        );
        db.execute(&format!(
            "CREATE INDEX {stream}_ann ON {stream} USING hnsw (embedding vector_cosine_ops)"
        ))
        .await;
        db.execute(&format!(
            "UPDATE {source} SET embedding = ARRAY(SELECT cos(n::float8)::real \
            FROM generate_series(1, {dimensions}) n)::vector({dimensions})"
        ))
        .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy = 'differential'",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{stream}')"),
        )
        .await
        .expect("refresh wide vector");
        db.assert_st_matches_query(&stream, &format!("SELECT id, embedding FROM {source}"))
            .await;
    }
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
