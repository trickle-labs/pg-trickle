//! Revision-safe embedding generation with a deterministic fake provider.

mod e2e;

use e2e::E2eDb;

type Claim = (String, String, String);

async fn enqueue(db: &E2eDb, model: &str) {
    sqlx::query("SELECT embedding_demo.enqueue(1, $1)")
        .bind(model)
        .execute(&db.pool)
        .await
        .expect("enqueue generation");
}

async fn claim(db: &E2eDb, model: &str) -> Claim {
    sqlx::query_as("SELECT claimed_job::text, claimed_token::text, source_body FROM embedding_demo.claim($1, interval '1 minute')")
        .bind(model).fetch_one(&db.pool).await.expect("claim generation")
}

fn fake_embedding(body: &str, model: &str) -> &'static str {
    match (body, model) {
        (_, "model-v2") => "[0,0,1]",
        ("B", _) => "[0,1,0]",
        _ => "[1,0,0]",
    }
}

async fn complete(db: &E2eDb, job: &Claim, model: &str) -> bool {
    sqlx::query_scalar(
        "SELECT embedding_demo.complete($1::text::uuid, $2::text::uuid, $3::text::vector)",
    )
    .bind(&job.0)
    .bind(&job.1)
    .bind(fake_embedding(&job.2, model))
    .fetch_one(&db.pool)
    .await
    .expect("complete generation")
}

async fn refresh_differential(db: &E2eDb, table: &str) {
    let previous: i64 = db.query_scalar(&format!(
        "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_schema = 'embedding_demo' AND s.pgt_name = '{table}'"
    )).await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ],
        &format!("SELECT pgtrickle.refresh_stream_table('embedding_demo.{table}')"),
    )
    .await
    .expect("refresh revision-safe corpus");
    let differential: bool = db.query_scalar(&format!(
        "SELECT h.refresh_id > {previous} AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
         FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
         WHERE s.pgt_schema = 'embedding_demo' AND s.pgt_name = '{table}' ORDER BY h.refresh_id DESC LIMIT 1"
    )).await;
    assert!(differential, "{table} did not refresh differentially");
}

#[tokio::test]
async fn test_revision_safe_generation_and_model_rollover() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(include_str!("fixtures/pgvector_revision_pipeline.sql"))
        .execute(&db.pool)
        .await
        .expect("install revision fixture");
    db.execute("INSERT INTO embedding_demo.documents (id, body) VALUES (1, 'A')")
        .await;
    enqueue(&db, "model-v1").await;
    let old = claim(&db, "model-v1").await;
    assert_eq!(old.2, "A");

    db.execute("UPDATE embedding_demo.documents SET body = 'B' WHERE id = 1")
        .await;
    enqueue(&db, "model-v1").await;
    refresh_differential(&db, "corpus_v1").await;
    assert_eq!(db.count("embedding_demo.corpus_v1").await, 0);
    let current = claim(&db, "model-v1").await;
    assert_eq!(current.2, "B");
    assert!(complete(&db, &current, "model-v1").await);
    assert!(!complete(&db, &old, "model-v1").await);
    assert!(!complete(&db, &current, "model-v1").await);
    refresh_differential(&db, "corpus_v1").await;
    let row: (String, String) =
        sqlx::query_as("SELECT body, embedding::text FROM embedding_demo.corpus_v1 WHERE id = 1")
            .fetch_one(&db.pool)
            .await
            .expect("current corpus row");
    assert_eq!(row, ("B".into(), "[0,1,0]".into()));

    let revision: String = db
        .query_scalar("SELECT revision::text FROM embedding_demo.documents WHERE id = 1")
        .await;
    db.execute("UPDATE embedding_demo.documents SET metadata = 'unchanged input' WHERE id = 1")
        .await;
    enqueue(&db, "model-v1").await;
    let unchanged: String = db
        .query_scalar("SELECT revision::text FROM embedding_demo.documents WHERE id = 1")
        .await;
    assert_eq!(revision, unchanged);
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM embedding_demo.jobs WHERE model_version = 'model-v1'")
        .await;
    assert_eq!(count, 2, "metadata-only update must not add work");
    db.execute_seq(&[
        "BEGIN",
        "UPDATE embedding_demo.documents SET body = 'rolled back' WHERE id = 1",
        "SELECT embedding_demo.enqueue(1, 'model-v1')",
        "ROLLBACK",
    ])
    .await;
    let after_rollback: String = db
        .query_scalar("SELECT revision::text FROM embedding_demo.documents WHERE id = 1")
        .await;
    assert_eq!(after_rollback, revision);
    let after_count: i64 = db
        .query_scalar("SELECT count(*) FROM embedding_demo.jobs WHERE model_version = 'model-v1'")
        .await;
    assert_eq!(
        after_count, count,
        "source change and enqueue must roll back together"
    );

    db.execute("UPDATE embedding_demo.documents SET body = 'C' WHERE id = 1")
        .await;
    enqueue(&db, "model-v1").await;
    let abandoned = claim(&db, "model-v1").await;
    db.execute("UPDATE embedding_demo.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE state = 'running'").await;
    let reclaimed = claim(&db, "model-v1").await;
    assert_ne!(abandoned.1, reclaimed.1);
    assert!(!complete(&db, &abandoned, "model-v1").await);
    let failed: bool = sqlx::query_scalar("SELECT embedding_demo.fail($1::text::uuid, $2::text::uuid, 'temporary', interval '1 hour')")
        .bind(&reclaimed.0).bind(&reclaimed.1).fetch_one(&db.pool).await.expect("record failure");
    assert!(failed);
    let before_retry: Option<(String, String, String)> = sqlx::query_as(
        "SELECT claimed_job::text, claimed_token::text, source_body FROM embedding_demo.claim('model-v1', interval '1 minute')"
    ).fetch_optional(&db.pool).await.expect("check retry backoff");
    assert!(
        before_retry.is_none(),
        "failed work must wait for its retry time"
    );
    db.execute("UPDATE embedding_demo.jobs SET available_at = clock_timestamp() - interval '1 second' WHERE state = 'failed'").await;
    let retried = claim(&db, "model-v1").await;
    assert!(complete(&db, &retried, "model-v1").await);

    db.execute("UPDATE embedding_demo.documents SET body = 'D' WHERE id = 1")
        .await;
    enqueue(&db, "model-v1").await;
    let deleted = claim(&db, "model-v1").await;
    let incarnation: String = db
        .query_scalar("SELECT incarnation::text FROM embedding_demo.documents WHERE id = 1")
        .await;
    db.execute("DELETE FROM embedding_demo.documents WHERE id = 1")
        .await;
    db.execute("INSERT INTO embedding_demo.documents (id, body) VALUES (1, 'D')")
        .await;
    let recreated: String = db
        .query_scalar("SELECT incarnation::text FROM embedding_demo.documents WHERE id = 1")
        .await;
    assert_ne!(incarnation, recreated);
    assert!(!complete(&db, &deleted, "model-v1").await);
    refresh_differential(&db, "corpus_v1").await;
    assert_eq!(db.count("embedding_demo.corpus_v1").await, 0);
    enqueue(&db, "model-v1").await;
    let old_chunk = claim(&db, "model-v1").await;
    db.execute("UPDATE embedding_demo.documents SET chunk_version = 2 WHERE id = 1")
        .await;
    enqueue(&db, "model-v1").await;
    assert!(!complete(&db, &old_chunk, "model-v1").await);
    refresh_differential(&db, "corpus_v1").await;
    assert_eq!(db.count("embedding_demo.corpus_v1").await, 0);
    let new_chunk = claim(&db, "model-v1").await;
    assert!(complete(&db, &new_chunk, "model-v1").await);

    let missing_sql = "SELECT count(*) FROM embedding_demo.documents d WHERE d.published AND NOT EXISTS (\
        SELECT 1 FROM embedding_demo.results e WHERE e.document_id = d.id AND e.incarnation = d.incarnation \
        AND e.revision = d.revision AND e.chunk_version = d.chunk_version AND e.model_version = 'model-v2')";
    let missing: i64 = db.query_scalar(missing_sql).await;
    assert_eq!(missing, 1, "rollover must wait for the new model backfill");
    let blocked = db
        .try_execute("SELECT embedding_demo.switch_model('model-v2')")
        .await
        .expect_err("incomplete backfill must not switch the view");
    assert!(blocked.to_string().contains("backfill is incomplete"));
    enqueue(&db, "model-v2").await;
    let v2 = claim(&db, "model-v2").await;
    assert!(complete(&db, &v2, "model-v2").await);
    let missing: i64 = db.query_scalar(missing_sql).await;
    assert_eq!(missing, 0);
    refresh_differential(&db, "corpus_v1").await;
    refresh_differential(&db, "corpus_v2").await;
    let ready: bool = db.query_scalar("SELECT count(*) = 2 AND bool_and(i.indisvalid AND i.indisready AND am.amname = 'hnsw' AND op.opcname = 'vector_cosine_ops') FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am am ON am.oid = c.relam JOIN pg_opclass op ON op.oid = i.indclass[0] WHERE i.indexrelid IN ('embedding_demo.corpus_v1_ann'::regclass, 'embedding_demo.corpus_v2_ann'::regclass)").await;
    assert!(ready);
    db.assert_st_matches_query("embedding_demo.corpus_v2", "SELECT d.id, d.body, e.incarnation, e.revision, e.chunk_version, e.model_version, e.embedding FROM embedding_demo.documents d JOIN embedding_demo.results e ON e.document_id = d.id AND e.incarnation = d.incarnation AND e.revision = d.revision AND e.chunk_version = d.chunk_version WHERE d.published AND e.model_version = 'model-v2'").await;
    let v1: String = db
        .query_scalar("SELECT embedding::text FROM embedding_demo.search_corpus WHERE id = 1")
        .await;
    assert_eq!(v1, "[1,0,0]");
    db.execute("DROP INDEX embedding_demo.corpus_v2_ann").await;
    let blocked = db
        .try_execute("SELECT embedding_demo.switch_model('model-v2')")
        .await
        .expect_err("missing ANN index must block cutover");
    assert!(blocked.to_string().contains("ANN index is unavailable"));
    db.execute("CREATE INDEX corpus_v2_ann ON embedding_demo.corpus_v2 USING hnsw ((embedding::vector(3)) vector_cosine_ops)").await;
    db.execute("SELECT embedding_demo.switch_model('model-v2')")
        .await;
    let switched: String = db
        .query_scalar("SELECT embedding::text FROM embedding_demo.search_corpus WHERE id = 1")
        .await;
    assert_eq!(switched, "[0,0,1]");
    let nearest: i64 = db.query_scalar("SELECT id FROM embedding_demo.search_corpus ORDER BY embedding::vector(3) <=> '[0,0,1]'::vector LIMIT 1").await;
    assert_eq!(nearest, 1);
    db.execute("SELECT embedding_demo.switch_model('model-v1')")
        .await;
    let rolled_back: String = db
        .query_scalar("SELECT embedding::text FROM embedding_demo.search_corpus WHERE id = 1")
        .await;
    assert_eq!(rolled_back, v1);
}

#[tokio::test]
async fn test_model_cutover_holds_source_lock_and_rolls_back() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(include_str!("fixtures/pgvector_revision_pipeline.sql"))
        .execute(&db.pool)
        .await
        .expect("install revision fixture");
    db.execute("INSERT INTO embedding_demo.documents (id, body) VALUES (1, 'A')")
        .await;
    for model in ["model-v1", "model-v2"] {
        enqueue(&db, model).await;
        let job = claim(&db, model).await;
        assert!(complete(&db, &job, model).await);
    }
    refresh_differential(&db, "corpus_v1").await;
    let mut cutover = db.pool.begin().await.expect("begin cutover");
    sqlx::query("SELECT embedding_demo.switch_model('model-v2')")
        .execute(&mut *cutover)
        .await
        .expect("prepare cutover");
    for statement in [
        "UPDATE embedding_demo.documents SET body = 'B' WHERE id = 1",
        "DELETE FROM embedding_demo.results WHERE document_id = 1",
    ] {
        let mut writer = db.pool.begin().await.expect("begin concurrent writer");
        sqlx::query("SET LOCAL statement_timeout = '300ms'")
            .execute(&mut *writer)
            .await
            .expect("set timeout");
        let error = sqlx::query(statement)
            .execute(&mut *writer)
            .await
            .expect_err("cutover must block concurrent source changes");
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("57014")
        );
        writer.rollback().await.expect("release writer");
    }
    cutover.rollback().await.expect("roll back cutover");
    let unchanged: String = db
        .query_scalar("SELECT embedding::text FROM embedding_demo.search_corpus WHERE id = 1")
        .await;
    assert_eq!(unchanged, "[1,0,0]");
    db.execute("UPDATE embedding_demo.documents SET metadata = 'after rollback' WHERE id = 1")
        .await;
    db.execute("UPDATE embedding_demo.results SET embedding = embedding WHERE document_id = 1")
        .await;
    db.execute("SELECT embedding_demo.switch_model('model-v2')")
        .await;
    let switched: String = db
        .query_scalar("SELECT embedding::text FROM embedding_demo.search_corpus WHERE id = 1")
        .await;
    assert_eq!(switched, "[0,0,1]");
}

#[tokio::test]
async fn test_revision_safe_rechunk_rejects_inflight_results() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(include_str!("fixtures/pgvector_revision_pipeline.sql"))
        .execute(&db.pool)
        .await
        .expect("install revision fixture");
    db.execute("INSERT INTO embedding_demo.document_sets VALUES (7)")
        .await;
    db.execute("SELECT embedding_demo.replace_chunks(7, ARRAY['A', 'B'], 1, 'model-v1')")
        .await;
    let first = claim(&db, "model-v1").await;
    let inflight = claim(&db, "model-v1").await;
    assert!(complete(&db, &first, "model-v1").await);
    refresh_differential(&db, "corpus_v1").await;
    assert_eq!(db.count("embedding_demo.corpus_v1").await, 1);

    db.execute_seq(&[
        "BEGIN",
        "SELECT embedding_demo.replace_chunks(7, ARRAY['rolled back'], 2, 'model-v1')",
        "ROLLBACK",
    ])
    .await;
    assert_eq!(db.count("embedding_demo.documents").await, 2);
    db.execute("SELECT embedding_demo.replace_chunks(7, ARRAY['B', 'A', 'C'], 2, 'model-v1')")
        .await;
    assert!(!complete(&db, &inflight, "model-v1").await);
    refresh_differential(&db, "corpus_v1").await;
    assert_eq!(db.count("embedding_demo.corpus_v1").await, 0);
    for _ in 0..3 {
        let chunk = claim(&db, "model-v1").await;
        assert!(complete(&db, &chunk, "model-v1").await);
    }
    refresh_differential(&db, "corpus_v1").await;
    let rows: Vec<(i32, String, String)> = sqlx::query_as(
        "SELECT d.chunk_index, c.body, c.embedding::text FROM embedding_demo.corpus_v1 c \
         JOIN embedding_demo.documents d ON d.id = c.id WHERE d.document_set_id = 7 ORDER BY d.chunk_index",
    )
    .fetch_all(&db.pool)
    .await
    .expect("current chunks");
    assert_eq!(
        rows,
        vec![
            (1, "B".into(), "[0,1,0]".into()),
            (2, "A".into(), "[1,0,0]".into()),
            (3, "C".into(), "[1,0,0]".into()),
        ]
    );
    db.execute("SELECT embedding_demo.replace_chunks(7, ARRAY['A'], 3, 'model-v1')")
        .await;
    let deleted = claim(&db, "model-v1").await;
    db.execute("DELETE FROM embedding_demo.document_sets WHERE id = 7")
        .await;
    db.execute("INSERT INTO embedding_demo.document_sets VALUES (7)")
        .await;
    db.execute("SELECT embedding_demo.replace_chunks(7, ARRAY['B'], 3, 'model-v1')")
        .await;
    assert!(!complete(&db, &deleted, "model-v1").await);
    refresh_differential(&db, "corpus_v1").await;
    assert_eq!(db.count("embedding_demo.corpus_v1").await, 0);
    let recreated = claim(&db, "model-v1").await;
    assert!(complete(&db, &recreated, "model-v1").await);
    refresh_differential(&db, "corpus_v1").await;
    let body: String = db
        .query_scalar("SELECT body FROM embedding_demo.corpus_v1")
        .await;
    assert_eq!(body, "B");
}
