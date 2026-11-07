//! Executable hybrid retrieval fixture with tenant-isolated candidate sets.

mod e2e;

use e2e::E2eDb;

const TENANT_A: &str = "00000000-0000-0000-0000-000000000001";
const TENANT_B: &str = "00000000-0000-0000-0000-000000000002";

fn uses_index(plan: &serde_json::Value, expected: &str) -> bool {
    match plan {
        serde_json::Value::Object(fields) => {
            fields.get("Index Name").and_then(|name| name.as_str()) == Some(expected)
                || fields.values().any(|child| uses_index(child, expected))
        }
        serde_json::Value::Array(children) => {
            children.iter().any(|child| uses_index(child, expected))
        }
        _ => false,
    }
}

async fn refresh_differential(db: &E2eDb) {
    let previous: i64 = db.query_scalar(
        "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'hybrid_search_corpus'"
    ).await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ],
        "SELECT pgtrickle.refresh_stream_table('hybrid_search_corpus')",
    )
    .await
    .expect("refresh hybrid corpus");
    let differential: bool = db.query_scalar(&format!(
        "SELECT h.refresh_id > {previous} AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
         FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
         WHERE s.pgt_name = 'hybrid_search_corpus' ORDER BY h.refresh_id DESC LIMIT 1"
    )).await;
    assert!(differential, "hybrid corpus did not refresh differentially");
}

async fn search(
    db: &E2eDb,
    context: Option<&str>,
    requested: &str,
    terms: &str,
) -> Vec<(String, i64, f64)> {
    let mut tx = db.pool.begin().await.expect("begin reader transaction");
    sqlx::query("SET LOCAL ROLE hybrid_reader")
        .execute(&mut *tx)
        .await
        .expect("set restricted reader");
    if let Some(tenant) = context {
        sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
            .bind(tenant)
            .execute(&mut *tx)
            .await
            .expect("set local tenant");
    }
    let rows = sqlx::query_as(include_str!("fixtures/pgvector_hybrid_query.sql"))
        .bind(requested)
        .bind("[1,0,0]")
        .bind(terms)
        .fetch_all(&mut *tx)
        .await
        .expect("hybrid retrieval");
    tx.rollback().await.expect("reset reader context");
    rows
}

#[tokio::test]
async fn test_hybrid_candidates_refresh_and_tenant_isolation() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(include_str!("fixtures/pgvector_hybrid_setup.sql"))
        .execute(&db.pool)
        .await
        .expect("install hybrid fixture");
    db.execute("CREATE ROLE hybrid_reader NOLOGIN").await;
    db.execute("GRANT SELECT ON hybrid_search_corpus TO hybrid_reader")
        .await;
    db.execute(&format!(
        "INSERT INTO hybrid_documents VALUES \
         ('{TENANT_A}', 1, 'red apple', 'fruit', true), \
         ('{TENANT_A}', 2, 'blue berry', 'fruit', true), \
         ('{TENANT_B}', 1, 'secret apple', 'fruit', true)"
    ))
    .await;
    db.execute(&format!(
        "INSERT INTO hybrid_embeddings VALUES \
         ('{TENANT_A}', 1, '[1,0,0]'), ('{TENANT_A}', 2, '[0,1,0]'), \
         ('{TENANT_B}', 1, '[1,0,0]')"
    ))
    .await;
    refresh_differential(&db).await;

    let ready: bool = db.query_scalar(
        "SELECT i.indisvalid AND i.indisready AND am.amname = 'hnsw' AND op.opcname = 'vector_cosine_ops' \
         FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am am ON am.oid = c.relam \
         JOIN pg_opclass op ON op.oid = i.indclass[0] \
         WHERE i.indexrelid = 'hybrid_search_cosine_idx'::regclass"
    ).await;
    assert!(ready, "cosine candidate index must be valid and ready");
    let mut tx = db.pool.begin().await.expect("begin plan check");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .expect("test index eligibility");
    sqlx::query("SET LOCAL enable_bitmapscan = off")
        .execute(&mut *tx)
        .await
        .expect("exclude bitmap scan for eligibility check");
    sqlx::query("SET LOCAL enable_indexonlyscan = off")
        .execute(&mut *tx)
        .await
        .expect("exclude covering scan for eligibility check");
    sqlx::query("SET LOCAL ROLE hybrid_reader")
        .execute(&mut *tx)
        .await
        .expect("set restricted reader");
    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(TENANT_A)
        .execute(&mut *tx)
        .await
        .expect("set tenant");
    let plan: serde_json::Value = sqlx::query_scalar(
        "EXPLAIN (FORMAT JSON) SELECT id FROM hybrid_search_corpus WHERE tenant_id = $1::text::uuid \
         ORDER BY embedding::vector(3) <=> '[1,0,0]'::vector LIMIT 20"
    )
    .bind(TENANT_A)
    .fetch_one(&mut *tx)
    .await
    .expect("explain vector candidates");
    assert!(
        uses_index(&plan, "hybrid_search_cosine_idx"),
        "candidate index is not eligible: {plan}"
    );
    let half_ready: bool = sqlx::query_scalar(
        "SELECT i.indisvalid AND i.indisready AND am.amname = 'hnsw' AND op.opcname = 'halfvec_cosine_ops' \
         FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am am ON am.oid = c.relam \
         JOIN pg_opclass op ON op.oid = i.indclass[0] \
         WHERE i.indexrelid = 'hybrid_search_half_cosine_idx'::regclass"
    ).fetch_one(&mut *tx).await.expect("inspect halfvec index");
    assert!(half_ready, "halfvec index must be valid and ready");
    let half_plan: serde_json::Value = sqlx::query_scalar(
        "EXPLAIN (FORMAT JSON) SELECT id FROM hybrid_search_corpus WHERE tenant_id = $1::text::uuid \
         ORDER BY embedding::halfvec(3) <=> '[1,0,0]'::halfvec LIMIT 20"
    ).bind(TENANT_A).fetch_one(&mut *tx).await.expect("explain halfvec candidates");
    assert!(
        uses_index(&half_plan, "hybrid_search_half_cosine_idx"),
        "halfvec index is not eligible: {half_plan}"
    );
    let approximate: Vec<(String, i64, f64)> =
        sqlx::query_as(include_str!("fixtures/pgvector_halfvec_rerank_query.sql"))
            .bind(TENANT_A)
            .bind("[1,0,0]")
            .fetch_all(&mut *tx)
            .await
            .expect("halfvec candidates and full-vector rerank");
    let forbidden: Vec<(String, i64, f64)> =
        sqlx::query_as(include_str!("fixtures/pgvector_halfvec_rerank_query.sql"))
            .bind(TENANT_B)
            .bind("[1,0,0]")
            .fetch_all(&mut *tx)
            .await
            .expect("cross-tenant halfvec query");
    assert!(
        forbidden.is_empty(),
        "halfvec candidates must respect tenant RLS"
    );
    sqlx::query("SET LOCAL enable_indexscan = off")
        .execute(&mut *tx)
        .await
        .expect("use exact baseline");
    let exact: Vec<(String, i64, f64)> = sqlx::query_as(
        "SELECT tenant_id::text, id, embedding::vector(3) <=> $2::text::vector AS distance \
         FROM hybrid_search_corpus WHERE tenant_id = $1::text::uuid ORDER BY distance, id LIMIT 10",
    )
    .bind(TENANT_A)
    .bind("[1,0,0]")
    .fetch_all(&mut *tx)
    .await
    .expect("exact full-vector baseline");
    assert_eq!(
        approximate, exact,
        "small deterministic fixture should rerank to the exact result"
    );
    tx.rollback().await.expect("end plan check");

    let rows = search(&db, Some(TENANT_A), TENANT_A, "apple").await;
    assert_eq!(rows.iter().map(|r| r.1).collect::<Vec<_>>(), vec![1, 2]);
    assert!(rows.iter().all(|r| r.0 == TENANT_A));
    let lexical_boost = search(&db, Some(TENANT_A), TENANT_A, "berry").await;
    assert_eq!(
        lexical_boost.iter().map(|r| r.1).collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(
        search(&db, Some(TENANT_B), TENANT_B, "apple").await.len(),
        1
    );
    assert!(
        search(&db, Some(TENANT_A), TENANT_B, "apple")
            .await
            .is_empty()
    );
    assert!(search(&db, None, TENANT_A, "apple").await.is_empty());
    let mut conn = db.pool.acquire().await.expect("acquire pooled connection");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin tenant transaction");
    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(TENANT_A)
        .execute(&mut *conn)
        .await
        .expect("set tenant locally");
    sqlx::query("COMMIT")
        .execute(&mut *conn)
        .await
        .expect("commit tenant transaction");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin reused transaction");
    sqlx::query("SET LOCAL ROLE hybrid_reader")
        .execute(&mut *conn)
        .await
        .expect("set reader");
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM hybrid_search_corpus")
        .fetch_one(&mut *conn)
        .await
        .expect("query without tenant context");
    assert_eq!(
        visible, 0,
        "tenant context leaked across pooled transactions"
    );
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("end reused transaction");
    assert_eq!(
        search(&db, Some(TENANT_A), TENANT_A, "unmatched")
            .await
            .len(),
        2
    );

    db.execute(&format!(
        "UPDATE hybrid_documents SET visible = false WHERE tenant_id = '{TENANT_A}' AND id = 1"
    ))
    .await;
    db.execute(&format!(
        "DELETE FROM hybrid_embeddings WHERE tenant_id = '{TENANT_A}' AND document_id = 2"
    ))
    .await;
    refresh_differential(&db).await;
    assert!(
        search(&db, Some(TENANT_A), TENANT_A, "apple")
            .await
            .is_empty()
    );
    assert_eq!(
        search(&db, Some(TENANT_B), TENANT_B, "apple").await.len(),
        1
    );
}

#[tokio::test]
async fn test_partitioned_corpus_parent_rls_and_leaf_privileges() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE EXTENSION vector").await;
    db.execute("CREATE TABLE partition_docs (tenant_id uuid NOT NULL, id int NOT NULL, embedding vector(3), PRIMARY KEY (tenant_id, id))").await;
    db.execute(&format!(
        "INSERT INTO partition_docs VALUES ('{TENANT_A}', 1, '[1,0,0]'), ('{TENANT_B}', 1, '[0,1,0]')"
    )).await;
    db.execute(
        "SELECT pgtrickle.create_stream_table('partition_corpus', \
         $$SELECT tenant_id, id, embedding FROM partition_docs$$, '1h', 'DIFFERENTIAL', \
         partition_by => 'HASH:tenant_id:4')",
    )
    .await;
    db.execute("ALTER TABLE partition_corpus ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute("ALTER TABLE partition_corpus FORCE ROW LEVEL SECURITY")
        .await;
    db.execute("CREATE POLICY tenant_policy ON partition_corpus FOR SELECT USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid)").await;
    db.execute("CREATE ROLE partition_reader NOLOGIN").await;
    db.execute("GRANT SELECT ON partition_corpus TO partition_reader")
        .await;
    let leaf: String = db
        .query_scalar("SELECT tableoid::regclass::text FROM partition_corpus LIMIT 1")
        .await;
    let mut tx = db.pool.begin().await.expect("begin reader transaction");
    sqlx::query("SET LOCAL ROLE partition_reader")
        .execute(&mut *tx)
        .await
        .expect("set reader");
    sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
        .bind(TENANT_A)
        .execute(&mut *tx)
        .await
        .expect("set tenant");
    let rows: Vec<(String, i32)> =
        sqlx::query_as("SELECT tenant_id::text, id FROM partition_corpus")
            .fetch_all(&mut *tx)
            .await
            .expect("query parent");
    assert_eq!(rows, vec![(TENANT_A.to_string(), 1)]);
    tx.rollback().await.expect("end reader transaction");
    let error = db
        .try_execute_with_role(
            "SET ROLE partition_reader",
            &format!("SELECT * FROM {leaf}"),
            "RESET ROLE",
        )
        .await
        .expect_err("reader must not access a leaf directly");
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501")
    );
}
