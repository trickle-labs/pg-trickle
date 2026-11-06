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
