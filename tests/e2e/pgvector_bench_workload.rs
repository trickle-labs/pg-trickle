//! Opt-in database measurements for filtered pgvector retrieval and refresh.

use super::e2e::E2eDb;
use std::{collections::HashSet, time::Instant};

fn percentiles(mut values: Vec<f64>) -> serde_json::Value {
    values.sort_by(f64::total_cmp);
    let at = |fraction: f64| values[((values.len() as f64 * fraction).ceil() as usize) - 1];
    serde_json::json!({"p50": at(0.50), "p95": at(0.95), "p99": at(0.99), "samples": values})
}

fn has_index(value: &serde_json::Value, index: &str) -> bool {
    match value {
        serde_json::Value::Object(fields) => {
            fields.get("Index Name").and_then(|v| v.as_str()) == Some(index)
                || fields.values().any(|v| has_index(v, index))
        }
        serde_json::Value::Array(items) => items.iter().any(|v| has_index(v, index)),
        _ => false,
    }
}

#[test]
fn percentile_report_uses_nearest_rank() {
    let values = percentiles(vec![4.0, 1.0, 3.0, 2.0]);
    assert_eq!(values["p50"], 2.0);
    assert_eq!(values["p95"], 4.0);
    assert_eq!(values["p99"], 4.0);
}

/// Writes one JSON report to stdout; no timing or recall thresholds are imposed.
#[tokio::test]
#[ignore = "database benchmark; run explicitly with --run-ignored all --no-capture"]
async fn test_pgvector_filtered_ann_workload() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE EXTENSION vector").await;
    let dim: usize = std::env::var("PGVECTOR_BENCH_DIM")
        .unwrap_or_else(|_| "384".into())
        .parse()
        .expect("dimension");
    let rows: usize = std::env::var("PGVECTOR_BENCH_ROWS")
        .unwrap_or_else(|_| "1000".into())
        .parse()
        .expect("rows");
    let samples: usize = std::env::var("PGVECTOR_BENCH_SAMPLES")
        .unwrap_or_else(|_| "20".into())
        .parse()
        .expect("samples");
    assert!((1..=2000).contains(&dim) && rows >= 100 && (1..=rows).contains(&samples));
    db.execute(&format!("CREATE TABLE vector_bench_source (id bigint PRIMARY KEY, tenant int NOT NULL, embedding vector({dim}) NOT NULL)")).await;
    // Stable hash-derived coordinates make the input reproducible without a session RNG.
    db.execute(&format!("INSERT INTO vector_bench_source SELECT i, (i % 10)::int, \
        ARRAY(SELECT (('x' || substr(md5(i::text || ':' || n::text), 1, 8))::bit(32)::bigint / 2147483648.0 - 1)::real \
        FROM generate_series(1, {dim}) n ORDER BY n)::vector({dim}) FROM generate_series(1, {rows}) i")).await;
    db.execute(
        "SELECT pgtrickle.create_stream_table('vector_bench_corpus', \
        'SELECT id, tenant, embedding FROM vector_bench_source', '1h', 'DIFFERENTIAL')",
    )
    .await;
    db.execute("ANALYZE vector_bench_corpus").await;
    let versions: (String, String, String) = sqlx::query_as(
        "SELECT version(), \
        (SELECT extversion FROM pg_extension WHERE extname = 'vector'), \
        (SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("versions");
    let mut results = Vec::new();
    for method in ["hnsw", "ivfflat"] {
        db.execute(&format!(
            "CREATE INDEX vector_bench_ann ON vector_bench_corpus USING {method} \
            ((embedding::vector({dim})) vector_cosine_ops){}",
            if method == "ivfflat" {
                " WITH (lists = 10)"
            } else {
                ""
            }
        ))
        .await;
        db.execute("ANALYZE vector_bench_corpus").await;
        let index_bytes: i64 = db
            .query_scalar("SELECT pg_relation_size('vector_bench_ann')")
            .await;
        let corpus_bytes: i64 = db
            .query_scalar("SELECT pg_total_relation_size('vector_bench_corpus')")
            .await;
        let mut latencies = Vec::new();
        let mut recalls = Vec::new();
        let mut recorded_plan = serde_json::Value::Null;
        let mut settings = (String::new(), String::new());
        for id in 1..=samples {
            let (tenant, query): (i32, String) = sqlx::query_as(
                "SELECT tenant, embedding::text FROM vector_bench_source WHERE id = $1",
            )
            .bind(id as i64)
            .fetch_one(&db.pool)
            .await
            .expect("query vector");
            let mut tx = db.pool.begin().await.expect("benchmark transaction");
            sqlx::query("SET LOCAL enable_seqscan = off")
                .execute(&mut *tx)
                .await
                .expect("force ANN eligibility");
            sqlx::query("SET LOCAL enable_indexonlyscan = off")
                .execute(&mut *tx)
                .await
                .expect("exclude covering index scan");
            sqlx::query("SET LOCAL enable_bitmapscan = off")
                .execute(&mut *tx)
                .await
                .expect("exclude bitmap scan");
            let query_sql = format!(
                "SELECT id FROM vector_bench_corpus WHERE tenant = $1 \
                 ORDER BY embedding::vector({dim}) <=> $2::text::vector LIMIT 10"
            );
            if id == 1 {
                settings = sqlx::query_as(
                    "SELECT current_setting('hnsw.ef_search'), current_setting('ivfflat.probes')",
                )
                .fetch_one(&mut *tx)
                .await
                .expect("ANN settings");
                let explain = format!("EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) {query_sql}");
                recorded_plan = sqlx::query_scalar(sqlx::AssertSqlSafe(explain))
                    .bind(tenant)
                    .bind(&query)
                    .fetch_one(&mut *tx)
                    .await
                    .expect("ANN plan");
                assert!(
                    has_index(&recorded_plan, "vector_bench_ann"),
                    "ANN index not used: {recorded_plan}"
                );
            }
            let start = Instant::now();
            let ann: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(query_sql))
                .bind(tenant)
                .bind(&query)
                .fetch_all(&mut *tx)
                .await
                .expect("ANN query");
            latencies.push(start.elapsed().as_secs_f64() * 1000.0);
            sqlx::query("SET LOCAL enable_indexscan = off")
                .execute(&mut *tx)
                .await
                .expect("exact scan");
            sqlx::query("SET LOCAL enable_bitmapscan = off")
                .execute(&mut *tx)
                .await
                .expect("exact scan");
            sqlx::query("SET LOCAL enable_seqscan = on")
                .execute(&mut *tx)
                .await
                .expect("exact scan");
            let exact_sql = format!(
                "SELECT id FROM vector_bench_corpus WHERE tenant = $1 \
                 ORDER BY embedding::vector({dim}) <=> $2::text::vector, id LIMIT 10"
            );
            let exact: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(exact_sql))
                .bind(tenant)
                .bind(&query)
                .fetch_all(&mut *tx)
                .await
                .expect("exact query");
            assert!(!exact.is_empty());
            let expected: HashSet<_> = exact.iter().copied().collect();
            recalls.push(
                ann.iter().filter(|id| expected.contains(id)).count() as f64 / exact.len() as f64,
            );
            tx.rollback().await.expect("end benchmark transaction");
        }
        results.push(serde_json::json!({"method": method, "metric": "cosine", "tenant_fraction": 0.1,
            "k": 10, "retrieval_ms": percentiles(latencies), "recall_at_k": recalls,
            "hnsw_ef_search": settings.0, "ivfflat_probes": settings.1,
            "index_bytes": index_bytes, "corpus_total_bytes_including_indexes": corpus_bytes, "ann_plan": recorded_plan}));
        db.execute("DROP INDEX vector_bench_ann").await;
    }
    db.execute(&format!(
        "CREATE INDEX vector_bench_ann ON vector_bench_corpus USING hnsw \
        ((embedding::vector({dim})) vector_cosine_ops)"
    ))
    .await;
    let mut refresh_ms = Vec::new();
    let mut wal_bytes = Vec::new();
    for cycle in 0..3 {
        let changed = if cycle == 0 { 1 } else { rows / 10 };
        if cycle == 2 {
            db.execute(&format!(
                "DELETE FROM vector_bench_source WHERE id <= {changed}"
            ))
            .await;
        } else {
            let source_id = rows - cycle;
            db.execute(&format!(
                "UPDATE vector_bench_source SET embedding = \
                (SELECT embedding FROM vector_bench_source WHERE id = {source_id}) WHERE id <= {changed}"
            ))
            .await;
        }
        let before: String = db
            .query_scalar("SELECT pg_current_wal_insert_lsn()::text")
            .await;
        let start = Instant::now();
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy = 'differential'",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            "SELECT pgtrickle.refresh_stream_table('vector_bench_corpus')",
        )
        .await
        .expect("differential refresh");
        refresh_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        let written: f64 = sqlx::query_scalar(
            "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::text::pg_lsn)::float8",
        )
        .bind(before)
        .fetch_one(&db.pool)
        .await
        .expect("WAL delta");
        wal_bytes.push(written);
        let differential: bool = db.query_scalar("SELECT h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
            FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
            WHERE s.pgt_name = 'vector_bench_corpus' ORDER BY refresh_id DESC LIMIT 1").await;
        assert!(differential, "refresh fell back from differential");
        db.assert_st_matches_query(
            "vector_bench_corpus",
            "SELECT id, tenant, embedding FROM vector_bench_source",
        )
        .await;
    }
    println!(
        "PGVECTOR_BENCHMARK_JSON={}",
        serde_json::json!({"schema": 1, "postgres": versions.0,
        "pgvector": versions.1, "pg_trickle": versions.2, "host_arch": std::env::consts::ARCH,
        "data": "md5(id:dimension), seedless deterministic", "rows": rows, "dimensions": dim,
        "ann": results, "refresh": {"operations": ["update embedding", "update embedding", "delete"],
        "changed_rows": [1, rows / 10, rows / 10], "elapsed_ms": refresh_ms,
        "wal_bytes_cluster_wide": wal_bytes, "notes": "HNSW index maintained; WAL is cluster-wide between LSN reads and excludes source mutations"}})
    );
}
