//! Opt-in database measurements for filtered pgvector retrieval and refresh.

use super::e2e::E2eDb;
use std::{
    collections::HashSet,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

const CENTROID_COMPONENT_WITHIN_BOUND: &str = "x.actual IS NOT NULL AND x.expected IS NOT NULL AND abs(x.actual::float8 - x.expected::float8) <= 1e-5 + 1e-6 * abs(x.expected::float8)";

struct SubscriptionPlanLog {
    container: String,
    path: PathBuf,
}

impl SubscriptionPlanLog {
    fn capture(&self) -> Result<(), String> {
        let output = Command::new("docker")
            .args(["logs", "--timestamps", &self.container])
            .output()
            .map_err(|error| format!("read PostgreSQL logs: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "docker logs failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let mut logs = output.stdout;
        logs.extend_from_slice(&output.stderr);
        std::fs::write(&self.path, logs).map_err(|error| format!("write PostgreSQL logs: {error}"))
    }
}

impl Drop for SubscriptionPlanLog {
    fn drop(&mut self) {
        if let Err(error) = self.capture() {
            eprintln!("could not preserve subscription plan log: {error}");
        }
    }
}

fn subscription_plan_marker(
    stage: &str,
    rows: usize,
    subscriptions: usize,
    analyzed: bool,
    round: usize,
) -> String {
    format!(
        "DO $marker$ BEGIN RAISE LOG 'PGVECTOR_SUB_{stage} rows={rows} subscriptions={subscriptions} analyzed={analyzed} round={round}'; END; $marker$"
    )
}

#[test]
fn subscription_plan_marker_identifies_the_case() {
    assert!(
        subscription_plan_marker("START", 1000, 10, false, 2)
            .contains("PGVECTOR_SUB_START rows=1000 subscriptions=10 analyzed=false round=2")
    );
}

fn require_pgvector_version(installed: &str) {
    if let Ok(expected) = std::env::var("PGVECTOR_EXPECTED_VERSION") {
        assert_eq!(installed, expected, "unexpected pgvector version");
    }
}

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

fn has_source_seq_scan(value: &serde_json::Value, relation: &str) -> bool {
    match value {
        serde_json::Value::Object(fields) => {
            (fields.get("Node Type").and_then(|v| v.as_str()) == Some("Seq Scan")
                && fields.get("Relation Name").and_then(|v| v.as_str()) == Some(relation))
                || fields.values().any(|v| has_source_seq_scan(v, relation))
        }
        serde_json::Value::Array(items) => items.iter().any(|v| has_source_seq_scan(v, relation)),
        _ => false,
    }
}

fn subscription_counts(value: &str, max: usize, allow_zero: bool) -> Result<Vec<usize>, String> {
    let counts: Vec<usize> = value
        .split(',')
        .map(|item| {
            let count = item
                .trim()
                .parse::<usize>()
                .map_err(|_| format!("invalid count: {item}"))?;
            if count > max || (!allow_zero && count == 0) {
                return Err(format!("count outside allowed range: {count}"));
            }
            Ok(count)
        })
        .collect::<Result<_, _>>()?;
    if counts.iter().copied().collect::<HashSet<_>>().len() != counts.len() {
        return Err("duplicate count".into());
    }
    Ok(counts)
}

#[test]
fn subscription_benchmark_count_validation() {
    assert_eq!(
        subscription_counts("0, 10,1000", 1000, true).unwrap(),
        vec![0, 10, 1000]
    );
    assert!(subscription_counts("", 1000, true).is_err());
    assert!(subscription_counts("0", 1000, false).is_err());
    assert!(subscription_counts("1001", 1000, true).is_err());
    assert!(subscription_counts("1,1", 1000, true).is_err());
}

#[tokio::test]
async fn centroid_benchmark_oracle_rejects_missing_components() {
    let db = E2eDb::new().await.with_extension().await;
    let comparison = format!(
        "SELECT COALESCE(bool_and({CENTROID_COMPONENT_WITHIN_BOUND}), false) FROM unnest($1::real[], $2::real[]) AS x(actual, expected)"
    );
    for (actual, expected, valid) in [
        (vec![1.0_f32, 2.0], vec![1.0_f32, 2.0], true),
        (vec![1.0_f32], vec![1.0_f32, 2.0], false),
        (vec![1.0_f32, 2.0], vec![1.0_f32], false),
        (vec![1.0_f32, 2.1], vec![1.0_f32, 2.0], false),
    ] {
        let matched: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(comparison.as_str()))
            .bind(actual)
            .bind(expected)
            .fetch_one(&db.pool)
            .await
            .expect("evaluate centroid component oracle");
        assert_eq!(matched, valid);
    }
}

#[test]
fn percentile_report_uses_nearest_rank() {
    let values = percentiles(vec![4.0, 1.0, 3.0, 2.0]);
    assert_eq!(values["p50"], 2.0);
    assert_eq!(values["p95"], 4.0);
    assert_eq!(values["p99"], 4.0);
}

#[test]
fn exact_baseline_requires_source_sequential_scan() {
    let plan = serde_json::json!([{"Plan": {"Node Type": "Limit", "Plans": [
        {"Node Type": "Seq Scan", "Relation Name": "vector_bench_corpus"}
    ]}}]);
    assert!(has_source_seq_scan(&plan, "vector_bench_corpus"));
    assert!(!has_source_seq_scan(&plan, "different_table"));
    assert!(!has_source_seq_scan(
        &serde_json::json!({"Node Type": "Index Scan", "Relation Name": "vector_bench_corpus"}),
        "vector_bench_corpus"
    ));
}

/// Measure one-row membership transitions across corpus and subscription counts.
#[tokio::test]
#[ignore = "database benchmark; run explicitly with --run-ignored all --no-capture"]
async fn test_pgvector_subscription_scaling_workload() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE EXTENSION vector").await;
    let versions: (String, String, String) = sqlx::query_as(
        "SELECT version(), (SELECT extversion FROM pg_extension WHERE extname='vector'), \
         (SELECT extversion FROM pg_extension WHERE extname='pg_trickle')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("inspect benchmark versions");
    require_pgvector_version(&versions.1);
    let plan_log = std::env::var_os("PGVECTOR_SUB_PLAN_LOG").map(|path| SubscriptionPlanLog {
        container: db.container_id().to_owned(),
        path: path.into(),
    });
    let row_counts = subscription_counts(
        &std::env::var("PGVECTOR_SUB_ROWS").unwrap_or_else(|_| "1000,10000".into()),
        1_000_000,
        false,
    )
    .expect("PGVECTOR_SUB_ROWS must contain row counts from 1 to 1000000");
    let subscription_counts = subscription_counts(
        &std::env::var("PGVECTOR_SUB_COUNTS").unwrap_or_else(|_| "0,1,10".into()),
        1000,
        true,
    )
    .expect("PGVECTOR_SUB_COUNTS must contain counts from 0 to 1000");
    let dimensions: usize = std::env::var("PGVECTOR_SUB_DIM")
        .unwrap_or_else(|_| "3".into())
        .parse()
        .expect("PGVECTOR_SUB_DIM must be an integer");
    assert!(
        (2..=1536).contains(&dimensions),
        "PGVECTOR_SUB_DIM must be between 2 and 1536"
    );
    let rounds: usize = std::env::var("PGVECTOR_SUB_ROUNDS")
        .unwrap_or_else(|_| "5".into())
        .parse()
        .expect("PGVECTOR_SUB_ROUNDS must be an integer");
    assert!(
        (1..=100).contains(&rounds),
        "PGVECTOR_SUB_ROUNDS must be between 1 and 100"
    );
    let statistics = std::env::var("PGVECTOR_SUB_STATISTICS").unwrap_or_else(|_| "analyzed".into());
    let analyze_cases: &[bool] = match statistics.as_str() {
        "analyzed" => &[true],
        "unanalyzed" => &[false],
        "both" => &[false, true],
        _ => panic!("PGVECTOR_SUB_STATISTICS must be analyzed, unanalyzed, or both"),
    };
    let force_explicit =
        std::env::var("PGVECTOR_SUB_FORCE_EXPLICIT").unwrap_or_else(|_| "0".into());
    assert!(
        force_explicit == "0" || force_explicit == "1",
        "PGVECTOR_SUB_FORCE_EXPLICIT must be 0 or 1"
    );
    let force_explicit = force_explicit == "1";
    let vector = |position: usize| {
        format!(
            "[{}]",
            (0..dimensions)
                .map(|i| if i == position { "1" } else { "0" })
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let first = vector(0);
    let second = vector(1);
    if force_explicit {
        db.execute("CREATE FUNCTION public.subscription_bench_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$").await;
    }
    let mut measurements = Vec::new();
    for &analyzed in analyze_cases {
        for &rows in &row_counts {
            for &subscriptions in &subscription_counts {
                let source = format!(
                    "alert_measure_src_{rows}_{subscriptions}_{}",
                    u8::from(analyzed)
                );
                let stream = format!(
                    "alert_measure_st_{rows}_{subscriptions}_{}",
                    u8::from(analyzed)
                );
                db.execute(&format!(
                    "CREATE TABLE {source} (id int PRIMARY KEY, embedding vector({dimensions}))"
                ))
                .await;
                db.execute(&format!(
                "INSERT INTO {source} SELECT i, '{first}'::vector FROM generate_series(1,{rows}) i"
            ))
            .await;
                db.create_st(
                    &stream,
                    &format!("SELECT id, embedding FROM {source}"),
                    "1h",
                    "DIFFERENTIAL",
                )
                .await;
                db.execute(&format!(
                    "SELECT pgtrickle.pause_scheduler(ARRAY['public.{stream}'])"
                ))
                .await;
                db.execute(&format!(
                    "ALTER TABLE {stream} SET (autovacuum_enabled = false)"
                ))
                .await;
                if force_explicit {
                    db.execute(&format!("CREATE TRIGGER subscription_bench_noop AFTER INSERT OR UPDATE OR DELETE ON {stream} FOR EACH ROW EXECUTE FUNCTION public.subscription_bench_noop()" )).await;
                }
                if analyzed {
                    db.execute(&format!("ANALYZE {stream}")).await;
                }
                let (estimated_rows, last_analyze, last_autoanalyze): (f32, Option<String>, Option<String>) = sqlx::query_as("SELECT c.reltuples, st.last_analyze::text, st.last_autoanalyze::text FROM pg_catalog.pg_class c JOIN pg_catalog.pg_stat_all_tables st ON st.relid=c.oid WHERE c.oid=$1::text::regclass")
                .bind(&stream).fetch_one(&db.pool).await.expect("inspect stream statistics");
                if !analyzed {
                    assert!(
                        last_analyze.is_none() && last_autoanalyze.is_none(),
                        "control stream was analyzed before measurement"
                    );
                }
                let mut listener = sqlx::postgres::PgListener::connect_with(&db.pool)
                    .await
                    .expect("connect subscription benchmark listener");
                for index in 0..subscriptions {
                    let channel = format!("alert_measure_{index}");
                    listener
                        .listen(&channel)
                        .await
                        .expect("listen for measured transitions");
                    db.execute(&format!("SELECT pgtrickle.subscribe_distance('{stream}', '{channel}', 'embedding', '{first}', '<->', 0.5)")).await;
                }
                let pgt_id: i64 = sqlx::query_scalar(
                "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid=$1::text::regclass",
            )
            .bind(&stream)
            .fetch_one(&db.pool)
            .await
            .expect("inspect subscription stream");
                let mut samples = Vec::new();
                for round in 0..rounds {
                    let vector = if round % 2 == 0 { &second } else { &first };
                    db.execute(&format!(
                        "UPDATE {source} SET embedding='{vector}' WHERE id=1"
                    ))
                    .await;
                    let mut tx = db
                        .pool
                        .begin()
                        .await
                        .expect("begin subscription measurement");
                    sqlx::query("SET LOCAL pg_trickle.refresh_strategy = 'differential'")
                        .execute(&mut *tx)
                        .await
                        .expect("force differential refresh");
                    sqlx::query("SET LOCAL pg_trickle.differential_max_change_ratio = 0")
                        .execute(&mut *tx)
                        .await
                        .expect("disable ratio fallback");
                    if force_explicit {
                        sqlx::query("SET LOCAL pg_trickle.user_triggers = 'auto'")
                            .execute(&mut *tx)
                            .await
                            .expect("enable control trigger");
                    }
                    if plan_log.is_some() {
                        for statement in [
                            "LOAD 'auto_explain'",
                            "SET LOCAL auto_explain.log_min_duration = 0",
                            "SET LOCAL auto_explain.log_analyze = on",
                            "SET LOCAL auto_explain.log_buffers = on",
                            "SET LOCAL auto_explain.log_timing = off",
                            "SET LOCAL auto_explain.log_nested_statements = on",
                            "SET LOCAL auto_explain.log_format = 'json'",
                        ] {
                            sqlx::query(statement)
                                .execute(&mut *tx)
                                .await
                                .expect("configure nested plan capture");
                        }
                        sqlx::query(sqlx::AssertSqlSafe(subscription_plan_marker(
                            "START",
                            rows,
                            subscriptions,
                            analyzed,
                            round,
                        )))
                        .execute(&mut *tx)
                        .await
                        .expect("mark plan capture start");
                    }
                    let counters = "SELECT pg_stat_get_xact_tuples_returned($1::text::regclass)::bigint, \
                    pg_stat_get_xact_tuples_fetched($1::text::regclass)::bigint, \
                    pg_stat_get_xact_tuples_returned($2::text::regclass)::bigint, \
                    pg_stat_get_xact_tuples_fetched($2::text::regclass)::bigint";
                    let before: (i64, i64, i64, i64) = sqlx::query_as(counters)
                        .bind(&stream)
                        .bind(&source)
                        .fetch_one(&mut *tx)
                        .await
                        .expect("read initial tuple counters");
                    let start = Instant::now();
                    sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
                        .bind(&stream)
                        .execute(&mut *tx)
                        .await
                        .expect("refresh subscription stream");
                    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                    if plan_log.is_some() {
                        sqlx::query(sqlx::AssertSqlSafe(subscription_plan_marker(
                            "END",
                            rows,
                            subscriptions,
                            analyzed,
                            round,
                        )))
                        .execute(&mut *tx)
                        .await
                        .expect("mark plan capture end");
                    }
                    let after: (i64, i64, i64, i64) = sqlx::query_as(counters)
                        .bind(&stream)
                        .bind(&source)
                        .fetch_one(&mut *tx)
                        .await
                        .expect("read final tuple counters");
                    let (snapshot, ids) = if subscriptions == 0 {
                        (None, None)
                    } else {
                        let snapshot: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                            "SELECT count(*) FROM pg_temp.__pgt_distance_pre_{pgt_id}"
                        )))
                        .fetch_one(&mut *tx)
                        .await
                        .expect("inspect affected snapshot");
                        let ids: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                            "SELECT count(*) FROM pg_temp.__pgt_distance_ids_{pgt_id}"
                        )))
                        .fetch_one(&mut *tx)
                        .await
                        .expect("inspect affected identities");
                        assert_eq!(
                            (snapshot, ids),
                            (1, 1),
                            "one changed row must use the bounded snapshot"
                        );
                        (Some(snapshot), Some(ids))
                    };
                    let (differential, strategy, refresh_id): (bool, String, i64) = sqlx::query_as("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback, h.merge_strategy_used, h.refresh_id FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id=$1 ORDER BY h.refresh_id DESC LIMIT 1")
                    .bind(pgt_id).fetch_one(&mut *tx).await.expect("inspect measured strategy");
                    assert!(differential, "measurement must use differential refresh");
                    if force_explicit {
                        assert_eq!(
                            strategy, "explicit_dml",
                            "all control cases must use the same merge path"
                        );
                    }
                    let commit_start = Instant::now();
                    tx.commit().await.expect("commit measured refresh");
                    let commit_ms = commit_start.elapsed().as_secs_f64() * 1000.0;
                    let delivery_start = Instant::now();
                    let mut channels = HashSet::new();
                    for _ in 0..subscriptions {
                        let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
                            .await
                            .expect("wait for measured transition")
                            .expect("receive measured transition");
                        assert!(
                            channels.insert(event.channel().to_string()),
                            "duplicate subscription event"
                        );
                        let payload: serde_json::Value = serde_json::from_str(event.payload())
                            .expect("parse transition payload");
                        assert_eq!(payload["refresh_id"], refresh_id);
                        assert_eq!(payload["entered"], if round % 2 == 0 { 0 } else { 1 });
                        assert_eq!(payload["exited"], if round % 2 == 0 { 1 } else { 0 });
                    }
                    assert_eq!(channels.len(), subscriptions);
                    let delivery_ms = (subscriptions > 0)
                        .then(|| delivery_start.elapsed().as_secs_f64() * 1000.0);
                    let correct: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT s.embedding::text = d.embedding::text FROM {stream} s JOIN {source} d USING(id) WHERE s.id=1")))
                    .fetch_one(&db.pool).await.expect("check changed result");
                    assert!(
                        correct,
                        "measured refresh must propagate the changed vector"
                    );
                    let tuple_delta = [
                        after.0 - before.0,
                        after.1 - before.1,
                        after.2 - before.2,
                        after.3 - before.3,
                    ];
                    assert!(tuple_delta.iter().all(|value| *value >= 0));
                    samples.push(serde_json::json!({"round":round,"refresh_ms":elapsed_ms,"commit_ms":commit_ms,"notification_wait_ms":delivery_ms,"merge_strategy":strategy,
                    "stream_sequential_tuples_returned":tuple_delta[0],"stream_index_tuples_fetched":tuple_delta[1],
                    "source_sequential_tuples_returned":tuple_delta[2],"source_index_tuples_fetched":tuple_delta[3],
                    "snapshot_rows":snapshot,"affected_identities":ids}));
                }
                measurements.push(serde_json::json!({"initial_rows":rows,"subscriptions":subscriptions,"analyzed":analyzed,"estimated_rows":estimated_rows,"last_analyze":last_analyze,"last_autoanalyze":last_autoanalyze,"samples":samples}));
            }
        }
    }
    if let Some(log) = &plan_log {
        log.capture().expect("preserve subscription plan log");
    }
    println!(
        "PGVECTOR_SUBSCRIPTION_BENCHMARK_JSON={}",
        serde_json::json!({"postgres":versions.0,"pgvector":versions.1,"pg_trickle":versions.2,"dimensions":dimensions,"force_explicit_with_noop_trigger":force_explicit,"nested_plan_capture":plan_log.is_some(),"measurements":measurements,"notes":"One row alternates across the threshold; each subscription emits its expected event. Refresh, commit, and post-commit listener wait are timed separately; notification delivery can begin before the listener wait. Tuple counters include repeated visits, not distinct rows. Plan instrumentation adds overhead. No controlled concurrency is recorded."})
    );
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
    let centroid_samples: usize = std::env::var("PGVECTOR_CENTROID_SAMPLES")
        .unwrap_or_else(|_| "5".into())
        .parse()
        .expect("centroid samples");
    let centroid_rewrite_rows: usize = std::env::var("PGVECTOR_CENTROID_REWRITE_ROWS")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .expect("centroid rewrite rows");
    assert!(
        (1..=2000).contains(&dim)
            && rows >= 100
            && (1..=rows).contains(&samples)
            && (1..=50).contains(&centroid_samples)
            && (1..=rows.saturating_sub(rows / 10 + 2)).contains(&centroid_rewrite_rows)
    );
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
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.vector_bench_corpus'])")
        .await;
    db.execute("ANALYZE vector_bench_corpus").await;
    db.execute(
        "SELECT pgtrickle.create_stream_table('vector_bench_full', \
        'SELECT id, tenant, embedding FROM vector_bench_source', '1h', 'FULL')",
    )
    .await;
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.vector_bench_full'])")
        .await;
    let versions: (String, String, String) = sqlx::query_as(
        "SELECT version(), \
        (SELECT extversion FROM pg_extension WHERE extname = 'vector'), \
        (SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("versions");
    require_pgvector_version(&versions.1);
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
        let mut exact_plan = serde_json::Value::Null;
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
            if id == 1 {
                let explain = format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {exact_sql}");
                exact_plan = sqlx::query_scalar(sqlx::AssertSqlSafe(explain))
                    .bind(tenant)
                    .bind(&query)
                    .fetch_one(&mut *tx)
                    .await
                    .expect("exact baseline plan");
                assert!(
                    has_source_seq_scan(&exact_plan, "vector_bench_corpus"),
                    "exact baseline did not scan the corpus: {exact_plan}"
                );
                assert!(
                    !has_index(&exact_plan, "vector_bench_ann"),
                    "exact baseline used the ANN index: {exact_plan}"
                );
            }
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
            "index_bytes": index_bytes, "corpus_total_bytes_including_indexes": corpus_bytes, "ann_plan": recorded_plan, "exact_plan": exact_plan}));
        db.execute("DROP INDEX vector_bench_ann").await;
    }
    db.execute(&format!(
        "CREATE INDEX vector_bench_ann ON vector_bench_corpus USING hnsw \
        ((embedding::vector({dim})) vector_cosine_ops)"
    ))
    .await;
    db.execute(&format!(
        "CREATE INDEX vector_bench_full_ann ON vector_bench_full USING hnsw \
        ((embedding::vector({dim})) vector_cosine_ops)"
    ))
    .await;
    let mut refresh_ms = Vec::new();
    let mut wal_bytes = Vec::new();
    let mut full_refresh_ms = Vec::new();
    let mut full_wal_bytes = Vec::new();
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
        let before_full: String = db
            .query_scalar("SELECT pg_current_wal_insert_lsn()::text")
            .await;
        let start_full = Instant::now();
        db.refresh_st("vector_bench_full").await;
        full_refresh_ms.push(start_full.elapsed().as_secs_f64() * 1000.0);
        let full_written: f64 = sqlx::query_scalar(
            "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::text::pg_lsn)::float8",
        )
        .bind(before_full)
        .fetch_one(&db.pool)
        .await
        .expect("full refresh WAL delta");
        full_wal_bytes.push(full_written);
        let full: bool = db
            .query_scalar(
                "SELECT h.action = 'FULL' AND h.status = 'COMPLETED' \
            FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
            WHERE s.pgt_name = 'vector_bench_full' ORDER BY refresh_id DESC LIMIT 1",
            )
            .await;
        assert!(full, "expected completed full refresh");
        db.assert_st_matches_query(
            "vector_bench_full",
            "SELECT id, tenant, embedding FROM vector_bench_source",
        )
        .await;
    }
    db.execute(&format!("CREATE TABLE vector_centroid_source (id bigint PRIMARY KEY, small_group int NOT NULL, large_group int NOT NULL, embedding vector({dim}) NOT NULL)")).await;
    db.execute("INSERT INTO vector_centroid_source SELECT id, (id % 100)::int, (id % 2)::int, embedding FROM vector_bench_source").await;
    db.execute("CREATE INDEX vector_centroid_small_group ON vector_centroid_source (small_group)")
        .await;
    db.execute("CREATE INDEX vector_centroid_large_group ON vector_centroid_source (large_group)")
        .await;
    db.execute("ANALYZE vector_centroid_source").await;
    for group in ["small", "large"] {
        let query = format!(
            "SELECT {group}_group, avg(embedding) AS centroid FROM vector_centroid_source GROUP BY {group}_group"
        );
        for (label, mode, algebraic) in [
            ("differential", "DIFFERENTIAL", false),
            ("algebraic", "DIFFERENTIAL", true),
            ("full", "FULL", false),
        ] {
            let stream = format!("vector_centroid_{group}_{label}");
            let create = format!(
                "SELECT pgtrickle.create_stream_table('{stream}', $${query}$$, '1h', '{mode}')"
            );
            let centroid_setting = if algebraic {
                "SET pg_trickle.enable_centroid_state = on"
            } else {
                "SET pg_trickle.enable_centroid_state = off"
            };
            let pause = format!("SELECT pgtrickle.pause_scheduler(ARRAY['public.{stream}'])");
            db.execute_seq(&[
                "SET pg_trickle.enable_vector_agg = on",
                centroid_setting,
                &create,
                &pause,
            ])
            .await;
        }
    }
    for group in ["small", "large"] {
        let stream = format!("vector_centroid_{group}_algebraic");
        let reset = format!(
            "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = '{stream}'::regclass"
        );
        let refresh = format!("SELECT pgtrickle.refresh_stream_table('{stream}')");
        db.execute_seq(&[
            "SET pg_trickle.enable_vector_agg = on",
            "SET pg_trickle.enable_centroid_state = on",
            &reset,
            &refresh,
        ])
        .await;
        let bound: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = $1::regclass AND c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL AND c.contract_generation = t.contract_generation)")
            .bind(&stream).fetch_one(&db.pool).await.expect("inspect centroid benchmark baseline");
        assert!(bound, "centroid baseline was not bound for {stream}");
    }
    let target = rows - 1;
    db.execute(&format!("UPDATE vector_centroid_source SET embedding = (SELECT embedding FROM vector_centroid_source WHERE id = {rows}) WHERE id = {target}")).await;
    let mut centroid = Vec::new();
    for group in ["small", "large"] {
        let group_value = if group == "small" {
            target % 100
        } else {
            target % 2
        };
        let explanation: String = db
            .query_scalar(&format!(
                "SELECT pgtrickle.explain_stream_table('vector_centroid_{group}_differential')"
            ))
            .await;
        assert!(
            explanation.contains("centroid: GROUP_RESCAN"),
            "centroid strategy is not group rescan: {explanation}"
        );
        let group_rows: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM vector_centroid_source WHERE {group}_group = {group_value}"
            ))
            .await;
        let explain = format!(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT avg(embedding) FROM vector_centroid_source WHERE {group}_group = {group_value}"
        );
        let group_plan: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(explain))
            .fetch_one(&db.pool)
            .await
            .expect("explain representative group rescan");
        let mut measurements = Vec::new();
        for (label, mode, algebraic) in [
            ("group_rescan", "DIFFERENTIAL", false),
            ("algebraic", "DIFFERENTIAL", true),
            ("full", "FULL", false),
        ] {
            let suffix = if label == "group_rescan" {
                "differential"
            } else {
                label
            };
            let stream = format!("vector_centroid_{group}_{suffix}");
            let before: String = db
                .query_scalar("SELECT pg_current_wal_insert_lsn()::text")
                .await;
            let start = Instant::now();
            let refresh = format!(
                "EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) SELECT pgtrickle.refresh_stream_table('{stream}')"
            );
            let mut conn = db
                .pool
                .acquire()
                .await
                .expect("benchmark centroid connection");
            for setting in [
                "SET pg_trickle.enable_vector_agg = on",
                "SET pg_trickle.differential_max_change_ratio = 0",
                if algebraic {
                    "SET pg_trickle.enable_centroid_state = on"
                } else {
                    "SET pg_trickle.enable_centroid_state = off"
                },
            ] {
                sqlx::query(setting)
                    .execute(&mut *conn)
                    .await
                    .expect("configure centroid benchmark");
            }
            sqlx::query("BEGIN")
                .execute(&mut *conn)
                .await
                .expect("begin centroid measurement");
            let before_source: (i64, i64) = sqlx::query_as(
                "SELECT COALESCE(pg_stat_get_xact_tuples_returned('vector_centroid_source'::regclass), 0)::bigint, \
                        COALESCE(pg_stat_get_xact_tuples_fetched('vector_centroid_source'::regclass), 0)::bigint",
            )
            .fetch_one(&mut *conn)
            .await
            .expect("read initial source tuple counters");
            let refresh_plan: serde_json::Value =
                sqlx::query_scalar(sqlx::AssertSqlSafe(refresh.as_str()))
                    .fetch_one(&mut *conn)
                    .await
                    .expect("explain and refresh centroid");
            let after_source: (i64, i64) = sqlx::query_as(
                "SELECT COALESCE(pg_stat_get_xact_tuples_returned('vector_centroid_source'::regclass), 0)::bigint, \
                        COALESCE(pg_stat_get_xact_tuples_fetched('vector_centroid_source'::regclass), 0)::bigint",
            )
            .fetch_one(&mut *conn)
            .await
            .expect("read final source tuple counters");
            sqlx::query("COMMIT")
                .execute(&mut *conn)
                .await
                .expect("commit centroid measurement");
            let source_sequential_tuples = after_source.0 - before_source.0;
            let source_index_tuples = after_source.1 - before_source.1;
            assert!(source_sequential_tuples >= 0 && source_index_tuples >= 0);
            let source_tuples = source_sequential_tuples + source_index_tuples;
            if algebraic {
                assert_eq!(source_tuples, 0, "algebraic refresh scanned source history");
            } else {
                assert!(
                    source_tuples > 0,
                    "source scan counters did not record {label}"
                );
            }
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            let wal_bytes: f64 = sqlx::query_scalar(
                "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::text::pg_lsn)::float8",
            )
            .bind(before)
            .fetch_one(&db.pool)
            .await
            .expect("centroid WAL delta");
            let history: (String, bool, String) = sqlx::query_as("SELECT h.action::text, h.was_full_fallback, h.merge_strategy_used FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name=$1 AND h.status='COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
                .bind(&stream).fetch_one(&db.pool).await.expect("centroid refresh history");
            assert_eq!(history.0, mode);
            assert!(!history.1, "centroid refresh fell back to full");
            if algebraic {
                assert_eq!(
                    history.2, "centroid_algebraic",
                    "algebraic refresh did not run"
                );
            }
            let numerical = format!(
                "WITH native AS (SELECT {group}_group, avg(embedding) AS centroid FROM vector_centroid_source GROUP BY {group}_group) SELECT COALESCE(max(abs(x.actual::float8 - x.expected::float8)), 0)::float8, COALESCE(bool_and({CENTROID_COMPONENT_WITHIN_BOUND}), false) AND count(*) = (SELECT count(*) FROM native) * {dim} AND (SELECT count(*) FROM {stream}) = (SELECT count(*) FROM native) AND count(DISTINCT s.{group}_group) = (SELECT count(*) FROM native) FROM {stream} s JOIN native n USING ({group}_group) CROSS JOIN LATERAL unnest(s.centroid::real[], n.centroid::real[]) AS x(actual, expected)"
            );
            let (max_error, within_bound): (f64, bool) =
                sqlx::query_as(sqlx::AssertSqlSafe(numerical.as_str()))
                    .fetch_one(&db.pool)
                    .await
                    .expect("compare centroid numerical error");
            assert!(
                within_bound,
                "{group} {label}: centroid output exceeded its numerical bound or has missing components (max error {max_error})"
            );
            if !algebraic {
                let centroid_expr = if mode == "FULL" {
                    "avg(embedding)".to_string()
                } else {
                    format!("avg(embedding)::vector({dim})")
                };
                db.assert_st_matches_query(&stream, &format!("SELECT {group}_group, {centroid_expr} AS centroid FROM vector_centroid_source GROUP BY {group}_group")).await;
            }
            let storage_bytes: i64 =
                sqlx::query_scalar("SELECT pg_total_relation_size($1::regclass)::bigint")
                    .bind(&stream)
                    .fetch_one(&db.pool)
                    .await
                    .expect("centroid storage size");
            let state_bytes: i64 = sqlx::query_scalar("SELECT COALESCE(sum(octet_length(g.row_id) + octet_length(pg_catalog.array_send(g.state)) + octet_length(g.state_checksum) + 16), 0)::bigint FROM pgtrickle.pgt_centroid_groups g JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = $1::regclass")
                .bind(&stream).fetch_one(&db.pool).await.expect("centroid logical state size");
            measurements.push(serde_json::json!({"strategy": label, "mode": mode, "actual_strategy": history.2, "elapsed_ms": elapsed_ms, "wal_bytes_cluster_wide": wal_bytes, "storage_bytes_including_indexes": storage_bytes, "auxiliary_state_logical_bytes": state_bytes, "max_absolute_component_error": max_error, "source_sequential_tuples_returned": source_sequential_tuples, "source_index_tuples_fetched": source_index_tuples, "refresh_plan": refresh_plan}));
        }
        centroid.push(serde_json::json!({"group_shape": group, "changed_rows": 1, "affected_group_rows": group_rows, "representative_group_query_plan": group_plan, "refresh": measurements}));
    }
    let mut repeated = Vec::new();
    let mut timings = vec![vec![Vec::<f64>::new(); 3]; 2];
    let groups = ["small", "large"];
    let variants = ["differential", "algebraic", "full"];
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire repeated benchmark session");
    for setting in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
    ] {
        sqlx::query(setting)
            .execute(&mut *conn)
            .await
            .expect("configure repeated centroid benchmark");
    }
    let first_rewritten = rows - centroid_rewrite_rows;
    let alternate_source = rows / 10 + 1;
    let distinct_inputs: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT public.vector_send((SELECT embedding FROM vector_centroid_source WHERE id = {alternate_source})) <> public.vector_send((SELECT embedding FROM vector_centroid_source WHERE id = {rows}))"
    )))
    .fetch_one(&mut *conn)
    .await
    .expect("check repeated centroid inputs");
    assert!(distinct_inputs, "repeated source vectors must be distinct");
    for round in 0..centroid_samples {
        let source_id = if round % 2 == 0 {
            alternate_source
        } else {
            rows
        };
        let mutation = format!(
            "UPDATE vector_centroid_source SET embedding = (SELECT embedding FROM vector_centroid_source WHERE id = {source_id}) WHERE id >= {first_rewritten} AND id < {rows}"
        );
        let updated = sqlx::query(sqlx::AssertSqlSafe(mutation))
            .execute(&mut *conn)
            .await
            .expect("mutate repeated centroid source");
        assert_eq!(updated.rows_affected(), centroid_rewrite_rows as u64);
        for group_offset in 0..groups.len() {
            let group_idx = (group_offset + round) % groups.len();
            let group = groups[group_idx];
            for offset in 0..variants.len() {
                let variant_idx = (offset + round) % variants.len();
                let variant = variants[variant_idx];
                let stream = format!("vector_centroid_{group}_{variant}");
                sqlx::query(if variant == "algebraic" {
                    "SET pg_trickle.enable_centroid_state = on"
                } else {
                    "SET pg_trickle.enable_centroid_state = off"
                })
                .execute(&mut *conn)
                .await
                .expect("select repeated centroid strategy");
                let start = Instant::now();
                sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
                    .bind(&stream)
                    .execute(&mut *conn)
                    .await
                    .expect("repeat centroid refresh");
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                let history: (String, bool, String) = sqlx::query_as("SELECT h.action::text, h.was_full_fallback, h.merge_strategy_used FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = $1 AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
                    .bind(&stream).fetch_one(&mut *conn).await.expect("inspect repeated centroid strategy");
                assert_eq!(
                    history.0,
                    if variant == "full" {
                        "FULL"
                    } else {
                        "DIFFERENTIAL"
                    }
                );
                assert!(!history.1, "repeated centroid refresh fell back to FULL");
                if variant == "algebraic" && history.2 != "centroid_algebraic" {
                    let detail: String = sqlx::query_scalar("SELECT format('baseline=%s frontier=%s materialized_xid=%s rebase_marker=%s history=%s', COALESCE(c.baseline_frontier::text, 'null'), COALESCE(t.frontier::text, 'null'), COALESCE(c.materialized_xid::text, 'null'), COALESCE(c.last_change_limit_rebase::text, 'null'), COALESCE(h.refresh_reason_detail, 'null')) FROM pgtrickle.pgt_stream_tables t LEFT JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) LEFT JOIN LATERAL (SELECT refresh_reason_detail FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_name = $1")
                        .bind(&stream).fetch_one(&mut *conn).await.expect("inspect unexpected centroid fallback");
                    panic!(
                        "round {round} {group} expected algebraic maintenance, got {}: {detail}",
                        history.2
                    );
                }
                let comparison = format!(
                    "WITH affected AS (SELECT DISTINCT {group}_group FROM vector_centroid_source WHERE id >= {first_rewritten} AND id < {rows}), \
                     native AS (SELECT {group}_group, avg(embedding)::real[] AS centroid FROM vector_centroid_source WHERE {group}_group IN (SELECT {group}_group FROM affected) GROUP BY {group}_group) \
                     SELECT count(*)::bigint, (SELECT count(*) FROM native)::bigint, count(DISTINCT s.{group}_group)::bigint, \
                       COALESCE(max(abs(x.actual::float8 - x.expected::float8)), 0)::float8, \
                       COALESCE(bool_and({CENTROID_COMPONENT_WITHIN_BOUND}), false) \
                     FROM {stream} s JOIN native n USING ({group}_group) CROSS JOIN LATERAL unnest(s.centroid::real[], n.centroid) x(actual, expected)"
                );
                let (components, expected_groups, actual_groups, max_error, within_bound): (
                    i64,
                    i64,
                    i64,
                    f64,
                    bool,
                ) = sqlx::query_as(sqlx::AssertSqlSafe(comparison))
                    .fetch_one(&mut *conn)
                    .await
                    .expect("compare repeated centroid refresh");
                assert!(
                    components == expected_groups * dim as i64
                        && actual_groups == expected_groups
                        && within_bound,
                    "round {round} {group} {variant}: components={components}, expected_groups={expected_groups}, actual_groups={actual_groups}, max_error={max_error}"
                );
                timings[group_idx][variant_idx].push(elapsed_ms);
                repeated.push(serde_json::json!({"round": round, "group_shape": group, "strategy": variant, "actual_strategy": history.2, "elapsed_ms": elapsed_ms, "max_absolute_component_error": max_error}));
            }
        }
    }
    let repeated_summary = groups
        .iter()
        .enumerate()
        .flat_map(|(group_idx, group)| {
            let timings = &timings;
            variants
                .iter()
                .enumerate()
                .map(move |(variant_idx, variant)| {
                    serde_json::json!({
                        "group_shape": group, "strategy": variant,
                        "refresh_ms": percentiles(timings[group_idx][variant_idx].clone())
                    })
                })
        })
        .collect::<Vec<_>>();
    println!(
        "PGVECTOR_BENCHMARK_JSON={}",
        serde_json::json!({"schema": 1, "postgres": versions.0,
        "pgvector": versions.1, "pg_trickle": versions.2, "host_arch": std::env::consts::ARCH,
        "data": "md5(id:dimension), seedless deterministic", "rows": rows, "dimensions": dim,
        "ann": results, "centroid_baseline": centroid, "centroid_repeated": {"rounds": centroid_samples, "changed_rows_per_round": centroid_rewrite_rows, "observations": repeated, "summary": repeated_summary, "notes": "Each round changes the configured number of source vectors; group and strategy order rotate. All affected groups are checked against native AVG. Times cover the refresh call without EXPLAIN, mutation or validation. No warmup, concurrent load, or repeated rebase measurements."}, "centroid_notes": "Each refresh is executed under EXPLAIN ANALYZE; SPI work may not be individually visible in the plan. Source tuple counters cover same-transaction sequential returns and index fetches, including repeated visits, during the refresh. Group rescan, algebraic and FULL run in that order. WAL excludes the source mutation; auxiliary bytes are logical payload estimates.", "refresh": {"operations": ["update embedding", "update embedding", "delete"],
        "changed_rows": [1, rows / 10, rows / 10], "elapsed_ms": refresh_ms,
        "wal_bytes_cluster_wide": wal_bytes,
        "full_elapsed_ms": full_refresh_ms, "full_wal_bytes_cluster_wide": full_wal_bytes,
        "notes": "Separate equivalent HNSW indexes; differential always runs first; WAL is cluster-wide between LSN reads and excludes source mutations"}})
    );
}
