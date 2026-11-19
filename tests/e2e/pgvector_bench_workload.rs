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

#[cfg(not(target_os = "linux"))]
async fn reset_centroid_cache(_db: &mut E2eDb) -> f64 {
    panic!("cold-cache centroid measurements require a Linux Docker runner");
}

#[cfg(target_os = "linux")]
async fn reset_centroid_cache(db: &mut E2eDb) -> f64 {
    assert_eq!(
        std::env::var("GITHUB_ACTIONS").as_deref(),
        Ok("true"),
        "host page-cache clearing is limited to the disposable GitHub Actions runner"
    );
    let started = Instant::now();
    db.pool.close().await;
    let stop = tokio::process::Command::new("docker")
        .args(["stop", "--time", "1", db.container_id()])
        .output()
        .await
        .expect("restart isolated PostgreSQL container for cold-cache sample");
    assert!(
        stop.status.success(),
        "stop PostgreSQL for cold-cache sample failed: {}",
        String::from_utf8_lossy(&stop.stderr)
    );
    let drop_cache = tokio::process::Command::new("sudo")
        .args([
            "-n",
            "sh",
            "-c",
            "sync && echo 3 > /proc/sys/vm/drop_caches",
        ])
        .output()
        .await
        .expect("drop host page cache for cold-cache sample");
    let start = tokio::process::Command::new("docker")
        .args(["start", db.container_id()])
        .output()
        .await
        .expect("start PostgreSQL after cold-cache reset");
    assert!(
        start.status.success(),
        "start PostgreSQL after cold-cache reset failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    db.pool = db.reconnect_after_restart().await;
    assert!(
        drop_cache.status.success(),
        "drop host page cache failed: {}",
        String::from_utf8_lossy(&drop_cache.stderr)
    );
    started.elapsed().as_secs_f64() * 1000.0
}

async fn centroid_refresh_with_writer_lock(
    db: &E2eDb,
    connection: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    refresh_sql: &str,
    writer_id: i64,
) -> (serde_json::Value, f64, f64, f64) {
    sqlx::query("SET application_name = 'pgvector-centroid-refresh'")
        .execute(&mut **connection)
        .await
        .expect("label centroid refresh session");
    let refresh_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut **connection)
        .await
        .expect("read centroid refresh backend");
    let mut writer = db.pool.acquire().await.expect("acquire centroid writer");
    sqlx::query("SET application_name = 'pgvector-centroid-writer'")
        .execute(&mut *writer)
        .await
        .expect("label centroid writer session");
    let writer_started = Instant::now();
    sqlx::query("BEGIN")
        .execute(&mut *writer)
        .await
        .expect("begin controlled centroid writer");
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .expect("read centroid writer backend");
    let writer_lock_started = Instant::now();
    // shortcut: toggle an unprojected key only; broaden writes before claiming sustained-writer performance.
    let update = format!(
        "UPDATE vector_centroid_source SET id = CASE id WHEN {writer_id} THEN {next_writer_id} ELSE {writer_id} END WHERE id IN ({writer_id}, {next_writer_id})",
        next_writer_id = writer_id + 1
    );
    sqlx::query(sqlx::AssertSqlSafe(update))
        .execute(&mut *writer)
        .await
        .expect("update controlled centroid writer row");

    let refresh_started = Instant::now();
    let refresh =
        sqlx::query_scalar::<_, serde_json::Value>(sqlx::AssertSqlSafe(refresh_sql.to_owned()))
            .fetch_one(&mut **connection);
    tokio::pin!(refresh);
    let minimum_hold = Duration::from_millis(10);
    let mut last_poll = refresh_started;
    let mut observed_lock_wait = Duration::ZERO;
    let mut writer_committed = false;
    let plan = loop {
        tokio::select! {
            result = &mut refresh => {
                break result.expect("run centroid refresh with writer lock");
            }
            _ = tokio::time::sleep(Duration::from_millis(1)) => {
                let now = Instant::now();
                let is_blocked: bool = sqlx::query_scalar(
                    "SELECT $1 = ANY(pg_catalog.pg_blocking_pids($2))",
                )
                .bind(writer_pid)
                .bind(refresh_pid)
                .fetch_one(&db.pool)
                .await
                .expect("inspect controlled centroid writer lock");
                if is_blocked {
                    observed_lock_wait += now.saturating_duration_since(last_poll);
                }
                last_poll = now;
                if observed_lock_wait >= minimum_hold {
                    sqlx::query("COMMIT")
                        .execute(&mut *writer)
                        .await
                        .expect("release controlled centroid writer lock");
                    writer_committed = true;
                    break refresh.await.expect("run centroid refresh after writer commit");
                }
            }
        }
    };
    if !writer_committed {
        let hold_elapsed = if observed_lock_wait.is_zero() {
            writer_lock_started.elapsed()
        } else {
            observed_lock_wait
        };
        tokio::time::sleep(minimum_hold.saturating_sub(hold_elapsed)).await;
        sqlx::query("COMMIT")
            .execute(&mut *writer)
            .await
            .expect("commit controlled centroid writer");
    }
    (
        plan,
        refresh_started.elapsed().as_secs_f64() * 1000.0,
        observed_lock_wait.as_secs_f64() * 1000.0,
        writer_started.elapsed().as_secs_f64() * 1000.0,
    )
}

fn centroid_plan_buffers(explain: &serde_json::Value) -> serde_json::Value {
    let root = explain
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item.get("Plan"))
        .unwrap_or(explain);
    serde_json::json!({
        "shared_hit_blocks": root.get("Shared Hit Blocks").and_then(serde_json::Value::as_i64).unwrap_or(0),
        "shared_read_blocks": root.get("Shared Read Blocks").and_then(serde_json::Value::as_i64).unwrap_or(0),
        "shared_dirtied_blocks": root.get("Shared Dirtied Blocks").and_then(serde_json::Value::as_i64).unwrap_or(0),
        "shared_written_blocks": root.get("Shared Written Blocks").and_then(serde_json::Value::as_i64).unwrap_or(0)
    })
}

#[test]
fn centroid_plan_buffers_reads_root_buffer_counts() {
    let plan = serde_json::json!([{"Plan": {
        "Shared Hit Blocks": 7,
        "Shared Read Blocks": 3,
        "Shared Dirtied Blocks": 2,
        "Shared Written Blocks": 1
    }}]);
    assert_eq!(
        centroid_plan_buffers(&plan),
        serde_json::json!({
            "shared_hit_blocks": 7,
            "shared_read_blocks": 3,
            "shared_dirtied_blocks": 2,
            "shared_written_blocks": 1
        })
    );
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
    let pivots: usize = std::env::var("PGVECTOR_SUB_PIVOTS")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .expect("PGVECTOR_SUB_PIVOTS must be an integer");
    assert!(
        (1..=1000).contains(&pivots),
        "PGVECTOR_SUB_PIVOTS must be between 1 and 1000"
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
    let mutation_shape =
        std::env::var("PGVECTOR_SUB_MUTATION_SHAPE").unwrap_or_else(|_| "single_update".into());
    assert!(
        ["single_update", "small_batch", "repeated_updates_delete"]
            .contains(&mutation_shape.as_str()),
        "PGVECTOR_SUB_MUTATION_SHAPE must be single_update, small_batch, or repeated_updates_delete"
    );
    let batch_rows: usize = std::env::var("PGVECTOR_SUB_BATCH_ROWS")
        .unwrap_or_else(|_| "10".into())
        .parse()
        .expect("PGVECTOR_SUB_BATCH_ROWS must be an integer");
    assert!((1..=1000).contains(&batch_rows));
    if mutation_shape == "small_batch" {
        assert!(batch_rows * 2 <= row_counts.iter().copied().max().unwrap_or(0));
    }
    assert!(pivots < dimensions);
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
                "INSERT INTO {source} SELECT i, CASE WHEN i % 2 = 0 THEN '{first}'::vector ELSE '{second}'::vector END FROM generate_series(1,{rows}) i"
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
                    let pivot_axis = 1 + index % pivots;
                    let mut values = vec![0.0; dimensions];
                    values[0] = 1.0;
                    values[pivot_axis] = 0.1;
                    let values = values
                        .into_iter()
                        .map(|value| format!("{value:.4}"))
                        .collect::<Vec<_>>();
                    let pivot = format!("[{}]", values.join(","));
                    let threshold = if index % 2 == 0 { 0.5 } else { 2.0 };
                    db.execute(&format!("SELECT pgtrickle.subscribe_distance('{stream}', '{channel}', 'embedding', '{pivot}', '<->', {threshold})")).await;
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
                    let target = if round % 2 == 0 { &first } else { &second };
                    let other = if round % 2 == 0 { &second } else { &first };
                    let source_wal_before: String =
                        sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
                            .fetch_one(&db.pool)
                            .await
                            .expect("read source-change WAL position");
                    let mutation_start = Instant::now();
                    let changed_rows = match mutation_shape.as_str() {
                        "single_update" => {
                            db.execute(&format!(
                                "UPDATE {source} SET embedding='{target}' WHERE id=1"
                            ))
                            .await;
                            1
                        }
                        "small_batch" => {
                            let last_id = batch_rows * 2 - 1;
                            db.execute(&format!(
                                "UPDATE {source} SET embedding='{target}' \
                                 WHERE id <= {last_id} AND id % 2 = 1"
                            ))
                            .await;
                            batch_rows
                        }
                        "repeated_updates_delete" => {
                            for vector in [target, other, target] {
                                db.execute(&format!(
                                    "UPDATE {source} SET embedding='{vector}' WHERE id=1"
                                ))
                                .await;
                            }
                            if round == 0 {
                                db.execute(&format!("DELETE FROM {source} WHERE id=2"))
                                    .await;
                                2
                            } else {
                                1
                            }
                        }
                        _ => unreachable!("mutation shape was validated"),
                    };
                    let source_mutation_ms = mutation_start.elapsed().as_secs_f64() * 1000.0;
                    let source_wal_bytes: i64 = sqlx::query_scalar(
                        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint",
                    )
                    .bind(&source_wal_before)
                    .fetch_one(&db.pool)
                    .await
                    .expect("measure source-change WAL bytes");
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
                    let buffer_counters = "SELECT COALESCE(s.heap_blks_read, 0)::bigint, \
                    COALESCE(s.heap_blks_hit, 0)::bigint, COALESCE(s.idx_blks_read, 0)::bigint, \
                    COALESCE(s.idx_blks_hit, 0)::bigint, COALESCE(d.heap_blks_read, 0)::bigint, \
                    COALESCE(d.heap_blks_hit, 0)::bigint, COALESCE(d.idx_blks_read, 0)::bigint, \
                    COALESCE(d.idx_blks_hit, 0)::bigint FROM pg_statio_user_tables s \
                    CROSS JOIN pg_statio_user_tables d WHERE s.relid=$1::text::regclass \
                    AND d.relid=$2::text::regclass";
                    let before: (i64, i64, i64, i64) = sqlx::query_as(counters)
                        .bind(&stream)
                        .bind(&source)
                        .fetch_one(&mut *tx)
                        .await
                        .expect("read initial tuple counters");
                    let buffers_before: (i64, i64, i64, i64, i64, i64, i64, i64) =
                        sqlx::query_as(buffer_counters)
                            .bind(&stream)
                            .bind(&source)
                            .fetch_one(&mut *tx)
                            .await
                            .expect("read initial relation buffer counters");
                    let wal_before: String =
                        sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
                            .fetch_one(&mut *tx)
                            .await
                            .expect("read refresh WAL position");
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
                    sqlx::query("SELECT pg_stat_clear_snapshot()")
                        .execute(&mut *tx)
                        .await
                        .expect("refresh relation-statistics snapshot");
                    let buffers_after: (i64, i64, i64, i64, i64, i64, i64, i64) =
                        sqlx::query_as(buffer_counters)
                            .bind(&stream)
                            .bind(&source)
                            .fetch_one(&mut *tx)
                            .await
                            .expect("read final relation buffer counters");
                    let buffer_delta = [
                        buffers_after.0 - buffers_before.0,
                        buffers_after.1 - buffers_before.1,
                        buffers_after.2 - buffers_before.2,
                        buffers_after.3 - buffers_before.3,
                        buffers_after.4 - buffers_before.4,
                        buffers_after.5 - buffers_before.5,
                        buffers_after.6 - buffers_before.6,
                        buffers_after.7 - buffers_before.7,
                    ];
                    assert!(buffer_delta.iter().all(|value| *value >= 0));
                    let (snapshot, ids, snapshot_kind) = if subscriptions == 0 {
                        (None, None, "none")
                    } else {
                        let snapshot: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                            "SELECT count(*) FROM pg_temp.__pgt_distance_pre_{pgt_id}"
                        )))
                        .fetch_one(&mut *tx)
                        .await
                        .expect("inspect affected snapshot");
                        let ids_name = format!("pg_temp.__pgt_distance_ids_{pgt_id}");
                        let bounded: bool =
                            sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                                .bind(&ids_name)
                                .fetch_one(&mut *tx)
                                .await
                                .expect("inspect snapshot path");
                        if bounded {
                            let ids: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                                "SELECT count(*) FROM {ids_name}"
                            )))
                            .fetch_one(&mut *tx)
                            .await
                            .expect("inspect affected identities");
                            assert!(snapshot > 0 && ids >= changed_rows as i64);
                            (Some(snapshot), Some(ids), "bounded")
                        } else {
                            assert!(snapshot > 0 && snapshot <= rows as i64);
                            (Some(snapshot), None, "full")
                        }
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
                    let refresh_wal_bytes: i64 = sqlx::query_scalar(
                        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint",
                    )
                    .bind(&wal_before)
                    .fetch_one(&db.pool)
                    .await
                    .expect("measure refresh WAL bytes");
                    let expected_transition = match mutation_shape.as_str() {
                        "repeated_updates_delete" if round == 0 => (1, 1),
                        "repeated_updates_delete" if round % 2 == 0 => (1, 0),
                        "repeated_updates_delete" => (0, 1),
                        _ if round % 2 == 0 => (changed_rows as i64, 0),
                        _ => (0, changed_rows as i64),
                    };
                    let delivery_start = Instant::now();
                    let mut channels = HashSet::new();
                    let expected_channels = (0..subscriptions)
                        .filter(|index| {
                            if index % 2 == 0 {
                                true
                            } else {
                                mutation_shape == "repeated_updates_delete" && round == 0
                            }
                        })
                        .count();
                    for index in 0..subscriptions {
                        let expected = if index % 2 == 0 {
                            expected_transition
                        } else if mutation_shape == "repeated_updates_delete" && round == 0 {
                            (0, 1)
                        } else {
                            (0, 0)
                        };
                        if expected == (0, 0) {
                            continue;
                        }
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
                        assert_eq!(event.channel(), format!("alert_measure_{index}"));
                        assert_eq!(payload["entered"], expected.0);
                        assert_eq!(payload["exited"], expected.1);
                    }
                    assert_eq!(channels.len(), expected_channels);
                    let delivery_ms = (expected_channels > 0)
                        .then(|| delivery_start.elapsed().as_secs_f64() * 1000.0);
                    if expected_channels < subscriptions {
                        assert!(
                            tokio::time::timeout(Duration::from_millis(20), listener.recv())
                                .await
                                .is_err(),
                            "broad-threshold subscriptions must not emit membership events"
                        );
                    }
                    let correct: bool = match mutation_shape.as_str() {
                        "small_batch" => {
                            let last_id = batch_rows * 2 - 1;
                            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                                "SELECT count(*) = {batch_rows} AND \
                                 bool_and(s.embedding::text = d.embedding::text) \
                                 FROM {stream} s JOIN {source} d USING (id) \
                                 WHERE d.id <= {last_id} AND d.id % 2 = 1"
                            )))
                            .fetch_one(&db.pool)
                            .await
                            .expect("check every changed batch result")
                        }
                        "repeated_updates_delete" => {
                            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                                "SELECT s.embedding::text = d.embedding::text AND \
                                 NOT EXISTS (SELECT 1 FROM {stream} WHERE id=2) \
                                 FROM {stream} s JOIN {source} d USING (id) WHERE s.id=1"
                            )))
                            .fetch_one(&db.pool)
                            .await
                            .expect("check repeated update and delete results")
                        }
                        _ => sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                            "SELECT s.embedding::text = d.embedding::text \
                             FROM {stream} s JOIN {source} d USING (id) WHERE s.id=1"
                        )))
                        .fetch_one(&db.pool)
                        .await
                        .expect("check changed result"),
                    };
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
                    samples.push(serde_json::json!({"round":round,"mutation_shape":mutation_shape,"changed_rows":changed_rows,"source_mutation_ms":source_mutation_ms,"source_wal_bytes":source_wal_bytes,"refresh_ms":elapsed_ms,"commit_ms":commit_ms,"refresh_wal_bytes":refresh_wal_bytes,"notification_wait_ms":delivery_ms,"merge_strategy":strategy,
                    "stream_sequential_tuples_returned":tuple_delta[0],"stream_index_tuples_fetched":tuple_delta[1],
                    "source_sequential_tuples_returned":tuple_delta[2],"source_index_tuples_fetched":tuple_delta[3],
                    "stream_heap_blks_read":buffer_delta[0],"stream_heap_blks_hit":buffer_delta[1],"stream_idx_blks_read":buffer_delta[2],"stream_idx_blks_hit":buffer_delta[3],
                    "source_heap_blks_read":buffer_delta[4],"source_heap_blks_hit":buffer_delta[5],"source_idx_blks_read":buffer_delta[6],"source_idx_blks_hit":buffer_delta[7],
                    "snapshot_rows":snapshot,"affected_identities":ids,"snapshot_kind":snapshot_kind}));
                }
                measurements.push(serde_json::json!({"initial_rows":rows,"subscriptions":subscriptions,"analyzed":analyzed,"mutation_shape":mutation_shape,"estimated_rows":estimated_rows,"last_analyze":last_analyze,"last_autoanalyze":last_autoanalyze,"samples":samples}));
            }
        }
    }
    if let Some(log) = &plan_log {
        log.capture().expect("preserve subscription plan log");
    }
    println!(
        "PGVECTOR_SUBSCRIPTION_BENCHMARK_JSON={}",
        serde_json::json!({"postgres":versions.0,"pgvector":versions.1,"pg_trickle":versions.2,"dimensions":dimensions,"distinct_pivots":pivots,"selective_threshold":0.5,"broad_threshold":2.0,"force_explicit_with_noop_trigger":force_explicit,"nested_plan_capture":plan_log.is_some(),"measurements":measurements,"notes":"The corpus alternates selective and broad membership. Single-row, small-batch, and repeated-update/delete paths report source mutation, refresh, commit, WAL, tuple, buffer, and post-commit listener measurements separately. Docker CPU/memory samples are retained per case. Broad subscriptions are checked for absent membership events, except that deletes legitimately emit exits. Notification delivery can begin before the listener wait. Tuple counters include repeated visits, not distinct rows. Plan instrumentation adds overhead. No controlled concurrency is recorded."})
    );
}

/// Writes one JSON report to stdout; no timing or recall thresholds are imposed.
#[tokio::test]
#[ignore = "database benchmark; run explicitly with --run-ignored all --no-capture"]
async fn test_pgvector_filtered_ann_workload() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    let centroid_cache_mode =
        std::env::var("PGVECTOR_CENTROID_CACHE_MODE").unwrap_or_else(|_| "warm".into());
    let centroid_concurrent_writer = std::env::var("PGVECTOR_CENTROID_CONCURRENT_WRITER")
        .map(|value| value == "1")
        .unwrap_or(false);
    assert!(
        matches!(centroid_cache_mode.as_str(), "warm" | "cold"),
        "unsupported centroid cache mode: {centroid_cache_mode}"
    );
    assert!(
        !centroid_concurrent_writer || centroid_cache_mode == "warm",
        "controlled writer measurements require the warm cache mode"
    );
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
    db.execute(&format!("CREATE TABLE vector_centroid_source (id bigint PRIMARY KEY, small_group int NOT NULL, medium_group int NOT NULL, large_group int NOT NULL, skewed_group int NOT NULL, embedding vector({dim}) NOT NULL)")).await;
    db.execute(&format!("INSERT INTO vector_centroid_source SELECT id, (id % 100)::int, (id % 10)::int, (id % 2)::int, CASE WHEN id > {} THEN 0 ELSE (id % 10 + 1)::int END, embedding FROM vector_bench_source", rows / 5)).await;
    db.execute_seq(&[
        "CREATE INDEX vector_centroid_small_group ON vector_centroid_source (small_group)",
        "CREATE INDEX vector_centroid_medium_group ON vector_centroid_source (medium_group)",
        "CREATE INDEX vector_centroid_large_group ON vector_centroid_source (large_group)",
        "CREATE INDEX vector_centroid_skewed_group ON vector_centroid_source (skewed_group)",
    ])
    .await;
    db.execute("ANALYZE vector_centroid_source").await;
    let (largest_skewed_group, smallest_skewed_group): (i64, i64) = sqlx::query_as(
        "SELECT max(group_rows)::bigint, min(group_rows)::bigint FROM (SELECT count(*) AS group_rows FROM vector_centroid_source GROUP BY skewed_group) groups",
    )
    .fetch_one(&db.pool)
    .await
    .expect("measure skewed group sizes");
    assert!(
        largest_skewed_group >= (rows as i64 * 7 / 10)
            && smallest_skewed_group * 10 < largest_skewed_group,
        "centroid fixture does not contain a strongly skewed group: largest={largest_skewed_group}, smallest={smallest_skewed_group}"
    );
    let group_shapes = [
        ("small", "small_group"),
        ("medium", "medium_group"),
        ("large", "large_group"),
        ("skewed", "skewed_group"),
    ];
    for &(group, group_column) in &group_shapes {
        let query = format!(
            "SELECT {group_column}, avg(embedding) AS centroid FROM vector_centroid_source GROUP BY {group_column}"
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
    for &(group, _) in &group_shapes {
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
    let target = rows / 2;
    db.execute(&format!("UPDATE vector_centroid_source SET embedding = (SELECT embedding FROM vector_centroid_source WHERE id = {rows}) WHERE id = {target}")).await;
    let mut centroid = Vec::new();
    for &(group, group_column) in &group_shapes {
        let group_value: i32 = db
            .query_scalar(&format!(
                "SELECT {group_column} FROM vector_centroid_source WHERE id = {target}"
            ))
            .await;
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
                "SELECT count(*) FROM vector_centroid_source WHERE {group_column} = {group_value}"
            ))
            .await;
        let explain = format!(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT avg(embedding) FROM vector_centroid_source WHERE {group_column} = {group_value}"
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
                "WITH native AS (SELECT {group_column}, avg(embedding) AS centroid FROM vector_centroid_source GROUP BY {group_column}) SELECT COALESCE(max(abs(x.actual::float8 - x.expected::float8)), 0)::float8, COALESCE(bool_and({CENTROID_COMPONENT_WITHIN_BOUND}), false) AND count(*) = (SELECT count(*) FROM native) * {dim} AND (SELECT count(*) FROM {stream}) = (SELECT count(*) FROM native) AND count(DISTINCT s.{group_column}) = (SELECT count(*) FROM native) FROM {stream} s JOIN native n USING ({group_column}) CROSS JOIN LATERAL unnest(s.centroid::real[], n.centroid::real[]) AS x(actual, expected)"
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
                db.assert_st_matches_query(&stream, &format!("SELECT {group_column}, {centroid_expr} AS centroid FROM vector_centroid_source GROUP BY {group_column}")).await;
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
    let centroid_writer_id = rows as i64 + 1;
    if centroid_concurrent_writer {
        db.execute(&format!(
            "INSERT INTO vector_centroid_source SELECT {centroid_writer_id}, 1000, 1000, 1000, 1000, embedding FROM vector_centroid_source WHERE id = 1"
        ))
        .await;
        for &(group, _) in &group_shapes {
            for variant in ["differential", "algebraic", "full"] {
                let stream = format!("vector_centroid_{group}_{variant}");
                let centroid_setting = if variant == "algebraic" {
                    "SET pg_trickle.enable_centroid_state = on"
                } else {
                    "SET pg_trickle.enable_centroid_state = off"
                };
                let refresh = format!("SELECT pgtrickle.refresh_stream_table('{stream}')");
                db.execute_seq(&[
                    "SET pg_trickle.enable_vector_agg = on",
                    centroid_setting,
                    &refresh,
                ])
                .await;
            }
        }
    }
    let mut repeated = Vec::new();
    let mut timings = vec![vec![Vec::<f64>::new(); 3]; group_shapes.len()];
    let mut execution_timings = vec![vec![Vec::<f64>::new(); 3]; group_shapes.len()];
    let mut lock_wait_timings = vec![vec![Vec::<f64>::new(); 3]; group_shapes.len()];
    let mut writer_transaction_timings = vec![vec![Vec::<f64>::new(); 3]; group_shapes.len()];
    let mut cache_reset_timings = vec![vec![Vec::<f64>::new(); 3]; group_shapes.len()];
    let variants = ["differential", "algebraic", "full"];
    let warmup_rounds = 3;
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
    for round in 0..warmup_rounds + centroid_samples {
        let source_id = if round % 2 == 0 {
            alternate_source
        } else {
            rows
        };
        let mutation_start = Instant::now();
        let mutation = format!(
            "UPDATE vector_centroid_source SET embedding = (SELECT embedding FROM vector_centroid_source WHERE id = {source_id}) WHERE id >= {first_rewritten} AND id < {rows}"
        );
        let updated = sqlx::query(sqlx::AssertSqlSafe(mutation))
            .execute(&mut *conn)
            .await
            .expect("mutate repeated centroid source");
        assert_eq!(updated.rows_affected(), centroid_rewrite_rows as u64);
        let mutation_ms = mutation_start.elapsed().as_secs_f64() * 1000.0;
        for group_offset in 0..group_shapes.len() {
            let group_idx = (group_offset + round) % group_shapes.len();
            let (group, group_column) = group_shapes[group_idx];
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
                let cache_reset_ms = if round >= warmup_rounds && centroid_cache_mode == "cold" {
                    drop(conn);
                    let reset_ms = reset_centroid_cache(&mut db).await;
                    conn = db
                        .pool
                        .acquire()
                        .await
                        .expect("reconnect cold-cache centroid session");
                    for setting in [
                        "SET pg_trickle.enable_vector_agg = on",
                        "SET pg_trickle.refresh_strategy = 'differential'",
                        "SET pg_trickle.differential_max_change_ratio = 0",
                        if variant == "algebraic" {
                            "SET pg_trickle.enable_centroid_state = on"
                        } else {
                            "SET pg_trickle.enable_centroid_state = off"
                        },
                    ] {
                        sqlx::query(setting)
                            .execute(&mut *conn)
                            .await
                            .expect("restore cold-cache centroid settings");
                    }
                    Some(reset_ms)
                } else {
                    None
                };
                if centroid_cache_mode == "warm" {
                    let warm_source: i64 = sqlx::query_scalar(
                        "SELECT COALESCE(sum(pg_catalog.pg_column_size(embedding)), 0)::bigint FROM vector_centroid_source",
                    )
                    .fetch_one(&mut *conn)
                    .await
                    .expect("warm centroid source pages");
                    let warm_stream = format!(
                        "SELECT COALESCE(sum(pg_catalog.pg_column_size(centroid)), 0)::bigint FROM {stream}"
                    );
                    let warm_stream_bytes: i64 =
                        sqlx::query_scalar(sqlx::AssertSqlSafe(warm_stream))
                            .fetch_one(&mut *conn)
                            .await
                            .expect("warm centroid materialized pages");
                    let warm_state_bytes: i64 = sqlx::query_scalar(
                        "SELECT COALESCE(sum(pg_catalog.pg_column_size(g.state)), 0)::bigint FROM pgtrickle.pgt_centroid_groups g JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = $1::regclass",
                    )
                    .bind(&stream)
                    .fetch_one(&mut *conn)
                    .await
                    .expect("warm centroid auxiliary-state pages");
                    assert!(warm_source >= 0 && warm_stream_bytes >= 0 && warm_state_bytes >= 0);
                }
                sqlx::query("BEGIN")
                    .execute(&mut *conn)
                    .await
                    .expect("begin repeated centroid measurement");
                let before_lsn: String =
                    sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
                        .fetch_one(&mut *conn)
                        .await
                        .expect("read centroid WAL start");
                let before_source: (i64, i64) = sqlx::query_as(
                    "SELECT COALESCE(pg_stat_get_xact_tuples_returned('vector_centroid_source'::regclass), 0)::bigint, \
                            COALESCE(pg_stat_get_xact_tuples_fetched('vector_centroid_source'::regclass), 0)::bigint",
                )
                .fetch_one(&mut *conn)
                .await
                .expect("read centroid tuple counters before refresh");
                let start = Instant::now();
                let refresh_sql = format!(
                    "EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) SELECT pgtrickle.refresh_stream_table('{stream}')"
                );
                let (refresh_plan, elapsed_ms, writer_lock_wait_ms, writer_transaction_ms) =
                    if centroid_concurrent_writer && round >= warmup_rounds {
                        centroid_refresh_with_writer_lock(
                            &db,
                            &mut conn,
                            &refresh_sql,
                            centroid_writer_id,
                        )
                        .await
                    } else {
                        let plan: serde_json::Value =
                            sqlx::query_scalar(sqlx::AssertSqlSafe(refresh_sql))
                                .fetch_one(&mut *conn)
                                .await
                                .expect("repeat centroid refresh with buffer counts");
                        (plan, start.elapsed().as_secs_f64() * 1000.0, 0.0, 0.0)
                    };
                let refresh_execution_ms = (elapsed_ms - writer_lock_wait_ms).max(0.0);
                let buffer_usage = centroid_plan_buffers(&refresh_plan);
                let after_source: (i64, i64) = sqlx::query_as(
                    "SELECT COALESCE(pg_stat_get_xact_tuples_returned('vector_centroid_source'::regclass), 0)::bigint, \
                            COALESCE(pg_stat_get_xact_tuples_fetched('vector_centroid_source'::regclass), 0)::bigint",
                )
                .fetch_one(&mut *conn)
                .await
                .expect("read centroid tuple counters after refresh");
                let source_sequential_tuples = after_source.0 - before_source.0;
                let source_index_tuples = after_source.1 - before_source.1;
                assert!(source_sequential_tuples >= 0 && source_index_tuples >= 0);
                let wal_bytes: f64 = sqlx::query_scalar(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::text::pg_lsn)::float8",
                )
                .bind(&before_lsn)
                .fetch_one(&mut *conn)
                .await
                .expect("measure repeated centroid WAL");
                let history: (String, bool, String, Option<String>) = sqlx::query_as("SELECT h.action::text, h.was_full_fallback, h.merge_strategy_used, h.refresh_reason_detail::text FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = $1 AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
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
                let detail = history
                    .3
                    .as_deref()
                    .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                    .unwrap_or_default();
                let concurrent_group = if centroid_concurrent_writer {
                    " UNION SELECT 1000"
                } else {
                    ""
                };
                let comparison = format!(
                    "WITH affected AS (SELECT DISTINCT {group_column} FROM vector_centroid_source WHERE id >= {first_rewritten} AND id < {rows}{concurrent_group}), \
                     native AS (SELECT {group_column}, avg(embedding)::real[] AS centroid FROM vector_centroid_source WHERE {group_column} IN (SELECT {group_column} FROM affected) GROUP BY {group_column}) \
                     SELECT count(*)::bigint, (SELECT count(*) FROM native)::bigint, count(DISTINCT s.{group_column})::bigint, \
                       COALESCE(max(abs(x.actual::float8 - x.expected::float8)), 0)::float8, \
                       COALESCE(bool_and({CENTROID_COMPONENT_WITHIN_BOUND}), false) \
                     FROM {stream} s JOIN native n USING ({group_column}) CROSS JOIN LATERAL unnest(s.centroid::real[], n.centroid) x(actual, expected)"
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
                let affected_source_rows: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT count(*) FROM vector_centroid_source WHERE {group_column} IN (SELECT DISTINCT {group_column} FROM vector_centroid_source WHERE id >= {first_rewritten} AND id < {rows})"
                )))
                .fetch_one(&mut *conn)
                .await
                .expect("measure affected centroid source rows");
                let storage_bytes: i64 =
                    sqlx::query_scalar("SELECT pg_total_relation_size($1::regclass)::bigint")
                        .bind(&stream)
                        .fetch_one(&mut *conn)
                        .await
                        .expect("measure repeated centroid storage");
                let state_bytes: i64 = sqlx::query_scalar("SELECT COALESCE(sum(octet_length(g.row_id) + octet_length(pg_catalog.array_send(g.state)) + octet_length(g.state_checksum) + 16), 0)::bigint FROM pgtrickle.pgt_centroid_groups g JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = $1::regclass")
                    .bind(&stream).fetch_one(&mut *conn).await.expect("measure repeated centroid state");
                if round >= warmup_rounds {
                    let sample = round - warmup_rounds;
                    timings[group_idx][variant_idx].push(elapsed_ms);
                    execution_timings[group_idx][variant_idx].push(refresh_execution_ms);
                    if let Some(reset_ms) = cache_reset_ms {
                        cache_reset_timings[group_idx][variant_idx].push(reset_ms);
                    }
                    if centroid_concurrent_writer {
                        lock_wait_timings[group_idx][variant_idx].push(writer_lock_wait_ms);
                        writer_transaction_timings[group_idx][variant_idx]
                            .push(writer_transaction_ms);
                    }
                    repeated.push(serde_json::json!({
                        "sample": sample,
                        "group_shape": group,
                        "group_column": group_column,
                        "strategy": variant,
                        "cache_mode": &centroid_cache_mode,
                        "concurrent_writer": centroid_concurrent_writer,
                        "actual_strategy": history.2,
                        "changed_rows": centroid_rewrite_rows,
                        "affected_source_rows": affected_source_rows,
                        "source_mutation_ms": mutation_ms,
                        "refresh_ms": elapsed_ms,
                        "refresh_execution_after_lock_ms": refresh_execution_ms,
                        "cache_reset_ms": cache_reset_ms,
                        "writer_lock_wait_ms": if centroid_concurrent_writer { Some(writer_lock_wait_ms) } else { None },
                        "writer_transaction_ms": if centroid_concurrent_writer { Some(writer_transaction_ms) } else { None },
                        "buffer_usage": buffer_usage,
                        "refresh_wal_bytes_cluster_wide": wal_bytes,
                        "source_sequential_tuples_returned": source_sequential_tuples,
                        "source_index_tuples_fetched": source_index_tuples,
                        "storage_bytes_including_indexes": storage_bytes,
                        "auxiliary_state_logical_bytes": state_bytes,
                        "fallback_reason": detail.get("centroid_fallback").and_then(|value| value.get("reason")),
                        "rebase": detail.get("centroid_rebase"),
                        "max_absolute_component_error": max_error
                    }));
                }
                sqlx::query("COMMIT")
                    .execute(&mut *conn)
                    .await
                    .expect("commit repeated centroid measurement");
            }
        }
    }
    let repeated_summary = group_shapes
        .iter()
        .enumerate()
        .flat_map(|(group_idx, (group, _))| {
            let timings = &timings;
            let execution_timings = &execution_timings;
            let lock_wait_timings = &lock_wait_timings;
            let writer_transaction_timings = &writer_transaction_timings;
            let cache_reset_timings = &cache_reset_timings;
            variants
                .iter()
                .enumerate()
                .map(move |(variant_idx, variant)| {
                    let mut summary = serde_json::json!({
                        "group_shape": group, "strategy": variant,
                        "refresh_ms": percentiles(timings[group_idx][variant_idx].clone()),
                        "refresh_execution_after_lock_ms": percentiles(execution_timings[group_idx][variant_idx].clone())
                    });
                    if !lock_wait_timings[group_idx][variant_idx].is_empty() {
                        summary["writer_lock_wait_ms"] = percentiles(lock_wait_timings[group_idx][variant_idx].clone());
                    }
                    if !writer_transaction_timings[group_idx][variant_idx].is_empty() {
                        summary["writer_transaction_ms"] = percentiles(writer_transaction_timings[group_idx][variant_idx].clone());
                    }
                    if !cache_reset_timings[group_idx][variant_idx].is_empty() {
                        summary["cache_reset_ms"] = percentiles(cache_reset_timings[group_idx][variant_idx].clone());
                    }
                    summary
                })
        })
        .collect::<Vec<_>>();
    let all_lock_waits = lock_wait_timings
        .iter()
        .flatten()
        .flat_map(|values| values.iter().copied())
        .collect::<Vec<_>>();
    let all_cache_resets = cache_reset_timings
        .iter()
        .flatten()
        .flat_map(|values| values.iter().copied())
        .collect::<Vec<_>>();
    let lock_wait_summary = if all_lock_waits.is_empty() {
        serde_json::Value::Null
    } else {
        percentiles(all_lock_waits)
    };
    let cache_reset_summary = if all_cache_resets.is_empty() {
        serde_json::Value::Null
    } else {
        percentiles(all_cache_resets)
    };
    println!(
        "PGVECTOR_BENCHMARK_JSON={}",
        serde_json::json!({"schema": 1, "postgres": versions.0,
        "pgvector": versions.1, "pg_trickle": versions.2, "host_arch": std::env::consts::ARCH,
        "data": "md5(id:dimension), seedless deterministic", "rows": rows, "dimensions": dim,
        "ann": results, "centroid_baseline": centroid, "centroid_repeated": {"warmup_rounds": warmup_rounds, "measured_rounds": centroid_samples, "changed_rows_per_round": centroid_rewrite_rows, "cache_mode": &centroid_cache_mode, "concurrent_writer": centroid_concurrent_writer, "lock_wait_ms": lock_wait_summary, "cache_reset_ms": cache_reset_summary, "observations": repeated, "summary": repeated_summary, "notes": "Each measured round changes the configured number of source vectors; group and strategy order rotate after three warmup rounds. Warm samples pre-read source, materialized, and centroid-state pages. Cold samples stop PostgreSQL, drop the Linux host page cache, then start the isolated container before each timed refresh; reset time is reported separately. Concurrent samples toggle an unprojected source key while refresh runs, preserving centroid values; observed writer lock waits are sampled at 1 ms intervals, and the writer transaction remains open for at least 10 ms. This is a controlled single-writer overlap case, not sustained writer throughput. All affected groups are checked against native AVG. Refresh timings exclude mutation, cache reset, and validation; mutation time, WAL, tuple visits, buffer counts, storage, and state bytes are recorded separately."}, "centroid_notes": "The one-row baseline refreshes use EXPLAIN ANALYZE; SPI work may not be individually visible in the plan. Repeated samples use EXPLAIN ANALYZE BUFFERS WAL and record top-level shared block counts, source tuple visits, storage, and auxiliary bytes. WAL is cluster-wide and includes the concurrent writer in writer cases; auxiliary bytes are logical payload estimates.", "refresh": {"operations": ["update embedding", "update embedding", "delete"],
        "changed_rows": [1, rows / 10, rows / 10], "elapsed_ms": refresh_ms,
        "wal_bytes_cluster_wide": wal_bytes,
        "full_elapsed_ms": full_refresh_ms, "full_wal_bytes_cluster_wide": full_wal_bytes,
        "notes": "Separate equivalent HNSW indexes; differential always runs first; WAL is cluster-wide between LSN reads and excludes source mutations"}})
    );
}
