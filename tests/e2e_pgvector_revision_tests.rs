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

async fn run_live_rag_script(db: &E2eDb, script: &str, args: &[&str]) -> std::process::Output {
    run_live_rag_script_as(db, script, args, None).await
}

async fn run_live_rag_script_as(
    db: &E2eDb,
    script: &str,
    args: &[&str],
    role: Option<&str>,
) -> std::process::Output {
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let port = db
        .connection_string()
        .split('@')
        .nth(1)
        .expect("connection authority")
        .split('/')
        .next()
        .expect("host and port")
        .rsplit(':')
        .next()
        .expect("port");
    let mut command = tokio::process::Command::new("python3");
    command
        .arg(format!(
            "{}/benchmarks/pgvector/{script}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .args(args)
        .env_remove("PGSERVICE")
        .env("PGHOST", "127.0.0.1")
        .env("PGPORT", port)
        .env("PGDATABASE", database)
        .env("PGUSER", "postgres")
        .env("PGPASSWORD", "postgres")
        .env("PGSSLMODE", "disable")
        .env_remove("PGOPTIONS");
    if let Some(role) = role {
        command.env("PGOPTIONS", format!("-c role={role}"));
    }
    command.output().await.expect("run live RAG script")
}

#[tokio::test]
async fn test_live_rag_batched_benchmark_compares_equivalent_corpora() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE EXTENSION vector").await;
    let args = [
        "--rows",
        "4",
        "--dimensions",
        "384",
        "--batch",
        "2",
        "--cycles",
        "3",
        "--candidate-commit",
        "0000000000000000000000000000000000000000",
        "--package-sha256",
        "0000000000000000000000000000000000000000000000000000000000000000",
    ];
    let output = run_live_rag_script(&db, "run_live_rag_batches.py", &args).await;
    assert!(
        output.status.success(),
        "batch benchmark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse batch report");
    let samples = report["samples"].as_array().expect("batch samples");
    assert_eq!(samples.len(), 3);
    assert_eq!(
        samples[0]["order"],
        serde_json::json!(["application", "periodic_full", "differential"])
    );
    assert_eq!(
        samples[1]["order"],
        serde_json::json!(["periodic_full", "differential", "application"])
    );
    for sample in samples {
        assert_eq!(sample["generation"]["accepted"], 2);
        for name in ["application", "periodic_full", "differential"] {
            assert_eq!(sample["corpora_exact"][name], true);
            assert_eq!(
                sample["retrieval"][name]["metrics"]["hybrid_recall_at_10"],
                1.0
            );
            assert!(sample["maintenance"][name]["cluster_wal_bytes"].is_number());
        }
        assert_eq!(sample["differential_refresh"]["status"], "COMPLETED");
        assert!(
            sample["differential_refresh"]
                .get("refresh_reason")
                .is_some()
        );
        assert!(
            sample["differential_refresh"]
                .get("merge_strategy_used")
                .is_some()
        );
    }
    let repeated = run_live_rag_script(&db, "run_live_rag_batches.py", &args).await;
    assert!(
        !repeated.status.success(),
        "must refuse an already populated fixture"
    );
}

#[tokio::test]
async fn test_live_rag_mixed_benchmark_guards_churn_before_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE EXTENSION vector").await;
    let args = [
        "--rows",
        "8",
        "--dimensions",
        "384",
        "--batch",
        "4",
        "--cycles",
        "2",
        "--workload",
        "mixed",
        "--concurrent-readers",
        "1",
        "--candidate-commit",
        "0000000000000000000000000000000000000000",
        "--package-sha256",
        "0000000000000000000000000000000000000000000000000000000000000000",
    ];
    let output = run_live_rag_script(&db, "run_live_rag_batches.py", &args).await;
    assert!(
        output.status.success(),
        "mixed benchmark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse mixed report");
    assert_eq!(report["workload"], "mixed");
    assert_eq!(report["concurrent_readers"], 1);
    let samples = report["samples"].as_array().expect("mixed samples");
    assert_eq!(samples.len(), 2);
    for sample in samples {
        assert_eq!(sample["generation"]["accepted"], 2);
        assert_eq!(sample["availability_after_commit"]["eligible"], 0);
        assert_eq!(sample["availability_after_generation"]["eligible"], 2);
        for name in ["application", "periodic_full", "differential"] {
            let load = &sample["maintenance"][name]["reader_load"];
            assert_eq!(load["readers"], 1);
            assert!(load["queries"].as_u64().expect("reader count") > 0);
            assert!(
                load["overlapping_queries"].as_u64().expect("overlap count")
                    <= load["queries"].as_u64().unwrap()
            );
            assert_eq!(sample["availability_after_commit"][name], 0);
            assert_eq!(sample["availability_after_generation"][name], 0);
            assert_eq!(sample["maintenance"][name]["availability"][name], 2);
            assert_eq!(sample["corpora_exact"][name], true);
        }
    }
}

#[tokio::test]
async fn test_live_rag_converges_after_writes_during_maintenance() {
    exercise_live_rag_concurrent_write(false, false, false).await;
}

#[tokio::test]
async fn test_live_rag_converges_after_writes_during_active_maintenance() {
    exercise_live_rag_concurrent_write(true, false, false).await;
}

#[tokio::test]
async fn test_live_rag_recovers_after_maintenance_cancellation() {
    exercise_live_rag_concurrent_write(true, true, false).await;
}

#[tokio::test]
async fn test_live_rag_recovers_after_database_crash_during_maintenance() {
    exercise_live_rag_concurrent_write(true, false, true).await;
}

async fn exercise_live_rag_concurrent_write(active_maintenance: bool, cancel: bool, restart: bool) {
    use std::time::Duration;

    let mut db = if restart {
        E2eDb::new_dedicated().await
    } else {
        E2eDb::new().await
    }
    .with_extension()
    .await;
    let setup = run_live_rag_script(&db, "setup_live_rag.py", &["--dimensions", "3"]).await;
    assert!(
        setup.status.success(),
        "{}",
        String::from_utf8_lossy(&setup.stderr)
    );
    db.execute_seq(&[
        "INSERT INTO embedding_demo.documents(id, body) VALUES (1, 'A'), (2, 'B'), (3, 'C')",
        "INSERT INTO live_rag.access(document_id, tenant_id) VALUES (1, 10), (2, 10), (3, 10)",
        "INSERT INTO embedding_demo.results(document_id, incarnation, revision, chunk_version, model_version, embedding) SELECT id, incarnation, revision, chunk_version, 'model-v1', '[1,0,0]'::vector FROM embedding_demo.documents",
        "CREATE FUNCTION live_rag.test_maintenance_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(13961396); IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$",
    ]).await;
    for (table, maintenance, lock, create_trigger, drop_trigger) in [
        (
            "application",
            "SELECT live_rag.sync_application(ARRAY[1,2,3]::bigint[])",
            "LOCK TABLE live_rag.application IN ACCESS EXCLUSIVE MODE",
            "CREATE TRIGGER rag_barrier BEFORE INSERT OR UPDATE OR DELETE ON live_rag.application FOR EACH ROW EXECUTE FUNCTION live_rag.test_maintenance_barrier()",
            "DROP TRIGGER rag_barrier ON live_rag.application",
        ),
        (
            "periodic_full",
            "SELECT live_rag.recompute_full()",
            "LOCK TABLE live_rag.periodic_full IN ACCESS EXCLUSIVE MODE",
            "CREATE TRIGGER rag_barrier BEFORE INSERT OR UPDATE OR DELETE ON live_rag.periodic_full FOR EACH ROW EXECUTE FUNCTION live_rag.test_maintenance_barrier()",
            "DROP TRIGGER rag_barrier ON live_rag.periodic_full",
        ),
        (
            "differential",
            "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
            "LOCK TABLE live_rag.differential IN ACCESS EXCLUSIVE MODE",
            "CREATE TRIGGER rag_barrier BEFORE INSERT OR UPDATE OR DELETE ON live_rag.differential FOR EACH ROW EXECUTE FUNCTION live_rag.test_maintenance_barrier()",
            "DROP TRIGGER rag_barrier ON live_rag.differential",
        ),
    ] {
        db.execute("UPDATE live_rag.access SET visible=true").await;
        db.execute("SELECT live_rag.sync_application(ARRAY[1,2,3]::bigint[])")
            .await;
        db.execute("SELECT live_rag.recompute_full()").await;
        db.execute("SELECT pgtrickle.refresh_stream_table('live_rag.differential')")
            .await;
        let before_cancel: Option<String> = if cancel || restart {
            Some(db.query_scalar(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY id), '[]'::jsonb)::text FROM live_rag.{table} c")).await)
        } else {
            None
        };
        db.execute("UPDATE live_rag.access SET visible=false WHERE document_id=1")
            .await;

        if active_maintenance {
            db.execute(create_trigger).await;
        }

        let mut blocker = db.pool.begin().await.expect("begin maintenance barrier");
        let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *blocker)
            .await
            .expect("inspect barrier");
        sqlx::query(if active_maintenance {
            "SELECT pg_advisory_xact_lock(13961396)"
        } else {
            lock
        })
        .execute(&mut *blocker)
        .await
        .expect("hold maintenance barrier");
        let pool = db.pool.clone();
        let task = tokio::spawn(async move {
            let mut conn = pool.acquire().await?;
            sqlx::query("SET pg_trickle.refresh_strategy = 'differential'")
                .execute(&mut *conn)
                .await?;
            sqlx::query("SET pg_trickle.differential_max_change_ratio = 0")
                .execute(&mut *conn)
                .await?;
            sqlx::query(maintenance).execute(&mut *conn).await
        });
        let maintenance_pid = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let blocked: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1 = ANY(pg_blocking_pids(pid)) LIMIT 1")
                    .bind(blocker_pid).fetch_optional(&db.pool).await.expect("inspect blocked maintenance");
                if let Some(pid) = blocked { break pid; }
                assert!(!task.is_finished(), "{table} maintenance finished before the barrier");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.expect("maintenance did not reach the barrier");
        tokio::time::timeout(
            Duration::from_secs(10),
            db.execute("UPDATE live_rag.access SET visible=false WHERE document_id=2"),
        )
        .await
        .expect("source write did not commit during maintenance");
        let recovery_started = std::time::Instant::now();
        let result = if restart {
            let killed = tokio::process::Command::new("docker")
                .args(["kill", "--signal", "KILL", db.container_id()])
                .output()
                .await
                .expect("kill PostgreSQL container");
            assert!(
                killed.status.success(),
                "{}",
                String::from_utf8_lossy(&killed.stderr)
            );
            let result = tokio::time::timeout(Duration::from_secs(30), task)
                .await
                .expect("crashed maintenance did not stop")
                .expect("join crashed maintenance");
            drop(blocker);
            db.pool.close().await;
            let started = tokio::process::Command::new("docker")
                .args(["start", db.container_id()])
                .output()
                .await
                .expect("restart PostgreSQL container");
            assert!(
                started.status.success(),
                "{}",
                String::from_utf8_lossy(&started.stderr)
            );
            db.pool = db.reconnect_after_restart().await;
            let hidden: i64 = db.query_scalar("SELECT count(*) FROM live_rag.access WHERE document_id IN (1,2) AND NOT visible").await;
            assert_eq!(
                hidden, 2,
                "committed visibility changes must survive restart"
            );
            result
        } else if cancel {
            let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
                .bind(maintenance_pid)
                .fetch_one(&db.pool)
                .await
                .expect("cancel maintenance");
            assert!(cancelled, "maintenance backend disappeared");
            let result = tokio::time::timeout(Duration::from_secs(30), task)
                .await
                .expect("maintenance cancellation timed out")
                .expect("join maintenance");
            blocker
                .rollback()
                .await
                .expect("release cancellation barrier");
            result
        } else {
            blocker.commit().await.expect("release maintenance barrier");
            tokio::time::timeout(Duration::from_secs(30), task)
                .await
                .expect("maintenance timed out")
                .expect("join maintenance")
        };
        if cancel || restart {
            let error = result.expect_err("interrupted maintenance unexpectedly committed");
            if cancel {
                assert_eq!(
                    error.as_database_error().and_then(|e| e.code()).as_deref(),
                    Some("57014"),
                    "{error}"
                );
            }
            let after: String = db.query_scalar(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY id), '[]'::jsonb)::text FROM live_rag.{table} c")).await;
            assert_eq!(
                Some(after),
                before_cancel,
                "cancelled {table} maintenance changed the corpus"
            );
        } else {
            result.expect("complete maintenance");
        }

        if table == "differential" && !cancel && !restart {
            let incremental: bool = db.query_scalar("SELECT h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1").await;
            assert!(incremental, "maintenance must remain differential");
        }

        if active_maintenance {
            db.execute(drop_trigger).await;
        }

        for view in [
            "application_current",
            "periodic_full_current",
            "differential_current",
        ] {
            let leaked: i64 = db
                .query_scalar(&format!(
                    "SELECT count(*) FROM live_rag.{view} WHERE id IN (1,2)"
                ))
                .await;
            assert_eq!(
                leaked, 0,
                "{view} exposed a hidden document during {table} maintenance"
            );
        }
        db.execute("SELECT live_rag.sync_application(ARRAY[1,2,3]::bigint[])")
            .await;
        db.execute("SELECT live_rag.recompute_full()").await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy = 'differential'",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
        )
        .await
        .expect("retry differential maintenance");
        let retried: bool = db.query_scalar("SELECT h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1").await;
        assert!(retried, "reconciliation must remain differential");
        if cancel || restart {
            eprintln!(
                "{table} interruption to completed reconciliation: {:.3}s",
                recovery_started.elapsed().as_secs_f64()
            );
        }
        for corpus in ["application", "periodic_full", "differential"] {
            db.assert_st_matches_query(
                &format!("live_rag.{corpus}"),
                "SELECT * FROM live_rag.truth",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn test_live_rag_paced_writes_and_tenant_reads_during_refresh() {
    use std::time::{Duration, Instant};

    let db = E2eDb::new().await.with_extension().await;
    let setup = run_live_rag_script(&db, "setup_live_rag.py", &["--dimensions", "384"]).await;
    assert!(
        setup.status.success(),
        "{}",
        String::from_utf8_lossy(&setup.stderr)
    );
    db.execute_seq(&[
        "INSERT INTO embedding_demo.documents(id, body) SELECT n, 'document ' || n FROM generate_series(1,1000) n",
        "INSERT INTO live_rag.access(document_id, tenant_id) SELECT id, CASE WHEN id <= 900 THEN 10 ELSE 20 END FROM embedding_demo.documents",
        "INSERT INTO embedding_demo.results(document_id, incarnation, revision, chunk_version, model_version, embedding) SELECT id, incarnation, revision, chunk_version, 'model-v1', array_fill(0.5::real, ARRAY[384])::vector FROM embedding_demo.documents",
        "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
        "CREATE ROLE live_rag_load_reader NOLOGIN",
        "INSERT INTO live_rag.tenant_readers SELECT oid, 10 FROM pg_roles WHERE rolname='live_rag_load_reader'",
        "GRANT USAGE ON SCHEMA live_rag TO live_rag_load_reader",
        "GRANT SELECT ON live_rag.differential_authorized TO live_rag_load_reader",
        "CREATE FUNCTION live_rag.load_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(13961397); IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$",
        "CREATE TRIGGER rag_load_barrier BEFORE INSERT OR UPDATE OR DELETE ON live_rag.differential FOR EACH ROW EXECUTE FUNCTION live_rag.load_barrier()",
        "UPDATE live_rag.access SET visible=false WHERE document_id=1",
    ]).await;
    let mut blocker = db.pool.begin().await.expect("begin refresh barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("inspect barrier");
    sqlx::query("SELECT pg_advisory_xact_lock(13961397)")
        .execute(&mut *blocker)
        .await
        .expect("hold barrier");
    let pool = db.pool.clone();
    let refresh = tokio::spawn(async move {
        let mut conn = pool.acquire().await?;
        sqlx::query("SET pg_trickle.refresh_strategy='differential'")
            .execute(&mut *conn)
            .await?;
        sqlx::query("SET pg_trickle.differential_max_change_ratio=0")
            .execute(&mut *conn)
            .await?;
        sqlx::query("SELECT pgtrickle.refresh_stream_table('live_rag.differential')")
            .execute(&mut *conn)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1 = ANY(pg_blocking_pids(pid)))")
                .bind(blocker_pid).fetch_one(&db.pool).await.expect("inspect refresh barrier");
            if blocked { break; }
            assert!(!refresh.is_finished(), "refresh finished before row barrier");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("refresh did not reach row barrier");

    let mut reader = db.pool.acquire().await.expect("acquire tenant reader");
    sqlx::query("SET ROLE live_rag_load_reader")
        .execute(&mut *reader)
        .await
        .expect("assume reader role");
    let started = Instant::now();
    for id in 2_i64..=101 {
        let updated = sqlx::query("UPDATE live_rag.access SET visible=false WHERE document_id=$1")
            .bind(id)
            .execute(&db.pool)
            .await
            .expect("commit visibility change");
        assert_eq!(updated.rows_affected(), 1);
        let leaked: i64 = sqlx::query_scalar("SELECT count(*) FROM live_rag.differential_authorized WHERE tenant_id <> 10 OR id <= $1")
            .bind(id).fetch_one(&mut *reader).await.expect("check tenant and visibility");
        assert_eq!(leaked, 0, "hidden or foreign document became visible");
        let _: Vec<i64> = sqlx::query_scalar("SELECT id FROM live_rag.differential_authorized ORDER BY embedding::vector(384) <=> array_fill(0.5::real, ARRAY[384])::vector LIMIT 10")
            .fetch_all(&mut *reader).await.expect("run tenant vector read");
        assert!(
            !refresh.is_finished(),
            "refresh must remain at the barrier during load"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_secs(5));
    sqlx::query("RESET ROLE")
        .execute(&mut *reader)
        .await
        .expect("reset reader role");
    drop(reader);
    blocker.commit().await.expect("release refresh barrier");
    tokio::time::timeout(Duration::from_secs(30), refresh)
        .await
        .expect("refresh timed out")
        .expect("join refresh")
        .expect("complete refresh");
    let incremental: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(incremental, "loaded refresh must remain differential");
    db.execute("DROP TRIGGER rag_load_barrier ON live_rag.differential")
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
    )
    .await
    .expect("reconcile concurrent writes");
    let retried: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(retried, "reconciliation must remain differential");
    db.assert_st_matches_query("live_rag.differential", "SELECT * FROM live_rag.truth")
        .await;
    eprintln!(
        "100 committed visibility changes and tenant reads during refresh: {:.3}s",
        elapsed.as_secs_f64()
    );
}

#[tokio::test]
#[ignore = "measures unblocked concurrent workload overlap"]
async fn test_live_rag_unblocked_writes_and_reads_during_refresh() {
    let rows: i64 = std::env::var("PGT_RAG_LOAD_ROWS")
        .unwrap_or_else(|_| "1000".into())
        .parse()
        .expect("PGT_RAG_LOAD_ROWS must be an integer");
    let dimensions: i32 = std::env::var("PGT_RAG_LOAD_DIMENSIONS")
        .unwrap_or_else(|_| "384".into())
        .parse()
        .expect("PGT_RAG_LOAD_DIMENSIONS must be an integer");
    let changes: i64 = std::env::var("PGT_RAG_LOAD_CHANGES")
        .unwrap_or_else(|_| "100".into())
        .parse()
        .expect("PGT_RAG_LOAD_CHANGES must be an integer");
    assert!(
        (1000..=1_000_000).contains(&rows),
        "rows must be between 1000 and 1000000"
    );
    assert!(
        [384, 768, 1536].contains(&dimensions),
        "dimensions must be 384, 768 or 1536"
    );
    assert!(
        (1..=1000).contains(&changes) && changes < rows * 9 / 10,
        "changes must be 1–1000 and stay within tenant 10"
    );
    for approach in ["application", "periodic_full", "differential"] {
        exercise_live_rag_unblocked_workload(approach, rows, dimensions, changes).await;
    }
}

async fn exercise_live_rag_unblocked_workload(
    approach: &'static str,
    rows: i64,
    dimensions: i32,
    changes: i64,
) {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    };
    use std::time::Duration;

    let (initial, maintenance, view, role) = match approach {
        "application" => (
            "SELECT live_rag.sync_application(ARRAY(SELECT id FROM live_rag.truth)::bigint[])",
            "live_rag.test_app_maintenance()",
            "application_authorized",
            "live_rag_stream_reader_app",
        ),
        "periodic_full" => (
            "SELECT live_rag.recompute_full()",
            "live_rag.recompute_full()",
            "periodic_full_authorized",
            "live_rag_stream_reader_full",
        ),
        "differential" => (
            "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
            "pgtrickle.refresh_stream_table('live_rag.differential')",
            "differential_authorized",
            "live_rag_stream_reader_diff",
        ),
        _ => panic!("unknown test approach"),
    };
    let db = E2eDb::new().await.with_extension().await;
    let setup_started = std::time::Instant::now();
    let dimension_arg = dimensions.to_string();
    let setup =
        run_live_rag_script(&db, "setup_live_rag.py", &["--dimensions", &dimension_arg]).await;
    assert!(
        setup.status.success(),
        "{}",
        String::from_utf8_lossy(&setup.stderr)
    );
    db.execute("SELECT pgtrickle.drop_stream_table('embedding_demo.corpus_v1', true)")
        .await;
    db.execute("SELECT pgtrickle.drop_stream_table('embedding_demo.corpus_v2', true)")
        .await;
    if approach != "differential" {
        db.execute("SELECT pgtrickle.drop_stream_table('live_rag.differential', true)")
            .await;
    }
    let stream_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables")
        .await;
    assert_eq!(stream_count, i64::from(approach == "differential"));
    sqlx::query("INSERT INTO embedding_demo.documents(id, body) SELECT n, 'document ' || n FROM generate_series(1,$1::bigint) n")
        .bind(rows).execute(&db.pool).await.expect("seed documents");
    sqlx::query("INSERT INTO live_rag.access(document_id, tenant_id) SELECT id, CASE WHEN id <= $1::bigint * 9 / 10 THEN 10 ELSE 20 END FROM embedding_demo.documents")
        .bind(rows).execute(&db.pool).await.expect("seed tenants");
    sqlx::query("INSERT INTO embedding_demo.results(document_id, incarnation, revision, chunk_version, model_version, embedding) SELECT d.id, d.incarnation, d.revision, d.chunk_version, 'model-v1', ARRAY(SELECT ((((pg_catalog.hashint8(d.id * 8191 + i::bigint * 104729)::bigint & 2147483647) % 2001) - 1000)::real / 1000) FROM generate_series(1,$1::integer) i ORDER BY i)::vector FROM embedding_demo.documents d")
        .bind(dimensions).execute(&db.pool).await.expect("seed varied embeddings");
    db.execute(initial).await;
    db.execute_seq(&[
        "VACUUM (ANALYZE) embedding_demo.documents",
        "VACUUM (ANALYZE) embedding_demo.results",
        "VACUUM (ANALYZE) live_rag.access",
    ])
    .await;
    db.execute(&format!("VACUUM (ANALYZE) live_rag.{approach}"))
        .await;
    if approach == "application" {
        // This watermark is safe only for the ordered single-writer trace below.
        db.execute_seq(&[
            "CREATE TABLE live_rag.test_app_progress(last_id bigint NOT NULL)",
            "INSERT INTO live_rag.test_app_progress VALUES (0)",
            "CREATE FUNCTION live_rag.test_app_maintenance() RETURNS void LANGUAGE plpgsql AS $$ DECLARE prior bigint; newest bigint; BEGIN SELECT last_id INTO prior FROM live_rag.test_app_progress FOR UPDATE; SELECT COALESCE(max(document_id), prior) INTO newest FROM live_rag.access WHERE NOT visible AND document_id > prior; PERFORM live_rag.sync_application(ARRAY(SELECT document_id FROM live_rag.access WHERE NOT visible AND document_id > prior AND document_id <= newest)::bigint[]); UPDATE live_rag.test_app_progress SET last_id=newest; END $$",
        ]).await;
    }
    // The approach, role, view and maintenance SQL are selected from the fixed test cases above.
    db.execute(&format!("CREATE ROLE {role} NOLOGIN")).await;
    db.execute(&format!(
        "INSERT INTO live_rag.tenant_readers SELECT oid, 10 FROM pg_roles WHERE rolname='{role}'"
    ))
    .await;
    db.execute(&format!("GRANT USAGE ON SCHEMA live_rag TO {role}"))
        .await;
    db.execute(&format!("GRANT SELECT ON live_rag.{view} TO {role}"))
        .await;
    db.execute(&format!("CREATE FUNCTION live_rag.measure_refresh() RETURNS TABLE(started float8, finished float8) LANGUAGE plpgsql AS $$ BEGIN started := extract(epoch FROM clock_timestamp()); PERFORM {maintenance}; finished := extract(epoch FROM clock_timestamp()); RETURN NEXT; END $$")).await;
    db.execute("UPDATE live_rag.access SET visible=false WHERE document_id=1")
        .await;
    let setup_elapsed = setup_started.elapsed();

    let workload_started = std::time::Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicI64::new(1));
    let writer_pool = db.pool.clone();
    let writer_progress = committed.clone();
    let writer = tokio::spawn(async move {
        let mut samples = Vec::new();
        let mut client_samples = Vec::new();
        for id in 2_i64..=changes + 1 {
            let client_start = workload_started.elapsed().as_secs_f64();
            let mut tx = writer_pool.begin().await.expect("begin visibility change");
            let interval: (f64, f64) = sqlx::query_as("UPDATE live_rag.access SET visible=false WHERE document_id=$1 RETURNING extract(epoch FROM statement_timestamp())::float8, extract(epoch FROM clock_timestamp())::float8")
                .bind(id).fetch_one(&mut *tx).await.expect("update visibility");
            tx.commit().await.expect("commit visibility change");
            client_samples.push((client_start, workload_started.elapsed().as_secs_f64()));
            samples.push(interval);
            writer_progress.store(id, Ordering::Release);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        (samples, client_samples)
    });
    let reader_pool = db.pool.clone();
    let reader_stop = stop.clone();
    let reader_progress = committed.clone();
    let reader = tokio::spawn(async move {
        let mut conn = reader_pool.acquire().await.expect("acquire tenant reader");
        sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE {role}")))
            .execute(&mut *conn)
            .await
            .expect("assume reader role");
        let mut samples = Vec::new();
        let mut guards = Vec::new();
        let mut client_samples = Vec::new();
        let mut client_guards = Vec::new();
        while !reader_stop.load(Ordering::Acquire) {
            let hidden_through = reader_progress.load(Ordering::Acquire);
            let guard_start = workload_started.elapsed().as_secs_f64();
            let guard: (f64, f64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT extract(epoch FROM statement_timestamp())::float8, extract(epoch FROM clock_timestamp())::float8, count(*) FROM live_rag.{view} WHERE tenant_id <> 10 OR id <= $1"
            )))
            .bind(hidden_through)
            .fetch_one(&mut *conn)
            .await
            .expect("check tenant and visibility");
            assert_eq!(guard.2, 0, "hidden or foreign document became visible");
            client_guards.push((guard_start, workload_started.elapsed().as_secs_f64()));
            guards.push((guard.0, guard.1));
            let read_start = workload_started.elapsed().as_secs_f64();
            let sample: (f64, f64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!("WITH candidates AS MATERIALIZED (SELECT id FROM live_rag.{view} ORDER BY embedding::vector({dimensions}) <=> array_fill(0.5::real, ARRAY[{dimensions}])::vector LIMIT 10) SELECT extract(epoch FROM statement_timestamp())::float8, extract(epoch FROM clock_timestamp())::float8, count(*) FROM candidates")))
                .fetch_one(&mut *conn).await.expect("run tenant vector read");
            assert!(sample.2 <= 10);
            client_samples.push((read_start, workload_started.elapsed().as_secs_f64()));
            samples.push((sample.0, sample.1));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        sqlx::query("RESET ROLE")
            .execute(&mut *conn)
            .await
            .expect("reset reader role");
        (guards, client_guards, samples, client_samples)
    });

    let mut conn = db.pool.acquire().await.expect("acquire refresh connection");
    sqlx::query("SET pg_trickle.refresh_strategy='differential'")
        .execute(&mut *conn)
        .await
        .expect("set differential strategy");
    sqlx::query("SET pg_trickle.differential_max_change_ratio=0")
        .execute(&mut *conn)
        .await
        .expect("disable fallback ratio");
    let mut refreshes: Vec<(f64, f64)> = Vec::new();
    let mut client_refreshes: Vec<(f64, f64)> = Vec::new();
    let mut cadence = tokio::time::interval(Duration::from_millis(200));
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while !writer.is_finished() && refreshes.len() < 1000 {
        cadence.tick().await;
        if writer.is_finished() {
            break;
        }
        let client_start = workload_started.elapsed().as_secs_f64();
        let interval: (f64, f64) =
            sqlx::query_as("SELECT started, finished FROM live_rag.measure_refresh()")
                .fetch_one(&mut *conn)
                .await
                .expect("run unblocked refresh");
        client_refreshes.push((client_start, workload_started.elapsed().as_secs_f64()));
        if approach == "differential" {
            let incremental: bool = sqlx::query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1")
                .fetch_one(&mut *conn).await.expect("inspect refresh history");
            assert!(incremental, "refresh must remain differential");
        }
        refreshes.push(interval);
    }
    assert!(
        writer.is_finished(),
        "refresh safety limit reached before writer finished"
    );
    let (writes, client_writes) = writer.await.expect("join writer");
    let workload_elapsed = workload_started.elapsed();
    stop.store(true, Ordering::Release);
    let (guards, client_guards, reads, client_reads) = reader.await.expect("join reader");
    let overlaps = |samples: &[(f64, f64)]| {
        samples
            .iter()
            .filter(|(start, end)| {
                refreshes
                    .iter()
                    .any(|(begin, finish)| start < finish && end > begin)
            })
            .count()
    };
    let overlapping_writes = overlaps(&writes);
    let overlapping_reads = overlaps(&reads);
    let overlapping_guards = overlaps(&guards);
    let client_overlaps = |samples: &[(f64, f64)]| {
        samples
            .iter()
            .filter(|(start, end)| {
                client_refreshes
                    .iter()
                    .any(|(begin, finish)| start < finish && end > begin)
            })
            .count()
    };
    assert!(
        !writes.is_empty() && !reads.is_empty() && !refreshes.is_empty(),
        "workload produced no samples"
    );
    sqlx::query(sqlx::AssertSqlSafe(format!("SELECT {maintenance}")))
        .execute(&mut *conn)
        .await
        .expect("reconcile remaining writes");
    if approach == "differential" {
        let reconciled: bool = sqlx::query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1")
            .fetch_one(&mut *conn).await.expect("inspect reconciliation");
        assert!(reconciled, "final reconciliation must remain differential");
    }
    drop(conn);
    db.assert_st_matches_query(
        &format!("live_rag.{approach}"),
        "SELECT * FROM live_rag.truth",
    )
    .await;
    eprintln!(
        "{approach} unblocked workload: rows={rows} dimensions={dimensions} setup={:.3}s, {} writes ({} overlapping), {} guards ({} overlapping), {} reads ({} overlapping), {} maintenance calls, {:.3}s",
        setup_elapsed.as_secs_f64(),
        writes.len(),
        overlapping_writes,
        guards.len(),
        overlapping_guards,
        reads.len(),
        overlapping_reads,
        refreshes.len(),
        workload_elapsed.as_secs_f64()
    );
    eprintln!(
        "{approach} client-window overlaps: {} writes, {} guards, {} reads",
        client_overlaps(&client_writes),
        client_overlaps(&client_guards),
        client_overlaps(&client_reads)
    );
    for (name, intervals) in [
        ("write statement", &writes),
        ("guard read", &guards),
        ("vector read", &reads),
        ("refresh call", &refreshes),
        ("write client", &client_writes),
        ("guard client", &client_guards),
        ("vector client", &client_reads),
        ("maintenance client", &client_refreshes),
    ] {
        let mut durations: Vec<f64> = intervals
            .iter()
            .map(|(start, end)| (end - start) * 1000.0)
            .collect();
        durations.sort_by(f64::total_cmp);
        let percentile = |n: usize| durations[(durations.len() * n).div_ceil(100) - 1];
        eprintln!(
            "{name} milliseconds: p50={:.3} p95={:.3} p99={:.3}",
            percentile(50),
            percentile(95),
            percentile(99)
        );
    }
    eprintln!(
        "unblocked_samples={}",
        serde_json::json!({"approach": approach, "rows": rows, "dimensions": dimensions, "changes": changes, "writes": writes, "guards": guards, "reads": reads, "refreshes": refreshes, "client_writes": client_writes, "client_guards": client_guards, "client_reads": client_reads, "client_refreshes": client_refreshes})
    );
}

#[tokio::test]
async fn test_live_rag_baselines_recheck_revisions_and_converge() {
    let db = E2eDb::new().await.with_extension().await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let port = db
        .connection_string()
        .split('@')
        .nth(1)
        .expect("connection authority")
        .split('/')
        .next()
        .expect("host and port")
        .rsplit(':')
        .next()
        .expect("port");
    let setup = run_live_rag_script(&db, "setup_live_rag.py", &["--dimensions", "3"]).await;
    assert!(
        setup.status.success(),
        "live RAG setup failed: {}",
        String::from_utf8_lossy(&setup.stderr)
    );
    db.execute_seq(&[
        "INSERT INTO embedding_demo.documents(id, body) VALUES (1, 'A'), (2, 'A')",
        "INSERT INTO live_rag.access(document_id, tenant_id) VALUES (1, 10), (2, 20)",
    ])
    .await;
    enqueue(&db, "model-v1").await;
    db.execute("SELECT embedding_demo.enqueue(2, 'model-v1')")
        .await;
    let output = run_live_rag_script(
        &db,
        "fake_embedding_worker.py",
        &["--model", "model-v1", "--duplicate"],
    )
    .await;
    assert!(
        output.status.success(),
        "fake worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse worker report");
    assert_eq!(report["accepted"], 2);
    assert_eq!(report["duplicate_rejected"], 2);
    db.execute_seq(&[
        "SELECT live_rag.sync_application(ARRAY[1,2]::bigint[])",
        "SELECT live_rag.recompute_full()",
    ])
    .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
    )
    .await
    .expect("refresh differential corpus");
    let differential: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(
        differential,
        "live RAG comparison requires differential maintenance"
    );
    for table in ["application", "periodic_full", "differential"] {
        db.assert_st_matches_query(&format!("live_rag.{table}"), "SELECT * FROM live_rag.truth")
            .await;
    }
    let mut results = Vec::new();
    for view in [
        "application_current",
        "periodic_full_current",
        "differential_current",
    ] {
        let output = tokio::process::Command::new("psql")
            .args([
                "-X",
                "-w",
                "-qAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-v",
                &format!("corpus={view}"),
                "-v",
                "tenant=10",
                "-v",
                "query_vector=[1,0,0]",
                "-v",
                "terms=A",
                "-f",
            ])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/benchmarks/pgvector/live_rag_hybrid_query.sql"
            ))
            .env_remove("PGSERVICE")
            .env("PGHOST", "127.0.0.1")
            .env("PGPORT", port)
            .env("PGDATABASE", &database)
            .env("PGUSER", "postgres")
            .env("PGPASSWORD", "postgres")
            .env("PGSSLMODE", "disable")
            .output()
            .await
            .expect("run hybrid query");
        assert!(
            output.status.success(),
            "hybrid query failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        results.push(String::from_utf8(output.stdout).expect("hybrid output"));
    }
    assert!(
        results[0].starts_with("1|"),
        "tenant result must contain its authorized row"
    );
    assert_eq!(
        results[0].lines().count(),
        1,
        "other tenants must not appear"
    );
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0], results[2]);
    let vector_version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname='vector'")
        .await;
    for view in [
        "application_current",
        "periodic_full_current",
        "differential_current",
    ] {
        let output = run_live_rag_script(
            &db,
            "measure_live_rag_recall.py",
            &[
                "--corpus",
                view,
                "--tenant",
                "10",
                "--dimensions",
                "3",
                "--query-vector",
                "[1,0,0]",
                "--terms",
                "A",
                "--k",
                "2",
                "--expected-pgvector",
                &vector_version,
                "--candidate-commit",
                "0000000000000000000000000000000000000000",
                "--package-sha256",
                "0000000000000000000000000000000000000000000000000000000000000000",
            ],
        )
        .await;
        assert!(
            output.status.success(),
            "recall measurement failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("parse recall report");
        assert_eq!(report["metrics"]["exact_count"], 1);
        assert_eq!(report["metrics"]["candidate_count"], 1);
        assert_eq!(report["metrics"]["recall_at_k"], 1.0);
        assert_eq!(report["metrics"]["underfill"], 1);
        assert_eq!(report["metrics"]["forced_candidate_count"], 1);
        assert_eq!(report["metrics"]["forced_matched"], 1);
        assert_eq!(report["metrics"]["forced_recall_at_k"], 1.0);
        assert_eq!(report["metrics"]["forced_underfill"], 1);
        assert_eq!(report["metrics"]["hybrid_exact_count"], 1);
        assert_eq!(report["metrics"]["hybrid_candidate_count"], 1);
        assert_eq!(report["metrics"]["hybrid_recall_at_10"], 1.0);
        assert_eq!(report["metrics"]["hybrid_underfill"], 9);
    }

    db.execute_seq(&[
        "UPDATE embedding_demo.documents SET body='B' WHERE id=1",
        "SELECT embedding_demo.enqueue(1, 'model-v1')",
    ])
    .await;
    let stale = claim(&db, "model-v1").await;
    db.execute_seq(&[
        "UPDATE embedding_demo.documents SET body='C' WHERE id=1",
        "UPDATE live_rag.access SET visible=false WHERE document_id=2",
        "SELECT embedding_demo.enqueue(1, 'model-v1')",
    ])
    .await;
    for view in [
        "application_current",
        "periodic_full_current",
        "differential_current",
    ] {
        assert_eq!(
            db.count(&format!("live_rag.{view}")).await,
            0,
            "{view} exposed a stale or hidden row"
        );
    }
    assert!(!complete(&db, &stale, "model-v1").await);
    let current = claim(&db, "model-v1").await;
    assert!(complete(&db, &current, "model-v1").await);
    assert!(!complete(&db, &current, "model-v1").await);
    db.execute("DELETE FROM embedding_demo.documents WHERE id=2")
        .await;
    db.execute_seq(&[
        "SELECT live_rag.sync_application(ARRAY[1,2]::bigint[])",
        "SELECT live_rag.recompute_full()",
    ])
    .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
    )
    .await
    .expect("refresh changed differential corpus");
    for table in ["application", "periodic_full", "differential"] {
        db.assert_st_matches_query(&format!("live_rag.{table}"), "SELECT * FROM live_rag.truth")
            .await;
    }
    let tenant: i64 = db
        .query_scalar("SELECT tenant_id FROM live_rag.differential_current WHERE id=1")
        .await;
    assert_eq!(tenant, 10);
}

#[tokio::test]
async fn test_live_rag_authorized_views_enforce_tenant_and_visibility() {
    let db = E2eDb::new().await.with_extension().await;
    let setup = run_live_rag_script(&db, "setup_live_rag.py", &["--dimensions", "3"]).await;
    assert!(
        setup.status.success(),
        "setup failed: {}",
        String::from_utf8_lossy(&setup.stderr)
    );
    db.execute_seq(&[
        "INSERT INTO embedding_demo.documents(id, body) VALUES (1, 'tenant one'), (2, 'tenant two')",
        "INSERT INTO live_rag.access(document_id, tenant_id) VALUES (1, 10), (2, 20)",
        "SELECT embedding_demo.enqueue(1, 'model-v1')",
        "SELECT embedding_demo.enqueue(2, 'model-v1')",
    ]).await;
    let worker =
        run_live_rag_script(&db, "fake_embedding_worker.py", &["--model", "model-v1"]).await;
    assert!(
        worker.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&worker.stderr)
    );
    db.execute_seq(&[
        "SELECT live_rag.sync_application(ARRAY[1,2]::bigint[])",
        "SELECT live_rag.recompute_full()",
        "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
        "CREATE ROLE live_rag_reader_a NOLOGIN",
        "CREATE ROLE live_rag_reader_b NOLOGIN",
        "INSERT INTO live_rag.tenant_readers SELECT oid, CASE rolname WHEN 'live_rag_reader_a' THEN 10 ELSE 20 END FROM pg_roles WHERE rolname IN ('live_rag_reader_a', 'live_rag_reader_b')",
        "GRANT USAGE ON SCHEMA live_rag TO live_rag_reader_a, live_rag_reader_b",
        "GRANT SELECT ON live_rag.application_authorized, live_rag.periodic_full_authorized, live_rag.differential_authorized TO live_rag_reader_a, live_rag_reader_b",
    ]).await;
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire tenant reader session");
    for (role, expected, tenant, other_tenant) in [
        ("live_rag_reader_a", 1_i64, 10, 20),
        ("live_rag_reader_b", 2_i64, 20, 10),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE {role}")))
            .execute(&mut *conn)
            .await
            .expect("assume reader role");
        for view in [
            "application_authorized",
            "periodic_full_authorized",
            "differential_authorized",
        ] {
            let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM live_rag.{view} ORDER BY id"
            )))
            .fetch_all(&mut *conn)
            .await
            .expect("read authorized corpus");
            assert_eq!(ids, vec![expected]);
            for (requested_tenant, visible) in [(tenant, true), (other_tenant, false)] {
                let hybrid = include_str!("../benchmarks/pgvector/live_rag_hybrid_query.sql")
                    .replace("live_rag.:\"corpus\"", &format!("live_rag.{view}"))
                    .replace(":'tenant'", &requested_tenant.to_string())
                    .replace(":'query_vector'", "'[1,0,0]'")
                    .replace(":'terms'", "'tenant'");
                let results: Vec<(i64, f64)> = sqlx::query_as(sqlx::AssertSqlSafe(hybrid))
                    .fetch_all(&mut *conn)
                    .await
                    .expect("run authorized hybrid query");
                assert_eq!(
                    results.into_iter().map(|row| row.0).collect::<Vec<_>>(),
                    if visible { vec![expected] } else { vec![] }
                );
            }
        }
        assert!(
            sqlx::query("SELECT * FROM live_rag.differential_current")
                .execute(&mut *conn)
                .await
                .is_err(),
            "reader must not access unfiltered corpus"
        );
        assert!(
            sqlx::query("SELECT * FROM live_rag.tenant_readers")
                .execute(&mut *conn)
                .await
                .is_err(),
            "reader must not inspect or edit role mapping"
        );
        sqlx::query("RESET ROLE")
            .execute(&mut *conn)
            .await
            .expect("reset reader role");
    }
    let vector_version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname='vector'")
        .await;
    let recall = run_live_rag_script_as(
        &db,
        "measure_live_rag_recall.py",
        &[
            "--corpus",
            "differential_authorized",
            "--tenant",
            "10",
            "--dimensions",
            "3",
            "--query-vector",
            "[1,0,0]",
            "--terms",
            "tenant",
            "--k",
            "2",
            "--expected-pgvector",
            &vector_version,
            "--candidate-commit",
            "0000000000000000000000000000000000000000",
            "--package-sha256",
            "0000000000000000000000000000000000000000000000000000000000000000",
        ],
        Some("live_rag_reader_a"),
    )
    .await;
    assert!(
        recall.status.success(),
        "restricted recall failed: {}",
        String::from_utf8_lossy(&recall.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&recall.stdout).expect("parse restricted recall");
    assert_eq!(report["metrics"]["exact_count"], 1);
    assert_eq!(report["metrics"]["candidate_count"], 1);
    assert_eq!(report["metrics"]["hybrid_exact_count"], 1);
    assert_eq!(report["metrics"]["hybrid_candidate_count"], 1);
    assert_eq!(report["metrics"]["hybrid_recall_at_10"], 1.0);
    let empty = run_live_rag_script_as(
        &db,
        "measure_live_rag_recall.py",
        &[
            "--corpus",
            "differential_authorized",
            "--tenant",
            "20",
            "--dimensions",
            "3",
            "--query-vector",
            "[1,0,0]",
            "--terms",
            "tenant",
            "--expected-pgvector",
            &vector_version,
            "--candidate-commit",
            "0000000000000000000000000000000000000000",
            "--package-sha256",
            "0000000000000000000000000000000000000000000000000000000000000000",
        ],
        Some("live_rag_reader_a"),
    )
    .await;
    assert!(
        empty.status.success(),
        "empty restricted recall failed: {}",
        String::from_utf8_lossy(&empty.stderr)
    );
    let empty_report: serde_json::Value =
        serde_json::from_slice(&empty.stdout).expect("parse empty recall");
    assert_eq!(empty_report["metrics"]["hybrid_exact_count"], 0);
    assert_eq!(empty_report["metrics"]["hybrid_candidate_count"], 0);
    assert!(empty_report["metrics"]["hybrid_recall_at_10"].is_null());
    db.execute("UPDATE live_rag.access SET tenant_id=20 WHERE document_id=1")
        .await;
    sqlx::query("SET ROLE live_rag_reader_a")
        .execute(&mut *conn)
        .await
        .expect("assume original reader");
    for view in [
        "application_authorized",
        "periodic_full_authorized",
        "differential_authorized",
    ] {
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM live_rag.{view}"
        )))
        .fetch_one(&mut *conn)
        .await
        .expect("check transferred row");
        assert_eq!(count, 0, "{view} exposed a transferred row before refresh");
    }
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset reader role");
    db.execute_seq(&[
        "SELECT live_rag.sync_application(ARRAY[1]::bigint[])",
        "SELECT live_rag.recompute_full()",
        "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
    ])
    .await;
    sqlx::query("SET ROLE live_rag_reader_b")
        .execute(&mut *conn)
        .await
        .expect("assume new reader");
    for view in [
        "application_authorized",
        "periodic_full_authorized",
        "differential_authorized",
    ] {
        let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT id FROM live_rag.{view} ORDER BY id"
        )))
        .fetch_all(&mut *conn)
        .await
        .expect("read transferred row");
        assert_eq!(ids, vec![1, 2]);
    }
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset reader role");
    db.execute("UPDATE live_rag.access SET visible=false WHERE document_id=2")
        .await;
    sqlx::query("SET ROLE live_rag_reader_b")
        .execute(&mut *conn)
        .await
        .expect("assume new reader");
    for view in [
        "application_authorized",
        "periodic_full_authorized",
        "differential_authorized",
    ] {
        let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT id FROM live_rag.{view} ORDER BY id"
        )))
        .fetch_all(&mut *conn)
        .await
        .expect("check hidden row");
        assert_eq!(ids, vec![1], "{view} exposed a hidden row before refresh");
    }
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset reader role");
    db.execute(
        "DELETE FROM live_rag.tenant_readers WHERE role_oid='live_rag_reader_b'::regrole::oid",
    )
    .await;
    sqlx::query("SET ROLE live_rag_reader_b")
        .execute(&mut *conn)
        .await
        .expect("assume revoked reader");
    for view in [
        "application_authorized",
        "periodic_full_authorized",
        "differential_authorized",
    ] {
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM live_rag.{view}"
        )))
        .fetch_one(&mut *conn)
        .await
        .expect("check revoked reader");
        assert_eq!(count, 0, "{view} exposed rows after access was revoked");
    }
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset reader role");
}

#[tokio::test]
async fn test_live_rag_wide_setup_and_worker_match() {
    let db = E2eDb::new().await.with_extension().await;
    let dimension = std::env::var("LIVE_RAG_TEST_DIM").unwrap_or_else(|_| "384".into());
    let expected: i32 = dimension
        .parse()
        .expect("LIVE_RAG_TEST_DIM must be an integer");
    assert!((1..=1536).contains(&expected));
    let setup = run_live_rag_script(&db, "setup_live_rag.py", &["--dimensions", &dimension]).await;
    assert!(
        setup.status.success(),
        "wide setup failed: {}",
        String::from_utf8_lossy(&setup.stderr)
    );
    db.execute_seq(&[
        "INSERT INTO embedding_demo.documents(id, body) VALUES (1, 'wide vector')",
        "INSERT INTO live_rag.access(document_id, tenant_id) VALUES (1, 10)",
        "SELECT embedding_demo.enqueue(1, 'model-v1')",
    ])
    .await;
    let output = run_live_rag_script(
        &db,
        "fake_embedding_worker.py",
        &["--model", "model-v1", "--dimensions", &dimension],
    )
    .await;
    assert!(
        output.status.success(),
        "wide worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let dimensions: i32 = db
        .query_scalar(
            "SELECT vector_dims(embedding) FROM embedding_demo.results WHERE document_id=1",
        )
        .await;
    assert_eq!(dimensions, expected);
    db.execute_seq(&[
        "SELECT live_rag.sync_application(ARRAY[1]::bigint[])",
        "SELECT live_rag.recompute_full()",
    ])
    .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('live_rag.differential')",
    )
    .await
    .expect("refresh wide differential corpus");
    let differential: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_schema='live_rag' AND s.pgt_name='differential' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(differential, "wide corpus must refresh differentially");
    for table in ["application", "periodic_full", "differential"] {
        db.assert_st_matches_query(&format!("live_rag.{table}"), "SELECT * FROM live_rag.truth")
            .await;
    }
    let vector_version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname='vector'")
        .await;
    let query_vector = format!("[{}]", vec!["1"; expected as usize].join(","));
    let report = run_live_rag_script(
        &db,
        "measure_live_rag_recall.py",
        &[
            "--corpus",
            "differential_current",
            "--tenant",
            "10",
            "--dimensions",
            &dimension,
            "--query-vector",
            &query_vector,
            "--terms",
            "wide",
            "--expected-pgvector",
            &vector_version,
            "--candidate-commit",
            "0000000000000000000000000000000000000000",
            "--package-sha256",
            "0000000000000000000000000000000000000000000000000000000000000000",
        ],
    )
    .await;
    assert!(
        report.status.success(),
        "wide recall failed: {}",
        String::from_utf8_lossy(&report.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&report.stdout).expect("parse wide recall");
    assert_eq!(report["metrics"]["hybrid_recall_at_10"], 1.0);
}

#[tokio::test]
async fn test_revision_safe_publication_enqueues_missing_work_without_duplicates() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(include_str!("fixtures/pgvector_revision_pipeline.sql"))
        .execute(&db.pool)
        .await
        .expect("install revision fixture");
    db.execute("INSERT INTO embedding_demo.documents (id, body, published) VALUES (1, 'A', false)")
        .await;
    enqueue(&db, "model-v1").await;
    assert_eq!(db.count("embedding_demo.jobs").await, 0);
    db.execute_seq(&[
        "BEGIN",
        "UPDATE embedding_demo.documents SET published = true WHERE id = 1",
        "SELECT embedding_demo.enqueue(1, 'model-v1')",
        "ROLLBACK",
    ])
    .await;
    assert_eq!(db.count("embedding_demo.jobs").await, 0);
    let published: bool = db
        .query_scalar("SELECT published FROM embedding_demo.documents WHERE id = 1")
        .await;
    assert!(!published);

    db.execute_seq(&[
        "BEGIN",
        "UPDATE embedding_demo.documents SET published = true WHERE id = 1",
        "SELECT embedding_demo.enqueue(1, 'model-v1')",
        "COMMIT",
    ])
    .await;
    assert_eq!(db.count("embedding_demo.jobs").await, 1);
    refresh_differential(&db, "corpus_v1").await;
    assert_eq!(db.count("embedding_demo.corpus_v1").await, 0);
    let job = claim(&db, "model-v1").await;
    assert!(complete(&db, &job, "model-v1").await);
    refresh_differential(&db, "corpus_v1").await;
    assert_eq!(db.count("embedding_demo.corpus_v1").await, 1);

    db.execute("UPDATE embedding_demo.documents SET published = false WHERE id = 1")
        .await;
    refresh_differential(&db, "corpus_v1").await;
    assert_eq!(db.count("embedding_demo.corpus_v1").await, 0);
    db.execute_seq(&[
        "BEGIN",
        "UPDATE embedding_demo.documents SET published = true WHERE id = 1",
        "SELECT embedding_demo.enqueue(1, 'model-v1')",
        "COMMIT",
    ])
    .await;
    assert_eq!(db.count("embedding_demo.jobs").await, 1);
    refresh_differential(&db, "corpus_v1").await;
    let row: (String, String) =
        sqlx::query_as("SELECT body, embedding::text FROM embedding_demo.corpus_v1 WHERE id = 1")
            .fetch_one(&db.pool)
            .await
            .expect("read republished corpus row");
    assert_eq!(row, ("A".to_string(), "[1,0,0]".to_string()));
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
    db.execute("CREATE INDEX corpus_v2_ann ON embedding_demo.corpus_v2 USING hnsw ((embedding::vector(3)) vector_cosine_ops) WHERE id < 0").await;
    let blocked = db
        .try_execute("SELECT embedding_demo.switch_model('model-v2')")
        .await
        .expect_err("partial ANN index must block cutover");
    assert!(blocked.to_string().contains("ANN index is unavailable"));
    db.execute("DROP INDEX embedding_demo.corpus_v2_ann").await;
    db.execute("CREATE INDEX corpus_v2_ann ON embedding_demo.corpus_v2 USING hnsw (((embedding::vector(3) + '[1,0,0]'::vector)::vector(3)) vector_cosine_ops)").await;
    let blocked = db
        .try_execute("SELECT embedding_demo.switch_model('model-v2')")
        .await
        .expect_err("ANN index on a different expression must block cutover");
    assert!(blocked.to_string().contains("ANN index is unavailable"));
    db.execute("DROP INDEX embedding_demo.corpus_v2_ann").await;
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
