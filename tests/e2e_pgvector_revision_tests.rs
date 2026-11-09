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
