//! Assertion and AddressSanitizer coverage for the selected unsafe boundaries.
mod e2e;

use e2e::E2eDb;
use sqlx::{PgConnection, Row};
use std::{process::Command, time::Duration};

type PgTrickleSessionDiagnostics = (
    i32,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);
type SchedulerJobOutcome = (String, Option<String>, Option<String>, Option<String>);
type SchedulerJobDiagnostic = (i64, String, Option<String>, Option<String>, Option<String>);

async fn configure_fast_scheduler(db: &E2eDb) {
    db.alter_system_set_and_wait("pg_trickle.scheduler_interval_ms", "100", "100")
        .await;
    db.alter_system_set_and_wait("pg_trickle.min_schedule_seconds", "1", "1")
        .await;
    db.alter_system_set_and_wait("pg_trickle.auto_backoff", "off", "off")
        .await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler did not appear in pg_stat_activity"
    );
}

fn docker_output(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker command is available for the Testcontainers E2E run")
}

#[tokio::test]
async fn test_instrumented_pg18_boot_and_assertions_enabled() {
    let db = E2eDb::new().await.with_extension().await;
    let assertions: String = sqlx::query_scalar("SHOW debug_assertions")
        .fetch_one(&db.pool)
        .await
        .expect("PostgreSQL reports assertion state");
    assert_eq!(
        assertions, "on",
        "PostgreSQL was built with --enable-cassert"
    );
    let version: String = db.query_scalar("SELECT version()").await;
    assert!(
        version.contains("PostgreSQL 18.6"),
        "unexpected server version: {version}"
    );
    eprintln!("instrumented PostgreSQL version: {version}");

    for (binary, path) in [
        ("postgres", "/usr/local/pgsql/bin/postgres"),
        ("pg_trickle.so", "/usr/local/pgsql/lib/pg_trickle.so"),
    ] {
        let output = docker_output(&[
            "exec",
            db.container_id(),
            "sh",
            "-c",
            &format!("nm -D '{path}' | grep -E '__asan_report_(load|store)'"),
        ]);
        assert!(
            output.status.success(),
            "{binary} must expose AddressSanitizer instrumentation: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        eprintln!(
            "ASan symbols in {binary}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

#[tokio::test]
async fn test_instrumented_invalid_memory_probe_is_detected() {
    let db = E2eDb::new().await.with_extension().await;
    let output = docker_output(&["exec", db.container_id(), "/usr/local/bin/asan_probe"]);
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "invalid-memory probe must fail");
    assert!(
        diagnostic.contains("ERROR: AddressSanitizer: heap-buffer-overflow")
            && diagnostic.contains("asan_probe.c"),
        "probe must fail at its known ASan fault site; stderr was: {diagnostic}"
    );
    eprintln!(
        "ASAN_EXPECTED_PROBE_DIAGNOSTIC_BEGIN\n{diagnostic}\nASAN_EXPECTED_PROBE_DIAGNOSTIC_END"
    );
}

#[tokio::test]
async fn test_pipeline_copy_null_and_toasted_rows_survive_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE unsafe_copy_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO unsafe_copy_src SELECT n, CASE WHEN n % 3 = 0 THEN NULL ELSE (SELECT string_agg(md5(n::text || ':' || seq::text), '' ORDER BY seq) FROM generate_series(1, 76) AS chunks(seq)) END FROM generate_series(1, 32) n")
        .await;
    let toast_relation: String = db
        .query_scalar(
            "SELECT format('%I.%I', toast_ns.nspname, toast.relname) \
             FROM pg_class AS source \
             JOIN pg_class AS toast ON toast.oid = source.reltoastrelid \
             JOIN pg_namespace AS toast_ns ON toast_ns.oid = toast.relnamespace \
             WHERE source.oid = 'unsafe_copy_src'::regclass \
               AND source.reltoastrelid <> 0",
        )
        .await;
    let toast_chunks: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {toast_relation}"
    )))
    .fetch_one(&db.pool)
    .await
    .expect("read chunks from the source table's actual TOAST relation");
    assert!(
        toast_chunks > 0,
        "source table's TOAST relation {toast_relation} must contain chunks before refresh"
    );
    db.create_st(
        "unsafe_copy_st",
        "SELECT id, val FROM unsafe_copy_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;

    for cycle in 0..3 {
        db.execute(&format!(
            "INSERT INTO unsafe_copy_src VALUES ({}, NULL), ({}, (SELECT string_agg(md5('insert-{}:' || seq::text), '' ORDER BY seq) FROM generate_series(1, 76) AS chunks(seq)))",
            100 + cycle * 2,
            101 + cycle * 2,
            101 + cycle * 2
        ))
        .await;
        db.execute(&format!(
            "UPDATE unsafe_copy_src SET val = (SELECT string_agg(md5('update-{}-{}:' || seq::text), '' ORDER BY seq) FROM generate_series(1, 76) AS chunks(seq)) WHERE id = {}",
            cycle,
            1 + cycle,
            1 + cycle
        ))
        .await;
        db.execute(&format!(
            "DELETE FROM unsafe_copy_src WHERE id = {}",
            2 + cycle
        ))
        .await;
        let mut conn = db.pool.acquire().await.expect("acquire refresh backend");
        sqlx::query("SET pg_trickle.pipeline_batch_size = 1")
            .execute(&mut *conn)
            .await
            .expect("force and log the bounded tuple-copy pipeline");
        sqlx::query("SET log_min_messages = DEBUG1")
            .execute(&mut *conn)
            .await
            .expect("enable the pipeline path witness");
        sqlx::query("SELECT pgtrickle.refresh_stream_table('unsafe_copy_st')")
            .execute(&mut *conn)
            .await
            .expect("refresh through the production pipeline");
        db.assert_st_matches_query("unsafe_copy_st", "SELECT id, val FROM unsafe_copy_src")
            .await;
    }

    let logs = docker_output(&["logs", db.container_id()]);
    let logs = format!(
        "{}\n{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
    assert!(
        logs.contains("v0.87 pipeline") || logs.contains("pipeline batches="),
        "refresh must witness the production tuple-copy pipeline path"
    );
}

async fn session_state(conn: &mut PgConnection) -> (String, String, String, String) {
    let row = sqlx::query(
        "SELECT current_user::text, current_setting('role'), current_setting('search_path'), current_setting('row_security')",
    )
    .fetch_one(conn)
    .await
    .expect("read same-backend authorization state");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

async fn assert_allowed_and_denied(conn: &mut PgConnection, prefix: &str) {
    let allowed: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {prefix}_allowed LIMIT 1"
    )))
    .fetch_one(&mut *conn)
    .await
    .expect("restored caller can read its allowed table");
    assert_eq!(allowed, 1);
    let denied = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {prefix}_denied"
    )))
    .fetch_optional(&mut *conn)
    .await;
    assert!(denied.is_err(), "restored caller remains denied");
}

async fn assert_in_backend_context_restored(
    db: &E2eDb,
    conn: &mut PgConnection,
    context: &str,
    stream_table: &str,
    expected_message: &str,
) {
    let backend_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await
        .expect("read the backend that will run the direct API body probe");
    let error = sqlx::query("SELECT pgtrickle.e2e_catch_security_context_error($1, $2)")
        .bind(context)
        .bind(stream_table)
        .execute(&mut *conn)
        .await
        .expect_err("instrumented API body rethrows its expected ERROR after recording state");
    assert!(
        error.to_string().contains(expected_message),
        "probe propagated the expected PostgreSQL ERROR: {error}"
    );

    let logs = docker_output(&["logs", db.container_id()]);
    let logs = format!(
        "{}\n{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
    const MARKER: &str = "PGTRICKLE_E2E_SECURITY_CONTEXT_STATE:";
    let witness = logs
        .lines()
        .filter_map(|line| line.split_once(MARKER).map(|(_, json)| json))
        .filter_map(|json| serde_json::from_str::<serde_json::Value>(json.trim()).ok())
        .find(|witness| {
            witness["context"] == context
                && witness["backend_pid"]
                    .as_str()
                    .and_then(|pid| pid.parse::<i32>().ok())
                    == Some(backend_pid)
        })
        .unwrap_or_else(|| {
            panic!(
                "backend log must contain the in-catch state witness for context {context}, PID {backend_pid}; logs:\n{logs}"
            )
        });
    assert!(
        witness["error"]
            .as_str()
            .is_some_and(|message| message.contains(expected_message)),
        "witness records the expected ERROR: {witness}"
    );
    let before = witness["before"]
        .as_array()
        .expect("witness has pre-call state");
    let after = witness["after"]
        .as_array()
        .expect("witness has post-catch state");
    assert_eq!(
        before.len(),
        5,
        "state includes PID, current_user, role, path, and row_security"
    );
    assert_eq!(
        after.len(),
        5,
        "state includes PID, current_user, role, path, and row_security"
    );
    assert_eq!(
        before[0].as_str().and_then(|pid| pid.parse::<i32>().ok()),
        Some(backend_pid),
        "witness uses the pinned backend"
    );
    assert_eq!(
        after[0].as_str().and_then(|pid| pid.parse::<i32>().ok()),
        Some(backend_pid),
        "catch remains on the pinned backend"
    );
    assert_eq!(
        &before[1..],
        &after[1..],
        "{context} context cleanup restores role, search_path, and row_security inside the running backend before SQL rollback"
    );
}

#[tokio::test]
async fn test_owner_context_pg_error_restores_state_and_continues() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute_seq(&[
        "CREATE ROLE unsafe_owner LOGIN",
        "CREATE ROLE unsafe_caller LOGIN",
        "GRANT unsafe_owner TO unsafe_caller",
        "CREATE TABLE unsafe_owner_allowed (id INT)",
        "INSERT INTO unsafe_owner_allowed VALUES (1)",
        "CREATE TABLE unsafe_owner_denied (id INT)",
    ])
    .await;
    db.execute_seq(&[
        "GRANT SELECT ON unsafe_owner_allowed TO unsafe_caller",
        "GRANT USAGE ON SCHEMA pgtrickle TO unsafe_caller",
        "GRANT USAGE ON SCHEMA pgtrickle TO unsafe_owner",
    ])
    .await;
    db.execute_seq(&[
        "CREATE TABLE unsafe_owner_src (id INT PRIMARY KEY)",
        "INSERT INTO unsafe_owner_src VALUES (1)",
    ])
    .await;
    db.create_st(
        "unsafe_owner_st",
        "SELECT id, CASE WHEN id = 2 THEN 1 / (id - 2) ELSE 1 END AS value FROM unsafe_owner_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute_seq(&[
        "ALTER TABLE unsafe_owner_st OWNER TO unsafe_owner",
        "GRANT SELECT ON unsafe_owner_src TO unsafe_owner",
        "GRANT EXECUTE ON FUNCTION pgtrickle.refresh_stream_table(text) TO unsafe_caller",
        "GRANT EXECUTE ON FUNCTION pgtrickle.e2e_catch_security_context_error(text, text) TO unsafe_caller",
    ])
    .await;
    db.execute("INSERT INTO unsafe_owner_src VALUES (2)").await;

    let mut conn = db.pool.acquire().await.expect("acquire pinned backend");
    sqlx::query("SET ROLE unsafe_caller")
        .execute(&mut *conn)
        .await
        .expect("set caller baseline");
    sqlx::query("SET search_path = public")
        .execute(&mut *conn)
        .await
        .expect("set caller search path baseline");
    sqlx::query("SET row_security = off")
        .execute(&mut *conn)
        .await
        .expect("set caller row-security baseline");
    let before = session_state(&mut conn).await;
    let error = sqlx::query("SELECT pgtrickle.refresh_stream_table('unsafe_owner_st')")
        .execute(&mut *conn)
        .await
        .expect_err("refresh must raise the controlled division-by-zero ERROR");
    assert!(error.to_string().contains("division by zero"), "{error}");
    assert_eq!(session_state(&mut conn).await, before);
    assert_in_backend_context_restored(
        &db,
        &mut conn,
        "owner",
        "unsafe_owner_st",
        "division by zero",
    )
    .await;
    assert_eq!(session_state(&mut conn).await, before);
    assert_allowed_and_denied(&mut conn, "unsafe_owner").await;
}

#[tokio::test]
async fn test_caller_context_pg_error_restores_state_and_continues() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE ROLE unsafe_outbox_caller LOGIN").await;
    db.execute_seq(&[
        "CREATE TABLE unsafe_caller_allowed (id INT)",
        "INSERT INTO unsafe_caller_allowed VALUES (1)",
        "CREATE TABLE unsafe_caller_denied (id INT)",
        "CREATE TABLE unsafe_caller_src (id INT PRIMARY KEY, val TEXT)",
        "INSERT INTO unsafe_caller_src VALUES (1, 'a')",
    ])
    .await;
    db.create_st(
        "unsafe_caller_st",
        "SELECT id, val FROM unsafe_caller_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE EXTENSION IF NOT EXISTS pg_tide VERSION '0.53.0'")
        .await;
    db.execute_seq(&[
        "ALTER TABLE unsafe_caller_st OWNER TO unsafe_outbox_caller",
        "GRANT USAGE ON SCHEMA pgtrickle, tide TO unsafe_outbox_caller",
        "GRANT SELECT ON unsafe_caller_src, tide.tide_outbox_config, unsafe_caller_allowed TO unsafe_outbox_caller",
        "GRANT EXECUTE ON FUNCTION pgtrickle.attach_outbox(text, integer, integer) TO unsafe_outbox_caller",
        "GRANT EXECUTE ON FUNCTION pgtrickle.e2e_catch_security_context_error(text, text) TO unsafe_outbox_caller",
        "CREATE OR REPLACE FUNCTION tide.outbox_create(p_name text, p_retention_hours integer DEFAULT 24, p_inline_threshold integer DEFAULT 10000) RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('search_path', 'pg_temp', false); PERFORM set_config('row_security', 'off', false); RAISE EXCEPTION 'unsafe caller context probe'; END $$",
    ])
    .await;

    let mut conn = db.pool.acquire().await.expect("acquire pinned backend");
    sqlx::query("SET ROLE unsafe_outbox_caller")
        .execute(&mut *conn)
        .await
        .expect("set caller baseline");
    sqlx::query("SET search_path = public")
        .execute(&mut *conn)
        .await
        .expect("set caller search path baseline");
    // The failure probe sets this to off, so an on baseline makes restoration observable.
    sqlx::query("SET row_security = on")
        .execute(&mut *conn)
        .await
        .expect("set caller row-security baseline");
    let before = session_state(&mut conn).await;
    let error = sqlx::query("SELECT pgtrickle.attach_outbox('unsafe_caller_st')")
        .execute(&mut *conn)
        .await
        .expect_err("outbox adapter must propagate its deliberate PostgreSQL ERROR");
    assert!(error.to_string().contains("unsafe caller context probe"));
    assert_eq!(session_state(&mut conn).await, before);
    assert_in_backend_context_restored(
        &db,
        &mut conn,
        "caller",
        "unsafe_caller_st",
        "unsafe caller context probe",
    )
    .await;
    assert_eq!(session_state(&mut conn).await, before);
    assert_allowed_and_denied(&mut conn, "unsafe_caller").await;
}

#[tokio::test]
async fn test_dispatch_worker_cancel_cleanup_and_restart_recovers() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.execute_seq(&[
        "CREATE TABLE unsafe_worker_src (id INT PRIMARY KEY, val TEXT)",
        "INSERT INTO unsafe_worker_src VALUES (1, 'a')",
    ])
    .await;
    db.create_st(
        "unsafe_worker_st",
        "SELECT id, val FROM unsafe_worker_src",
        "10s",
        "FULL",
    )
    .await;
    db.execute("INSERT INTO unsafe_worker_src VALUES (2, 'b')")
        .await;
    db.nudge_launcher_rescan().await;

    let mut blocker = db.pool.acquire().await.expect("acquire source lock");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("read source lock blocker PID");
    sqlx::query("BEGIN")
        .execute(&mut *blocker)
        .await
        .expect("begin source-lock transaction");
    // A FULL scheduled refresh must scan its source table. Lock that relation
    // so the real background worker stays active in PostgreSQL's lock wait.
    sqlx::query("LOCK TABLE unsafe_worker_src IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold source table while scheduled FULL refresh scans it");
    let worker = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let worker: Option<i32> = sqlx::query_scalar(
                "SELECT pid FROM pg_stat_activity WHERE application_name = 'pg_trickle_dispatcher' AND backend_type = 'pg_trickle refresh worker' AND wait_event_type = 'Lock' AND $1 = ANY(pg_blocking_pids(pid)) LIMIT 1",
            )
            .bind(blocker_pid)
            .fetch_optional(&db.pool)
            .await
            .expect("find active dispatched worker");
            if let Some(worker) = worker {
                break worker;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let worker = match worker {
        Ok(worker) => worker,
        Err(_) => {
            let diagnostics: Vec<PgTrickleSessionDiagnostics> = sqlx::query_as(
                    "SELECT pid, application_name, backend_type, state, wait_event_type, wait_event FROM pg_stat_activity WHERE backend_type LIKE 'pg_trickle%' ORDER BY pid",
                )
                .fetch_all(&db.pool)
                .await
                .expect("inspect pg_trickle workers after dispatch timeout");
            panic!(
                "scheduler did not dispatch a worker blocked by PID {blocker_pid}; pg_trickle sessions: {diagnostics:?}"
            );
        }
    };
    let job_id: i64 = sqlx::query_scalar(
        "SELECT job_id FROM pgtrickle.pgt_scheduler_jobs WHERE worker_pid = $1 ORDER BY started_at DESC LIMIT 1",
    )
    .bind(worker)
    .fetch_one(&db.pool)
    .await
    .expect("find durable scheduler job for the witnessed worker");
    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(worker)
        .fetch_one(&db.pool)
        .await
        .expect("cancel active dispatched worker");
    assert!(cancelled, "cancel reaches witnessed refresh worker PID");
    let before_recovery: i64 = sqlx::query_scalar("SELECT count(*) FROM unsafe_worker_st")
        .fetch_one(&db.pool)
        .await
        .expect("read last committed output after worker cancellation");
    assert_eq!(
        before_recovery, 1,
        "cancellation preserves committed output"
    );
    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .expect("release source lock");

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let outcome: Option<SchedulerJobOutcome> = sqlx::query_as(
                "SELECT status, outcome_code, outcome_sqlstate, outcome_detail FROM pgtrickle.pgt_scheduler_jobs WHERE job_id = $1",
            )
            .bind(job_id)
            .fetch_optional(&db.pool)
            .await
            .expect("inspect dispatched worker job outcome");
            if let Some((status, outcome_code, sqlstate, outcome_detail)) = outcome
                && status != "RUNNING"
                && status != "QUEUED"
            {
                assert_eq!(status, "PERMANENT_FAILED");
                assert_eq!(outcome_code.as_deref(), Some("CANCELLED"));
                assert_eq!(
                    sqlstate.as_deref(),
                    Some("57014"),
                    "unexpected SQLSTATE conversion for cancellation outcome: {outcome_detail:?}"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("cancelled worker records its query-cancel error");

    let committed_after_cancel: i64 = sqlx::query_scalar("SELECT count(*) FROM unsafe_worker_st")
        .fetch_one(&db.pool)
        .await
        .expect("read committed output after worker cancellation");
    assert_eq!(
        committed_after_cancel, 1,
        "cancelled worker preserves the committed checkpoint"
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let active: i32 =
                sqlx::query_scalar("SELECT active_workers FROM pgtrickle.metrics_summary()")
                    .fetch_one(&db.pool)
                    .await
                    .expect("read worker slot accounting after cancellation");
            if active == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("cancelled worker slot is reconciled before recovery");

    db.execute("SELECT pgtrickle.resume_stream_table('unsafe_worker_st')")
        .await;
    db.nudge_launcher_rescan().await;

    let recovered = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM unsafe_worker_st")
                .fetch_one(&db.pool)
                .await
                .expect("read committed output");
            if count == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await;
    if recovered.is_err() {
        let status: String = sqlx::query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'unsafe_worker_st'",
        )
        .fetch_one(&db.pool)
        .await
        .expect("inspect stream status after scheduled recovery timeout");
        let jobs: Vec<SchedulerJobDiagnostic> = sqlx::query_as(
            "SELECT job_id, status, outcome_code, outcome_sqlstate, outcome_detail FROM pgtrickle.pgt_scheduler_jobs WHERE root_pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'unsafe_worker_st') ORDER BY job_id",
        )
        .fetch_all(&db.pool)
        .await
        .expect("inspect dispatched job outcomes after scheduled recovery timeout");
        panic!(
            "replacement worker did not complete after resume; stream status={status}, jobs={jobs:?}"
        );
    }
    db.assert_st_matches_query("unsafe_worker_st", "SELECT id, val FROM unsafe_worker_src")
        .await;
    let active: i32 = sqlx::query_scalar("SELECT active_workers FROM pgtrickle.metrics_summary()")
        .fetch_one(&db.pool)
        .await
        .expect("read worker slot accounting");
    assert_eq!(active, 0, "cancelled worker slot is reconciled");
}

#[tokio::test]
async fn test_zz_instrumented_diagnostics_are_clean() {
    let db = E2eDb::new().await.with_extension().await;
    let stopped = docker_output(&["stop", db.container_id()]);
    assert!(
        stopped.status.success(),
        "gracefully stop PostgreSQL so its detect_leaks=1 exit report is included: {}",
        String::from_utf8_lossy(&stopped.stderr)
    );
    eprintln!(
        "ASAN_CONTAINER_STOP_BEGIN\n{}{}\nASAN_CONTAINER_STOP_END",
        String::from_utf8_lossy(&stopped.stdout),
        String::from_utf8_lossy(&stopped.stderr)
    );
    let output = docker_output(&["logs", db.container_id()]);
    assert!(
        output.status.success(),
        "raw PostgreSQL container logs are available"
    );
    let logs = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for diagnostic in [
        "ERROR: AddressSanitizer:",
        "ERROR: LeakSanitizer:",
        "LeakSanitizer: detected memory leaks",
        "Assertion failed",
        "FailedAssertion",
        "TRAP: ",
    ] {
        assert!(
            !logs.contains(diagnostic),
            "selected workloads produced unexplained diagnostic {diagnostic}: {logs}"
        );
    }
    eprintln!("ASAN_RAW_POSTGRES_LOGS_BEGIN\n{logs}\nASAN_RAW_POSTGRES_LOGS_END");
}
