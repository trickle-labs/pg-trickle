//! Transaction and concurrency schedules for refresh publication atomicity.

mod e2e;
#[allow(dead_code)]
#[path = "conformance/reference_clients.rs"]
mod reference_clients;

use e2e::{E2eDb, oracle};
use sqlx::{AssertSqlSafe, Postgres, pool::PoolConnection};
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Debug, PartialEq, Eq)]
struct RefreshProgress {
    frontier: Option<String>,
    data_timestamp: Option<String>,
    last_refresh_at: Option<String>,
    is_populated: bool,
    last_success_id: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct DeltaProgress {
    log_head: i64,
    batch_count: i64,
    payload_rows: i64,
}

fn check_delta_progress(expected: &DeltaProgress, actual: &DeltaProgress) -> Result<(), String> {
    if expected == actual {
        Ok(())
    } else {
        Err(format!(
            "output-delta checkpoint differs: expected {expected:?}, observed {actual:?}"
        ))
    }
}

#[derive(Debug)]
struct BarrierWitness {
    pid: i32,
    application_name: String,
    wait_event: String,
    blockers: Vec<i32>,
}

fn phase_code(phase: &str) -> i32 {
    match phase {
        "input_boundary" => 1,
        "after_apply" => 2,
        "during_finalize" => 3,
        _ => panic!("unknown atomicity phase {phase}"),
    }
}

async fn pgt_id(db: &E2eDb, name: &str) -> i64 {
    sqlx::query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1")
        .bind(name)
        .fetch_one(&db.pool)
        .await
        .unwrap_or_else(|error| panic!("failed to get pgt_id for {name}: {error}"))
}

async fn progress(db: &E2eDb, id: i64) -> RefreshProgress {
    let row: (Option<String>, Option<String>, Option<String>, bool, i64) = sqlx::query_as(
        "SELECT frontier::text, data_timestamp::text, last_refresh_at::text, is_populated,
                COALESCE((SELECT max(refresh_id) FROM pgtrickle.pgt_refresh_history
                          WHERE pgt_id = $1 AND status = 'COMPLETED'), 0)
         FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap_or_else(|error| panic!("failed to read refresh progress for {id}: {error}"));
    RefreshProgress {
        frontier: row.0,
        data_timestamp: row.1,
        last_refresh_at: row.2,
        is_populated: row.3,
        last_success_id: row.4,
    }
}

async fn assert_committed_checkpoint(
    db: &E2eDb,
    id: i64,
    stream_table: &str,
    checkpoint: &str,
    expected: &RefreshProgress,
) {
    if let Err(diff) = oracle::compare_sts(db, stream_table, checkpoint).await {
        panic!("public output changed across an uncommitted refresh: {diff}");
    }
    let actual = progress(db, id).await;
    assert_eq!(
        &actual, expected,
        "frontier, data timestamp, population state, or successful history advanced"
    );
}

async fn checkpoint_mismatch(
    db: &E2eDb,
    id: i64,
    stream_table: &str,
    checkpoint: &str,
    expected: &RefreshProgress,
) -> Result<(), String> {
    if let Err(diff) = oracle::compare_sts(db, stream_table, checkpoint).await {
        return Err(format!("public output changed: {diff}"));
    }
    let actual = progress(db, id).await;
    if actual.frontier != expected.frontier {
        return Err(format!(
            "frontier changed without output: {:?} -> {:?}",
            expected.frontier, actual.frontier
        ));
    }
    if actual.data_timestamp != expected.data_timestamp
        || actual.last_refresh_at != expected.last_refresh_at
        || actual.is_populated != expected.is_populated
        || actual.last_success_id != expected.last_success_id
    {
        return Err(format!(
            "refresh progress changed without output: {actual:?}"
        ));
    }
    Ok(())
}

async fn snapshot_table(db: &E2eDb, source: &str, checkpoint: &str) {
    db.execute(&format!("CREATE TABLE {checkpoint} AS TABLE {source}"))
        .await;
}

async fn set_chaos(db: &E2eDb, stream_name: &str, phase: &str) {
    db.alter_system_set_and_wait("pg_trickle.test_chaos_phase", &format!("'{phase}'"), phase)
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_for_table",
        &format!("'{stream_name}'"),
        stream_name,
    )
    .await;
}

async fn clear_chaos(db: &E2eDb) {
    db.alter_system_set_and_wait("pg_trickle.test_chaos_for_table", "''", "")
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_phase",
        "'before_apply'",
        "before_apply",
    )
    .await;
}

async fn configure_scheduler(db: &E2eDb) {
    for (setting, value, expected) in [
        ("pg_trickle.scheduler_interval_ms", "100", "100"),
        ("pg_trickle.min_schedule_seconds", "1", "1"),
        ("pg_trickle.auto_backoff", "off", "off"),
        ("pg_trickle.max_consecutive_errors", "1", "1"),
        ("pg_trickle.parallel_refresh_mode", "'off'", "off"),
    ] {
        db.alter_system_set_and_wait(setting, value, expected).await;
    }
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler did not appear"
    );
}

async fn hold_barrier(db: &E2eDb, id: i64, phase: &str) -> (PoolConnection<Postgres>, i32) {
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("failed to acquire barrier connection");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await
        .expect("failed to get barrier backend PID");
    let lock_id = i32::try_from(id).expect("fixture pgt_id fits advisory-lock key");
    sqlx::query("SELECT pg_advisory_lock($1, $2)")
        .bind(lock_id)
        .bind(phase_code(phase))
        .execute(&mut *conn)
        .await
        .expect("failed to hold the refresh barrier");
    (conn, pid)
}

async fn release_barrier(conn: &mut PoolConnection<Postgres>, id: i64, phase: &str) {
    let lock_id = i32::try_from(id).expect("fixture pgt_id fits advisory-lock key");
    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1, $2)")
        .bind(lock_id)
        .bind(phase_code(phase))
        .fetch_one(&mut **conn)
        .await
        .expect("failed to release the refresh barrier");
    assert!(released, "test observer did not own the refresh barrier");
}

async fn wait_for_barrier(
    db: &E2eDb,
    id: i64,
    phase: &str,
    blocker_pid: i32,
    timeout: Duration,
) -> BarrierWitness {
    let prefix = format!("pgt_atomicity:{id}:{phase}:%");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let witness: Option<(i32, String, String, Vec<i32>)> = sqlx::query_as(
            "SELECT a.pid, a.application_name, a.wait_event, pg_blocking_pids(a.pid)
             FROM pg_stat_activity a
             WHERE a.application_name LIKE $1 AND a.wait_event_type = 'Lock'
               AND $2 = ANY(pg_blocking_pids(a.pid))",
        )
        .bind(&prefix)
        .bind(blocker_pid)
        .fetch_optional(&db.pool)
        .await
        .expect("failed to inspect the refresh barrier");
        if let Some((pid, application_name, wait_event, blockers)) = witness {
            assert!(blockers.contains(&blocker_pid));
            return BarrierWitness {
                pid,
                application_name,
                wait_event,
                blockers,
            };
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("refresh did not reach {phase} barrier for pgt_id={id}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn start_manual_refresh(
    db: &E2eDb,
    name: &str,
) -> (JoinHandle<Result<(), sqlx::Error>>, i32) {
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("failed to acquire manual refresh connection");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await
        .expect("failed to get manual refresh backend PID");
    let name = name.to_owned();
    let task = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
            .bind(name)
            .execute(&mut *conn)
            .await
            .map(|_| ())
    });
    (task, pid)
}

async fn wait_for_scheduler_failure(db: &E2eDb, id: i64) -> (i64, String, String) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let query = sqlx::query_as(
            "SELECT refresh_id, action, error_message
             FROM pgtrickle.pgt_refresh_history
             WHERE pgt_id = $1 AND status = 'FAILED' AND initiated_by = 'SCHEDULER'
             ORDER BY refresh_id DESC LIMIT 1",
        )
        .bind(id)
        .fetch_optional(&db.pool)
        .await;
        let failure: Option<(i64, String, Option<String>)> = match query {
            Ok(failure) => failure,
            Err(error) => {
                let logs = tokio::process::Command::new("docker")
                    .args(["logs", "--tail", "200", db.container_id()])
                    .output()
                    .await
                    .map(|output| {
                        format!(
                            "{}{}",
                            String::from_utf8_lossy(&output.stdout),
                            String::from_utf8_lossy(&output.stderr)
                        )
                    })
                    .unwrap_or_else(|log_error| {
                        format!("could not read container logs: {log_error}")
                    });
                panic!("failed to inspect scheduler failure history: {error}\n{logs}");
            }
        };
        if let Some((refresh_id, action, message)) = failure {
            return (refresh_id, action, message.unwrap_or_default());
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("scheduler did not persist a failed refresh for pgt_id={id}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_scheduler_status(db: &E2eDb, name: &str, expected: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let status: String = sqlx::query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1",
        )
        .bind(name)
        .fetch_one(&db.pool)
        .await
        .expect("failed to inspect scheduler status");
        if status == expected {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("stream table {name} stayed {status}, expected {expected}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_scheduler_recovery(db: &E2eDb, id: i64, failed_id: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let recovered: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM pgtrickle.pgt_refresh_history
                 WHERE pgt_id = $1 AND refresh_id > $2 AND status = 'COMPLETED'
                   AND initiated_by = 'SCHEDULER' AND action = 'REINITIALIZE')",
        )
        .bind(id)
        .bind(failed_id)
        .fetch_one(&db.pool)
        .await
        .expect("failed to inspect scheduler recovery history");
        if recovered {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("scheduler did not recover pgt_id={id} after failure {failed_id}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn assert_recovered_action(db: &E2eDb, name: &str, query: &str, expected_action: &str) {
    if let Err(diff) = oracle::compare_st_to_query(db, &format!("public.{name}"), query).await {
        panic!("recovered output differs from committed source: {diff}");
    }
    let history: (String, String) = sqlx::query_as(
        "SELECT action, initiated_by FROM pgtrickle.pgt_refresh_history h
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id)
         WHERE s.pgt_name = $1 AND h.status = 'COMPLETED'
         ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .bind(name)
    .fetch_one(&db.pool)
    .await
    .expect("successful recovery history should exist");
    assert_eq!(history.0, expected_action);
    assert!(matches!(history.1.as_str(), "MANUAL" | "SCHEDULER"));
}

async fn run_apply_or_finalize_failure_pair(phase: &str) {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    configure_scheduler(&db).await;

    let stem = format!("atomic_{phase}");
    let source = format!("{stem}_source");
    let manual = format!("{stem}_manual");
    let scheduler = format!("{stem}_scheduler");
    let query = format!("SELECT id, value FROM public.{source}");
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (1, 'baseline')"
    ))
    .await;
    db.create_st(&manual, &query, "1h", "DIFFERENTIAL").await;
    db.refresh_st(&manual).await;

    let manual_id = pgt_id(&db, &manual).await;
    let manual_checkpoint = format!("public.{manual}_checkpoint");
    snapshot_table(&db, &format!("public.{manual}"), &manual_checkpoint).await;
    let manual_progress = progress(&db, manual_id).await;
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (2, 'manual-pending')"
    ))
    .await;
    set_chaos(&db, &manual, phase).await;
    let (mut manual_barrier, manual_blocker) = hold_barrier(&db, manual_id, phase).await;
    let (mut manual_task, _) = start_manual_refresh(&db, &manual).await;
    let manual_witness = tokio::select! {
        result = &mut manual_task => {
            let logs = tokio::process::Command::new("docker")
                .args(["logs", "--tail", "100", db.container_id()])
                .output()
                .await
                .map(|output| {
                    format!(
                        "{}{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    )
                })
                .unwrap_or_else(|error| format!("could not read container logs: {error}"));
            panic!("manual refresh returned before {phase} barrier: {result:?}\n{logs}");
        },
        witness = wait_for_barrier(
            &db,
            manual_id,
            phase,
            manual_blocker,
            Duration::from_secs(10),
        ) => witness,
    };
    assert!(manual_witness.pid > 0);
    assert!(!manual_witness.wait_event.is_empty());
    assert!(manual_witness.application_name.contains("rows=1"));
    assert_committed_checkpoint(
        &db,
        manual_id,
        &format!("public.{manual}"),
        &manual_checkpoint,
        &manual_progress,
    )
    .await;
    release_barrier(&mut manual_barrier, manual_id, phase).await;
    let manual_error = manual_task
        .await
        .expect("manual refresh task panicked")
        .expect_err("faulted manual refresh must fail")
        .to_string();
    assert!(
        manual_error.contains("PGT_TEST_FAILPOINT_REACHED"),
        "{manual_error}"
    );
    assert!(manual_error.contains(phase), "{manual_error}");
    assert!(manual_error.contains("observed_rows=1"), "{manual_error}");
    clear_chaos(&db).await;
    assert_committed_checkpoint(
        &db,
        manual_id,
        &format!("public.{manual}"),
        &manual_checkpoint,
        &manual_progress,
    )
    .await;
    db.refresh_st(&manual).await;
    assert_recovered_action(&db, &manual, &query, "DIFFERENTIAL").await;

    // The scheduled fixture starts at the current committed source result.
    db.create_st(&scheduler, &query, "1s", "DIFFERENTIAL").await;
    db.refresh_st(&scheduler).await;
    let scheduler_id = pgt_id(&db, &scheduler).await;
    let scheduler_checkpoint = format!("public.{scheduler}_checkpoint");
    snapshot_table(&db, &format!("public.{scheduler}"), &scheduler_checkpoint).await;
    let scheduler_progress = progress(&db, scheduler_id).await;
    set_chaos(&db, &scheduler, phase).await;
    let (mut scheduler_barrier, scheduler_blocker) = hold_barrier(&db, scheduler_id, phase).await;
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (3, 'scheduler-pending')"
    ))
    .await;
    let scheduler_witness = wait_for_barrier(
        &db,
        scheduler_id,
        phase,
        scheduler_blocker,
        Duration::from_secs(30),
    )
    .await;
    assert!(scheduler_witness.pid > 0);
    assert!(scheduler_witness.blockers.contains(&scheduler_blocker));
    assert_committed_checkpoint(
        &db,
        scheduler_id,
        &format!("public.{scheduler}"),
        &scheduler_checkpoint,
        &scheduler_progress,
    )
    .await;
    release_barrier(&mut scheduler_barrier, scheduler_id, phase).await;
    let (failed_id, action, message) = wait_for_scheduler_failure(&db, scheduler_id).await;
    assert_eq!(action, "DIFFERENTIAL");
    assert!(message.contains("PGT_TEST_FAILPOINT_REACHED"), "{message}");
    assert!(message.contains(phase), "{message}");
    assert!(message.contains("observed_rows=1"), "{message}");
    clear_chaos(&db).await;
    wait_for_scheduler_status(&db, &scheduler, "SUSPENDED").await;
    assert_committed_checkpoint(
        &db,
        scheduler_id,
        &format!("public.{scheduler}"),
        &scheduler_checkpoint,
        &scheduler_progress,
    )
    .await;
    db.execute(&format!(
        "SELECT pgtrickle.resume_stream_table('public.{scheduler}')"
    ))
    .await;
    wait_for_scheduler_recovery(&db, scheduler_id, failed_id).await;
    // resume_stream_table marks capture state for reinitialization before the scheduler resumes.
    assert_recovered_action(&db, &scheduler, &query, "REINITIALIZE").await;
}

#[tokio::test]
async fn test_refresh_after_apply_failure_rolls_back() {
    run_apply_or_finalize_failure_pair("after_apply").await;
}

#[tokio::test]
async fn test_refresh_finalization_failure_rolls_back() {
    run_apply_or_finalize_failure_pair("during_finalize").await;
}

#[tokio::test]
async fn test_refresh_caller_rollback_preserves_committed_state() {
    let db = E2eDb::new().await.with_extension().await;
    let source = "atomic_caller_source";
    let stream = "atomic_caller_st";
    let query = format!("SELECT id, value FROM public.{source}");
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!("INSERT INTO public.{source} VALUES (1, 'base')"))
        .await;
    db.create_st(stream, &query, "1h", "DIFFERENTIAL").await;
    db.refresh_st(stream).await;
    let id = pgt_id(&db, stream).await;
    let checkpoint = "public.atomic_caller_checkpoint";
    snapshot_table(&db, &format!("public.{stream}"), checkpoint).await;
    let baseline = progress(&db, id).await;
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (2, 'pending')"
    ))
    .await;

    let mut caller = db.pool.acquire().await.expect("acquire caller backend");
    sqlx::query("BEGIN")
        .execute(&mut *caller)
        .await
        .expect("begin caller transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
        .bind(stream)
        .execute(&mut *caller)
        .await
        .expect("manual refresh should succeed inside caller transaction");
    let caller_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM public.{stream}"
    )))
    .fetch_one(&mut *caller)
    .await
    .expect("caller should see its refreshed output");
    assert_eq!(caller_count, 2);
    assert_committed_checkpoint(&db, id, &format!("public.{stream}"), checkpoint, &baseline).await;
    sqlx::query("ROLLBACK")
        .execute(&mut *caller)
        .await
        .expect("rollback caller transaction");
    assert_committed_checkpoint(&db, id, &format!("public.{stream}"), checkpoint, &baseline).await;

    db.refresh_st(stream).await;
    assert_recovered_action(&db, stream, &query, "DIFFERENTIAL").await;
}

#[tokio::test]
async fn test_refresh_savepoint_rollback_preserves_outer_transaction() {
    let db = E2eDb::new().await.with_extension().await;
    let source = "atomic_savepoint_source";
    let stream = "atomic_savepoint_st";
    let query = format!("SELECT id, value FROM public.{source}");
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!("INSERT INTO public.{source} VALUES (1, 'base')"))
        .await;
    db.execute("CREATE TABLE public.atomic_outer_marker (value INT PRIMARY KEY)")
        .await;
    db.create_st(stream, &query, "1h", "DIFFERENTIAL").await;
    db.refresh_st(stream).await;
    let id = pgt_id(&db, stream).await;
    let checkpoint = "public.atomic_savepoint_checkpoint";
    snapshot_table(&db, &format!("public.{stream}"), checkpoint).await;
    let baseline = progress(&db, id).await;
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (2, 'pending')"
    ))
    .await;

    let mut caller = db.pool.acquire().await.expect("acquire caller backend");
    sqlx::query("BEGIN")
        .execute(&mut *caller)
        .await
        .expect("begin outer transaction");
    sqlx::query("INSERT INTO public.atomic_outer_marker VALUES (7)")
        .execute(&mut *caller)
        .await
        .expect("insert outer marker");
    sqlx::query("SAVEPOINT before_refresh")
        .execute(&mut *caller)
        .await
        .expect("create refresh savepoint");
    sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
        .bind(stream)
        .execute(&mut *caller)
        .await
        .expect("manual refresh should succeed inside savepoint");
    let caller_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM public.{stream}"
    )))
    .fetch_one(&mut *caller)
    .await
    .expect("caller should see refreshed output before rollback");
    assert_eq!(caller_count, 2);
    assert_committed_checkpoint(&db, id, &format!("public.{stream}"), checkpoint, &baseline).await;

    sqlx::query("ROLLBACK TO SAVEPOINT before_refresh")
        .execute(&mut *caller)
        .await
        .expect("rollback refresh savepoint");
    let after_rollback_count: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM public.{stream}"
    )))
    .fetch_one(&mut *caller)
    .await
    .expect("outer transaction should see the restored output");
    assert_eq!(after_rollback_count, 1);
    sqlx::query("COMMIT")
        .execute(&mut *caller)
        .await
        .expect("commit outer transaction");
    let marker_count: i64 = db
        .query_scalar("SELECT count(*) FROM public.atomic_outer_marker WHERE value = 7")
        .await;
    assert_eq!(marker_count, 1, "work before the savepoint must persist");
    assert_committed_checkpoint(&db, id, &format!("public.{stream}"), checkpoint, &baseline).await;

    db.refresh_st(stream).await;
    assert_recovered_action(&db, stream, &query, "DIFFERENTIAL").await;
}

#[tokio::test]
async fn test_refresh_two_callers_publish_one_consistent_result() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    let source = "atomic_compete_source";
    let stream = "atomic_compete_st";
    let query = format!("SELECT id, value FROM public.{source}");
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!("INSERT INTO public.{source} VALUES (1, 'base')"))
        .await;
    db.create_st(stream, &query, "1h", "DIFFERENTIAL").await;
    db.refresh_st(stream).await;
    let id = pgt_id(&db, stream).await;
    let checkpoint = "public.atomic_compete_checkpoint";
    snapshot_table(&db, &format!("public.{stream}"), checkpoint).await;
    let baseline = progress(&db, id).await;
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (2, 'pending')"
    ))
    .await;
    set_chaos(&db, stream, "input_boundary").await;
    let (mut barrier, blocker_pid) = hold_barrier(&db, id, "input_boundary").await;
    let (first, _) = start_manual_refresh(&db, stream).await;
    let witness = wait_for_barrier(
        &db,
        id,
        "input_boundary",
        blocker_pid,
        Duration::from_secs(10),
    )
    .await;
    assert!(witness.pid > 0);
    let notices = db
        .try_execute_with_notices(&format!(
            "SELECT pgtrickle.refresh_stream_table('{stream}')"
        ))
        .await
        .expect("overlapping refresh should be skipped cleanly");
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains("refresh skipped")),
        "second caller must observe the advisory-lock skip: {notices:?}"
    );
    assert_committed_checkpoint(&db, id, &format!("public.{stream}"), checkpoint, &baseline).await;
    release_barrier(&mut barrier, id, "input_boundary").await;
    first
        .await
        .expect("first refresh task panicked")
        .expect("first refresh should complete");
    clear_chaos(&db).await;
    assert_recovered_action(&db, stream, &query, "DIFFERENTIAL").await;
}

#[tokio::test]
async fn test_refresh_concurrent_writer_preserves_next_batch() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    let source = "atomic_writer_source";
    let stream = "atomic_writer_st";
    let query = format!("SELECT id, value FROM public.{source}");
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!("INSERT INTO public.{source} VALUES (1, 'base')"))
        .await;
    db.create_st(stream, &query, "1h", "DIFFERENTIAL").await;
    db.refresh_st(stream).await;
    let id = pgt_id(&db, stream).await;
    let baseline_table = "public.atomic_writer_checkpoint";
    snapshot_table(&db, &format!("public.{stream}"), baseline_table).await;
    let baseline = progress(&db, id).await;
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (2, 'first-batch')"
    ))
    .await;
    db.execute(&format!(
        "CREATE TABLE public.atomic_writer_first_batch AS {query}"
    ))
    .await;
    set_chaos(&db, stream, "input_boundary").await;
    let (mut barrier, blocker_pid) = hold_barrier(&db, id, "input_boundary").await;
    let (refresh, _) = start_manual_refresh(&db, stream).await;
    let witness = wait_for_barrier(
        &db,
        id,
        "input_boundary",
        blocker_pid,
        Duration::from_secs(10),
    )
    .await;
    assert!(witness.pid > 0);
    assert_committed_checkpoint(
        &db,
        id,
        &format!("public.{stream}"),
        baseline_table,
        &baseline,
    )
    .await;

    let mut writer = db.pool.acquire().await.expect("acquire source writer");
    sqlx::query("SET lock_timeout = '2s'")
        .execute(&mut *writer)
        .await
        .expect("set writer lock timeout");
    tokio::time::timeout(
        Duration::from_secs(3),
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO public.{source} VALUES (3, 'second-batch')"
        )))
        .execute(&mut *writer),
    )
    .await
    .expect("source writer must commit while refresh is active")
    .expect("concurrent source write must commit");
    db.execute(&format!(
        "CREATE TABLE public.atomic_writer_final AS {query}"
    ))
    .await;

    release_barrier(&mut barrier, id, "input_boundary").await;
    refresh
        .await
        .expect("manual refresh task panicked")
        .expect("bounded first refresh should complete");
    clear_chaos(&db).await;
    if let Err(diff) = oracle::compare_sts(
        &db,
        &format!("public.{stream}"),
        "public.atomic_writer_first_batch",
    )
    .await
    {
        panic!("first refresh crossed its captured input boundary: {diff}");
    }
    db.refresh_st(stream).await;
    if let Err(diff) = oracle::compare_sts(
        &db,
        &format!("public.{stream}"),
        "public.atomic_writer_final",
    )
    .await
    {
        panic!("later committed source batch was not recovered exactly: {diff}");
    }
}

#[tokio::test]
async fn test_refresh_auxiliary_state_failure_rolls_back() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    let left = "atomic_aux_left";
    let right = "atomic_aux_right";
    let stream = "atomic_aux_st";
    db.execute(&format!(
        "CREATE TABLE public.{left} (id SERIAL PRIMARY KEY, value INT NOT NULL)"
    ))
    .await;
    db.execute(&format!(
        "CREATE TABLE public.{right} (id SERIAL PRIMARY KEY, value INT NOT NULL)"
    ))
    .await;
    db.execute(&format!(
        "INSERT INTO public.{left} (value) VALUES (1), (2)"
    ))
    .await;
    db.execute(&format!(
        "INSERT INTO public.{right} (value) VALUES (2), (3)"
    ))
    .await;
    let query =
        format!("SELECT value FROM public.{left} INTERSECT SELECT value FROM public.{right}");
    // Set-operation maintenance is FULL-only; this fixture exercises its
    // transaction-owned private multiplicity relation.
    db.create_st(stream, &query, "1h", "FULL").await;
    db.refresh_st(stream).await;
    let id = pgt_id(&db, stream).await;
    let checkpoint = "public.atomic_aux_checkpoint";
    snapshot_table(&db, &format!("public.{stream}"), checkpoint).await;
    let baseline = progress(&db, id).await;
    let state_relation = format!("pgtrickle.\"__pgt_setop_state_{id}\"");
    let state_checkpoint = "public.atomic_aux_state_checkpoint";
    db.execute(&format!(
        "CREATE TABLE {state_checkpoint} AS TABLE {state_relation}"
    ))
    .await;
    let catalog_state: String = db
        .query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s.node_ordinal)::text, '[]') \
             FROM pgtrickle.pgt_set_operation_states s WHERE s.pgt_id = {id}"
        ))
        .await;
    assert_ne!(catalog_state, "[]", "fixture must have real set-op state");

    db.execute(&format!("INSERT INTO public.{left} (value) VALUES (9)"))
        .await;
    db.execute(&format!("INSERT INTO public.{right} (value) VALUES (9)"))
        .await;
    set_chaos(&db, stream, "after_apply").await;
    let (mut barrier, blocker_pid) = hold_barrier(&db, id, "after_apply").await;
    let (refresh, _) = start_manual_refresh(&db, stream).await;
    let witness =
        wait_for_barrier(&db, id, "after_apply", blocker_pid, Duration::from_secs(10)).await;
    let applied_rows = witness
        .application_name
        .rsplit_once("rows=")
        .and_then(|(_, rows)| rows.parse::<i64>().ok())
        .unwrap_or_default();
    assert!(
        applied_rows > 0,
        "set-operation barrier must witness applied rows: {}",
        witness.application_name
    );
    release_barrier(&mut barrier, id, "after_apply").await;
    let error = refresh
        .await
        .expect("set-op refresh task panicked")
        .expect_err("after-apply fault must fail set-op refresh")
        .to_string();
    assert!(error.contains("PGT_TEST_FAILPOINT_REACHED"), "{error}");
    clear_chaos(&db).await;
    assert_committed_checkpoint(&db, id, &format!("public.{stream}"), checkpoint, &baseline).await;
    let exact_state: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS (SELECT * FROM {state_relation} EXCEPT ALL SELECT * FROM {state_checkpoint}) \
             AND NOT EXISTS (SELECT * FROM {state_checkpoint} EXCEPT ALL SELECT * FROM {state_relation})"
        ))
        .await;
    assert!(
        exact_state,
        "auxiliary set-operation rows must roll back exactly"
    );

    db.refresh_st(stream).await;
    if let Err(diff) = oracle::compare_st_to_query(&db, &format!("public.{stream}"), &query).await {
        panic!("FULL recovery did not match its exact source query: {diff}");
    }
    let (left_count, right_count): (i64, i64) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT __pgt_count_l, __pgt_count_r FROM {state_relation} WHERE value = 9"
    )))
    .fetch_one(&db.pool)
    .await
    .expect("recovered private set-operation row should exist");
    assert_eq!((left_count, right_count), (1, 1));
}

async fn delta_progress(db: &E2eDb, id: i64, relation: &str) -> DeltaProgress {
    let log_head: i64 = sqlx::query_scalar(
        "SELECT log_head FROM pgtrickle.pgt_output_delta_logs WHERE pgt_id = $1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .expect("output-delta log should exist");
    let batch_count: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_output_delta_batches WHERE pgt_id = $1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .expect("failed to read output-delta batches");
    let payload_rows: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*)::bigint FROM {relation}"
    )))
    .fetch_one(&db.pool)
    .await
    .expect("failed to read output-delta payload");
    DeltaProgress {
        log_head,
        batch_count,
        payload_rows,
    }
}

#[tokio::test]
async fn test_refresh_partial_finalization_control_is_detected() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    let source = "atomic_delta_source";
    let stream = "atomic_delta_st";
    let root = format!("public.{stream}");
    let query = format!("SELECT id, value FROM public.{source}");
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!("INSERT INTO public.{source} VALUES (1, 'base')"))
        .await;
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table(\
             name => '{stream}', query => '{query}', schedule => '1h', \
             refresh_mode => 'DIFFERENTIAL', initialize => false, \
             orchestration_mode => 'EXTERNAL')"
    ))
    .await;
    let graph = reference_clients::graph_digest(&db.pool, &root).await;
    reference_clients::refresh_graph(&db.pool, &root, &graph).await;
    let contract = reference_clients::stream_contract_digest(&db.pool, &root).await;
    let consumer =
        reference_clients::register(&db.pool, &root, "atomic_delta_consumer", &contract).await;
    let (token, _) = reference_clients::begin_resnapshot(&db.pool, &consumer.consumer_id).await;
    assert_eq!(
        reference_clients::ack_resnapshot(&db.pool, &consumer.consumer_id, &token).await,
        "ACTIVE"
    );
    let id = pgt_id(&db, stream).await;
    let checkpoint = "public.atomic_delta_checkpoint";
    snapshot_table(&db, &root, checkpoint).await;
    let baseline = progress(&db, id).await;
    let baseline_delta = delta_progress(&db, id, &consumer.delta_relation).await;
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (2, 'pending')"
    ))
    .await;
    set_chaos(&db, stream, "during_finalize").await;

    let mut blocker = db.pool.acquire().await.expect("acquire delta barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("get delta barrier backend PID");
    let lock_id = i32::try_from(id).expect("fixture pgt_id fits advisory-lock key");
    sqlx::query("SELECT pg_advisory_lock($1, $2)")
        .bind(lock_id)
        .bind(phase_code("during_finalize"))
        .execute(&mut *blocker)
        .await
        .expect("hold finalization barrier");
    let pool = db.pool.clone();
    let root_for_task = root.clone();
    let graph_for_task = graph.clone();
    let refresh = tokio::spawn(async move {
        sqlx::query(
            "SELECT * FROM pgtrickle.refresh_graph_strict(ARRAY[$1::regclass], $2::bytea, 'ALLOW')",
        )
        .bind(root_for_task)
        .bind(graph_for_task)
        .fetch_one(&pool)
        .await
    });
    let witness = wait_for_barrier(
        &db,
        id,
        "during_finalize",
        blocker_pid,
        Duration::from_secs(10),
    )
    .await;
    assert!(witness.application_name.contains("rows=1"));
    assert_committed_checkpoint(&db, id, &root, checkpoint, &baseline).await;
    let blocked_delta = delta_progress(&db, id, &consumer.delta_relation).await;
    if let Err(mismatch) = check_delta_progress(&baseline_delta, &blocked_delta) {
        panic!("uncommitted output-delta batch must remain invisible: {mismatch}");
    }

    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1, $2)")
        .bind(lock_id)
        .bind(phase_code("during_finalize"))
        .fetch_one(&mut *blocker)
        .await
        .expect("release finalization barrier");
    assert!(released);
    let error = refresh
        .await
        .expect("external graph refresh task panicked")
        .expect_err("finalization failpoint must roll back the graph call");
    assert!(error.to_string().contains("PGT_TEST_FAILPOINT_REACHED"));
    clear_chaos(&db).await;
    assert_committed_checkpoint(&db, id, &root, checkpoint, &baseline).await;
    let failed_delta = delta_progress(&db, id, &consumer.delta_relation).await;
    if let Err(mismatch) = check_delta_progress(&baseline_delta, &failed_delta) {
        panic!("failed finalization published output-delta state: {mismatch}");
    }

    // Semantic negative control: simulate an independently committed log-head
    // write from a broken finalizer while its matching batch has rolled back.
    sqlx::query(
        "UPDATE pgtrickle.pgt_output_delta_logs SET log_head = log_head + 1 WHERE pgt_id = $1",
    )
    .bind(id)
    .execute(&db.pool)
    .await
    .expect("inject isolated partial-finalization control");
    let control_delta = delta_progress(&db, id, &consumer.delta_relation).await;
    assert_eq!(control_delta.log_head, baseline_delta.log_head + 1);
    assert_eq!(control_delta.batch_count, baseline_delta.batch_count);
    assert_eq!(control_delta.payload_rows, baseline_delta.payload_rows);
    let detected = check_delta_progress(&baseline_delta, &control_delta)
        .expect_err("checkpoint check must reject an independently advanced log head");
    assert!(detected.contains("log_head"), "{detected}");
    sqlx::query("UPDATE pgtrickle.pgt_output_delta_logs SET log_head = $1 WHERE pgt_id = $2")
        .bind(baseline_delta.log_head)
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("restore output-delta control state");

    reference_clients::refresh_graph(&db.pool, &root, &graph).await;
    if let Err(diff) = oracle::compare_st_to_query(&db, &root, &query).await {
        panic!("output-delta fixture did not recover to committed input: {diff}");
    }
    let recovered_delta = delta_progress(&db, id, &consumer.delta_relation).await;
    assert_eq!(recovered_delta.log_head, baseline_delta.log_head + 1);
    assert_eq!(recovered_delta.batch_count, baseline_delta.batch_count + 1);
    assert_eq!(
        recovered_delta.payload_rows,
        baseline_delta.payload_rows + 1
    );
}

#[tokio::test]
async fn test_refresh_early_progress_control_is_detected() {
    let db = E2eDb::new().await.with_extension().await;
    let source = "atomic_progress_control_source";
    let stream = "atomic_progress_control_st";
    let query = format!("SELECT id, value FROM public.{source}");
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!("INSERT INTO public.{source} VALUES (1, 'base')"))
        .await;
    db.create_st(stream, &query, "1h", "DIFFERENTIAL").await;
    db.refresh_st(stream).await;
    let id = pgt_id(&db, stream).await;
    let checkpoint = "public.atomic_progress_control_checkpoint";
    snapshot_table(&db, &format!("public.{stream}"), checkpoint).await;
    let baseline = progress(&db, id).await;
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (2, 'pending')"
    ))
    .await;
    let source_oid: String = sqlx::query_scalar(
        "SELECT source_relid::text FROM pgtrickle.pgt_dependencies
         WHERE pgt_id = $1 AND source_type = 'TABLE' LIMIT 1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .expect("fixture source dependency should exist");
    set_chaos(&db, stream, "after_apply").await;
    let (mut barrier, blocker_pid) = hold_barrier(&db, id, "after_apply").await;
    let (refresh, _) = start_manual_refresh(&db, stream).await;
    wait_for_barrier(&db, id, "after_apply", blocker_pid, Duration::from_secs(10)).await;
    assert_committed_checkpoint(&db, id, &format!("public.{stream}"), checkpoint, &baseline).await;
    release_barrier(&mut barrier, id, "after_apply").await;
    let error = refresh
        .await
        .expect("control refresh task panicked")
        .expect_err("after-apply control refresh must fail");
    assert!(error.to_string().contains("PGT_TEST_FAILPOINT_REACHED"));
    clear_chaos(&db).await;
    assert_committed_checkpoint(&db, id, &format!("public.{stream}"), checkpoint, &baseline).await;

    // A separate committed frontier-only write models the partial-finalizer
    // defect after the real refresh transaction has released its row lock.
    sqlx::query(
        "UPDATE pgtrickle.pgt_stream_tables
         SET frontier = jsonb_set(frontier,
             ARRAY['sources', $2, 'lsn'], to_jsonb(pg_current_wal_lsn()::text), false)
         WHERE pgt_id = $1",
    )
    .bind(id)
    .bind(source_oid)
    .execute(&db.pool)
    .await
    .expect("commit only the stored progress in the negative control");

    let error = checkpoint_mismatch(&db, id, &format!("public.{stream}"), checkpoint, &baseline)
        .await
        .expect_err("early progress control must be detected");
    assert!(error.contains("frontier changed without output"), "{error}");
    sqlx::query("UPDATE pgtrickle.pgt_stream_tables SET frontier = $1::jsonb WHERE pgt_id = $2")
        .bind(baseline.frontier.as_deref())
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("restore frontier after semantic control");
    db.refresh_st(stream).await;
    assert_recovered_action(&db, stream, &query, "DIFFERENTIAL").await;
}
