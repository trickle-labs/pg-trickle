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

#[tokio::test]
async fn test_scheduler_pause_drain_waits_for_witnessed_inline_refresh() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.scheduler_drain_timeout", "1", "1")
        .await;
    db.execute("CREATE TABLE pause_drain_source(id int PRIMARY KEY,value text)")
        .await;
    db.execute("INSERT INTO pause_drain_source VALUES(1,'baseline')")
        .await;
    let query = "SELECT id,value FROM pause_drain_source";
    let stream = "pause_drain_st";
    db.create_st(stream, query, "1h", "DIFFERENTIAL").await;
    db.create_st("pause_drain_control", query, "1h", "DIFFERENTIAL")
        .await;
    let id = pgt_id(&db, stream).await;
    let before = progress(&db, id).await;
    let phase = "input_boundary";
    set_chaos(&db, stream, phase).await;
    let (mut barrier, blocker) = hold_barrier(&db, id, phase).await;
    db.execute("INSERT INTO pause_drain_source VALUES(2,'pending-inline')")
        .await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at=now()-interval '2 hours' WHERE pgt_name='pause_drain_st'")
        .await;
    let witness = wait_for_barrier(&db, id, phase, blocker, Duration::from_secs(90)).await;
    assert_eq!(db.count(stream).await, 1);
    assert_eq!(progress(&db, id).await.frontier, before.frontier);
    let started = std::time::Instant::now();
    let pause = tokio::time::timeout(
        Duration::from_secs(10),
        sqlx::query("SELECT pgtrickle.pause_scheduler(ARRAY['pause_drain_st'])").execute(&db.pool),
    )
    .await
    .expect("pause drain must have a bounded outcome");
    let elapsed = started.elapsed().as_millis();
    let still_blocked: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND $2=ANY(pg_blocking_pids(pid)))",
    )
    .bind(witness.pid)
    .bind(blocker)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    let artifact = std::path::Path::new(&directory).join("scheduler-pause-inline-drain.json");
    let mut observation = serde_json::json!({
        "installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
        "phase":"drain","pgt_id":id,"scheduler_pid":witness.pid,
        "application_name":witness.application_name,"wait_event":witness.wait_event,
        "blockers":witness.blockers,"still_blocked":still_blocked,
        "pause_error":pause.as_ref().err().map(ToString::to_string),"elapsed_ms":elapsed,
        "frontier_before":before.frontier,"frontier_at_pause":progress(&db,id).await.frontier,
        "rows_at_pause":db.count(stream).await,
    });
    std::fs::write(&artifact, serde_json::to_vec_pretty(&observation).unwrap()).unwrap();
    assert!(
        still_blocked,
        "the witnessed inline refresh must remain in flight"
    );
    assert!(
        pause
            .as_ref()
            .err()
            .is_some_and(|error| error.to_string().contains("pause_scheduler timed out")),
        "pause cannot report a successful drain while its inline refresh is blocked: {pause:?}"
    );
    clear_chaos(&db).await;
    release_barrier(&mut barrier, id, phase).await;
    assert!(
        db.wait_for_condition(
            "admitted inline refresh finishes after pause timeout",
            "SELECT count(*)=2 FROM pause_drain_st",
            Duration::from_secs(30),
            Duration::from_millis(25)
        )
        .await
    );
    db.assert_st_matches_query(stream, query).await;
    db.execute("INSERT INTO pause_drain_source VALUES(3,'pending-paused')")
        .await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at=now()-interval '2 hours' WHERE pgt_name IN ('pause_drain_st','pause_drain_control')")
        .await;
    let paused = progress(&db, id).await;
    assert!(
        db.wait_for_condition(
            "control witnesses another scheduler dispatch while target is paused",
            "SELECT count(*)=3 FROM pause_drain_control",
            Duration::from_secs(30),
            Duration::from_millis(25)
        )
        .await
    );
    assert_eq!(db.count(stream).await, 2);
    assert_eq!(progress(&db, id).await, paused);
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['pause_drain_st'])")
        .await;
    assert!(
        db.wait_for_condition(
            "resumed inline target catches up",
            "SELECT count(*)=3 FROM pause_drain_st",
            Duration::from_secs(30),
            Duration::from_millis(25)
        )
        .await
    );
    db.assert_st_matches_query(stream, query).await;
    observation["phase"] = "complete".into();
    observation["rows_after_resume"] = db.count(stream).await.into();
    observation["frontier_after_resume"] = progress(&db, id).await.frontier.into();
    std::fs::write(&artifact, serde_json::to_vec_pretty(&observation).unwrap()).unwrap();
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
            let logs = tokio::process::Command::new("docker")
                .args(["logs", "--tail", "100", db.container_id()])
                .output()
                .await
                .expect("read container logs");
            panic!(
                "refresh did not reach {phase} barrier for pgt_id={id}\n{}{}",
                String::from_utf8_lossy(&logs.stdout),
                String::from_utf8_lossy(&logs.stderr)
            );
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
    db.create_st(&scheduler, &query, "1h", "DIFFERENTIAL").await;
    db.refresh_st(&scheduler).await;
    let scheduler_id = pgt_id(&db, &scheduler).await;
    let scheduler_checkpoint = format!("public.{scheduler}_checkpoint");
    snapshot_table(&db, &format!("public.{scheduler}"), &scheduler_checkpoint).await;
    set_chaos(&db, &scheduler, phase).await;
    let (mut scheduler_barrier, scheduler_blocker) = hold_barrier(&db, scheduler_id, phase).await;
    let mut schedule_change = db
        .pool
        .begin()
        .await
        .expect("begin scheduler setup transaction");
    // Keep the 1h cadence's deadlines; a 1s cadence allows only 100ms at the barrier.
    sqlx::query(
        "UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at = now() - interval '2 hours'
         WHERE pgt_id = $1",
    )
    .bind(scheduler_id)
    .execute(&mut *schedule_change)
    .await
    .expect("make scheduler fixture immediately due");
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO public.{source} VALUES (3, 'scheduler-pending')"
    )))
    .execute(&mut *schedule_change)
    .await
    .expect("commit pending source change");
    schedule_change
        .commit()
        .await
        .expect("commit scheduler setup transaction");
    let scheduler_witness = wait_for_barrier(
        &db,
        scheduler_id,
        phase,
        scheduler_blocker,
        Duration::from_secs(90),
    )
    .await;
    // Keep the barrier held beyond the former 100ms lock deadline.
    tokio::time::sleep(Duration::from_millis(250)).await;
    // Compare rollback to the last committed boundary; a no-data scheduler tick may finish first.
    let scheduler_progress = progress(&db, scheduler_id).await;
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
    assert_eq!(action, "DIFFERENTIAL", "failure {failed_id}: {message}");
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
async fn test_refresh_nonfinite_aggregate_keeps_later_writer_for_next_cut() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("CREATE TABLE atomic_special_source(id int PRIMARY KEY,val numeric)")
        .await;
    db.execute("INSERT INTO atomic_special_source VALUES(1,'Infinity'),(2,1)")
        .await;
    let stream = "atomic_special_st";
    let query = "SELECT SUM(val) AS total,AVG(val) AS mean FROM atomic_special_source";
    db.create_st(stream, query, "1h", "DIFFERENTIAL").await;
    db.execute("DELETE FROM atomic_special_source WHERE id=1")
        .await;
    let id = pgt_id(&db, stream).await;
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
    tokio::time::timeout(
        Duration::from_secs(5),
        db.execute("INSERT INTO atomic_special_source VALUES(3,3)"),
    )
    .await
    .expect("later writer must commit while the refresh waits at its selected cut");
    release_barrier(&mut barrier, id, "input_boundary").await;
    tokio::time::timeout(Duration::from_secs(10), refresh)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    clear_chaos(&db).await;
    let first: bool = db
        .query_scalar("SELECT total=1 AND mean=1 FROM atomic_special_st")
        .await;
    assert!(
        first,
        "the first aggregate refresh must exclude the later writer"
    );
    db.refresh_st(stream).await;
    let second: bool = db
        .query_scalar("SELECT total=4 AND mean=2 FROM atomic_special_st")
        .await;
    assert!(
        second,
        "the next cut must consume the later writer exactly once"
    );
    assert_recovered_action(&db, stream, query, "DIFFERENTIAL").await;
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
    let mut baseline = progress(&db, id).await;
    let mut baseline_delta = delta_progress(&db, id, &consumer.delta_relation).await;
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

    reference_clients::refresh_graph(&db.pool, &root, &graph).await;
    if let Err(diff) = oracle::compare_st_to_query(&db, &root, &query).await {
        panic!("output-delta fixture did not recover to committed input: {diff}");
    }
    db.execute(&format!("DROP TABLE {checkpoint}")).await;
    snapshot_table(&db, &root, checkpoint).await;
    baseline = progress(&db, id).await;
    baseline_delta = delta_progress(&db, id, &consumer.delta_relation).await;

    // Drop the batch insert inside the real finalizer while its payload and
    // log-head writes continue; the committed checkpoint oracle must catch it.
    db.execute(&format!(
        "INSERT INTO public.{source} VALUES (3, 'control-pending')"
    ))
    .await;
    set_chaos(&db, stream, "control_partial_finalization").await;
    let (mut blocker, blocker_pid) = hold_barrier(&db, id, "during_finalize").await;
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
    if let Err(mismatch) = check_delta_progress(
        &baseline_delta,
        &delta_progress(&db, id, &consumer.delta_relation).await,
    ) {
        panic!("uncommitted output-delta control became visible: {mismatch}");
    }
    release_barrier(&mut blocker, id, "during_finalize").await;
    refresh
        .await
        .expect("partial-finalization control task panicked")
        .expect("semantic control refresh should commit");
    clear_chaos(&db).await;
    if let Err(diff) = oracle::compare_st_to_query(&db, &root, &query).await {
        panic!("control refresh output should match committed input: {diff}");
    }
    let control_delta = delta_progress(&db, id, &consumer.delta_relation).await;
    let detected = check_delta_progress(&baseline_delta, &control_delta)
        .expect_err("checkpoint oracle must reject the partial-finalization control");
    assert!(detected.contains("log_head"), "{detected}");
    assert!(detected.contains("batch_count"), "{detected}");
    assert!(detected.contains("payload_rows"), "{detected}");

    sqlx::query(
        "DELETE FROM pgtrickle.pgt_output_delta_batches WHERE pgt_id = $1 AND batch_token > $2",
    )
    .bind(id)
    .bind(baseline_delta.log_head)
    .execute(&db.pool)
    .await
    .expect("remove control batch state");
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM {} WHERE batch_token > $1",
        consumer.delta_relation
    )))
    .bind(baseline_delta.log_head)
    .execute(&db.pool)
    .await
    .expect("remove control payload state");
    sqlx::query("UPDATE pgtrickle.pgt_output_delta_logs SET log_head = $1 WHERE pgt_id = $2")
        .bind(baseline_delta.log_head)
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("restore output-delta control checkpoint");
    assert_eq!(
        delta_progress(&db, id, &consumer.delta_relation).await,
        baseline_delta
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

    set_chaos(&db, stream, "control_early_progress").await;
    let (mut barrier, blocker_pid) = hold_barrier(&db, id, "during_finalize").await;
    let (refresh, _) = start_manual_refresh(&db, stream).await;
    let witness = wait_for_barrier(
        &db,
        id,
        "during_finalize",
        blocker_pid,
        Duration::from_secs(10),
    )
    .await;
    assert!(witness.application_name.contains("rows=1"));
    assert_committed_checkpoint(&db, id, &format!("public.{stream}"), checkpoint, &baseline).await;
    release_barrier(&mut barrier, id, "during_finalize").await;
    refresh
        .await
        .expect("early-progress control task panicked")
        .expect("semantic control refresh should commit");
    clear_chaos(&db).await;
    let error = checkpoint_mismatch(&db, id, &format!("public.{stream}"), checkpoint, &baseline)
        .await
        .expect_err("checkpoint oracle must reject the early-progress control");
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

#[tokio::test]
async fn test_snapshot_during_witnessed_cancel_keeps_committed_rows_and_frontier() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    let stream = "atomic_snapshot_cancel";
    let query = "SELECT id,value FROM atomic_snapshot_source";
    db.execute("CREATE TABLE atomic_snapshot_source(id INT PRIMARY KEY,value TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO atomic_snapshot_source VALUES(1,'committed')")
        .await;
    db.create_st(stream, query, "1h", "DIFFERENTIAL").await;
    let id = pgt_id(&db, stream).await;
    let baseline = progress(&db, id).await;
    snapshot_table(&db, stream, "atomic_snapshot_checkpoint").await;
    db.execute("UPDATE atomic_snapshot_source SET value='pending'")
        .await;
    set_chaos(&db, stream, "after_apply").await;
    let (mut barrier, blocker) = hold_barrier(&db, id, "after_apply").await;
    let (refresh, refresh_pid) = start_manual_refresh(&db, stream).await;
    let witness = wait_for_barrier(&db, id, "after_apply", blocker, Duration::from_secs(10)).await;
    assert_eq!(witness.pid, refresh_pid);
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    let artifact = std::path::Path::new(&directory).join("snapshot-cancel.json");
    std::fs::write(
        &artifact,
        serde_json::to_vec_pretty(&serde_json::json!({
            "phase":"after_apply", "refresh_pid":refresh_pid,
            "blockers":witness.blockers, "wait_event":witness.wait_event,
            "expected_frontier":baseline.frontier, "stage":"before cancellation",
        }))
        .unwrap(),
    )
    .unwrap();
    let mut connection = db.pool.acquire().await.unwrap();
    let snapshot_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let mut snapshot = tokio::spawn(async move {
        sqlx::query_scalar::<_, String>(
            "SELECT pgtrickle.snapshot_stream_table('atomic_snapshot_cancel')",
        )
        .fetch_one(&mut *connection)
        .await
    });
    let mut completed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        tokio::select! {
            result = &mut snapshot => { completed=Some(result.unwrap().unwrap()); break; }
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let blocked: bool = sqlx::query_scalar("SELECT $1=ANY(pg_blocking_pids($2))")
            .bind(refresh_pid)
            .bind(snapshot_pid)
            .fetch_one(&db.pool)
            .await
            .unwrap();
        if blocked {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "snapshot neither completed nor witnessed blocking"
        );
    }
    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(refresh_pid)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert!(cancelled);
    let error = tokio::time::timeout(Duration::from_secs(5), refresh)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("canceling statement due to user request"),
        "{error}"
    );
    release_barrier(&mut barrier, id, "after_apply").await;
    clear_chaos(&db).await;
    let snapshot_name = match completed {
        Some(name) => name,
        None => tokio::time::timeout(Duration::from_secs(5), snapshot)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    };
    let snapshot_frontier: Option<String> = sqlx::query_scalar(
        "SELECT frontier::text FROM pgtrickle.pgt_snapshots WHERE snapshot_relid=$1::regclass::oid",
    )
    .bind(&snapshot_name)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(snapshot_frontier, baseline.frontier);
    db.assert_st_matches_query(
        "atomic_snapshot_checkpoint",
        &format!("SELECT id,value FROM {snapshot_name}"),
    )
    .await;
    assert_committed_checkpoint(&db, id, stream, "atomic_snapshot_checkpoint", &baseline).await;
    db.refresh_st(stream).await;
    assert_recovered_action(&db, stream, query, "DIFFERENTIAL").await;
    db.execute("SELECT pgtrickle.snapshot_stream_table('atomic_snapshot_cancel','public.atomic_snapshot_after')").await;
    db.assert_st_matches_query(
        "atomic_snapshot_after",
        &format!("SELECT id,value FROM {stream}"),
    )
    .await;
    std::fs::write(
        artifact,
        serde_json::to_vec_pretty(&serde_json::json!({
            "phase":"after_apply", "refresh_pid":refresh_pid,"snapshot_pid":snapshot_pid,
            "blockers":witness.blockers,"wait_event":witness.wait_event,
            "expected_frontier":baseline.frontier,"snapshot_frontier":snapshot_frontier,
            "cancel_error":error.to_string(),"coherent":true,
        }))
        .unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn test_join_endpoint_fence_witnesses_writer_blocking_and_next_cut() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;
    db.execute("CREATE TABLE endpoint_fence_l(id INT PRIMARY KEY,v NUMERIC)")
        .await;
    db.execute("CREATE TABLE endpoint_fence_r(id INT PRIMARY KEY,v NUMERIC)")
        .await;
    db.execute("INSERT INTO endpoint_fence_l VALUES(1,1)").await;
    db.execute("INSERT INTO endpoint_fence_r VALUES(1,0)").await;
    let stream = "endpoint_fence_st";
    let query = "SELECT l.id,100/(l.v-r.v) quotient FROM endpoint_fence_l l JOIN endpoint_fence_r r ON l.id=r.id";
    db.create_st(stream, query, "24h", "DIFFERENTIAL").await;
    sqlx::raw_sql(
        "BEGIN; UPDATE endpoint_fence_l SET v=2; UPDATE endpoint_fence_r SET v=1; COMMIT",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let id = pgt_id(&db, stream).await;
    set_chaos(&db, stream, "input_boundary").await;
    let (mut barrier, blocker) = hold_barrier(&db, id, "input_boundary").await;
    let (refresh, refresh_pid) = start_manual_refresh(&db, stream).await;
    let witness =
        wait_for_barrier(&db, id, "input_boundary", blocker, Duration::from_secs(10)).await;
    assert_eq!(witness.pid, refresh_pid);
    let locks: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT relation)::bigint FROM pg_locks WHERE pid=$1 AND granted AND mode='ShareLock' \
         AND relation IN ('endpoint_fence_l'::regclass,'endpoint_fence_r'::regclass)"
    ).bind(refresh_pid).fetch_one(&db.pool).await.unwrap();
    assert_eq!(
        locks, 2,
        "both endpoint sources must be fenced before selecting the cut"
    );
    let mut writer_conn = db.pool.acquire().await.unwrap();
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer_conn)
        .await
        .unwrap();
    sqlx::query("SET lock_timeout='5s'")
        .execute(&mut *writer_conn)
        .await
        .unwrap();
    let writer = tokio::spawn(async move {
        sqlx::query("UPDATE endpoint_fence_l SET v=3 WHERE id=1")
            .execute(&mut *writer_conn)
            .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let blocked: bool = sqlx::query_scalar("SELECT $1=ANY(pg_blocking_pids($2))")
            .bind(refresh_pid)
            .bind(writer_pid)
            .fetch_one(&db.pool)
            .await
            .unwrap();
        if blocked {
            break;
        }
        assert!(
            !writer.is_finished(),
            "writer escaped the admitted source fence"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "writer did not reach the source fence"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    release_barrier(&mut barrier, id, "input_boundary").await;
    tokio::time::timeout(Duration::from_secs(5), refresh)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    clear_chaos(&db).await;
    let first: f64 = db
        .query_scalar("SELECT quotient::float8 FROM endpoint_fence_st")
        .await;
    assert_eq!(
        first, 100.0,
        "first refresh must consume only the selected endpoint"
    );
    let oid = db.table_oid("endpoint_fence_l").await;
    let buffer: String = db.query_scalar(&format!(
        "SELECT format('%I.%I',current_setting('pg_trickle.change_buffer_schema'), \
         'changes_'||coalesce(source_stable_name,source_relid::text)) FROM pgtrickle.pgt_change_tracking WHERE source_relid={oid}"
    )).await;
    let pending: bool = db.query_scalar(&format!(
        "SELECT (SELECT max(lsn) FROM {buffer} WHERE action IN ('I','D')) > \
          (frontier->'sources'->'{oid}'->>'lsn')::pg_lsn FROM pgtrickle.pgt_stream_tables WHERE pgt_name='{stream}'"
    )).await;
    assert!(
        pending,
        "the writer's later captured change must remain beyond the published cut"
    );
    db.refresh_st(stream).await;
    db.assert_st_matches_query(stream, query).await;
    let next: f64 = db
        .query_scalar("SELECT quotient::float8 FROM endpoint_fence_st")
        .await;
    assert_eq!(next, 50.0);
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        std::path::Path::new(&directory).join("join-endpoint-fence.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "phase":"input_boundary","refresh_pid":refresh_pid,"writer_pid":writer_pid,
            "refresh_wait":witness.wait_event,"refresh_blockers":witness.blockers,
            "source_share_locks":locks,"writer_blocked_by_refresh":true,
            "first_quotient":first,"later_capture_beyond_cut":pending,"next_quotient":next,
        }))
        .unwrap(),
    )
    .unwrap();
}
