//! COR-19: A refresh failpoint on a permanent DVM corpus scenario preserves
//! the last committed correct result, and the stream table converges again
//! after recovery and a further mutation.

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

use dvm_fuzz::load_scenario;
use e2e::{E2eDb, oracle};
use std::path::PathBuf;
use std::time::Duration;

fn corpus_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/corpus/dvm_regressions")
        .join(name)
}

fn dollar_quote(value: &str) -> String {
    ["$dvm$", value, "$dvm$"].concat()
}

/// Poll until the stream table reaches `SUSPENDED` status (copied pattern
/// from `e2e_cleanup_chaos_tests.rs`).
async fn wait_for_suspended(db: &E2eDb, pgt_name: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let status: String = db
            .query_scalar(&format!(
                "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{pgt_name}'"
            ))
            .await;
        if status == "SUSPENDED" {
            return true;
        }
    }
}

async fn wait_for_scheduler_failure(
    db: &E2eDb,
    pgt_name: &str,
    timeout: Duration,
) -> Option<(i64, String, String, String, String, bool, String)> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let failure = sqlx::query_as(
            "SELECT h.refresh_id, h.action, h.initiated_by, h.error_code,
                    h.error_sqlstate, h.retryable, h.error_message
             FROM pgtrickle.pgt_refresh_history h
             JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
             WHERE st.pgt_name = $1
               AND h.status = 'FAILED'
               AND h.initiated_by = 'SCHEDULER'
               AND h.action = 'DIFFERENTIAL'
             ORDER BY h.refresh_id DESC
             LIMIT 1",
        )
        .bind(pgt_name)
        .fetch_optional(&db.pool)
        .await
        .ok()
        .flatten();
        if let Some(failure) = failure {
            return Some(failure);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_scheduler_recovery(
    db: &E2eDb,
    pgt_name: &str,
    failed_refresh_id: i64,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let recovered: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
                WHERE st.pgt_name = $1
                  AND h.refresh_id > $2
                  AND h.status = 'COMPLETED'
                  AND h.initiated_by = 'SCHEDULER'
                  AND st.status = 'ACTIVE'
                  AND st.consecutive_errors = 0
            )",
        )
        .bind(pgt_name)
        .bind(failed_refresh_id)
        .fetch_one(&db.pool)
        .await
        .unwrap_or(false);
        if recovered {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn test_dvm_failpoint_preserves_last_committed_result_and_recovers() {
    let scenario = load_scenario(&corpus_path("cor939_two_leaf_snapshot.json"))
        .expect("valid DVM corpus scenario");

    let db = E2eDb::new_dedicated().await.with_extension().await;

    // ── Fast, sequential scheduler with a low error threshold ────────────
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.max_consecutive_errors = 3")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'off'")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.max_consecutive_errors", "3")
        .await;
    db.wait_for_setting("pg_trickle.parallel_refresh_mode", "off")
        .await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler did not appear within 90 s"
    );

    // ── Manual schema/setup/initial-data + create_stream_table + refresh ─
    db.execute(&format!("CREATE SCHEMA {}", scenario.schema.name))
        .await;
    for sql in &scenario.schema.setup_sql {
        db.execute(sql).await;
    }
    for sql in &scenario.initial_data {
        db.execute(sql).await;
    }
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('{}', {}, '{}', '{}')",
        scenario.query.stream_table,
        dollar_quote(&scenario.query.defining_query),
        "1s",
        scenario.execution.requested_refresh_mode
    ))
    .await;
    db.execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('{}')",
        scenario.query.stream_table
    ))
    .await;

    // ── Baseline: stream table matches the direct query ──────────────────
    oracle::compare_st_to_query(
        &db,
        &scenario.query.stream_table,
        &scenario.query.defining_query,
    )
    .await
    .expect("baseline stream table should match direct query");

    let checkpoint = format!("{}.two_leaf_checkpoint", scenario.schema.name);
    db.execute(&format!(
        "CREATE TABLE {checkpoint} AS TABLE {}",
        scenario.query.stream_table
    ))
    .await;

    // ── Turn the refresh failpoint ON for this stream table ──────────────
    let pgt_name = scenario
        .query
        .stream_table
        .rsplit('.')
        .next()
        .expect("stream table name");
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_for_table",
        &format!("'{pgt_name}'"),
        pgt_name,
    )
    .await;

    // Commit real source changes after enabling the scheduler failpoint. The
    // failed scheduler attempt must have pending journaled input to process.
    let cycle = scenario.cycles.first().expect("scenario has a cycle");
    for mutation in &cycle.mutations {
        db.execute(&mutation.sql).await;
    }

    let failure = wait_for_scheduler_failure(&db, pgt_name, Duration::from_secs(60))
        .await
        .expect("scheduler did not persist the injected failure");
    let (refresh_id, action, initiated_by, error_code, error_sqlstate, retryable, error_message) =
        failure;
    assert_eq!(action, "DIFFERENTIAL");
    assert_eq!(initiated_by, "SCHEDULER");
    assert_eq!(error_code, "SERIALIZATION");
    assert_eq!(error_sqlstate, "40001");
    assert!(retryable);
    assert!(error_message.contains("PGT_TEST_FAILPOINT_REACHED"));
    assert!(error_message.contains("phase=before_apply"));
    assert!(error_message.contains("action=DIFFERENTIAL"));
    assert!(error_message.contains("actual_path=NOT_REACHED"));
    let scheduler_pid: i32 = error_message
        .split("backend_pid=")
        .nth(1)
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse().ok())
        .expect("failure marker must include the scheduler backend PID");
    assert!(scheduler_pid > 0);

    let (requested_mode, refresh_mode, effective_mode, needs_reinit): (
        String,
        String,
        Option<String>,
        bool,
    ) = sqlx::query_as(
        "SELECT requested_refresh_mode, refresh_mode, effective_refresh_mode, needs_reinit
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1",
    )
    .bind(pgt_name)
    .fetch_one(&db.pool)
    .await
    .expect("failed to inspect scheduler failure path");
    assert_eq!(requested_mode, "DIFFERENTIAL");
    assert_eq!(refresh_mode, "DIFFERENTIAL");
    assert_eq!(effective_mode.as_deref(), Some("DIFFERENTIAL"));
    assert!(!needs_reinit);

    let suspended = wait_for_suspended(&db, pgt_name, Duration::from_secs(60)).await;
    let (status_at_timeout, _mode, _populated, consecutive_errors_at_timeout) =
        db.pgt_status(pgt_name).await;
    assert!(
        suspended,
        "stream table should have entered SUSPENDED after chaos-injected refresh failures; \
         status={status_at_timeout}, consecutive_errors={consecutive_errors_at_timeout}"
    );

    // ── Failpoint invariant: the last committed correct result is preserved ─
    oracle::compare_sts(&db, &scenario.query.stream_table, &checkpoint)
        .await
        .expect("stream table must still match its pre-fault committed result");

    // ── Turn the failpoint OFF and resume ─────────────────────────────────
    db.alter_system_set_and_wait("pg_trickle.test_chaos_for_table", "''", "")
        .await;
    db.execute(&format!(
        "SELECT pgtrickle.resume_stream_table('{}')",
        scenario.query.stream_table
    ))
    .await;

    assert!(
        wait_for_scheduler_recovery(&db, pgt_name, refresh_id, Duration::from_secs(60)).await,
        "scheduler did not complete a successful recovery attempt"
    );

    // ── Later convergence: stream table matches direct query again ───────
    oracle::compare_st_to_query(
        &db,
        &scenario.query.stream_table,
        &scenario.query.defining_query,
    )
    .await
    .expect("stream table should converge to direct query after recovery and mutation");
}
