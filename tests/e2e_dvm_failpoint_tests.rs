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

#[tokio::test]
async fn test_dvm_failpoint_preserves_last_committed_result_and_recovers() {
    let scenario = load_scenario(&corpus_path("cor939_two_leaf_snapshot.json"))
        .expect("valid DVM corpus scenario");

    let db = E2eDb::new().await.with_extension().await;

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
        scenario.execution.schedule,
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

    let suspended = wait_for_suspended(&db, pgt_name, Duration::from_secs(60)).await;
    let (status_at_timeout, _mode, _populated, consecutive_errors_at_timeout) =
        db.pgt_status(pgt_name).await;
    assert!(
        suspended,
        "stream table should have entered SUSPENDED after chaos-injected refresh failures; \
         status={status_at_timeout}, consecutive_errors={consecutive_errors_at_timeout}"
    );

    // ── Failpoint invariant: the last committed correct result is preserved ─
    oracle::compare_st_to_query(
        &db,
        &scenario.query.stream_table,
        &scenario.query.defining_query,
    )
    .await
    .expect("stream table must still match direct query while suspended by the failpoint");

    // ── Turn the failpoint OFF and resume ─────────────────────────────────
    db.alter_system_set_and_wait("pg_trickle.test_chaos_for_table", "''", "")
        .await;
    db.execute(&format!(
        "SELECT pgtrickle.resume_stream_table('{}')",
        scenario.query.stream_table
    ))
    .await;

    // ── Apply the first mutation cycle and refresh ────────────────────────
    let cycle = scenario.cycles.first().expect("scenario has a cycle");
    for mutation in &cycle.mutations {
        db.execute(&mutation.sql).await;
    }
    db.execute(&format!(
        "SELECT pgtrickle.refresh_stream_table('{}')",
        scenario.query.stream_table
    ))
    .await;

    // ── Later convergence: stream table matches direct query again ───────
    oracle::compare_st_to_query(
        &db,
        &scenario.query.stream_table,
        &scenario.query.defining_query,
    )
    .await
    .expect("stream table should converge to direct query after recovery and mutation");
}
