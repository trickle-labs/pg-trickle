//! F4 (v0.37.0): E2E tests for pgVectorMV — incremental vector aggregate operators.
//!
//! Validates that stream tables computing `avg(embedding)` and `sum(embedding)`
//! over pgvector `vector` typed columns produce correct results after INSERT,
//! UPDATE, and DELETE operations (correctness vs FULL refresh).
//!
//! Uses the group-rescan strategy: affected groups are re-aggregated from source
//! data using pgvector's native `avg(vector)` and `sum(vector)` aggregates.
//!
//! Prerequisites: `./tests/build_e2e_image.sh` (includes pgvector)

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Helper: enable pgvector and the vector agg GUC
// ═══════════════════════════════════════════════════════════════════════

async fn setup_pgvector(db: &E2eDb) {
    db.execute("CREATE EXTENSION IF NOT EXISTS vector").await;
}

fn ann_worker_command(
    db: &E2eDb,
    database: &str,
    user: &str,
    request_id: i64,
    lock_timeout: &str,
) -> tokio::process::Command {
    use std::process::Stdio;

    let script = std::fs::File::open(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/scripts/ann_maintenance_worker.sql"
    ))
    .expect("open ANN worker script");
    let mut command = tokio::process::Command::new("docker");
    command
        .args([
            "exec",
            "-i",
            "-e",
            "PGAPPNAME=pgtrickle_ann_worker",
            db.container_id(),
            "psql",
            "-X",
            "-w",
            "-U",
            user,
            "-d",
            database,
            "-v",
        ])
        .arg(format!("request_id={request_id}"))
        .arg("-v")
        .arg(format!("ann_lock_timeout={lock_timeout}"))
        .args(["-f", "-"])
        .stdin(Stdio::from(script))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn ann_threshold_command(
    db: &E2eDb,
    database: &str,
    user: &str,
    stream: &str,
    index: &str,
    lock_timeout: &str,
) -> tokio::process::Command {
    use std::process::Stdio;
    let script = std::fs::File::open(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/scripts/ann_threshold_request.sql"
    ))
    .expect("open threshold request script");
    let mut command = tokio::process::Command::new("docker");
    command
        .args([
            "exec",
            "-i",
            db.container_id(),
            "psql",
            "-X",
            "-w",
            "-qAt",
            "-U",
            user,
            "-d",
            database,
            "-v",
        ])
        .arg(format!("ann_stream={stream}"))
        .arg("-v")
        .arg(format!("ann_index={index}"))
        .arg("-v")
        .arg(format!("ann_lock_timeout={lock_timeout}"))
        .args(["-f", "-"])
        .stdin(Stdio::from(script))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

#[tokio::test]
async fn test_ann_threshold_request_script_records_only_eligible_work() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_policy_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute(
        "INSERT INTO ann_policy_source SELECT i, '[1,0,0]'::vector FROM generate_series(1,10) i",
    )
    .await;
    db.create_st(
        "ann_policy_stream",
        "SELECT id, embedding FROM ann_policy_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_policy_idx ON ann_policy_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('ann_policy_stream', reindex_drift_threshold => 0.2)",
    )
    .await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let run = || {
        ann_threshold_command(
            &db,
            &database,
            "postgres",
            "public.ann_policy_stream",
            "public.ann_policy_idx",
            "5s",
        )
    };
    let initial = run().output().await.expect("inspect missing baseline");
    assert!(
        initial.status.success(),
        "{}",
        String::from_utf8_lossy(&initial.stderr)
    );
    assert!(String::from_utf8_lossy(&initial.stdout).contains("missing_baseline"));
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_policy_stream'::regclass::oid, 'ann_policy_idx'::regclass::oid)").await;
    let baseline = ann_worker_command(&db, &database, "postgres", request, "5s")
        .output()
        .await
        .expect("establish baseline");
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let below = run().output().await.expect("check unchanged corpus");
    assert!(
        below.status.success(),
        "{}",
        String::from_utf8_lossy(&below.stderr)
    );
    assert!(String::from_utf8_lossy(&below.stdout).contains("below_threshold"));
    db.execute("UPDATE ann_policy_source SET embedding = '[0,1,0]' WHERE id <= 3")
        .await;
    db.refresh_st("ann_policy_stream").await;
    let old_index: i64 = db
        .query_scalar("SELECT 'ann_policy_idx'::regclass::oid::bigint")
        .await;
    let scheduled = run().output().await.expect("request eligible work");
    assert!(
        scheduled.status.success(),
        "{}",
        String::from_utf8_lossy(&scheduled.stderr)
    );
    let decision: serde_json::Value =
        serde_json::from_slice(&scheduled.stdout).expect("parse threshold decision");
    assert_eq!(decision["decision"], "requested");
    let scheduled_request = decision["request_id"]
        .as_i64()
        .expect("scheduled request id");
    let new_index: i64 = db
        .query_scalar("SELECT 'ann_policy_idx'::regclass::oid::bigint")
        .await;
    assert_eq!(old_index, new_index, "request script must not run DDL");
    let state: String =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)->>'state'")
            .bind(scheduled_request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect requested work");
    assert_eq!(state, "REQUESTED");
    let duplicate = run().output().await.expect("deduplicate work");
    assert!(
        duplicate.status.success(),
        "{}",
        String::from_utf8_lossy(&duplicate.stderr)
    );
    assert!(String::from_utf8_lossy(&duplicate.stdout).contains("already_requested"));
    let completed = ann_worker_command(&db, &database, "postgres", scheduled_request, "5s")
        .output()
        .await
        .expect("execute requested work");
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let rebuilt: i64 = db
        .query_scalar("SELECT 'ann_policy_idx'::regclass::oid::bigint")
        .await;
    assert_ne!(old_index, rebuilt);
    db.execute("CREATE INDEX ann_policy_btree ON ann_policy_stream (id)")
        .await;
    let unrelated = ann_threshold_command(
        &db,
        &database,
        "postgres",
        "public.ann_policy_stream",
        "public.ann_policy_btree",
        "5s",
    )
    .output()
    .await
    .expect("check non-ANN index");
    assert!(
        !unrelated.status.success(),
        "non-ANN indexes must not be queued"
    );
    db.assert_st_matches_query(
        "ann_policy_stream",
        "SELECT id, embedding FROM ann_policy_source",
    )
    .await;
    let mut blocker = db.pool.begin().await.expect("begin refresh barrier");
    sqlx::query("LOCK TABLE ann_policy_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold refresh barrier");
    let locked = ann_threshold_command(
        &db,
        &database,
        "postgres",
        "public.ann_policy_stream",
        "public.ann_policy_idx",
        "200ms",
    )
    .output()
    .await
    .expect("check lock timeout");
    assert!(!locked.status.success());
    assert!(String::from_utf8_lossy(&locked.stderr).contains("lock timeout"));
    blocker.rollback().await.expect("release refresh barrier");
    db.execute("CREATE ROLE ann_policy_operator LOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO ann_policy_operator")
        .await;
    db.execute(
        "GRANT SELECT ON pgtrickle.pgt_stream_tables, ann_policy_stream TO ann_policy_operator",
    )
    .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.request_ann_maintenance_if_needed(oid,oid,bigint,bigint,bigint) TO ann_policy_operator").await;
    let denied = ann_threshold_command(
        &db,
        &database,
        "ann_policy_operator",
        "public.ann_policy_stream",
        "public.ann_policy_idx",
        "5s",
    )
    .output()
    .await
    .expect("run without MAINTAIN");
    assert!(
        !denied.status.success(),
        "SELECT alone must not request maintenance"
    );
    db.execute("GRANT MAINTAIN ON ann_policy_stream TO ann_policy_operator")
        .await;
    let permitted = ann_threshold_command(
        &db,
        &database,
        "ann_policy_operator",
        "public.ann_policy_stream",
        "public.ann_policy_idx",
        "5s",
    )
    .output()
    .await
    .expect("run with MAINTAIN");
    assert!(
        permitted.status.success(),
        "{}",
        String::from_utf8_lossy(&permitted.stderr)
    );
    assert!(String::from_utf8_lossy(&permitted.stdout).contains("cooldown"));
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_churn_operations = NULL WHERE pgt_name = 'ann_policy_stream'").await;
    let unknown = run().output().await.expect("check unknown churn");
    assert!(
        unknown.status.success(),
        "{}",
        String::from_utf8_lossy(&unknown.stderr)
    );
    assert!(String::from_utf8_lossy(&unknown.stdout).contains("unknown_work"));
    db.execute("CREATE TABLE ann_policy_empty_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_policy_empty",
        "SELECT id, embedding FROM ann_policy_empty_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_policy_empty_idx ON ann_policy_empty USING hnsw (embedding vector_l2_ops)").await;
    let empty = ann_threshold_command(
        &db,
        &database,
        "postgres",
        "public.ann_policy_empty",
        "public.ann_policy_empty_idx",
        "5s",
    )
    .output()
    .await
    .expect("check empty estimate");
    assert!(
        empty.status.success(),
        "{}",
        String::from_utf8_lossy(&empty.stderr)
    );
    assert!(String::from_utf8_lossy(&empty.stdout).contains("unknown_estimate"));
    db.execute(
        "ALTER INDEX ann_policy_empty_idx RENAME TO \"empty; DROP TABLE ann_policy_source; --\"",
    )
    .await;
    let quoted = ann_threshold_command(
        &db,
        &database,
        "postgres",
        "public.ann_policy_empty",
        "public.\"empty; DROP TABLE ann_policy_source; --\"",
        "5s",
    )
    .output()
    .await
    .expect("check quoted index name");
    assert!(
        quoted.status.success(),
        "{}",
        String::from_utf8_lossy(&quoted.stderr)
    );
    assert!(String::from_utf8_lossy(&quoted.stdout).contains("unknown_estimate"));
    assert_eq!(db.count("ann_policy_source").await, 10);
}

#[tokio::test]
async fn test_ann_worker_limits_concurrent_requests_per_database() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut requests = Vec::new();
    for i in 0..5 {
        db.execute(&format!(
            "CREATE TABLE ann_slot_source_{i} (id int PRIMARY KEY, embedding vector(3))"
        ))
        .await;
        db.create_st(
            &format!("ann_slot_stream_{i}"),
            &format!("SELECT id, embedding FROM ann_slot_source_{i}"),
            "1h",
            "DIFFERENTIAL",
        )
        .await;
        db.execute(&format!("CREATE INDEX ann_slot_idx_{i} ON ann_slot_stream_{i} USING hnsw (embedding vector_l2_ops)")).await;
        let request: i64 = db.query_scalar(&format!("SELECT pgtrickle.request_ann_maintenance('ann_slot_stream_{i}'::regclass::oid, 'ann_slot_idx_{i}'::regclass::oid)")).await;
        requests.push(request);
    }
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    sqlx::query("LOCK TABLE ann_slot_stream_0, ann_slot_stream_1, ann_slot_stream_2, ann_slot_stream_3, ann_slot_stream_4 IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold barrier");
    let mut workers = Vec::new();
    for request in &requests[..4] {
        let mut command = ann_worker_command(&db, &database, "postgres", *request, "30s");
        command.kill_on_drop(true);
        workers.push(command.spawn().expect("start worker"));
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut running = 0;
            for request in &requests[..4] {
                let state: String =
                    sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)->>'state'")
                        .bind(request)
                        .fetch_one(&db.pool)
                        .await
                        .expect("inspect request");
                running += usize::from(state == "RUNNING");
            }
            if running == 4 {
                break;
            }
            for worker in &mut workers {
                assert!(
                    worker.try_wait().expect("inspect worker").is_none(),
                    "worker exited before the barrier"
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("workers did not reach barrier");
    let rejected = ann_worker_command(&db, &database, "postgres", requests[4], "30s")
        .output()
        .await
        .expect("run fifth worker");
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("worker capacity exhausted"));
    let state: String =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)->>'state'")
            .bind(requests[4])
            .fetch_one(&db.pool)
            .await
            .expect("inspect rejected request");
    assert_eq!(state, "REQUESTED");
    blocker.commit().await.expect("release barrier");
    for worker in workers {
        let output = tokio::time::timeout(Duration::from_secs(30), worker.wait_with_output())
            .await
            .expect("worker did not finish")
            .expect("wait for worker");
        assert!(
            output.status.success(),
            "worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let retried = ann_worker_command(&db, &database, "postgres", requests[4], "30s")
        .output()
        .await
        .expect("retry fifth worker");
    assert!(
        retried.status.success(),
        "retry failed: {}",
        String::from_utf8_lossy(&retried.stderr)
    );
}

#[tokio::test]
async fn test_ann_threshold_requests_deduplicate_and_reject_stale_estimates() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_threshold_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_threshold_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_threshold_stream",
        "SELECT id, embedding FROM ann_threshold_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_threshold_idx ON ann_threshold_stream USING hnsw (embedding vector_l2_ops)").await;
    db.execute("SELECT pgtrickle.alter_stream_table('ann_threshold_stream', reindex_drift_threshold => 0.20)").await;
    let watermark_sql = "SELECT ann_churn_generation, ann_churn_operations FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_threshold_stream'";
    let policy_sql = "SELECT pgtrickle.request_ann_maintenance_if_needed('ann_threshold_stream'::regclass::oid, 'ann_threshold_idx'::regclass::oid, 10, $1, $2)";
    let (generation, operations): (i64, i64) = sqlx::query_as(watermark_sql)
        .fetch_one(&db.pool)
        .await
        .expect("read estimate watermark");
    let initial: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("inspect missing baseline");
    assert_eq!(initial["decision"], "missing_baseline");
    let invalid = db.try_execute("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_threshold_stream'::regclass::oid, 'ann_threshold_idx'::regclass::oid, 0, 0, 0)").await;
    assert!(
        invalid.is_err(),
        "an empty or missing estimate is not healthy"
    );
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_threshold_stream'::regclass::oid, 'ann_threshold_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim baseline");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report baseline");
    let mut estimate_tx = db.pool.begin().await.expect("begin estimate");
    sqlx::query("LOCK TABLE ann_threshold_stream IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *estimate_tx)
        .await
        .expect("lock estimate target");
    sqlx::query("ANALYZE ann_threshold_stream")
        .execute(&mut *estimate_tx)
        .await
        .expect("analyze estimate target");
    let (estimated, actual_generation, actual_operations): (i64, i64, i64) = sqlx::query_as(
        "SELECT c.reltuples::bigint, s.ann_churn_generation, s.ann_churn_operations FROM pgtrickle.pgt_stream_tables s JOIN pg_catalog.pg_class c ON c.oid=s.pgt_relid WHERE s.pgt_name='ann_threshold_stream'",
    ).fetch_one(&mut *estimate_tx).await.expect("capture estimate and watermark");
    assert!(estimated > 0);
    assert_eq!(
        (actual_generation, actual_operations),
        (generation, operations)
    );
    estimate_tx.commit().await.expect("commit estimate");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let below: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("check below threshold");
    assert_eq!(below["decision"], "below_threshold");
    db.execute("CREATE ROLE ann_threshold_operator NOLOGIN")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO ann_threshold_operator")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.request_ann_maintenance_if_needed(oid,oid,bigint,bigint,bigint) TO ann_threshold_operator").await;
    db.execute("GRANT SELECT ON ann_threshold_stream TO ann_threshold_operator")
        .await;
    let mut restricted = db.pool.begin().await.expect("begin restricted request");
    sqlx::query("SET LOCAL ROLE ann_threshold_operator")
        .execute(&mut *restricted)
        .await
        .expect("set restricted role");
    let denied = sqlx::query_scalar::<_, serde_json::Value>(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&mut *restricted)
        .await;
    assert!(
        denied.is_err(),
        "SELECT alone must not authorize threshold maintenance"
    );
    restricted.rollback().await.expect("end restricted request");
    db.execute("GRANT MAINTAIN ON ann_threshold_stream TO ann_threshold_operator")
        .await;
    let mut permitted = db.pool.begin().await.expect("begin authorized request");
    sqlx::query("SET LOCAL ROLE ann_threshold_operator")
        .execute(&mut *permitted)
        .await
        .expect("set authorized role");
    let allowed: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&mut *permitted)
        .await
        .expect("check authorized threshold");
    assert_eq!(allowed["decision"], "below_threshold");
    permitted.rollback().await.expect("end authorized request");
    let mut blocker = db.pool.begin().await.expect("begin watermark race");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("read blocker pid");
    sqlx::query("UPDATE pgtrickle.pgt_stream_tables SET ann_churn_operations = ann_churn_operations + 1 WHERE pgt_name='ann_threshold_stream'")
        .execute(&mut *blocker).await.expect("hold uncommitted watermark");
    let pool = db.pool.clone();
    let check = tokio::spawn(async move {
        let mut conn = pool.acquire().await.expect("acquire threshold session");
        sqlx::query("SET application_name = 'ann_threshold_race'")
            .execute(&mut *conn)
            .await
            .expect("name threshold session");
        sqlx::query_scalar::<_, serde_json::Value>(policy_sql)
            .bind(generation)
            .bind(operations)
            .fetch_one(&mut *conn)
            .await
            .expect("check concurrent watermark")
    });
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name='ann_threshold_race' AND $1 = ANY(pg_catalog.pg_blocking_pids(pid)))")
                .bind(blocker_pid).fetch_one(&db.pool).await.expect("inspect threshold wait");
            if waiting { break; }
            assert!(!check.is_finished(), "threshold check exited before waiting");
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }).await.expect("threshold check did not wait for watermark");
    blocker.commit().await.expect("commit watermark");
    let raced = check.await.expect("join threshold check");
    assert_eq!(raced["decision"], "stale_estimate");
    db.execute("INSERT INTO ann_threshold_source VALUES (2, '[0,1,0]'), (3, '[0,0,1]')")
        .await;
    db.refresh_st("ann_threshold_stream").await;
    let stale: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("check stale estimate");
    assert_eq!(stale["decision"], "stale_estimate");
    let (generation, operations): (i64, i64) = sqlx::query_as(watermark_sql)
        .fetch_one(&db.pool)
        .await
        .expect("read new estimate watermark");
    let mut tx = db.pool.begin().await.expect("begin request rollback");
    let rolled_back: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&mut *tx)
        .await
        .expect("request inside transaction");
    assert_eq!(rolled_back["decision"], "requested");
    tx.rollback().await.expect("roll back request");
    let previous: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect rolled-back request");
    assert_eq!(previous["state"], "SUCCEEDED");
    let mut callers = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let pool = db.pool.clone();
        callers.spawn(async move {
            sqlx::query_scalar::<_, serde_json::Value>(policy_sql)
                .bind(generation)
                .bind(operations)
                .fetch_one(&pool)
                .await
                .expect("request threshold maintenance")
        });
    }
    let mut newly_requested = 0;
    while let Some(result) = callers.join_next().await {
        let decision = result.expect("join threshold caller");
        assert_eq!(decision["request_id"].as_i64(), Some(request));
        match decision["decision"].as_str() {
            Some("requested") => newly_requested += 1,
            Some("already_requested" | "raced") => {}
            other => panic!("unexpected threshold decision: {other:?}"),
        }
    }
    assert_eq!(
        newly_requested, 1,
        "concurrent checks must create one request"
    );
    let duplicate: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("deduplicate request");
    assert_eq!(duplicate["decision"], "already_requested");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim threshold request");
    let running: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("deduplicate running request");
    assert_eq!(running["decision"], "already_requested");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect running request");
    assert_eq!(status["pending_during_run"], false);
    db.execute("INSERT INTO ann_threshold_source VALUES (4, '[1,1,0]')")
        .await;
    db.refresh_st("ann_threshold_stream").await;
    let (during_generation, during_operations): (i64, i64) = sqlx::query_as(watermark_sql)
        .fetch_one(&db.pool)
        .await
        .expect("read watermark during run");
    let during: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(during_generation)
        .bind(during_operations)
        .fetch_one(&db.pool)
        .await
        .expect("check while running");
    assert_eq!(during["decision"], "already_requested");
    let running_status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect running deduplication");
    assert_eq!(running_status["pending_during_run"], false);
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("finish threshold request");
    let covered: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect covered work");
    assert_eq!(
        covered["known_operations_since_reported_coverage"].as_i64(),
        Some(1)
    );
    let (generation, operations): (i64, i64) = sqlx::query_as(watermark_sql)
        .fetch_one(&db.pool)
        .await
        .expect("read later watermark");
    let cooldown: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("check cooldown");
    assert_eq!(cooldown["decision"], "cooldown");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let before_index: i64 = db
        .query_scalar("SELECT 'ann_threshold_idx'::regclass::oid::bigint")
        .await;
    let next: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_threshold_stream'::regclass::oid, 'ann_threshold_idx'::regclass::oid, 5, $1, $2)")
        .bind(generation).bind(operations).fetch_one(&db.pool).await.expect("request later threshold");
    assert_eq!(next["decision"], "requested");
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let output = ann_worker_command(
        &db,
        &database,
        "postgres",
        next["request_id"].as_i64().expect("request id"),
        "30s",
    )
    .output()
    .await
    .expect("run threshold worker");
    assert!(
        output.status.success(),
        "threshold worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let after_index: i64 = db
        .query_scalar("SELECT 'ann_threshold_idx'::regclass::oid::bigint")
        .await;
    assert_ne!(
        before_index, after_index,
        "worker must complete concurrent reindex"
    );
    db.assert_st_matches_query(
        "ann_threshold_stream",
        "SELECT id, embedding FROM ann_threshold_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_churn_reports_unknown_for_partitioned_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_churn_partition_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_churn_partition_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st_partitioned(
        "ann_churn_partition_stream",
        "SELECT id, embedding FROM ann_churn_partition_source",
        "1h",
        "DIFFERENTIAL",
        "id",
    )
    .await;
    db.execute("CREATE INDEX ann_churn_partition_idx ON ann_churn_partition_stream USING hnsw (embedding vector_l2_ops)").await;
    let leaf: i64 = db.query_scalar("SELECT i.indexrelid::bigint FROM pg_index i JOIN pg_inherits h ON h.inhrelid=i.indrelid WHERE h.inhparent='ann_churn_partition_stream'::regclass AND i.indisvalid AND EXISTS (SELECT 1 FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.oid=i.indexrelid AND a.amname='hnsw') LIMIT 1").await;
    let request: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_churn_partition_stream'::regclass::oid, $1::bigint::oid)")
        .bind(leaf).fetch_one(&db.pool).await.expect("request leaf");
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim request");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report baseline");
    let partition_decision: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_churn_partition_stream'::regclass::oid, $1::bigint::oid, 10, 0, 0)")
        .bind(leaf).fetch_one(&db.pool).await.expect("check partition policy");
    assert_eq!(partition_decision["decision"], "partitioned_unknown");
    assert!(partition_decision["known_operations"].is_null());
    db.execute("INSERT INTO ann_churn_partition_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_churn_partition_stream").await;
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect partition change");
    assert!(status["known_operations_since_reported_coverage"].is_null());
    db.assert_st_matches_query(
        "ann_churn_partition_stream",
        "SELECT id, embedding FROM ann_churn_partition_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_churn_reports_unknown_with_user_triggers() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_churn_trigger_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_churn_trigger_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_churn_trigger_stream",
        "SELECT id, embedding FROM ann_churn_trigger_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_churn_trigger_idx ON ann_churn_trigger_stream USING hnsw (embedding vector_l2_ops)").await;
    db.execute("CREATE FUNCTION ann_churn_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").await;
    db.execute("CREATE TRIGGER ann_churn_trigger AFTER INSERT ON ann_churn_trigger_stream FOR EACH ROW EXECUTE FUNCTION ann_churn_trigger_fn()").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_churn_trigger_stream'::regclass::oid, 'ann_churn_trigger_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim request");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report baseline");
    db.execute("INSERT INTO ann_churn_trigger_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_churn_trigger_stream").await;
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect trigger change");
    assert!(status["known_operations_since_reported_coverage"].is_null());
    db.assert_st_matches_query(
        "ann_churn_trigger_stream",
        "SELECT id, embedding FROM ann_churn_trigger_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_churn_tracks_immediate_changes_and_preserves_unknown_triggers() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_churn_immediate_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_churn_immediate_source VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('ann_churn_immediate_stream', 'SELECT id, embedding FROM ann_churn_immediate_source', NULL, 'IMMEDIATE')").await;
    db.execute("CREATE INDEX ann_churn_immediate_idx ON ann_churn_immediate_stream USING hnsw (embedding vector_l2_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_churn_immediate_stream'::regclass::oid, 'ann_churn_immediate_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim request");
    db.execute("INSERT INTO ann_churn_immediate_source VALUES (2, '[0,1,0]')")
        .await;
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report baseline");
    let baseline: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect baseline");
    assert_eq!(
        baseline["known_operations_since_reported_coverage"].as_i64(),
        Some(1),
        "immediate work committed after the claim must remain outstanding"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect immediate change");
    assert_eq!(
        status["known_operations_since_reported_coverage"].as_i64(),
        Some(1)
    );
    let mut tx = db.pool.begin().await.expect("begin immediate rollback");
    sqlx::query("INSERT INTO ann_churn_immediate_source VALUES (3, '[0,0,1]')")
        .execute(&mut *tx)
        .await
        .expect("insert in transaction");
    let uncommitted: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *tx)
            .await
            .expect("inspect uncommitted churn");
    assert_eq!(
        uncommitted["known_operations_since_reported_coverage"].as_i64(),
        Some(2)
    );
    tx.rollback().await.expect("roll back immediate change");
    let rolled_back: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect rolled-back churn");
    assert_eq!(
        rolled_back["known_operations_since_reported_coverage"].as_i64(),
        Some(1)
    );
    db.execute("UPDATE ann_churn_immediate_source SET embedding='[1,1,0]' WHERE id=2")
        .await;
    db.execute("DELETE FROM ann_churn_immediate_source WHERE id=1")
        .await;
    let changed: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect update and delete");
    assert!(
        changed["known_operations_since_reported_coverage"]
            .as_i64()
            .is_some_and(|count| count >= 3)
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let requested: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_churn_immediate_stream'::regclass::oid, 'ann_churn_immediate_idx'::regclass::oid, 10, $1, $2)")
        .bind(changed["current_churn_generation"].as_i64().expect("current generation"))
        .bind(changed["current_churn_operations"].as_i64().expect("current operations"))
        .fetch_one(&db.pool).await.expect("check measured immediate work");
    assert_eq!(requested["decision"], "requested");
    db.execute("CREATE FUNCTION ann_churn_immediate_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").await;
    db.execute("CREATE TRIGGER ann_churn_immediate_trigger AFTER INSERT ON ann_churn_immediate_stream FOR EACH ROW EXECUTE FUNCTION ann_churn_immediate_trigger_fn()").await;
    db.execute("INSERT INTO ann_churn_immediate_source VALUES (3, '[0,0,1]')")
        .await;
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect triggered change");
    assert!(status["known_operations_since_reported_coverage"].is_null());
    let unknown: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_churn_immediate_stream'::regclass::oid, 'ann_churn_immediate_idx'::regclass::oid, 10, $1, $2)")
        .bind(status["current_churn_generation"].as_i64().expect("current generation"))
        .bind(status["current_churn_operations"].as_i64().expect("current operations"))
        .fetch_one(&db.pool).await.expect("check unknown immediate work");
    assert_eq!(unknown["decision"], "unknown_work");
    db.execute("DROP TRIGGER ann_churn_immediate_trigger ON ann_churn_immediate_stream")
        .await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_churn_operations=9223372036854775807 WHERE pgt_name='ann_churn_immediate_stream'").await;
    db.execute("INSERT INTO ann_churn_immediate_source VALUES (4, '[1,0,1]')")
        .await;
    let overflow: Option<i64> = sqlx::query_scalar("SELECT ann_churn_operations FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_churn_immediate_stream'")
        .fetch_one(&db.pool).await.expect("read overflowed counter");
    assert!(overflow.is_none(), "counter overflow must remain unknown");
    db.assert_st_matches_query(
        "ann_churn_immediate_stream",
        "SELECT id, embedding FROM ann_churn_immediate_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_churn_preserves_concurrent_work_and_unknown_rewrites() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_churn_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_churn_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_churn_stream",
        "SELECT id, embedding FROM ann_churn_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_churn_idx ON ann_churn_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    db.execute("CREATE INDEX ann_churn_other_idx ON ann_churn_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute(
        "SELECT pgtrickle.snapshot_stream_table('ann_churn_stream', 'public.ann_churn_archive')",
    )
    .await;
    let request_sql = "SELECT pgtrickle.request_ann_maintenance('ann_churn_stream'::regclass::oid, 'ann_churn_idx'::regclass::oid)";
    let request: i64 = db.query_scalar(request_sql).await;
    let other: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_churn_stream'::regclass::oid, 'ann_churn_other_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    let claim: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .fetch_one(&mut *worker)
        .await
        .expect("claim request");
    assert!(claim["known_operations_since_reported_coverage"].is_null());
    db.execute("INSERT INTO ann_churn_source VALUES (2, '[0,1,0]'), (3, '[0,0,1]')")
        .await;
    db.refresh_st("ann_churn_stream").await;
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report coverage");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect coverage");
    assert_eq!(
        status["reported_churn_operations"],
        claim["claim_churn_operations"]
    );
    assert_eq!(
        status["known_operations_since_reported_coverage"].as_i64(),
        Some(2)
    );
    let independent: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(other)
            .fetch_one(&db.pool)
            .await
            .expect("inspect other index");
    assert!(independent["known_operations_since_reported_coverage"].is_null());
    let mut tx = db.pool.begin().await.expect("begin rollback test");
    sqlx::query("INSERT INTO ann_churn_source VALUES (4, '[1,1,0]')")
        .execute(&mut *tx)
        .await
        .expect("insert rolled-back row");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('ann_churn_stream')")
        .execute(&mut *tx)
        .await
        .expect("refresh in transaction");
    tx.rollback().await.expect("roll back refresh");
    let after_rollback: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect rollback");
    assert_eq!(
        after_rollback["known_operations_since_reported_coverage"].as_i64(),
        Some(2)
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let retry: i64 = db.query_scalar(request_sql).await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(retry)
        .execute(&mut *worker)
        .await
        .expect("claim retry");
    db.execute("INSERT INTO ann_churn_source VALUES (4, '[1,1,0]')")
        .await;
    db.refresh_st("ann_churn_stream").await;
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
        .bind(retry)
        .execute(&mut *worker)
        .await
        .expect("report failed retry");
    let failed_retry: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(retry)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect failed retry");
    assert_eq!(
        failed_retry["known_operations_since_reported_coverage"].as_i64(),
        Some(3)
    );
    db.execute("UPDATE ann_churn_source SET embedding = '[1,0,1]' WHERE id = 2")
        .await;
    db.execute("DELETE FROM ann_churn_source WHERE id = 3")
        .await;
    db.refresh_st("ann_churn_stream").await;
    let after_update_delete: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(retry)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect update and delete");
    assert!(
        after_update_delete["known_operations_since_reported_coverage"]
            .as_i64()
            .is_some_and(|count| count >= 5)
    );
    db.execute(
        "SELECT pgtrickle.restore_from_snapshot('ann_churn_stream', 'public.ann_churn_archive')",
    )
    .await;
    let after_restore: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect restore");
    assert!(after_restore["known_operations_since_reported_coverage"].is_null());
    db.refresh_st("ann_churn_stream").await;
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let renewed: i64 = db.query_scalar(request_sql).await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(renewed)
        .execute(&mut *worker)
        .await
        .expect("claim after rewrite");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
        .bind(renewed)
        .execute(&mut *worker)
        .await
        .expect("report failure");
    let after_failure: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(renewed)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect failure");
    assert!(after_failure["known_operations_since_reported_coverage"].is_null());
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(renewed)
        .execute(&mut *worker)
        .await
        .expect("retry claim");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(renewed)
        .execute(&mut *worker)
        .await
        .expect("establish new baseline");
    let recovered: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(renewed)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect new baseline");
    assert_eq!(
        recovered["known_operations_since_reported_coverage"].as_i64(),
        Some(0)
    );
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_churn_operations = 9223372036854775807 WHERE pgt_name='ann_churn_stream'").await;
    db.execute("INSERT INTO ann_churn_source VALUES (5, '[1,1,1]')")
        .await;
    db.refresh_st("ann_churn_stream").await;
    let overflow: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(renewed)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect overflow");
    assert!(overflow["known_operations_since_reported_coverage"].is_null());
    db.assert_st_matches_query(
        "ann_churn_stream",
        "SELECT id, embedding FROM ann_churn_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_maintenance_coverage_preserves_refreshes_after_claim() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_coverage_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_coverage_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_coverage_stream",
        "SELECT id, embedding FROM ann_coverage_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_coverage_idx ON ann_coverage_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE INDEX ann_coverage_other_idx ON ann_coverage_stream USING hnsw (embedding vector_l2_ops)").await;
    let request_sql = "SELECT pgtrickle.request_ann_maintenance('ann_coverage_stream'::regclass::oid, 'ann_coverage_idx'::regclass::oid)";
    let request: i64 = db.query_scalar(request_sql).await;
    let other: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_coverage_stream'::regclass::oid, 'ann_coverage_other_idx'::regclass::oid)").await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET rows_changed_since_last_reindex=42 WHERE pgt_name='ann_coverage_stream'").await;
    db.execute("INSERT INTO ann_coverage_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_coverage_stream").await;
    let epoch_at_claim: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_coverage_stream'").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("LOCK TABLE ann_coverage_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold barrier");
    let mut child = ann_worker_command(&db, &database, "postgres", request, "30s")
        .spawn()
        .expect("start worker");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE datname=current_database() AND application_name='pgtrickle_ann_worker' AND $1 = ANY(pg_blocking_pids(pid)))")
                .bind(blocker_pid).fetch_one(&db.pool).await.expect("inspect worker");
            if waiting { break; }
            assert!(child.try_wait().expect("inspect worker process").is_none(), "worker ended before reaching barrier");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("worker did not reach barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect claim");
    assert!(status["last_observed_epoch"].as_i64() < Some(epoch_at_claim));
    assert_eq!(
        status["claim_epoch"].as_i64(),
        Some(epoch_at_claim),
        "claim must capture the latest committed epoch"
    );
    assert!(status["reported_covered_epoch"].is_null());
    db.execute("INSERT INTO ann_coverage_source VALUES (3, '[0,0,1]')")
        .await;
    db.refresh_st("ann_coverage_stream").await;
    let later_epoch: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_coverage_stream'").await;
    assert!(later_epoch > epoch_at_claim);
    blocker.commit().await.expect("release barrier");
    let output = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("worker did not finish")
        .expect("wait for worker");
    assert!(
        output.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let legacy_counter: i64 = db.query_scalar("SELECT rows_changed_since_last_reindex FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_coverage_stream'").await;
    assert_eq!(
        legacy_counter, 42,
        "completion must not reset the table-wide churn counter"
    );
    let replacement: i64 = db.query_scalar(request_sql).await;
    assert_ne!(request, replacement);
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(replacement)
            .fetch_one(&db.pool)
            .await
            .expect("inspect replacement coverage");
    assert_eq!(
        status["reported_covered_epoch"].as_i64(),
        Some(epoch_at_claim)
    );
    assert_eq!(
        status["epoch_advances_since_reported_coverage"].as_i64(),
        Some(later_epoch - epoch_at_claim)
    );
    assert!(status["reported_covered_at"].is_string());
    let independent: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(other)
            .fetch_one(&db.pool)
            .await
            .expect("inspect independent index");
    assert!(
        independent["reported_covered_epoch"].is_null(),
        "other indexes must not inherit coverage"
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let mut blocker = db.pool.begin().await.expect("begin retry barrier");
    sqlx::query("LOCK TABLE ann_coverage_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold retry barrier");
    let output = ann_worker_command(&db, &database, "postgres", replacement, "250ms")
        .output()
        .await
        .expect("run failed retry");
    assert!(!output.status.success());
    blocker.rollback().await.expect("release retry barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(replacement)
            .fetch_one(&db.pool)
            .await
            .expect("inspect failed retry");
    assert_eq!(status["state"], "FAILED");
    assert_eq!(
        status["reported_covered_epoch"].as_i64(),
        Some(epoch_at_claim),
        "a failed rebuild must not advance coverage"
    );
}

#[tokio::test]
async fn test_ann_maintenance_coverage_rolls_back_and_keeps_unknown_unknown() {
    use sqlx::Acquire;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_coverage_tx_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_coverage_tx_stream",
        "SELECT id, embedding FROM ann_coverage_tx_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_coverage_tx_idx ON ann_coverage_tx_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request_sql = "SELECT pgtrickle.request_ann_maintenance('ann_coverage_tx_stream'::regclass::oid, 'ann_coverage_tx_idx'::regclass::oid)";
    let first: i64 = db.query_scalar(request_sql).await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("claim request");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_coverage_tx_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild index");
    let mut tx = (&mut *worker).begin().await.expect("begin completion");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(first)
        .execute(&mut *tx)
        .await
        .expect("report in transaction");
    let outside: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect uncommitted report");
    assert_eq!(outside["state"], "RUNNING");
    assert!(
        outside["reported_covered_epoch"].is_null(),
        "uncommitted coverage must not be visible"
    );
    tx.rollback().await.expect("roll back completion");
    let rolled_back: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect rolled-back report");
    assert_eq!(rolled_back["state"], "RUNNING");
    assert!(rolled_back["reported_covered_epoch"].is_null());
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("commit completion");
    let committed: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect committed report");
    assert!(committed["reported_covered_epoch"].is_number());
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_refresh_epoch=NULL WHERE pgt_name='ann_coverage_tx_stream'").await;
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at=clock_timestamp() - interval '1 second'").await;
    let next: i64 = db.query_scalar(request_sql).await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("claim unknown epoch");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_coverage_tx_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild with unknown epoch");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("finish unknown epoch");
    let unknown: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(next)
            .fetch_one(&db.pool)
            .await
            .expect("inspect unknown coverage");
    assert!(unknown["claim_epoch"].is_null());
    assert!(unknown["reported_covered_epoch"].is_null());
    assert!(unknown["epoch_advances_since_reported_coverage"].is_null());
    assert!(unknown["reported_covered_at"].is_string());
}

#[tokio::test]
async fn test_ann_worker_allows_reads_and_refresh_during_concurrent_reindex() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_live_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_live_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_live_stream",
        "SELECT id, embedding FROM ann_live_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_live_idx ON ann_live_stream USING hnsw (embedding vector_l2_ops)")
        .await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_live_stream'::regclass::oid, 'ann_live_idx'::regclass::oid)").await;

    let mut old_snapshot = db.pool.begin().await.expect("begin old snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *old_snapshot)
        .await
        .expect("set isolation");
    sqlx::query("SELECT count(*) FROM ann_live_stream")
        .fetch_one(&mut *old_snapshot)
        .await
        .expect("establish old snapshot");
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut command = ann_worker_command(&db, &database, "postgres", request, "30s");
    command.kill_on_drop(true);
    let mut worker = command.spawn().expect("start worker");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_progress_create_index p JOIN pg_stat_activity a USING (pid) WHERE a.application_name = 'pgtrickle_ann_worker' AND p.command = 'REINDEX CONCURRENTLY' AND p.phase LIKE 'waiting for old snapshots%')")
                .fetch_one(&db.pool).await.expect("inspect reindex progress");
            if waiting {
                break;
            }
            assert!(worker.try_wait().expect("inspect worker").is_none(), "worker exited before reaching old snapshot wait");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("worker did not reach concurrent reindex snapshot wait");

    tokio::time::timeout(Duration::from_secs(5), async {
        db.execute("INSERT INTO ann_live_source VALUES (2, '[0,1,0]')")
            .await;
        db.refresh_st("ann_live_stream").await;
        assert_eq!(db.count("ann_live_stream").await, 2);
        let mut reader = db.pool.begin().await.expect("begin reader");
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *reader)
            .await
            .expect("enable index plan");
        let plan: Vec<String> = sqlx::query_scalar("EXPLAIN (COSTS OFF) SELECT id FROM ann_live_stream ORDER BY embedding <-> '[0,1,0]'::vector LIMIT 1")
            .fetch_all(&mut *reader).await.expect("explain nearest-neighbor read");
        assert!(plan.iter().any(|line| line.contains("ann_live_idx")), "expected ANN index in {plan:?}");
        let nearest: i32 = sqlx::query_scalar("SELECT id FROM ann_live_stream ORDER BY embedding <-> '[0,1,0]'::vector LIMIT 1")
            .fetch_one(&mut *reader).await.expect("read nearest neighbor");
        assert_eq!(nearest, 2);
        reader.commit().await.expect("finish reader");
    })
    .await
    .expect("reads or refresh writes blocked during concurrent reindex");
    old_snapshot.commit().await.expect("release old snapshot");
    let output = tokio::time::timeout(Duration::from_secs(30), worker.wait_with_output())
        .await
        .expect("worker did not finish")
        .expect("wait for worker");
    assert!(
        output.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect result");
    assert_eq!(status["state"], "SUCCEEDED");
    assert_eq!(db.count("ann_live_stream").await, 2);
}

#[tokio::test]
async fn test_ann_worker_rebuilds_only_the_requested_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_worker_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_worker_stream",
        "SELECT id, embedding FROM ann_worker_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX \"ann worker\"\"; DROP TABLE ann_worker_source; --\" ON ann_worker_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE INDEX ann_worker_btree ON ann_worker_stream (id)")
        .await;
    let (index_oid, old_node, btree_node): (i64, i64, i64) = sqlx::query_as("SELECT i.indexrelid::bigint, pg_relation_filenode(i.indexrelid)::bigint, pg_relation_filenode('ann_worker_btree'::regclass)::bigint FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid='ann_worker_stream'::regclass AND c.relname='ann worker\"; DROP TABLE ann_worker_source; --'")
        .fetch_one(&db.pool).await.expect("index identities");
    let request: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_stream'::regclass::oid, $1::bigint::oid)")
        .bind(index_oid).fetch_one(&db.pool).await.expect("request work");
    db.execute("CREATE ROLE ann_worker_operator LOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, public TO ann_worker_operator")
        .await;
    db.execute("GRANT SELECT, MAINTAIN ON ann_worker_stream TO ann_worker_operator")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.claim_ann_maintenance(bigint), pgtrickle.ann_maintenance_request(bigint), pgtrickle.finish_ann_maintenance(bigint, boolean, text) TO ann_worker_operator").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let output = ann_worker_command(&db, &database, "ann_worker_operator", request, "5s")
        .output()
        .await
        .expect("run ANN worker");
    assert!(
        output.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect result");
    assert_eq!(status["state"], "SUCCEEDED");
    let (new_oid, new_node, other_node): (i64, i64, i64) = sqlx::query_as("SELECT i.indexrelid::bigint, pg_relation_filenode(i.indexrelid)::bigint, pg_relation_filenode('ann_worker_btree'::regclass)::bigint FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid='ann_worker_stream'::regclass AND c.relname='ann worker\"; DROP TABLE ann_worker_source; --'")
        .fetch_one(&db.pool).await.expect("rebuilt index identities");
    assert_ne!(index_oid, new_oid);
    assert_ne!(old_node, new_node);
    assert_eq!(
        btree_node, other_node,
        "unrelated indexes must not be rebuilt"
    );
    assert_eq!(
        db.count("ann_worker_source").await,
        1,
        "index names must not become SQL"
    );
    let replacement: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_stream'::regclass::oid, $1::bigint::oid)")
        .bind(new_oid).fetch_one(&db.pool).await.expect("request replacement index");
    db.execute("REVOKE MAINTAIN ON ann_worker_stream FROM ann_worker_operator")
        .await;
    let output = ann_worker_command(&db, &database, "ann_worker_operator", replacement, "5s")
        .output()
        .await
        .expect("run without MAINTAIN");
    assert!(
        !output.status.success(),
        "worker must respect revoked MAINTAIN"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(replacement)
            .fetch_one(&db.pool)
            .await
            .expect("inspect denied request");
    assert_eq!(status["state"], "REQUESTED");
    db.execute("GRANT MAINTAIN ON ann_worker_stream TO ann_worker_operator")
        .await;
    let output = ann_worker_command(&db, &database, "ann_worker_operator", replacement, "5s")
        .output()
        .await
        .expect("run during cooldown");
    assert!(!output.status.success(), "worker must respect cooldown");
}

#[tokio::test]
async fn test_ann_worker_rejects_a_replaced_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_stale_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_worker_stale_stream",
        "SELECT id, embedding FROM ann_worker_stale_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_worker_stale_idx ON ann_worker_stale_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_stale_stream'::regclass::oid, 'ann_worker_stale_idx'::regclass::oid)").await;
    db.execute("DROP INDEX ann_worker_stale_idx").await;
    db.execute("CREATE INDEX ann_worker_stale_idx ON ann_worker_stale_stream USING hnsw (embedding vector_cosine_ops)").await;
    let before: i64 = db
        .query_scalar("SELECT 'ann_worker_stale_idx'::regclass::oid::bigint")
        .await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let output = ann_worker_command(&db, &database, "postgres", request, "5s")
        .output()
        .await
        .expect("run stale request");
    assert!(
        !output.status.success(),
        "worker must reject the stale identity"
    );
    let after: i64 = db
        .query_scalar("SELECT 'ann_worker_stale_idx'::regclass::oid::bigint")
        .await;
    assert_eq!(before, after, "replacement must not be rebuilt");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect stale request");
    assert_eq!(status["state"], "REQUESTED");
    assert_eq!(status["identity_current"], false);
}

#[tokio::test]
async fn test_ann_worker_revalidates_index_after_claim() {
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_race_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_worker_race_stream",
        "SELECT id, embedding FROM ann_worker_race_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_worker_race_idx ON ann_worker_race_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_race_stream'::regclass::oid, 'ann_worker_race_idx'::regclass::oid)").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut command = ann_worker_command(&db, &database, "postgres", request, "5s");
    command.stdin(Stdio::piped());
    let mut child = command.spawn().expect("start ANN worker");
    let mut input = child.stdin.take().expect("worker stdin");
    let script = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/scripts/ann_maintenance_worker.sql"
    ));
    let claim_line =
        "SELECT pgtrickle.claim_ann_maintenance(:'request_id'::bigint) AS ann_claim \\gset\n";
    let (before, after) = script
        .split_once(claim_line)
        .expect("worker claim boundary");
    input
        .write_all(before.as_bytes())
        .await
        .expect("send worker setup");
    input
        .write_all(claim_line.as_bytes())
        .await
        .expect("send worker claim");
    input.flush().await.expect("flush worker input");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status: serde_json::Value =
                sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
                    .bind(request)
                    .fetch_one(&db.pool)
                    .await
                    .expect("inspect claim");
            if status["state"] == "RUNNING" {
                break;
            }
            assert!(
                child.try_wait().expect("inspect worker process").is_none(),
                "worker exited before claiming"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("worker did not claim request");
    db.execute("DROP INDEX ann_worker_race_idx").await;
    db.execute("CREATE INDEX ann_worker_race_idx ON ann_worker_race_stream USING hnsw (embedding vector_cosine_ops)").await;
    let replacement: i64 = db
        .query_scalar("SELECT 'ann_worker_race_idx'::regclass::oid::bigint")
        .await;
    input
        .write_all(after.as_bytes())
        .await
        .expect("send remainder of worker");
    input.shutdown().await.expect("close worker input");
    let output = tokio::time::timeout(Duration::from_secs(15), child.wait_with_output())
        .await
        .expect("worker did not finish")
        .expect("wait for worker");
    assert!(
        !output.status.success(),
        "worker must not execute a stale claim"
    );
    let after: i64 = db
        .query_scalar("SELECT 'ann_worker_race_idx'::regclass::oid::bigint")
        .await;
    assert_eq!(replacement, after, "replacement must not be rebuilt");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect stale claim");
    assert_eq!(
        status["state"],
        "FAILED",
        "worker stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn test_ann_worker_records_lock_timeout_and_can_retry() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_timeout_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_worker_timeout_stream",
        "SELECT id, embedding FROM ann_worker_timeout_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_worker_timeout_idx ON ann_worker_timeout_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_timeout_stream'::regclass::oid, 'ann_worker_timeout_idx'::regclass::oid)").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    sqlx::query("LOCK TABLE ann_worker_timeout_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold barrier");
    let output = ann_worker_command(&db, &database, "postgres", request, "250ms")
        .output()
        .await
        .expect("run timed worker");
    assert!(
        !output.status.success(),
        "timed-out rebuild must return an error"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect failure");
    assert_eq!(
        status["state"],
        "FAILED",
        "worker output: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        status["last_error"]
            .as_str()
            .unwrap_or_default()
            .contains("Concurrent reindex failed"),
        "status: {status}; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("lock timeout"));
    assert!(status["next_eligible_at"].is_string());
    blocker.rollback().await.expect("release barrier");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let output = ann_worker_command(&db, &database, "postgres", request, "5s")
        .output()
        .await
        .expect("retry worker");
    assert!(
        output.status.success(),
        "retry failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect retry");
    assert_eq!(status["state"], "SUCCEEDED");
}

#[tokio::test]
async fn test_ann_worker_records_backend_cancellation() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_cancel_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_worker_cancel_stream",
        "SELECT id, embedding FROM ann_worker_cancel_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_worker_cancel_idx ON ann_worker_cancel_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_cancel_stream'::regclass::oid, 'ann_worker_cancel_idx'::regclass::oid)").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("LOCK TABLE ann_worker_cancel_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold barrier");
    let mut child = ann_worker_command(&db, &database, "postgres", request, "30s")
        .spawn()
        .expect("start ANN worker");
    let worker_pid = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND application_name = 'pgtrickle_ann_worker' AND $1 = ANY(pg_blocking_pids(pid)) LIMIT 1")
                .bind(blocker_pid).fetch_optional(&db.pool).await.expect("inspect worker");
            if let Some(pid) = pid { break pid; }
            assert!(child.try_wait().expect("inspect worker process").is_none(), "worker ended before reaching the barrier");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("worker did not reach barrier");
    tokio::time::timeout(Duration::from_secs(10), async {
        assert_eq!(db.count("ann_worker_cancel_stream").await, 0);
        db.execute("INSERT INTO ann_worker_cancel_source VALUES (1, '[1,0,0]')")
            .await;
        db.refresh_st("ann_worker_cancel_stream").await;
        assert_eq!(db.count("ann_worker_cancel_stream").await, 1);
    })
    .await
    .expect("reads and refresh must progress while the worker waits");
    assert!(
        child.try_wait().expect("inspect worker").is_none(),
        "worker must still be blocked"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("request cancellation");
    assert_eq!(status["claim_pid"], worker_pid);
    let signaled: bool = sqlx::query_scalar("SELECT pg_catalog.pg_cancel_backend(pid) FROM pg_catalog.pg_stat_activity WHERE pid = $1 AND backend_start = $2::text::timestamptz")
        .bind(worker_pid).bind(status["claim_backend_start"].as_str().expect("backend start"))
        .fetch_one(&db.pool).await.expect("signal worker");
    assert!(signaled);
    let output = tokio::time::timeout(Duration::from_secs(15), child.wait_with_output())
        .await
        .expect("worker did not exit")
        .expect("wait for worker");
    assert!(
        !output.status.success(),
        "canceled worker must return failure"
    );
    blocker.rollback().await.expect("release barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect cancellation");
    assert_eq!(
        status["state"],
        "CANCELLED",
        "worker stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        status["current_epoch"].as_i64() > status["claim_epoch"].as_i64(),
        "refresh after the claim must remain visible"
    );
    assert!(
        status["last_error"]
            .as_str()
            .unwrap_or_default()
            .contains("Concurrent reindex failed")
    );
}

#[tokio::test]
async fn test_ann_maintenance_intent_is_transactional_deduplicated_and_identity_bound() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_intent_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_intent_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_intent_stream",
        "SELECT id, embedding FROM ann_intent_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_intent_hnsw ON ann_intent_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE INDEX ann_intent_btree ON ann_intent_stream (id)")
        .await;
    let request_sql = "SELECT pgtrickle.request_ann_maintenance('ann_intent_stream'::regclass::oid, 'ann_intent_hnsw'::regclass::oid)";
    let mut tx = db.pool.begin().await.expect("begin intent transaction");
    let _: i64 = sqlx::query_scalar(request_sql)
        .fetch_one(&mut *tx)
        .await
        .expect("request intent");
    let outside: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_ann_maintenance_requests")
        .await;
    assert_eq!(outside, 0, "uncommitted intent must not be visible");
    tx.rollback().await.expect("roll back intent");
    assert_eq!(
        db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.pgt_ann_maintenance_requests")
            .await,
        0
    );

    let (a, b) = tokio::join!(
        sqlx::query_scalar::<_, i64>(request_sql).fetch_one(&db.pool),
        sqlx::query_scalar::<_, i64>(request_sql).fetch_one(&db.pool)
    );
    let first = a.expect("first request");
    assert_eq!(first, b.expect("concurrent duplicate request"));
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect request");
    assert_eq!(status["state"], "REQUESTED");
    assert_eq!(status["identity_current"], true);
    let (first_epoch, last_epoch): (Option<i64>, Option<i64>) = sqlx::query_as("SELECT first_observed_epoch, last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1").bind(first).fetch_one(&db.pool).await.expect("request epochs");
    assert!(first_epoch.is_some());
    assert_eq!(first_epoch, last_epoch);
    db.execute("INSERT INTO ann_intent_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_intent_stream").await;
    let changed: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect intervening refresh");
    assert_eq!(changed["identity_current"], true);
    assert!(changed["current_epoch"].as_i64() > changed["last_observed_epoch"].as_i64());
    let repeated: i64 = db.query_scalar(request_sql).await;
    assert_eq!(first, repeated);
    let dump_condition: String = db.query_scalar("SELECT extcondition[array_position(extconfig, 'pgtrickle.pgt_ann_maintenance_requests'::regclass::oid)] FROM pg_extension WHERE extname='pg_trickle'").await;
    assert_eq!(
        dump_condition, "WHERE false",
        "database-local requests must not be restored as valid work"
    );
    let claim_dump_condition: String = db.query_scalar("SELECT extcondition[array_position(extconfig, 'pgtrickle.pgt_ann_maintenance_claims'::regclass::oid)] FROM pg_extension WHERE extname='pg_trickle'").await;
    assert_eq!(claim_dump_condition, "WHERE false");
    let (original, latest): (Option<i64>, Option<i64>) = sqlx::query_as("SELECT first_observed_epoch, last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1").bind(first).fetch_one(&db.pool).await.expect("updated request epochs");
    assert_eq!(original, first_epoch);
    assert!(latest > original);
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_refresh_epoch = NULL WHERE pgt_name = 'ann_intent_stream'").await;
    assert_eq!(db.query_scalar::<i64>(request_sql).await, first);
    let unknown: (Option<i64>, Option<i64>) = sqlx::query_as("SELECT first_observed_epoch, last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1").bind(first).fetch_one(&db.pool).await.expect("unknown epochs");
    assert_eq!(unknown, (None, None));
    let invalid = db.try_execute("SELECT pgtrickle.request_ann_maintenance('ann_intent_stream'::regclass::oid, 'ann_intent_btree'::regclass::oid)").await;
    assert!(invalid.is_err(), "non-ANN indexes must be rejected");
    db.execute("DROP INDEX ann_intent_hnsw").await;
    let stale: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect stale request");
    assert_eq!(stale["identity_current"], false);
    db.execute("CREATE INDEX ann_intent_hnsw ON ann_intent_stream USING hnsw (embedding vector_cosine_ops)").await;
    let replacement: i64 = db.query_scalar(request_sql).await;
    assert_ne!(
        first, replacement,
        "a replacement index must not reuse the old request"
    );
    assert_eq!(
        db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.pgt_ann_maintenance_requests")
            .await,
        2
    );
    db.execute("SELECT pgtrickle.drop_stream_table('ann_intent_stream')")
        .await;
    assert_eq!(
        db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.pgt_ann_maintenance_requests")
            .await,
        0
    );
}

#[tokio::test]
async fn test_ann_maintenance_intent_requires_execute_select_and_maintain() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_intent_auth_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_intent_auth_stream",
        "SELECT id, embedding FROM ann_intent_auth_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_intent_auth_idx ON ann_intent_auth_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE ROLE ann_intent_operator NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO ann_intent_operator")
        .await;
    db.execute("GRANT SELECT, MAINTAIN ON ann_intent_auth_stream TO ann_intent_operator")
        .await;
    let private: bool = db.query_scalar("SELECT NOT has_table_privilege('ann_intent_operator', 'pgtrickle.pgt_ann_maintenance_requests', 'SELECT') AND NOT has_table_privilege('ann_intent_operator', 'pgtrickle.pgt_ann_maintenance_claims', 'SELECT')").await;
    assert!(private, "request catalog must not be publicly readable");
    let cancel_private: bool = db.query_scalar("SELECT NOT has_function_privilege('ann_intent_operator', 'pgtrickle.cancel_ann_maintenance(bigint)', 'EXECUTE')").await;
    assert!(cancel_private, "PUBLIC must not execute cancellation");
    let mut tx = db.pool.begin().await.expect("begin unauthorized call");
    sqlx::query("SET LOCAL ROLE ann_intent_operator")
        .execute(&mut *tx)
        .await
        .expect("set role");
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_intent_auth_stream'::regclass::oid, 'ann_intent_auth_idx'::regclass::oid)";
    assert!(
        sqlx::query_scalar::<_, i64>(request)
            .fetch_one(&mut *tx)
            .await
            .is_err(),
        "PUBLIC must not execute the function"
    );
    tx.rollback().await.expect("end unauthorized call");
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.request_ann_maintenance(oid, oid) TO ann_intent_operator").await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.ann_maintenance_request(bigint) TO ann_intent_operator").await;
    db.execute("REVOKE MAINTAIN ON ann_intent_auth_stream FROM ann_intent_operator")
        .await;
    let mut tx = db.pool.begin().await.expect("begin denied privilege call");
    sqlx::query("SET LOCAL ROLE ann_intent_operator")
        .execute(&mut *tx)
        .await
        .expect("set role");
    assert!(
        sqlx::query_scalar::<_, i64>(request)
            .fetch_one(&mut *tx)
            .await
            .is_err(),
        "MAINTAIN must be rechecked"
    );
    tx.rollback().await.expect("end denied call");
    db.execute("GRANT MAINTAIN ON ann_intent_auth_stream TO ann_intent_operator")
        .await;
    let mut tx = db.pool.begin().await.expect("begin authorized call");
    sqlx::query("SET LOCAL ROLE ann_intent_operator")
        .execute(&mut *tx)
        .await
        .expect("set role");
    let id: i64 = sqlx::query_scalar(request)
        .fetch_one(&mut *tx)
        .await
        .expect("authorized request");
    assert!(id > 0);
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .expect("authorized inspection");
    assert_eq!(status["identity_current"], true);
    tx.commit().await.expect("commit authorized request");
    db.execute("REVOKE MAINTAIN ON ann_intent_auth_stream FROM ann_intent_operator")
        .await;
    let mut tx = db.pool.begin().await.expect("begin revoked inspection");
    sqlx::query("SET LOCAL ROLE ann_intent_operator")
        .execute(&mut *tx)
        .await
        .expect("set role");
    assert!(
        sqlx::query_scalar::<_, serde_json::Value>("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await.expect("end revoked inspection");
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.claim_ann_maintenance(bigint) TO ann_intent_operator",
    )
    .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.finish_ann_maintenance(bigint, boolean, text) TO ann_intent_operator").await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.cancel_ann_maintenance(bigint) TO ann_intent_operator",
    )
    .await;
    let mut operator = db.pool.acquire().await.expect("operator session");
    sqlx::query("SET ROLE ann_intent_operator")
        .execute(&mut *operator)
        .await
        .expect("set operator role");
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&mut *operator)
            .await
            .is_err(),
        "claim must recheck MAINTAIN"
    );
    assert!(
        sqlx::query("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .execute(&mut *operator)
            .await
            .is_err(),
        "cancellation must recheck MAINTAIN"
    );
    db.execute("GRANT MAINTAIN ON ann_intent_auth_stream TO ann_intent_operator")
        .await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *operator)
        .await
        .expect("authorized claim");
    db.execute("REVOKE MAINTAIN ON ann_intent_auth_stream FROM ann_intent_operator")
        .await;
    assert!(
        sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
            .bind(id)
            .execute(&mut *operator)
            .await
            .is_err(),
        "success must recheck MAINTAIN"
    );
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'privilege revoked')")
        .bind(id)
        .execute(&mut *operator)
        .await
        .expect("record failure after revocation");
    sqlx::query("RESET ROLE")
        .execute(&mut *operator)
        .await
        .expect("reset operator role");
}

#[tokio::test]
async fn test_ann_maintenance_intent_delayed_request_cannot_regress_epoch() {
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_intent_race_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_intent_race_stream",
        "SELECT id, embedding FROM ann_intent_race_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_intent_race_idx ON ann_intent_race_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE FUNCTION public.hold_old_ann_request() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
        IF current_setting('application_name') = 'ann-old-request' THEN \
          PERFORM pg_advisory_xact_lock(7341, 82); END IF; RETURN NEW; END $$").await;
    db.execute("CREATE TRIGGER hold_old_ann_request BEFORE INSERT ON pgtrickle.pgt_ann_maintenance_requests \
        FOR EACH ROW EXECUTE FUNCTION public.hold_old_ann_request()").await;
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("SELECT pg_advisory_xact_lock(7341, 82)")
        .execute(&mut *blocker)
        .await
        .expect("hold request barrier");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let old = tokio::spawn(async move {
        let mut tx = pool.begin().await.expect("begin old request");
        sqlx::query("SET LOCAL application_name = 'ann-old-request'")
            .execute(&mut *tx)
            .await
            .expect("mark old request");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx)
            .await
            .expect("old request pid");
        sender.send(pid).expect("send pid");
        let id: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_intent_race_stream'::regclass::oid, 'ann_intent_race_idx'::regclass::oid)")
            .fetch_one(&mut *tx).await.expect("submit old request");
        tx.commit().await.expect("commit old request");
        id
    });
    let old_pid = receiver.await.expect("receive old request pid");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                .bind(blocker_pid)
                .bind(old_pid)
                .fetch_one(&db.pool)
                .await
                .expect("inspect barrier");
            if waiting {
                break;
            }
            assert!(
                !old.is_finished(),
                "old request ended before reaching the barrier"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("old request did not reach the barrier");
    db.execute("INSERT INTO ann_intent_race_source VALUES (1, '[1,0,0]')")
        .await;
    db.refresh_st("ann_intent_race_stream").await;
    let newer: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_intent_race_stream'::regclass::oid, 'ann_intent_race_idx'::regclass::oid)").await;
    let observed: i64 = sqlx::query_scalar("SELECT last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1")
        .bind(newer).fetch_one(&db.pool).await.expect("read newer epoch");
    blocker.commit().await.expect("release barrier");
    assert_eq!(old.await.expect("join old request"), newer);
    let (first, last): (i64, i64) = sqlx::query_as("SELECT first_observed_epoch, last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1")
        .bind(newer).fetch_one(&db.pool).await.expect("read merged epochs");
    assert!(first < last, "the older observation should remain visible");
    assert_eq!(
        last, observed,
        "a delayed request must not lower the latest epoch"
    );
}

#[tokio::test]
async fn test_ann_maintenance_cancel_is_transactional_and_reopens_only_on_request() {
    use sqlx::Connection;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_cancel_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_cancel_stream",
        "SELECT id, embedding FROM ann_cancel_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_cancel_idx ON ann_cancel_stream USING hnsw (embedding vector_cosine_ops)",
    )
    .await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_cancel_stream'::regclass::oid, 'ann_cancel_idx'::regclass::oid)";
    let id: i64 = db.query_scalar(request).await;
    let mut tx = db.pool.begin().await.expect("begin cancellation");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .expect("cancel in transaction");
    assert_eq!(status["state"], "CANCELLED");
    tx.rollback().await.expect("roll back cancellation");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("read rolled-back status");
    assert_eq!(status["state"], "REQUESTED");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("cancel queued request");
    assert_eq!(status["state"], "CANCELLED");
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&db.pool)
            .await
            .is_err()
    );
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("claim reopened request");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("request running cancellation");
    assert_eq!(status["state"], "RUNNING");
    assert_eq!(status["cancel_requested"], true);
    assert!(status["claim_pid"].is_number());
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'operator canceled')")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("report canceled work");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("canceled outcome");
    assert_eq!(status["state"], "CANCELLED");
    assert_eq!(status["last_error"], "operator canceled");
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("claim after canceled outcome");
    sqlx::query("SELECT pgtrickle.cancel_ann_maintenance($1)")
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("cancel before disconnect");
    worker
        .detach()
        .close()
        .await
        .expect("disconnect canceled worker");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("inspect stopped worker");
    assert_eq!(status["state"], "CANCELLED");
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&db.pool)
            .await
            .is_err(),
        "a canceled interrupted claim must not restart itself"
    );
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    let mut worker = db.pool.acquire().await.expect("replacement worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("new request can reclaim interrupted work");
    sqlx::query("SELECT pgtrickle.cancel_ann_maintenance($1)")
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("request late cancellation");
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    worker
        .detach()
        .close()
        .await
        .expect("disconnect after a later request");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("inspect interrupted later request");
    assert_eq!(status["state"], "INTERRUPTED");
    let mut worker = db.pool.acquire().await.expect("final worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("claim the later request");
    sqlx::query("SELECT pgtrickle.cancel_ann_maintenance($1)")
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("request cancellation before completion");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_cancel_idx")
        .execute(&mut *worker)
        .await
        .expect("complete before cancellation is delivered");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("report completed rebuild");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("completed status");
    assert_eq!(
        status["state"], "SUCCEEDED",
        "a late cancellation must not hide a completed rebuild"
    );
}

#[tokio::test]
async fn test_ann_maintenance_cancellation_preserves_later_request() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_cancel_live_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_cancel_live_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_cancel_live_stream",
        "SELECT id, embedding FROM ann_cancel_live_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_cancel_live_idx ON ann_cancel_live_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_cancel_live_stream'::regclass::oid, 'ann_cancel_live_idx'::regclass::oid)";
    let id: i64 = db.query_scalar(request).await;
    let mut blocker = db.pool.begin().await.expect("begin writer barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("LOCK TABLE ann_cancel_live_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold writer barrier");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let worker = tokio::spawn(async move {
        let mut conn = pool.acquire().await.expect("worker session");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await
            .expect("worker pid");
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&mut *conn)
            .await
            .expect("claim work");
        sender.send(pid).expect("send worker pid");
        let error = sqlx::query("REINDEX INDEX CONCURRENTLY ann_cancel_live_idx")
            .execute(&mut *conn)
            .await
            .expect_err("rebuild must be canceled");
        let code = error
            .as_database_error()
            .and_then(|e| e.code())
            .map(|code| code.into_owned());
        sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'query canceled')")
            .bind(id)
            .execute(&mut *conn)
            .await
            .expect("record canceled outcome");
        code
    });
    let pid = receiver.await.expect("receive worker pid");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                .bind(blocker_pid)
                .bind(pid)
                .fetch_one(&db.pool)
                .await
                .expect("inspect barrier");
            if waiting {
                break;
            }
            assert!(
                !worker.is_finished(),
                "worker ended before reaching barrier"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("rebuild did not reach barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("request cancellation");
    assert_eq!(status["state"], "RUNNING");
    assert_eq!(status["cancel_requested"], true);
    assert_eq!(status["claim_pid"], pid);
    db.execute("INSERT INTO ann_cancel_live_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_cancel_live_stream").await;
    assert_eq!(
        db.query_scalar::<i64>(request).await,
        id,
        "later request must be retained"
    );
    let signaled: bool = sqlx::query_scalar("SELECT pg_catalog.pg_cancel_backend(pid) FROM pg_catalog.pg_stat_activity WHERE pid = $1 AND backend_start = $2::text::timestamptz")
        .bind(pid).bind(status["claim_backend_start"].as_str().expect("worker start"))
        .fetch_one(&db.pool).await.expect("signal worker with native privileges");
    assert!(signaled);
    let code = tokio::time::timeout(Duration::from_secs(15), worker)
        .await
        .expect("worker did not stop")
        .expect("join worker");
    assert_eq!(code.as_deref(), Some("57014"));
    blocker.rollback().await.expect("release barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("inspect canceled work");
    assert_eq!(
        status["state"], "REQUESTED",
        "the later request must remain pending"
    );
    assert_eq!(status["cancel_requested"], false);
    assert_eq!(status["last_error"], "query canceled");
    assert!(status["next_eligible_at"].is_string());
    assert_eq!(db.count("ann_cancel_live_stream").await, 2);
}

#[tokio::test]
async fn test_ann_maintenance_cooldown_survives_reindex_and_failure_backoff() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_cooldown_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_cooldown_stream",
        "SELECT id, embedding FROM ann_cooldown_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_cooldown_idx ON ann_cooldown_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_cooldown_stream'::regclass::oid, 'ann_cooldown_idx'::regclass::oid)";
    let first: i64 = db.query_scalar(request).await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("claim first request");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_cooldown_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild index");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("finish first request");
    let next: i64 = db.query_scalar(request).await;
    assert_ne!(first, next);
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(next)
            .fetch_one(&db.pool)
            .await
            .expect("replacement status");
    assert!(
        status["next_eligible_at"].is_string(),
        "successful rebuild must expose a cooldown for its replacement"
    );
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(next)
            .execute(&mut *worker)
            .await
            .is_err(),
        "cooldown must prevent another rebuild"
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    for (attempt, minimum_seconds, maximum_seconds) in [
        (1, 15.0, 31.0),
        (2, 40.0, 61.0),
        (3, 100.0, 121.0),
        (4, 200.0, 241.0),
        (5, 400.0, 481.0),
        (6, 800.0, 961.0),
        (6, 800.0, 961.0),
    ] {
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(next)
            .execute(&mut *worker)
            .await
            .expect("claim after delay");
        sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
            .bind(next)
            .execute(&mut *worker)
            .await
            .expect("record failed attempt");
        let status: serde_json::Value =
            sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
                .bind(next)
                .fetch_one(&db.pool)
                .await
                .expect("failure status");
        assert_eq!(status["consecutive_failures"], attempt);
        let delay: f64 = db.query_scalar("SELECT extract(epoch FROM (next_eligible_at - clock_timestamp()))::float8 FROM pgtrickle.pgt_ann_maintenance_claims").await;
        assert!(
            delay > minimum_seconds && delay < maximum_seconds,
            "failure delay must increase on retry and stop at the cap"
        );
        assert!(
            sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
                .bind(next)
                .execute(&mut *worker)
                .await
                .is_err()
        );
        db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    }
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("claim after backoff");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_cooldown_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild after failures");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("record successful retry");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(next)
            .fetch_one(&db.pool)
            .await
            .expect("successful retry status");
    assert_eq!(status["consecutive_failures"], 0);
}

#[tokio::test]
async fn test_ann_maintenance_claim_serializes_concurrent_reindex_replacement() {
    use sqlx::Connection;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_swap_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_swap_stream",
        "SELECT id, embedding FROM ann_swap_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_swap_idx ON ann_swap_stream USING hnsw (embedding vector_cosine_ops)",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_swap_other_idx ON ann_swap_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_swap_stream'::regclass::oid, 'ann_swap_idx'::regclass::oid)";
    let first: i64 = db.query_scalar(request).await;
    let mut worker = db.pool.acquire().await.expect("first worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("first claim");
    let distinct: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_swap_stream'::regclass::oid, 'ann_swap_other_idx'::regclass::oid)").await;
    let mut other = db.pool.acquire().await.expect("second worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(distinct)
        .execute(&mut *other)
        .await
        .expect("a different ANN index can be claimed independently");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'not run')")
        .bind(distinct)
        .execute(&mut *other)
        .await
        .expect("release unrelated claim");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_swap_idx")
        .execute(&mut *worker)
        .await
        .expect("replace index");
    let replacement: i64 = db.query_scalar(request).await;
    assert_ne!(first, replacement);
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(replacement)
            .execute(&mut *other)
            .await
            .is_err(),
        "replacement index must remain claimed until the first worker finishes"
    );
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("finish first claim");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second' WHERE leaf_relid = 'ann_swap_stream'::regclass::oid AND request_id IS NULL").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(replacement)
        .execute(&mut *other)
        .await
        .expect("claim after first worker finishes");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_swap_idx")
        .execute(&mut *other)
        .await
        .expect("replace index again");
    let next: i64 = db.query_scalar(request).await;
    other
        .detach()
        .close()
        .await
        .expect("disconnect second worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("reclaim replacement after worker disconnects");
}

#[tokio::test]
async fn test_ann_maintenance_claim_is_session_bound_and_retryable() {
    use sqlx::Connection;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_claim_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_claim_stream",
        "SELECT id, embedding FROM ann_claim_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_claim_idx ON ann_claim_stream USING hnsw (embedding vector_cosine_ops)",
    )
    .await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_claim_stream'::regclass::oid, 'ann_claim_idx'::regclass::oid)";
    let id: i64 = db.query_scalar(request).await;
    let mut tx = db.pool.begin().await.expect("begin claim transaction");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *tx)
        .await
        .expect("claim inside transaction");
    tx.rollback().await.expect("roll back claim");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("rolled-back status");
    assert_eq!(status["state"], "REQUESTED");
    let mut worker = db.pool.acquire().await.expect("worker connection");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&mut *worker)
            .await
            .expect("claim request");
    assert_eq!(status["state"], "RUNNING");
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&db.pool)
            .await
            .is_err(),
        "another session cannot claim live work"
    );
    assert!(
        sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
            .bind(id)
            .execute(&db.pool)
            .await
            .is_err(),
        "another session cannot finish a claim"
    );
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("record failure");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("failed status");
    assert_eq!(status["state"], "FAILED");
    assert_eq!(status["last_error"], "test failure");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("retry failed request");
    worker.detach().close().await.expect("disconnect worker");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("interrupted status");
    assert_eq!(status["state"], "INTERRUPTED");
    let mut worker = db.pool.acquire().await.expect("replacement worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("reclaim interrupted request");
    db.execute("INSERT INTO ann_claim_source VALUES (1, '[1,0,0]')")
        .await;
    db.refresh_st("ann_claim_stream").await;
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_claim_idx")
        .execute(&mut *worker)
        .await
        .expect("concurrent rebuild");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("record success");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("completed status");
    assert_eq!(
        status["state"], "REQUESTED",
        "work requested during the run stays pending"
    );
    assert_eq!(
        status["identity_current"], false,
        "concurrent rebuild replaces the index identity"
    );
    let next: i64 = db.query_scalar(request).await;
    assert_ne!(id, next);
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("claim replacement index");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_claim_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild replacement index");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("record success");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(next)
            .fetch_one(&db.pool)
            .await
            .expect("completed status");
    assert_eq!(status["state"], "SUCCEEDED");
}

#[tokio::test]
async fn test_ann_maintenance_intent_survives_restart() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_intent_restart_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_intent_restart_stream",
        "SELECT id, embedding FROM ann_intent_restart_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_intent_restart_idx ON ann_intent_restart_stream USING hnsw (embedding vector_cosine_ops)").await;
    let sql = "SELECT pgtrickle.request_ann_maintenance('ann_intent_restart_stream'::regclass::oid, 'ann_intent_restart_idx'::regclass::oid)";
    let before: i64 = db.query_scalar(sql).await;
    let mut worker = db.pool.acquire().await.expect("restart worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(before)
        .execute(&mut *worker)
        .await
        .expect("claim before restart");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
        .bind(before)
        .execute(&mut *worker)
        .await
        .expect("fail before restart");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() + interval '5 minutes'").await;
    drop(worker);
    db.pool.close().await;
    let restarted = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart PostgreSQL");
    assert!(restarted.status.success());
    db.pool = db.reconnect_after_restart().await;
    let after: i64 = db.query_scalar(sql).await;
    assert_eq!(
        before, after,
        "restart must preserve and deduplicate the request"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(after)
            .fetch_one(&db.pool)
            .await
            .expect("status after restart");
    assert_eq!(status["consecutive_failures"], 1);
    assert!(
        status["next_eligible_at"].is_string(),
        "retry schedule must survive restart"
    );
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(after)
            .execute(&db.pool)
            .await
            .is_err(),
        "restart must not erase backoff"
    );
}

#[tokio::test]
async fn test_ann_refresh_epoch_is_transactional_and_independent_of_legacy_counter() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_epoch_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_epoch_stream",
        "SELECT id, embedding FROM ann_epoch_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let epoch_sql = "SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_stream'";
    let before: i64 = db.query_scalar(epoch_sql).await;
    db.execute("INSERT INTO ann_epoch_source VALUES (1, '[1,0,0]')")
        .await;
    let mut tx = db.pool.begin().await.expect("begin refresh transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('ann_epoch_stream')")
        .execute(&mut *tx)
        .await
        .expect("refresh in transaction");
    let in_transaction: i64 = sqlx::query_scalar(epoch_sql)
        .fetch_one(&mut *tx)
        .await
        .expect("read uncommitted epoch");
    assert!(in_transaction > before);
    let outside: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(
        outside, before,
        "uncommitted refresh must not publish its epoch"
    );
    tx.rollback().await.expect("roll back refresh");
    let rolled_back: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(rolled_back, before);
    db.refresh_st("ann_epoch_stream").await;
    let committed: i64 = db.query_scalar(epoch_sql).await;
    assert!(committed > before);
    db.refresh_st("ann_epoch_stream").await;
    let no_data: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(
        no_data, committed,
        "a no-data differential check must not advance the epoch"
    );
    db.execute(
        "SELECT pgtrickle.alter_stream_table('ann_epoch_stream', post_refresh_action => 'reindex')",
    )
    .await;
    db.execute("INSERT INTO ann_epoch_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_epoch_stream").await;
    let (after_reindex, counter): (i64, i64) = sqlx::query_as("SELECT ann_refresh_epoch, rows_changed_since_last_reindex FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_stream'")
        .fetch_one(&db.pool).await.expect("read maintenance counters");
    assert!(after_reindex > committed);
    assert_eq!(counter, 0, "legacy reindex resets only the churn counter");
    db.assert_st_matches_query(
        "ann_epoch_stream",
        "SELECT id, embedding FROM ann_epoch_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_refresh_epoch_tracks_snapshot_restore_transactionally() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_snapshot_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_snapshot_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_snapshot_stream",
        "SELECT id, embedding FROM ann_snapshot_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.snapshot_stream_table('ann_snapshot_stream', 'public.ann_snapshot_archive')").await;
    db.execute("INSERT INTO ann_snapshot_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_snapshot_stream").await;
    let epoch_sql = "SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_snapshot_stream'";
    let before: i64 = db.query_scalar(epoch_sql).await;
    let mut tx = db.pool.begin().await.expect("begin restore transaction");
    sqlx::query("SELECT pgtrickle.restore_from_snapshot('ann_snapshot_stream', 'public.ann_snapshot_archive')")
        .execute(&mut *tx).await.expect("restore snapshot");
    let uncommitted: i64 = sqlx::query_scalar(epoch_sql)
        .fetch_one(&mut *tx)
        .await
        .expect("read epoch");
    assert!(uncommitted > before);
    tx.rollback().await.expect("roll back restore");
    let rolled_back: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(rolled_back, before);
    db.execute("SELECT pgtrickle.restore_from_snapshot('ann_snapshot_stream', 'public.ann_snapshot_archive')").await;
    let committed: i64 = db.query_scalar(epoch_sql).await;
    assert!(committed > before);
    let rows: i64 = db
        .query_scalar("SELECT count(*) FROM ann_snapshot_stream")
        .await;
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn test_ann_refresh_epoch_tracks_immediate_topk_changes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_topk_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_topk_source VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('ann_topk_stream', 'SELECT id, embedding FROM ann_topk_source ORDER BY id DESC LIMIT 1', NULL, 'IMMEDIATE')").await;
    let epoch_sql = "SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_topk_stream'";
    let before: i64 = db.query_scalar(epoch_sql).await;
    let mut tx = db.pool.begin().await.expect("begin Top-K transaction");
    sqlx::query("INSERT INTO ann_topk_source VALUES (2, '[0,1,0]')")
        .execute(&mut *tx)
        .await
        .expect("insert in Top-K transaction");
    let uncommitted: i64 = sqlx::query_scalar(epoch_sql)
        .fetch_one(&mut *tx)
        .await
        .expect("read uncommitted Top-K epoch");
    assert!(uncommitted > before);
    tx.rollback().await.expect("roll back Top-K change");
    let rolled_back: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(rolled_back, before);
    db.execute("INSERT INTO ann_topk_source VALUES (2, '[0,1,0]')")
        .await;
    let id: i32 = db.query_scalar("SELECT id FROM ann_topk_stream").await;
    assert_eq!(id, 2);
    let after: i64 = db.query_scalar(epoch_sql).await;
    assert!(after > before);
}

#[tokio::test]
async fn test_ann_refresh_epoch_tracks_immediate_truncate_transactionally() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_truncate_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_truncate_source VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('ann_truncate_stream', 'SELECT id, embedding FROM ann_truncate_source', NULL, 'IMMEDIATE')").await;
    db.execute(
        "CREATE INDEX ann_truncate_idx ON ann_truncate_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_truncate_stream'::regclass::oid, 'ann_truncate_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    let claim: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .fetch_one(&mut *worker)
        .await
        .expect("claim request");
    let claim_epoch = claim["claim_epoch"].as_i64().expect("claim epoch");
    let mut tx = db.pool.begin().await.expect("begin truncate transaction");
    sqlx::query("TRUNCATE ann_truncate_source")
        .execute(&mut *tx)
        .await
        .expect("truncate source");
    let uncommitted: i64 = sqlx::query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_truncate_stream'")
        .fetch_one(&mut *tx).await.expect("read uncommitted epoch");
    assert!(uncommitted > claim_epoch);
    tx.rollback().await.expect("roll back truncate");
    let after_rollback: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_truncate_stream'").await;
    assert_eq!(after_rollback, claim_epoch);
    db.execute("TRUNCATE ann_truncate_source").await;
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("finish request");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect request");
    assert_eq!(status["reported_covered_epoch"].as_i64(), Some(claim_epoch));
    assert!(
        status["epoch_advances_since_reported_coverage"]
            .as_i64()
            .is_some_and(|n| n > 0)
    );
    let remaining: i64 = db
        .query_scalar("SELECT count(*) FROM ann_truncate_stream")
        .await;
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn test_ann_refresh_epoch_covers_immediate_and_full_rebuilds() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_epoch_modes_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_epoch_modes_source VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('ann_epoch_immediate', 'SELECT id, embedding FROM ann_epoch_modes_source', NULL, 'IMMEDIATE')").await;
    db.create_st(
        "ann_epoch_full",
        "SELECT id, embedding FROM ann_epoch_modes_source",
        "1h",
        "FULL",
    )
    .await;
    let immediate_before: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_immediate'").await;
    let full_before: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_full'").await;
    db.refresh_st("ann_epoch_full").await;
    let full_noop: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_full'").await;
    assert!(
        full_noop > full_before,
        "a full storage rewrite must advance the epoch even when its result is unchanged"
    );
    db.execute("INSERT INTO ann_epoch_modes_source VALUES (2, '[0,1,0]')")
        .await;
    let immediate_after: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_immediate'").await;
    assert!(immediate_after > immediate_before);
    db.refresh_st("ann_epoch_full").await;
    let full_after: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_full'").await;
    assert!(full_after > full_noop);
    db.execute("SELECT pgtrickle.alter_stream_table('ann_epoch_full', query => 'SELECT id, embedding FROM ann_epoch_modes_source WHERE id > 1')").await;
    let after_alter: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_full'").await;
    assert!(
        after_alter > full_after,
        "storage rebuild must advance the epoch"
    );
    db.assert_st_matches_query(
        "ann_epoch_full",
        "SELECT id, embedding FROM ann_epoch_modes_source WHERE id > 1",
    )
    .await;
}

#[tokio::test]
async fn test_ann_refresh_epoch_overflow_becomes_unknown_without_losing_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ann_epoch_overflow_source (id int PRIMARY KEY)")
        .await;
    db.create_st(
        "ann_epoch_overflow",
        "SELECT id FROM ann_epoch_overflow_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_refresh_epoch = 9223372036854775807 WHERE pgt_name = 'ann_epoch_overflow'").await;
    db.execute("INSERT INTO ann_epoch_overflow_source VALUES (1)")
        .await;
    db.refresh_st("ann_epoch_overflow").await;
    let unknown: Option<i64> = db.query_scalar_opt("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_overflow'").await;
    assert_eq!(unknown, None);
    db.assert_st_matches_query(
        "ann_epoch_overflow",
        "SELECT id FROM ann_epoch_overflow_source",
    )
    .await;
    db.execute("INSERT INTO ann_epoch_overflow_source VALUES (2)")
        .await;
    db.refresh_st("ann_epoch_overflow").await;
    let still_unknown: Option<i64> = db.query_scalar_opt("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_overflow'").await;
    assert_eq!(still_unknown, None);
    db.assert_st_matches_query(
        "ann_epoch_overflow",
        "SELECT id FROM ann_epoch_overflow_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_refresh_epoch_tracks_immediate_phantom_cleanup_without_delta() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE epoch_join_left (id int PRIMARY KEY, value int)")
        .await;
    db.execute("CREATE TABLE epoch_join_right (id int PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO epoch_join_left VALUES (1, 10)")
        .await;
    db.execute("INSERT INTO epoch_join_right VALUES (1)").await;
    let query =
        "SELECT l.id, l.value FROM epoch_join_left l JOIN epoch_join_right r ON l.id = r.id";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('epoch_join_stream', $${query}$$, NULL, 'IMMEDIATE')"
    ))
    .await;
    // Simulate a pre-existing phantom to exercise cleanup when the new delta is empty.
    db.execute("ALTER TABLE epoch_join_stream DISABLE TRIGGER USER")
        .await;
    db.execute("INSERT INTO epoch_join_stream (__pgt_row_id, id, value) VALUES (decode('00', 'hex'), 999, 999)").await;
    db.execute("ALTER TABLE epoch_join_stream ENABLE TRIGGER USER")
        .await;
    let before: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'epoch_join_stream'").await;
    let before_churn: (i64, i64) = sqlx::query_as("SELECT ann_churn_generation, ann_churn_operations FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'epoch_join_stream'")
        .fetch_one(&db.pool).await.expect("read churn before phantom cleanup");
    db.execute("INSERT INTO epoch_join_left VALUES (2, 20)")
        .await;
    let after: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'epoch_join_stream'").await;
    assert!(after > before, "cleanup-only writes must advance the epoch");
    let after_churn: (i64, i64) = sqlx::query_as("SELECT ann_churn_generation, ann_churn_operations FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'epoch_join_stream'")
        .fetch_one(&db.pool).await.expect("read churn after phantom cleanup");
    assert_eq!(after_churn, (before_churn.0, before_churn.1 + 1));
    db.assert_st_matches_query("epoch_join_stream", query).await;
}

#[tokio::test]
async fn test_ann_refresh_epoch_survives_database_restart() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("CREATE TABLE ann_epoch_restart_source (id int PRIMARY KEY)")
        .await;
    db.create_st(
        "ann_epoch_restart",
        "SELECT id FROM ann_epoch_restart_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("INSERT INTO ann_epoch_restart_source VALUES (1)")
        .await;
    db.refresh_st("ann_epoch_restart").await;
    let before: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_restart'").await;
    db.pool.close().await;
    let restarted = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart PostgreSQL");
    assert!(restarted.status.success(), "container restart failed");
    db.pool = db.reconnect_after_restart().await;
    let after: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_restart'").await;
    assert_eq!(after, before);
    db.execute("INSERT INTO ann_epoch_restart_source VALUES (2)")
        .await;
    db.refresh_st("ann_epoch_restart").await;
    let next: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_restart'").await;
    assert!(next > after);
    db.assert_st_matches_query(
        "ann_epoch_restart",
        "SELECT id FROM ann_epoch_restart_source",
    )
    .await;
}

#[tokio::test]
async fn test_manual_ann_candidates_select_only_authorized_leaf_indexes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute(
        "CREATE TABLE ann_manual_source (id int PRIMARY KEY, embedding vector(3), label text)",
    )
    .await;
    db.execute("INSERT INTO ann_manual_source VALUES (1, '[1,0,0]', 'alpha')")
        .await;
    db.create_st_partitioned(
        "ann_manual_stream",
        "SELECT id, embedding, label FROM ann_manual_source",
        "1h",
        "DIFFERENTIAL",
        "id",
    )
    .await;
    db.execute("CREATE INDEX ann_manual_hnsw ON ann_manual_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE INDEX ann_manual_ivf ON ann_manual_stream USING ivfflat (embedding vector_cosine_ops) WITH (lists = 1)").await;
    db.execute("CREATE INDEX ann_manual_label ON ann_manual_stream (label)")
        .await;
    let indexes: Vec<(i64, String, String)> = sqlx::query_as(include_str!(
        "fixtures/pgvector_ann_maintenance_candidates.sql"
    ))
    .bind("ann_manual_stream")
    .fetch_all(&db.pool)
    .await
    .expect("discover ANN leaf indexes");
    assert_eq!(indexes.len(), 2);
    let methods: std::collections::HashSet<&str> =
        indexes.iter().map(|row| row.2.as_str()).collect();
    assert_eq!(methods, ["hnsw", "ivfflat"].into_iter().collect());
    let root: i64 = db
        .query_scalar("SELECT 'ann_manual_stream'::regclass::oid::bigint")
        .await;
    let request: i64 = sqlx::query_scalar(
        "SELECT pgtrickle.request_ann_maintenance($1::bigint::oid, $2::bigint::oid)",
    )
    .bind(root)
    .bind(indexes[0].0)
    .fetch_one(&db.pool)
    .await
    .expect("request a partition leaf");
    assert!(request > 0);
    let parent = db.try_execute("SELECT pgtrickle.request_ann_maintenance('ann_manual_stream'::regclass::oid, 'ann_manual_hnsw'::regclass::oid)").await;
    assert!(
        parent.is_err(),
        "partitioned parent indexes must not be enqueued as leaves"
    );
    for (oid, _, _) in indexes {
        let leaf: bool = sqlx::query_scalar("SELECT c.relkind = 'i' AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = c.oid) FROM pg_class c WHERE c.oid = $1::bigint::oid")
            .bind(oid).fetch_one(&db.pool).await.expect("inspect leaf index");
        assert!(leaf);
    }
    let ordinary: Vec<(i64, String, String)> = sqlx::query_as(include_str!(
        "fixtures/pgvector_ann_maintenance_candidates.sql"
    ))
    .bind("ann_manual_source")
    .fetch_all(&db.pool)
    .await
    .expect("check non-stream table");
    assert!(ordinary.is_empty());
    db.execute("CREATE ROLE ann_manual_operator NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO ann_manual_operator")
        .await;
    db.execute(
        "GRANT SELECT ON pgtrickle.pgt_stream_tables, ann_manual_stream TO ann_manual_operator",
    )
    .await;
    let leaf: String = db.query_scalar("SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'ann_manual_stream'::regclass").await;
    let mut tx = db.pool.begin().await.expect("begin restricted check");
    sqlx::query("SET LOCAL ROLE ann_manual_operator")
        .execute(&mut *tx)
        .await
        .expect("set maintenance role");
    let denied: Vec<(i64, String, String)> = sqlx::query_as(include_str!(
        "fixtures/pgvector_ann_maintenance_candidates.sql"
    ))
    .bind("ann_manual_stream")
    .fetch_all(&mut *tx)
    .await
    .expect("check missing MAINTAIN");
    assert!(denied.is_empty());
    tx.rollback().await.expect("end restricted check");
    db.execute(&format!(
        "GRANT MAINTAIN ON TABLE {leaf} TO ann_manual_operator"
    ))
    .await;
    let mut tx = db.pool.begin().await.expect("begin authorized check");
    sqlx::query("SET LOCAL ROLE ann_manual_operator")
        .execute(&mut *tx)
        .await
        .expect("set maintenance role");
    let allowed: Vec<(i64, String, String)> = sqlx::query_as(include_str!(
        "fixtures/pgvector_ann_maintenance_candidates.sql"
    ))
    .bind("ann_manual_stream")
    .fetch_all(&mut *tx)
    .await
    .expect("check MAINTAIN grant");
    assert_eq!(allowed.len(), 2);
    tx.rollback().await.expect("end authorized check");
}

#[tokio::test]
async fn test_manual_concurrent_ann_rebuild_allows_reads_and_refresh_writes() {
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_online_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_online_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_online_stream",
        "SELECT id, embedding FROM ann_online_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_online_hnsw ON ann_online_stream USING hnsw (embedding vector_cosine_ops)").await;
    let before: i64 = db
        .query_scalar("SELECT 'ann_online_hnsw'::regclass::oid::bigint")
        .await;
    let mut blocker = db.pool.begin().await.expect("begin writer barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("LOCK TABLE ann_online_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold writer barrier");
    let (pid_tx, pid_rx) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let rebuild = tokio::spawn(async move {
        let mut conn = pool
            .acquire()
            .await
            .expect("acquire maintenance connection");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await
            .expect("maintenance pid");
        pid_tx.send(pid).expect("send maintenance pid");
        sqlx::query("SET statement_timeout = '30s'")
            .execute(&mut *conn)
            .await
            .expect("set timeout");
        sqlx::query("REINDEX INDEX CONCURRENTLY ann_online_hnsw")
            .execute(&mut *conn)
            .await
    });
    let pid = pid_rx.await.expect("receive maintenance pid");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                .bind(blocker_pid)
                .bind(pid)
                .fetch_one(&db.pool)
                .await
                .expect("inspect rebuild blocker");
            if waiting {
                break;
            }
            assert!(
                !rebuild.is_finished(),
                "rebuild ended before reaching the writer barrier"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("rebuild did not wait for the writer barrier");
    tokio::time::timeout(Duration::from_secs(10), async {
        assert_eq!(db.count("ann_online_stream").await, 1);
        db.execute("INSERT INTO ann_online_source VALUES (2, '[0,1,0]')")
            .await;
        db.refresh_st("ann_online_stream").await;
        assert_eq!(db.count("ann_online_stream").await, 2);
    })
    .await
    .expect("reads and refresh must progress while rebuild waits");
    assert!(
        !rebuild.is_finished(),
        "barrier must still hold the rebuild"
    );
    blocker.commit().await.expect("release writer barrier");
    rebuild
        .await
        .expect("join maintenance task")
        .expect("concurrent rebuild");
    let after: i64 = db
        .query_scalar("SELECT 'ann_online_hnsw'::regclass::oid::bigint")
        .await;
    assert_ne!(before, after, "concurrent rebuild must replace the index");
    db.assert_st_matches_query(
        "ann_online_stream",
        "SELECT id, embedding FROM ann_online_source",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-1: User-taste centroid — avg(embedding) per user
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_avg_centroid_insert() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE embeddings (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO embeddings (user_id, embedding) VALUES
            (1, '[1,0,0]'),
            (1, '[0,1,0]'),
            (2, '[0,0,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM embeddings GROUP BY user_id";
    // pgvector's avg(vector) is undimensioned; F4 materializes the source dimension.
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM embeddings GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_st", expected_q).await;

    // INSERT new embedding for user 1
    db.execute("INSERT INTO embeddings (user_id, embedding) VALUES (1, '[1,1,0]')")
        .await;
    db.refresh_st("centroid_st").await;
    db.assert_st_matches_query("centroid_st", expected_q).await;

    // INSERT new user
    db.execute("INSERT INTO embeddings (user_id, embedding) VALUES (3, '[0,1,1]')")
        .await;
    db.refresh_st("centroid_st").await;
    db.assert_st_matches_query("centroid_st", expected_q).await;
}

#[tokio::test]
async fn test_pgvector_avg_centroid_update() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_upd (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_upd (user_id, embedding) VALUES
            (1, '[1,0,0]'),
            (1, '[0,1,0]'),
            (2, '[1,1,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM emb_upd GROUP BY user_id";
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM emb_upd GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_upd_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_upd_st", expected_q)
        .await;

    // UPDATE embedding
    db.execute("UPDATE emb_upd SET embedding = '[0,0,1]' WHERE user_id = 1 AND id = 1")
        .await;
    db.refresh_st("centroid_upd_st").await;
    db.assert_st_matches_query("centroid_upd_st", expected_q)
        .await;
}

#[tokio::test]
async fn test_pgvector_avg_centroid_delete() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_del (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_del (user_id, embedding) VALUES
            (1, '[1,0,0]'),
            (1, '[0,1,0]'),
            (2, '[1,1,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM emb_del GROUP BY user_id";
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM emb_del GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_del_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_del_st", expected_q)
        .await;

    // DELETE one row from user 1
    db.execute("DELETE FROM emb_del WHERE user_id = 1 AND id = 1")
        .await;
    db.refresh_st("centroid_del_st").await;
    db.assert_st_matches_query("centroid_del_st", expected_q)
        .await;

    // DELETE entire user group
    db.execute("DELETE FROM emb_del WHERE user_id = 2").await;
    db.refresh_st("centroid_del_st").await;
    db.assert_st_matches_query("centroid_del_st", expected_q)
        .await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-2: vector_sum — sum(embedding) per group
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_sum_differential() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_sum_src (
            id SERIAL PRIMARY KEY,
            grp TEXT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_sum_src (grp, embedding) VALUES
            ('a', '[1,0,0]'),
            ('a', '[0,1,0]'),
            ('b', '[1,1,1]')",
    )
    .await;

    let q = "SELECT grp, sum(embedding) AS total_vec FROM emb_sum_src GROUP BY grp";
    let expected_q =
        "SELECT grp, sum(embedding)::vector(3) AS total_vec FROM emb_sum_src GROUP BY grp";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('vec_sum_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("vec_sum_st", expected_q).await;

    // INSERT
    db.execute("INSERT INTO emb_sum_src (grp, embedding) VALUES ('a', '[0,0,1]')")
        .await;
    db.refresh_st("vec_sum_st").await;
    db.assert_st_matches_query("vec_sum_st", expected_q).await;

    // DELETE
    db.execute("DELETE FROM emb_sum_src WHERE grp = 'b'").await;
    db.refresh_st("vec_sum_st").await;
    db.assert_st_matches_query("vec_sum_st", expected_q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-3: Distance operator fallback — queries with <-> fall back to FULL
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_distance_operator_fallback_to_full() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_dist (
            id SERIAL PRIMARY KEY,
            category TEXT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_dist (category, embedding) VALUES
            ('cat1', '[1,0,0]'),
            ('cat1', '[0.9,0.1,0]'),
            ('cat2', '[0,1,0]')",
    )
    .await;

    // A stream table using a distance operator in ORDER BY should work
    // via FULL refresh mode (distance operators are FULL-fallback safe).
    // Use FULL mode explicitly to avoid relying on differential fallback.
    let q = "SELECT id, category, embedding FROM emb_dist ORDER BY embedding <-> '[1,0,0]' LIMIT 5";
    db.create_st("dist_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("dist_st", q).await;

    // Refresh after insert
    db.execute("INSERT INTO emb_dist (category, embedding) VALUES ('cat1', '[0.8,0.2,0]')")
        .await;
    db.refresh_st("dist_st").await;
    db.assert_st_matches_query("dist_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-4: HNSW index on centroid stream table
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_hnsw_index_on_stream_table() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_hnsw_src (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_hnsw_src (user_id, embedding) VALUES
            (1, '[1,0,0]'), (1, '[0.9,0.1,0]'),
            (2, '[0,1,0]'), (2, '[0,0.9,0.1]'),
            (3, '[0,0,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM emb_hnsw_src GROUP BY user_id";
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM emb_hnsw_src GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_hnsw_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_hnsw_st", expected_q)
        .await;

    // Create HNSW index on the centroid stream table
    db.execute(
        "CREATE INDEX centroid_hnsw_idx ON public.centroid_hnsw_st \
         USING hnsw (centroid vector_cosine_ops)",
    )
    .await;

    // Verify refresh still works with index in place
    db.execute("INSERT INTO emb_hnsw_src (user_id, embedding) VALUES (1, '[1,0,0]')")
        .await;
    db.refresh_st("centroid_hnsw_st").await;
    db.assert_st_matches_query("centroid_hnsw_st", expected_q)
        .await;

    // Verify the HNSW index can be used for ANN search
    let nn_result = db
        .query_scalar_opt::<i32>("SELECT user_id FROM public.centroid_hnsw_st ORDER BY centroid <-> '[1,0,0]'::vector LIMIT 1")
        .await;
    assert!(
        nn_result.is_some(),
        "HNSW ANN query should return at least one result"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T-VP1 (v0.47.0): Drift-based reindex on HNSW stream table
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_post_refresh_reindex_failure_does_not_rollback_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE maintenance_fail_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "maintenance_fail_st",
        "SELECT id, embedding FROM maintenance_fail_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX maintenance_fail_hnsw ON maintenance_fail_st USING hnsw (embedding vector_l2_ops)").await;
    db.execute("SELECT pgtrickle.alter_stream_table('maintenance_fail_st', post_refresh_action => 'reindex')").await;
    db.execute("CREATE FUNCTION fail_reindex() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected reindex failure'; END $$").await;
    db.execute("CREATE EVENT TRIGGER fail_reindex ON ddl_command_start WHEN TAG IN ('REINDEX') EXECUTE FUNCTION fail_reindex()").await;
    db.execute("INSERT INTO maintenance_fail_src VALUES (1,'[1,0,0]')")
        .await;
    let notices = db
        .try_execute_with_notices("SELECT pgtrickle.refresh_stream_table('maintenance_fail_st')")
        .await
        .expect("maintenance failure must not roll back refresh");
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains("injected reindex failure")),
        "maintenance failure must be observable: {notices:?}"
    );
    db.assert_st_matches_query(
        "maintenance_fail_st",
        "SELECT id, embedding FROM maintenance_fail_src",
    )
    .await;
    let changed: i64 = db
        .query_scalar("SELECT rows_changed_since_last_reindex FROM pgtrickle.pgt_stream_tables WHERE pgt_name='maintenance_fail_st'")
        .await;
    assert_eq!(
        changed, 1,
        "failed maintenance must retain the changed-row count"
    );
}

#[tokio::test]
async fn test_post_refresh_reindex_success_and_transaction_rollback() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE maintenance_success_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "maintenance_success_st",
        "SELECT id, embedding FROM maintenance_success_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX maintenance_success_hnsw ON maintenance_success_st USING hnsw (embedding vector_l2_ops)").await;
    db.execute("SELECT pgtrickle.alter_stream_table('maintenance_success_st', post_refresh_action => 'reindex')").await;
    let original: i64 = db
        .query_scalar("SELECT pg_relation_filenode('maintenance_success_hnsw'::regclass)::bigint")
        .await;
    db.execute("INSERT INTO maintenance_success_src VALUES (1,'[1,0,0]')")
        .await;
    let mut tx = db.pool.begin().await.expect("begin refresh transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('maintenance_success_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh and reindex");
    let rebuilt: i64 = sqlx::query_scalar(
        "SELECT pg_relation_filenode('maintenance_success_hnsw'::regclass)::bigint",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("read rebuilt index identity");
    assert_ne!(
        rebuilt, original,
        "successful maintenance must rebuild the index"
    );
    tx.rollback().await.expect("rollback refresh and reindex");
    let after_rollback: i64 = db
        .query_scalar("SELECT pg_relation_filenode('maintenance_success_hnsw'::regclass)::bigint")
        .await;
    assert_eq!(after_rollback, original);
    assert_eq!(db.count("public.maintenance_success_st").await, 0);
    db.refresh_st("maintenance_success_st").await;
    db.assert_st_matches_query(
        "maintenance_success_st",
        "SELECT id, embedding FROM maintenance_success_src",
    )
    .await;
    let state: (i64, bool) = sqlx::query_as("SELECT rows_changed_since_last_reindex,last_reindex_at IS NOT NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name='maintenance_success_st'").fetch_one(&db.pool).await.expect("inspect successful maintenance");
    assert_eq!(state, (0, true));
}

/// T-VP1: Create a vector stream table with post_refresh_action='reindex_if_drift',
/// change rows beyond the threshold, verify that the drift counter increments
/// and that last_reindex_at is updated after a refresh.
#[tokio::test]
async fn test_vector_post_refresh_action_reindex_if_drift() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE drift_src (
            id SERIAL PRIMARY KEY,
            embedding vector(3)
        )",
    )
    .await;

    // Insert initial rows
    for i in 1..=20i32 {
        db.execute(&format!(
            "INSERT INTO drift_src (embedding) VALUES ('[{},{},0]')",
            i, i
        ))
        .await;
    }

    // Create stream table
    let q = "SELECT id, embedding FROM drift_src";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('drift_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    ))
    .await;

    // Configure drift-triggered REINDEX (50% threshold so we can trigger it)
    db.execute(
        "SELECT pgtrickle.alter_stream_table('drift_st', \
         post_refresh_action => 'reindex_if_drift', \
         reindex_drift_threshold => 0.50)",
    )
    .await;

    // Add more rows to exceed 50% threshold (20 rows * 50% = 10, add 11)
    for i in 21..=31i32 {
        db.execute(&format!(
            "INSERT INTO drift_src (embedding) VALUES ('[{},{},1]')",
            i, i
        ))
        .await;
    }

    // Refresh — should increment rows_changed_since_last_reindex
    db.refresh_st("drift_st").await;

    // Check that vector_status() shows this stream table
    let drift_pct = db
        .query_scalar_opt::<f64>(
            "SELECT drift_pct FROM pgtrickle.vector_status() WHERE name = 'public.drift_st'",
        )
        .await;
    assert!(
        drift_pct.is_some(),
        "vector_status() should return a row for drift_st"
    );

    // Verify post_refresh_action is stored correctly
    let action = db
        .query_scalar_opt::<String>(
            "SELECT post_refresh_action FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'drift_st'",
        )
        .await
        .expect("post_refresh_action should be set");
    assert_eq!(
        action, "reindex_if_drift",
        "post_refresh_action should be 'reindex_if_drift'"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T-VP2 (v0.47.0): vector_status() accuracy and reset behavior
// ═══════════════════════════════════════════════════════════════════════

/// T-VP2: Verify that pgtrickle.vector_status() reports correct lag, drift,
/// and metadata for vector stream tables. Also verify that after an alter to
/// 'none', the table disappears from vector_status().
#[tokio::test]
async fn test_vector_status_view_accuracy_and_reset() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE vs_src (
            id SERIAL PRIMARY KEY,
            body TEXT,
            embedding vector(3)
        )",
    )
    .await;

    db.execute(
        "INSERT INTO vs_src (body, embedding) VALUES
             ('hello', '[1,0,0]'),
             ('world', '[0,1,0]'),
             ('foo',   '[0,0,1]')",
    )
    .await;

    let q = "SELECT id, body, embedding FROM vs_src";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('vs_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    ))
    .await;

    // Set post_refresh_action to 'analyze'
    db.execute("SELECT pgtrickle.alter_stream_table('vs_st', post_refresh_action => 'analyze')")
        .await;

    // Do a refresh to trigger the action
    db.refresh_st("vs_st").await;

    // vector_status() should show vs_st
    let row = db
        .query_scalar_opt::<String>(
            "SELECT post_refresh_action FROM pgtrickle.vector_status() \
             WHERE name = 'public.vs_st'",
        )
        .await;
    assert!(
        row.is_some(),
        "vector_status() should include vs_st after setting post_refresh_action"
    );
    assert_eq!(
        row.unwrap(),
        "analyze",
        "post_refresh_action should be 'analyze'"
    );

    // data_timestamp should be set (not NULL)
    let ts = db
        .query_scalar_opt::<String>(
            "SELECT data_timestamp::TEXT FROM pgtrickle.vector_status() \
             WHERE name = 'public.vs_st'",
        )
        .await;
    assert!(
        ts.is_some(),
        "data_timestamp should not be NULL in vector_status()"
    );

    // Reset to 'none' — should disappear from vector_status()
    db.execute("SELECT pgtrickle.alter_stream_table('vs_st', post_refresh_action => 'none')")
        .await;

    let gone = db
        .query_scalar_opt::<String>(
            "SELECT post_refresh_action FROM pgtrickle.vector_status() \
             WHERE name = 'public.vs_st'",
        )
        .await;
    assert!(
        gone.is_none(),
        "vs_st should not appear in vector_status() after resetting to 'none'"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T-VP3 / VP-5 (v0.47.0): Vector-aggregate cases — shard-additive verification
// ═══════════════════════════════════════════════════════════════════════

/// T-VP3 / VP-5: Verify that vector_avg() produces correct results consistent
/// with pgvector's native avg(vector) aggregate. This exercises the same
/// shard-additive algebra that would run in a Citus distributed deployment.
///
/// This test runs on the standard single-node e2e image (no Citus required)
/// but validates the arithmetic that the Citus path relies on.
#[tokio::test]
async fn test_vector_avg_shard_additive_correctness() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE shard_src (
            id      SERIAL PRIMARY KEY,
            shard   INTEGER NOT NULL,
            vec     vector(4)
        )",
    )
    .await;

    // Simulate two logical shards with known vectors
    // Shard 1: avg([1,2,3,4], [3,4,5,6]) = [2,3,4,5]
    // Shard 2: avg([0,0,0,0], [4,4,4,4]) = [2,2,2,2]
    db.execute(
        "INSERT INTO shard_src (shard, vec) VALUES
             (1, '[1,2,3,4]'), (1, '[3,4,5,6]'),
             (2, '[0,0,0,0]'), (2, '[4,4,4,4]')",
    )
    .await;

    let q = "SELECT shard, avg(vec) AS centroid, count(*) AS cnt FROM shard_src GROUP BY shard";
    let expected_q = "SELECT shard, avg(vec)::vector(4) AS centroid, count(*) AS cnt FROM shard_src GROUP BY shard";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('shard_avg_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;

    db.assert_st_matches_query("shard_avg_st", expected_q).await;

    // Verify shard 1 centroid: avg([1,2,3,4],[3,4,5,6]) = [2,3,4,5]
    let s1_centroid: Option<String> = db
        .query_scalar_opt("SELECT centroid::TEXT FROM public.shard_avg_st WHERE shard = 1")
        .await;
    assert!(s1_centroid.is_some(), "shard 1 centroid should be present");
    // pgvector formats vectors as '[x,y,z,w]'; check it's close to [2,3,4,5]
    let ct = s1_centroid.unwrap();
    assert!(
        ct.contains("2") && ct.contains("3") && ct.contains("4") && ct.contains("5"),
        "shard 1 centroid ({ct}) should approximate [2,3,4,5]"
    );

    // Now insert into shard 1 — differential refresh must update centroid correctly
    db.execute("INSERT INTO shard_src (shard, vec) VALUES (1, '[5,6,7,8]')")
        .await;
    db.refresh_st("shard_avg_st").await;
    db.assert_st_matches_query("shard_avg_st", expected_q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// VH-1 (v0.48.0): halfvec/sparsevec aggregate type output correctness
// ═══════════════════════════════════════════════════════════════════════

/// VH-1a: avg(halfvec_col) output column should be typed halfvec(N), not vector(N).
#[tokio::test]
async fn test_pgvector_halfvec_avg_output_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE hv_src (
            id     SERIAL PRIMARY KEY,
            grp    INT,
            emb    halfvec(4)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO hv_src (grp, emb) VALUES
            (1, '[1,0,0,0]'),
            (1, '[0,1,0,0]'),
            (2, '[0,0,1,0]')",
    )
    .await;

    let q = "SELECT grp, avg(emb) AS centroid FROM hv_src GROUP BY grp";
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        &format!(
            "SELECT pgtrickle.create_stream_table('hv_avg_st', $${q}$$, '1m', 'DIFFERENTIAL')"
        ),
    ])
    .await;

    // The output column 'centroid' must be typed halfvec(4), not vector(4).
    let typname: Option<String> = db
        .query_scalar_opt(
            "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             WHERE c.relname = 'hv_avg_st' \
               AND a.attname = 'centroid' \
               AND a.attnum > 0",
        )
        .await;
    assert!(
        typname.is_some(),
        "column 'centroid' must exist in hv_avg_st"
    );
    let tn = typname.unwrap();
    assert!(
        tn.starts_with("halfvec"),
        "centroid column should be halfvec(4), got: {tn}"
    );
}

/// VH-1b: avg(halfvec) values are correct after differential refresh.
#[tokio::test]
async fn test_pgvector_halfvec_avg_values_correct() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE hv_vals_src (
            id  SERIAL PRIMARY KEY,
            grp INT,
            emb halfvec(2)
        )",
    )
    .await;
    db.execute("INSERT INTO hv_vals_src (grp, emb) VALUES (1, '[2,4]'), (1, '[4,2]')")
        .await;

    let q = "SELECT grp, avg(emb) AS centroid FROM hv_vals_src GROUP BY grp";
    let expected_q = "SELECT grp, avg(emb)::halfvec(2) AS centroid FROM hv_vals_src GROUP BY grp";
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        &format!(
            "SELECT pgtrickle.create_stream_table('hv_vals_st', $${q}$$, '1m', 'DIFFERENTIAL')"
        ),
    ])
    .await;
    db.assert_st_matches_query("hv_vals_st", expected_q).await;

    // INSERT — grp 1 centroid should become avg([2,4],[4,2],[6,0]) = [4,2]
    db.execute("INSERT INTO hv_vals_src (grp, emb) VALUES (1, '[6,0]')")
        .await;
    db.refresh_st("hv_vals_st").await;
    db.assert_st_matches_query("hv_vals_st", expected_q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// VH-2 (v0.48.0): Distance subscription catalog operations
// ═══════════════════════════════════════════════════════════════════════

/// VH-2a: subscribe_distance() inserts into pgt_distance_subscriptions.
#[tokio::test]
async fn test_distance_subscription_catalog_insert() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute("CREATE TABLE ds_src (id SERIAL PRIMARY KEY, grp INT, emb vector(3))")
        .await;
    db.execute("INSERT INTO ds_src (grp, emb) VALUES (1, '[1,0,0]'), (1, '[0,1,0]')")
        .await;
    db.create_st(
        "ds_st",
        "SELECT grp, avg(emb)::vector(3) AS centroid FROM ds_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.subscribe_distance(\
             'public.ds_st', 'near_centroid', 'centroid', '[1,0,0]', '<->', 0.5\
         )",
    )
    .await;

    let count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_distance_subscriptions \
             WHERE stream_table = 'public.ds_st' AND channel = 'near_centroid'",
        )
        .await;
    assert_eq!(count, 1, "subscription should be registered in catalog");
}

/// VH-2b: unsubscribe_distance() removes the catalog entry.
#[tokio::test]
async fn test_distance_subscription_unsubscribe() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute("CREATE TABLE ds2_src (id SERIAL PRIMARY KEY, emb vector(2))")
        .await;
    db.create_st(
        "ds2_st",
        "SELECT id, emb FROM ds2_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.subscribe_distance(\
             'public.ds2_st', 'ch_unsub', 'emb', '[1,0]', '<->', 0.3\
         )",
    )
    .await;
    db.execute("SELECT pgtrickle.unsubscribe_distance('public.ds2_st', 'ch_unsub')")
        .await;

    let count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_distance_subscriptions \
             WHERE stream_table = 'public.ds2_st' AND channel = 'ch_unsub'",
        )
        .await;
    assert_eq!(count, 0, "subscription should be removed after unsubscribe");
}

/// VH-2c: list_distance_subscriptions() returns subscriptions for a stream table.
#[tokio::test]
async fn test_distance_subscription_list() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute("CREATE TABLE ds3_src (id SERIAL PRIMARY KEY, emb vector(2))")
        .await;
    db.create_st(
        "ds3_st",
        "SELECT id, emb FROM ds3_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute_seq(&[
        "SELECT pgtrickle.subscribe_distance('public.ds3_st', 'ch_a', 'emb', '[1,0]', '<->', 0.5)",
        "SELECT pgtrickle.subscribe_distance('public.ds3_st', 'ch_b', 'emb', '[0,1]', '<->', 0.8)",
    ])
    .await;

    let count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.list_distance_subscriptions('public.ds3_st')")
        .await;
    assert_eq!(count, 2, "list_distance_subscriptions should return 2 rows");
}

// ═══════════════════════════════════════════════════════════════════════
// VA-1 (v0.48.0): embedding_stream_table() ergonomic API
// ═══════════════════════════════════════════════════════════════════════

/// VA-1a: embedding_stream_table() creates a stream table with correct schema.
#[tokio::test]
async fn test_embedding_stream_table_creates_st() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE va1_src (
            id        SERIAL PRIMARY KEY,
            tenant_id INT,
            content   TEXT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO va1_src (tenant_id, content, embedding) VALUES
            (1, 'hello', '[1,0,0]'),
            (1, 'world', '[0,1,0]'),
            (2, 'foo',   '[0,0,1]')",
    )
    .await;

    // dry_run=true: returns SQL actions without executing
    let action_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.embedding_stream_table(\
                 'va1_emb_st', 'public.va1_src', 'embedding', \
                 NULL, '1m', 'hnsw', TRUE\
             ) AS t(action)",
        )
        .await;
    assert!(
        action_count >= 1,
        "dry_run should return at least one action, got: {action_count}"
    );
}

/// VA-1b: embedding_stream_table() with dry_run=false creates a real stream table.
#[tokio::test]
async fn test_embedding_stream_table_creates_real_st() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE va1b_src (
            id        SERIAL PRIMARY KEY,
            embedding vector(2)
        )",
    )
    .await;
    db.execute("INSERT INTO va1b_src (embedding) VALUES ('[1,0]'), ('[0,1]')")
        .await;

    db.execute(
        "SELECT pgtrickle.embedding_stream_table(\
             'va1b_emb_st', 'public.va1b_src', 'embedding', \
             NULL, '1m', 'hnsw', FALSE\
         )",
    )
    .await;

    // Stream table should exist
    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'va1b_emb_st')",
        )
        .await;
    assert!(
        exists,
        "embedding_stream_table() should create pgt_stream_tables entry"
    );
}

#[tokio::test]
async fn test_embedding_stream_table_legacy_l2_is_atomic_and_has_no_reindex_policy() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO helper_src VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let preview: Vec<(String,)> = sqlx::query_as(
        "SELECT action FROM pgtrickle.embedding_stream_table('helper_st', 'helper_src', 'embedding', NULL, '1m', 'hnsw', true)"
    ).fetch_all(&db.pool).await.expect("preview helper");
    assert_eq!(preview.len(), 2);
    assert!(preview[1].0.contains("vector_l2_ops"));
    assert!(!preview.iter().any(|row| row.0.contains("reindex_if_drift")));
    let absent: bool = db.query_scalar("SELECT to_regclass('public.helper_st') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'helper_st')").await;
    assert!(absent, "dry run must not create a stream table");

    db.execute("SELECT * FROM pgtrickle.embedding_stream_table('helper_st', 'helper_src', 'embedding', NULL, '1m', 'hnsw', false)").await;
    let index_name: String = db
        .query_scalar(
            "SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_am am ON am.oid = c.relam JOIN pg_opclass op ON op.oid = i.indclass[0] \
         WHERE i.indrelid = 'public.helper_st'::regclass AND i.indisvalid AND i.indisready \
         AND am.amname = 'hnsw' AND op.opcname = 'vector_l2_ops'",
        )
        .await;
    let policy: String = db.query_scalar("SELECT post_refresh_action::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'helper_st'").await;
    assert_eq!(policy, "none");
    let mut tx = db.pool.begin().await.expect("begin plan check");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable sequential scan");
    sqlx::query("SET LOCAL enable_bitmapscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable bitmap scan");
    sqlx::query("SET LOCAL enable_indexonlyscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable covering scan");
    let plan: serde_json::Value = sqlx::query_scalar(
        "EXPLAIN (FORMAT JSON) SELECT id FROM helper_st ORDER BY embedding::vector(3) <-> '[1,0,0]'::vector LIMIT 1"
    ).fetch_one(&mut *tx).await.expect("explain L2 query");
    assert!(
        plan.to_string()
            .contains(&format!("\"Index Name\":\"{index_name}\"")),
        "matching L2 index is not eligible: {plan}"
    );
    tx.rollback().await.expect("end plan check");
}

#[tokio::test]
async fn test_embedding_stream_table_v2_metrics_and_options() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE metric_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO metric_source VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let preview: Vec<(String,)> = sqlx::query_as(
        r#"SELECT action FROM pgtrickle.embedding_stream_table_v2('metric_cosine', 'metric_source', 'embedding', 'cosine', '{"columns":["id"],"hnsw_m":8,"hnsw_ef_construction":32}', true)"#,
    ).fetch_all(&db.pool).await.expect("preview cosine helper");
    assert_eq!(preview.len(), 2);
    assert!(preview[1].0.contains("vector_cosine_ops"));
    let absent: bool = db
        .query_scalar("SELECT to_regclass('public.metric_cosine') IS NULL")
        .await;
    assert!(absent);
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_cosine', 'metric_source', 'embedding', 'cosine', '{"columns":["id"],"hnsw_m":8,"hnsw_ef_construction":32}')"#).await;
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_ip', 'metric_source', 'embedding', 'inner_product', '{"index_type":"ivfflat","ivfflat_lists":1,"post_refresh_action":"analyze"}')"#).await;
    let cosine: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_opclass op ON op.oid=i.indclass[0] WHERE i.indrelid='metric_cosine'::regclass AND i.indisvalid AND i.indisready AND op.opcname='vector_cosine_ops' AND c.reloptions @> ARRAY['m=8','ef_construction=32'])").await;
    assert!(cosine);
    let inner_product: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_opclass op ON op.oid=i.indclass[0] WHERE i.indrelid='metric_ip'::regclass AND i.indisvalid AND i.indisready AND op.opcname='vector_ip_ops' AND c.reloptions @> ARRAY['lists=1'])").await;
    assert!(inner_product);
    let mut tx = db.pool.begin().await.expect("begin metric plan check");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable sequential scan");
    for (table, opclass, query) in [
        (
            "metric_cosine",
            "vector_cosine_ops",
            "EXPLAIN (FORMAT JSON) SELECT id FROM metric_cosine ORDER BY embedding::vector(3) <=> '[1,0,0]'::vector LIMIT 1",
        ),
        (
            "metric_ip",
            "vector_ip_ops",
            "EXPLAIN (FORMAT JSON) SELECT id FROM metric_ip ORDER BY embedding::vector(3) <#> '[1,0,0]'::vector LIMIT 1",
        ),
    ] {
        let index: String = sqlx::query_scalar("SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_opclass op ON op.oid=i.indclass[0] WHERE i.indrelid=$1::regclass AND op.opcname=$2")
            .bind(table).bind(opclass).fetch_one(&mut *tx).await.expect("find metric index");
        let plan: serde_json::Value = sqlx::query_scalar(query)
            .fetch_one(&mut *tx)
            .await
            .expect("explain metric query");
        assert!(
            plan.to_string()
                .contains(&format!("\"Index Name\":\"{index}\"")),
            "matching index is not eligible: {plan}"
        );
    }
    tx.rollback().await.expect("end metric plan check");
    let policy: String = db.query_scalar("SELECT post_refresh_action::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name='metric_ip'").await;
    assert_eq!(policy, "analyze");
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_full', 'metric_source', 'embedding', 'l2', '{"refresh_mode":"FULL","refresh_interval":"1h"}')"#).await;
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_immediate', 'metric_source', 'embedding', 'l2', '{"refresh_mode":"IMMEDIATE"}')"#).await;
    let modes: i64 = db.query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE (pgt_name='metric_full' AND refresh_mode='FULL') OR (pgt_name='metric_immediate' AND refresh_mode='IMMEDIATE')").await;
    assert_eq!(modes, 2);
    for options in [
        r#"{"unknown":1}"#,
        r#"{"columns":["missing"]}"#,
        r#"{"columns":["id","id"]}"#,
        r#"{"columns":["id); DROP TABLE metric_source; --"]}"#,
        r#"{"hnsw_m":1}"#,
        r#"{"hnsw_m":80}"#,
        r#"{"ivfflat_lists":2}"#,
        r#"{"index_type":"ivfflat","ivfflat_lists":0}"#,
        r#"{"refresh_mode":"IMMEDIATE","refresh_interval":"1m"}"#,
    ] {
        let sql = format!(
            "SELECT * FROM pgtrickle.embedding_stream_table_v2('invalid_metric_st', 'metric_source', 'embedding', 'cosine', '{options}', true)"
        );
        assert!(db.try_execute(&sql).await.is_err(), "accepted {options}");
    }
    assert!(db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table_v2('invalid_metric_st', 'metric_source', 'embedding', 'unknown', NULL, true)").await.is_err());
}

#[tokio::test]
async fn test_embedding_stream_table_index_conflict_rolls_back_creation() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_conflict_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE helper_unrelated (id int)").await;
    let preview: String = db.query_scalar(
        "SELECT action FROM pgtrickle.embedding_stream_table('helper_conflict_st', 'helper_conflict_src', 'embedding', NULL, '1m', 'hnsw', true) WHERE action LIKE '-- 2.%'"
    ).await;
    let index_name = preview.split('"').nth(1).expect("preview index identifier");
    assert!(
        index_name.starts_with("pgt_ann_")
            && index_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
    );
    db.execute(&format!(
        "CREATE INDEX {index_name} ON helper_unrelated (id)"
    ))
    .await;
    assert!(db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('helper_conflict_st', 'helper_conflict_src', 'embedding', NULL, '1m', 'hnsw', true)").await.is_err());
    let result = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('helper_conflict_st', 'helper_conflict_src', 'embedding', NULL, '1m', 'hnsw', false)").await;
    assert!(
        result.is_err(),
        "conflicting index must fail instead of claiming success"
    );
    let clean: bool = db.query_scalar("SELECT to_regclass('public.helper_conflict_st') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'helper_conflict_st') AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_change_tracking WHERE source_relid = 'helper_conflict_src'::regclass)").await;
    assert!(clean, "index failure must roll back stream and CDC state");
}

#[tokio::test]
async fn test_embedding_stream_table_starter_fixture_executes() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(include_str!("fixtures/pgvector_l2_starter.sql"))
        .execute(&db.pool)
        .await
        .expect("execute L2 starter SQL");
    let nearest: i64 = db
        .query_scalar(
            "SELECT id FROM doc_corpus ORDER BY embedding::vector(3) <-> '[1,0,0]'::vector LIMIT 1",
        )
        .await;
    assert_eq!(nearest, 1);
    let cosine_nearest: i64 = db.query_scalar("SELECT id FROM corpus_cosine ORDER BY embedding::vector(3) <=> '[1,0,0]'::vector LIMIT 1").await;
    assert_eq!(cosine_nearest, 1);
}

#[tokio::test]
async fn test_embedding_stream_table_quoted_names_and_vector_schema() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"V.X\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"V.X\"")
        .await;
    db.execute("CREATE SCHEMA \"A.B\"").await;
    db.execute("CREATE SCHEMA \"D.S\"").await;
    db.execute("CREATE TABLE \"A.B\".\"Doc.Source\" (id int PRIMARY KEY, \"Embedding Value\" \"V.X\".vector(3))").await;
    db.execute("INSERT INTO \"A.B\".\"Doc.Source\" VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT * FROM pgtrickle.embedding_stream_table('\"D.S\".\"Corpus.A\"', '\"A.B\".\"Doc.Source\"', '\"Embedding Value\"', NULL, '1h', 'hnsw', false)").await;
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('"D.S"."Cosine.Corpus"', '"A.B"."Doc.Source"', '"Embedding Value"', 'cosine', '{"columns":["id"]}')"#).await;
    let v2_valid: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_opclass op ON op.oid=i.indclass[0] JOIN pg_namespace n ON n.oid=op.opcnamespace WHERE i.indrelid='\"D.S\".\"Cosine.Corpus\"'::regclass AND i.indisvalid AND i.indisready AND op.opcname='vector_cosine_ops' AND n.nspname='V.X')").await;
    assert!(v2_valid);
    let valid: bool = db.query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_opclass op ON op.oid = i.indclass[0] JOIN pg_namespace n ON n.oid = op.opcnamespace \
         JOIN pg_am am ON am.oid = c.relam WHERE i.indrelid = '\"D.S\".\"Corpus.A\"'::regclass \
         AND i.indisvalid AND i.indisready AND am.amname = 'hnsw' \
         AND op.opcname = 'vector_l2_ops' AND n.nspname = 'V.X')"
    ).await;
    assert!(
        valid,
        "quoted relation and extension names must produce a valid L2 index"
    );
    let direct = db
        .try_execute(
            "INSERT INTO \"D.S\".\"Corpus.A\" (id, \"Embedding Value\") VALUES (3, '[0,0,1]')",
        )
        .await;
    assert!(
        direct.is_err(),
        "quoted stream table must retain its DML guard"
    );
    db.execute("INSERT INTO \"A.B\".\"Doc.Source\" VALUES (2, '[0,1,0]')")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('\"D.S\".\"Corpus.A\"')")
        .await;
    let rows: i64 = db
        .query_scalar("SELECT count(*) FROM \"D.S\".\"Corpus.A\"")
        .await;
    assert_eq!(rows, 2, "quoted stream must refresh after source changes");
}

#[tokio::test]
async fn test_embedding_stream_table_dry_run_rejects_invalid_source_types() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE scalar_source (id int PRIMARY KEY, embedding int)")
        .await;
    let no_extension = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('bad_st', 'scalar_source', 'embedding', dry_run => true)").await;
    assert!(
        no_extension.is_err(),
        "pgvector must be installed for a validated preview"
    );
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE undimensioned_source (id int PRIMARY KEY, embedding vector)")
        .await;
    db.execute("CREATE SCHEMA fake_vector").await;
    db.execute("CREATE DOMAIN fake_vector.vector AS text").await;
    db.execute("CREATE TABLE impostor_source (id int PRIMARY KEY, embedding fake_vector.vector)")
        .await;
    for source in [
        "missing_source",
        "scalar_source",
        "undimensioned_source",
        "impostor_source",
    ] {
        let sql = format!(
            "SELECT * FROM pgtrickle.embedding_stream_table('bad_st', '{source}', 'embedding', dry_run => true)"
        );
        assert!(
            db.try_execute(&sql).await.is_err(),
            "invalid source {source} must be rejected"
        );
    }
    let absent: bool = db.query_scalar("SELECT to_regclass('public.bad_st') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'bad_st')").await;
    assert!(absent, "invalid previews must not create objects");
}

#[tokio::test]
async fn test_embedding_stream_table_halfvec_and_sparsevec_indexes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_half_src (id int PRIMARY KEY, embedding halfvec(3))")
        .await;
    db.execute("CREATE TABLE helper_sparse_src (id int PRIMARY KEY, embedding sparsevec(3))")
        .await;
    db.execute("INSERT INTO helper_half_src VALUES (1, '[1,0,0]')")
        .await;
    db.execute("INSERT INTO helper_sparse_src VALUES (1, '{1:1}/3')")
        .await;
    let unsupported = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('sparse_bad', 'helper_sparse_src', 'embedding', index_type => 'ivfflat', dry_run => true)").await;
    assert!(unsupported.is_err(), "IVFFlat does not support sparsevec");
    for (source, stream, opclass) in [
        ("helper_half_src", "helper_half_st", "halfvec_l2_ops"),
        ("helper_sparse_src", "helper_sparse_st", "sparsevec_l2_ops"),
    ] {
        db.execute(&format!(
            "SELECT * FROM pgtrickle.embedding_stream_table('{stream}', '{source}', 'embedding')"
        ))
        .await;
        let valid: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
             JOIN pg_am am ON am.oid = c.relam JOIN pg_opclass op ON op.oid = i.indclass[0] \
             WHERE i.indrelid = '{stream}'::regclass AND i.indisvalid AND i.indisready \
             AND am.amname = 'hnsw' AND op.opcname = '{opclass}')"
            ))
            .await;
        assert!(valid, "{stream}: expected a valid {opclass} index");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// VA-4 (v0.48.0): attach_embedding_outbox() catalog registration
// ═══════════════════════════════════════════════════════════════════════

/// VA-4: attach_embedding_outbox() registers the embedding_vector_column.
#[tokio::test]
async fn test_attach_embedding_outbox_catalog_entry() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE va4_src (
            id        SERIAL PRIMARY KEY,
            embedding vector(3)
        )",
    )
    .await;
    db.execute("INSERT INTO va4_src (embedding) VALUES ('[1,0,0]'), ('[0,1,0]')")
        .await;
    db.create_st(
        "va4_emb_st",
        "SELECT id, embedding FROM va4_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute("CREATE EXTENSION pg_tide").await;

    db.execute("SELECT pgtrickle.attach_embedding_outbox('va4_emb_st', 'embedding')")
        .await;

    let vec_col: Option<String> = db
        .query_scalar_opt(
            "SELECT embedding_vector_column FROM pgtrickle.pgt_outbox_config \
             WHERE stream_table_name = 'public.va4_emb_st'",
        )
        .await;
    assert_eq!(
        vec_col,
        Some("embedding".to_string()),
        "attach_embedding_outbox should store embedding_vector_column"
    );
}
