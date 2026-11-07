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
        status["refreshes_since_reported_coverage"].as_i64(),
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
    assert!(unknown["refreshes_since_reported_coverage"].is_null());
    assert!(unknown["reported_covered_at"].is_string());
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
    db.execute("INSERT INTO epoch_join_left VALUES (2, 20)")
        .await;
    let after: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'epoch_join_stream'").await;
    assert!(after > before, "cleanup-only writes must advance the epoch");
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
async fn test_pgvector_compat_installed_version() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let (server, installed, pg_trickle): (i32, String, String) = sqlx::query_as(
        "SELECT current_setting('server_version_num')::int, \
         (SELECT extversion FROM pg_extension WHERE extname = 'vector'), \
         (SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("inspect installed versions");
    assert_eq!(server / 10_000, 18, "wrong PostgreSQL major version");
    assert_eq!(pg_trickle, env!("CARGO_PKG_VERSION"));
    if let Ok(expected) = std::env::var("PGVECTOR_EXPECTED_VERSION") {
        assert_eq!(server, 180_006, "wrong PostgreSQL compatibility image");
        assert_eq!(installed, expected, "wrong pgvector test image");
    }
}

#[tokio::test]
async fn test_pgvector_helper_missing_extension_is_actionable_and_atomic() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE no_vector_source (id int PRIMARY KEY, embedding text)")
        .await;
    let error = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table_v2('no_vector_stream', 'no_vector_source', 'embedding', 'cosine')")
        .await.expect_err("helper requires pgvector");
    assert!(
        error
            .to_string()
            .contains("pgvector extension is not installed"),
        "unexpected error: {error}"
    );
    let clean: bool = db
        .query_scalar(
            "SELECT to_regclass('no_vector_stream') IS NULL AND NOT EXISTS \
        (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'no_vector_stream')",
        )
        .await;
    assert!(clean, "failed helper must not leave a stream table");
}

#[tokio::test]
async fn test_pgvector_helper_invalid_column_is_actionable_and_atomic() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE invalid_vector_source (id int PRIMARY KEY, embedding text)")
        .await;
    for column in ["missing", "embedding"] {
        let error = db.try_execute(&format!("SELECT * FROM pgtrickle.embedding_stream_table_v2('invalid_vector_stream', 'invalid_vector_source', '{column}', 'cosine', '{{\"columns\":[\"id\"]}}')"))
            .await.expect_err("helper requires a pgvector column");
        assert!(
            error.to_string().contains("must be a pgvector"),
            "unexpected error: {error}"
        );
        let clean: bool = db
            .query_scalar(
                "SELECT to_regclass('invalid_vector_stream') IS NULL AND NOT EXISTS \
            (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'invalid_vector_stream')",
            )
            .await;
        assert!(clean, "failed helper must not leave a stream table");
    }
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

#[tokio::test]
async fn test_wide_vectors_refresh_without_oversized_covering_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE bounded_index_source (id int PRIMARY KEY, value int)")
        .await;
    db.create_st(
        "bounded_index_stream",
        "SELECT id, value FROM bounded_index_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let covered: bool = db.query_scalar("SELECT indnatts > indnkeyatts FROM pg_index WHERE indexrelid = 'bounded_index_stream_row_id_idx'::regclass").await;
    assert!(covered, "small fixed-width outputs should remain covered");
    for dimensions in [768, 1536] {
        let source = format!("wide_vectors_{dimensions}");
        let stream = format!("wide_vector_st_{dimensions}");
        db.execute(&format!(
            "CREATE TABLE {source} (id int PRIMARY KEY, embedding vector({dimensions}))"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} SELECT 1, ARRAY(SELECT sin(n::float8)::real \
            FROM generate_series(1, {dimensions}) n)::vector({dimensions})"
        ))
        .await;
        db.create_st(
            &stream,
            &format!("SELECT id, embedding FROM {source}"),
            "1h",
            "DIFFERENTIAL",
        )
        .await;
        let no_includes: bool = db
            .query_scalar(&format!(
                "SELECT indnatts = indnkeyatts FROM pg_index \
            WHERE indexrelid = '{stream}_row_id_idx'::regclass"
            ))
            .await;
        assert!(
            no_includes,
            "unbounded vectors must not enter the B-tree index"
        );
        db.execute(&format!(
            "CREATE INDEX {stream}_ann ON {stream} USING hnsw (embedding vector_cosine_ops)"
        ))
        .await;
        db.execute(&format!(
            "UPDATE {source} SET embedding = ARRAY(SELECT cos(n::float8)::real \
            FROM generate_series(1, {dimensions}) n)::vector({dimensions})"
        ))
        .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy = 'differential'",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{stream}')"),
        )
        .await
        .expect("refresh wide vector");
        db.assert_st_matches_query(&stream, &format!("SELECT id, embedding FROM {source}"))
            .await;
    }
}

#[tokio::test]
async fn test_vector_aggregate_unknown_dimensions_rejects_mixed_input_without_data_loss() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE unknown_vector_dims (id int PRIMARY KEY, embedding vector)")
        .await;
    db.execute("INSERT INTO unknown_vector_dims VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SELECT pgtrickle.create_stream_table('unknown_vector_st', $$SELECT avg(embedding) AS centroid FROM unknown_vector_dims$$, '1m', 'DIFFERENTIAL')",
    ]).await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='unknown_vector_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "vector");
    let error = db.try_execute("CREATE INDEX unknown_vector_ann ON unknown_vector_st USING hnsw (centroid vector_l2_ops)").await.expect_err("an undimensioned vector cannot be indexed directly");
    assert!(
        error.to_string().contains("dimensions"),
        "unexpected error: {error}"
    );

    db.execute("INSERT INTO unknown_vector_dims VALUES (2, '[1,0]')")
        .await;
    let error = db
        .try_execute_with_config(
            &[
                "SET pg_trickle.enable_vector_agg = on",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            "SELECT pgtrickle.refresh_stream_table('unknown_vector_st')",
        )
        .await
        .expect_err("mixed dimensions must not be silently narrowed");
    assert!(
        error.to_string().contains("dimensions"),
        "unexpected error: {error}"
    );
    let centroid: String = db
        .query_scalar("SELECT centroid::text FROM unknown_vector_st")
        .await;
    assert_eq!(centroid, "[1,0,0]");
}

#[tokio::test]
async fn test_vector_aggregate_qualified_join_uses_argument_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE typed_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE typed_scalars (id int PRIMARY KEY, embedding double precision)")
        .await;
    db.execute("INSERT INTO typed_vectors VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.execute("INSERT INTO typed_scalars VALUES (1, 42), (2, 10)")
        .await;
    let query = "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('typed_join_st', $${query}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    db.execute("UPDATE typed_scalars SET embedding = 20 WHERE id = 2")
        .await;
    db.execute("UPDATE typed_vectors SET embedding = '[0,0,1]' WHERE id = 2")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_join_st')",
    ])
    .await;
    let differential: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name='typed_join_st' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(differential);
    db.assert_st_matches_query("typed_join_st", "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding)::vector(3) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)").await;
    let scalar_type: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='typed_join_st'::regclass AND attname='scalar_mean'").await;
    assert_eq!(scalar_type, "double precision");
    db.execute("UPDATE typed_vectors SET embedding=NULL WHERE id=1")
        .await;
    db.execute("UPDATE typed_scalars SET embedding=NULL WHERE id=1")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_join_st')",
    ])
    .await;
    db.assert_st_matches_query("typed_join_st", "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding)::vector(3) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)").await;
    db.execute("DELETE FROM typed_vectors WHERE id=2").await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_join_st')",
    ])
    .await;
    db.assert_st_matches_query("typed_join_st", "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding)::vector(3) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)").await;
    db.execute("INSERT INTO typed_vectors VALUES (3, '[1,1,0]')")
        .await;
    db.execute("INSERT INTO typed_scalars VALUES (3, 7)").await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_join_st')",
    ])
    .await;
    db.assert_st_matches_query("typed_join_st", "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding)::vector(3) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)").await;
}

#[tokio::test]
async fn test_vector_aggregate_cast_uses_cast_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE typed_casts (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO typed_casts VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let query = "SELECT avg(embedding::halfvec(3)) AS centroid FROM typed_casts";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('typed_cast_st', $${query}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    db.execute("UPDATE typed_casts SET embedding = '[0,0,1]' WHERE id = 2")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('typed_cast_st')",
    ])
    .await;
    let differential: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name='typed_cast_st' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(differential);
    db.assert_st_matches_query(
        "typed_cast_st",
        "SELECT avg(embedding::halfvec(3))::halfvec(3) AS centroid FROM typed_casts",
    )
    .await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='typed_cast_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "halfvec(3)");
}

#[tokio::test]
async fn test_vector_aggregate_join_uses_each_bound_dimension() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE dims_three (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE dims_two (id int PRIMARY KEY, embedding vector(2))")
        .await;
    db.execute("INSERT INTO dims_three VALUES (1, '[1,0,0]')")
        .await;
    db.execute("INSERT INTO dims_two VALUES (1, '[1,0]')").await;
    let create = "SELECT pgtrickle.create_stream_table('dims_st', $$SELECT avg(a.embedding) AS three, avg(b.embedding) AS two FROM dims_three a JOIN dims_two b USING(id)$$, '1m', 'DIFFERENTIAL')";
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", create])
        .await;
    let types: Vec<(String, String)> = sqlx::query_as("SELECT attname::text, format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='dims_st'::regclass AND attname IN ('three','two') ORDER BY attname").fetch_all(&db.pool).await.expect("read dimensioned output types");
    assert_eq!(
        types,
        vec![
            ("three".into(), "vector(3)".into()),
            ("two".into(), "vector(2)".into())
        ]
    );
    db.execute("UPDATE dims_two SET embedding='[0,1]' WHERE id=1")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('dims_st')",
    ])
    .await;
    db.assert_st_matches_query("dims_st", "SELECT avg(a.embedding)::vector(3) AS three, avg(b.embedding)::vector(2) AS two FROM dims_three a JOIN dims_two b USING(id)").await;
}

#[tokio::test]
async fn test_vector_aggregate_normalized_expression_keeps_unknown_dimension() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE normalized_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO normalized_source VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let create = "SELECT pgtrickle.create_stream_table('normalized_st', $$SELECT avg(l2_normalize(embedding)) AS centroid FROM normalized_source$$, '1m', 'DIFFERENTIAL')";
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", create])
        .await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='normalized_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "vector");
    db.execute("UPDATE normalized_source SET embedding='[0,0,1]' WHERE id=2")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('normalized_st')",
    ])
    .await;
    db.assert_st_matches_query(
        "normalized_st",
        "SELECT avg(l2_normalize(embedding)) AS centroid FROM normalized_source",
    )
    .await;
}

#[tokio::test]
async fn test_vector_aggregate_immediate_uses_analyzed_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE immediate_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO immediate_vectors VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", "SELECT pgtrickle.create_stream_table('immediate_vectors_st', $$SELECT avg(embedding::halfvec(3)) AS centroid FROM immediate_vectors$$, NULL, 'IMMEDIATE')", "UPDATE immediate_vectors SET embedding='[0,1,0]' WHERE id=1"]).await;
    db.assert_st_matches_query(
        "immediate_vectors_st",
        "SELECT avg(embedding::halfvec(3))::halfvec(3) AS centroid FROM immediate_vectors",
    )
    .await;
}

#[tokio::test]
async fn test_vector_aggregate_nonpublic_extension_schema() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"Vec.X\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"Vec.X\"")
        .await;
    db.execute("CREATE TABLE private_vectors (id int PRIMARY KEY, embedding \"Vec.X\".vector(3))")
        .await;
    db.execute("INSERT INTO private_vectors VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&["SET search_path = public, \"Vec.X\"", "SET pg_trickle.enable_vector_agg = on", "SELECT pgtrickle.create_stream_table('private_vector_st', $$SELECT avg(embedding) AS centroid FROM private_vectors$$, '1m', 'DIFFERENTIAL')"]).await;
    let type_matches: bool = db.query_scalar("SELECT atttypid='\"Vec.X\".vector'::regtype AND atttypmod=3 FROM pg_attribute WHERE attrelid='private_vector_st'::regclass AND attname='centroid'").await;
    assert!(type_matches);
    db.execute("UPDATE private_vectors SET embedding='[0,1,0]' WHERE id=1")
        .await;
    db.execute_seq(&[
        "SET search_path = public, \"Vec.X\"",
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('private_vector_st')",
    ])
    .await;
    let same: bool = db
        .query_scalar("SELECT centroid::text = '[0,1,0]' FROM private_vector_st")
        .await;
    assert!(same);
}

#[tokio::test]
async fn test_vector_projection_retains_typmod_for_direct_ann_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE projected_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO projected_vectors VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('projected_vector_st', $$SELECT id, embedding FROM projected_vectors$$, '1m', 'DIFFERENTIAL')").await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='projected_vector_st'::regclass AND attname='embedding'").await;
    assert_eq!(typ, "vector(3)");
    db.execute("CREATE INDEX projected_vector_ann ON projected_vector_st USING hnsw (embedding vector_l2_ops)").await;
    let mut tx = db.pool.begin().await.expect("begin plan check");
    sqlx::query("SET LOCAL enable_seqscan=off")
        .execute(&mut *tx)
        .await
        .expect("disable sequential scan");
    let plan: serde_json::Value = sqlx::query_scalar("EXPLAIN (FORMAT JSON) SELECT id FROM projected_vector_st ORDER BY embedding <-> '[1,0,0]'::vector LIMIT 1").fetch_one(&mut *tx).await.expect("explain vector search");
    assert!(
        plan.to_string().contains("projected_vector_ann"),
        "direct index is not eligible: {plan}"
    );
    tx.rollback().await.expect("end plan check");
}

#[tokio::test]
async fn test_vector_aggregate_cte_alias_and_group_key_changes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE cte_vectors (id int PRIMARY KEY, grp int, embedding vector(3))")
        .await;
    db.execute("INSERT INTO cte_vectors VALUES (1, 1, '[1,0,0]'), (2, 2, '[0,1,0]')")
        .await;
    let query = "WITH c AS (SELECT id, grp, embedding AS vec FROM cte_vectors) SELECT grp, avg(vec) AS centroid FROM c GROUP BY grp";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('cte_vectors_st', $${query}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='cte_vectors_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "vector(3)");
    db.execute("UPDATE cte_vectors SET grp=2, embedding='[0,0,1]' WHERE id=1")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('cte_vectors_st')",
    ])
    .await;
    db.assert_st_matches_query("cte_vectors_st", "WITH c AS (SELECT id, grp, embedding AS vec FROM cte_vectors) SELECT grp, avg(vec)::vector(3) AS centroid FROM c GROUP BY grp").await;
}

#[tokio::test]
async fn test_vector_aggregate_disabled_reports_actionable_error() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE disabled_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    let error = db.try_execute_with_config(&["SET pg_trickle.enable_vector_agg = off"], "SELECT pgtrickle.create_stream_table('disabled_vector_st', $$SELECT avg(embedding) AS centroid FROM disabled_vectors$$, '1m', 'DIFFERENTIAL')").await.expect_err("vector aggregate should require opt-in");
    assert!(
        error.to_string().contains("enable_vector_agg"),
        "unexpected error: {error}"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = off", "SELECT pgtrickle.create_stream_table('disabled_auto_st', $$SELECT avg(embedding) AS centroid FROM disabled_vectors$$, '1m', 'AUTO')"]).await;
    let mode: String = db.query_scalar("SELECT refresh_mode::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name='disabled_auto_st'").await;
    assert_eq!(mode, "FULL");
}

#[tokio::test]
async fn test_vector_aggregate_alter_query_keeps_expression_index_usable() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alter_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alter_vectors VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", "SELECT pgtrickle.create_stream_table('alter_vectors_st', $$SELECT avg(embedding) AS centroid FROM alter_vectors$$, '1m', 'DIFFERENTIAL')"]).await;
    db.execute("CREATE INDEX alter_vectors_ann ON alter_vectors_st USING hnsw ((l2_normalize(centroid)::vector(3)) vector_l2_ops)").await;
    let alter = "SELECT pgtrickle.alter_stream_table('alter_vectors_st', query => $$SELECT avg(embedding) AS centroid FROM alter_vectors WHERE id > 0$$)";
    let error = db
        .try_execute_with_config(&["SET pg_trickle.enable_vector_agg = on"], alter)
        .await
        .expect_err("ALTER must not silently drop the expression index");
    assert!(
        error
            .to_string()
            .contains("expression or ANN index alter_vectors_ann"),
        "unexpected error: {error}"
    );
    let usable: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid=to_regclass('alter_vectors_ann') AND indisvalid AND indisready)").await;
    assert!(usable, "ALTER QUERY must retain a usable expression index");
    db.assert_st_matches_query(
        "alter_vectors_st",
        "SELECT avg(embedding)::vector(3) AS centroid FROM alter_vectors",
    )
    .await;
    db.execute("DROP INDEX alter_vectors_ann").await;
    db.execute("CREATE INDEX alter_vectors_ann_plain ON alter_vectors_st USING hnsw (centroid vector_l2_ops)").await;
    let error = db
        .try_execute_with_config(&["SET pg_trickle.enable_vector_agg = on"], alter)
        .await
        .expect_err("ALTER must not silently drop a plain ANN index");
    assert!(
        error.to_string().contains("alter_vectors_ann_plain"),
        "unexpected error: {error}"
    );
    db.execute("DROP INDEX alter_vectors_ann_plain").await;
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", alter])
        .await;
    db.execute("CREATE INDEX alter_vectors_ann ON alter_vectors_st USING hnsw ((l2_normalize(centroid)::vector(3)) vector_l2_ops)").await;
    db.assert_st_matches_query(
        "alter_vectors_st",
        "SELECT avg(embedding)::vector(3) AS centroid FROM alter_vectors WHERE id > 0",
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
// F4-3: Explicit FULL refresh for a distance-ordered Top-K query
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_distance_topk_explicit_full() {
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

    // This exercises explicit FULL mode, not automatic fallback.
    let q = "SELECT id, category, embedding FROM emb_dist ORDER BY embedding <-> '[1,0,0]' LIMIT 5";
    db.create_st("dist_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("dist_st", q).await;

    // Refresh after insert
    db.execute("INSERT INTO emb_dist (category, embedding) VALUES ('cat1', '[0.8,0.2,0]')")
        .await;
    db.refresh_st("dist_st").await;
    db.assert_st_matches_query("dist_st", q).await;
}

#[tokio::test]
async fn test_pgvector_row_local_distance_differential() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE distance_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO distance_src VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let query = "SELECT id, embedding <=> '[1,0,0]'::vector AS distance \
                 FROM distance_src WHERE embedding <-> '[1,0,0]'::vector < 1";
    db.create_st("distance_local_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st("distance_auto_st", query, "1h", "AUTO").await;
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('distance_immediate_st', $${query}$$, refresh_mode => 'IMMEDIATE')"
    )).await;

    for mutation in [
        "INSERT INTO distance_src VALUES (3, '[0.9,0.1,0]')",
        "UPDATE distance_src SET embedding = '[0,1,0]' WHERE id = 1",
        "UPDATE distance_src SET embedding = '[1,0,0]' WHERE id = 2",
        "DELETE FROM distance_src WHERE id = 3",
    ] {
        db.execute(mutation).await;
        db.assert_st_matches_query("distance_immediate_st", query)
            .await;
        for name in ["distance_local_st", "distance_auto_st"] {
            let previous_id: i64 = db
                .query_scalar(&format!(
                    "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = '{name}'"
                ))
                .await;
            db.try_execute_with_config(
                &[
                    "SET pg_trickle.refresh_strategy = 'differential'",
                    "SET pg_trickle.differential_max_change_ratio = 0",
                ],
                &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
            )
            .await
            .expect("refresh row-local vector query");
            let differential: bool = db.query_scalar(&format!(
                "SELECT h.refresh_id > {previous_id} AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
                 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                 WHERE s.pgt_name = '{name}' ORDER BY h.refresh_id DESC LIMIT 1"
            )).await;
            assert!(differential, "{name}: no completed differential refresh");
            db.assert_st_matches_query(name, query).await;
        }
    }
    db.execute_seq(&[
        "BEGIN",
        "INSERT INTO distance_src VALUES (10, '[1,0,0]')",
        "ROLLBACK",
    ])
    .await;
    for name in [
        "distance_local_st",
        "distance_auto_st",
        "distance_immediate_st",
    ] {
        db.assert_st_matches_query(name, query).await;
    }
}

#[tokio::test]
async fn test_pgvector_distance_topk_recomputes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE distance_rank_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute(
        "INSERT INTO distance_rank_src VALUES (1, '[1,0,0]'), (2, '[0,1,0]'), (3, '[0,0,1]')",
    )
    .await;
    let query = "SELECT id, embedding FROM distance_rank_src \
                 ORDER BY embedding <-> '[1,0,0]'::vector, id LIMIT 2";
    db.create_st("distance_rank_st", query, "1h", "DIFFERENTIAL")
        .await;

    for mutation in [
        "INSERT INTO distance_rank_src VALUES (4, '[0.9,0,0]')",
        "UPDATE distance_rank_src SET embedding = '[0.8,0,0]' WHERE id = 3",
        "DELETE FROM distance_rank_src WHERE id = 1",
        "DELETE FROM distance_rank_src WHERE id IN (2, 3, 4)",
    ] {
        db.execute(mutation).await;
        let previous_id: i64 = db.query_scalar(
            "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'distance_rank_st'"
        ).await;
        db.refresh_st("distance_rank_st").await;
        let recomputed: bool = db.query_scalar(&format!(
            "SELECT h.refresh_id > {previous_id} AND h.status = 'COMPLETED' AND h.merge_strategy_used = 'TOP_K' \
             FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
             WHERE s.pgt_name = 'distance_rank_st' ORDER BY h.refresh_id DESC LIMIT 1"
        )).await;
        assert!(
            recomputed,
            "Top-K refresh was not recorded as recomputation"
        );
        db.assert_st_matches_query("distance_rank_st", query).await;
    }
}

#[tokio::test]
async fn test_pgvector_distance_types_and_functions_differential() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    for (kind, first, second, zero) in [
        ("vector", "[1,0,0]", "[0,1,0]", "[0,0,0]"),
        ("halfvec", "[1,0,0]", "[0,1,0]", "[0,0,0]"),
        ("sparsevec", "{1:1}/3", "{2:1}/3", "{}/3"),
    ] {
        let source = format!("distance_{kind}_src");
        let stream = format!("distance_{kind}_st");
        db.execute(&format!(
            "CREATE TABLE {source} (id int PRIMARY KEY, embedding {kind}(3))"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES (1, '{first}'), (2, '{zero}'), (3, NULL)"
        ))
        .await;
        let pivot = format!("'{first}'::{kind}");
        let query = format!(
            "SELECT id, embedding <-> {pivot} AS l2, embedding <=> {pivot} AS cosine, \
             embedding <#> {pivot} AS neg_ip, embedding <+> {pivot} AS l1, \
             l2_distance(embedding, {pivot}) AS l2_fn, \
             cosine_distance(embedding, {pivot}) AS cosine_fn, \
             inner_product(embedding, {pivot}) AS ip_fn, \
             l1_distance(embedding, {pivot}) AS l1_fn FROM {source}"
        );
        db.create_st(&stream, &query, "1h", "DIFFERENTIAL").await;
        db.assert_st_matches_query(&stream, &query).await;

        for mutation in [
            format!("INSERT INTO {source} VALUES (4, '{second}')"),
            format!("UPDATE {source} SET embedding = '{second}' WHERE id IN (1, 3)"),
            format!("DELETE FROM {source} WHERE id = 2"),
        ] {
            db.execute(&mutation).await;
            let previous_id: i64 = db
                .query_scalar(&format!(
                    "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = '{stream}'"
                ))
                .await;
            db.try_execute_with_config(
                &[
                    "SET pg_trickle.refresh_strategy = 'differential'",
                    "SET pg_trickle.differential_max_change_ratio = 0",
                ],
                &format!("SELECT pgtrickle.refresh_stream_table('{stream}')"),
            )
            .await
            .expect("refresh vector distance expressions");
            let differential: bool = db.query_scalar(&format!(
                "SELECT h.refresh_id > {previous_id} AND h.action = 'DIFFERENTIAL' \
                 AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
                 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                 WHERE s.pgt_name = '{stream}' ORDER BY h.refresh_id DESC LIMIT 1"
            )).await;
            assert!(differential, "{kind}: no completed differential refresh");
            db.assert_st_matches_query(&stream, &query).await;
        }
    }
}

#[tokio::test]
async fn test_pgvector_distance_changing_pivot_differential() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE distance_docs (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE distance_pivot (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO distance_docs VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.execute("INSERT INTO distance_pivot VALUES (1, '[1,0,0]')")
        .await;
    let query = "SELECT d.id, d.embedding <-> p.embedding AS distance \
                 FROM distance_docs d CROSS JOIN distance_pivot p WHERE p.id = 1";
    db.create_st("distance_pivot_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("UPDATE distance_pivot SET embedding = '[0,1,0]' WHERE id = 1")
        .await;
    let previous_id: i64 = db
        .query_scalar(
            "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'distance_pivot_st'",
        )
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ],
        "SELECT pgtrickle.refresh_stream_table('distance_pivot_st')",
    )
    .await
    .expect("refresh after changing the pivot");
    let differential: bool = db
        .query_scalar(&format!(
            "SELECT h.refresh_id > {previous_id} AND h.action = 'DIFFERENTIAL' \
         AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
         FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
         WHERE s.pgt_name = 'distance_pivot_st' ORDER BY h.refresh_id DESC LIMIT 1"
        ))
        .await;
    assert!(differential, "pivot change did not refresh differentially");
    db.assert_st_matches_query("distance_pivot_st", query).await;
}

#[tokio::test]
async fn test_pgvector_distance_non_public_extension_schema() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"Vector.Ext\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"Vector.Ext\"")
        .await;
    db.execute(
        "CREATE TABLE schema_distance_src (id int PRIMARY KEY, embedding \"Vector.Ext\".vector(3), pivot \"Vector.Ext\".vector(3))",
    )
    .await;
    db.execute("INSERT INTO schema_distance_src VALUES (1, '[1,0,0]', '[1,0,0]')")
        .await;
    let query = "SELECT id, embedding OPERATOR(\"Vector.Ext\".<=>) pivot AS distance \
                 FROM schema_distance_src";
    db.create_st("schema_distance_st", query, "1h", "DIFFERENTIAL")
        .await;
    let previous_id: i64 = db
        .query_scalar(
            "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'schema_distance_st'",
        )
        .await;
    db.execute("UPDATE schema_distance_src SET embedding = '[0,1,0]' WHERE id = 1")
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ],
        "SELECT pgtrickle.refresh_stream_table('schema_distance_st')",
    )
    .await
    .expect("refresh schema-qualified vector distance");
    let differential: bool = db.query_scalar(&format!(
        "SELECT h.refresh_id > {previous_id} AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
         FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
         WHERE s.pgt_name = 'schema_distance_st' ORDER BY h.refresh_id DESC LIMIT 1"
    )).await;
    assert!(differential, "schema-qualified distance fell back to FULL");
    db.assert_st_matches_query("schema_distance_st", query)
        .await;
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
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SELECT pgtrickle.refresh_stream_table('centroid_hnsw_st')",
    ])
    .await;
    db.assert_st_matches_query("centroid_hnsw_st", expected_q)
        .await;

    let index_ready: bool = db
        .query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_index i \
         JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_am am ON am.oid = c.relam \
         JOIN pg_opclass op ON op.oid = i.indclass[0] \
         WHERE i.indexrelid = 'public.centroid_hnsw_idx'::regclass \
         AND i.indrelid = 'public.centroid_hnsw_st'::regclass \
         AND i.indisvalid AND i.indisready AND am.amname = 'hnsw' \
         AND op.opcname = 'vector_cosine_ops')",
        )
        .await;
    assert!(index_ready, "expected a valid, ready cosine HNSW index");

    // Test index eligibility separately from the planner's cost-based choice.
    let mut tx = db.pool.begin().await.expect("begin plan transaction");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .expect("discourage sequential scan");
    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT user_id FROM public.centroid_hnsw_st ORDER BY centroid <=> '[1,0,0]'::vector LIMIT 1",
    )
    .fetch_all(&mut *tx)
    .await
    .expect("explain cosine ANN query");
    assert!(
        plan.iter().any(|line| line.contains("centroid_hnsw_idx")),
        "cosine query must be eligible for the HNSW index: {plan:?}"
    );
    let wrong_metric_plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT user_id FROM public.centroid_hnsw_st ORDER BY centroid <-> '[1,0,0]'::vector LIMIT 1",
    )
    .fetch_all(&mut *tx)
    .await
    .expect("explain L2 query");
    assert!(
        wrong_metric_plan
            .iter()
            .all(|line| !line.contains("centroid_hnsw_idx")),
        "L2 ordering must not use the cosine HNSW index: {wrong_metric_plan:?}"
    );
    tx.rollback().await.expect("end plan transaction");

    // Verify the matching search returns the nearest centroid.
    let nn_result = db
        .query_scalar_opt::<i32>("SELECT user_id FROM public.centroid_hnsw_st ORDER BY centroid <=> '[1,0,0]'::vector LIMIT 1")
        .await;
    assert_eq!(nn_result, Some(1));
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

#[tokio::test]
async fn test_vector_diagnostics_catalog_visibility_and_index_metadata() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute(
        "CREATE TABLE diagnostic_src (id int PRIMARY KEY, a vector(3), b vector(3), c vector)",
    )
    .await;
    db.execute("INSERT INTO diagnostic_src VALUES (1, '[1,0,0]', '[0,1,0]', '[1,1,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('diagnostic_st', $$SELECT id, a, b, c FROM diagnostic_src$$, '1m', 'DIFFERENTIAL')").await;
    db.execute("CREATE INDEX diagnostic_cosine ON diagnostic_st USING hnsw (a vector_cosine_ops) WHERE id > 0").await;
    db.execute("CREATE INDEX diagnostic_expression ON diagnostic_st USING hnsw ((b::vector(3)) vector_l2_ops)").await;

    let columns = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'post_refresh_action' = 'none' AND v->>'dimensions' = '3' AND v->'embedding_freshness' = 'null'::jsonb AND v->'maintenance_pending' = 'null'::jsonb").await;
    assert_eq!(columns, 2);
    let unknown = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'column' = 'c' AND v->'dimensions' = 'null'::jsonb AND v->'requested_index_state' = 'null'::jsonb AND v->>'requested_index_reason' IS NOT NULL").await;
    assert_eq!(unknown, 1);
    let indexes = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'access_method' = 'hnsw' AND v->>'valid' = 'true' AND v->>'ready' = 'true' AND ((v->>'index' = 'diagnostic_cosine' AND v->>'opclass' = 'vector_cosine_ops' AND v->>'predicate' IS NOT NULL) OR (v->>'index' = 'diagnostic_expression' AND v->>'opclass' = 'vector_l2_ops'))").await;
    assert_eq!(indexes, 2);

    db.execute("CREATE ROLE diagnostic_observer").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO diagnostic_observer")
        .await;
    let mut conn = db.pool.acquire().await.expect("acquire connection");
    sqlx::query("SET ROLE diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("set role");
    let hidden: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtrickle.vector_stream_status_v2()")
            .fetch_one(&mut *conn)
            .await
            .expect("inspect as observer");
    assert_eq!(hidden, 0);
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset role");
    sqlx::query("GRANT SELECT ON diagnostic_st TO diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("grant select");
    sqlx::query("SET ROLE diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("set role");
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.ann_index_status_v2()")
        .fetch_one(&mut *conn)
        .await
        .expect("inspect as observer");
    assert_eq!(visible, 2);
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset role");
}

#[tokio::test]
async fn test_vector_diagnostics_reports_partition_leaf_indexes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE diagnostic_partition_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st_partitioned(
        "diagnostic_partition_st",
        "SELECT id, embedding FROM diagnostic_partition_src",
        "1m",
        "DIFFERENTIAL",
        "id",
    )
    .await;
    db.execute("CREATE INDEX diagnostic_partition_ann ON diagnostic_partition_st USING hnsw (embedding vector_l2_ops)").await;
    let leaves = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE v->>'stream_table_oid' = 'diagnostic_partition_st'::regclass::oid::text AND v->>'table_oid' <> v->>'stream_table_oid' AND v->>'opclass' = 'vector_l2_ops'").await;
    assert!(leaves > 0);
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

#[tokio::test]
async fn test_distance_subscription_notifies_only_membership_crossings() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_source (id int PRIMARY KEY, note text, embedding vector(3))")
        .await;
    db.create_st(
        "alert_st",
        "SELECT id, note, embedding FROM alert_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen("distance_crossing")
        .await
        .expect("listen for crossings");
    db.execute("SELECT pgtrickle.subscribe_distance('public.alert_st', 'distance_crossing', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    db.execute("INSERT INTO alert_source VALUES (1, 'first', '[1,0,0]')")
        .await;
    db.refresh_st("alert_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("enter notification")
        .expect("receive enter");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert_eq!(payload["exited"], 0);
    db.execute("UPDATE alert_source SET note='changed' WHERE id=1")
        .await;
    db.refresh_st("alert_st").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "unchanged membership must not notify"
    );
    drop(listener);
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("reconnect listener");
    listener
        .listen("distance_crossing")
        .await
        .expect("listen for exit");
    db.execute("UPDATE alert_source SET embedding='[0,1,0]' WHERE id=1")
        .await;
    db.refresh_st("alert_st").await;
    let value: String = db
        .query_scalar("SELECT embedding::text FROM alert_st WHERE id=1")
        .await;
    assert_eq!(value, "[0,1,0]");
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("exit notification")
        .expect("receive exit");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 0);
    assert_eq!(payload["exited"], 1);
    db.refresh_st("alert_st").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "unchanged refresh must not notify"
    );
    let mut tx = db
        .pool
        .begin()
        .await
        .expect("begin rolled-back source change");
    sqlx::query("INSERT INTO alert_source VALUES (2, 'rolled back', '[1,0,0]')")
        .execute(&mut *tx)
        .await
        .expect("insert in transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('alert_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh in transaction");
    tx.rollback().await.expect("rollback");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "rolled-back change must not notify"
    );
}

#[tokio::test]
async fn test_distance_subscription_alter_query_reports_committed_crossings() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_alter_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_alter_src VALUES (1, '[1,0,0]'), (2, '[1,0,0]')")
        .await;
    db.create_st(
        "alert_alter_st",
        "SELECT id, embedding FROM alert_alter_src WHERE id = 1",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_alter_st', 'alter_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("alter_crossings").await.expect("listen");

    db.execute("SELECT pgtrickle.alter_stream_table('alert_alter_st', query => $$SELECT id, embedding FROM alert_alter_src WHERE id = 2$$)").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("receive ALTER event")
        .expect("listener event");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert_eq!(payload["exited"], 1);
    let refresh_id = payload["refresh_id"].as_i64().expect("refresh id");
    let recorded: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id = $1 AND s.pgt_name = 'alert_alter_st' AND h.action = 'FULL' AND h.status = 'COMPLETED')")
        .bind(refresh_id).fetch_one(&db.pool).await.expect("refresh history");
    assert!(recorded);

    db.execute("SELECT pgtrickle.alter_stream_table('alert_alter_st', query => $$SELECT id, embedding FROM alert_alter_src WHERE id = 2$$)").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err()
    );

    let mut tx = db.pool.begin().await.expect("begin rollback test");
    sqlx::query("SELECT pgtrickle.alter_stream_table('alert_alter_st', query => $$SELECT id, embedding FROM alert_alter_src WHERE id = 1$$)")
        .execute(&mut *tx).await.expect("alter before rollback");
    tx.rollback().await.expect("rollback ALTER");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err()
    );
    let id: i32 = db.query_scalar("SELECT id FROM alert_alter_st").await;
    assert_eq!(id, 2);
    let latest: i64 = db.query_scalar("SELECT max(refresh_id) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'alert_alter_st')").await;
    assert_eq!(
        latest, refresh_id,
        "rolled-back ALTER must not leave history"
    );

    db.execute("UPDATE pgtrickle.pgt_distance_subscriptions SET query_vector = 'invalid' WHERE channel = 'alter_crossings'").await;
    db.execute("SELECT pgtrickle.alter_stream_table('alert_alter_st', query => $$SELECT id, embedding FROM alert_alter_src WHERE id = 1$$)").await;
    let id: i32 = db.query_scalar("SELECT id FROM alert_alter_st").await;
    assert_eq!(id, 1, "notification failure must not roll back ALTER");
}

#[tokio::test]
async fn test_distance_subscription_partition_rebuild_reports_crossings() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_partition_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_partition_src VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.create_st(
        "alert_partition_st",
        "SELECT id, embedding FROM alert_partition_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_partition_st', 'partition_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen("partition_crossings")
        .await
        .expect("listen");
    db.execute("UPDATE alert_partition_src SET embedding = CASE id WHEN 1 THEN '[0,1,0]'::vector ELSE '[1,0,0]'::vector END").await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('alert_partition_st', partition_by => 'HASH:id:2')",
    )
    .await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("receive partition rebuild event")
        .expect("listener event");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert_eq!(payload["exited"], 1);
    let refresh_id = payload["refresh_id"].as_i64().expect("refresh id");
    let recorded: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id = $1 AND s.pgt_name = 'alert_partition_st' AND h.action = 'FULL' AND h.status = 'COMPLETED')").bind(refresh_id).fetch_one(&db.pool).await.expect("refresh history");
    assert!(recorded);

    let mut tx = db.pool.begin().await.expect("begin rollback test");
    sqlx::query("UPDATE alert_partition_src SET embedding = '[1,0,0]' WHERE id = 1")
        .execute(&mut *tx)
        .await
        .expect("update before rollback");
    sqlx::query("SELECT pgtrickle.alter_stream_table('alert_partition_st', partition_by => '')")
        .execute(&mut *tx)
        .await
        .expect("rebuild before rollback");
    tx.rollback().await.expect("rollback partition rebuild");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err()
    );
    let latest: i64 = db.query_scalar("SELECT max(refresh_id) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'alert_partition_st')").await;
    assert_eq!(
        latest, refresh_id,
        "rolled-back rebuild must not leave history"
    );
}

#[tokio::test]
async fn test_distance_subscription_mode_change_reports_crossings() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_mode_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_mode_src VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "alert_mode_st",
        "SELECT id, embedding FROM alert_mode_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_mode_st', 'mode_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("mode_crossings").await.expect("listen");
    db.execute("UPDATE alert_mode_src SET embedding = '[0,1,0]' WHERE id = 1")
        .await;
    db.execute("SELECT pgtrickle.alter_stream_table('alert_mode_st', refresh_mode => 'FULL')")
        .await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("receive mode-change event")
        .expect("listener event");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 0);
    assert_eq!(payload["exited"], 1);
    let refresh_id = payload["refresh_id"].as_i64().expect("refresh id");
    let recorded: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id = $1 AND s.pgt_name = 'alert_mode_st' AND h.action = 'FULL' AND h.status = 'COMPLETED')").bind(refresh_id).fetch_one(&db.pool).await.expect("refresh history");
    assert!(recorded);

    let mut tx = db.pool.begin().await.expect("begin rollback test");
    sqlx::query("UPDATE alert_mode_src SET embedding = '[1,0,0]' WHERE id = 1")
        .execute(&mut *tx)
        .await
        .expect("update before rollback");
    sqlx::query(
        "SELECT pgtrickle.alter_stream_table('alert_mode_st', refresh_mode => 'DIFFERENTIAL')",
    )
    .execute(&mut *tx)
    .await
    .expect("change mode before rollback");
    tx.rollback().await.expect("rollback mode change");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err()
    );
    let latest: i64 = db.query_scalar("SELECT max(refresh_id) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'alert_mode_st')").await;
    assert_eq!(
        latest, refresh_id,
        "rolled-back mode change must not leave history"
    );
}

#[tokio::test]
async fn test_distance_subscription_alter_query_notifies_parent_and_downstream() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_chain_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_chain_src VALUES (1, '[1,0,0]'), (2, '[1,0,0]')")
        .await;
    db.create_st(
        "alert_chain_parent",
        "SELECT id, embedding FROM alert_chain_src WHERE id = 1",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SELECT pgtrickle.create_stream_table('alert_chain_child', $$SELECT id, avg(embedding)::vector(3) AS embedding FROM alert_chain_parent GROUP BY id$$, '1h', 'DIFFERENTIAL')",
        "RESET pg_trickle.enable_vector_agg",
    ]).await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_chain_parent', 'parent_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen("parent_crossings")
        .await
        .expect("listen to parent");
    listener
        .listen("child_crossings")
        .await
        .expect("listen to child");
    let mut registration_tx = db.pool.begin().await.expect("begin registration");
    sqlx::query("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'alert_chain_child' FOR UPDATE")
        .execute(&mut *registration_tx).await.expect("lock child catalog row");
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *registration_tx)
        .await
        .expect("registration PID");
    let pool = db.pool.clone();
    let (pid_sender, pid_receiver) = tokio::sync::oneshot::channel();
    let alter = tokio::spawn(async move {
        let mut conn = pool.acquire().await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await?;
        let _ = pid_sender.send(pid);
        sqlx::query("SELECT pgtrickle.alter_stream_table('alert_chain_parent', query => $$SELECT id, embedding FROM alert_chain_src WHERE id = 2$$)").execute(&mut *conn).await
    });
    let alter_pid = tokio::time::timeout(Duration::from_secs(10), pid_receiver)
        .await
        .expect("ALTER PID timeout")
        .expect("ALTER PID");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: bool =
                sqlx::query_scalar("SELECT $1 = ANY(pg_catalog.pg_blocking_pids($2))")
                    .bind(blocker)
                    .bind(alter_pid)
                    .fetch_one(&db.pool)
                    .await
                    .expect("inspect ALTER lock");
            if blocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("ALTER must wait for downstream registration");
    sqlx::query("SELECT pgtrickle.subscribe_distance('alert_chain_child', 'child_crossings', 'embedding', '[1,0,0]', '<->', 0.5)")
        .execute(&mut *registration_tx).await.expect("register child subscription");
    registration_tx
        .commit()
        .await
        .expect("commit child subscription");
    tokio::time::timeout(Duration::from_secs(30), alter)
        .await
        .expect("ALTER completes")
        .expect("ALTER task")
        .expect("ALTER succeeds");
    let child_records: i64 = db.query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'alert_chain_child' AND h.action = 'FULL' AND h.initiated_by = 'MANUAL' AND h.status = 'COMPLETED'").await;
    assert_eq!(
        child_records, 1,
        "downstream rebuild must have notification history"
    );
    let child_ids: Vec<i32> = sqlx::query_scalar("SELECT id FROM alert_chain_child ORDER BY id")
        .fetch_all(&db.pool)
        .await
        .expect("read downstream");
    assert_eq!(
        child_ids,
        vec![2],
        "downstream rebuild must contain replacement rows"
    );
    let mut channels = std::collections::BTreeSet::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .unwrap_or_else(|_| panic!("missing chain event; received {channels:?}"))
            .expect("listener event");
        let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
        assert_eq!(payload["entered"], 1);
        assert_eq!(payload["exited"], 1);
        let name = match event.channel() {
            "parent_crossings" => "alert_chain_parent",
            "child_crossings" => "alert_chain_child",
            other => panic!("unexpected channel {other}"),
        };
        let recorded: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id = $1 AND s.pgt_name = $2 AND h.status = 'COMPLETED')")
            .bind(payload["refresh_id"].as_i64().expect("refresh id")).bind(name).fetch_one(&db.pool).await.expect("refresh history");
        assert!(recorded);
        channels.insert(event.channel().to_string());
    }
    assert_eq!(
        channels,
        [
            "child_crossings".to_string(),
            "parent_crossings".to_string()
        ]
        .into_iter()
        .collect()
    );
}

#[tokio::test]
async fn test_distance_subscription_failed_alter_retry_restores_snapshot_context() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_retry_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_retry_src VALUES (1, '[1,0,0]'), (2, '[1,0,0]')")
        .await;
    db.create_st(
        "alert_retry_st",
        "SELECT id, embedding FROM alert_retry_src WHERE id = 1",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_retry_st', 'retry_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    db.execute("CREATE FUNCTION public.fail_alert_alter() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.pgt_name = 'alert_retry_st' AND NEW.status = 'SUSPENDED' AND current_setting('test.fail_alert_alter', true) = 'on' THEN RAISE EXCEPTION 'injected ALTER failure'; END IF; RETURN NEW; END $$").await;
    db.execute("CREATE TRIGGER fail_alert_alter BEFORE UPDATE ON pgtrickle.pgt_stream_tables FOR EACH ROW EXECUTE FUNCTION public.fail_alert_alter()").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("retry_crossings").await.expect("listen");
    let mut conn = db.pool.acquire().await.expect("acquire connection");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin");
    sqlx::query("SAVEPOINT before_failed_alter")
        .execute(&mut *conn)
        .await
        .expect("savepoint");
    sqlx::query("SET LOCAL test.fail_alert_alter = 'on'")
        .execute(&mut *conn)
        .await
        .expect("enable failure");
    let alter = "SELECT pgtrickle.alter_stream_table('alert_retry_st', query => $$SELECT id, embedding FROM alert_retry_src WHERE id = 2$$)";
    let error = sqlx::query(alter)
        .execute(&mut *conn)
        .await
        .expect_err("injected ALTER must fail");
    assert!(error.to_string().contains("injected ALTER failure"));
    sqlx::query("ROLLBACK TO SAVEPOINT before_failed_alter")
        .execute(&mut *conn)
        .await
        .expect("rollback failed ALTER");
    sqlx::query("SET LOCAL test.fail_alert_alter = 'off'")
        .execute(&mut *conn)
        .await
        .expect("disable failure");
    sqlx::query(alter)
        .execute(&mut *conn)
        .await
        .expect("retry ALTER");
    sqlx::query("COMMIT")
        .execute(&mut *conn)
        .await
        .expect("commit retry");
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("receive retry event")
        .expect("listener event");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert_eq!(payload["exited"], 1);
}

#[tokio::test]
async fn test_distance_subscription_small_delta_snapshots_only_affected_rows() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_batch_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute(
        "INSERT INTO alert_batch_src SELECT i, '[1,0,0]'::vector FROM generate_series(1,1000) i",
    )
    .await;
    db.create_st(
        "alert_batch_st",
        "SELECT id, embedding FROM alert_batch_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('public.alert_batch_st', 'batch_alerts', 'embedding', '[1,0,0]', '<->', 0.5)")
        .await;
    db.execute("UPDATE alert_batch_src SET embedding='[0,1,0]' WHERE id=1")
        .await;
    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_schema='public' AND pgt_name='alert_batch_st'")
        .await;
    let mut tx = db.pool.begin().await.expect("begin refresh transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('alert_batch_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh one changed row");
    let snapshot_sql = format!("SELECT count(*) FROM pg_temp.__pgt_distance_pre_{pgt_id}");
    let before_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(snapshot_sql))
        .fetch_one(&mut *tx)
        .await
        .expect("inspect affected pre-refresh rows");
    let ids_sql = format!("SELECT count(*) FROM pg_temp.__pgt_distance_ids_{pgt_id}");
    let id_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(ids_sql))
        .fetch_one(&mut *tx)
        .await
        .expect("inspect affected identities");
    assert_eq!(before_count, 1);
    assert_eq!(id_count, 1);
    let plan_sql = format!(
        "EXPLAIN (ANALYZE, BUFFERS) SELECT st.* FROM pg_temp.__pgt_distance_ids_{pgt_id} d \
         CROSS JOIN LATERAL (SELECT st.* FROM public.alert_batch_st st \
         WHERE pgtrickle.row_probe_v1(st.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) \
           AND st.__pgt_row_id = d.__pgt_row_id OFFSET 0) st"
    );
    let plan: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(plan_sql))
        .fetch_all(&mut *tx)
        .await
        .expect("inspect affected-row lookup plan");
    assert!(
        plan.iter().any(|line| line.contains("Index")
            && line.contains("Scan")
            && line.contains("row_id_idx")),
        "affected-row lookup should use the row-identity index: {plan:?}"
    );
    tx.commit().await.expect("commit refresh");
}

#[tokio::test]
async fn test_distance_subscription_typed_predicates_and_authorization() {
    use sqlx::postgres::PgListener;
    use std::collections::HashSet;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_typed_src (id int PRIMARY KEY, dense vector(3), half halfvec(3), bits bit(3), sparse sparsevec(3))").await;
    db.create_st(
        "alert_typed_st",
        "SELECT id, dense, half, bits, sparse FROM alert_typed_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    for channel in [
        "typed_dense",
        "typed_cosine",
        "typed_half",
        "typed_bits",
        "typed_jaccard",
        "typed_sparse",
    ] {
        listener.listen(channel).await.expect("listen");
    }
    for sql in [
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','typed_dense','dense','[1,0,0]','<#>',-0.5)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','typed_cosine','dense','[1,0,0]','<=>',0.1)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_half','half','[1,0,0]','<->',0.5)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_bits','bits','101','<~>',1)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_jaccard','bits','101','<%>',0.5)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_sparse','sparse','{1:1}/3','<->',0.5)",
    ] {
        db.execute(sql).await;
    }
    for sql in [
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_dim','dense','[1,0]','<->',0.5)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_op','bits','101','<->',0.5)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_nan','dense','[1,0,0]','<->','NaN'::float8)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_inf','dense','[1,0,0]','<->','Infinity'::float8)",
    ] {
        assert!(
            db.try_execute(sql).await.is_err(),
            "invalid subscription accepted: {sql}"
        );
    }
    db.execute("INSERT INTO alert_typed_src VALUES (1,'[1,0,0]','[1,0,0]','101','{1:1}/3')")
        .await;
    db.refresh_st("alert_typed_st").await;
    let mut channels = HashSet::new();
    for _ in 0..6 {
        let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .expect("typed alert")
            .expect("receive typed alert");
        let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
        assert_eq!(payload["entered"], 1);
        channels.insert(event.channel().to_owned());
    }
    assert_eq!(channels.len(), 6);

    db.execute("CREATE ROLE alert_intruder NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, public TO alert_intruder")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.subscribe_distance(text,text,text,text,text,double precision), pgtrickle.unsubscribe_distance(text,text) TO alert_intruder").await;
    let error = db.try_execute_with_role("SET ROLE alert_intruder", "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','stolen','dense','[1,0,0]','<->',0.5)", "RESET ROLE").await.expect_err("non-owner subscription must fail");
    assert!(
        error.to_string().contains("must be owner"),
        "unexpected denial: {error}"
    );
    let mut tx = db.pool.begin().await.expect("begin observer transaction");
    sqlx::query("SET LOCAL ROLE alert_intruder")
        .execute(&mut *tx)
        .await
        .expect("set role");
    let visible: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtrickle.list_distance_subscriptions(NULL)")
            .fetch_one(&mut *tx)
            .await
            .expect("list visible subscriptions");
    assert_eq!(visible, 0);
    tx.rollback().await.expect("end observer transaction");

    db.execute("CREATE ROLE alert_owner NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, public TO alert_owner")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.subscribe_distance(text,text,text,text,text,double precision), pgtrickle.unsubscribe_distance(text,text) TO alert_owner").await;
    db.execute("ALTER TABLE public.alert_typed_st OWNER TO alert_owner")
        .await;
    let result = db.try_execute_with_role("SET ROLE alert_owner", "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','owner_alert','dense','[1,0,0]','<->',0.5)", "RESET ROLE").await;
    assert!(
        result.is_ok(),
        "stream owner should be able to subscribe: {result:?}"
    );
    let mut tx = db.pool.begin().await.expect("begin owner transaction");
    sqlx::query("SET LOCAL ROLE alert_owner")
        .execute(&mut *tx)
        .await
        .expect("set owner role");
    let visible: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgtrickle.list_distance_subscriptions('public.alert_typed_st')",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("list owner subscriptions");
    assert_eq!(visible, 7);
    tx.rollback().await.expect("end owner transaction");
}

#[tokio::test]
async fn test_distance_subscription_key_delete_and_null_transitions() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_key_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_key_src VALUES (1,'[1,0,0]')")
        .await;
    db.create_st(
        "alert_key_st",
        "SELECT id, embedding FROM alert_key_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("key_alert").await.expect("listen");
    db.execute("SELECT pgtrickle.subscribe_distance('alert_key_st','key_alert','embedding','[1,0,0]','<->',0.5)").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), listener.recv())
            .await
            .is_err(),
        "registration establishes a baseline"
    );
    for (mutation, entered, exited) in [
        ("UPDATE alert_key_src SET id=2 WHERE id=1", 1, 1),
        ("DELETE FROM alert_key_src WHERE id=2", 0, 1),
        ("INSERT INTO alert_key_src VALUES (3,'[1,0,0]')", 1, 0),
        ("UPDATE alert_key_src SET embedding=NULL WHERE id=3", 0, 1),
    ] {
        db.execute(mutation).await;
        db.refresh_st("alert_key_st").await;
        let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .expect("crossing notification")
            .expect("receive crossing");
        let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
        assert_eq!(payload["entered"], entered, "{mutation}");
        assert_eq!(payload["exited"], exited, "{mutation}");
    }
}

#[tokio::test]
async fn test_distance_subscription_full_refresh_and_edit_baseline() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_full_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_full_src VALUES (1,'[1,0,0]')")
        .await;
    db.create_st(
        "alert_full_st",
        "SELECT id, embedding FROM alert_full_src",
        "1m",
        "FULL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("full_alert").await.expect("listen");
    db.execute("SELECT pgtrickle.subscribe_distance('alert_full_st','full_alert','embedding','[1,0,0]','<->',0.5)").await;
    db.execute("UPDATE alert_full_src SET embedding='[0,1,0]' WHERE id=1")
        .await;
    db.refresh_st("alert_full_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("full exit")
        .expect("receive full exit");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["exited"], 1);
    db.execute("SELECT pgtrickle.subscribe_distance('alert_full_st','full_alert','embedding','[0,1,0]','<->',0.5)").await;
    db.refresh_st("alert_full_st").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "subscription edits establish a baseline"
    );
    db.execute("DELETE FROM alert_full_src WHERE id=1").await;
    db.refresh_st("alert_full_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("full delete exit")
        .expect("receive full delete exit");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["exited"], 1);
    db.execute("INSERT INTO alert_full_src VALUES (2,'[0,1,0]')")
        .await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit=true WHERE pgt_name='alert_full_st'",
    )
    .await;
    db.refresh_st("alert_full_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("reinitialization entry")
        .expect("receive reinitialization entry");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
}

#[tokio::test]
async fn test_distance_subscription_quoted_names_and_extension_schema() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"Vector Ext\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"Vector Ext\"")
        .await;
    db.execute("CREATE SCHEMA \"Odd.Schema\"").await;
    db.execute("CREATE TABLE \"Odd.Schema\".source (id int PRIMARY KEY, \"vec weird\" \"Vector Ext\".vector(3))").await;
    db.execute("SELECT pgtrickle.create_stream_table('\"Odd.Schema\".\"Alert Table\"', $$SELECT id, \"vec weird\" FROM \"Odd.Schema\".source$$, '1m', 'DIFFERENTIAL')").await;
    let channel = "alerts \"odd'";
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen(channel)
        .await
        .expect("listen to quoted channel");
    db.execute_seq(&[
        "SET search_path TO \"Odd.Schema\", public",
        "SELECT pgtrickle.subscribe_distance('\"Alert Table\"', 'alerts \"odd''', '\"vec weird\"', '[1,0,0]', '<->', 0.5)",
        "RESET search_path",
    ]).await;
    db.execute("INSERT INTO \"Odd.Schema\".source VALUES (1,'[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('\"Odd.Schema\".\"Alert Table\"')")
        .await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("quoted alert")
        .expect("receive alert");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(event.channel(), channel);
    assert_eq!(payload["schema"], "Odd.Schema");
    assert_eq!(payload["stream_table"], "Alert Table");
    assert_eq!(payload["entered"], 1);
    db.execute("SELECT pgtrickle.drop_stream_table('\"Odd.Schema\".\"Alert Table\"')")
        .await;
    let count: i64 = db.query_scalar("SELECT count(*) FROM pgtrickle.pgt_distance_subscriptions WHERE channel='alerts \"odd''' ").await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_distance_subscription_rejects_unsupported_notification_modes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_modes_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "alert_modes_st",
        "SELECT id, embedding FROM alert_modes_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_modes_st','mode_alert','embedding','[1,0,0]','<->',0.5)").await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('alert_modes_st', pooler_compatibility_mode => true)",
    )
    .await;
    let error = db.try_execute("SELECT pgtrickle.subscribe_distance('alert_modes_st','new_alert','embedding','[1,0,0]','<->',0.5)").await.expect_err("pooler mode must reject subscriptions");
    assert!(
        error.to_string().contains("pooler compatibility"),
        "unexpected error: {error}"
    );
    db.execute("INSERT INTO alert_modes_src VALUES (1,'[1,0,0]')")
        .await;
    let notices = db
        .try_execute_with_notices("SELECT pgtrickle.refresh_stream_table('alert_modes_st')")
        .await
        .expect("refresh remains successful");
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains("distance notifications are disabled")),
        "missing pooler warning: {notices:?}"
    );
    db.create_st(
        "alert_immediate_st",
        "SELECT id, embedding FROM alert_modes_src",
        "1m",
        "IMMEDIATE",
    )
    .await;
    let error = db.try_execute("SELECT pgtrickle.subscribe_distance('alert_immediate_st','immediate_alert','embedding','[1,0,0]','<->',0.5)").await.expect_err("IMMEDIATE mode must reject subscriptions");
    assert!(
        error.to_string().contains("IMMEDIATE"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn test_distance_subscription_waits_for_refresh_catalog_lock() {
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_lock_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "alert_lock_st",
        "SELECT id, embedding FROM alert_lock_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut tx = db.pool.begin().await.expect("begin lock transaction");
    sqlx::query(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name='alert_lock_st' FOR UPDATE",
    )
    .execute(&mut *tx)
    .await
    .expect("hold refresh catalog lock");
    let pool = db.pool.clone();
    let mut registration = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.subscribe_distance('alert_lock_st','lock_alert','embedding','[1,0,0]','<->',0.5)")
            .execute(&pool).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut registration)
            .await
            .is_err(),
        "registration must wait for the refresh lock"
    );
    tx.commit().await.expect("release refresh catalog lock");
    tokio::time::timeout(Duration::from_secs(5), registration)
        .await
        .expect("registration completes")
        .expect("registration task")
        .expect("registration succeeds");
}

#[tokio::test]
async fn test_distance_subscription_fused_chain_scheduler_emits_crossing() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    for setting in [
        "ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100",
        "ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1",
        "ALTER SYSTEM SET pg_trickle.auto_backoff = off",
        "ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'on'",
        "ALTER SYSTEM SET pg_trickle.enable_fused_refresh = on",
    ] {
        db.execute(setting).await;
    }
    db.reload_config_and_wait().await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "scheduler did not start"
    );
    db.execute("CREATE TABLE fused_alert_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "fused_alert_first",
        "SELECT id, embedding FROM fused_alert_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "fused_alert_second",
        "SELECT id FROM fused_alert_first",
        "1s",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("INSERT INTO fused_alert_src VALUES (1,'[0,1,0]')")
        .await;
    assert!(
        db.wait_for_condition(
            "fused chain control converged",
            "SELECT count(*)=1 FROM fused_alert_second",
            Duration::from_secs(30),
            Duration::from_millis(100)
        )
        .await
    );
    // shortcut: workers start with cold template caches, so this checks chain scheduling, not fused SQL; add that control when fusion can be witnessed.
    assert!(db.wait_for_condition("fused chain control job", "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_scheduler_jobs WHERE unit_kind='fused_chain' AND status='SUCCEEDED')", Duration::from_secs(30), Duration::from_millis(100)).await);
    let previous_job: i64 = db
        .query_scalar(
            "SELECT max(job_id) FROM pgtrickle.pgt_scheduler_jobs WHERE unit_kind='fused_chain'",
        )
        .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("fused_alert").await.expect("listen");
    db.execute("SELECT pgtrickle.subscribe_distance('fused_alert_first','fused_alert','embedding','[1,0,0]','<->',0.5)").await;
    db.execute("INSERT INTO fused_alert_src VALUES (100,'[1,0,0]')")
        .await;
    let event = tokio::time::timeout(Duration::from_secs(30), listener.recv())
        .await
        .expect("scheduler crossing")
        .expect("receive scheduler crossing");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert!(db.wait_for_condition("subscribed fused chain job", &format!("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_scheduler_jobs WHERE unit_kind='fused_chain' AND status='SUCCEEDED' AND job_id > {previous_job})"), Duration::from_secs(30), Duration::from_millis(100)).await);
    let initiated_by: String = db.query_scalar("SELECT h.initiated_by FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='fused_alert_first' AND h.refresh_id=(SELECT max(h2.refresh_id) FROM pgtrickle.pgt_refresh_history h2 WHERE h2.pgt_id=s.pgt_id AND h2.status='COMPLETED')").await;
    assert_ne!(initiated_by, "SCHEDULER_FUSED");
}

#[tokio::test]
async fn test_distance_subscription_invalid_stored_value_does_not_abort_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_error_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "alert_error_st",
        "SELECT id, embedding FROM alert_error_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('public.alert_error_st', 'distance_error', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    db.execute("UPDATE pgtrickle.pgt_distance_subscriptions SET query_vector='invalid' WHERE stream_table='public.alert_error_st'").await;
    db.execute("INSERT INTO alert_error_source VALUES (1, '[1,0,0]')")
        .await;
    let notices = db
        .try_execute_with_notices("SELECT pgtrickle.refresh_stream_table('alert_error_st')")
        .await
        .expect("alert failure must not abort refresh");
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains("distance subscription failed")),
        "missing failure warning: {notices:?}"
    );
    let count: i64 = db.query_scalar("SELECT count(*) FROM alert_error_st").await;
    assert_eq!(count, 1);
}

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
    let threshold: f64 = db
        .query_scalar("SELECT threshold FROM pgtrickle.pgt_distance_subscriptions WHERE stream_table = 'public.ds_st' AND channel = 'near_centroid'")
        .await;
    assert_eq!(threshold, 0.5);
    db.execute("SELECT pgtrickle.subscribe_distance('public.ds_st', 'near_centroid', 'centroid', '[1,0,0]', '<->', 0.3)").await;
    let threshold: f64 = db
        .query_scalar("SELECT threshold FROM pgtrickle.list_distance_subscriptions('public.ds_st') WHERE channel = 'near_centroid'")
        .await;
    assert_eq!(threshold, 0.3);
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
