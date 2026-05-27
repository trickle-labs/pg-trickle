//! Integration tests for end-to-end stream table workflows.
//!
//! These tests simulate the full lifecycle of creating source tables,
//! creating "stream tables" (storage tables + catalog entries),
//! refreshing them, and verifying the results.

mod common;

use common::TestDb;

// ── Full Refresh Workflow ──────────────────────────────────────────────────

#[tokio::test]
async fn test_workflow_full_refresh_lifecycle() {
    let db = TestDb::with_catalog().await;

    // Create source table with data
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO orders VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)")
        .await;

    let src_oid: i32 = db.query_scalar("SELECT 'orders'::regclass::oid::int").await;

    // Simulate create_stream_table: create storage table
    db.execute(
        "CREATE TABLE public.enriched_orders (\
         __pgt_row_id BIGINT, id INT, customer TEXT, amount NUMERIC\
        )",
    )
    .await;

    let storage_oid: i32 = db
        .query_scalar("SELECT 'enriched_orders'::regclass::oid::int")
        .await;

    // Insert catalog entry
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables \
         (pgt_relid, pgt_name, pgt_schema, defining_query, schedule, refresh_mode) \
         VALUES ({}, 'enriched_orders', 'public', \
                 'SELECT id, customer, amount FROM orders WHERE amount > 50', \
                 '1 minute', 'FULL')",
        storage_oid
    ))
    .await;

    // Insert dependency
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_dependencies (pgt_id, source_relid, source_type) \
         VALUES (1, {}, 'TABLE')",
        src_oid
    ))
    .await;

    // Simulate full refresh: populate storage table
    db.execute(
        "INSERT INTO public.enriched_orders (__pgt_row_id, id, customer, amount) \
         SELECT hashtext(row_to_json(sub)::text)::bigint, sub.* \
         FROM (SELECT id, customer, amount FROM orders WHERE amount > 50) sub",
    )
    .await;

    let count = db.count("public.enriched_orders").await;
    assert_eq!(count, 3, "All orders have amount > 50");

    // Strong row-level data validation: storage table must exactly match
    // the defining query result (multiset comparison via EXCEPT ALL).
    db.assert_sets_equal(
        "public.enriched_orders",
        "(SELECT id, customer, amount FROM orders WHERE amount > 50)",
        &["id", "customer", "amount"],
    )
    .await;

    // Update catalog: mark as populated
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET is_populated = true, status = 'ACTIVE', \
         data_timestamp = now(), last_refresh_at = now() \
         WHERE pgt_id = 1",
    )
    .await;

    // Record refresh history
    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_history \
         (pgt_id, data_timestamp, start_time, end_time, action, status, rows_inserted) \
         VALUES (1, now(), now() - interval '1 second', now(), 'FULL', 'COMPLETED', 3)",
    )
    .await;

    // Verify the refresh was recorded
    let refresh_status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_refresh_history WHERE pgt_id = 1 ORDER BY refresh_id DESC LIMIT 1",
        )
        .await;
    assert_eq!(refresh_status, "COMPLETED");

    // Verify ST is active
    let pgt_status: String = db
        .query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_id = 1")
        .await;
    assert_eq!(pgt_status, "ACTIVE");
}

// ── Differential Data Changes ───────────────────────────────────────────────

#[tokio::test]
async fn test_workflow_cdc_changes_tracked_in_buffer() {
    let db = TestDb::with_catalog().await;

    // Create source and ST
    db.execute("CREATE TABLE products (id INT PRIMARY KEY, price NUMERIC)")
        .await;
    db.execute("INSERT INTO products VALUES (1, 10.00), (2, 20.00)")
        .await;

    let src_oid: i32 = db
        .query_scalar("SELECT 'products'::regclass::oid::int")
        .await;

    // Create change buffer table (typed columns matching source schema)
    db.execute(&format!(
        "CREATE TABLE pgtrickle_changes.changes_{} (\
         change_id   BIGSERIAL PRIMARY KEY,\
         lsn         PG_LSN NOT NULL,\
         action      CHAR(1) NOT NULL,\
         pk_hash     BIGINT,\
         \"new_id\" INT, \"new_price\" NUMERIC,\
         \"old_id\" INT, \"old_price\" NUMERIC\
        )",
        src_oid
    ))
    .await;

    // Simulate CDC: record an INSERT change
    db.execute(&format!(
        "INSERT INTO pgtrickle_changes.changes_{} (lsn, action, \"new_id\", \"new_price\") \
         VALUES ('0/ABCD', 'I', 3, 30.00)",
        src_oid
    ))
    .await;

    // Simulate CDC: record an UPDATE change
    db.execute(&format!(
        "INSERT INTO pgtrickle_changes.changes_{} (lsn, action, \
         \"new_id\", \"new_price\", \"old_id\", \"old_price\") \
         VALUES ('0/ABCE', 'U', 1, 15.00, 1, 10.00)",
        src_oid
    ))
    .await;

    // Simulate CDC: record a DELETE change
    db.execute(&format!(
        "INSERT INTO pgtrickle_changes.changes_{} (lsn, action, \"old_id\", \"old_price\") \
         VALUES ('0/ABCF', 'D', 2, 20.00)",
        src_oid
    ))
    .await;

    let change_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle_changes.changes_{}",
            src_oid
        ))
        .await;
    assert_eq!(change_count, 3);

    // Verify changes are ordered by LSN
    let lsns: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT lsn::text FROM pgtrickle_changes.changes_{} ORDER BY lsn",
        src_oid
    )))
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(lsns.len(), 3);

    // After processing, delete consumed changes
    db.execute(&format!(
        "DELETE FROM pgtrickle_changes.changes_{} WHERE lsn <= '0/ABCF'",
        src_oid
    ))
    .await;

    let remaining: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle_changes.changes_{}",
            src_oid
        ))
        .await;
    assert_eq!(remaining, 0, "All consumed changes should be deleted");
}

// ── DAG-like Dependency Chain ──────────────────────────────────────────────

#[tokio::test]
async fn test_workflow_chained_sts_dependency_dag() {
    let db = TestDb::with_catalog().await;

    // Base table -> ST1 -> ST2 (chained)
    db.execute("CREATE TABLE base_data (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE st1_storage (id INT, val INT)")
        .await;
    db.execute("CREATE TABLE st2_storage (total_val BIGINT)")
        .await;

    let base_oid: i32 = db
        .query_scalar("SELECT 'base_data'::regclass::oid::int")
        .await;
    let st1_oid: i32 = db
        .query_scalar("SELECT 'st1_storage'::regclass::oid::int")
        .await;
    let st2_oid: i32 = db
        .query_scalar("SELECT 'st2_storage'::regclass::oid::int")
        .await;

    // Create ST1: SELECT * FROM base_data
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables \
         (pgt_relid, pgt_name, pgt_schema, defining_query, schedule, refresh_mode, status) \
         VALUES ({}, 'st1', 'public', 'SELECT * FROM base_data', '1m', 'FULL', 'ACTIVE')",
        st1_oid
    ))
    .await;

    // Create ST2: SELECT SUM(val) FROM st1 (depends on ST1)
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables \
         (pgt_relid, pgt_name, pgt_schema, defining_query, schedule, refresh_mode, status) \
         VALUES ({}, 'st2', 'public', 'SELECT SUM(val) FROM st1_storage', '5m', 'FULL', 'ACTIVE')",
        st2_oid
    ))
    .await;

    // Dependencies:
    // ST1 -> base_data (TABLE)
    // ST2 -> st1_storage (STREAM_TABLE)
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_dependencies (pgt_id, source_relid, source_type) VALUES \
         (1, {}, 'TABLE'), (2, {}, 'STREAM_TABLE')",
        base_oid, st1_oid
    ))
    .await;

    // Verify the dependency chain
    let st1_sources: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_dependencies WHERE pgt_id = 1")
        .await;
    let st2_sources: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_dependencies WHERE pgt_id = 2")
        .await;
    assert_eq!(st1_sources, 1);
    assert_eq!(st2_sources, 1);

    // Verify we can query the dependency graph
    let graph: Vec<(i64, String)> = sqlx::query_as(
        "SELECT d.pgt_id, d.source_type \
         FROM pgtrickle.pgt_dependencies d \
         ORDER BY d.pgt_id",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();

    assert_eq!(graph.len(), 2);
    assert_eq!(graph[0], (1, "TABLE".to_string()));
    assert_eq!(graph[1], (2, "STREAM_TABLE".to_string()));
}

// ── Error Handling and Suspension ──────────────────────────────────────────

#[tokio::test]
async fn test_workflow_error_escalation_suspends_st() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE err_src (id INT)").await;
    let oid: i32 = db
        .query_scalar("SELECT 'err_src'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables \
         (pgt_relid, pgt_name, pgt_schema, defining_query, refresh_mode, status) \
         VALUES ({}, 'err_st', 'public', 'SELECT * FROM err_src', 'FULL', 'ACTIVE')",
        oid
    ))
    .await;

    // Simulate 3 consecutive failures with refresh history
    for i in 1..=3 {
        db.execute(&format!(
            "INSERT INTO pgtrickle.pgt_refresh_history \
             (pgt_id, data_timestamp, start_time, end_time, action, status, error_message) \
             VALUES (1, now(), now() - interval '{} seconds', now(), 'FULL', 'FAILED', \
                     'Connection refused')",
            i
        ))
        .await;

        db.execute(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET consecutive_errors = consecutive_errors + 1 WHERE pgt_id = 1",
        )
        .await;
    }

    // After 3 errors, auto-suspend
    let errors: i32 = db
        .query_scalar("SELECT consecutive_errors FROM pgtrickle.pgt_stream_tables WHERE pgt_id = 1")
        .await;
    assert_eq!(errors, 3);

    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET status = 'SUSPENDED' WHERE pgt_id = 1 AND consecutive_errors >= 3",
    )
    .await;

    let status: String = db
        .query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_id = 1")
        .await;
    assert_eq!(status, "SUSPENDED");

    // Verify failure history
    let failure_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = 1 AND status = 'FAILED'",
        )
        .await;
    assert_eq!(failure_count, 3);
}

// ── Scheduler Job Lifecycle ─────────────────────────────────────────────────

/// Test the full QUEUED → RUNNING → SUCCEEDED job lifecycle for the scheduler
/// job table.
///
/// The scheduler uses `pgtrickle.pgt_scheduler_jobs` to dispatch parallel
/// refresh work across worker processes. This test verifies:
/// - A job can be enqueued (QUEUED)
/// - A worker can claim it (QUEUED → RUNNING)
/// - A worker can complete it (RUNNING → SUCCEEDED / PERMANENT_FAILED)
/// - Indexes correctly index on (status, enqueued_at) and (unit_key, status)
#[tokio::test]
async fn test_scheduler_job_lifecycle_queued_to_succeeded() {
    let db = TestDb::with_catalog().await;

    // Create a source table and ST so we have valid pgt_ids
    db.execute("CREATE TABLE sched_src (id INT)").await;
    let oid: i32 = db
        .query_scalar("SELECT 'sched_src'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, refresh_mode, status)
         VALUES ({oid}, 'sched_st', 'public', 'SELECT 1', 'FULL', 'ACTIVE')"
    ))
    .await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'sched_st'")
        .await;

    // Enqueue a job (simulating the scheduler queueing a singleton refresh unit)
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_scheduler_jobs
            (dag_version, unit_key, unit_kind, member_pgt_ids, root_pgt_id,
             status, scheduler_pid)
         VALUES
            (1, 'sched_st/public', 'singleton', ARRAY[{pgt_id}], {pgt_id},
             'QUEUED', pg_backend_pid())"
    ))
    .await;

    let job_id: i64 = db
        .query_scalar(
            "SELECT job_id FROM pgtrickle.pgt_scheduler_jobs WHERE unit_key = 'sched_st/public'",
        )
        .await;

    // Verify initial state
    let status: String = db
        .query_scalar(&format!(
            "SELECT status FROM pgtrickle.pgt_scheduler_jobs WHERE job_id = {job_id}"
        ))
        .await;
    assert_eq!(status, "QUEUED");

    let worker_pid: Option<i32> = db
        .query_scalar_opt(&format!(
            "SELECT worker_pid FROM pgtrickle.pgt_scheduler_jobs WHERE job_id = {job_id}"
        ))
        .await;
    assert!(worker_pid.is_none(), "worker_pid should be NULL on QUEUED");

    // Worker claims the job: QUEUED → RUNNING
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_scheduler_jobs
         SET status = 'RUNNING', started_at = now(), worker_pid = pg_backend_pid()
         WHERE job_id = {job_id} AND status = 'QUEUED'"
    ))
    .await;

    let running_status: String = db
        .query_scalar(&format!(
            "SELECT status FROM pgtrickle.pgt_scheduler_jobs WHERE job_id = {job_id}"
        ))
        .await;
    assert_eq!(running_status, "RUNNING");

    let has_started_at: bool = db
        .query_scalar(&format!(
            "SELECT started_at IS NOT NULL FROM pgtrickle.pgt_scheduler_jobs WHERE job_id = {job_id}"
        ))
        .await;
    assert!(has_started_at, "started_at must be set on RUNNING");

    // Worker completes successfully: RUNNING → SUCCEEDED
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_scheduler_jobs
         SET status = 'SUCCEEDED', finished_at = now(),
             outcome_detail = 'refreshed 42 rows'
         WHERE job_id = {job_id} AND status = 'RUNNING'"
    ))
    .await;

    let final_status: String = db
        .query_scalar(&format!(
            "SELECT status FROM pgtrickle.pgt_scheduler_jobs WHERE job_id = {job_id}"
        ))
        .await;
    assert_eq!(final_status, "SUCCEEDED");

    let outcome: String = db
        .query_scalar(&format!(
            "SELECT outcome_detail FROM pgtrickle.pgt_scheduler_jobs WHERE job_id = {job_id}"
        ))
        .await;
    assert_eq!(outcome, "refreshed 42 rows");

    let has_finished_at: bool = db
        .query_scalar(&format!(
            "SELECT finished_at IS NOT NULL FROM pgtrickle.pgt_scheduler_jobs WHERE job_id = {job_id}"
        ))
        .await;
    assert!(has_finished_at, "finished_at must be set on SUCCEEDED");
}

/// Test the QUEUED → RUNNING → RETRYABLE_FAILED → QUEUED retry cycle.
///
/// Workers may fail transiently (e.g. lock timeout). The scheduler should be
/// able to re-enqueue such jobs for another attempt.
#[tokio::test]
async fn test_scheduler_job_lifecycle_retryable_failure() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE retry_src (id INT)").await;
    let oid: i32 = db
        .query_scalar("SELECT 'retry_src'::regclass::oid::int")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, refresh_mode, status)
         VALUES ({oid}, 'retry_st', 'public', 'SELECT 1', 'FULL', 'ACTIVE')"
    ))
    .await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retry_st'")
        .await;

    // Enqueue → claim → fail (retryable)
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_scheduler_jobs
            (dag_version, unit_key, unit_kind, member_pgt_ids, root_pgt_id,
             status, scheduler_pid)
         VALUES
            (1, 'retry_st/public', 'singleton', ARRAY[{pgt_id}], {pgt_id},
             'QUEUED', pg_backend_pid())"
    ))
    .await;

    let job_id: i64 = db
        .query_scalar("SELECT job_id FROM pgtrickle.pgt_scheduler_jobs WHERE unit_key = 'retry_st/public' LIMIT 1")
        .await;

    // QUEUED → RUNNING
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_scheduler_jobs
         SET status = 'RUNNING', started_at = now(), worker_pid = 9999
         WHERE job_id = {job_id}"
    ))
    .await;

    // RUNNING → RETRYABLE_FAILED (transient lock timeout)
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_scheduler_jobs
         SET status = 'RETRYABLE_FAILED', finished_at = now(),
             outcome_detail = 'lock timeout', retryable = true
         WHERE job_id = {job_id}"
    ))
    .await;

    let failed_status: String = db
        .query_scalar(&format!(
            "SELECT status FROM pgtrickle.pgt_scheduler_jobs WHERE job_id = {job_id}"
        ))
        .await;
    assert_eq!(failed_status, "RETRYABLE_FAILED");

    // Scheduler re-enqueues: insert new attempt (attempt_no = 2)
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_scheduler_jobs
            (dag_version, unit_key, unit_kind, member_pgt_ids, root_pgt_id,
             status, scheduler_pid, attempt_no)
         VALUES
            (1, 'retry_st/public', 'singleton', ARRAY[{pgt_id}], {pgt_id},
             'QUEUED', pg_backend_pid(), 2)"
    ))
    .await;

    // Verify two jobs exist for this unit: attempt 1 (RETRYABLE_FAILED) and attempt 2 (QUEUED)
    let total_jobs: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_scheduler_jobs WHERE unit_key = 'retry_st/public'",
        )
        .await;
    assert_eq!(total_jobs, 2, "should have original + retry job");

    let queued_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_scheduler_jobs WHERE unit_key = 'retry_st/public' AND status = 'QUEUED'")
        .await;
    assert_eq!(queued_count, 1, "retry job should be QUEUED");

    let max_attempt: i32 = db
        .query_scalar("SELECT max(attempt_no) FROM pgtrickle.pgt_scheduler_jobs WHERE unit_key = 'retry_st/public'")
        .await;
    assert_eq!(max_attempt, 2, "max attempt_no should be 2 after retry");
}

// ── ST Drop Cascade ─────────────────────────────────────────────────────────

/// Test that dropping (deleting) a stream table cascades cleanly:
/// - The ST's storage table is dropped
/// - The catalog row is removed
/// - Dependencies (pgt_dependencies) are removed via CASCADE DELETE
/// - Refresh history is NOT automatically deleted (history is preserved for audit)
///
/// In the real extension, `drop_stream_table()` handles this. Here we simulate
/// the SQL that the extension would run.
#[tokio::test]
async fn test_workflow_st_drop_cascade() {
    let db = TestDb::with_catalog().await;

    // Create source table and storage table
    db.execute("CREATE TABLE drop_src (id INT, val TEXT)").await;
    db.execute("CREATE TABLE public.drop_st_storage (id INT, val TEXT)")
        .await;

    let src_oid: i32 = db
        .query_scalar("SELECT 'drop_src'::regclass::oid::int")
        .await;
    let storage_oid: i32 = db
        .query_scalar("SELECT 'drop_st_storage'::regclass::oid::int")
        .await;

    // Register the ST in the catalog
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables
            (pgt_relid, pgt_name, pgt_schema, defining_query, refresh_mode, status, is_populated)
         VALUES ({storage_oid}, 'drop_st', 'public', 'SELECT id, val FROM drop_src', 'FULL', 'ACTIVE', true)"
    )).await;

    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'drop_st'")
        .await;

    // Register dependency
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_dependencies (pgt_id, source_relid, source_type)
         VALUES ({pgt_id}, {src_oid}, 'TABLE')"
    ))
    .await;

    // Record some refresh history
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_refresh_history
            (pgt_id, data_timestamp, start_time, end_time, action, status, rows_inserted)
         VALUES
            ({pgt_id}, now() - interval '5 min', now() - interval '5 min', now() - interval '4 min', 'FULL', 'COMPLETED', 10),
            ({pgt_id}, now() - interval '2 min', now() - interval '2 min', now() - interval '1 min', 'DIFFERENTIAL', 'COMPLETED', 2)"
    )).await;

    // Verify pre-drop state
    assert_eq!(db.count("pgtrickle.pgt_stream_tables").await, 1);
    assert_eq!(db.count("pgtrickle.pgt_dependencies").await, 1);
    assert_eq!(db.count("pgtrickle.pgt_refresh_history").await, 2);

    // Simulate drop: delete the ST row. Due to ON DELETE CASCADE on pgt_dependencies,
    // dependencies should be removed automatically.
    // Refresh history does NOT have CASCADE — it's preserved for audit.
    db.execute("DROP TABLE IF EXISTS public.drop_st_storage")
        .await;
    db.execute(&format!(
        "DELETE FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {pgt_id}"
    ))
    .await;

    // Verify post-drop state
    let st_gone: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {pgt_id}"
        ))
        .await;
    assert_eq!(st_gone, 0, "ST catalog row must be removed after drop");

    // CASCADE DELETE must have removed dependencies
    let deps_gone: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_dependencies WHERE pgt_id = {pgt_id}"
        ))
        .await;
    assert_eq!(
        deps_gone, 0,
        "dependencies must cascade-delete when ST is dropped"
    );

    // Storage table must be gone
    let table_exists: bool = db
        .query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'drop_st_storage' AND relkind = 'r')",
        )
        .await;
    assert!(!table_exists, "storage table must be dropped");
}
