//! #1097: checked LSN semantics through packaged refresh and recovery callers.

mod e2e;

use e2e::E2eDb;
use sqlx::AssertSqlSafe;
#[cfg(not(feature = "light-e2e"))]
use std::time::Duration;

async fn set_source_lsn(db: &E2eDb, stream: &str, lsn: &str) {
    let (pgt_id, source_oid): (i64, i64) = sqlx::query_as(
        "SELECT st.pgt_id, d.source_relid::bigint \
         FROM pgtrickle.pgt_stream_tables st \
         JOIN pgtrickle.pgt_dependencies d USING (pgt_id) \
         WHERE st.pgt_name = $1 LIMIT 1",
    )
    .bind(stream)
    .fetch_one(&db.pool)
    .await
    .expect("stream source identity should exist");
    sqlx::query(AssertSqlSafe(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET frontier = jsonb_set(frontier, ARRAY['sources', $2::text, 'lsn'], to_jsonb($3::text)) \
         WHERE pgt_id = $1"
            .to_string(),
    ))
    .bind(pgt_id)
    .bind(source_oid)
    .bind(lsn)
    .execute(&db.pool)
    .await
    .expect("frontier fixture update should complete");
}

// #1158: CDC LSN order can differ from transaction commit order.
#[tokio::test]
async fn test_lsn_manual_refresh_late_commit_preserves_exact_results() {
    manual_late_commit_preserves_exact_results(false).await;
}

#[tokio::test]
async fn test_lsn_manual_refresh_repeated_transaction_preserves_late_commit() {
    manual_late_commit_preserves_exact_results(true).await;
}

async fn manual_late_commit_preserves_exact_results(warm_transaction: bool) {
    let db = E2eDb::new().await.with_extension().await;
    let mut refresher = db.pool.acquire().await.expect("pin fresh manual backend");
    let source = "lsn_late_commit_source";
    let stream = "lsn_late_commit_stream";
    let query = format!("SELECT id, value FROM {source}");
    db.execute(&format!(
        "CREATE TABLE {source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!("INSERT INTO {source} VALUES (1, 'baseline')"))
        .await;
    db.create_st(stream, &query, "1h", "DIFFERENTIAL").await;
    #[cfg(not(feature = "light-e2e"))]
    db.execute(&format!(
        "SELECT pgtrickle.pause_scheduler(ARRAY['{stream}'])"
    ))
    .await;
    db.refresh_st(stream).await;
    db.assert_st_matches_query(stream, &query).await;
    let history_before: i64 = db
        .query_scalar("SELECT COALESCE(MAX(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    let source_oid: i64 = db
        .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
        .await;
    let buffer = db.change_buffer_table(source_oid).await;

    if warm_transaction {
        sqlx::query("BEGIN")
            .execute(&mut *refresher)
            .await
            .expect("begin caller transaction");
        sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
            .bind(stream)
            .execute(&mut *refresher)
            .await
            .expect("refresh once before writer A exists");
    }

    let mut writer_a = db.pool.begin().await.expect("begin writer A");
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {source} VALUES (2, 'late commit')"
    )))
    .execute(&mut *writer_a)
    .await
    .expect("writer A captures CDC before writer B starts");
    let (writer_pid, writer_xid): (i32, String) =
        sqlx::query_as("SELECT pg_backend_pid(), pg_current_xact_id()::text")
            .fetch_one(&mut *writer_a)
            .await
            .expect("capture open writer identity");
    let a_lsn: String = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT MAX(lsn)::text FROM {buffer}"
    )))
    .fetch_one(&mut *writer_a)
    .await
    .expect("writer A must see its captured CDC row");

    let mut writer_b = db.pool.begin().await.expect("begin writer B");
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {source} VALUES (3, 'early commit')"
    )))
    .execute(&mut *writer_b)
    .await
    .expect("writer B captures a later CDC row");
    writer_b.commit().await.expect("commit writer B first");
    let ordered: bool = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT MAX(lsn) > $1::pg_lsn FROM {buffer}"
    )))
    .bind(&a_lsn)
    .fetch_one(&db.pool)
    .await
    .expect("witness a < b using committed CDC rows");
    assert!(ordered, "writer B must have a strictly later CDC LSN");
    let open_writer: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
         WHERE pid = $1 AND backend_xid::text = $2 AND state = 'idle in transaction')",
    )
    .bind(writer_pid)
    .bind(&writer_xid)
    .fetch_one(&db.pool)
    .await
    .expect("witness writer A remains uncommitted");
    assert!(open_writer);

    // Refresh while A is open, in either a fresh or an existing caller transaction.
    sqlx::query("SET statement_timeout = '10s'")
        .execute(&mut *refresher)
        .await
        .expect("bound the manual refresh wait");
    sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
        .bind(stream)
        .execute(&mut *refresher)
        .await
        .expect("manual refresh must finish while writer A is open");
    let frontier_before_commit: String = sqlx::query_scalar(
        "SELECT frontier->'sources'->($1::text)->>'lsn' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $2",
    )
    .bind(source_oid.to_string())
    .bind(stream)
    .fetch_one(&mut *refresher)
    .await
    .expect("read the caller's actual frontier before A commits");
    writer_a.commit().await.expect("commit writer A last");
    sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
        .bind(stream)
        .execute(&mut *refresher)
        .await
        .expect("next manual DIFFERENTIAL refresh must recover both writers");
    if warm_transaction {
        sqlx::query("COMMIT")
            .execute(&mut *refresher)
            .await
            .expect("commit caller refreshes");
    }

    let safe_frontier: bool = sqlx::query_scalar("SELECT $1::pg_lsn < $2::pg_lsn")
        .bind(&frontier_before_commit)
        .bind(&a_lsn)
        .fetch_one(&db.pool)
        .await
        .expect("compare the first refresh frontier to writer A");
    db.assert_st_matches_query(stream, &query).await;
    assert!(safe_frontier, "manual refresh advanced past an open writer");
    let modes: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT action, initiated_by, was_full_fallback FROM pgtrickle.pgt_refresh_history \
         WHERE refresh_id > $1 ORDER BY refresh_id",
    )
    .bind(history_before)
    .fetch_all(&db.pool)
    .await
    .expect("read actual refresh modes");
    assert_eq!(
        modes,
        vec![("DIFFERENTIAL".into(), "MANUAL".into(), false); if warm_transaction { 3 } else { 2 }],
        "neither scheduler nor FULL refresh may repair the result"
    );
}

#[tokio::test]
async fn test_lsn_manual_refresh_own_writes_visible_in_caller_transaction() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE lsn_own_source (id INT PRIMARY KEY, value TEXT NOT NULL)")
        .await;
    let query = "SELECT id, value FROM lsn_own_source";
    db.create_st("lsn_own_stream", query, "1h", "DIFFERENTIAL")
        .await;
    #[cfg(not(feature = "light-e2e"))]
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['lsn_own_stream'])")
        .await;
    let mut caller = db.pool.begin().await.expect("begin caller transaction");
    sqlx::query("INSERT INTO lsn_own_source VALUES (1, 'own write')")
        .execute(&mut *caller)
        .await
        .expect("capture caller CDC");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('lsn_own_stream')")
        .execute(&mut *caller)
        .await
        .expect("refresh caller writes before commit");
    let rows: Vec<(i32, String)> = sqlx::query_as("SELECT id, value FROM lsn_own_stream")
        .fetch_all(&mut *caller)
        .await
        .expect("read refreshed caller writes");
    assert_eq!(rows, vec![(1, "own write".into())]);
    caller.commit().await.expect("commit write and refresh");
    db.assert_st_matches_query("lsn_own_stream", query).await;
}

#[tokio::test]
async fn test_lsn_manual_refresh_fixed_snapshots_preserve_frontier() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE lsn_fixed_source (id INT PRIMARY KEY, value TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO lsn_fixed_source VALUES (1, 'baseline')")
        .await;
    let query = "SELECT id, value FROM lsn_fixed_source";
    db.create_st("lsn_fixed_stream", query, "1h", "DIFFERENTIAL")
        .await;
    #[cfg(not(feature = "light-e2e"))]
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['lsn_fixed_stream'])")
        .await;
    let frontier_sql = "SELECT frontier::text FROM pgtrickle.pgt_stream_tables \
                        WHERE pgt_name = 'lsn_fixed_stream'";
    let baseline: String = db.query_scalar(frontier_sql).await;
    db.execute("INSERT INTO lsn_fixed_source VALUES (2, 'pending')")
        .await;
    for isolation in ["REPEATABLE READ", "SERIALIZABLE"] {
        let mut caller = db.pool.begin().await.expect("begin fixed-snapshot caller");
        sqlx::query(AssertSqlSafe(format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation}"
        )))
        .execute(&mut *caller)
        .await
        .expect("set fixed-snapshot isolation");
        let error = sqlx::query("SELECT pgtrickle.refresh_stream_table('lsn_fixed_stream')")
            .execute(&mut *caller)
            .await
            .expect_err("a fixed snapshot cannot prove late-commit visibility")
            .to_string();
        assert!(error.contains("READ COMMITTED"), "{error}");
        caller.rollback().await.expect("rollback rejected refresh");
        assert_eq!(db.query_scalar::<String>(frontier_sql).await, baseline);
        db.assert_st_matches_query("lsn_fixed_stream", &format!("{query} WHERE id = 1"))
            .await;
    }
    db.refresh_st("lsn_fixed_stream").await;
    db.assert_st_matches_query("lsn_fixed_stream", query).await;
}

#[tokio::test]
async fn test_lsn_integrated_callers_preserve_valid_and_invalid_frontiers() {
    let db = E2eDb::new().await.with_extension().await;
    let query = "SELECT id, value FROM lsn_contract_source";
    db.execute("CREATE TABLE lsn_contract_source (id INT PRIMARY KEY, value TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO lsn_contract_source VALUES (1, 'first')")
        .await;
    db.create_st("lsn_contract_stream", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st("lsn_contract_stream").await;

    let pg_numeric_semantics: bool = db
        .query_scalar(
            "SELECT '0/A'::pg_lsn = '0/0000000A'::pg_lsn \
             AND 'F/FFFFFFFF'::pg_lsn < '10/0'::pg_lsn",
        )
        .await;
    assert!(
        pg_numeric_semantics,
        "PostgreSQL pg_lsn is the runtime oracle"
    );

    // Persist an unpadded spelling of a real committed PostgreSQL LSN, as used
    // by predecessor frontiers, rather than inventing a database LSN value.
    let committed_lsn: String = db.query_scalar("SELECT pg_current_wal_lsn()::text").await;
    let predecessor_spelling: String = sqlx::query_scalar(
        "SELECT split_part($1, '/', 1) || '/' || \
         COALESCE(NULLIF(ltrim(split_part($1, '/', 2), '0'), ''), '0')",
    )
    .bind(&committed_lsn)
    .fetch_one(&db.pool)
    .await
    .expect("predecessor LSN spelling should be derived from committed WAL");
    let predecessor_matches: bool = sqlx::query_scalar("SELECT $1::pg_lsn = $2::pg_lsn")
        .bind(&predecessor_spelling)
        .bind(&committed_lsn)
        .fetch_one(&db.pool)
        .await
        .expect("PostgreSQL should compare predecessor LSN numerically");
    assert!(
        predecessor_matches,
        "predecessor spelling must preserve position"
    );
    set_source_lsn(&db, "lsn_contract_stream", &predecessor_spelling).await;
    db.execute("INSERT INTO lsn_contract_source VALUES (2, 'second')")
        .await;
    db.refresh_st("lsn_contract_stream").await;
    db.assert_st_matches_query("lsn_contract_stream", query)
        .await;

    let rows_before: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM lsn_contract_stream")
        .await;
    set_source_lsn(&db, "lsn_contract_stream", "not-an-lsn").await;
    let recovery = sqlx::query_scalar::<_, String>("SELECT pgtrickle.validate_recovery()")
        .fetch_one(&db.pool)
        .await;
    assert!(
        recovery.is_err(),
        "public recovery validation must reject a malformed persisted LSN"
    );
    assert!(
        db.try_execute("SELECT pgtrickle.refresh_stream_table('lsn_contract_stream')")
            .await
            .is_err(),
        "malformed durable frontier must reject refresh"
    );
    let rows_after: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM lsn_contract_stream")
        .await;
    assert_eq!(
        rows_after, rows_before,
        "failed refresh must preserve public rows"
    );
    let stored_lsn: String = db
        .query_scalar(
            "SELECT frontier->'sources'->(d.source_relid::text)->>'lsn' \
             FROM pgtrickle.pgt_stream_tables st \
             JOIN pgtrickle.pgt_dependencies d USING (pgt_id) \
             WHERE st.pgt_name = 'lsn_contract_stream' LIMIT 1",
        )
        .await;
    assert_eq!(
        stored_lsn, "not-an-lsn",
        "failed refresh must not reset progress"
    );
}

#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_lsn_scheduler_preserves_malformed_frontier_and_recovers() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    for (setting, value, expected) in [
        ("pg_trickle.scheduler_interval_ms", "100", "100"),
        ("pg_trickle.min_schedule_seconds", "1", "1"),
        ("pg_trickle.auto_backoff", "off", "off"),
        ("pg_trickle.max_consecutive_errors", "20", "20"),
        ("pg_trickle.cdc_mode", "'trigger'", "trigger"),
    ] {
        db.alter_system_set_and_wait(setting, value, expected).await;
    }
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler must run the malformed-frontier case"
    );

    db.execute("CREATE TABLE lsn_scheduled_source (id INT PRIMARY KEY, value TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO lsn_scheduled_source VALUES (1, 'committed')")
        .await;
    db.create_st(
        "lsn_scheduled_stream",
        "SELECT id, value FROM lsn_scheduled_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query(
        "lsn_scheduled_stream",
        "SELECT id, value FROM lsn_scheduled_source",
    )
    .await;

    let (source_oid, valid_lsn): (i64, String) = sqlx::query_as(
        "SELECT d.source_relid::bigint, \
                st.frontier->'sources'->(d.source_relid::text)->>'lsn' \
         FROM pgtrickle.pgt_stream_tables st \
         JOIN pgtrickle.pgt_dependencies d USING (pgt_id) \
         WHERE st.pgt_name = 'lsn_scheduled_stream' LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read valid persisted frontier before corruption");
    set_source_lsn(&db, "lsn_scheduled_stream", "not-an-lsn").await;
    db.execute("INSERT INTO lsn_scheduled_source VALUES (2, 'held in buffer')")
        .await;
    let buffer_table = db.change_buffer_table(source_oid).await;
    let buffered_before: i64 = db
        .query_scalar(&format!("SELECT count(*)::bigint FROM {buffer_table}"))
        .await;
    assert!(
        buffered_before > 0,
        "committed row must be present in CDC buffer"
    );

    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET last_refresh_at = now() - interval '2 hours' \
         WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                         WHERE pgt_name = 'lsn_scheduled_stream')",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let last_refresh_stale: bool = sqlx::query_scalar(
        "SELECT last_refresh_at < now() - interval '1 hour' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'lsn_scheduled_stream'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read scheduler progress timestamp");
    assert!(
        last_refresh_stale,
        "scheduler must not record progress for a malformed persisted LSN"
    );

    let rows_after_failure: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM lsn_scheduled_stream")
        .await;
    assert_eq!(
        rows_after_failure, 1,
        "failed refresh must preserve public rows"
    );
    let malformed_lsn: String = db
        .query_scalar(
            "SELECT frontier->'sources'->(d.source_relid::text)->>'lsn' \
             FROM pgtrickle.pgt_stream_tables st \
             JOIN pgtrickle.pgt_dependencies d USING (pgt_id) \
             WHERE st.pgt_name = 'lsn_scheduled_stream' LIMIT 1",
        )
        .await;
    assert_eq!(
        malformed_lsn, "not-an-lsn",
        "failure must not reset progress"
    );
    let buffered_after: i64 = db
        .query_scalar(&format!("SELECT count(*)::bigint FROM {buffer_table}"))
        .await;
    assert_eq!(
        buffered_after, buffered_before,
        "failed refresh must retain CDC rows"
    );

    set_source_lsn(&db, "lsn_scheduled_stream", &valid_lsn).await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET last_refresh_at = now() - interval '2 hours' \
         WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                         WHERE pgt_name = 'lsn_scheduled_stream')",
    )
    .await;
    assert!(
        db.wait_for_condition(
            "valid frontier recovery",
            "SELECT count(*) = 2 FROM lsn_scheduled_stream",
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .await,
        "scheduler must recover and commit progress after restoring a valid frontier"
    );
    db.assert_st_matches_query(
        "lsn_scheduled_stream",
        "SELECT id, value FROM lsn_scheduled_source",
    )
    .await;
}

#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_lsn_scheduler_cdc_holdback_recovers_after_long_writer() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    for (setting, value, expected) in [
        ("pg_trickle.scheduler_interval_ms", "100", "100"),
        ("pg_trickle.min_schedule_seconds", "1", "1"),
        ("pg_trickle.auto_backoff", "off", "off"),
        ("pg_trickle.cdc_mode", "'trigger'", "trigger"),
    ] {
        db.alter_system_set_and_wait(setting, value, expected).await;
    }
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler must run the frontier holdback workflow"
    );

    db.execute("CREATE TABLE lsn_holdback_source (id INT PRIMARY KEY, value TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO lsn_holdback_source VALUES (1, 'committed')")
        .await;
    db.create_st(
        "lsn_holdback_stream",
        "SELECT id, value FROM lsn_holdback_source",
        "1s",
        "DIFFERENTIAL",
    )
    .await;
    assert!(
        db.wait_for_condition(
            "initial stream refresh",
            "SELECT EXISTS(SELECT 1 FROM lsn_holdback_stream WHERE id = 1)",
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .await,
        "initial row must be visible before the writer holdback case"
    );

    let mut writer = db.pool.acquire().await.expect("acquire writer connection");
    sqlx::query("BEGIN")
        .execute(&mut *writer)
        .await
        .expect("begin long-running writer");
    sqlx::query("INSERT INTO lsn_holdback_source VALUES (2, 'held')")
        .execute(&mut *writer)
        .await
        .expect("write row while transaction remains open");
    let writer_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(&mut *writer)
        .await
        .expect("read writer WAL position");

    tokio::time::sleep(Duration::from_millis(500)).await;
    let held_rows: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM lsn_holdback_stream")
        .await;
    assert_eq!(held_rows, 1, "uncommitted source row must remain invisible");
    let frontier_not_past_writer: bool = sqlx::query_scalar(
        "SELECT (st.frontier->'sources'->(d.source_relid::text)->>'lsn')::pg_lsn \
                <= $1::pg_lsn \
         FROM pgtrickle.pgt_stream_tables st \
         JOIN pgtrickle.pgt_dependencies d USING (pgt_id) \
         WHERE st.pgt_name = 'lsn_holdback_stream' LIMIT 1",
    )
    .bind(&writer_lsn)
    .fetch_one(&db.pool)
    .await
    .expect("read scheduler frontier against PostgreSQL numeric ordering");
    assert!(
        frontier_not_past_writer,
        "scheduler frontier must not pass the still-open writer's WAL position"
    );

    sqlx::query("COMMIT")
        .execute(&mut *writer)
        .await
        .expect("commit held source row");
    drop(writer);
    assert!(
        db.wait_for_condition(
            "held row recovery",
            "SELECT count(*) = 2 FROM lsn_holdback_stream",
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .await,
        "scheduler must recover the committed row after the writer finishes"
    );
    db.assert_st_matches_query(
        "lsn_holdback_stream",
        "SELECT id, value FROM lsn_holdback_source",
    )
    .await;
}

#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_lsn_wal_transition_rejects_malformed_and_preserves_results() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    for (setting, value, expected) in [
        ("pg_trickle.scheduler_interval_ms", "100", "100"),
        ("pg_trickle.min_schedule_seconds", "1", "1"),
        ("pg_trickle.auto_backoff", "off", "off"),
        ("pg_trickle.cdc_mode", "'auto'", "auto"),
    ] {
        db.alter_system_set_and_wait(setting, value, expected).await;
    }
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler must run the transition workflow"
    );

    db.execute("CREATE TABLE lsn_transition_source (id INT PRIMARY KEY, value TEXT NOT NULL)")
        .await;
    db.execute("ALTER TABLE lsn_transition_source REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO lsn_transition_source VALUES (1, 'committed')")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'lsn_transition_stream',\
            query => $$SELECT id, value FROM lsn_transition_source$$,\
            schedule => '1s',\
            refresh_mode => 'DIFFERENTIAL',\
            cdc_mode => 'wal'\
        )",
    )
    .await;

    let reached_wal = db
        .wait_for_condition(
            "initial WAL transition",
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_dependencies \
             WHERE source_relid = 'lsn_transition_source'::regclass \
               AND cdc_mode = 'WAL')",
            Duration::from_secs(90),
            Duration::from_millis(50),
        )
        .await;
    assert!(reached_wal, "source must reach WAL mode");
    let receipts_settled = db
        .wait_for_condition(
            "initial WAL receipts acknowledged",
            "SELECT NOT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_receipts \
             WHERE source_relid = 'lsn_transition_source'::regclass \
               AND status <> 'ACKNOWLEDGED')",
            Duration::from_secs(90),
            Duration::from_millis(50),
        )
        .await;
    assert!(
        receipts_settled,
        "WAL buffers must settle before the boundary check"
    );

    db.assert_st_matches_query(
        "lsn_transition_stream",
        "SELECT id, value FROM lsn_transition_source",
    )
    .await;
    let before: (String, Option<String>) = sqlx::query_as(
        "SELECT d.cdc_mode, d.cutover_lsn::text \
         FROM pgtrickle.pgt_dependencies d \
         WHERE d.source_relid = 'lsn_transition_source'::regclass",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read completed transition state");
    assert_eq!(before.0, "WAL");
    assert!(
        before.1.is_none(),
        "completed transition clears its temporary cutover LSN"
    );
    let slot_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots \
             WHERE slot_name = 'pgtrickle_' || \
                   pgtrickle.source_stable_name('lsn_transition_source'::regclass::oid))",
        )
        .await;
    assert!(slot_exists, "completed WAL transition retains its slot");
    let pending_receipts: i64 = db
        .query_scalar(
            "SELECT count(*)::bigint FROM pgtrickle.pgt_wal_receipts \
             WHERE source_relid = 'lsn_transition_source'::regclass \
               AND status <> 'ACKNOWLEDGED'",
        )
        .await;
    assert_eq!(pending_receipts, 0, "transition buffers must be settled");

    assert!(
        db.try_execute(
            "UPDATE pgtrickle.pgt_dependencies \
             SET cutover_lsn = 'not-an-lsn'::pg_lsn \
             WHERE source_relid = 'lsn_transition_source'::regclass",
        )
        .await
        .is_err(),
        "PostgreSQL pg_lsn boundary must reject malformed transition input"
    );
    let after_invalid: (String, Option<String>) = sqlx::query_as(
        "SELECT d.cdc_mode, d.cutover_lsn::text \
         FROM pgtrickle.pgt_dependencies d \
         WHERE d.source_relid = 'lsn_transition_source'::regclass",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read state after malformed transition input");
    assert_eq!(
        after_invalid, before,
        "rejected input must preserve transition state"
    );
    let pending_after_invalid: i64 = db
        .query_scalar(
            "SELECT count(*)::bigint FROM pgtrickle.pgt_wal_receipts \
             WHERE source_relid = 'lsn_transition_source'::regclass \
               AND status <> 'ACKNOWLEDGED'",
        )
        .await;
    assert_eq!(pending_after_invalid, pending_receipts);
    db.assert_st_matches_query(
        "lsn_transition_stream",
        "SELECT id, value FROM lsn_transition_source",
    )
    .await;

    db.assert_st_matches_query(
        "lsn_transition_stream",
        "SELECT id, value FROM lsn_transition_source",
    )
    .await;
}
