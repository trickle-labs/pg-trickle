//! #1095: durable WAL receipt recovery, replay, and shared-source cleanup.

mod e2e;

use e2e::E2eDb;
use sqlx::{AssertSqlSafe, PgPool, Postgres, pool::PoolConnection};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(90);

struct Fixture {
    db: E2eDb,
    source: String,
    stream: String,
    journal: String,
    oid: i64,
    slot: String,
    identity: Identity,
}

#[derive(Debug, PartialEq, Eq)]
struct Identity {
    sentinel: String,
    system_identifier: String,
    database_oid: i64,
    container_id: String,
    volume_identity: String,
}

fn phase_code(phase: &str) -> i64 {
    match phase {
        "before_receipt" => 1,
        "before_replay" => 2,
        "before_ack" => 3,
        "after_slot_advance" => 4,
        _ => panic!("unknown WAL receipt phase {phase}"),
    }
}

fn lock_key(oid: i64, phase: &str) -> i64 {
    oid * 10 + phase_code(phase)
}

async fn configure_scheduler(db: &E2eDb) {
    for (setting, value, expected) in [
        ("pg_trickle.scheduler_interval_ms", "100", "100"),
        ("pg_trickle.min_schedule_seconds", "1", "1"),
        ("pg_trickle.auto_backoff", "off", "off"),
        ("pg_trickle.max_consecutive_errors", "20", "20"),
        ("pg_trickle.parallel_refresh_mode", "'off'", "off"),
    ] {
        db.alter_system_set_and_wait(setting, value, expected).await;
    }
    assert!(
        db.wait_for_scheduler(WAIT).await,
        "pg_trickle scheduler did not appear"
    );
}

async fn cdc_mode(db: &E2eDb, source: &str) -> Option<String> {
    let oid = db.table_oid(source).await;
    db.query_scalar_opt(&format!(
        "SELECT cdc_mode FROM pgtrickle.pgt_dependencies WHERE source_relid = {oid} LIMIT 1"
    ))
    .await
}

async fn wait_for_cdc_mode(db: &E2eDb, source: &str, expected: &str) {
    assert!(
        db.wait_for_condition(
            "WAL CDC mode",
            &format!(
                "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_dependencies d \
                 WHERE d.source_relid = '{source}'::regclass::oid \
                   AND d.cdc_mode = '{expected}')"
            ),
            WAIT,
            Duration::from_millis(50),
        )
        .await,
        "source {source} did not reach CDC mode {expected}, observed {:?}",
        cdc_mode(db, source).await
    );
}

async fn commit_row(db: &E2eDb, source: &str, journal: &str, id: i32, value: &str) {
    let mut tx = db.pool.begin().await.expect("begin journal transaction");
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO public.{source} (id, value) VALUES ($1, $2)"
    )))
    .bind(id)
    .bind(value)
    .execute(&mut *tx)
    .await
    .expect("source operation should commit");
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO public.{journal} (id, value) VALUES ($1, $2)"
    )))
    .bind(id)
    .bind(value)
    .execute(&mut *tx)
    .await
    .expect("independent journal operation should commit");
    tx.commit()
        .await
        .expect("journal transaction should commit");
}

async fn identity(db: &E2eDb, sentinel_table: &str, container_id: &str) -> Identity {
    identity_pool(&db.pool, sentinel_table, container_id).await
}

async fn volume_identity(container_id: &str) -> String {
    let output = tokio::process::Command::new("docker")
        .args([
            "inspect",
            "--format={{range .Mounts}}{{if eq .Destination \"/var/lib/postgresql\"}}{{.Name}}:{{.Source}}{{end}}{{end}}",
            container_id,
        ])
        .output()
        .await
        .expect("inspect PostgreSQL data volume");
    assert!(output.status.success(), "Docker volume inspection failed");
    let identity = String::from_utf8(output.stdout)
        .expect("Docker volume identity should be UTF-8")
        .trim()
        .to_string();
    assert!(
        !identity.is_empty(),
        "dedicated PostgreSQL container must have a data volume"
    );
    identity
}

async fn identity_pool(pool: &PgPool, sentinel_table: &str, container_id: &str) -> Identity {
    let sentinel: String = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT value FROM public.{sentinel_table} WHERE id = 1"
    )))
    .fetch_one(pool)
    .await
    .expect("recovery sentinel should exist");
    let system_identifier: String =
        sqlx::query_scalar("SELECT system_identifier::text FROM pg_control_system()")
            .fetch_one(pool)
            .await
            .expect("PostgreSQL system identifier should be readable");
    let database_oid: i64 = sqlx::query_scalar(
        "SELECT oid::bigint FROM pg_database WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await
    .expect("database identity should be readable");
    Identity {
        sentinel,
        system_identifier,
        database_oid,
        container_id: container_id.to_string(),
        volume_identity: volume_identity(container_id).await,
    }
}

async fn setup_wal_fixture(case_name: &str) -> Fixture {
    let stem = case_name.replace('-', "_");
    let db = E2eDb::new_dedicated().await.with_extension().await;
    configure_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'auto'", "auto")
        .await;

    let source = format!("wal_{stem}_source");
    let stream = format!("wal_{stem}_stream");
    let journal = format!("wal_{stem}_journal");
    let sentinel_table = format!("wal_{stem}_identity");
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!(
        "ALTER TABLE public.{source} REPLICA IDENTITY FULL"
    ))
    .await;
    db.execute(&format!(
        "CREATE TABLE public.{journal} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!(
        "CREATE TABLE public.{sentinel_table} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!(
        "INSERT INTO public.{sentinel_table} VALUES (1, 'retained-{stem}')"
    ))
    .await;
    commit_row(&db, &source, &journal, 1, "baseline").await;

    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table(\
            name => '{stream}',\
            query => $$SELECT id, value FROM public.{source}$$,\
            schedule => '1s',\
            refresh_mode => 'DIFFERENTIAL',\
            cdc_mode => 'wal'\
        )"
    ))
    .await;
    wait_for_cdc_mode(&db, &source, "WAL").await;
    assert!(
        db.wait_for_condition(
            "initial WAL receipts acknowledged",
            &format!(
                "SELECT NOT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_receipts \
                 WHERE source_relid = '{source}'::regclass::oid \
                   AND status <> 'ACKNOWLEDGED')"
            ),
            WAIT,
            Duration::from_millis(50),
        )
        .await,
        "initial WAL receipts did not settle"
    );
    db.assert_st_matches_query(&stream, &format!("SELECT id, value FROM public.{journal}"))
        .await;

    let oid = i64::from(db.table_oid(&source).await);
    let stable_name: String = db
        .query_scalar(&format!("SELECT pgtrickle.source_stable_name({oid}::oid)"))
        .await;
    let slot = format!("pgtrickle_{stable_name}");
    let identity = identity(&db, &sentinel_table, db.container_id()).await;
    eprintln!(
        "[Q1095] case={case_name} source_oid={oid} capture=WAL slot={slot} system_identifier={} volume={}",
        identity.system_identifier, identity.volume_identity
    );
    Fixture {
        db,
        source,
        stream,
        journal,
        oid,
        slot,
        identity,
    }
}

async fn set_wal_phase(db: &E2eDb, oid: i64, phase: &str, value: &str) {
    db.alter_system_set_and_wait("pg_trickle.test_chaos_phase", &format!("'{phase}'"), phase)
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_for_table",
        &format!("'wal:{oid}:{value}'"),
        &format!("wal:{oid}:{value}"),
    )
    .await;
}

async fn clear_wal_phase(db: &E2eDb) {
    db.alter_system_set_and_wait("pg_trickle.test_chaos_for_table", "''", "")
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_phase",
        "'before_apply'",
        "before_apply",
    )
    .await;
}

async fn disable_scheduler(db: &E2eDb) {
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
}

async fn hold_wal_barrier(db: &E2eDb, oid: i64, phase: &str) -> (PoolConnection<Postgres>, i32) {
    let mut connection = db.pool.acquire().await.expect("acquire WAL barrier");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .expect("get WAL barrier PID");
    sqlx::query("SELECT pg_advisory_lock($1::bigint)")
        .bind(lock_key(oid, phase))
        .execute(&mut *connection)
        .await
        .expect("hold WAL receipt barrier");
    (connection, pid)
}

async fn release_wal_barrier(connection: &mut PoolConnection<Postgres>, oid: i64, phase: &str) {
    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1::bigint)")
        .bind(lock_key(oid, phase))
        .fetch_one(&mut **connection)
        .await
        .expect("release WAL receipt barrier");
    assert!(released, "WAL barrier connection must own the lock");
}

async fn wait_for_wal_barrier(db: &E2eDb, oid: i64, phase: &str, blocker_pid: i32) -> i32 {
    let pattern = format!("pgt_wal_receipt:{oid}:{phase}%");
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let witness: Option<(i32, String)> = sqlx::query_as(
            "SELECT a.pid, a.application_name
               FROM pg_stat_activity a
              WHERE a.application_name LIKE $1
                AND a.wait_event_type = 'Lock'
                AND $2 = ANY(pg_blocking_pids(a.pid))",
        )
        .bind(&pattern)
        .bind(blocker_pid)
        .fetch_optional(&db.pool)
        .await
        .expect("inspect WAL receipt barrier");
        if let Some((pid, application_name)) = witness {
            assert!(application_name.starts_with(&pattern[..pattern.len() - 1]));
            return pid;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("WAL decoder did not reach {phase} for source OID {oid}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn terminate_backend(db: &E2eDb, pid: i32) {
    let terminated: bool = db
        .query_scalar(&format!("SELECT pg_terminate_backend({pid})"))
        .await;
    assert!(terminated, "WAL worker backend {pid} was not terminated");
}

async fn restart_and_check_named(fixture: &Fixture, sentinel_table: &str) -> PgPool {
    fixture.db.pool.close().await;
    let output = tokio::process::Command::new("docker")
        .args(["restart", fixture.db.container_id()])
        .output()
        .await
        .expect("docker restart must run");
    assert!(output.status.success(), "docker restart failed");
    let pool = fixture.db.reconnect_after_restart().await;
    let after = identity_pool(&pool, sentinel_table, fixture.db.container_id()).await;
    assert_eq!(after.container_id, fixture.identity.container_id);
    assert_eq!(after.system_identifier, fixture.identity.system_identifier);
    assert_eq!(after.database_oid, fixture.identity.database_oid);
    assert_eq!(after.sentinel, fixture.identity.sentinel);
    assert_eq!(after.volume_identity, fixture.identity.volume_identity);
    pool
}

async fn exact_diff(pool: &PgPool, stream: &str, journal: &str) -> i64 {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*)::bigint FROM (\
            SELECT id, value FROM (\
                SELECT id, value FROM public.{stream} \
                EXCEPT ALL \
                SELECT id, value FROM public.{journal}\
            ) AS missing \
            UNION ALL \
            SELECT id, value FROM (\
                SELECT id, value FROM public.{journal} \
                EXCEPT ALL \
                SELECT id, value FROM public.{stream}\
            ) AS extra\
        ) AS differences"
    )))
    .fetch_one(pool)
    .await
    .expect("compare stream output with journal")
}

async fn wait_for_exact(pool: &PgPool, stream: &str, journal: &str) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if exact_diff(pool, stream, journal).await == 0 {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("stream {stream} did not converge to independent journal {journal}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn matching_receipts(db: &E2eDb, oid: i64, status: &str, value: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_wal_receipts
          WHERE source_relid = $1 AND status = $2 AND data LIKE $3",
    )
    .bind(oid)
    .bind(status)
    .bind(format!("%{value}%"))
    .fetch_one(&db.pool)
    .await
    .expect("inspect WAL receipts")
}

async fn matching_receipt_lsn(db: &E2eDb, oid: i64, status: &str, value: &str) -> String {
    sqlx::query_scalar(
        "SELECT lsn::text FROM pgtrickle.pgt_wal_receipts
          WHERE source_relid = $1 AND status = $2 AND data LIKE $3
          ORDER BY lsn LIMIT 1",
    )
    .bind(oid)
    .bind(status)
    .bind(format!("%{value}%"))
    .fetch_one(&db.pool)
    .await
    .expect("inspect WAL receipt LSN")
}

async fn wait_for_receipt(db: &E2eDb, oid: i64, status: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if matching_receipts(db, oid, status, value).await > 0 {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            let state: (i64, i64, i64) = sqlx::query_as(
                "SELECT count(*) FILTER (WHERE status = 'RECEIVED'), \
                        count(*) FILTER (WHERE status = 'APPLIED'), \
                        count(*) FILTER (WHERE status = 'ACKNOWLEDGED') \
                   FROM pgtrickle.pgt_wal_receipts WHERE source_relid = $1",
            )
            .bind(oid)
            .fetch_one(&db.pool)
            .await
            .expect("inspect WAL receipt counts");
            panic!(
                "no {status} WAL receipt for {value}; received={} applied={} acknowledged={}",
                state.0, state.1, state.2
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn discard_buffered_source_changes(fixture: &Fixture) {
    let buffer = fixture.db.change_buffer_table(fixture.oid).await;
    fixture
        .db
        .execute(&format!("DELETE FROM {buffer} WHERE action <> 'S'"))
        .await;
}

async fn consume_slot_copy(fixture: &Fixture) {
    let _: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pg_logical_slot_get_changes($1, NULL, NULL)",
    )
    .bind(&fixture.slot)
    .fetch_one(&fixture.db.pool)
    .await
    .expect("consume the sole WAL recovery copy");
}

async fn slot_reached(db: &E2eDb, slot: &str, lsn: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS(
                 SELECT 1 FROM pg_replication_slots
                  WHERE slot_name = $1 AND confirmed_flush_lsn >= $2::pg_lsn
             )",
    )
    .bind(slot)
    .bind(lsn)
    .fetch_one(&db.pool)
    .await
    .expect("inspect WAL slot boundary")
}

async fn receipt_slot_reached(db: &E2eDb, slot: &str, oid: i64, value: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS(
            SELECT 1
              FROM pg_replication_slots s
              JOIN pgtrickle.pgt_wal_receipts r ON r.slot_name = s.slot_name
             WHERE s.slot_name = $1 AND r.source_relid = $2
               AND r.data LIKE $3 AND s.confirmed_flush_lsn >= r.lsn
        )",
    )
    .bind(slot)
    .bind(oid)
    .bind(format!("%{value}%"))
    .fetch_one(&db.pool)
    .await
    .expect("inspect WAL slot acknowledgement")
}

async fn run_interruption_case(phase: &str, status: &str, restart_twice: bool) {
    let fixture = setup_wal_fixture(phase).await;
    let sentinel_table = format!("wal_{}_identity", phase.replace('-', "_"));
    let value = format!("{phase}-committed");
    let (mut barrier, blocker_pid) = hold_wal_barrier(&fixture.db, fixture.oid, phase).await;
    set_wal_phase(&fixture.db, fixture.oid, phase, &value).await;
    commit_row(&fixture.db, &fixture.source, &fixture.journal, 2, &value).await;
    let worker_pid = wait_for_wal_barrier(&fixture.db, fixture.oid, phase, blocker_pid).await;

    if phase == "before_receipt" {
        assert_eq!(
            matching_receipts(&fixture.db, fixture.oid, "RECEIVED", &value).await,
            0
        );
    } else {
        wait_for_receipt(&fixture.db, fixture.oid, status, &value).await;
        if phase == "after_slot_advance" {
            assert!(receipt_slot_reached(&fixture.db, &fixture.slot, fixture.oid, &value).await);
        }
    }
    terminate_backend(&fixture.db, worker_pid).await;
    release_wal_barrier(&mut barrier, fixture.oid, phase).await;
    drop(barrier);
    clear_wal_phase(&fixture.db).await;

    let pool = restart_and_check_named(&fixture, &sentinel_table).await;
    wait_for_exact(&pool, &fixture.stream, &fixture.journal).await;
    let acknowledged: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_wal_receipts
          WHERE source_relid = $1 AND status = 'ACKNOWLEDGED' AND data LIKE $2",
    )
    .bind(fixture.oid)
    .bind(format!("%{value}%"))
    .fetch_one(&pool)
    .await
    .expect("inspect recovered receipt state");
    assert!(acknowledged > 0, "recovery must acknowledge {value}");

    if restart_twice {
        pool.close().await;
        let output = tokio::process::Command::new("docker")
            .args(["restart", fixture.db.container_id()])
            .output()
            .await
            .expect("second docker restart must run");
        assert!(output.status.success(), "second docker restart failed");
        let pool = fixture.db.reconnect_after_restart().await;
        let after = identity_pool(&pool, &sentinel_table, fixture.db.container_id()).await;
        assert_eq!(after, fixture.identity);
        wait_for_exact(&pool, &fixture.stream, &fixture.journal).await;
    }
}

#[tokio::test]
async fn test_wal_recovery_before_receipt_commit_preserves_committed_changes() {
    run_interruption_case("before_receipt", "RECEIVED", false).await;
}

#[tokio::test]
async fn test_wal_recovery_after_receipt_commit_before_ack_converges_once() {
    run_interruption_case("before_replay", "RECEIVED", false).await;
}

#[tokio::test]
async fn test_wal_recovery_after_apply_before_ack_does_not_reapply() {
    run_interruption_case("before_ack", "APPLIED", true).await;
}

#[tokio::test]
async fn test_wal_recovery_after_slot_advance_finishes_acknowledgement() {
    run_interruption_case("after_slot_advance", "APPLIED", false).await;
}

#[tokio::test]
async fn test_trigger_to_wal_handoff_preserves_pending_commits() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    configure_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;
    let source = "wal_handoff_source";
    let stream = "wal_handoff_stream";
    let journal = "wal_handoff_journal";
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!(
        "ALTER TABLE public.{source} REPLICA IDENTITY FULL"
    ))
    .await;
    db.execute(&format!(
        "CREATE TABLE public.{journal} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    commit_row(&db, source, journal, 1, "baseline").await;
    db.create_st(
        stream,
        &format!("SELECT id, value FROM public.{source}"),
        "1s",
        "DIFFERENTIAL",
    )
    .await;
    db.assert_st_matches_query(stream, &format!("SELECT id, value FROM public.{journal}"))
        .await;
    commit_row(&db, source, journal, 2, "pending-trigger").await;
    let oid = i64::from(db.table_oid(source).await);
    let buffer = db.change_buffer_table(oid).await;
    assert!(
        db.query_scalar::<i64>(&format!(
            "SELECT count(*) FROM {buffer} WHERE action <> 'S'"
        ))
        .await
            > 0,
        "trigger capture must retain the pending handoff batch"
    );
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'auto'", "auto")
        .await;
    wait_for_cdc_mode(&db, source, "WAL").await;
    let stable_name: String = db
        .query_scalar(&format!("SELECT pgtrickle.source_stable_name({oid}::oid)"))
        .await;
    let slot = format!("pgtrickle_{stable_name}");
    commit_row(&db, source, journal, 3, "after-wal").await;
    wait_for_receipt(&db, oid, "ACKNOWLEDGED", "after-wal").await;
    wait_for_exact(&db.pool, stream, journal).await;
    db.assert_st_matches_query(stream, &format!("SELECT id, value FROM public.{journal}"))
        .await;
    assert!(
        db.query_scalar::<bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
        ))
        .await,
        "handoff must retain the WAL slot {slot}"
    );
    assert!(
        matching_receipts(&db, oid, "ACKNOWLEDGED", "after-wal").await > 0,
        "the post-handoff commit must have an acknowledged WAL receipt"
    );
    eprintln!("[Q1095] trigger_to_wal source_oid={oid} slot={slot} capture=WAL");
}

async fn cleanup_fixture() -> (E2eDb, String, String, String, i64, String) {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    configure_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;
    let source = "cleanup_shared_source";
    let slow = "cleanup_slow_stream";
    let fast = "cleanup_fast_stream";
    let journal = "cleanup_journal";
    db.execute(&format!(
        "CREATE TABLE public.{source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!(
        "CREATE TABLE public.{journal} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    commit_row(&db, source, journal, 1, "baseline").await;
    db.create_st(
        slow,
        &format!("SELECT id, value FROM public.{source}"),
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        fast,
        &format!("SELECT id, value FROM public.{source}"),
        "1h",
        "FULL",
    )
    .await;
    db.execute(&format!(
        "CREATE TABLE public.cleanup_slow_checkpoint AS TABLE public.{slow}"
    ))
    .await;
    commit_row(&db, source, journal, 2, "shared-change").await;
    let oid = i64::from(db.table_oid(source).await);
    let buffer = db.change_buffer_table(oid).await;
    (
        db,
        source.to_string(),
        slow.to_string(),
        fast.to_string(),
        oid,
        buffer,
    )
}

async fn frontier(db: &E2eDb, stream: &str, oid: i64) -> String {
    db.query_scalar(&format!(
        "SELECT frontier->'sources'->'{oid}'->>'lsn' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{stream}'"
    ))
    .await
}

async fn run_shared_source_cleanup_case() {
    let (db, _source, slow, fast, oid, buffer) = cleanup_fixture().await;
    db.refresh_st(&fast).await;
    db.assert_st_matches_query(&fast, "SELECT id, value FROM public.cleanup_journal")
        .await;
    db.assert_st_matches_query(
        &slow,
        "SELECT id, value FROM public.cleanup_slow_checkpoint",
    )
    .await;
    let slow_x = frontier(&db, &slow, oid).await;
    let fast_y = frontier(&db, &fast, oid).await;
    assert!(
        db.query_scalar::<bool>(&format!("SELECT '{fast_y}'::pg_lsn > '{slow_x}'::pg_lsn"))
            .await,
        "fast consumer must be ahead of slow consumer"
    );
    let needed_rows: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {buffer} WHERE action <> 'S' AND lsn > '{slow_x}'::pg_lsn"
        ))
        .await;
    assert!(
        needed_rows > 0,
        "cleanup must retain rows above slow frontier X"
    );
    db.refresh_st(&slow).await;
    db.assert_st_matches_query(&slow, "SELECT id, value FROM public.cleanup_journal")
        .await;
    db.refresh_st(&fast).await;
    db.assert_st_matches_query(&slow, "SELECT id, value FROM public.cleanup_journal")
        .await;
    let remaining: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {buffer} WHERE action <> 'S' AND lsn > '{fast_y}'::pg_lsn"
        ))
        .await;
    assert_eq!(
        remaining, 0,
        "cleanup may reclaim rows through Y after catch-up"
    );
}

#[tokio::test]
async fn test_shared_source_cleanup_retains_slow_consumer_changes() {
    run_shared_source_cleanup_case().await;
}

#[tokio::test]
async fn test_shared_source_cleanup_catchup_remains_exact() {
    run_shared_source_cleanup_case().await;
}

async fn assert_mismatch(pool: &PgPool, stream: &str, journal: &str) {
    assert!(
        exact_diff(pool, stream, journal).await > 0,
        "semantic control must detect a public result mismatch"
    );
}

#[tokio::test]
async fn test_wal_recovery_semantic_controls_are_detected() {
    let clean = setup_wal_fixture("control_clean_replay").await;
    run_clean_replay_control(clean).await;

    let early = setup_wal_fixture("control_early_ack").await;
    run_loss_control(early, "early_acknowledgement").await;

    let dropped = setup_wal_fixture("control_dropped_receipt").await;
    run_dropped_receipt_control(dropped).await;

    let duplicate = setup_wal_fixture("control_duplicate_apply").await;
    commit_row(
        &duplicate.db,
        &duplicate.source,
        &duplicate.journal,
        2,
        "duplicate-control",
    )
    .await;
    let pool = duplicate.db.pool.clone();
    wait_for_exact(&pool, &duplicate.stream, &duplicate.journal).await;
    let duplicate_relation = "public.wal_control_duplicate_actual";
    duplicate
        .db
        .execute(&format!(
            "CREATE TABLE {duplicate_relation} AS \
             SELECT id, value FROM public.{} \
             UNION ALL SELECT id, value FROM public.{}",
            duplicate.stream, duplicate.stream
        ))
        .await;
    assert_mismatch(&pool, "wal_control_duplicate_actual", &duplicate.journal).await;
    eprintln!("[Q1095] control=duplicate_application detected=true");

    let (cleanup_db, _source, slow, fast, oid, buffer) = cleanup_fixture().await;
    cleanup_db.refresh_st(&fast).await;
    let slow_x = frontier(&cleanup_db, &slow, oid).await;
    cleanup_db
        .execute(&format!(
            "DELETE FROM {buffer} WHERE action <> 'S' AND lsn > '{slow_x}'::pg_lsn"
        ))
        .await;
    cleanup_db.refresh_st(&slow).await;
    assert_mismatch(&cleanup_db.pool, &slow, "cleanup_journal").await;
    eprintln!("[Q1095] control=premature_cleanup detected=true");
}

async fn run_loss_control(fixture: Fixture, label: &str) {
    let value = "control-loss";
    let (mut barrier, blocker_pid) =
        hold_wal_barrier(&fixture.db, fixture.oid, "before_replay").await;
    set_wal_phase(&fixture.db, fixture.oid, "before_replay", value).await;
    commit_row(&fixture.db, &fixture.source, &fixture.journal, 2, value).await;
    let worker_pid =
        wait_for_wal_barrier(&fixture.db, fixture.oid, "before_replay", blocker_pid).await;
    wait_for_receipt(&fixture.db, fixture.oid, "RECEIVED", value).await;
    let receipt_lsn = matching_receipt_lsn(&fixture.db, fixture.oid, "RECEIVED", value).await;
    fixture
        .db
        .execute(&format!(
            "UPDATE pgtrickle.pgt_wal_receipts SET status = 'ACKNOWLEDGED' \
             WHERE source_relid = {} AND data LIKE '%{value}%'",
            fixture.oid
        ))
        .await;
    discard_buffered_source_changes(&fixture).await;
    consume_slot_copy(&fixture).await;
    assert_mismatch(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
    disable_scheduler(&fixture.db).await;
    terminate_backend(&fixture.db, worker_pid).await;
    release_wal_barrier(&mut barrier, fixture.oid, "before_replay").await;
    drop(barrier);
    clear_wal_phase(&fixture.db).await;
    assert!(
        slot_reached(&fixture.db, &fixture.slot, &receipt_lsn).await,
        "early acknowledgement must advance the slot past the receipt"
    );
    eprintln!("[Q1095] control={label} detected=true");
}

async fn run_dropped_receipt_control(fixture: Fixture) {
    let value = "control-drop";
    let (mut barrier, blocker_pid) =
        hold_wal_barrier(&fixture.db, fixture.oid, "before_replay").await;
    set_wal_phase(&fixture.db, fixture.oid, "before_replay", value).await;
    commit_row(&fixture.db, &fixture.source, &fixture.journal, 2, value).await;
    let worker_pid =
        wait_for_wal_barrier(&fixture.db, fixture.oid, "before_replay", blocker_pid).await;
    wait_for_receipt(&fixture.db, fixture.oid, "RECEIVED", value).await;
    let receipt_lsn = matching_receipt_lsn(&fixture.db, fixture.oid, "RECEIVED", value).await;
    fixture
        .db
        .execute(&format!(
            "DELETE FROM pgtrickle.pgt_wal_receipts WHERE source_relid = {} AND data LIKE '%{value}%'",
            fixture.oid
        ))
    .await;
    discard_buffered_source_changes(&fixture).await;
    consume_slot_copy(&fixture).await;
    assert_mismatch(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
    disable_scheduler(&fixture.db).await;
    terminate_backend(&fixture.db, worker_pid).await;
    release_wal_barrier(&mut barrier, fixture.oid, "before_replay").await;
    drop(barrier);
    clear_wal_phase(&fixture.db).await;
    assert!(
        slot_reached(&fixture.db, &fixture.slot, &receipt_lsn).await,
        "dropped receipt must be past the slot boundary"
    );
    let no_receipt: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS(
                 SELECT 1 FROM pgtrickle.pgt_wal_receipts
                  WHERE source_relid = $1 AND data LIKE $2
             )",
    )
    .bind(fixture.oid)
    .bind(format!("%{value}%"))
    .fetch_one(&fixture.db.pool)
    .await
    .expect("inspect dropped-receipt recovery guard");
    assert!(
        no_receipt,
        "dropped receipt must remain absent after slot acknowledgement"
    );
    eprintln!("[Q1095] control=dropped_receipt detected=true");
}

async fn run_clean_replay_control(fixture: Fixture) {
    let value = "control-clean";
    let (mut barrier, blocker_pid) =
        hold_wal_barrier(&fixture.db, fixture.oid, "before_replay").await;
    set_wal_phase(&fixture.db, fixture.oid, "before_replay", value).await;
    commit_row(&fixture.db, &fixture.source, &fixture.journal, 2, value).await;
    let worker_pid =
        wait_for_wal_barrier(&fixture.db, fixture.oid, "before_replay", blocker_pid).await;
    wait_for_receipt(&fixture.db, fixture.oid, "RECEIVED", value).await;
    terminate_backend(&fixture.db, worker_pid).await;
    release_wal_barrier(&mut barrier, fixture.oid, "before_replay").await;
    drop(barrier);
    clear_wal_phase(&fixture.db).await;
    wait_for_exact(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
}
