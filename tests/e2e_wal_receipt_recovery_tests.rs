//! #1095: durable WAL receipt recovery, replay, and shared-source cleanup.

mod e2e;
#[path = "../verification/receipt.rs"]
mod receipt_limit;

use e2e::E2eDb;
use sqlx::{AssertSqlSafe, PgPool, Postgres, pool::PoolConnection};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(90);

#[tokio::test]
async fn test_receipt_prefix_sql_control_rejects_executable_off_by_one() {
    use sha2::{Digest, Sha256};
    let digest = |bytes: &[u8]| {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };

    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    db.execute("CREATE TABLE semantic_prefix_source(id int PRIMARY KEY,value text)")
        .await;
    for slot in ["semantic_prefix_baseline", "semantic_prefix_mutant"] {
        let created: String = sqlx::query_scalar(
            "SELECT slot_name::text FROM pg_create_logical_replication_slot($1,'test_decoding')",
        )
        .bind(slot)
        .fetch_one(&db.pool)
        .await
        .expect("slot setup errors cannot qualify a semantic kill");
        assert_eq!(created, slot);
    }
    db.execute("INSERT INTO semantic_prefix_source VALUES(1,'first'),(2,'first')")
        .await;
    db.execute("INSERT INTO semantic_prefix_source VALUES(3,'later')")
        .await;
    type Message = (String, String, String);
    let peek: Vec<Message>=sqlx::query_as("SELECT lsn::text,xid::text,data FROM pg_logical_slot_peek_changes('semantic_prefix_baseline',NULL,NULL,'skip-empty-xacts','1')")
        .fetch_all(&db.pool).await.expect("peek must execute before prefix qualification");
    assert!(
        peek.iter()
            .filter(|message| message.2.starts_with("COMMIT"))
            .count()
            >= 2,
        "two committed transactions must be visible"
    );
    let end = peek
        .iter()
        .position(|message| message.2.starts_with("COMMIT"))
        .unwrap()
        + 1;
    let expected = peek[..end].to_vec();
    assert!(
        expected
            .iter()
            .any(|message| message.2.contains("semantic_prefix_source"))
    );
    let limit = receipt_limit::receipt_ack_row_limit(expected.len())
        .expect("native complete prefix must fit checked row bound");
    let sql = "SELECT lsn::text,xid::text,data FROM pg_logical_slot_get_changes($1,NULL,$2,'skip-empty-xacts','1')";
    let baseline: Vec<Message> = sqlx::query_as(sql)
        .bind("semantic_prefix_baseline")
        .bind(limit)
        .fetch_all(&db.pool)
        .await
        .expect("correct baseline SQL must execute");
    assert_eq!(
        baseline, expected,
        "checked limit must consume exactly the represented complete prefix"
    );
    let mutant_limit = limit.checked_add(1).unwrap();
    let mutant: Vec<Message> = sqlx::query_as(sql)
        .bind("semantic_prefix_mutant")
        .bind(mutant_limit)
        .fetch_all(&db.pool)
        .await
        .expect("SQL/infrastructure errors cannot count as detecting the off-by-one mutant");
    assert_ne!(
        mutant, expected,
        "executable n+1 SQL limit must fail the exact-prefix comparison"
    );
    assert!(
        mutant.len() > expected.len() && mutant[..end] == expected,
        "mutant must cross the represented prefix, not fail elsewhere"
    );
    assert!(
        mutant[end..]
            .iter()
            .any(|message| message.2.contains("semantic_prefix_source")),
        "the extra prefix must include unrepresented source changes"
    );
    let directory = std::path::Path::new(".p2p/work/issue-1218-semantic-controls/evidence");
    std::fs::create_dir_all(directory).unwrap();
    std::fs::write(directory.join("receipt-prefix-sql-control.json"),serde_json::to_vec_pretty(&serde_json::json!({
        "schema":"pg-trickle-semantic-sql-controls-v1","selected":1,"case":"incomplete-receipt-prefix",
        "expected_failure_class":"ReceiptPrefixMismatch","failure_class":"ReceiptPrefixMismatch",
        "expected_phase":"acknowledge_complete_prefix","phase":"acknowledge_complete_prefix",
        "baseline_passed":true,"mutant_executed":true,"baseline_limit":limit,"mutant_limit":mutant_limit,
        "source_path":"verification/receipt.rs","source_sha256":digest(include_bytes!("../verification/receipt.rs")),
        "test_source_sha256":digest(include_bytes!("e2e_wal_receipt_recovery_tests.rs")),
        "sql_sha256":digest(sql.as_bytes()),"expected":expected,"baseline":baseline,"mutant":mutant,
        "installed_candidate_commit":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
        "boundary":"The production checked limit feeds PostgreSQL logical-slot SQL; the mutant changes that executable SQL parameter from n to n+1. No decoder, receipt catalog, or cleanup proof is claimed."
    })).unwrap()).unwrap();
    for slot in ["semantic_prefix_baseline", "semantic_prefix_mutant"] {
        sqlx::query("SELECT pg_drop_replication_slot($1)")
            .bind(slot)
            .execute(&db.pool)
            .await
            .unwrap();
    }
}

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

fn identity_matches(expected: &Identity, actual: &Identity) -> bool {
    expected == actual
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
    let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
        .fetch_one(&mut *tx)
        .await
        .expect("record committed input transaction identity");
    tx.commit()
        .await
        .expect("journal transaction should commit");
    eprintln!("[WAL journal] source={source} journal={journal} xid={xid} id={id} value={value}");
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

async fn create_identity_sentinel(db: &E2eDb, table: &str, value: &str) {
    db.execute(&format!(
        "CREATE TABLE public.{table} (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO public.{table} (id, value) VALUES (1, $1)"
    )))
    .bind(value)
    .execute(&db.pool)
    .await
    .expect("commit recovery sentinel");
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
    let sentinel: String = db.query_scalar("SELECT gen_random_uuid()::text").await;
    create_identity_sentinel(&db, &sentinel_table, &sentinel).await;
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
        "[Q1095] case={case_name} source_oid={oid} capture=WAL slot={slot} sentinel={} system_identifier={} volume={}",
        identity.sentinel, identity.system_identifier, identity.volume_identity
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
    assert!(
        identity_matches(&fixture.identity, &after),
        "recovery must use the retained database identity"
    );
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

async fn matching_receipts(pool: &PgPool, oid: i64, status: &str, value: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_wal_receipts
          WHERE source_relid = $1 AND status = $2 AND data LIKE $3",
    )
    .bind(oid)
    .bind(status)
    .bind(format!("%{value}%"))
    .fetch_one(pool)
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

async fn wait_for_receipt(pool: &PgPool, oid: i64, status: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if matching_receipts(pool, oid, status, value).await > 0 {
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
            .fetch_one(pool)
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

#[tokio::test]
async fn test_wal_receipt_commit_order_preserves_late_old_lsn_transaction() {
    let fixture = setup_wal_fixture("commit_order").await;
    let mut late = fixture.db.pool.begin().await.unwrap();
    for table in [&fixture.source, &fixture.journal] {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO public.{table} VALUES (2, 'late-old-lsn')"
        )))
        .execute(&mut *late)
        .await
        .unwrap();
    }
    commit_row(
        &fixture.db,
        &fixture.source,
        &fixture.journal,
        3,
        "early-new-lsn",
    )
    .await;
    wait_for_receipt(
        &fixture.db.pool,
        fixture.oid,
        "ACKNOWLEDGED",
        "early-new-lsn",
    )
    .await;
    // The writer fence may hold the stream frontier while the older transaction is open.
    late.commit().await.unwrap();
    wait_for_receipt(
        &fixture.db.pool,
        fixture.oid,
        "ACKNOWLEDGED",
        "late-old-lsn",
    )
    .await;
    wait_for_exact(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
    assert_eq!(
        cdc_mode(&fixture.db, &fixture.source).await.as_deref(),
        Some("WAL")
    );
    let rows: i64 = fixture
        .db
        .query_scalar(&format!("SELECT count(*) FROM {}", fixture.stream))
        .await;
    assert_eq!(
        rows, 3,
        "late commit must survive the newer row's consumed frontier"
    );
}

#[tokio::test]
async fn test_wal_receipt_small_limits_preserve_complete_transactions() {
    let fixture = setup_wal_fixture("complete_prefix").await;
    // 52 source rows + 52 journal rows + BEGIN/COMMIT straddle the minimum GUC value (100).
    for (cycle, limit) in [100, 106, 107].into_iter().enumerate() {
        fixture
            .db
            .alter_system_set_and_wait(
                "pg_trickle.wal_max_changes_per_poll",
                &limit.to_string(),
                &limit.to_string(),
            )
            .await;
        let before: i64 = fixture
            .db
            .query_scalar("SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
            .await;
        let token = format!("complete-prefix-{cycle}");
        let mut tx = fixture.db.pool.begin().await.unwrap();
        for table in [&fixture.source, &fixture.journal] {
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO public.{table} SELECT id, $3 FROM generate_series($1::int, $2::int) id"
            )))
            .bind(2 + cycle as i32 * 52)
            .bind(53 + cycle as i32 * 52)
            .bind(&token)
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        wait_for_receipt(&fixture.db.pool, fixture.oid, "ACKNOWLEDGED", &token).await;
        wait_for_exact(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
        let mode = cdc_mode(&fixture.db, &fixture.source).await;
        if mode.as_deref() != Some("WAL") {
            let logs = tokio::process::Command::new("docker")
                .args(["logs", "--tail", "200", fixture.db.container_id()])
                .output()
                .await
                .unwrap();
            panic!(
                "limit={limit}: capture={mode:?}\n{}",
                String::from_utf8_lossy(&logs.stderr)
            );
        }
        let (messages, pending, commits): (i64, i64, i64) = sqlx::query_as(
            "SELECT count(*), count(*) FILTER (WHERE status <> 'ACKNOWLEDGED'), \
             count(*) FILTER (WHERE data LIKE 'COMMIT %') \
             FROM pgtrickle.pgt_wal_receipts WHERE source_relid = $1 AND source_xid::text = $2",
        )
        .bind(fixture.oid)
        .bind(xid)
        .fetch_one(&fixture.db.pool)
        .await
        .unwrap();
        assert_eq!((messages, pending, commits), (106, 0, 1), "limit={limit}");
        let reinitialized: bool = fixture.db.query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_refresh_history \
             WHERE refresh_id > {before} AND (action IN ('FULL', 'REINITIALIZE') OR was_full_fallback))"
        )).await;
        assert!(
            !reinitialized,
            "limit={limit}: fallback cannot qualify receipt batching"
        );
    }
}

async fn run_interruption_case(phase: &str, status: &str, restart_twice: bool) {
    let fixture = setup_wal_fixture(phase).await;
    let sentinel_table = format!("wal_{}_identity", phase.replace('-', "_"));
    let value = format!("{phase}-committed");
    let next_value = format!("{phase}-next-batch");
    let (mut barrier, blocker_pid) = hold_wal_barrier(&fixture.db, fixture.oid, phase).await;
    set_wal_phase(&fixture.db, fixture.oid, phase, &value).await;
    commit_row(&fixture.db, &fixture.source, &fixture.journal, 2, &value).await;
    let worker_pid = wait_for_wal_barrier(&fixture.db, fixture.oid, phase, blocker_pid).await;

    if phase == "before_receipt" {
        assert_eq!(
            matching_receipts(&fixture.db.pool, fixture.oid, "RECEIVED", &value).await,
            0
        );
    } else {
        wait_for_receipt(&fixture.db.pool, fixture.oid, status, &value).await;
        if phase == "after_slot_advance" {
            assert!(receipt_slot_reached(&fixture.db, &fixture.slot, fixture.oid, &value).await);
            commit_row(
                &fixture.db,
                &fixture.source,
                &fixture.journal,
                3,
                &next_value,
            )
            .await;
        }
    }
    terminate_backend(&fixture.db, worker_pid).await;
    release_wal_barrier(&mut barrier, fixture.oid, phase).await;
    drop(barrier);
    clear_wal_phase(&fixture.db).await;

    let pool = restart_and_check_named(&fixture, &sentinel_table).await;
    wait_for_exact(&pool, &fixture.stream, &fixture.journal).await;
    wait_for_receipt(&pool, fixture.oid, "ACKNOWLEDGED", &value).await;
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

    if phase == "after_slot_advance" {
        wait_for_receipt(&pool, fixture.oid, "ACKNOWLEDGED", &next_value).await;
        wait_for_exact(&pool, &fixture.stream, &fixture.journal).await;
    }

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
        assert!(identity_matches(&fixture.identity, &after));
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
async fn test_wal_recovery_identity_rejects_fresh_volume() {
    let retained = E2eDb::new_dedicated().await;
    let sentinel: String = retained
        .query_scalar("SELECT gen_random_uuid()::text")
        .await;
    create_identity_sentinel(&retained, "recovery_identity", &sentinel).await;
    let expected = identity(&retained, "recovery_identity", retained.container_id()).await;

    let replacement = E2eDb::new_dedicated().await;
    create_identity_sentinel(&replacement, "recovery_identity", &sentinel).await;
    let actual = identity(
        &replacement,
        "recovery_identity",
        replacement.container_id(),
    )
    .await;

    assert_eq!(actual.sentinel, expected.sentinel);
    assert_ne!(actual.volume_identity, expected.volume_identity);
    assert!(!identity_matches(&expected, &actual));
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
    wait_for_receipt(&db.pool, oid, "ACKNOWLEDGED", "after-wal").await;
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
        matching_receipts(&db.pool, oid, "ACKNOWLEDGED", "after-wal").await > 0,
        "the post-handoff commit must have an acknowledged WAL receipt"
    );
    eprintln!("[Q1095] trigger_to_wal source_oid={oid} slot={slot} capture=WAL");
}

async fn run_trigger_fallback_case(phase: &str) {
    let mut fixture = setup_wal_fixture(&format!("fallback_{phase}")).await;
    let db = &fixture.db;
    db.alter_system_set_and_wait(
        "pg_trickle.refresh_strategy",
        "'differential'",
        "differential",
    )
    .await;
    let shared = format!("{}_shared", fixture.stream);
    db.alter_system_set_and_wait("pg_trickle.cdc_capture_mode", "'hold'", "hold")
        .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_paused", "on", "on")
        .await;
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table(\
            name => '{shared}', query => $$SELECT id, value FROM public.{}$$,\
            schedule => '1h', refresh_mode => 'DIFFERENTIAL', cdc_mode => 'wal')",
        fixture.source
    ))
    .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_paused", "off", "off")
        .await;
    wait_for_cdc_mode(db, &fixture.source, "WAL").await;
    for stream in [&fixture.stream, &shared] {
        db.refresh_st(stream).await;
        db.execute(&format!(
            "SELECT pgtrickle.pause_scheduler(ARRAY['public.{stream}'])"
        ))
        .await;
        assert!(
            db.query_scalar::<bool>(&format!(
                "SELECT NOT needs_reinit AND frontier IS NOT NULL \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{stream}'"
            ))
            .await,
            "consumer must start with an established baseline"
        );
    }
    let history: i64 = db
        .query_scalar("SELECT coalesce(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    let buffer = db.change_buffer_table(fixture.oid).await;
    let value = format!("fallback-{phase}-pending");
    let (barrier, blocker_pid) = hold_wal_barrier(db, fixture.oid, phase).await;
    set_wal_phase(db, fixture.oid, phase, &value).await;
    commit_row(db, &fixture.source, &fixture.journal, 2, &value).await;
    wait_for_wal_barrier(db, fixture.oid, phase, blocker_pid).await;
    assert_eq!(
        db.query_scalar::<i64>(&format!(
            "SELECT count(*) FROM {buffer} WHERE action <> 'S'"
        ))
        .await,
        0,
        "pending commit must have no trigger or replay copy"
    );
    assert!(exact_diff(&db.pool, &fixture.stream, &fixture.journal).await > 0);
    if phase == "before_replay" {
        wait_for_receipt(&db.pool, fixture.oid, "RECEIVED", &value).await;
        let receipt: (String, String, String) = sqlx::query_as(
            "SELECT lsn::text, source_xid::text, data FROM pgtrickle.pgt_wal_receipts \
             WHERE source_relid = $1 AND status = 'RECEIVED' AND source_change AND data LIKE $2",
        )
        .bind(fixture.oid)
        .bind(format!("%{value}%"))
        .fetch_one(&db.pool)
        .await
        .expect("retain the real pending receipt witness");
        eprintln!(
            "[Q1159] phase={phase} source_oid={} receipt={receipt:?}",
            fixture.oid
        );
    } else {
        assert_eq!(
            matching_receipts(&db.pool, fixture.oid, "RECEIVED", &value).await,
            0
        );
    }

    // Change the effective GUC request without alter_stream_table's separate forced cutover.
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables SET requested_cdc_mode = NULL \
         WHERE pgt_name IN ('{}', '{shared}')",
        fixture.stream
    ))
    .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_paused", "on", "on")
        .await;
    clear_wal_phase(db).await;
    // Skip graceful shutdown: releasing the barrier backend first lets the decoder commit.
    let output = tokio::process::Command::new("docker")
        .args(["kill", "--signal", "KILL", db.container_id()])
        .output()
        .await
        .expect("kill the retained PostgreSQL container at the witnessed boundary");
    assert!(output.status.success(), "Docker kill failed");
    let output = tokio::process::Command::new("docker")
        .args(["start", db.container_id()])
        .output()
        .await
        .expect("restart the retained PostgreSQL container");
    assert!(output.status.success(), "Docker start failed");
    drop(barrier);
    fixture.db.pool = fixture.db.reconnect_after_restart().await;
    let db = &fixture.db;
    for stream in [&fixture.stream, &shared] {
        db.execute(&format!(
            "SELECT pgtrickle.pause_scheduler(ARRAY['public.{stream}'])"
        ))
        .await;
    }
    assert_eq!(
        matching_receipts(&db.pool, fixture.oid, "RECEIVED", &value).await > 0,
        phase == "before_replay",
        "restart must preserve the witnessed pending-work state"
    );
    db.alter_system_set_and_wait("pg_trickle.cdc_paused", "off", "off")
        .await;
    wait_for_cdc_mode(db, &fixture.source, "TRIGGER").await;
    assert!(
        db.query_scalar::<bool>(&format!(
            "SELECT NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}')",
            fixture.slot
        ))
        .await
    );
    for stream in [&fixture.stream, &shared] {
        let invalidated: bool = db
            .query_scalar(&format!(
                "SELECT needs_reinit AND frontier IS NULL FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = '{stream}'"
            ))
            .await;
        eprintln!("[Q1159] phase={phase} stream={stream} invalidated={invalidated}");
        db.refresh_st(stream).await;
        assert_eq!(exact_diff(&db.pool, stream, &fixture.journal).await, 0);
        assert!(
            invalidated,
            "fallback must invalidate every consumer before successful refresh: {stream}"
        );
        let action: String = db
            .query_scalar(&format!(
                "SELECT h.action FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = '{stream}' AND h.refresh_id > {history} \
             AND h.status = 'COMPLETED' ORDER BY refresh_id DESC LIMIT 1"
            ))
            .await;
        assert_eq!(
            action, "REINITIALIZE",
            "recovery must be explicit in history"
        );
        assert!(
            !db.query_scalar::<bool>(&format!(
                "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{stream}'"
            ))
            .await
        );
    }
    commit_row(
        db,
        &fixture.source,
        &fixture.journal,
        3,
        "after-trigger-fallback",
    )
    .await;
    assert!(
        db.query_scalar::<i64>(&format!(
            "SELECT count(*) FROM {buffer} WHERE action <> 'S'"
        ))
        .await
            > 0,
        "future writes must be trigger-captured"
    );
    for stream in [&fixture.stream, &shared] {
        db.refresh_st(stream).await;
        assert_eq!(exact_diff(&db.pool, stream, &fixture.journal).await, 0);
        assert!(
            db.query_scalar::<bool>(&format!(
                "SELECT h.action = 'DIFFERENTIAL' AND NOT h.was_full_fallback \
             FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = '{stream}' AND h.status = 'COMPLETED' \
             ORDER BY h.refresh_id DESC LIMIT 1"
            ))
            .await,
            "future trigger writes must refresh without a hidden FULL repair"
        );
    }
    let sentinel = format!("wal_fallback_{phase}_identity");
    assert!(identity_matches(
        &fixture.identity,
        &identity(db, &sentinel, db.container_id()).await
    ));
    eprintln!(
        "[Q1159] phase={phase} source_oid={} fallback=TRIGGER recovery=REINITIALIZE parity=exact",
        fixture.oid
    );
}

#[tokio::test]
async fn test_wal_trigger_fallback_received_receipt_requires_rebaseline() {
    run_trigger_fallback_case("before_replay").await;
}

#[tokio::test]
async fn test_wal_trigger_fallback_undecoded_commit_requires_rebaseline() {
    run_trigger_fallback_case("before_receipt").await;
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
    wait_for_receipt(&fixture.db.pool, fixture.oid, "RECEIVED", value).await;
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
    let acknowledged_before_apply: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_receipts \
          WHERE source_relid = $1 AND lsn = $2::pg_lsn \
            AND status = 'ACKNOWLEDGED' AND source_change)",
    )
    .bind(fixture.oid)
    .bind(&receipt_lsn)
    .fetch_one(&fixture.db.pool)
    .await
    .expect("inspect early acknowledgement control");
    let no_buffered_change = {
        let buffer = fixture.db.change_buffer_table(fixture.oid).await;
        fixture
            .db
            .query_scalar::<bool>(&format!(
                "SELECT NOT EXISTS(SELECT 1 FROM {buffer} WHERE action <> 'S')"
            ))
            .await
    };
    assert!(
        acknowledged_before_apply && no_buffered_change,
        "control must acknowledge the receipt before replay with no buffered copy"
    );
    assert!(slot_reached(&fixture.db, &fixture.slot, &receipt_lsn).await);
    assert_mismatch(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
    clear_wal_phase(&fixture.db).await;
    terminate_backend(&fixture.db, worker_pid).await;
    release_wal_barrier(&mut barrier, fixture.oid, "before_replay").await;
    drop(barrier);
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
    wait_for_receipt(&fixture.db.pool, fixture.oid, "RECEIVED", value).await;
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
    let buffer = fixture.db.change_buffer_table(fixture.oid).await;
    let no_buffered_change: bool = fixture
        .db
        .query_scalar(&format!(
            "SELECT NOT EXISTS(SELECT 1 FROM {buffer} WHERE action <> 'S')"
        ))
        .await;
    assert!(
        no_buffered_change,
        "control must remove the buffered recovery copy"
    );
    assert_mismatch(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
    clear_wal_phase(&fixture.db).await;
    terminate_backend(&fixture.db, worker_pid).await;
    release_wal_barrier(&mut barrier, fixture.oid, "before_replay").await;
    drop(barrier);
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
    wait_for_receipt(&fixture.db.pool, fixture.oid, "RECEIVED", value).await;
    terminate_backend(&fixture.db, worker_pid).await;
    release_wal_barrier(&mut barrier, fixture.oid, "before_replay").await;
    drop(barrier);
    clear_wal_phase(&fixture.db).await;
    wait_for_exact(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
}

fn record_lifecycle(
    path: &std::path::Path,
    history: &mut Vec<serde_json::Value>,
    event: serde_json::Value,
) {
    history.push(event);
    std::fs::write(path, serde_json::to_vec_pretty(history).unwrap()).unwrap();
}

async fn lifecycle_state(pool: &PgPool, stream: &str) -> serde_json::Value {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT jsonb_build_object('rows', \
             (SELECT coalesce(jsonb_agg(jsonb_build_array(id,value) ORDER BY id,value),'[]'::jsonb) \
              FROM public.{stream}), 'frontier', frontier, 'populated', is_populated) \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_schema='public' AND pgt_name=$1"
    )))
    .bind(stream)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn lifecycle_exact(
    fixture: &Fixture,
    stream: &str,
    path: &std::path::Path,
    history: &mut Vec<serde_json::Value>,
    phase: &str,
) {
    let actual = lifecycle_state(&fixture.db.pool, stream).await;
    let expected: serde_json::Value = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT coalesce(jsonb_agg(jsonb_build_array(id,value) ORDER BY id,value),'[]'::jsonb) \
         FROM public.{}",
        fixture.journal
    )))
    .fetch_one(&fixture.db.pool)
    .await
    .unwrap();
    record_lifecycle(
        path,
        history,
        serde_json::json!({
            "phase": phase, "stream": stream, "expected_rows": expected, "actual": actual,
        }),
    );
    assert_eq!(
        actual["rows"],
        expected,
        "{phase}; history: {}",
        path.display()
    );
    assert_eq!(
        exact_diff(&fixture.db.pool, stream, &fixture.journal).await,
        0
    );
}

async fn lifecycle_refresh(fixture: &Fixture, stream: &str) {
    let before: i64 = sqlx::query_scalar(
        "SELECT coalesce(max(h.refresh_id),0) FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name=$1",
    )
    .bind(stream)
    .fetch_one(&fixture.db.pool)
    .await
    .unwrap();
    fixture
        .db
        .try_execute_with_config(
            &[
                "SET pg_trickle.compact_threshold=1",
                "SET pg_trickle.refresh_strategy='differential'",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{stream}')"),
        )
        .await
        .unwrap();
    let (after, action): (i64, String) = sqlx::query_as(
        "SELECT h.refresh_id,h.action FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
         WHERE s.pgt_name=$1 AND h.status='COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .bind(stream)
    .fetch_one(&fixture.db.pool)
    .await
    .unwrap();
    assert!(
        after > before,
        "changed boundary must execute a fresh refresh"
    );
    assert_eq!(
        action, "DIFFERENTIAL",
        "lifecycle catch-up must consume retained history"
    );
}

#[tokio::test]
async fn test_restore_source_lock_defers_cleanup_until_rewind_is_visible() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;
    db.execute_seq(&[
        "CREATE TABLE restore_lock_source(id int PRIMARY KEY,value text)",
        "INSERT INTO restore_lock_source VALUES(1,'old')",
    ])
    .await;
    for stream in ["restore_lock_target", "restore_lock_fast"] {
        db.create_st(
            stream,
            "SELECT id,value FROM restore_lock_source",
            "24h",
            "DIFFERENTIAL",
        )
        .await;
    }
    db.execute("SELECT pgtrickle.snapshot_stream_table('restore_lock_target','public.restore_lock_archive')").await;
    let oid = i64::from(db.table_oid("restore_lock_source").await);
    let old = frontier(&db, "restore_lock_target", oid).await;
    let buffer = db.change_buffer_table(oid).await;
    db.execute("UPDATE restore_lock_source SET value='new'")
        .await;
    let cut: String = db
        .query_scalar(&format!(
            "SELECT max(lsn)::text FROM {buffer} WHERE action IN ('I','D')"
        ))
        .await;
    for stream in ["restore_lock_fast", "restore_lock_target"] {
        db.try_execute_with_config(
            &["SET pg_trickle.refresh_strategy='differential'"],
            &format!("SELECT pgtrickle.refresh_stream_table('{stream}')"),
        )
        .await
        .unwrap();
    }
    let needed = format!(
        "SELECT count(*) FROM {buffer} WHERE action IN ('I','D') AND lsn>'{old}'::pg_lsn AND lsn<='{cut}'::pg_lsn"
    );
    let before: i64 = db.query_scalar(&needed).await;
    assert!(
        before > 0,
        "the cleanup schedule must begin with retained required history"
    );
    let mut restore = db.pool.begin().await.unwrap();
    sqlx::query("SELECT pgtrickle.restore_from_snapshot('restore_lock_target','public.restore_lock_archive')")
        .execute(&mut *restore).await.unwrap();
    db.execute("INSERT INTO restore_lock_source VALUES(2,'concurrent')")
        .await;
    tokio::time::timeout(
        Duration::from_secs(20),
        db.try_execute_with_config(
            &["SET pg_trickle.refresh_strategy='full'"],
            "SELECT pgtrickle.refresh_stream_table('restore_lock_fast')",
        ),
    )
    .await
    .expect("cleanup must skip the restore lock without blocking refresh")
    .unwrap();
    let during: i64 = db.query_scalar(&needed).await;
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    let path = std::path::Path::new(&directory).join("restore-cleanup-ordering.json");
    let mut history = Vec::new();
    record_lifecycle(
        &path,
        &mut history,
        serde_json::json!({
            "step":"cleanup during actual uncommitted restore","installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
            "snapshot_cut":old,"captured_cut":cut,"required_rows_before":before,"required_rows_during":during,
        }),
    );
    assert_eq!(
        during, before,
        "cleanup cannot use an obsolete minimum while restore holds the source lock"
    );
    restore.commit().await.unwrap();
    db.execute("INSERT INTO restore_lock_source VALUES(3,'after-restore')")
        .await;
    db.try_execute_with_config(
        &["SET pg_trickle.refresh_strategy='full'"],
        "SELECT pgtrickle.refresh_stream_table('restore_lock_fast')",
    )
    .await
    .unwrap();
    let after: i64 = db.query_scalar(&needed).await;
    record_lifecycle(
        &path,
        &mut history,
        serde_json::json!({"step":"cleanup after committed rewind","required_rows":after,"target":lifecycle_state(&db.pool,"restore_lock_target").await}),
    );
    assert_eq!(
        after, before,
        "later cleanup must honor the published restored cut"
    );
    db.refresh_st("restore_lock_target").await;
    for stream in ["restore_lock_target", "restore_lock_fast"] {
        db.assert_st_matches_query(stream, "SELECT id,value FROM restore_lock_source")
            .await;
    }
    let remaining: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {buffer} WHERE action IN ('I','D')"
        ))
        .await;
    record_lifecycle(
        &path,
        &mut history,
        serde_json::json!({"step":"protected catch-up completed","remaining_consumed_rows":remaining}),
    );
    assert_eq!(
        remaining, 0,
        "cleanup may reclaim history after both consumers catch up"
    );
}

#[tokio::test]
async fn test_snapshot_waits_for_committed_refresh_before_loading_frontier() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    db.execute_seq(&[
        "CREATE TABLE snapshot_commit_source(id int PRIMARY KEY,value text)",
        "INSERT INTO snapshot_commit_source VALUES(1,'old')",
    ])
    .await;
    db.create_st(
        "snapshot_commit_stream",
        "SELECT id,value FROM snapshot_commit_source",
        "24h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("UPDATE snapshot_commit_source SET value='new'")
        .await;
    let mut refresh = db.pool.begin().await.unwrap();
    let refresh_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *refresh)
        .await
        .unwrap();
    sqlx::raw_sql("SET LOCAL pg_trickle.refresh_strategy='full'; SELECT pgtrickle.refresh_stream_table('snapshot_commit_stream')")
        .execute(&mut *refresh).await.unwrap();
    let committed:serde_json::Value=sqlx::query_scalar("SELECT jsonb_build_object('rows',(SELECT jsonb_agg(jsonb_build_array(id,value) ORDER BY id) FROM snapshot_commit_stream),'frontier',frontier) FROM pgtrickle.pgt_stream_tables WHERE pgt_name='snapshot_commit_stream'")
        .fetch_one(&mut *refresh).await.unwrap();
    let mut snapshot_conn = db.pool.acquire().await.unwrap();
    let snapshot_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *snapshot_conn)
        .await
        .unwrap();
    let snapshot = tokio::spawn(async move {
        sqlx::query_scalar::<_,String>("SELECT pgtrickle.snapshot_stream_table('snapshot_commit_stream','public.snapshot_commit_archive')")
            .fetch_one(&mut *snapshot_conn).await
    });
    assert!(
        db.wait_for_condition(
            "snapshot waiting behind actual uncommitted full refresh",
            &format!("SELECT {refresh_pid}=ANY(pg_blocking_pids({snapshot_pid}))"),
            Duration::from_secs(10),
            Duration::from_millis(20)
        )
        .await
    );
    assert!(
        !snapshot.is_finished(),
        "snapshot must be witnessed waiting before commit"
    );
    refresh.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), snapshot)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let archive:serde_json::Value=db.query_scalar("SELECT jsonb_build_object('rows',(SELECT jsonb_agg(jsonb_build_array(id,value) ORDER BY id) FROM snapshot_commit_archive),'frontier',frontier) FROM pgtrickle.pgt_snapshots WHERE snapshot_table='snapshot_commit_archive'").await;
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        std::path::Path::new(&directory).join("snapshot-committed-refresh.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
            "refresh_pid":refresh_pid,"snapshot_pid":snapshot_pid,"witnessed_blocker":true,
            "expected_committed":committed,"archive":archive,
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        archive, committed,
        "snapshot rows and metadata must describe the same committed refresh"
    );
}

#[tokio::test]
async fn test_restore_after_consumed_history_cleanup_reinitializes_before_catchup() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;
    db.execute("CREATE TABLE restore_history_source(id int PRIMARY KEY,value text NOT NULL)")
        .await;
    db.execute("INSERT INTO restore_history_source VALUES(1,'old')")
        .await;
    db.create_st(
        "restore_history_stream",
        "SELECT id,value FROM restore_history_source",
        "24h",
        "DIFFERENTIAL",
    )
    .await;
    let baseline = lifecycle_state(&db.pool, "restore_history_stream").await;
    let snapshot: String = db
        .query_scalar("SELECT pgtrickle.snapshot_stream_table('restore_history_stream','public.restore_history_archive')")
        .await;
    db.execute_seq(&[
        "UPDATE restore_history_source SET value='new' WHERE id=1",
        "INSERT INTO restore_history_source VALUES(2,'later')",
    ])
    .await;
    let oid = i64::from(db.table_oid("restore_history_source").await);
    let buffer = db.change_buffer_table(oid).await;
    let captured: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {buffer} WHERE action IN ('I','D')"
        ))
        .await;
    assert!(captured > 0, "committed changes must exist before cleanup");
    db.try_execute_with_config(
        &["SET pg_trickle.refresh_strategy='full'"],
        "SELECT pgtrickle.refresh_stream_table('restore_history_stream')",
    )
    .await
    .unwrap();
    db.assert_st_matches_query(
        "restore_history_stream",
        "SELECT id,value FROM restore_history_source",
    )
    .await;
    let retained: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {buffer} WHERE action IN ('I','D')"
        ))
        .await;
    assert_eq!(
        retained, 0,
        "ordinary full-refresh cleanup must remove consumed history"
    );
    sqlx::query("SELECT pgtrickle.restore_from_snapshot('restore_history_stream',$1)")
        .bind(&snapshot)
        .execute(&db.pool)
        .await
        .unwrap();
    let restored = lifecycle_state(&db.pool, "restore_history_stream").await;
    assert_eq!(
        restored, baseline,
        "restore must retain the snapshot rows/frontier pair"
    );
    let needs_reinit: bool = db
        .query_scalar("SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name='restore_history_stream'")
        .await;
    db.try_execute_with_config(
        &["SET pg_trickle.refresh_strategy='differential'"],
        "SELECT pgtrickle.refresh_stream_table('restore_history_stream')",
    )
    .await
    .unwrap();
    let recovered = lifecycle_state(&db.pool, "restore_history_stream").await;
    let action: String = db.query_scalar("SELECT action FROM pgtrickle.pgt_refresh_history WHERE pgt_id=(SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name='restore_history_stream') AND status='COMPLETED' ORDER BY refresh_id DESC LIMIT 1").await;
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(std::path::Path::new(&directory).join("restore-missing-history.json"),serde_json::to_vec_pretty(&serde_json::json!({
        "scenario":"restore-after-ordinary-cleanup","installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
        "captured_rows":captured,"retained_after_cleanup":retained,"snapshot_state":baseline,
        "restored_state":restored,"restored_needs_reinit":needs_reinit,"recovered_state":recovered,
        "expected_rows":[[1,"new"],[2,"later"]],"refresh_action":action,
    })).unwrap()).unwrap();
    db.assert_st_matches_query(
        "restore_history_stream",
        "SELECT id,value FROM restore_history_source",
    )
    .await;
    assert!(
        needs_reinit,
        "missing history requires protected reinitialization"
    );
    assert_eq!(action, "REINITIALIZE");
}

/// Each first failing prefix is retained with its seed, SQL operations and states.
#[tokio::test]
async fn test_lifecycle_seeded_restore_lag_and_crash_preserve_committed_journal() {
    use e2e::property_support::{SeededRng, TraceConfig};
    let config = TraceConfig::from_env();
    for seed in config.seeds(0x1219_0001) {
        let db = E2eDb::new_dedicated().await.with_extension().await;
        db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
            .await;
        db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
            .await;
        let mode = if seed & 1 == 0 { "row" } else { "statement" };
        db.alter_system_set_and_wait("pg_trickle.cdc_trigger_mode", &format!("'{mode}'"), mode)
            .await;
        let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
            .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
        std::fs::create_dir_all(&directory).unwrap();
        let path = std::path::Path::new(&directory).join(format!("history-{seed:016x}.json"));
        let revision = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        assert!(revision.status.success());
        let mut history = Vec::new();
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({
                "seed":seed, "cycles":config.cycles, "revision":String::from_utf8(revision.stdout).unwrap().trim(),
                "server_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").unwrap_or_else(|_|"unbound".into()),
                "configuration":{"capture":"trigger","trigger_mode":mode,"compact_threshold":1},
                "initial_rows":if seed & 1 != 0 {serde_json::json!([[0,"baseline"]])} else {serde_json::json!([])},
                "query":"SELECT id,value FROM history_source",
                "container":db.container_id(), "minimization":"first failing ordered prefix; SQL and transaction boundaries retained",
            }),
        );
        db.execute("CREATE TABLE history_source(id INT PRIMARY KEY,value TEXT NOT NULL)")
            .await;
        db.execute("CREATE TABLE history_journal(id INT PRIMARY KEY,value TEXT NOT NULL)")
            .await;
        create_identity_sentinel(&db, "history_identity", &seed.to_string()).await;
        if seed & 1 != 0 {
            commit_row(&db, "history_source", "history_journal", 0, "baseline").await;
        }
        for stream in ["history_slow", "history_fast"] {
            db.create_st(
                stream,
                "SELECT id,value FROM history_source",
                "1h",
                "DIFFERENTIAL",
            )
            .await;
        }
        let cluster_identity = identity(&db, "history_identity", db.container_id()).await;
        let mut fixture = Fixture {
            oid: i64::from(db.table_oid("history_source").await),
            db,
            source: "history_source".into(),
            stream: "history_fast".into(),
            journal: "history_journal".into(),
            slot: String::new(),
            identity: cluster_identity,
        };
        let capture = cdc_mode(&fixture.db, &fixture.source).await;
        assert_eq!(capture.as_deref(), Some("TRIGGER"));
        let triggers: Vec<String> = sqlx::query_scalar(
            "SELECT pg_get_triggerdef(oid) FROM pg_trigger \
             WHERE tgrelid=$1::oid AND NOT tgisinternal AND (tgtype & 28)<>0 ORDER BY oid",
        )
        .bind(fixture.oid)
        .fetch_all(&fixture.db.pool)
        .await
        .unwrap();
        assert!(!triggers.is_empty());
        let expected_mode = if mode == "row" {
            "FOR EACH ROW"
        } else {
            "FOR EACH STATEMENT"
        };
        assert!(
            triggers
                .iter()
                .all(|trigger| trigger.contains(expected_mode))
        );
        let settings: (String, String, String, String) = sqlx::query_as(
            "SELECT current_setting('fsync'),current_setting('synchronous_commit'), \
             current_setting('full_page_writes'),pg_postmaster_start_time()::text",
        )
        .fetch_one(&fixture.db.pool)
        .await
        .unwrap();
        assert_eq!(
            (&*settings.0, &*settings.1, &*settings.2),
            ("on", "on", "on")
        );
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({
                "step":"capture/durability witness","capture":capture,"triggers":triggers,
                "fsync":settings.0,"synchronous_commit":settings.1,"full_page_writes":settings.2,
                "postmaster_start":settings.3,
            }),
        );
        let baseline = lifecycle_state(&fixture.db.pool, &fixture.stream).await;
        let slow_baseline = lifecycle_state(&fixture.db.pool, "history_slow").await;
        let buffer: String = fixture
            .db
            .query_scalar(&format!(
                "SELECT format('%I.%I',current_setting('pg_trickle.change_buffer_schema'), \
             'changes_' || coalesce(source_stable_name,source_relid::text)) \
             FROM pgtrickle.pgt_change_tracking WHERE source_relid={}::oid",
                fixture.oid
            ))
            .await;
        let snapshot_sql = if seed & 1 == 0 {
            "SELECT pgtrickle.snapshot_stream_table('history_fast','public.history_archive')"
        } else {
            "SELECT pgtrickle.snapshot_stream_table('history_fast')"
        };
        let snapshot: String = fixture.db.query_scalar(snapshot_sql).await;
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({"step":"snapshot","sql":snapshot_sql,"name":snapshot,"state":baseline}),
        );
        let mut rng = SeededRng::new(seed);
        let mut ids = if seed & 1 != 0 { vec![0] } else { vec![] };
        for cycle in 1..=config.cycles {
            let value = rng.i32_range(-1000, 1000).to_string();
            let choice = rng.usize_range(0, 2);
            let operation = if ids.is_empty() || choice == 0 {
                let id = i32::try_from(cycle).unwrap();
                ids.push(id);
                format!("INSERT INTO $table VALUES ({id},'{value}')")
            } else {
                let index = rng.usize_range(0, ids.len() - 1);
                let id = ids[index];
                if choice == 2 && ids.len() > 1 {
                    ids.remove(index);
                    format!("DELETE FROM $table WHERE id={id}")
                } else {
                    format!("UPDATE $table SET value='{value}' WHERE id={id}")
                }
            };
            let sql = format!(
                "BEGIN; {}; {}; SAVEPOINT abandoned; \
                 UPDATE history_source SET value='abandoned'; ROLLBACK TO abandoned; COMMIT",
                operation.replace("$table", &fixture.source),
                operation.replace("$table", &fixture.journal)
            );
            record_lifecycle(
                &path,
                &mut history,
                serde_json::json!({
                    "step":"committed source+journal mutation","cycle":cycle,"sql":sql,
                }),
            );
            sqlx::raw_sql(AssertSqlSafe(sql))
                .execute(&fixture.db.pool)
                .await
                .unwrap();
            let before = lifecycle_state(&fixture.db.pool, &fixture.stream).await;
            let sql = "BEGIN; UPDATE history_source SET value='rolled-back'; \
                       SAVEPOINT abandoned; DELETE FROM history_source; ROLLBACK TO abandoned; \
                       SELECT pgtrickle.refresh_stream_table('history_fast'); ROLLBACK";
            record_lifecycle(
                &path,
                &mut history,
                serde_json::json!({"step":"rollback refresh/savepoint","sql":sql}),
            );
            sqlx::raw_sql(sql).execute(&fixture.db.pool).await.unwrap();
            let after = lifecycle_state(&fixture.db.pool, &fixture.stream).await;
            assert_eq!(
                after,
                before,
                "aborted rows/frontier published; {}",
                path.display()
            );
            let cut: String = fixture
                .db
                .query_scalar(&format!(
                    "SELECT max(lsn)::text FROM {buffer} WHERE action IN ('I','D')"
                ))
                .await;
            record_lifecycle(
                &path,
                &mut history,
                serde_json::json!({"step":"refresh boundary","expected_source_cut":cut}),
            );
            lifecycle_refresh(&fixture, "history_fast").await;
            lifecycle_exact(
                &fixture,
                "history_fast",
                &path,
                &mut history,
                "fast/compact",
            )
            .await;
            let covered: bool = fixture
                .db
                .query_scalar(&format!(
                    "SELECT (frontier->'sources'->'{}'->>'lsn')::pg_lsn >= '{cut}'::pg_lsn \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_name='history_fast'",
                    fixture.oid
                ))
                .await;
            assert!(
                covered,
                "committed rows require matching consumed cut; {}",
                path.display()
            );
            assert_eq!(
                lifecycle_state(&fixture.db.pool, "history_slow").await,
                slow_baseline,
                "slow consumer moved"
            );
        }
        let catchup_cut: String = fixture
            .db
            .query_scalar(&format!(
                "SELECT max(lsn)::text FROM {buffer} WHERE action IN ('I','D')"
            ))
            .await;
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({"step":"restore","snapshot":snapshot,"expected_source_cut":catchup_cut}),
        );
        sqlx::query("SELECT pgtrickle.restore_from_snapshot('history_fast',$1)")
            .bind(&snapshot)
            .execute(&fixture.db.pool)
            .await
            .unwrap();
        assert_eq!(
            lifecycle_state(&fixture.db.pool, "history_fast").await,
            baseline,
            "restore mixed rows/frontier"
        );
        lifecycle_refresh(&fixture, "history_fast").await;
        lifecycle_exact(
            &fixture,
            "history_fast",
            &path,
            &mut history,
            "restored catch-up",
        )
        .await;
        let before_crash = lifecycle_state(&fixture.db.pool, "history_fast").await;
        assert!(fixture.db.query_scalar::<bool>(&format!(
            "SELECT (frontier->'sources'->'{}'->>'lsn')::pg_lsn >= '{catchup_cut}'::pg_lsn FROM pgtrickle.pgt_stream_tables WHERE pgt_name='history_fast'",
            fixture.oid
        )).await, "restored catch-up must cover the committed cut");
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({"step":"crash after committed catch-up","retained_identity":format!("{:?}",fixture.identity)}),
        );
        fixture.db.pool.close().await;
        for args in [
            vec!["kill", "--signal", "KILL", fixture.db.container_id()],
            vec!["start", fixture.db.container_id()],
        ] {
            let output = tokio::process::Command::new("docker")
                .args(args)
                .output()
                .await
                .unwrap();
            assert!(
                output.status.success(),
                "retained restart failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        fixture.db.pool = fixture.db.reconnect_after_restart().await;
        let restarted: String = fixture
            .db
            .query_scalar("SELECT pg_postmaster_start_time()::text")
            .await;
        assert_ne!(restarted, settings.3, "recovery must restart PostgreSQL");
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({"step":"retained restart witnessed","postmaster_before":settings.3,"postmaster_after":restarted}),
        );
        assert_eq!(
            identity(&fixture.db, "history_identity", fixture.db.container_id()).await,
            fixture.identity
        );
        assert_eq!(
            lifecycle_state(&fixture.db.pool, "history_fast").await,
            before_crash,
            "retained restart must preserve the committed rows/frontier checkpoint"
        );
        lifecycle_exact(
            &fixture,
            "history_fast",
            &path,
            &mut history,
            "retained recovery",
        )
        .await;
        lifecycle_refresh(&fixture, "history_slow").await;
        lifecycle_exact(
            &fixture,
            "history_slow",
            &path,
            &mut history,
            "lagging catch-up",
        )
        .await;
        assert!(fixture.db.query_scalar::<bool>(&format!(
            "SELECT (frontier->'sources'->'{}'->>'lsn')::pg_lsn >= '{catchup_cut}'::pg_lsn FROM pgtrickle.pgt_stream_tables WHERE pgt_name='history_slow'",
            fixture.oid
        )).await, "lagging catch-up must cover the committed cut");
    }
}
