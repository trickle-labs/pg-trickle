//! #1095: durable WAL receipt recovery, replay, and shared-source cleanup.

mod e2e;
#[allow(dead_code)] // This binary uses selected receipt kernels for its controls.
#[path = "../verification/receipt.rs"]
mod receipt_limit;

use e2e::E2eDb;
use sqlx::{AssertSqlSafe, PgPool, Postgres, pool::PoolConnection};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(180);

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
        "before_slot_retirement" => 5,
        "after_slot_retirement" => 6,
        "slot_retirement_kept" => 7,
        "before_slot_adoption" => 8,
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
                 WHERE d.source_relid = '{source}'::regclass::oid) \
                   AND NOT EXISTS(SELECT 1 FROM pgtrickle.pgt_dependencies d \
                 WHERE d.source_relid = '{source}'::regclass::oid \
                   AND d.cdc_mode <> '{expected}')"
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
    let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::xid::text")
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
    // Avoid autovacuum contending with the short-lived receipt lock witnesses.
    db.execute("ALTER TABLE pgtrickle.pgt_wal_receipts SET (autovacuum_enabled = false)")
        .await;
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
            let state: String = db
                .query_scalar(
                    "SELECT jsonb_build_object(\
                        'dependencies', (SELECT jsonb_agg(to_jsonb(d)) FROM pgtrickle.pgt_dependencies d),\
                        'workers', (SELECT jsonb_agg(jsonb_build_object(\
                            'pid', pid, 'backend_type', backend_type, 'application_name', application_name,\
                            'state', state, 'wait_event', wait_event, 'query', query))\
                            FROM pg_stat_activity WHERE datname = current_database()),\
                        'slots', (SELECT jsonb_agg(to_jsonb(s)) FROM pg_replication_slots s)\
                    )::text",
                )
                .await;
            panic!("WAL decoder did not reach {phase} for source OID {oid}; state={state}");
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

async fn run_update_to_null_case(mode: &str) {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    configure_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'auto'", "auto")
        .await;
    let source = format!("null_{mode}_source");
    let stream = format!("null_{mode}_stream");
    db.execute(&format!(
        "CREATE TABLE {source} (id INT PRIMARY KEY, value TEXT)"
    ))
    .await;
    db.execute(&format!("ALTER TABLE {source} REPLICA IDENTITY FULL"))
        .await;
    db.execute(&format!("INSERT INTO {source} VALUES (1, 'before')"))
        .await;
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table(\
            name => '{stream}', query => $$SELECT id, value FROM {source}$$, \
            schedule => '1s', refresh_mode => 'DIFFERENTIAL', cdc_mode => '{mode}')"
    ))
    .await;
    let expected_mode = mode.to_uppercase();
    wait_for_cdc_mode(&db, &source, &expected_mode).await;
    // Drain refreshes while CDC remains active so buffered rows can be inspected.
    db.execute(&format!(
        "SELECT pgtrickle.pause_scheduler(ARRAY['public.{stream}'])"
    ))
    .await;
    let oid = i64::from(db.table_oid(&source).await);
    let buffer = db.change_buffer_table(oid).await;
    let token = "value[text]:null";
    db.refresh_st_with_retry(&stream).await;
    wait_for_cdc_mode(&db, &source, &expected_mode).await;
    assert_eq!(exact_diff(&db.pool, &stream, &source).await, 0);
    assert!(
        db.query_scalar::<bool>(&format!(
            "SELECT NOT needs_reinit AND frontier IS NOT NULL \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{stream}'"
        ))
        .await,
        "nullable update must start from a settled baseline"
    );
    let history: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    let runtime: (String, String, String, String) = sqlx::query_as(
        "SELECT current_setting('server_version'), pgtrickle.version(), \
                current_setting('wal_level'), current_setting('pg_trickle.cdc_mode')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("record loaded PostgreSQL/extension and CDC settings");
    eprintln!(
        "[Q1172] runtime={runtime:?} container={} capture={expected_mode}",
        db.container_id()
    );

    let mut tx = db.pool.begin().await.expect("begin nullable update");
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE {source} SET value = NULL WHERE id = 1"
    )))
    .execute(&mut *tx)
    .await
    .expect("update nullable source");
    let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
        .fetch_one(&mut *tx)
        .await
        .expect("identify nullable update transaction");
    tx.commit().await.expect("commit nullable update");

    if mode == "wal" {
        wait_for_receipt(&db.pool, oid, "ACKNOWLEDGED", token).await;
        let receipt: (String, String) = sqlx::query_as(
            "SELECT lsn::text, data FROM pgtrickle.pgt_wal_receipts \
             WHERE source_relid = $1 AND source_xid::text = $2 \
               AND source_change AND status = 'ACKNOWLEDGED'",
        )
        .bind(oid)
        .bind(&xid)
        .fetch_one(&db.pool)
        .await
        .expect("observe the committed update acknowledged by WAL capture");
        eprintln!("[Q1172] xid={xid} receipt={receipt:?}");
    }
    let buffered: Vec<(String, i32, Option<String>)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT action::text, id, value FROM {buffer} \
         WHERE source_xid::text = $1 AND action <> 'S' ORDER BY change_id"
    )))
    .bind(&xid)
    .fetch_all(&db.pool)
    .await
    .expect("inspect buffered old/new rows before refresh consumes them");
    eprintln!("[Q1172] capture={expected_mode} xid={xid} buffered={buffered:?}");
    assert_eq!(
        buffered,
        vec![
            ("D".into(), 1, Some("before".into())),
            ("I".into(), 1, None)
        ],
        "DELETE must preserve the old value; INSERT must contain SQL NULL"
    );
    wait_for_cdc_mode(&db, &source, &expected_mode).await;
    let reinit_reason: Option<String> = db
        .query_scalar_opt(&format!(
            "SELECT refresh_reason || ': ' || refresh_reason_detail \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{stream}' AND needs_reinit"
        ))
        .await;
    assert!(
        !db.query_scalar::<bool>(&format!(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{stream}'"
        ))
        .await,
        "nullable update must not require a rebuild: {reinit_reason:?}"
    );
    db.refresh_st(&stream).await;
    assert_eq!(exact_diff(&db.pool, &stream, &source).await, 0);
    let actual: Vec<(i32, Option<String>)> =
        sqlx::query_as(AssertSqlSafe(format!("SELECT id, value FROM {stream}")))
            .fetch_all(&db.pool)
            .await
            .expect("read exact nullable stream result");
    assert_eq!(actual, vec![(1, None)]);
    let refreshes: Vec<(String, bool)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT h.action, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE st.pgt_name = '{stream}' AND h.refresh_id > {history} AND h.status = 'COMPLETED'"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("verify differential refresh without fallback or reinitialization");
    assert_eq!(refreshes, vec![("DIFFERENTIAL".into(), false)]);
    assert_eq!(
        cdc_mode(&db, &source).await.as_deref(),
        Some(expected_mode.as_str())
    );
    eprintln!(
        "[Q1172] capture={expected_mode} xid={xid} result={actual:?} refresh=DIFFERENTIAL parity=exact"
    );
}

#[tokio::test]
async fn test_wal_update_to_null_preserves_old_row_and_exact_result() {
    run_update_to_null_case("wal").await;
}

#[tokio::test]
async fn test_trigger_update_to_null_preserves_old_row_and_exact_result() {
    run_update_to_null_case("trigger").await;
}

async fn matching_receipts(pool: &PgPool, oid: i64, status: &str, value: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_wal_receipts
          WHERE source_relid = $1 AND source_change AND status = $2 AND data LIKE $3",
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
          WHERE source_relid = $1 AND source_change AND status = $2 AND data LIKE $3
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
async fn test_wal_receipts_do_not_recursively_capture_their_own_payloads() {
    let fixture = setup_wal_fixture("feedback_bound").await;
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    let path = std::path::Path::new(&directory).join("receipt-feedback.json");
    let mut history = Vec::new();
    for cycle in 1..=6 {
        let value = format!("feedback-{cycle}");
        commit_row(
            &fixture.db,
            &fixture.source,
            &fixture.journal,
            cycle + 1,
            &value,
        )
        .await;
        assert!(fixture.db.wait_for_condition("actual source receipt acknowledged", &format!(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_receipts WHERE source_relid={} AND source_change AND status='ACKNOWLEDGED' AND data LIKE '%{value}%')",
            fixture.oid
        ), WAIT, Duration::from_millis(25)).await);
        let sample: serde_json::Value = sqlx::query_scalar(
            "SELECT jsonb_build_object('receipts',count(*),'max_payload_bytes',coalesce(max(octet_length(data)),0),
                'recursive_receipts',count(*) FILTER(WHERE data LIKE '%table pgtrickle.pgt_wal_receipts:%table pgtrickle.pgt_wal_receipts:%'))
             FROM pgtrickle.pgt_wal_receipts WHERE source_relid=$1",
        ).bind(fixture.oid).fetch_one(&fixture.db.pool).await.unwrap();
        let recursive_transaction: serde_json::Value = sqlx::query_scalar(
            "SELECT coalesce(jsonb_agg(jsonb_build_object('source_xid',source_xid,'source_change',source_change,'data',left(data,600)) ORDER BY receipt_id),'[]'::jsonb)
             FROM pgtrickle.pgt_wal_receipts WHERE source_relid=$1 AND source_xid IN (
               SELECT source_xid FROM pgtrickle.pgt_wal_receipts WHERE source_relid=$1
               AND data LIKE '%table pgtrickle.pgt_wal_receipts:%table pgtrickle.pgt_wal_receipts:%' LIMIT 1)",
        ).bind(fixture.oid).fetch_one(&fixture.db.pool).await.unwrap();
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({
                "cycle":cycle,"source_acknowledged":true,"sample":sample,
                "recursive_transaction":recursive_transaction,
                "installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
            }),
        );
        assert_eq!(
            sample["recursive_receipts"],
            0,
            "receipt bookkeeping must not become recursively nested durable payloads; {}",
            path.display()
        );
        assert_eq!(
            cdc_mode(&fixture.db, &fixture.source).await.as_deref(),
            Some("WAL")
        );
    }
    wait_for_exact(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
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
    // Receipt acknowledgement may precede completion of the asynchronous
    // TRIGGER-to-WAL handoff. Verify eventual WAL mode rather than sampling
    // the transient state at the exact moment the data converges.
    wait_for_cdc_mode(&fixture.db, &fixture.source, "WAL").await;
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
        let xid: String = sqlx::query_scalar("SELECT pg_current_xact_id()::xid::text")
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
    let mut fixture = setup_wal_fixture(phase).await;
    let sentinel_table = format!("wal_{}_identity", phase.replace('-', "_"));
    let value = format!("{phase}-committed");
    let next_value = format!("{phase}-next-batch");
    set_wal_phase(&fixture.db, fixture.oid, phase, &value).await;
    if phase == "before_receipt" {
        // A missed pre-receipt batch cannot be replayed; load the witness at server startup.
        fixture.db.pool = restart_and_check_named(&fixture, &sentinel_table).await;
        wait_for_cdc_mode(&fixture.db, &fixture.source, "WAL").await;
        commit_row(
            &fixture.db,
            &fixture.source,
            &fixture.journal,
            3,
            "restart-readiness",
        )
        .await;
        wait_for_receipt(
            &fixture.db.pool,
            fixture.oid,
            "ACKNOWLEDGED",
            "restart-readiness",
        )
        .await;
        wait_for_cdc_mode(&fixture.db, &fixture.source, "WAL").await;
    }
    let (mut barrier, blocker_pid) = hold_wal_barrier(&fixture.db, fixture.oid, phase).await;
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
async fn test_wal_receipts_acknowledge_transaction_larger_than_poll_limit() {
    let fixture = setup_wal_fixture("large_transaction").await;
    let db = &fixture.db;
    db.alter_system_set_and_wait("pg_trickle.wal_max_changes_per_poll", "100", "100")
        .await;
    db.execute(&format!(
        "WITH inserted AS (\
             INSERT INTO public.{} (id, value) \
             SELECT id, 'large-transaction-' || id FROM generate_series(2, 251) id \
             RETURNING id, value\
         ) INSERT INTO public.{} SELECT id, value FROM inserted",
        fixture.source, fixture.journal
    ))
    .await;
    assert!(
        db.wait_for_condition(
            "complete oversized WAL transaction acknowledged",
            &format!(
                "SELECT count(*) = 250 FROM pgtrickle.pgt_wal_receipts \
                 WHERE source_relid = {} AND source_change \
                   AND status = 'ACKNOWLEDGED' AND data LIKE '%large-transaction-%'",
                fixture.oid
            ),
            WAIT,
            Duration::from_millis(50),
        )
        .await,
        "every source change must be acknowledged despite the smaller poll limit"
    );
    assert_eq!(cdc_mode(db, &fixture.source).await.as_deref(), Some("WAL"));
    wait_for_exact(&db.pool, &fixture.stream, &fixture.journal).await;
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
    set_wal_phase(db, fixture.oid, phase, &value).await;
    let db = &fixture.db;
    // The witnessed restart below is the recovery event under test.
    wait_for_cdc_mode(db, &fixture.source, "WAL").await;
    let (barrier, blocker_pid) = hold_wal_barrier(db, fixture.oid, phase).await;
    commit_row(db, &fixture.source, &fixture.journal, 2, &value).await;
    wait_for_wal_barrier(db, fixture.oid, phase, blocker_pid).await;
    let buffered: String = db
        .query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(b)), '[]'::jsonb)::text \
             FROM {buffer} b WHERE action <> 'S'"
        ))
        .await;
    assert_eq!(
        db.query_scalar::<i64>(&format!(
            "SELECT count(*) FROM {buffer} WHERE action <> 'S' AND id = 2"
        ))
        .await,
        0,
        "pending commit must have no trigger or replay copy; buffered={buffered}"
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
        db.wait_for_condition(
            "committed trigger-fallback slot retirement",
            &format!(
                "SELECT NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}')",
                fixture.slot
            ),
            WAIT,
            Duration::from_millis(25)
        )
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

#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum LifecycleMutationKind {
    Insert,
    Update,
    Delete,
}

impl LifecycleMutationKind {
    fn name(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
struct LifecycleMutation {
    kind: LifecycleMutationKind,
    id: i32,
    value: i32,
}

impl LifecycleMutation {
    fn sql(self, table: &str) -> String {
        match self.kind {
            LifecycleMutationKind::Insert => {
                format!("INSERT INTO {table} VALUES ({},'{}')", self.id, self.value)
            }
            LifecycleMutationKind::Update => {
                format!(
                    "UPDATE {table} SET value='{}' WHERE id={}",
                    self.value, self.id
                )
            }
            LifecycleMutationKind::Delete => {
                format!("DELETE FROM {table} WHERE id={}", self.id)
            }
        }
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({"kind":self.kind.name(),"id":self.id,"value":self.value})
    }
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
        "PGT_LIFECYCLE_FAILURE:exact_rows; {phase}; history: {}",
        path.display()
    );
    assert_eq!(
        exact_diff(&fixture.db.pool, stream, &fixture.journal).await,
        0,
        "PGT_LIFECYCLE_FAILURE:exact_diff; {phase}; history: {}",
        path.display()
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
        "PGT_LIFECYCLE_FAILURE:fresh_refresh; stream={stream}"
    );
    assert_eq!(
        action, "DIFFERENTIAL",
        "PGT_LIFECYCLE_FAILURE:refresh_action; stream={stream}"
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
async fn test_consumer_initialization_does_not_cover_rows_missing_from_fixed_snapshot() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'trigger'", "trigger")
        .await;
    db.execute("CREATE TABLE registration_source(id int PRIMARY KEY,value text NOT NULL)")
        .await;
    db.execute("CREATE TABLE registration_journal(id int PRIMARY KEY,value text NOT NULL)")
        .await;
    commit_row(
        &db,
        "registration_source",
        "registration_journal",
        1,
        "baseline",
    )
    .await;
    db.create_st(
        "registration_existing",
        "SELECT id,value FROM registration_source",
        "24h",
        "DIFFERENTIAL",
    )
    .await;
    let oid = i64::from(db.table_oid("registration_source").await);
    let buffer = db.change_buffer_table(oid).await;
    let pending: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {buffer} WHERE action IN ('I','D')"
        ))
        .await;
    assert_eq!(pending, 0, "baseline must have no pending captured rows");
    let mut rejected = Vec::new();
    for (index, isolation) in ["REPEATABLE READ", "SERIALIZABLE"].iter().enumerate() {
        let mut caller = db.pool.begin().await.unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation}"
        )))
        .execute(&mut *caller)
        .await
        .unwrap();
        let pinned: i64 = sqlx::query_scalar("SELECT count(*) FROM registration_source")
            .fetch_one(&mut *caller)
            .await
            .unwrap();
        assert_eq!(pinned, index as i64 + 1);
        let snapshot: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
            .fetch_one(&mut *caller)
            .await
            .unwrap();
        commit_row(
            &db,
            "registration_source",
            "registration_journal",
            index as i32 + 2,
            "committed-after-snapshot",
        )
        .await;
        let captured_cut: String = db
            .query_scalar(&format!(
                "SELECT max(lsn)::text FROM {buffer} WHERE action IN ('I','D')"
            ))
            .await;
        let error = sqlx::query("SELECT pgtrickle.create_stream_table(name=>'registration_new',query=>'SELECT id,value FROM registration_source',schedule=>'24h',refresh_mode=>'DIFFERENTIAL')")
            .execute(&mut *caller).await.expect_err("fixed-snapshot initialization must fail before publishing a frontier").to_string();
        assert!(
            error.contains("READ COMMITTED") && error.contains("initialization"),
            "{error}"
        );
        caller.rollback().await.unwrap();
        assert!(db.query_scalar::<bool>("SELECT to_regclass('public.registration_new') IS NULL AND NOT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name='registration_new')").await);
        rejected.push(serde_json::json!({"isolation":isolation,"pinned_snapshot":snapshot,"pinned_rows":pinned,"committed_writer_cut":captured_cut,"error":error,"partial_registration":false}));
    }
    db.create_st(
        "registration_new",
        "SELECT id,value FROM registration_source",
        "24h",
        "DIFFERENTIAL",
    )
    .await;
    let initialized = lifecycle_state(&db.pool, "registration_new").await;
    db.assert_st_matches_query(
        "registration_new",
        "SELECT id,value FROM registration_journal",
    )
    .await;
    let before_refresh: i64 = db.query_scalar("SELECT max(refresh_id) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='registration_new'").await;
    commit_row(
        &db,
        "registration_source",
        "registration_journal",
        4,
        "after-initialization",
    )
    .await;
    let differential_cut: String = db
        .query_scalar(&format!(
            "SELECT max(lsn)::text FROM {buffer} WHERE action IN ('I','D')"
        ))
        .await;
    db.try_execute_with_config(
        &["SET pg_trickle.refresh_strategy='differential'"],
        "SELECT pgtrickle.refresh_stream_table('registration_new')",
    )
    .await
    .unwrap();
    let differential: (i64, String) = sqlx::query_as("SELECT refresh_id,action FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='registration_new' AND h.status='COMPLETED' ORDER BY refresh_id DESC LIMIT 1").fetch_one(&db.pool).await.unwrap();
    assert!(differential.0 > before_refresh);
    assert_eq!(differential.1, "DIFFERENTIAL");
    let covered: bool = sqlx::query_scalar("SELECT (frontier->'sources'->$1::text->>'lsn')::pg_lsn >= $2::pg_lsn FROM pgtrickle.pgt_stream_tables WHERE pgt_name='registration_new'")
        .bind(oid.to_string()).bind(&differential_cut).fetch_one(&db.pool).await.unwrap();
    assert!(
        covered,
        "differential retry must cover the committed writer cut"
    );
    let mut deferred = db.pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *deferred)
        .await
        .unwrap();
    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM registration_source")
        .fetch_one(&mut *deferred)
        .await
        .unwrap();
    commit_row(
        &db,
        "registration_source",
        "registration_journal",
        5,
        "after-deferred-snapshot",
    )
    .await;
    sqlx::query("SELECT pgtrickle.create_stream_table(name=>'registration_deferred',query=>'SELECT id,value FROM registration_source',schedule=>'24h',refresh_mode=>'DIFFERENTIAL',initialize=>false)")
        .execute(&mut *deferred).await.unwrap();
    deferred.commit().await.unwrap();
    assert!(db.query_scalar::<bool>("SELECT NOT is_populated AND frontier IS NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name='registration_deferred'").await);
    db.refresh_st("registration_deferred").await;
    db.assert_st_matches_query(
        "registration_deferred",
        "SELECT id,value FROM registration_journal",
    )
    .await;
    db.refresh_st("registration_new").await;
    let refreshed = lifecycle_state(&db.pool, "registration_new").await;
    let expected: serde_json::Value = db
        .query_scalar("SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM registration_journal t")
        .await;
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        std::path::Path::new(&directory).join("registration-fixed-snapshot.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
            "rejected":rejected,
            "differential_history":differential,"differential_writer_cut":differential_cut,"frontier_covers_cut":covered,
            "initialized":initialized,"refreshed":refreshed,"expected_journal":expected,
        }))
        .unwrap(),
    )
    .unwrap();
    db.assert_st_matches_query(
        "registration_new",
        "SELECT id,value FROM registration_journal",
    )
    .await;
}

async fn disable_wal_scheduler(fixture: &Fixture) {
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    assert!(fixture.db.wait_for_condition("idle WAL slot", &format!(
        "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{}' AND NOT active)",fixture.slot
    ), WAIT, Duration::from_millis(25)).await);
}

async fn retirement_pending(fixture: &Fixture) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name=$1)",
    )
    .bind(&fixture.slot)
    .fetch_one(&fixture.db.pool)
    .await
    .unwrap()
}

async fn slot_present(db: &E2eDb, slot: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)")
        .bind(slot)
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

async fn wait_for_retirement(fixture: &Fixture) {
    assert!(fixture.db.wait_for_condition("committed slot retirement", &format!(
        "SELECT NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{0}') AND NOT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='{0}')",fixture.slot
    ), WAIT, Duration::from_millis(25)).await);
}

#[tokio::test]
async fn test_slot_retirement_commits_before_drop_and_recovers_after_drop_crash() {
    let mut fixture = setup_wal_fixture("retirement_crash").await;
    disable_wal_scheduler(&fixture).await;
    assert!(
        !fixture
            .db
            .query_scalar::<bool>("SELECT pg_try_advisory_lock(1346851927,0)")
            .await,
        "live scheduler must retain the local pipeline session mutex"
    );
    fixture
        .db
        .execute(&format!(
            "SELECT pgtrickle.drop_stream_table('{}')",
            fixture.stream
        ))
        .await;
    assert!(retirement_pending(&fixture).await);
    assert!(
        slot_present(&fixture.db, &fixture.slot).await,
        "API commit cannot physically drop the slot"
    );
    fixture.db.pool = restart_and_check_named(&fixture, "wal_retirement_crash_identity").await;
    assert!(
        retirement_pending(&fixture).await,
        "retirement intent must survive a retained restart"
    );
    assert!(slot_present(&fixture.db, &fixture.slot).await);

    let phase = "after_slot_retirement";
    let (barrier, pid) = hold_wal_barrier(&fixture.db, fixture.oid, phase).await;
    set_wal_phase(&fixture.db, fixture.oid, phase, &fixture.slot).await;
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    wait_for_wal_barrier(&fixture.db, fixture.oid, phase, pid).await;
    assert!(
        !slot_present(&fixture.db, &fixture.slot).await,
        "witness must follow actual physical DROP"
    );
    assert!(
        retirement_pending(&fixture).await,
        "intent deletion is still uncommitted"
    );
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    clear_wal_phase(&fixture.db).await;
    for args in [
        vec!["kill", "--signal", "KILL", fixture.db.container_id()],
        vec!["start", fixture.db.container_id()],
    ] {
        let output = tokio::process::Command::new("docker")
            .args(args)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "retirement crash/restart failed");
    }
    drop(barrier);
    fixture.db.pool = fixture.db.reconnect_after_restart().await;
    assert!(
        retirement_pending(&fixture).await,
        "aborted worker deletion must retain committed intent"
    );
    assert!(!slot_present(&fixture.db, &fixture.slot).await);
    assert_eq!(
        identity(
            &fixture.db,
            "wal_retirement_crash_identity",
            fixture.db.container_id()
        )
        .await,
        fixture.identity
    );
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    wait_for_retirement(&fixture).await;
}

#[tokio::test]
async fn test_slot_retirement_keeps_active_stale_and_mismatched_slots() {
    let fixture = setup_wal_fixture("retirement_kept").await;
    disable_wal_scheduler(&fixture).await;
    // Exercise each guard in the actual worker, including SQL NULL physical-slot fields.
    for (slot, owner, create) in [
        (fixture.slot.clone(), "instance_id", None),
        (
            "retirement_stale".into(),
            "'stale-owner'",
            Some("SELECT pg_create_logical_replication_slot('retirement_stale','test_decoding')"),
        ),
        (
            "retirement_plugin".into(),
            "instance_id",
            Some("SELECT pg_create_logical_replication_slot('retirement_plugin','pgoutput')"),
        ),
        (
            "retirement_physical".into(),
            "instance_id",
            Some("SELECT pg_create_physical_replication_slot('retirement_physical')"),
        ),
    ] {
        if let Some(sql) = create {
            fixture.db.execute(sql).await;
        }
        fixture.db.execute(&format!("INSERT INTO pgtrickle.pgt_wal_slot_retirements(slot_name,source_relid,capture_instance_id) SELECT '{slot}',{}::oid,{owner} FROM pgtrickle.pgt_capture_instance",fixture.oid)).await;
        let phase = "slot_retirement_kept";
        let (mut barrier, pid) = hold_wal_barrier(&fixture.db, fixture.oid, phase).await;
        set_wal_phase(&fixture.db, fixture.oid, phase, &slot).await;
        fixture
            .db
            .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
            .await;
        wait_for_wal_barrier(&fixture.db, fixture.oid, phase, pid).await;
        assert!(
            slot_present(&fixture.db, &slot).await,
            "guard must preserve {slot}"
        );
        assert!(fixture.db.query_scalar::<bool>(&format!("SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='{slot}')")).await,
            "mismatch must retain its diagnostic intent");
        assert_eq!(
            cdc_mode(&fixture.db, &fixture.source).await.as_deref(),
            Some("WAL")
        );
        fixture
            .db
            .alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
            .await;
        clear_wal_phase(&fixture.db).await;
        release_wal_barrier(&mut barrier, fixture.oid, phase).await;
        drop(barrier);
        // Block until the witnessed worker transaction releases its intent lock.
        fixture
            .db
            .execute(&format!(
                "DELETE FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='{slot}'"
            ))
            .await;
        if create.is_some() {
            fixture
                .db
                .execute(&format!("SELECT pg_drop_replication_slot('{slot}')"))
                .await;
        }
    }
    let before_conflict = lifecycle_state(&fixture.db.pool, &fixture.stream).await;
    fixture.db.execute(&format!("INSERT INTO pgtrickle.pgt_wal_slot_retirements(slot_name,source_relid,capture_instance_id) VALUES('{}',{}::oid,'previous-owner')",fixture.slot,fixture.oid)).await;
    let rejected = sqlx::query("SELECT pgtrickle.drop_stream_table($1)")
        .bind(&fixture.stream)
        .execute(&fixture.db.pool)
        .await
        .expect_err("a stale retirement owner cannot be rebound by API cleanup");
    assert!(
        rejected
            .to_string()
            .contains("pending reservation for another source or capture owner")
    );
    assert_eq!(
        lifecycle_state(&fixture.db.pool, &fixture.stream).await,
        before_conflict
    );
    assert!(slot_present(&fixture.db, &fixture.slot).await);
    let queued_owner: String = sqlx::query_scalar(
        "SELECT capture_instance_id FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name=$1",
    )
    .bind(&fixture.slot)
    .fetch_one(&fixture.db.pool)
    .await
    .unwrap();
    assert_eq!(queued_owner, "previous-owner");
    fixture
        .db
        .execute(&format!(
            "DELETE FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='{}'",
            fixture.slot
        ))
        .await;
    fixture
        .db
        .execute(&format!(
            "SELECT pgtrickle.drop_stream_table('{}')",
            fixture.stream
        ))
        .await;
    let phase = "before_slot_retirement";
    let (mut barrier, pid) = hold_wal_barrier(&fixture.db, fixture.oid, phase).await;
    set_wal_phase(&fixture.db, fixture.oid, phase, &fixture.slot).await;
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    let worker = wait_for_wal_barrier(&fixture.db, fixture.oid, phase, pid).await;
    let mut recovery = fixture.db.pool.acquire().await.unwrap();
    let recovery_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *recovery)
        .await
        .unwrap();
    let recovery_task = tokio::spawn(async move {
        let result = sqlx::query("SELECT pgtrickle.recover_capture_instance()")
            .execute(&mut *recovery)
            .await;
        result.expect_err("the blocked recovery call must be cancelled");
    });
    assert!(
        fixture
            .db
            .wait_for_condition(
                "capture recovery blocked by retirement owner lock",
                &format!("SELECT {worker}=ANY(pg_blocking_pids({recovery_pid}))"),
                WAIT,
                Duration::from_millis(25)
            )
            .await
    );
    assert!(
        fixture
            .db
            .query_scalar::<bool>(&format!("SELECT pg_cancel_backend({recovery_pid})"))
            .await
    );
    recovery_task.await.unwrap();
    clear_wal_phase(&fixture.db).await;
    release_wal_barrier(&mut barrier, fixture.oid, phase).await;
    drop(barrier);
    wait_for_retirement(&fixture).await;
}

#[tokio::test]
async fn test_dropped_wal_source_retains_retirement_until_last_consumer_commits() {
    for legacy in [false, true] {
        let fixture = setup_wal_fixture(if legacy {
            "retirement_source_drop_legacy"
        } else {
            "retirement_source_drop"
        })
        .await;
        disable_wal_scheduler(&fixture).await;
        if legacy {
            fixture.db.execute(&format!("UPDATE pgtrickle.pgt_dependencies SET slot_name=NULL WHERE source_relid={}::oid",fixture.oid)).await;
        }
        fixture
            .db
            .execute(&format!("DROP TABLE {}", fixture.source))
            .await;
        assert!(
            retirement_pending(&fixture).await,
            "source-drop hook must retain exact slot provenance"
        );
        let phase = "slot_retirement_kept";
        let (mut barrier, pid) = hold_wal_barrier(&fixture.db, fixture.oid, phase).await;
        set_wal_phase(&fixture.db, fixture.oid, phase, &fixture.slot).await;
        fixture
            .db
            .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
            .await;
        wait_for_wal_barrier(&fixture.db, fixture.oid, phase, pid).await;
        assert!(
            slot_present(&fixture.db, &fixture.slot).await,
            "source removal cannot retire a surviving WAL consumer's slot"
        );
        fixture
            .db
            .alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
            .await;
        clear_wal_phase(&fixture.db).await;
        release_wal_barrier(&mut barrier, fixture.oid, phase).await;
        drop(barrier);
        let mut caller = fixture.db.pool.begin().await.unwrap();
        sqlx::query("SELECT pgtrickle.drop_stream_table($1)")
            .bind(&fixture.stream)
            .execute(&mut *caller)
            .await
            .unwrap();
        caller.rollback().await.unwrap();
        assert!(slot_present(&fixture.db, &fixture.slot).await);
        assert!(retirement_pending(&fixture).await);
        fixture
            .db
            .execute(&format!(
                "SELECT pgtrickle.drop_stream_table('{}')",
                fixture.stream
            ))
            .await;
        fixture
            .db
            .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
            .await;
        wait_for_retirement(&fixture).await;
    }
}

#[tokio::test]
async fn test_deferred_consumer_registration_rollback_preserves_wal_slot() {
    let fixture = setup_wal_fixture("deferred_rollback").await;
    disable_wal_scheduler(&fixture).await;
    let baseline = lifecycle_state(&fixture.db.pool, &fixture.stream).await;
    let mut caller = fixture.db.pool.begin().await.unwrap();
    sqlx::query(AssertSqlSafe(format!("SELECT pgtrickle.create_stream_table(name=>'deferred_abandoned',query=>'SELECT id,value FROM {}',schedule=>'24h',refresh_mode=>'DIFFERENTIAL',initialize=>false)",fixture.source)))
        .execute(&mut *caller).await.unwrap();
    caller.rollback().await.unwrap();
    assert!(slot_present(&fixture.db, &fixture.slot).await);
    assert!(
        !retirement_pending(&fixture).await,
        "rollback must discard retirement intent"
    );
    assert_eq!(
        cdc_mode(&fixture.db, &fixture.source).await.as_deref(),
        Some("WAL")
    );
    assert_eq!(
        lifecycle_state(&fixture.db.pool, &fixture.stream).await,
        baseline
    );
}

#[tokio::test]
async fn test_replacement_source_cannot_reassign_pending_slot_retirement() {
    let fixture = setup_wal_fixture("retirement_replacement").await;
    disable_wal_scheduler(&fixture).await;
    fixture
        .db
        .execute(&format!("DROP TABLE {}", fixture.source))
        .await;
    let original:(i64,String)=sqlx::query_as("SELECT source_relid::bigint,capture_instance_id FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name=$1")
        .bind(&fixture.slot).fetch_one(&fixture.db.pool).await.unwrap();
    assert_eq!(original.0, fixture.oid);
    fixture
        .db
        .execute(&format!(
            "CREATE TABLE {}(id int PRIMARY KEY,value text NOT NULL)",
            fixture.source
        ))
        .await;
    fixture
        .db
        .execute(&format!(
            "ALTER TABLE {} REPLICA IDENTITY FULL",
            fixture.source
        ))
        .await;
    fixture
        .db
        .execute(&format!(
            "INSERT INTO {} VALUES(1,'replacement')",
            fixture.source
        ))
        .await;
    let replacement_oid = i64::from(fixture.db.table_oid(&fixture.source).await);
    assert_ne!(replacement_oid, fixture.oid);
    let registration=sqlx::query(AssertSqlSafe(format!("SELECT pgtrickle.create_stream_table(name=>'retirement_replacement_new',query=>'SELECT id,value FROM {}',schedule=>'24h',refresh_mode=>'DIFFERENTIAL',cdc_mode=>'wal')",fixture.source)))
        .execute(&fixture.db.pool).await;
    let after:(i64,String)=sqlx::query_as("SELECT source_relid::bigint,capture_instance_id FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name=$1")
        .bind(&fixture.slot).fetch_one(&fixture.db.pool).await.unwrap();
    let state:String=fixture.db.query_scalar("SELECT jsonb_build_object(
        'dependencies',(SELECT jsonb_agg(to_jsonb(d)) FROM pgtrickle.pgt_dependencies d),
        'tracking',(SELECT jsonb_agg(to_jsonb(t)) FROM pgtrickle.pgt_change_tracking t),
        'slots',(SELECT jsonb_agg(jsonb_build_object('slot_name',slot_name,'plugin',plugin,'database',database,'confirmed_flush_lsn',confirmed_flush_lsn)) FROM pg_replication_slots)
    )::text").await;
    let cleanup = sqlx::query("SELECT pgtrickle.drop_stream_table($1)")
        .bind(&fixture.stream)
        .execute(&fixture.db.pool)
        .await;
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        std::path::Path::new(&directory).join("slot-retirement-source-replacement.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
            "old_oid":fixture.oid,"replacement_oid":replacement_oid,"slot":fixture.slot,
        "state_after_registration":serde_json::from_str::<serde_json::Value>(&state).unwrap(),
            "intent_before":original,"intent_after_registration":after,
            "registration_error":registration.as_ref().err().map(ToString::to_string),
            "old_consumer_cleanup_error":cleanup.as_ref().err().map(ToString::to_string),
        }))
        .unwrap(),
    )
    .unwrap();
    assert!(
        cleanup.is_ok(),
        "replacement registration must preserve old-consumer cleanup: {cleanup:?}"
    );
    assert!(
        registration.as_ref().err().is_some_and(|error| error
            .to_string()
            .contains("pending reservation for another source or capture owner")),
        "a replacement cannot take over a pending slot reservation"
    );
    assert_eq!(
        after, original,
        "a committed retirement intent cannot change source or owner"
    );
    assert!(slot_present(&fixture.db, &fixture.slot).await);
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    wait_for_retirement(&fixture).await;
    fixture.db.execute(&format!("SELECT pgtrickle.create_stream_table(name=>'retirement_replacement_new',query=>'SELECT id,value FROM {}',schedule=>'24h',refresh_mode=>'DIFFERENTIAL',cdc_mode=>'wal')",fixture.source)).await;
    wait_for_cdc_mode(&fixture.db, &fixture.source, "WAL").await;
    fixture
        .db
        .execute(&format!(
            "INSERT INTO {} VALUES(2,'replacement-after-wal')",
            fixture.source
        ))
        .await;
    wait_for_receipt(
        &fixture.db.pool,
        replacement_oid,
        "ACKNOWLEDGED",
        "replacement-after-wal",
    )
    .await;
    fixture.db.refresh_st("retirement_replacement_new").await;
    fixture
        .db
        .assert_st_matches_query(
            "retirement_replacement_new",
            &format!("SELECT id,value FROM {}", fixture.source),
        )
        .await;
}

#[tokio::test]
async fn test_slot_retirement_keeps_null_named_reference_across_source_oids() {
    let fixture = setup_wal_fixture("retirement_cross_oid").await;
    disable_wal_scheduler(&fixture).await;
    fixture
        .db
        .execute(&format!("DROP TABLE {}", fixture.source))
        .await;
    fixture
        .db
        .execute(&format!(
            "CREATE TABLE {}(id int PRIMARY KEY,value text)",
            fixture.source
        ))
        .await;
    let replacement_oid = i64::from(fixture.db.table_oid(&fixture.source).await);
    assert_ne!(replacement_oid, fixture.oid);
    // Exercise the legacy catalog shape explicitly; a dropped source normally
    // retains its original slot_name until retirement is committed.
    fixture
        .db
        .execute(&format!(
            "UPDATE pgtrickle.pgt_dependencies SET slot_name=NULL WHERE source_relid={}::oid",
            fixture.oid
        ))
        .await;
    assert!(fixture.db.query_scalar::<bool>(&format!("SELECT cdc_mode='WAL' AND slot_name IS NULL AND source_stable_name IS NOT NULL FROM pgtrickle.pgt_dependencies WHERE source_relid={}::oid",fixture.oid)).await);
    // Qualify the reassigned-intent shape observed on e0008f1 independently of new admission.
    fixture.db.execute(&format!("UPDATE pgtrickle.pgt_wal_slot_retirements SET source_relid={replacement_oid}::oid WHERE slot_name='{}'",fixture.slot)).await;
    let phase = "slot_retirement_kept";
    let (mut barrier, pid) = hold_wal_barrier(&fixture.db, replacement_oid, phase).await;
    set_wal_phase(&fixture.db, replacement_oid, phase, &fixture.slot).await;
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    let kept = format!(
        "EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.application_name='pgt_wal_receipt:{replacement_oid}:{phase}' AND {pid}=ANY(pg_blocking_pids(a.pid)))"
    );
    assert!(fixture.db.wait_for_condition("cross-OID retirement decision",&format!(
        "SELECT {kept} OR NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{0}') OR NOT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='{0}')",fixture.slot
    ),WAIT,Duration::from_millis(25)).await);
    let slot_preserved = slot_present(&fixture.db, &fixture.slot).await;
    let intent_preserved = retirement_pending(&fixture).await;
    let kept_witness = fixture
        .db
        .query_scalar::<bool>(&format!("SELECT {kept}"))
        .await;
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(std::path::Path::new(&directory).join("slot-retirement-cross-oid-reference.json"),serde_json::to_vec_pretty(&serde_json::json!({
        "installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
        "old_oid":fixture.oid,"queued_oid":replacement_oid,"slot":fixture.slot,
        "old_dependency":"WAL with NULL slot_name and durable stable name",
        "slot_preserved":slot_preserved,"intent_preserved":intent_preserved,"kept_witness":kept_witness,
    })).unwrap()).unwrap();
    assert!(
        slot_preserved && intent_preserved && kept_witness,
        "a cross-OID NULL-name WAL reference must prevent actual retirement"
    );
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    fixture
        .db
        .execute(&format!(
            "SELECT pgtrickle.drop_stream_table('{}')",
            fixture.stream
        ))
        .await;
    clear_wal_phase(&fixture.db).await;
    release_wal_barrier(&mut barrier, replacement_oid, phase).await;
    drop(barrier);
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    wait_for_retirement(&fixture).await;
}

#[tokio::test]
async fn test_slot_adoption_rejects_removed_consumer_and_recovers_orphan_after_crash() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    configure_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    for sql in [
        "CREATE TABLE retirement_orphan_source(id int PRIMARY KEY,value text)",
        "ALTER TABLE retirement_orphan_source REPLICA IDENTITY FULL",
        "INSERT INTO retirement_orphan_source VALUES(1,'baseline')",
    ] {
        db.execute(sql).await;
    }
    db.execute("SELECT pgtrickle.create_stream_table(name=>'retirement_orphan_stream',query=>'SELECT id,value FROM retirement_orphan_source',schedule=>'24h',refresh_mode=>'DIFFERENTIAL',cdc_mode=>'wal')").await;
    let oid = i64::from(db.table_oid("retirement_orphan_source").await);
    let stable: String = db
        .query_scalar(&format!("SELECT pgtrickle.source_stable_name({oid}::oid)"))
        .await;
    let slot = format!("pgtrickle_{stable}");
    let (mut before, before_pid) = hold_wal_barrier(&db, oid, "before_slot_adoption").await;
    set_wal_phase(&db, oid, "before_slot_adoption", &slot).await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    wait_for_wal_barrier(&db, oid, "before_slot_adoption", before_pid).await;
    assert!(
        slot_present(&db, &slot).await,
        "pristine creation must precede the witness"
    );
    assert!(db.query_scalar::<bool>(&format!("SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='{slot}')")).await,
        "creation must already have a committed reservation");
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    db.execute("SELECT pgtrickle.drop_stream_table('retirement_orphan_stream')")
        .await;
    release_wal_barrier(&mut before, oid, "before_slot_adoption").await;
    drop(before);
    let rejection = format!(
        "slot '{slot}' reservation no longer matches current source, consumer or capture ownership"
    );
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let logs = tokio::process::Command::new("docker")
            .args(["logs", "--tail", "100", db.container_id()])
            .output()
            .await
            .unwrap();
        assert!(logs.status.success(), "inspect actual adoption rejection");
        if String::from_utf8_lossy(&logs.stderr).contains(&rejection) {
            eprintln!("[slot adoption] rejected removed consumer: {rejection}");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker did not reject stale adoption: {}",
            String::from_utf8_lossy(&logs.stderr)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    clear_wal_phase(&db).await;
    for args in [
        vec!["kill", "--signal", "KILL", db.container_id()],
        vec!["start", db.container_id()],
    ] {
        let output = tokio::process::Command::new("docker")
            .args(args)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "orphan retained crash/restart failed"
        );
    }
    db.pool = db.reconnect_after_restart().await;
    assert!(slot_present(&db, &slot).await);
    assert!(db.query_scalar::<bool>(&format!("SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='{slot}')")).await);
    db.alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    assert!(db.wait_for_condition("orphan retired after rejected adoption",&format!(
        "SELECT NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{slot}') AND NOT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_slot_retirements WHERE slot_name='{slot}')"
    ),WAIT,Duration::from_millis(25)).await);
}

#[tokio::test]
async fn test_last_wal_consumer_drop_rollback_preserves_slot_and_frontier() {
    let fixture = setup_wal_fixture("drop_rollback").await;
    fixture
        .db
        .alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    assert!(fixture.db.wait_for_condition("idle WAL slot before rollback", &format!(
        "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{}' AND NOT active)",fixture.slot
    ), WAIT, Duration::from_millis(25)).await);
    let baseline = lifecycle_state(&fixture.db.pool, &fixture.stream).await;
    let mut caller = fixture.db.pool.begin().await.unwrap();
    sqlx::query("SELECT pgtrickle.drop_stream_table($1)")
        .bind(&fixture.stream)
        .execute(&mut *caller)
        .await
        .unwrap();
    caller.rollback().await.unwrap();
    let restored = lifecycle_state(&fixture.db.pool, &fixture.stream).await;
    let mode = cdc_mode(&fixture.db, &fixture.source).await;
    let slot_preserved: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1 AND plugin='test_decoding')")
        .bind(&fixture.slot).fetch_one(&fixture.db.pool).await.unwrap();
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        std::path::Path::new(&directory).join("slot-drop-rollback.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
            "baseline":baseline,"restored":restored,"mode":mode,"slot":fixture.slot,
            "slot_preserved":slot_preserved,"scheduler_disabled":true,"slot_was_idle":true,
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        restored, baseline,
        "rollback must restore rows and frontier"
    );
    assert_eq!(mode.as_deref(), Some("WAL"));
    assert!(
        slot_preserved,
        "rollback must preserve the restored consumer's WAL retention boundary"
    );
}

#[tokio::test]
async fn test_consumer_fixed_snapshot_rejection_preserves_existing_wal_slot() {
    let fixture = setup_wal_fixture("registration_reject").await;
    let mut rejected = Vec::new();
    for isolation in ["REPEATABLE READ", "SERIALIZABLE"] {
        let mut caller = fixture.db.pool.begin().await.unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation}"
        )))
        .execute(&mut *caller)
        .await
        .unwrap();
        let _: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT count(*) FROM {}",
            fixture.source
        )))
        .fetch_one(&mut *caller)
        .await
        .unwrap();
        let error = sqlx::query(AssertSqlSafe(format!("SELECT pgtrickle.create_stream_table(name=>'registration_rejected',query=>'SELECT id,value FROM {}',schedule=>'24h',refresh_mode=>'DIFFERENTIAL')",fixture.source)))
            .execute(&mut *caller).await.expect_err("reject before CDC setup can drop the existing slot").to_string();
        assert!(error.contains("READ COMMITTED"), "{error}");
        caller.rollback().await.unwrap();
        let slot_preserved: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1 AND plugin='test_decoding')")
            .bind(&fixture.slot).fetch_one(&fixture.db.pool).await.unwrap();
        assert!(slot_preserved);
        assert_eq!(
            cdc_mode(&fixture.db, &fixture.source).await.as_deref(),
            Some("WAL")
        );
        rejected.push(serde_json::json!({"isolation":isolation,"error":error,"slot_preserved":slot_preserved}));
    }
    commit_row(
        &fixture.db,
        &fixture.source,
        &fixture.journal,
        2,
        "after-rejected-registration",
    )
    .await;
    wait_for_receipt(
        &fixture.db.pool,
        fixture.oid,
        "ACKNOWLEDGED",
        "after-rejected-registration",
    )
    .await;
    wait_for_exact(&fixture.db.pool, &fixture.stream, &fixture.journal).await;
    let directory = std::env::var("PGT_LIFECYCLE_ARTIFACT_DIR")
        .unwrap_or_else(|_| "artifacts/lifecycle-histories".into());
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(std::path::Path::new(&directory).join("registration-wal-slot.json"),serde_json::to_vec_pretty(&serde_json::json!({
        "installed_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
        "slot":fixture.slot,"rejected":rejected,"subsequent_source_acknowledged":true,"exact_journal":true,
    })).unwrap()).unwrap();
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

/// The first failing history is retained with its seed, mutations and states.
#[tokio::test]
async fn test_lifecycle_seeded_restore_lag_and_crash_preserve_committed_journal() {
    use e2e::property_support::{SeededRng, TraceConfig};
    let config = TraceConfig::from_env();
    let replay_plan = std::env::var("PGT_LIFECYCLE_REPLAY_PLAN").ok().map(|path| {
        let bytes = std::fs::read(&path).expect("read lifecycle replay plan");
        let plan: Vec<LifecycleMutation> =
            serde_json::from_slice(&bytes).expect("parse lifecycle replay plan");
        assert!(
            !plan.is_empty(),
            "lifecycle replay plan must contain a mutation"
        );
        plan
    });
    let cycles = replay_plan
        .as_ref()
        .map_or(config.cycles, |plan| plan.len());
    let seeds = config.seeds(0x1219_0001);
    if replay_plan.is_some() {
        assert_eq!(seeds.len(), 1, "replay plans require one fixed seed");
    }
    for seed in seeds {
        let db = E2eDb::new_dedicated().await.with_extension().await;
        configure_scheduler(&db).await;
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
                "seed":seed, "cycles":cycles, "revision":String::from_utf8(revision.stdout).unwrap().trim(),
                "server_candidate":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").unwrap_or_else(|_|"unbound".into()),
                "configuration":{"capture":"trigger","trigger_mode":mode,"capture_handoff":"trigger-to-WAL after generated cycles","compact_threshold":1},
                "initial_rows":if seed & 1 != 0 {serde_json::json!([[0,"baseline"]])} else {serde_json::json!([])},
                "query":"SELECT id,value FROM history_source",
                "container":db.container_id(), "minimization":"preserve the first failing trace; shrink_recovery_history.py reduces mutation cycles, operation kinds and operands while retaining the tagged invariant",
            }),
        );
        db.execute("CREATE TABLE history_source(id INT PRIMARY KEY,value TEXT NOT NULL)")
            .await;
        db.execute("ALTER TABLE history_source REPLICA IDENTITY FULL")
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
        for cycle in 1..=cycles {
            let mutation = if let Some(plan) = &replay_plan {
                plan[cycle - 1]
            } else {
                let value = rng.i32_range(-1000, 1000);
                let choice = rng.usize_range(0, 2);
                if ids.is_empty() || choice == 0 {
                    let id = i32::try_from(cycle).unwrap();
                    ids.push(id);
                    LifecycleMutation {
                        kind: LifecycleMutationKind::Insert,
                        id,
                        value,
                    }
                } else {
                    let index = rng.usize_range(0, ids.len() - 1);
                    let id = ids[index];
                    if choice == 2 && ids.len() > 1 {
                        ids.remove(index);
                        LifecycleMutation {
                            kind: LifecycleMutationKind::Delete,
                            id,
                            value,
                        }
                    } else {
                        LifecycleMutation {
                            kind: LifecycleMutationKind::Update,
                            id,
                            value,
                        }
                    }
                }
            };
            let sql = format!(
                "BEGIN; {}; {}; SAVEPOINT abandoned; \
                 UPDATE history_source SET value='abandoned'; ROLLBACK TO abandoned; COMMIT",
                mutation.sql(&fixture.source),
                mutation.sql(&fixture.journal)
            );
            record_lifecycle(
                &path,
                &mut history,
                serde_json::json!({
                    "step":"committed source+journal mutation","cycle":cycle,
                    "mutation":mutation.json(),"sql":sql,
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
                "PGT_LIFECYCLE_FAILURE:abort_atomicity; cycle={cycle}; history: {}",
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
                "PGT_LIFECYCLE_FAILURE:source_cut; cycle={cycle}; history: {}",
                path.display()
            );
            assert_eq!(
                lifecycle_state(&fixture.db.pool, "history_slow").await,
                slow_baseline,
                "PGT_LIFECYCLE_FAILURE:slow_frontier; cycle={cycle}; history: {}",
                path.display()
            );
        }
        let fast_before_handoff = lifecycle_state(&fixture.db.pool, "history_fast").await;
        commit_row(
            &fixture.db,
            &fixture.source,
            &fixture.journal,
            -1,
            "handoff-pending",
        )
        .await;
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({
                "step":"trigger-captured commit before WAL transition",
                "id":-1,"value":"handoff-pending",
            }),
        );
        fixture
            .db
            .alter_system_set_and_wait("pg_trickle.cdc_mode", "'auto'", "auto")
            .await;
        fixture
            .db
            .alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
            .await;
        wait_for_cdc_mode(&fixture.db, &fixture.source, "WAL").await;
        commit_row(
            &fixture.db,
            &fixture.source,
            &fixture.journal,
            -2,
            "after-wal",
        )
        .await;
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({
                "step":"WAL-captured commit after transition",
                "id":-2,"value":"after-wal",
            }),
        );
        assert!(
            fixture
                .db
                .wait_for_condition(
                    "acknowledged post-transition WAL receipt",
                    &format!(
                        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_wal_receipts \
                         WHERE source_relid={}::oid AND source_change \
                           AND status='ACKNOWLEDGED' AND data LIKE '%after-wal%')",
                        fixture.oid
                    ),
                    WAIT,
                    Duration::from_millis(50),
                )
                .await,
            "PGT_LIFECYCLE_FAILURE:handoff_receipt; history: {}",
            path.display()
        );
        let handoff_mode = cdc_mode(&fixture.db, &fixture.source).await;
        assert_eq!(
            handoff_mode.as_deref(),
            Some("WAL"),
            "PGT_LIFECYCLE_FAILURE:handoff_mode; history: {}",
            path.display()
        );
        let fast_after_handoff = lifecycle_state(&fixture.db.pool, "history_fast").await;
        assert_eq!(
            fast_after_handoff["rows"],
            fast_before_handoff["rows"],
            "PGT_LIFECYCLE_FAILURE:handoff_output; history: {}",
            path.display()
        );
        assert_eq!(
            fast_after_handoff["frontier"],
            fast_before_handoff["frontier"],
            "PGT_LIFECYCLE_FAILURE:handoff_frontier; history: {}",
            path.display()
        );
        assert_eq!(
            lifecycle_state(&fixture.db.pool, "history_slow").await,
            slow_baseline,
            "PGT_LIFECYCLE_FAILURE:handoff_slow_frontier; history: {}",
            path.display()
        );
        record_lifecycle(
            &path,
            &mut history,
            serde_json::json!({
                "step":"trigger-to-WAL handoff witnessed",
                "capture":handoff_mode,"fast_state":fast_after_handoff,
                "slow_state":slow_baseline,
            }),
        );
        fixture
            .db
            .alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
            .await;
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
            "PGT_LIFECYCLE_FAILURE:restore_pair; history: {}",
            path.display()
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
        )).await, "PGT_LIFECYCLE_FAILURE:restored_source_cut; history: {}", path.display());
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
            fixture.identity,
            "PGT_LIFECYCLE_FAILURE:retained_identity; history: {}",
            path.display()
        );
        assert_eq!(
            lifecycle_state(&fixture.db.pool, "history_fast").await,
            before_crash,
            "PGT_LIFECYCLE_FAILURE:retained_checkpoint; history: {}",
            path.display()
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
        )).await, "PGT_LIFECYCLE_FAILURE:lagging_source_cut; history: {}", path.display());
    }
}
