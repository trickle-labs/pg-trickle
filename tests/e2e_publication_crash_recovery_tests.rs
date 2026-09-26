//! Real publisher-to-subscriber recovery qualification for downstream publications.

mod e2e;

use e2e::E2eDb;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{
    process::Stdio,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const WAIT: Duration = Duration::from_secs(120);
const PASSWORD: &str = "postgres";
static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

fn unique_suffix() -> String {
    format!(
        "{}_{}_{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after epoch")
            .subsec_nanos()
    )
}

struct DockerNetwork {
    name: String,
}

impl DockerNetwork {
    async fn create() -> Self {
        let name = format!("pgt_pub_{}", unique_suffix());
        let status = tokio::process::Command::new("docker")
            .args(["network", "create", &name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .expect("create Docker network");
        assert!(status.success(), "docker network create failed");
        Self { name }
    }
}

impl Drop for DockerNetwork {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["network", "rm", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

struct PgServer {
    name: String,
    port: u16,
    pool: PgPool,
    container_id: String,
}

impl PgServer {
    async fn start(name: &str, network: &str) -> Self {
        let configured_image =
            std::env::var("PGS_E2E_IMAGE").unwrap_or_else(|_| "pg_trickle_e2e:latest".into());
        let (image, tag) = configured_image
            .split_once(':')
            .map(|(image, tag)| (image.to_string(), tag.to_string()))
            .unwrap_or_else(|| (configured_image, "latest".into()));
        let output = tokio::process::Command::new("docker")
            .args([
                "run",
                "--detach",
                "--name",
                name,
                "--network",
                network,
                "--publish",
                "127.0.0.1::5432",
                "--env",
                "POSTGRES_PASSWORD=postgres",
                "--env",
                "POSTGRES_DB=postgres",
                &format!("{image}:{tag}"),
            ])
            .output()
            .await
            .expect("start PostgreSQL container");
        assert!(
            output.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let container_id = String::from_utf8(output.stdout)
            .expect("Docker container ID is UTF-8")
            .trim()
            .to_string();
        let port = mapped_port(&container_id).await;
        let pool = connect_with_retry(port).await;
        attest_candidate_install(&container_id);
        Self {
            name: name.to_string(),
            port,
            pool,
            container_id,
        }
    }

    async fn stop(&mut self, crash: bool) {
        self.pool.close().await;
        let action = if crash { "kill" } else { "stop" };
        let mut command = tokio::process::Command::new("docker");
        if crash {
            command.args(["kill", "--signal", "KILL", &self.container_id]);
        } else {
            command.args(["stop", &self.container_id]);
        }
        let output = command.output().await.expect("stop PostgreSQL container");
        assert!(
            output.status.success(),
            "docker {action} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    async fn start_container(&mut self) {
        let output = tokio::process::Command::new("docker")
            .args(["start", &self.container_id])
            .output()
            .await
            .expect("restart PostgreSQL container");
        assert!(
            output.status.success(),
            "docker start failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        self.port = mapped_port(&self.container_id).await;
        self.pool = connect_with_retry(self.port).await;
    }

    async fn restart(&mut self, crash: bool) {
        self.stop(crash).await;
        self.start_container().await;
    }

    async fn identity(&self) -> ServerIdentity {
        let (system_identifier, data_directory, postmaster_start): (String, String, String) =
            sqlx::query_as("SELECT system_identifier::text, current_setting('data_directory'), pg_postmaster_start_time()::text FROM pg_control_system()")
                .fetch_one(&self.pool).await.expect("read PostgreSQL data identity");
        ServerIdentity {
            container_id: self.container_id.clone(),
            system_identifier,
            data_directory,
            postmaster_start,
        }
    }

    async fn emit_logs(&self, role: &str) {
        let output = tokio::process::Command::new("docker")
            .args(["logs", "--tail", "80", &self.container_id])
            .output()
            .await
            .expect("capture PostgreSQL logs");
        eprintln!(
            "[Q1096] server={role} name={} container={} logs_begin\n{}{}[Q1096] server={role} logs_end",
            self.name,
            self.container_id,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

impl Drop for PgServer {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", &self.container_id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

struct ServerIdentity {
    container_id: String,
    system_identifier: String,
    data_directory: String,
    postmaster_start: String,
}

fn assert_retained_restart(before: &ServerIdentity, after: &ServerIdentity) {
    assert_eq!(
        before.container_id, after.container_id,
        "restart must keep the same container"
    );
    assert_eq!(
        before.system_identifier, after.system_identifier,
        "restart must keep the same PostgreSQL cluster"
    );
    assert_eq!(
        before.data_directory, after.data_directory,
        "restart must keep the same data directory"
    );
    assert_ne!(
        before.postmaster_start, after.postmaster_start,
        "a new postmaster must start"
    );
}

struct Pair {
    publisher: PgServer,
    subscriber: PgServer,
    network: DockerNetwork,
    source: String,
    stream: String,
    publication: String,
    subscription: String,
    slot: String,
}

impl Pair {
    async fn new(case: &str) -> Self {
        let network = DockerNetwork::create().await;
        let stem = format!("p1096_{}_{}", case, unique_suffix().replace('_', ""));
        let publisher = PgServer::start(&format!("{stem}_pub"), &network.name).await;
        let subscriber = PgServer::start(&format!("{stem}_sub"), &network.name).await;
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
            .execute(&publisher.pool)
            .await
            .expect("install pg_trickle on publisher");
        let source = format!("{stem}_source");
        let stream = format!("{stem}_stream");
        let subscription = format!("{stem}_sub");
        execute(&publisher.pool, &format!("CREATE TABLE public.{source} (id INT PRIMARY KEY, category TEXT NOT NULL, amount INT NOT NULL)")).await;
        execute(
            &publisher.pool,
            &format!("INSERT INTO public.{source} VALUES (1, 'a', 10), (2, 'b', 20)"),
        )
        .await;
        sqlx::query("SELECT pgtrickle.create_stream_table($1, $2, '1h', 'DIFFERENTIAL')")
            .bind(&stream)
            .bind(format!("SELECT id, category, amount FROM public.{source}"))
            .execute(&publisher.pool)
            .await
            .expect("create and initialize stream table");
        sqlx::query("SELECT pgtrickle.stream_table_to_publication($1)")
            .bind(format!("public.{stream}"))
            .execute(&publisher.pool)
            .await
            .expect("register stream table publication");
        let publication: String = sqlx::query_scalar(
            "SELECT p.pubname::text FROM pg_publication p JOIN pg_publication_rel pr ON pr.prpubid = p.oid WHERE pr.prrelid = $1::regclass")
            .bind(format!("public.{stream}")).fetch_one(&publisher.pool).await.expect("resolve actual stream-table publication");
        execute(&subscriber.pool, &format!("CREATE TABLE public.{stream} (__pgt_row_id BYTEA NOT NULL, id INT, category TEXT NOT NULL, amount INT NOT NULL)")).await;
        let identity_index = format!("{stream}_row_id_idx");
        execute(
            &subscriber.pool,
            &format!("CREATE UNIQUE INDEX {identity_index} ON public.{stream} (__pgt_row_id)"),
        )
        .await;
        execute(
            &subscriber.pool,
            &format!("ALTER TABLE public.{stream} REPLICA IDENTITY USING INDEX {identity_index}"),
        )
        .await;
        let conn = format!(
            "host={} port=5432 dbname=postgres user=postgres password={} connect_timeout=3",
            publisher.name, PASSWORD
        );
        let ddl = format!(
            "CREATE SUBSCRIPTION {subscription} CONNECTION '{}' PUBLICATION \"{publication}\"",
            conn.replace('\'', "''")
        );
        sqlx::query(sqlx::AssertSqlSafe(ddl))
            .execute(&subscriber.pool)
            .await
            .expect("create real PostgreSQL subscription");
        let slot: String =
            sqlx::query_scalar("SELECT subslotname::text FROM pg_subscription WHERE subname = $1")
                .bind(&subscription)
                .fetch_one(&subscriber.pool)
                .await
                .expect("read logical replication slot name");
        let pair = Self {
            publisher,
            subscriber,
            network,
            source,
            stream,
            publication,
            subscription,
            slot,
        };
        pair.wait_ready().await;
        pair.wait_exact(&[(1, "a", 10), (2, "b", 20)]).await;
        eprintln!(
            "[Q1096] case={case} publisher={} subscriber={} publication={} subscription={} slot={} initial_sync=exact",
            pair.publisher.container_id,
            pair.subscriber.container_id,
            pair.publication,
            pair.subscription,
            pair.slot
        );
        pair
    }

    async fn wait_ready(&self) {
        wait_until("subscription table ready", async || {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_subscription_rel WHERE srsubstate = 'r')",
            )
            .fetch_one(&self.subscriber.pool)
            .await
            .unwrap_or(false)
        })
        .await;
    }

    async fn rows(&self) -> Vec<(i32, String, i32)> {
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id, category, amount FROM public.{} ORDER BY id",
            self.stream
        )))
        .fetch_all(&self.subscriber.pool)
        .await
        .expect("read subscriber result")
    }

    async fn wait_exact(&self, expected: &[(i32, &str, i32)]) {
        wait_until("subscriber exact rows", async || {
            rows_match(&self.rows().await, expected)
        })
        .await;
        assert!(
            rows_match(&self.rows().await, expected),
            "subscriber rows differ from exact expected values"
        );
    }

    async fn refresh(&self) {
        sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
            .bind(format!("public.{}", self.stream))
            .execute(&self.publisher.pool)
            .await
            .expect("refresh published stream table");
    }

    async fn target_lsn(&self) -> String {
        sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
            .fetch_one(&self.publisher.pool)
            .await
            .expect("read publisher WAL checkpoint")
    }

    async fn slot_is_behind(&self, target: &str) -> bool {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND (confirmed_flush_lsn IS NULL OR confirmed_flush_lsn < $2::pg_lsn))")
            .bind(&self.slot).bind(target).fetch_one(&self.publisher.pool).await.expect("inspect publisher slot backlog")
    }

    async fn wait_slot_behind(&self, target: &str) {
        wait_until("logical slot backlog", async || {
            self.slot_is_behind(target).await
        })
        .await;
    }

    async fn wait_slot_caught_up(&self, target: &str) {
        wait_until("logical slot catch-up", async || {
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND confirmed_flush_lsn >= $2::pg_lsn)")
                .bind(&self.slot).bind(target).fetch_one(&self.publisher.pool).await.unwrap_or(false)
        }).await;
    }

    async fn emit_evidence(&self) {
        let publisher_identity = self.publisher.identity().await;
        let subscriber_identity = self.subscriber.identity().await;
        eprintln!(
            "[Q1096] data publisher={}:{}:{} subscriber={}:{}:{} publication={} slot={} network={}",
            publisher_identity.container_id,
            publisher_identity.system_identifier,
            publisher_identity.data_directory,
            subscriber_identity.container_id,
            subscriber_identity.system_identifier,
            subscriber_identity.data_directory,
            self.publication,
            self.slot,
            self.network.name
        );
        self.publisher.emit_logs("publisher").await;
        self.subscriber.emit_logs("subscriber").await;
    }
}

async fn connect_with_retry(port: u16) -> PgPool {
    let url = format!("postgres://postgres:{PASSWORD}@127.0.0.1:{port}/postgres?sslmode=disable");
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(2))
            .connect(&url)
            .await
        {
            Ok(pool) => return pool,
            Err(error) if tokio::time::Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => panic!("PostgreSQL did not accept connections: {error}"),
        }
    }
}

async fn mapped_port(container_id: &str) -> u16 {
    let output = tokio::process::Command::new("docker")
        .args(["port", container_id, "5432/tcp"])
        .output()
        .await
        .expect("read mapped PostgreSQL port");
    assert!(
        output.status.success(),
        "docker port failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Docker port mapping is UTF-8")
        .trim()
        .rsplit(':')
        .next()
        .expect("mapped port is present")
        .parse()
        .expect("mapped port is numeric")
}

async fn wait_until(label: &str, mut condition: impl std::ops::AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if condition().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {label}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn execute(pool: &PgPool, sql: &str) {
    sqlx::query(sqlx::AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .unwrap_or_else(|error| panic!("SQL failed: {error}\nSQL: {sql}"));
}

fn rows_match(actual: &[(i32, String, i32)], expected: &[(i32, &str, i32)]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(a, e)| a.0 == e.0 && a.1 == e.1 && a.2 == e.2)
}

fn attest_candidate_install(container_id: &str) {
    let (Some(output), Some(candidate_root)) = (
        std::env::var_os("PGT_RELEASE_ATTESTATION_PATH"),
        std::env::var_os("PGT_EXTENSION_DIR"),
    ) else {
        return;
    };
    let result = std::process::Command::new("python3")
        .args([
            "scripts/release_install_attestation.py",
            "--container",
            container_id,
            "--candidate-root",
        ])
        .arg(candidate_root)
        .args(["--output"])
        .arg(output)
        .output()
        .expect("run candidate installation attestation");
    assert!(
        result.status.success(),
        "candidate installation attestation failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[tokio::test]
async fn test_err4_publisher_connection_recovery_smoke() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE err4_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO err4_src SELECT g, 'initial_' || g FROM generate_series(1, 100) g")
        .await;
    db.create_st(
        "err4_st",
        "SELECT id, val FROM err4_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // This legacy smoke test checks publisher connection recovery only. Publication
    // registration remains optional here; it does not create or restart a subscriber.
    let registered = db
        .try_execute("SELECT pgtrickle.stream_table_to_publication('err4_st')")
        .await
        .is_ok();
    db.execute("INSERT INTO err4_src SELECT g, 'batch1_' || g FROM generate_series(101, 200) g")
        .await;
    db.refresh_st("err4_st").await;
    let _: i64 = db.query_scalar("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE state='idle' AND pid<>pg_backend_pid() AND application_name NOT LIKE 'pgtrickle%'").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    db.execute("INSERT INTO err4_src SELECT g, 'batch2_' || g FROM generate_series(201, 300) g")
        .await;
    db.refresh_st("err4_st").await;
    assert_eq!(
        db.count("public.err4_st").await,
        300,
        "publisher stream table rows survive connection-pool recovery"
    );
    assert_eq!(
        db.query_scalar::<i64>("SELECT count(DISTINCT id) FROM public.err4_st")
            .await,
        300
    );
    if registered {
        let count: i64 = db
            .query_scalar("SELECT count(*) FROM pg_publication WHERE pubname LIKE '%err4_st%'")
            .await;
        assert!(count > 0, "registered publication remains present");
    }
}

#[tokio::test]
async fn test_publication_recovery_initial_sync_dml_and_setup_failures() {
    let pair = Pair::new("sync").await;
    execute(
        &pair.publisher.pool,
        &format!("UPDATE public.{} SET amount = 11 WHERE id = 1", pair.source),
    )
    .await;
    execute(
        &pair.publisher.pool,
        &format!("DELETE FROM public.{} WHERE id = 2", pair.source),
    )
    .await;
    execute(
        &pair.publisher.pool,
        &format!("INSERT INTO public.{} VALUES (3, 'a', 30)", pair.source),
    )
    .await;
    pair.refresh().await;
    pair.wait_exact(&[(1, "a", 11), (3, "a", 30)]).await;
    execute(
        &pair.publisher.pool,
        &format!("INSERT INTO public.{} VALUES (4, 'a', 40)", pair.source),
    )
    .await;
    pair.refresh().await;
    pair.wait_exact(&[(1, "a", 11), (3, "a", 30), (4, "a", 40)])
        .await;

    let missing =
        sqlx::query("SELECT pgtrickle.stream_table_to_publication('missing_stream_table')")
            .execute(&pair.publisher.pool)
            .await;
    assert!(
        missing.is_err(),
        "missing stream table registration must fail explicitly"
    );
    let bad_subscription = format!(
        "CREATE SUBSCRIPTION p1096_bad CONNECTION 'host=127.0.0.1 port=1 dbname=postgres user=postgres connect_timeout=1' PUBLICATION \"{}\"",
        pair.publication
    );
    assert!(
        sqlx::query(sqlx::AssertSqlSafe(bad_subscription))
            .execute(&pair.subscriber.pool)
            .await
            .is_err(),
        "unreachable publisher setup must fail explicitly"
    );
    let orphan_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_subscription WHERE subname = 'p1096_bad'")
            .fetch_one(&pair.subscriber.pool)
            .await
            .expect("check failed subscription cleanup");
    assert_eq!(
        orphan_count, 0,
        "failed setup must not leave a subscriber or publisher-only fallback"
    );
    pair.emit_evidence().await;
}

#[tokio::test]
async fn test_publication_recovery_subscriber_restart_catches_up() {
    let mut pair = Pair::new("subscriber_restart").await;
    let before = pair.subscriber.identity().await;
    pair.subscriber.stop(false).await;
    execute(
        &pair.publisher.pool,
        &format!("INSERT INTO public.{} VALUES (3, 'a', 30)", pair.source),
    )
    .await;
    pair.refresh().await;
    let target = pair.target_lsn().await;
    pair.wait_slot_behind(&target).await;
    eprintln!(
        "[Q1096] subscriber_down slot={} outstanding_lsn={target}",
        pair.slot
    );
    pair.subscriber.start_container().await;
    let after = pair.subscriber.identity().await;
    assert_retained_restart(&before, &after);
    pair.wait_exact(&[(1, "a", 10), (2, "b", 20), (3, "a", 30)])
        .await;
    pair.wait_slot_caught_up(&target).await;
    eprintln!(
        "[Q1096] subscriber_restart exact=true postmaster_before={} postmaster_after={} slot_caught_up=true",
        before.postmaster_start, after.postmaster_start
    );
    pair.emit_evidence().await;
}

#[tokio::test]
async fn test_publication_recovery_publisher_crash_retains_slot_and_catches_up() {
    let mut pair = Pair::new("publisher_restart").await;
    let before = pair.publisher.identity().await;
    pair.subscriber.stop(false).await;
    execute(
        &pair.publisher.pool,
        &format!("INSERT INTO public.{} VALUES (3, 'a', 30)", pair.source),
    )
    .await;
    pair.refresh().await;
    let target = pair.target_lsn().await;
    pair.wait_slot_behind(&target).await;
    pair.publisher.restart(true).await;
    let after = pair.publisher.identity().await;
    assert_retained_restart(&before, &after);
    let publication_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_publication WHERE pubname = $1")
            .bind(&pair.publication)
            .fetch_one(&pair.publisher.pool)
            .await
            .expect("check retained publication");
    let slot_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1 AND slot_type = 'logical'",
    )
    .bind(&pair.slot)
    .fetch_one(&pair.publisher.pool)
    .await
    .expect("check retained logical slot");
    assert_eq!(
        (publication_count, slot_count),
        (1, 1),
        "publisher restart retains publication and subscriber slot"
    );
    pair.subscriber.start_container().await;
    pair.wait_exact(&[(1, "a", 10), (2, "b", 20), (3, "a", 30)])
        .await;
    pair.wait_slot_caught_up(&target).await;
    execute(
        &pair.publisher.pool,
        &format!("INSERT INTO public.{} VALUES (4, 'b', 40)", pair.source),
    )
    .await;
    pair.refresh().await;
    pair.wait_exact(&[(1, "a", 10), (2, "b", 20), (3, "a", 30), (4, "b", 40)])
        .await;
    eprintln!(
        "[Q1096] publisher_crash exact=true publication={} slot={} retained_identity=true postmaster_before={} postmaster_after={}",
        pair.publication, pair.slot, before.postmaster_start, after.postmaster_start
    );
    pair.emit_evidence().await;
}

#[tokio::test]
async fn test_publication_recovery_interrupted_catchup_and_negative_controls() {
    let mut pair = Pair::new("interrupted_catchup").await;
    execute(
        &pair.publisher.pool,
        &format!("INSERT INTO public.{} VALUES (4, 'a', 40)", pair.source),
    )
    .await;
    pair.refresh().await;
    pair.wait_exact(&[(1, "a", 10), (2, "b", 20), (4, "a", 40)])
        .await;

    execute(&pair.subscriber.pool, "CREATE FUNCTION public.p1096_wait_apply() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = 50 THEN PERFORM pg_advisory_xact_lock(1096001); END IF; RETURN NEW; END $$").await;
    execute(&pair.subscriber.pool, &format!("CREATE TRIGGER p1096_wait_apply BEFORE INSERT ON public.{} FOR EACH ROW EXECUTE FUNCTION public.p1096_wait_apply()", pair.stream)).await;
    execute(
        &pair.subscriber.pool,
        &format!(
            "ALTER TABLE public.{} ENABLE ALWAYS TRIGGER p1096_wait_apply",
            pair.stream
        ),
    )
    .await;
    let mut lock = pair
        .subscriber
        .pool
        .acquire()
        .await
        .expect("acquire subscriber barrier connection");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .expect("read barrier PID");
    sqlx::query("SELECT pg_advisory_lock(1096001)")
        .execute(&mut *lock)
        .await
        .expect("hold apply barrier");
    execute(
        &pair.publisher.pool,
        &format!("INSERT INTO public.{} VALUES (50, 'a', 50)", pair.source),
    )
    .await;
    pair.refresh().await;
    let (worker_pid, blockers) = wait_for_blocked_apply(&pair.subscriber.pool).await;
    assert!(
        blockers.contains(&blocker_pid),
        "apply worker must be blocked by the held advisory barrier; observed blockers {blockers:?}, barrier PID {blocker_pid}"
    );
    execute(
        &pair.publisher.pool,
        &format!("INSERT INTO public.{} VALUES (6, 'b', 60)", pair.source),
    )
    .await;
    pair.refresh().await;
    let target = pair.target_lsn().await;
    pair.wait_slot_behind(&target).await;
    let checkpoint = pair.rows().await;
    assert!(
        rows_match(&checkpoint, &[(1, "a", 10), (2, "b", 20), (4, "a", 40)]),
        "first batch remains delivered while later work is blocked"
    );
    eprintln!(
        "[Q1096] catchup_blocked worker_pid={worker_pid} blocker_pid={blocker_pid} delivered_ids=1,2,4 pending_ids=50,6 slot={} target_lsn={target}",
        pair.slot
    );
    let killed = tokio::process::Command::new("docker")
        .args(["kill", "--signal", "KILL", &pair.subscriber.container_id])
        .output()
        .await
        .expect("interrupt subscriber apply");
    assert!(killed.status.success(), "subscriber interruption failed");
    drop(lock);
    pair.subscriber.pool.close().await;
    pair.subscriber.start_container().await;
    pair.wait_exact(&[
        (1, "a", 10),
        (2, "b", 20),
        (4, "a", 40),
        (6, "b", 60),
        (50, "a", 50),
    ])
    .await;
    pair.wait_slot_caught_up(&target).await;
    eprintln!(
        "[Q1096] interrupted_catchup final_checkpoint_lsn={target} exact=true slot_caught_up=true"
    );

    let row_identity: Vec<u8> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_row_id FROM public.{} WHERE id = 4",
        pair.stream
    )))
    .fetch_one(&pair.subscriber.pool)
    .await
    .expect("save exact physical row identity for negative control repair");
    execute(
        &pair.subscriber.pool,
        &format!("DELETE FROM public.{} WHERE id = 4", pair.stream),
    )
    .await;
    assert!(
        !rows_match(
            &pair.rows().await,
            &[
                (1, "a", 10),
                (2, "b", 20),
                (4, "a", 40),
                (6, "b", 60),
                (50, "a", 50)
            ]
        ),
        "exact-result oracle detects a missing row"
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO public.{} (__pgt_row_id, id, category, amount) VALUES ($1, $2, $3, $4)",
        pair.stream
    )))
    .bind(row_identity)
    .bind(4_i32)
    .bind("a")
    .bind(40_i32)
    .execute(&pair.subscriber.pool)
    .await
    .expect("restore exact row after missing-row negative control");
    execute(
        &pair.subscriber.pool,
        &format!(
            "UPDATE public.{} SET amount = 999 WHERE id = 4",
            pair.stream
        ),
    )
    .await;
    assert!(
        !rows_match(
            &pair.rows().await,
            &[
                (1, "a", 10),
                (2, "b", 20),
                (4, "a", 40),
                (6, "b", 60),
                (50, "a", 50)
            ]
        ),
        "exact-result oracle detects an altered value"
    );
    execute(
        &pair.subscriber.pool,
        &format!("UPDATE public.{} SET amount = 40 WHERE id = 4", pair.stream),
    )
    .await;
    let valid_rows = [
        (1, "a", 10),
        (2, "b", 20),
        (4, "a", 40),
        (6, "b", 60),
        (50, "a", 50),
    ];
    assert!(
        rows_match(&pair.rows().await, &valid_rows),
        "manual control repair restores exact subscriber state"
    );

    execute(
        &pair.subscriber.pool,
        &format!("ALTER SUBSCRIPTION {} DISABLE", pair.subscription),
    )
    .await;
    execute(
        &pair.publisher.pool,
        &format!("INSERT INTO public.{} VALUES (7, 'b', 70)", pair.source),
    )
    .await;
    pair.refresh().await;
    let disabled_target = pair.target_lsn().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        rows_match(&pair.rows().await, &valid_rows),
        "disabled subscriber does not make progress"
    );
    assert!(
        pair.slot_is_behind(&disabled_target).await,
        "disabled subscriber leaves a witnessed slot backlog"
    );
    execute(
        &pair.subscriber.pool,
        &format!("ALTER SUBSCRIPTION {} ENABLE", pair.subscription),
    )
    .await;
    pair.wait_exact(&[
        (1, "a", 10),
        (2, "b", 20),
        (4, "a", 40),
        (6, "b", 60),
        (7, "b", 70),
        (50, "a", 50),
    ])
    .await;
    eprintln!(
        "[Q1096] negative_controls missing_row=detected altered_value=detected disabled_progress=detected disabled_slot_backlog=true"
    );
    pair.emit_evidence().await;
}

async fn wait_for_blocked_apply(pool: &PgPool) -> (i32, Vec<i32>) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let witness: Option<(i32, Vec<i32>)> = sqlx::query_as(
            "SELECT pid, pg_blocking_pids(pid) FROM pg_stat_activity WHERE backend_type LIKE 'logical replication%worker' AND wait_event_type = 'Lock' AND wait_event = 'advisory' AND cardinality(pg_blocking_pids(pid)) > 0 LIMIT 1")
            .fetch_optional(pool).await.expect("inspect blocked logical apply worker");
        if let Some(witness) = witness {
            return witness;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "logical replication worker did not reach subscriber barrier"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_err4_publication_registration_failure_is_clean() {
    let db = E2eDb::new().await.with_extension().await;
    let result = db
        .try_execute("SELECT pgtrickle.stream_table_to_publication('nonexistent_st')")
        .await;
    assert!(
        result.is_err(),
        "registration of a nonexistent stream table returns an error"
    );
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pg_publication WHERE pubname LIKE '%nonexistent_st%'")
        .await;
    assert_eq!(count, 0, "failed registration leaves no orphan publication");
}
