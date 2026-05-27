//! PB3 — PgBouncer transaction-mode E2E tests.
//!
//! These tests validate that `pooler_compatibility_mode = true` enables
//! pg_trickle to function correctly through a PgBouncer sidecar running
//! in transaction-pool mode.
//!
//! Infrastructure:
//! - A PostgreSQL 18.3 container with the pg_trickle extension
//! - A PgBouncer container in transaction-pool mode on a shared Docker
//!   network, proxying connections to the PostgreSQL container.
//!
//! Prerequisites: `just build-e2e-image`

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

static DB_COUNTER: AtomicUsize = AtomicUsize::new(1);

fn unique_suffix() -> String {
    format!(
        "{}_{}_{}",
        std::process::id(),
        DB_COUNTER.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

/// A test harness that runs PostgreSQL + PgBouncer on a shared Docker network.
struct PgBouncerTestDb {
    /// Direct connection to PostgreSQL (for admin / setup).
    admin_pool: PgPool,
    /// Connection through PgBouncer in transaction mode.
    bouncer_pool: PgPool,
    /// Direct connection string for tokio_postgres / LISTEN.
    admin_connection_string: String,
    _pg_container: ContainerAsync<GenericImage>,
    _pgb_container: ContainerAsync<GenericImage>,
    _network: DockerNetwork,
}

/// RAII wrapper that creates and cleans up a Docker network.
struct DockerNetwork {
    name: String,
}

impl DockerNetwork {
    async fn create() -> Self {
        let name = format!("pgt_pgb_{}", unique_suffix());
        let status = tokio::process::Command::new("docker")
            .args(["network", "create", &name])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .expect("failed to create Docker network");
        assert!(status.success(), "docker network create failed");
        DockerNetwork { name }
    }
}

impl Drop for DockerNetwork {
    fn drop(&mut self) {
        // Best-effort cleanup; ignore errors
        let name = self.name.clone();
        let _ = std::process::Command::new("docker")
            .args(["network", "rm", &name])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

fn e2e_image() -> (String, String) {
    match std::env::var("PGS_E2E_IMAGE") {
        Ok(val) if !val.is_empty() => {
            if let Some((name, tag)) = val.split_once(':') {
                (name.to_string(), tag.to_string())
            } else {
                (val, "latest".to_string())
            }
        }
        _ => ("pg_trickle_e2e".to_string(), "latest".to_string()),
    }
}

impl PgBouncerTestDb {
    async fn new() -> Self {
        let network = DockerNetwork::create().await;

        let (img_name, img_tag) = e2e_image();
        let pg_hostname = format!("pgt_pg_{}", unique_suffix());

        // ── Start PostgreSQL with the pg_trickle extension ─────────────
        let pg_container = GenericImage::new(img_name, img_tag)
            .with_exposed_port(5432_u16.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_DB", "postgres")
            .with_network(&network.name)
            .with_container_name(&pg_hostname)
            .start()
            .await
            .expect(
                "Failed to start PostgreSQL container. \
                 Did you run `just build-e2e-image` first?",
            );

        let pg_host_port = pg_container
            .get_host_port_ipv4(5432)
            .await
            .expect("failed to get PG host port");

        // Connect directly for admin tasks and CREATE EXTENSION
        let admin_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_host_port}/postgres");
        let admin_pool = connect_with_retry(&admin_url, 15).await;

        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
            .execute(&admin_pool)
            .await
            .expect("CREATE EXTENSION failed");

        // ── Start PgBouncer in transaction mode ────────────────────────
        // edoburu/pgbouncer uses env vars for configuration.
        // `DB_HOST` / `DB_USER` / `DB_PASSWORD` → upstream connection
        // `POOL_MODE = transaction`  → the mode we want to test
        // Default listen port is 5432.
        let pgb_container = GenericImage::new("edoburu/pgbouncer", "latest")
            .with_exposed_port(5432_u16.tcp())
            .with_wait_for(WaitFor::message_on_stderr("process up"))
            .with_env_var("DB_HOST", &pg_hostname)
            .with_env_var("DB_PORT", "5432")
            .with_env_var("DB_USER", "postgres")
            .with_env_var("DB_PASSWORD", "postgres")
            .with_env_var("POOL_MODE", "transaction")
            .with_env_var("MAX_CLIENT_CONN", "100")
            .with_env_var("DEFAULT_POOL_SIZE", "20")
            .with_env_var("AUTH_TYPE", "plain")
            .with_network(&network.name)
            .start()
            .await
            .expect("Failed to start PgBouncer container");

        let pgb_host_port = pgb_container
            .get_host_port_ipv4(5432)
            .await
            .expect("failed to get PgBouncer host port");

        let bouncer_url =
            format!("postgres://postgres:postgres@127.0.0.1:{pgb_host_port}/postgres");
        let bouncer_pool = connect_with_retry(&bouncer_url, 20).await;

        PgBouncerTestDb {
            admin_pool,
            bouncer_pool,
            admin_connection_string: admin_url,
            _pg_container: pg_container,
            _pgb_container: pgb_container,
            _network: network,
        }
    }

    /// Execute SQL on the direct admin connection.
    async fn admin_execute(&self, sql: &str) {
        sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .execute(&self.admin_pool)
            .await
            .unwrap_or_else(|e| panic!("admin SQL failed: {e}\nSQL: {sql}"));
    }

    /// Execute SQL through PgBouncer.
    async fn bouncer_execute(&self, sql: &str) {
        sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .execute(&self.bouncer_pool)
            .await
            .unwrap_or_else(|e| panic!("bouncer SQL failed: {e}\nSQL: {sql}"));
    }

    /// Query a scalar through PgBouncer.
    async fn bouncer_query_scalar<T>(&self, sql: &str) -> T
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + Unpin,
        (T,): for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned()))
            .fetch_one(&self.bouncer_pool)
            .await
            .unwrap_or_else(|e| panic!("bouncer scalar query failed: {e}\nSQL: {sql}"))
    }

    /// Query a scalar on the direct admin connection.
    async fn admin_query_scalar<T>(&self, sql: &str) -> T
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + Unpin,
        (T,): for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned()))
            .fetch_one(&self.admin_pool)
            .await
            .unwrap_or_else(|e| panic!("admin scalar query failed: {e}\nSQL: {sql}"))
    }
}

async fn connect_with_retry(url: &str, max_attempts: u32) -> PgPool {
    for attempt in 1..=max_attempts {
        match PgPoolOptions::new().max_connections(5).connect(url).await {
            Ok(pool) => match sqlx::query("SELECT 1").execute(&pool).await {
                Ok(_) => return pool,
                Err(e) if attempt < max_attempts => {
                    eprintln!(
                        "PgBouncer E2E connect attempt {attempt}/{max_attempts}: ping failed: {e}"
                    );
                }
                Err(e) => panic!("Failed to ping after {max_attempts} attempts: {e}"),
            },
            Err(e) if attempt < max_attempts => {
                eprintln!("PgBouncer E2E connect attempt {attempt}/{max_attempts}: {e}");
            }
            Err(e) => panic!("Failed to connect after {max_attempts} attempts: {e}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    unreachable!()
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Verify we can connect through PgBouncer and query the extension catalog.
#[tokio::test]
async fn test_pgbouncer_basic_connectivity() {
    let db = PgBouncerTestDb::new().await;

    // Direct connection should work
    let direct: i32 = db.admin_query_scalar("SELECT 1").await;
    assert_eq!(direct, 1);

    // PgBouncer connection should work
    let bouncer: i32 = db.bouncer_query_scalar("SELECT 1").await;
    assert_eq!(bouncer, 1);

    // Extension catalog should be visible through PgBouncer
    let exists: bool = db
        .bouncer_query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_trickle')",
        )
        .await;
    assert!(exists, "Extension should be visible through PgBouncer");
}

/// Create a stream table with `pooler_compatibility_mode = true` through
/// PgBouncer, verify it is created correctly in the catalog.
#[tokio::test]
async fn test_pgbouncer_create_st_with_pooler_mode() {
    let db = PgBouncerTestDb::new().await;

    // Create source table through PgBouncer
    db.bouncer_execute("CREATE TABLE src_data (id SERIAL PRIMARY KEY, val TEXT)")
        .await;
    db.bouncer_execute("INSERT INTO src_data (val) VALUES ('a'), ('b'), ('c')")
        .await;

    // Create stream table with pooler_compatibility_mode = true, through PgBouncer
    db.bouncer_execute(
        "SELECT pgtrickle.create_stream_table(\
             'my_st', \
             $$SELECT id, val FROM src_data$$, \
             '1h', \
             'FULL', \
             true, \
             NULL, \
             NULL, \
             NULL, \
             false, \
             true\
         )",
    )
    .await;

    // Verify catalog entry
    let mode: bool = db
        .admin_query_scalar(
            "SELECT pooler_compatibility_mode FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'my_st'",
        )
        .await;
    assert!(mode, "pooler_compatibility_mode should be true");
}

/// Manual full refresh through PgBouncer with `pooler_compatibility_mode = true`.
/// This exercises the code path that would normally use PREPARE/EXECUTE
/// and emit NOTIFY — both of which are skipped in pooler mode.
#[tokio::test]
async fn test_pgbouncer_manual_refresh_full() {
    let db = PgBouncerTestDb::new().await;

    db.bouncer_execute("CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price INT)")
        .await;
    db.bouncer_execute("INSERT INTO products (name, price) VALUES ('Widget', 10), ('Gadget', 20)")
        .await;

    // Create ST with pooler_compatibility_mode via direct connection (for reliability)
    db.admin_execute(
        "SELECT pgtrickle.create_stream_table(\
             'prod_st', \
             $$SELECT id, name, price FROM products$$, \
             '1h', \
             'FULL', \
             true, \
             NULL, \
             NULL, \
             NULL, \
             false, \
             true\
         )",
    )
    .await;

    // Manual refresh through PgBouncer — this is the key PB3 validation.
    // In pooler_compatibility_mode, no PREPARE/EXECUTE or NOTIFY should be
    // emitted, so this should succeed through a transaction-mode pooler.
    db.bouncer_execute("SELECT pgtrickle.refresh_stream_table('prod_st')")
        .await;

    // Verify data was refreshed
    let count: i64 = db
        .bouncer_query_scalar("SELECT count(*) FROM prod_st")
        .await;
    assert_eq!(count, 2, "ST should contain 2 rows after refresh");

    let total: i64 = db
        .bouncer_query_scalar("SELECT sum(price) FROM prod_st")
        .await;
    assert_eq!(total, 30, "sum(price) should be 30");
}

/// Insert new rows, refresh again, and verify the stream table is updated.
#[tokio::test]
async fn test_pgbouncer_refresh_reflects_source_changes() {
    let db = PgBouncerTestDb::new().await;

    db.admin_execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, amount INT)")
        .await;
    db.admin_execute("INSERT INTO orders (amount) VALUES (100), (200)")
        .await;

    db.admin_execute(
        "SELECT pgtrickle.create_stream_table(\
             'orders_st', \
             $$SELECT id, amount FROM orders$$, \
             '1h', \
             'FULL', \
             true, \
             NULL, \
             NULL, \
             NULL, \
             false, \
             true\
         )",
    )
    .await;

    db.bouncer_execute("SELECT pgtrickle.refresh_stream_table('orders_st')")
        .await;

    let count1: i64 = db
        .bouncer_query_scalar("SELECT count(*) FROM orders_st")
        .await;
    assert_eq!(count1, 2);

    // Insert more data through PgBouncer
    db.bouncer_execute("INSERT INTO orders (amount) VALUES (300), (400)")
        .await;

    // Second refresh through PgBouncer
    db.bouncer_execute("SELECT pgtrickle.refresh_stream_table('orders_st')")
        .await;

    let count2: i64 = db
        .bouncer_query_scalar("SELECT count(*) FROM orders_st")
        .await;
    assert_eq!(
        count2, 4,
        "ST should reflect inserted rows after re-refresh"
    );
}

/// Alter an existing stream table to enable `pooler_compatibility_mode`
/// through PgBouncer.
#[tokio::test]
async fn test_pgbouncer_alter_st_enable_pooler_mode() {
    let db = PgBouncerTestDb::new().await;

    db.admin_execute("CREATE TABLE items (id SERIAL PRIMARY KEY, label TEXT)")
        .await;
    db.admin_execute("INSERT INTO items (label) VALUES ('x'), ('y')")
        .await;

    // Create without pooler mode
    db.admin_execute(
        "SELECT pgtrickle.create_stream_table(\
             'items_st', \
             $$SELECT id, label FROM items$$, \
             '1h', \
             'FULL'\
         )",
    )
    .await;

    // Enable pooler mode via alter through PgBouncer
    db.bouncer_execute(
        "SELECT pgtrickle.alter_stream_table('items_st', pooler_compatibility_mode => true)",
    )
    .await;

    let mode: bool = db
        .admin_query_scalar(
            "SELECT pooler_compatibility_mode FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'items_st'",
        )
        .await;
    assert!(mode, "pooler_compatibility_mode should be true after alter");

    // Refresh should still work through PgBouncer
    db.bouncer_execute("SELECT pgtrickle.refresh_stream_table('items_st')")
        .await;

    let count: i64 = db
        .bouncer_query_scalar("SELECT count(*) FROM items_st")
        .await;
    assert_eq!(count, 2);
}

/// NOTIFY should be suppressed when `pooler_compatibility_mode = true`.
/// We verify this by checking that no notification is received on the
/// pg_trickle alert channel after a refresh.
#[tokio::test]
async fn test_pgbouncer_notify_suppressed_in_pooler_mode() {
    let db = PgBouncerTestDb::new().await;

    db.admin_execute("CREATE TABLE notif_src (id SERIAL PRIMARY KEY, v INT)")
        .await;
    db.admin_execute("INSERT INTO notif_src (v) VALUES (1)")
        .await;

    db.admin_execute(
        "SELECT pgtrickle.create_stream_table(\
             'notif_st', \
             $$SELECT id, v FROM notif_src$$, \
             '1h', \
             'FULL', \
             true, \
             NULL, \
             NULL, \
             NULL, \
             false, \
             true\
         )",
    )
    .await;

    // LISTEN on the admin (direct) connection for pg_trickle alerts
    let (client, mut connection) =
        tokio_postgres::connect(&db.admin_connection_string, tokio_postgres::NoTls)
            .await
            .unwrap();

    let notices = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let notices_clone = notices.clone();

    let conn_task = tokio::spawn(async move {
        while let Some(msg) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            if let Ok(tokio_postgres::AsyncMessage::Notification(n)) = msg {
                notices_clone.lock().await.push(n.channel().to_string());
            }
        }
    });

    client.execute("LISTEN pgtrickle_alert", &[]).await.unwrap();

    // Refresh with pooler_compatibility_mode = true
    db.admin_execute("SELECT pgtrickle.refresh_stream_table('notif_st')")
        .await;

    // Brief wait to allow any notification to propagate
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let received = notices.lock().await.clone();
    assert!(
        received.is_empty(),
        "Expected NO notifications with pooler_compatibility_mode = true, got: {:?}",
        received
    );

    drop(client);
    conn_task.abort();
}

/// Drop a stream table through PgBouncer in pooler mode.
#[tokio::test]
async fn test_pgbouncer_drop_st_through_bouncer() {
    let db = PgBouncerTestDb::new().await;

    db.admin_execute("CREATE TABLE drop_src (id SERIAL PRIMARY KEY)")
        .await;

    db.admin_execute(
        "SELECT pgtrickle.create_stream_table(\
             'drop_st', \
             $$SELECT id FROM drop_src$$, \
             '1h', \
             'FULL', \
             true, \
             NULL, \
             NULL, \
             NULL, \
             false, \
             true\
         )",
    )
    .await;

    db.bouncer_execute("SELECT pgtrickle.refresh_stream_table('drop_st')")
        .await;

    // Drop through PgBouncer
    db.bouncer_execute("SELECT pgtrickle.drop_stream_table('drop_st')")
        .await;

    let exists: bool = db
        .admin_query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'drop_st')",
        )
        .await;
    assert!(!exists, "ST should be removed from catalog after drop");
}
