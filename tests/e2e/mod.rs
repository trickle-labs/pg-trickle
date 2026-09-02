//! E2E test harness that boots a PostgreSQL 18.3 container with
//! the pg_trickle extension pre-installed.
//!
//! # Harness Selection
//!
//! - **Full (default)**: Custom Docker image with `shared_preload_libraries`,
//!   background worker, and shared memory.  Requires
//!   `./tests/build_e2e_image.sh`.
//! - **Light** (`--features light-e2e`): Stock `postgres:18.3` container with
//!   bind-mounted extension artifacts.  No background worker or scheduler.
//!   Much faster to build — only needs `cargo pgrx package`.
//!
//! # Usage
//!
//! ```rust
//! mod e2e;
//! use e2e::E2eDb;
//!
//! #[tokio::test]
//! async fn test_something() {
//!     let db = E2eDb::new().await.with_extension().await;
//!     db.create_st("my_st", "SELECT * FROM src", "1m", "FULL").await;
//! }
//! ```

// ── Light-E2E feature gate ─────────────────────────────────────────────
// When the `light-e2e` feature is active, use the lightweight harness that
// bind-mounts `cargo pgrx package` output into a stock PostgreSQL container.
#[cfg(feature = "light-e2e")]
mod light;
#[cfg(feature = "light-e2e")]
pub use light::E2eDb;

pub mod oracle;
pub mod property_support;

// ── Full E2E harness (default) ─────────────────────────────────────────
#[cfg(not(feature = "light-e2e"))]
use sqlx::{PgPool, postgres::PgPoolOptions};
#[cfg(not(feature = "light-e2e"))]
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicUsize, Ordering},
};
#[cfg(not(feature = "light-e2e"))]
use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt,
    core::{IntoContainerPort, Mount, WaitFor},
    runners::AsyncRunner,
};

#[cfg(not(feature = "light-e2e"))]
const IMAGE_NAME: &str = "pg_trickle_e2e";
#[cfg(not(feature = "light-e2e"))]
const IMAGE_TAG: &str = "latest";
#[cfg(not(feature = "light-e2e"))]
static SHARED_DB_COUNTER: AtomicUsize = AtomicUsize::new(1);
#[cfg(not(feature = "light-e2e"))]
static SHARED_CONTAINER: tokio::sync::OnceCell<SharedContainer> =
    tokio::sync::OnceCell::const_new();

/// Container ID registered for `atexit` cleanup.
///
/// `SHARED_CONTAINER` lives in a `static OnceCell` whose `Drop` is never
/// called (Rust does not run destructors for statics on process exit).
/// Ryuk — testcontainers' normal reaper — may not work in all environments
/// (e.g. macOS Docker Desktop, where the Docker socket is not reachable from
/// inside the Ryuk container). Storing the ID here lets a C-level `atexit`
/// handler stop and remove the container when the test binary exits normally.
#[cfg(not(feature = "light-e2e"))]
static SHARED_CONTAINER_CLEANUP_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
#[cfg(not(feature = "light-e2e"))]
// Serialises tests that use ALTER SYSTEM against the process-local shared
// container. `cargo test --test <name>` runs each test binary in its own
// process, and each process owns its own shared container via SHARED_CONTAINER,
// so cross-binary locking is unnecessary here.
static SHARED_POSTGRES_DB_LOCK: LazyLock<Arc<tokio::sync::Mutex<()>>> =
    LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(())));

#[cfg(not(feature = "light-e2e"))]
struct SharedContainer {
    admin_connection_string: String,
    /// Name of a pre-seeded template database that already has
    /// `CREATE EXTENSION pg_trickle` applied.  Per-test databases are
    /// cloned from this template via `CREATE DATABASE … TEMPLATE`, avoiding
    /// the full extension-install DDL cost on every test.
    template_db_name: String,
    port: u16,
    container_id: String,
    _container: Mutex<ContainerAsync<GenericImage>>,
}

#[cfg(not(feature = "light-e2e"))]
enum ContainerLease {
    Shared {
        _shared: &'static SharedContainer,
    },
    Dedicated {
        _container: Box<ContainerAsync<GenericImage>>,
    },
}

#[cfg(not(feature = "light-e2e"))]
fn e2e_image() -> (String, String) {
    match std::env::var("PGS_E2E_IMAGE") {
        Ok(val) if !val.is_empty() => {
            // Split "name:tag" — default to "latest" if no colon
            if let Some((name, tag)) = val.split_once(':') {
                (name.to_string(), tag.to_string())
            } else {
                (val, "latest".to_string())
            }
        }
        _ => (IMAGE_NAME.to_string(), IMAGE_TAG.to_string()),
    }
}

#[cfg(not(feature = "light-e2e"))]
fn coverage_mount() -> Option<Mount> {
    match std::env::var("PGS_E2E_COVERAGE_DIR") {
        Ok(dir) if !dir.is_empty() => Some(Mount::bind_mount(dir, "/coverage")),
        _ => None,
    }
}

#[cfg(not(feature = "light-e2e"))]
/// Verify an image exists locally before attempting to start a container.
///
/// testcontainers falls back to a Docker Hub pull when the image is not
/// found locally.  For local-only image names (like `pg_trickle_e2e`) that
/// produces a confusing "pull access denied" 404.  This check panics early
/// with a clear, actionable message instead.
///
/// Uses `docker image ls --format` rather than `docker image inspect` because
/// on macOS Docker Desktop with the containerd image store enabled,
/// `docker image inspect <name>:<tag>` returns exit code 1 even for images
/// that are listed by `docker images`.  The `--format` filter approach works
/// consistently across both classic and containerd image stores.
async fn assert_docker_image_exists(name: &str, tag: &str) {
    let output = tokio::process::Command::new("docker")
        .args([
            "image",
            "ls",
            "--format",
            "{{.Repository}}:{{.Tag}}",
            &format!("{}:{}", name, tag),
        ])
        .output()
        .await
        .expect("Failed to run `docker image ls` — is Docker running?");
    let found = std::str::from_utf8(&output.stdout)
        .unwrap_or("")
        .lines()
        .any(|line| line.trim() == format!("{name}:{tag}"));
    if !found {
        panic!(
            "Docker image {name}:{tag} not found locally.\n\
             Build it first:\n\
             • E2E tests:     just build-e2e-image\n\
             • Upgrade tests: just build-upgrade-image"
        );
    }
}

#[cfg(not(feature = "light-e2e"))]
async fn start_e2e_image(
    image: ContainerRequest<GenericImage>,
) -> testcontainers::core::error::Result<ContainerAsync<GenericImage>> {
    match std::env::var("PGS_E2E_PLATFORM") {
        Ok(platform) if !platform.is_empty() => image.with_platform(platform).start().await,
        _ => image.start().await,
    }
}

#[cfg(not(feature = "light-e2e"))]
fn shared_db_name(prefix: &str) -> String {
    let sequence = SHARED_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{}", std::process::id(), sequence)
}

#[cfg(not(feature = "light-e2e"))]
fn connection_string(port: u16, db_name: &str) -> String {
    format!("postgres://postgres:postgres@127.0.0.1:{port}/{db_name}")
}

#[cfg(not(feature = "light-e2e"))]
async fn create_database(admin_connection_string: &str, db_name: &str) {
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_connection_string)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect for CREATE DATABASE {db_name}: {e}"));

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE \"{db_name}\""
    )))
    .execute(&admin_pool)
    .await
    .unwrap_or_else(|e| panic!("Failed to CREATE DATABASE {db_name}: {e}"));

    admin_pool.close().await;
}

/// Create a database named `db_name` as a file-system clone of `template`.
///
/// PostgreSQL's `CREATE DATABASE … TEMPLATE` copies the data directory at the
/// block level, so the new database already has all extension objects
/// pre-installed — no second `CREATE EXTENSION` run is needed.
///
/// The template database must have zero active connections at call time.
/// Background workers (e.g. the pg_trickle scheduler) may connect to the
/// template after the extension is installed, so this function terminates
/// any lingering backends and retries up to 10 times before giving up.
#[cfg(not(feature = "light-e2e"))]
async fn create_database_from_template(
    admin_connection_string: &str,
    db_name: &str,
    template: &str,
) {
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_connection_string)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect for CREATE DATABASE {db_name}: {e}"));

    let create_sql = format!("CREATE DATABASE \"{db_name}\" TEMPLATE \"{template}\"");
    let terminate_sql = format!(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = '{template}' AND pid <> pg_backend_pid()"
    );

    let mut last_err = None;
    for attempt in 0u64..10 {
        if attempt > 0 {
            // Terminate any backends connected to the template DB
            // (e.g. background workers that auto-connected after extension install).
            let _ = sqlx::query(sqlx::AssertSqlSafe(terminate_sql.clone()))
                .execute(&admin_pool)
                .await;
            tokio::time::sleep(std::time::Duration::from_millis(100 * attempt)).await;
        }

        match sqlx::query(sqlx::AssertSqlSafe(create_sql.clone()))
            .execute(&admin_pool)
            .await
        {
            Ok(_) => {
                // The cloned DB inherits the template's per-database
                // pg_trickle.enabled = off setting.  Reset it so the
                // extension is active in the test database.
                let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "ALTER DATABASE \"{db_name}\" RESET pg_trickle.enabled"
                )))
                .execute(&admin_pool)
                .await;
                admin_pool.close().await;
                return;
            }
            Err(e) => {
                last_err = Some(e);
            }
        }
    }

    panic!(
        "Failed to CREATE DATABASE {db_name} from template after 10 attempts: {}",
        last_err.unwrap()
    );
}

/// Install pg_trickle once into a dedicated template database so that each
/// per-test database can be cloned cheaply via `CREATE DATABASE … TEMPLATE`.
///
/// Returns the name of the created template database.
#[cfg(not(feature = "light-e2e"))]
async fn create_extension_template(admin_connection_string: &str, port: u16) -> String {
    // Use a PID-scoped name so that multiple test binary processes sharing
    // the same PostgreSQL server (e.g. nextest running many binaries in
    // parallel) each get their own template without conflicting.
    let template_name = format!("pgt_ext_template_{}", std::process::id());
    let template_name = template_name.as_str();

    // Step 1 — create the template database (plain, no template itself).
    create_database(admin_connection_string, template_name).await;

    // Step 2 — install the extension on the template database.
    let template_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&connection_string(port, template_name))
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to template DB for extension init: {e}"));

    sqlx::query("CREATE EXTENSION pg_trickle CASCADE")
        .execute(&template_pool)
        .await
        .unwrap_or_else(|e| panic!("Failed to CREATE EXTENSION on template DB: {e}"));

    // Disable the scheduler on the template DB so background workers don't
    // connect to it.  Without this, `CREATE DATABASE … TEMPLATE` fails
    // because PostgreSQL requires zero connections to the source database.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE \"{template_name}\" SET pg_trickle.enabled = off"
    )))
    .execute(&template_pool)
    .await
    .unwrap_or_else(|e| panic!("Failed to disable pg_trickle on template DB: {e}"));

    // Close all connections before anyone can use this DB as a template.
    // `PgPool::close` waits until every acquired connection is returned.
    template_pool.close().await;

    // Terminate any background workers that connected to the template DB
    // between CREATE EXTENSION and ALTER DATABASE … SET enabled = off.
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_connection_string)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect for template cleanup: {e}"));
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = '{template_name}' AND pid <> pg_backend_pid()"
    )))
    .execute(&admin_pool)
    .await;
    admin_pool.close().await;

    // The scheduler may have initialized the durable capture owner before
    // the template was disabled. Do not clone that identity into test
    // databases; each database must establish its own owner on first use.
    let template_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&connection_string(port, template_name))
        .await
        .unwrap_or_else(|e| panic!("Failed to reconnect to template DB for cleanup: {e}"));
    sqlx::query("DELETE FROM pgtrickle.pgt_capture_instance")
        .execute(&template_pool)
        .await
        .unwrap_or_else(|e| panic!("Failed to clear template capture identity: {e}"));
    template_pool.close().await;

    template_name.to_string()
}

#[cfg(not(feature = "light-e2e"))]
async fn reset_server_configuration(admin_connection_string: &str) {
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_connection_string)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect for server config reset: {e}"));

    for sql in ["ALTER SYSTEM RESET ALL", "SELECT pg_reload_conf()"] {
        sqlx::query(sql)
            .execute(&admin_pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to reset server config with `{sql}`: {e}"));
    }

    admin_pool.close().await;
}

#[cfg(not(feature = "light-e2e"))]
async fn shared_container() -> &'static SharedContainer {
    SHARED_CONTAINER
        .get_or_init(|| async {
            let (img_name, img_tag) = e2e_image();
            assert_docker_image_exists(&img_name, &img_tag).await;
            let run_id = std::env::var("PGT_E2E_RUN_ID").ok();

            let mut image = GenericImage::new(img_name, img_tag)
                .with_exposed_port(5432_u16.tcp())
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_PASSWORD", "postgres")
                .with_env_var("POSTGRES_DB", "postgres")
                .with_label("com.pgtrickle.test", "true")
                .with_label("com.pgtrickle.suite", "full-e2e")
                .with_label("com.pgtrickle.repo", "pg-stream")
                .with_shm_size(536_870_912); // 512 MB — prevents POSIX shm exhaustion when many databases accumulate

            if let Some(run_id) = run_id {
                image = image.with_label("com.pgtrickle.run-id", run_id);
            }

            if let Some(mount) = coverage_mount() {
                image = image.with_mount(mount);
            }

            let container = start_e2e_image(image).await.expect(
                "Failed to start shared pg_trickle E2E container. \
                 Did you run ./tests/build_e2e_image.sh first?",
            );

            // Register an atexit handler to stop+remove the shared container
            // when the test binary exits.  This complements Ryuk in case Ryuk
            // cannot reach the Docker socket (common on macOS Docker Desktop).
            {
                let _ = SHARED_CONTAINER_CLEANUP_ID.set(container.id().to_string());

                unsafe extern "C" fn rm_shared_container_at_exit() {
                    if let Some(id) = SHARED_CONTAINER_CLEANUP_ID.get() {
                        // -f: stop if running; -v: also remove anonymous volumes
                        let _ = std::process::Command::new("docker")
                            .args(["rm", "-fv", id])
                            .stdout(std::process::Stdio::null())
                            .stderr(std::process::Stdio::null())
                            .status();
                    }
                }

                unsafe extern "C" {
                    fn atexit(func: unsafe extern "C" fn()) -> i32;
                }

                // SAFETY: `rm_shared_container_at_exit` is a plain C function
                // pointer that only touches `SHARED_CONTAINER_CLEANUP_ID`, a
                // `static OnceLock` safe to read after the async runtime is
                // torn down.  `std::process::Command` uses fork+exec and is
                // safe to call from an atexit handler.
                unsafe {
                    atexit(rm_shared_container_at_exit);
                }
            }

            // Retry getting the mapped port — Docker's port-mapping metadata is
            // occasionally not yet published immediately after the "ready"
            // log line, causing a transient `PortNotExposed` error.  Use up to
            // 30 attempts with a fixed 1-second gap (≤ 30 s total) so that
            // even heavily-loaded Docker daemons (e.g. macOS Docker Desktop)
            // have time to register the port before we give up.
            let port = {
                let mut attempt = 0u32;
                loop {
                    match container.get_host_port_ipv4(5432).await {
                        Ok(p) => break p,
                        Err(e) if attempt < 30 => {
                            attempt += 1;
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            let _ = e; // suppress unused-variable warning
                        }
                        Err(e) => panic!("Failed to get mapped port after retries: {e}"),
                    }
                }
            };
            let admin_connection_string = connection_string(port, "postgres");

            // Pre-seed a template database with the extension installed once.
            // Each per-test database is cloned from this template which avoids
            // running the full extension DDL on every individual test.
            let template_db_name = create_extension_template(&admin_connection_string, port).await;

            SharedContainer {
                admin_connection_string,
                template_db_name,
                port,
                container_id: container.id().to_string(),
                _container: Mutex::new(container),
            }
        })
        .await
}

/// A test database backed by a PostgreSQL 18.3 container with
/// the compiled pg_trickle extension installed and
/// `shared_preload_libraries` configured.
///
/// The container is automatically cleaned up when `E2eDb` is dropped.
#[cfg(not(feature = "light-e2e"))]
pub struct E2eDb {
    pub pool: PgPool,
    connection_string: String,
    container_id: String,
    _shared_scheduler_test_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    _container: ContainerLease,
}

#[cfg(not(feature = "light-e2e"))]
#[allow(dead_code)]
impl E2eDb {
    /// Start a fresh PostgreSQL 18.3 container with the extension installed.
    ///
    /// The container is ready to accept connections but the extension is NOT
    /// yet created. Call [`with_extension`] to run `CREATE EXTENSION`.
    pub async fn new() -> Self {
        let shared = shared_container().await;
        let db_name = shared_db_name("pgt_e2e");
        create_database_from_template(
            &shared.admin_connection_string,
            &db_name,
            &shared.template_db_name,
        )
        .await;
        let pool = Self::connect_with_retry(&connection_string(shared.port, &db_name), 15).await;
        let connection_string = connection_string(shared.port, &db_name);

        E2eDb {
            pool,
            connection_string,
            container_id: shared.container_id.clone(),
            _shared_scheduler_test_guard: None,
            _container: ContainerLease::Shared { _shared: shared },
        }
    }

    /// Start an isolated container for tests that restart PostgreSQL itself.
    pub async fn new_dedicated() -> Self {
        Self::new_with_db("pg_trickle_test").await
    }

    /// Start a fresh database WITHOUT the extension pre-installed.
    ///
    /// Unlike [`Self::new`] (which clones from the pre-seeded template), this
    /// creates a plain empty database.  Use this for upgrade tests that need
    /// to run `CREATE EXTENSION pg_trickle VERSION '<old_version>'` themselves.
    pub async fn new_without_extension() -> Self {
        let shared = shared_container().await;
        let db_name = shared_db_name("pgt_upgrade_e2e");
        create_database(&shared.admin_connection_string, &db_name).await;
        let pool = Self::connect_with_retry(&connection_string(shared.port, &db_name), 15).await;
        let connection_string = connection_string(shared.port, &db_name);

        E2eDb {
            pool,
            connection_string,
            container_id: shared.container_id.clone(),
            _shared_scheduler_test_guard: None,
            _container: ContainerLease::Shared { _shared: shared },
        }
    }

    /// Historical compatibility helper for scheduler-focused tests.
    ///
    /// Dynamic scheduler workers now connect to the database name supplied in
    /// `bgw_extra`, so these tests no longer need to run inside `postgres`
    /// itself. The remaining isolation concern is server-level state:
    /// scheduler tests use `ALTER SYSTEM`, which affects the whole shared
    /// container. This helper therefore resets server config, creates a fresh
    /// per-test database, and holds a process-local guard for the test's
    /// lifetime so parallel tests in the same binary cannot interfere.
    pub async fn new_on_postgres_db() -> Self {
        let shared_scheduler_test_guard = SHARED_POSTGRES_DB_LOCK.clone().lock_owned().await;
        let shared = shared_container().await;
        reset_server_configuration(&shared.admin_connection_string).await;

        let db_name = shared_db_name("pgt_sched_e2e");
        create_database_from_template(
            &shared.admin_connection_string,
            &db_name,
            &shared.template_db_name,
        )
        .await;
        let connection_string = connection_string(shared.port, &db_name);
        let pool = Self::connect_with_retry(&connection_string, 15).await;

        E2eDb {
            pool,
            connection_string,
            container_id: shared.container_id.clone(),
            _shared_scheduler_test_guard: Some(shared_scheduler_test_guard),
            _container: ContainerLease::Shared { _shared: shared },
        }
    }

    /// Start a container configured for benchmarking with resource
    /// constraints and tuning for reduced variance.
    ///
    /// Applies:
    /// - 256 MB shared memory (`--shm-size`)
    /// - PostgreSQL tuning: `work_mem`, `effective_cache_size`,
    ///   `synchronous_commit = off`, `max_wal_size`
    /// - `log_min_messages = info` so `[PGS_PROFILE]` lines appear
    ///   in the container log
    ///
    /// For CPU pinning (further reduces variance), run the benchmark
    /// with Docker CPU constraints externally:
    /// ```bash
    /// docker run --cpus=2 --cpuset-cpus=0,1 --memory=2g ...
    /// ```
    pub async fn new_bench() -> Self {
        Self::new_with_db_bench("pg_trickle_test").await
    }

    /// Get the Docker container ID (for `docker logs` and profile capture).
    pub fn container_id(&self) -> &str {
        &self.container_id
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Reconnect to a dedicated container after Docker may have remapped its host port.
    pub async fn reconnect_after_restart(&self) -> PgPool {
        let ContainerLease::Dedicated { _container } = &self._container else {
            panic!("reconnect_after_restart requires a dedicated container");
        };
        let port = _container
            .get_host_port_ipv4(5432_u16.tcp())
            .await
            .expect("Failed to get remapped PostgreSQL port after restart");
        let db_name = self
            .connection_string
            .rsplit_once('/')
            .map(|(_, db_name)| db_name)
            .expect("E2E connection string must contain a database name");
        Self::connect_with_retry(&connection_string(port, db_name), 30).await
    }

    /// Execute SQL on a dedicated connection and collect PostgreSQL notices.
    pub async fn try_execute_with_notices(
        &self,
        sql: &str,
    ) -> Result<Vec<String>, tokio_postgres::Error> {
        let (client, mut connection) =
            tokio_postgres::connect(&self.connection_string, tokio_postgres::NoTls).await?;

        let notices = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let notices_task = notices.clone();

        let connection_task = tokio::spawn(async move {
            while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                match message {
                    Ok(tokio_postgres::AsyncMessage::Notice(notice)) => {
                        notices_task.lock().await.push(notice.to_string());
                    }
                    Ok(_) => {}
                    Err(err) => return Err(err),
                }
            }
            Ok::<(), tokio_postgres::Error>(())
        });

        let execute_result = client.batch_execute(sql).await;
        drop(client);

        connection_task
            .await
            .unwrap_or_else(|e| panic!("notice collector task failed: {e}"))?;
        execute_result?;

        Ok(notices.lock().await.clone())
    }

    /// Internal: start a container using the given database name.
    async fn new_with_db(db_name: &str) -> Self {
        let (img_name, img_tag) = e2e_image();
        assert_docker_image_exists(&img_name, &img_tag).await;
        let run_id = std::env::var("PGT_E2E_RUN_ID").ok();
        let mut image = GenericImage::new(img_name, img_tag)
            .with_exposed_port(5432_u16.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_DB", db_name)
            .with_label("com.pgtrickle.test", "true")
            .with_label("com.pgtrickle.suite", "full-e2e")
            .with_label("com.pgtrickle.repo", "pg-stream");

        if let Some(run_id) = run_id {
            image = image.with_label("com.pgtrickle.run-id", run_id);
        }

        // When running under the coverage harness, bind-mount a host
        // directory at /coverage so profraw files are written to the host.
        if let Some(mount) = coverage_mount() {
            image = image.with_mount(mount);
        }

        let container = start_e2e_image(image).await.expect(
            "Failed to start pg_trickle E2E container. \
                     Did you run ./tests/build_e2e_image.sh first?",
        );

        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get mapped port");

        let connection_string = format!(
            "postgres://postgres:postgres@127.0.0.1:{}/{}",
            port, db_name,
        );

        let pool = Self::connect_with_retry(&connection_string, 15).await;

        E2eDb {
            pool,
            connection_string,
            container_id: container.id().to_string(),
            _shared_scheduler_test_guard: None,
            _container: ContainerLease::Dedicated {
                _container: Box::new(container),
            },
        }
    }

    /// Internal: start a bench-specific container with SHM and PG tuning.
    async fn new_with_db_bench(db_name: &str) -> Self {
        let (img_name, img_tag) = e2e_image();
        assert_docker_image_exists(&img_name, &img_tag).await;
        let run_id = std::env::var("PGT_E2E_RUN_ID").ok();
        let mut image = GenericImage::new(img_name, img_tag)
            .with_exposed_port(5432_u16.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_DB", db_name)
            .with_label("com.pgtrickle.test", "true")
            .with_label("com.pgtrickle.suite", "full-e2e")
            .with_label("com.pgtrickle.repo", "pg-stream")
            .with_shm_size(536_870_912); // 512 MB — headroom for work_mem×max_connections

        if let Some(run_id) = run_id {
            image = image.with_label("com.pgtrickle.run-id", run_id);
        }

        // When running under the coverage harness, bind-mount a host
        // directory at /coverage so profraw files are written to the host.
        if let Some(mount) = coverage_mount() {
            image = image.with_mount(mount);
        }

        let container = start_e2e_image(image).await.expect(
            "Failed to start bench container. \
             Did you run ./tests/build_e2e_image.sh first?",
        );

        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get mapped port");

        let connection_string = format!(
            "postgres://postgres:postgres@127.0.0.1:{}/{}",
            port, db_name,
        );

        let pool = Self::connect_with_retry(&connection_string, 15).await;

        let db = E2eDb {
            pool,
            connection_string,
            container_id: container.id().to_string(),
            _shared_scheduler_test_guard: None,
            _container: ContainerLease::Dedicated {
                _container: Box::new(container),
            },
        };

        // Apply runtime PostgreSQL tuning for stable benchmarks.
        // These are SIGHUP-level parameters that take effect after reload.
        // 256 MB: large enough for q05/q07/q08/q09 multi-CTE join deltas
        // at SF=0.01 without spilling to disk (SF=0.01 delta CTEs peak
        // ~180–220 MB for the 8-table join in q08).
        db.execute("ALTER SYSTEM SET work_mem = '256MB'").await;
        db.execute("ALTER SYSTEM SET effective_cache_size = '512MB'")
            .await;
        db.execute("ALTER SYSTEM SET maintenance_work_mem = '128MB'")
            .await;
        db.execute("ALTER SYSTEM SET synchronous_commit = 'off'")
            .await;
        db.execute("ALTER SYSTEM SET max_wal_size = '1GB'").await;
        // Cap temp file usage per query to prevent runaway disk
        // consumption from CTE materialisation and sort spills.
        // DI-11 deep-join planner hints (SET LOCAL temp_file_limit=-1)
        // override this per-transaction for 5+ table join delta queries,
        // but the system-wide cap protects against unlimited growth from
        // other queries (initial full refresh, IMMEDIATE IVM triggers).
        //
        // 16 GB rather than 4 GB so the initial full population of
        // 6/7-table TPC-H joins (q05/q07/q08/q09) — which runs before
        // DI-11 planner hints can apply since there is no incremental
        // delta yet — completes without spilling past the cap.
        db.execute("ALTER SYSTEM SET temp_file_limit = '16GB'")
            .await;
        // Aggressive autovacuum: change-buffer tables and stream tables
        // accumulate dead tuples rapidly during differential refreshes.
        // Without aggressive settings the default autovacuum can't keep
        // up, causing the PostgreSQL data directory to bloat (121 GB+
        // observed in TPC-H Phase 2 tests).
        db.execute("ALTER SYSTEM SET autovacuum_vacuum_scale_factor = '0.01'")
            .await;
        db.execute("ALTER SYSTEM SET autovacuum_vacuum_threshold = '50'")
            .await;
        db.execute("ALTER SYSTEM SET autovacuum_naptime = '5s'")
            .await;
        db.execute("ALTER SYSTEM SET autovacuum_vacuum_cost_delay = '2ms'")
            .await;
        db.execute("ALTER SYSTEM SET autovacuum_vacuum_cost_limit = '1000'")
            .await;
        // Enable INFO logging so [PGS_PROFILE] lines appear in server stderr
        db.execute("ALTER SYSTEM SET log_min_messages = 'info'")
            .await;
        // Raise the scheduler tick interval to its maximum so the background
        // worker does not auto-refresh during bench tests, which use explicit
        // manual refreshes.  This is a defence-in-depth companion to using
        // '24h' schedules for bench stream tables.
        db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = '60000'")
            .await;
        db.reload_config_and_wait().await;

        db
    }

    /// Retry connection with backoff — the container may need a moment
    /// after the "ready to accept connections" log line.
    async fn connect_with_retry(url: &str, max_attempts: u32) -> PgPool {
        for attempt in 1..=max_attempts {
            match tokio::time::timeout(std::time::Duration::from_secs(5), PgPool::connect(url))
                .await
            {
                Ok(Ok(pool)) => {
                    // Verify the connection actually works
                    match sqlx::query("SELECT 1").execute(&pool).await {
                        Ok(_) => return pool,
                        Err(e) if attempt < max_attempts => {
                            eprintln!(
                                "E2E connect attempt {}/{}: ping failed: {}",
                                attempt, max_attempts, e
                            );
                        }
                        Err(e) => {
                            panic!("E2E: Failed to ping after {} attempts: {}", max_attempts, e);
                        }
                    }
                }
                Ok(Err(e)) if attempt < max_attempts => {
                    eprintln!("E2E connect attempt {}/{}: {}", attempt, max_attempts, e);
                }
                Ok(Err(e)) => {
                    panic!(
                        "E2E: Failed to connect after {} attempts: {}",
                        max_attempts, e
                    );
                }
                Err(_) if attempt < max_attempts => {
                    eprintln!("E2E connect attempt {attempt}/{max_attempts}: timed out");
                }
                Err(_) => panic!("E2E: connection timed out after {max_attempts} attempts"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        unreachable!()
    }

    /// Install the extension (`CREATE EXTENSION pg_trickle`).
    ///
    /// This creates all catalog tables, views, event triggers, and
    /// SQL functions in the `pg_trickle` schema.
    ///
    /// If the `PGT_PARALLEL_MODE` environment variable is set to `on` or
    /// `dry_run`, the parallel refresh mode GUC is enabled for this
    /// database after extension creation.
    pub async fn with_extension(self) -> Self {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
            .execute(&self.pool)
            .await
            .expect("Failed to CREATE EXTENSION pg_trickle");

        // Signal the pg_trickle launcher to immediately discover this new database.
        // Without this, the launcher can sleep up to 10 s before its next poll cycle,
        // causing WAL-transition tests to time out waiting for the scheduler.
        // The SIGHUP wakes the launcher, which then sees the DAG-version bump from
        // CREATE EXTENSION, clears its skip-cache, and spawns the per-DB scheduler.
        sqlx::query("SELECT pg_reload_conf()")
            .execute(&self.pool)
            .await
            .expect("Failed to pg_reload_conf()");

        if let Ok(mode) = std::env::var("PGT_PARALLEL_MODE") {
            let mode = mode.to_ascii_lowercase();
            if mode == "on" || mode == "dry_run" {
                let sql = format!(
                    "ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = '{}'",
                    mode
                );
                self.execute(&sql).await;
                self.execute("SELECT pg_reload_conf()").await;
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }

        self
    }

    // ── SQL Execution Helpers ──────────────────────────────────────────

    /// Execute a SQL statement (panics on error).
    pub async fn execute(&self, sql: &str) {
        sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .execute(&self.pool)
            .await
            .unwrap_or_else(|e| panic!("SQL failed: {}\nSQL: {}", e, sql));
    }

    /// Execute a SQL statement, returning Ok/Err instead of panicking.
    pub async fn try_execute(&self, sql: &str) -> Result<(), sqlx::Error> {
        sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .execute(&self.pool)
            .await
            .map(|_| ())
    }

    /// Execute multiple SQL statements sequentially on the **same** connection.
    ///
    /// Use this whenever one statement sets session state (e.g. a GUC via
    /// `SET`) that must be visible to the next statement — a connection pool
    /// may dispatch each `execute()` call to a different backend connection.
    pub async fn execute_seq(&self, stmts: &[&str]) {
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("Failed to acquire DB connection for execute_seq");
        for sql in stmts {
            sqlx::query(sqlx::AssertSqlSafe((*sql).to_owned()))
                .execute(&mut *conn)
                .await
                .unwrap_or_else(|e| panic!("SQL failed: {}\nSQL: {}", e, sql));
        }
    }

    /// Run `config` statements on a dedicated connection (all must succeed),
    /// then run `sql` on the same connection and return Ok/Err.
    ///
    /// Use this when `sql` depends on session-local GUC values set by `config`.
    pub async fn try_execute_with_config(
        &self,
        config: &[&str],
        sql: &str,
    ) -> Result<(), sqlx::Error> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("Failed to acquire DB connection for try_execute_with_config");
        for stmt in config {
            sqlx::query(sqlx::AssertSqlSafe((*stmt).to_owned()))
                .execute(&mut *conn)
                .await
                .unwrap_or_else(|e| panic!("Config SQL failed: {}\nSQL: {}", e, stmt));
        }
        sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .execute(&mut *conn)
            .await
            .map(|_| ())
    }

    /// Execute `setup_sql` on a connection, then try `target_sql` and return its
    /// result, then run `teardown_sql` unconditionally.  All three statements use
    /// the **same** connection so session state (e.g. `SET ROLE`) is preserved.
    ///
    /// Use this instead of multi-statement strings passed to `try_execute`,
    /// which sqlx rejects with "cannot insert multiple commands into a prepared
    /// statement".
    pub async fn try_execute_with_role(
        &self,
        setup_sql: &str,
        target_sql: &str,
        teardown_sql: &str,
    ) -> Result<(), sqlx::Error> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("Failed to acquire DB connection for try_execute_with_role");
        sqlx::query(sqlx::AssertSqlSafe(setup_sql.to_owned()))
            .execute(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("setup SQL failed: {}\nSQL: {}", e, setup_sql));
        let result = sqlx::query(sqlx::AssertSqlSafe(target_sql.to_owned()))
            .execute(&mut *conn)
            .await
            .map(|_| ());
        // Always reset, even if target failed.
        let _ = sqlx::query(sqlx::AssertSqlSafe(teardown_sql.to_owned()))
            .execute(&mut *conn)
            .await;
        result
    }

    /// Reload PostgreSQL configuration and wait briefly for SIGHUP settings to apply.
    pub async fn reload_config_and_wait(&self) {
        self.execute("SELECT pg_reload_conf()").await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    /// Nudge the launcher so it re-probes this database promptly.
    ///
    /// `pg_reload_conf()` wakes the launcher from `wait_latch()`, but it does
    /// not change `last_attempt`. `pgtrickle._signal_launcher_rescan()` bumps
    /// the shared DAG version, which lets the launcher evict stale
    /// `last_attempt` entries on its next loop iteration.
    pub async fn nudge_launcher_rescan(&self) {
        let _ = self
            .try_execute("SELECT pgtrickle._signal_launcher_rescan()")
            .await;
        self.execute("SELECT pg_reload_conf()").await;
    }

    /// Read a GUC value via `SHOW`.
    pub async fn show_setting(&self, setting: &str) -> String {
        self.query_scalar(&format!("SHOW {setting}")).await
    }

    /// SET a GUC and immediately SHOW it on the **same** connection.
    ///
    /// Session-level SET is visible only on the connection that ran it; with
    /// a connection pool the subsequent SHOW may hit a different backend.
    /// This helper guarantees both statements share a single connection.
    pub async fn set_and_show_setting(&self, set_sql: &str, setting: &str) -> String {
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("Failed to acquire connection for set_and_show_setting");
        sqlx::query(sqlx::AssertSqlSafe(set_sql.to_owned()))
            .execute(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("SET failed: {e}\nSQL: {set_sql}"));
        let result: (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SHOW {setting}")))
            .fetch_one(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("SHOW {setting} failed: {e}"));
        result.0
    }

    /// Wait until `SHOW <setting>` reports the expected value.
    pub async fn wait_for_setting(&self, setting: &str, expected: &str) {
        for _ in 0..10 {
            let current = self.show_setting(setting).await;
            if current == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let current = self.show_setting(setting).await;
        panic!("{setting} did not reload to {expected}; current value is {current}");
    }

    /// Apply `ALTER SYSTEM SET` and wait for the new value to become visible.
    pub async fn alter_system_set_and_wait(&self, setting: &str, value_sql: &str, expected: &str) {
        self.execute(&format!("ALTER SYSTEM SET {setting} = {value_sql}"))
            .await;
        self.reload_config_and_wait().await;
        self.wait_for_setting(setting, expected).await;
    }

    /// Apply `ALTER SYSTEM RESET` and wait for the default value to become visible.
    pub async fn alter_system_reset_and_wait(&self, setting: &str, expected: &str) {
        self.execute(&format!("ALTER SYSTEM RESET {setting}")).await;
        self.reload_config_and_wait().await;
        self.wait_for_setting(setting, expected).await;
    }

    /// Get a single scalar value from a query.
    pub async fn query_scalar<T>(&self, sql: &str) -> T
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + Unpin,
        (T,): for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned()))
            .fetch_one(&self.pool)
            .await
            .unwrap_or_else(|e| panic!("Scalar query failed: {}\nSQL: {}", e, sql))
    }

    /// Get an optional scalar value from a query.
    ///
    /// Returns `None` both when no rows are returned *and* when the single
    /// returned value is `NULL` (e.g. `max()` / `min()` over an empty set).
    pub async fn query_scalar_opt<T>(&self, sql: &str) -> Option<T>
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + Unpin,
        (T,): for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        sqlx::query_scalar::<_, Option<T>>(sqlx::AssertSqlSafe(sql.to_owned()))
            .fetch_optional(&self.pool)
            .await
            .unwrap_or_else(|e| panic!("Scalar query failed: {}\nSQL: {}", e, sql))
            .flatten()
    }

    /// Count rows in a table.
    pub async fn count(&self, table: &str) -> i64 {
        self.query_scalar::<i64>(&format!("SELECT count(*) FROM {}", table))
            .await
    }

    /// Return the qualified change buffer table name for a source OID.
    ///
    /// v0.32.0+: buffer tables are named `changes_{stable_name}` (not `changes_{oid}`).
    /// Queries `pgt_change_tracking.source_stable_name` to get the correct name.
    pub async fn change_buffer_table(&self, source_oid: i64) -> String {
        let stable_name: String = self
            .query_scalar(&format!(
                "SELECT source_stable_name \
                 FROM pgtrickle.pgt_change_tracking \
                 WHERE source_relid = {}",
                source_oid
            ))
            .await;
        format!("pgtrickle_changes.changes_{}", stable_name)
    }

    /// Return the CDC INSERT trigger name for a source OID.
    ///
    /// v0.32.0+: triggers are named `pg_trickle_cdc_ins_{stable_name}`
    /// (stable 16-char xxhash64 hex) rather than `pg_trickle_cdc_ins_{oid}`.
    pub async fn cdc_trigger_name(&self, source_oid: i64) -> String {
        let stable_name: String = self
            .query_scalar(&format!(
                "SELECT pgtrickle.source_stable_name({}::oid)",
                source_oid
            ))
            .await;
        format!("pg_trickle_cdc_ins_{}", stable_name)
    }

    /// Execute a query and return all result rows as a single text string.
    ///
    /// Useful for capturing EXPLAIN output where each row is a text line.
    pub async fn query_text(&self, sql: &str) -> Option<String> {
        let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.to_owned()))
            .fetch_all(&self.pool)
            .await
            .ok()?;
        if rows.is_empty() {
            return None;
        }
        Some(
            rows.into_iter()
                .map(|(line,)| line)
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }

    // ── Extension API Helpers ──────────────────────────────────────────

    /// Create a stream table via `pgtrickle.create_stream_table()`.
    pub async fn create_st(&self, name: &str, query: &str, schedule: &str, refresh_mode: &str) {
        let sql = format!(
            "SELECT pgtrickle.create_stream_table('{name}', $${query}$$, \
             '{schedule}', '{refresh_mode}')"
        );
        self.execute(&sql).await;
    }

    /// Create a partitioned stream table (A1-1: `partition_by` parameter).
    ///
    /// The storage table is created as `PARTITION BY RANGE (partition_key)` with a
    /// default catch-all partition. Partition pruning during MERGE is enabled
    /// automatically by the A1-3 predicate injection path.
    pub async fn create_st_partitioned(
        &self,
        name: &str,
        query: &str,
        schedule: &str,
        refresh_mode: &str,
        partition_key: &str,
    ) {
        let sql = format!(
            "SELECT pgtrickle.create_stream_table('{name}', $${query}$$, \
             '{schedule}', '{refresh_mode}', partition_by => '{partition_key}')"
        );
        self.execute(&sql).await;
    }

    /// Create a stream table with explicit `initialize` parameter.
    pub async fn create_st_with_init(
        &self,
        name: &str,
        query: &str,
        schedule: &str,
        refresh_mode: &str,
        initialize: bool,
    ) {
        let sql = format!(
            "SELECT pgtrickle.create_stream_table('{name}', $${query}$$, \
             '{schedule}', '{refresh_mode}', {initialize})"
        );
        self.execute(&sql).await;
    }

    /// Refresh a stream table via `pgtrickle.refresh_stream_table()`.
    pub async fn refresh_st(&self, name: &str) {
        self.execute(&format!("SELECT pgtrickle.refresh_stream_table('{name}')"))
            .await;
    }

    /// Refresh a stream table, retrying when a concurrent background refresh holds the lock.
    ///
    /// The background scheduler may race with a manual refresh of a downstream ST
    /// (e.g. after the test manually refreshes an upstream ST, the scheduler detects
    /// staleness within its polling interval and acquires the session-level advisory
    /// lock on the same `pgt_id`). When `refresh_stream_table` returns
    /// "another refresh is already in progress", this helper sleeps 100 ms and retries
    /// until the lock clears or 10 seconds elapse.
    pub async fn refresh_st_with_retry(&self, name: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match self
                .try_execute(&format!("SELECT pgtrickle.refresh_stream_table('{name}')"))
                .await
            {
                Ok(_) => return,
                Err(e) if e.to_string().contains("already in progress") => {
                    if std::time::Instant::now() >= deadline {
                        panic!(
                            "refresh_st_with_retry: timed out waiting for \
                             concurrent refresh of '{name}' to complete"
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(e) => panic!("refresh_stream_table('{name}') failed: {e:?}"),
            }
        }
    }

    /// Drop a stream table via `pgtrickle.drop_stream_table()`.
    pub async fn drop_st(&self, name: &str) {
        self.execute(&format!("SELECT pgtrickle.drop_stream_table('{name}')"))
            .await;
    }

    /// Drop a stream table with cascade.
    pub async fn drop_st_cascade(&self, name: &str) {
        self.execute(&format!(
            "SELECT pgtrickle.drop_stream_table('{name}', cascade => true)"
        ))
        .await;
    }

    /// Alter a stream table via `pgtrickle.alter_stream_table()`.
    ///
    /// `args` should be the named arguments after the name, e.g.:
    /// `"schedule => '5m'"` or
    /// `"status => 'SUSPENDED'"`.
    pub async fn alter_st(&self, name: &str, args: &str) {
        self.execute(&format!(
            "SELECT pgtrickle.alter_stream_table('{name}', {args})"
        ))
        .await;
    }

    // ── Catalog Query Helpers ──────────────────────────────────────────

    /// Get the status tuple for a specific ST from the catalog.
    ///
    /// Returns `(status, refresh_mode, is_populated, consecutive_errors)`.
    pub async fn pgt_status(&self, name: &str) -> (String, String, bool, i32) {
        sqlx::query_as(
            "SELECT status, refresh_mode, is_populated, consecutive_errors \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema || '.' || pgt_name = $1 OR pgt_name = $1",
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await
        .unwrap_or_else(|e| panic!("pgt_status query failed for '{}': {}", name, e))
    }

    /// Verify a ST's contents match its defining query exactly (multiset equality).
    pub async fn assert_st_matches_query(&self, st_table: &str, defining_query: &str) {
        oracle::assert_st_query_exact(self, st_table, defining_query, "assert_st_matches_query")
            .await;
    }

    // ── Infrastructure Query Helpers ───────────────────────────────────

    /// Check if a trigger exists on a table.
    pub async fn trigger_exists(&self, trigger_name: &str, table: &str) -> bool {
        self.query_scalar::<bool>(&format!(
            "SELECT EXISTS(\
                SELECT 1 FROM pg_trigger t \
                JOIN pg_class c ON t.tgrelid = c.oid \
                WHERE t.tgname = '{trigger_name}' \
                AND c.relname = '{table}'\
            )"
        ))
        .await
    }

    /// Check if a table exists in a given schema.
    pub async fn table_exists(&self, schema: &str, table: &str) -> bool {
        self.query_scalar::<bool>(&format!(
            "SELECT EXISTS(\
                SELECT 1 FROM information_schema.tables \
                WHERE table_schema = '{schema}' AND table_name = '{table}'\
            )"
        ))
        .await
    }

    /// Get the OID of a table (as i32).
    pub async fn table_oid(&self, table: &str) -> i32 {
        self.query_scalar::<i32>(&format!("SELECT '{table}'::regclass::oid::int"))
            .await
    }

    /// Wait for any pg_trickle scheduler background worker to appear in
    /// `pg_stat_activity` for the current database.
    ///
    /// The launcher spawns schedulers dynamically; on a freshly-installed
    /// database the scheduler may not start for up to the launcher's 10-second
    /// polling interval. Call this after `with_extension()` + GUC setup to
    /// ensure the scheduler is running before relying on auto-refresh behaviour.
    ///
    /// Returns `true` if the scheduler was detected within `timeout`, or
    /// `false` if it never appeared. In the latter case the caller can assert
    /// or produce a meaningful failure message rather than a generic timeout.
    #[must_use]
    pub async fn wait_for_scheduler(&self, timeout: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        let nudge_interval = std::time::Duration::from_secs(10);
        // Trigger the first nudge immediately rather than waiting 10 s.
        let mut last_nudge = start - nudge_interval;
        loop {
            if start.elapsed() > timeout {
                return false;
            }

            let running: bool = self
                .query_scalar(
                    "SELECT EXISTS(\
                         SELECT 1 FROM pg_stat_activity \
                         WHERE backend_type = 'pg_trickle scheduler' \
                           AND datname = current_database()\
                     )",
                )
                .await;
            if running {
                return true;
            }

            if last_nudge.elapsed() >= nudge_interval {
                self.nudge_launcher_rescan().await;
                last_nudge = std::time::Instant::now();
            }

            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// Wait for the background scheduler to auto-refresh a ST.
    ///
    /// Polls `data_timestamp` until it advances past the initial value
    /// or the timeout expires. Returns `true` if a refresh was detected.
    #[must_use]
    pub async fn wait_for_auto_refresh(
        &self,
        pgt_name: &str,
        timeout: std::time::Duration,
    ) -> bool {
        let start = std::time::Instant::now();
        let initial_ts: Option<String> = self
            .query_scalar_opt(&format!(
                "SELECT data_timestamp::text \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{pgt_name}'"
            ))
            .await;

        loop {
            if start.elapsed() > timeout {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            let current_ts: Option<String> = self
                .query_scalar_opt(&format!(
                    "SELECT data_timestamp::text \
                     FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{pgt_name}'"
                ))
                .await;

            if current_ts != initial_ts && current_ts.is_some() {
                return true;
            }
        }
    }

    /// General-purpose async polling helper with exponential backoff.
    ///
    /// Evaluates `condition_sql` (must return a single `BOOLEAN`) repeatedly
    /// until it returns `true` or `timeout` expires.  The polling interval
    /// starts at `initial_backoff` and doubles on each iteration up to
    /// `max_backoff` (default: 2 s).
    ///
    /// Returns `true` if the condition was met, `false` on timeout.
    /// The `label` is used only in timeout log messages for diagnostics.
    #[must_use]
    pub async fn wait_for_condition(
        &self,
        label: &str,
        condition_sql: &str,
        timeout: std::time::Duration,
        initial_backoff: std::time::Duration,
    ) -> bool {
        let max_backoff = std::time::Duration::from_secs(2);
        let start = std::time::Instant::now();
        let mut backoff = initial_backoff;
        loop {
            let met: bool = self.query_scalar(condition_sql).await;
            if met {
                return true;
            }
            if start.elapsed() >= timeout {
                eprintln!(
                    "wait_for_condition({label}): timed out after {:.1}s",
                    timeout.as_secs_f64()
                );
                return false;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
        }
    }
}

// ── Shared profiling utilities ─────────────────────────────────────────────
//
// Used by both `e2e_bench_tests` and `e2e_tpch_tests` to extract
// `[PGS_PROFILE]` lines emitted by `src/refresh.rs` into the container log.

/// Per-phase timing extracted from `[PGS_PROFILE]` log lines.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ProfileData {
    pub decision_ms: f64,
    pub generate_ms: f64,
    pub merge_ms: f64,
    pub cleanup_ms: f64,
    pub total_ms: f64,
    pub affected: i64,
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize)]
#[allow(dead_code)]
pub struct VectorAggregateProfile {
    pub rows: u64,
    pub pages: u64,
    pub groups: u64,
    pub rescans: u64,
    pub bytes: u64,
    pub max_page_bytes: u64,
    pub read_ms: f64,
    pub reduce_ms: f64,
    pub rescan_ms: f64,
    pub apply_ms: f64,
}

/// Extract the last `[PGS_PROFILE]` line from docker container logs.
#[allow(dead_code)]
pub async fn extract_last_profile(container_id: &str) -> Option<ProfileData> {
    let output = tokio::process::Command::new("docker")
        .args(["logs", "--tail", "50", container_id])
        .output()
        .await
        .ok()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let line = stderr.lines().rev().find(|l| l.contains("[PGS_PROFILE]"))?;
    parse_profile_line(line)
}

#[allow(dead_code)]
pub async fn extract_last_vector_profile(container_id: &str) -> Option<VectorAggregateProfile> {
    let output = tokio::process::Command::new("docker")
        .args(["logs", "--tail", "200", container_id])
        .output()
        .await
        .ok()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let line = stderr
        .lines()
        .rev()
        .find(|line| line.contains("[PGS_VECTOR_AGG]"))?;
    let integer = |key: &str| -> Option<u64> {
        let rest = line.split_once(&format!("{key}="))?.1;
        rest.split_whitespace().next()?.parse().ok()
    };
    let decimal = |key: &str| -> Option<f64> {
        let rest = line.split_once(&format!("{key}="))?.1;
        rest.split_whitespace().next()?.parse().ok()
    };
    Some(VectorAggregateProfile {
        rows: integer("rows")?,
        pages: integer("pages")?,
        groups: integer("groups")?,
        rescans: integer("rescans")?,
        bytes: integer("bytes")?,
        max_page_bytes: integer("max_page_bytes")?,
        read_ms: decimal("read_ms")?,
        reduce_ms: decimal("reduce_ms")?,
        rescan_ms: decimal("rescan_ms")?,
        apply_ms: decimal("apply_ms")?,
    })
}

#[allow(dead_code)]
pub async fn container_memory_peak_bytes(container_id: &str) -> Option<u64> {
    let output = tokio::process::Command::new("docker")
        .args(["exec", container_id, "cat", "/sys/fs/cgroup/memory.peak"])
        .output()
        .await
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().parse().ok())?
}

/// Parse a `[PGS_PROFILE]` log line into structured data.
///
/// Format: `[PGS_PROFILE] decision=X.XXms generate+build=X.XXms
///          merge_exec=X.XXms cleanup=X.XXms total=X.XXms
///          affected=N mode=INCR path=cache_hit`
#[allow(dead_code)]
pub fn parse_profile_line(line: &str) -> Option<ProfileData> {
    let extract_ms = |key: &str| -> Option<f64> {
        let prefix = format!("{key}=");
        let start = line.find(&prefix)? + prefix.len();
        let rest = &line[start..];
        let end = rest.find("ms")?;
        rest[..end].parse().ok()
    };
    let extract_int = |key: &str| -> Option<i64> {
        let prefix = format!("{key}=");
        let start = line.find(&prefix)? + prefix.len();
        let rest = &line[start..];
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        rest[..end].parse().ok()
    };
    let extract_str = |key: &str| -> Option<String> {
        let prefix = format!("{key}=");
        let start = line.find(&prefix)? + prefix.len();
        let rest = &line[start..];
        let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
        Some(rest[..end].to_string())
    };
    Some(ProfileData {
        decision_ms: extract_ms("decision")?,
        generate_ms: extract_ms("generate+build")?,
        merge_ms: extract_ms("merge_exec")?,
        cleanup_ms: extract_ms("cleanup")?,
        total_ms: extract_ms("total")?,
        affected: extract_int("affected")?,
        path: extract_str("path")?,
    })
}
