//! Light-E2E test harness — stock PostgreSQL container with bind-mounted
//! extension artifacts.  No `shared_preload_libraries`, no background
//! worker, no shared memory.
//!
//! # How It Works
//!
//! 1. `cargo pgrx package` produces compiled extension artifacts in
//!    `target/release/pg_trickle-pg18/`.
//! 2. The artifacts are bind-mounted to `/tmp/pg_ext` inside a stock
//!    `postgres:18.3` container.
//! 3. An `exec` copies the files to the standard PostgreSQL extension
//!    directories.
//! 4. `CREATE EXTENSION pg_trickle` loads the extension on-demand.
//!
//! # Prerequisites
//!
//! ```bash
//! cargo pgrx package --pg-config $(pg_config --bindir)/pg_config
//! ```
//!
//! Or use the justfile target:
//! ```bash
//! just test-light-e2e
//! ```
//!
//! # Limitations
//!
//! - No background worker / scheduler (no `shared_preload_libraries`).
//! - No auto-refresh (`wait_for_auto_refresh` will always time out).
//! - Custom GUCs (`SET pg_trickle.*`) may not be available in all
//!   connections (registered only when `.so` is first loaded).
//! - On macOS, the Light E2E runner must package Linux artifacts via the
//!   Docker builder image and pass them through `PGT_EXTENSION_DIR`.

use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ExecCommand, IntoContainerPort, Mount, WaitFor},
    runners::AsyncRunner,
};

static SHARED_DB_COUNTER: AtomicUsize = AtomicUsize::new(1);
static SHARED_CONTAINER: tokio::sync::OnceCell<SharedContainer> =
    tokio::sync::OnceCell::const_new();

struct SharedContainer {
    admin_connection_string: String,
    /// Name of a pre-seeded template database that already has
    /// `CREATE EXTENSION pg_trickle` applied.  Per-test databases are
    /// cloned from this template via `CREATE DATABASE … TEMPLATE`, avoiding
    /// the full extension-install DDL cost on every test.
    template_db_name: String,
    port: u16,
    container_id: String,
    // When None the process does not own the container (it was started
    // externally by the shell script via PGT_LIGHT_E2E_PORT).
    _container: Option<Mutex<ContainerAsync<GenericImage>>>,
}

enum ContainerLease {
    Shared { _shared: &'static SharedContainer },
}

/// Find the `cargo pgrx package` output directory.
///
/// Checks `PGT_EXTENSION_DIR` env var first, then falls back to the
/// default pgrx package output path.
fn is_valid_light_e2e_package_dir(dir: &str) -> bool {
    let base = std::path::Path::new(dir);
    base.join("usr/share/postgresql/18/extension/pg_trickle.control")
        .exists()
        && base
            .join("usr/lib/postgresql/18/lib/pg_trickle.so")
            .exists()
}

fn find_extension_dir() -> String {
    if let Ok(dir) = std::env::var("PGT_EXTENSION_DIR")
        && !dir.is_empty()
        && is_valid_light_e2e_package_dir(&dir)
    {
        return dir;
    }

    // Default: cargo pgrx package output
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let default_path = format!("{}/target/release/pg_trickle-pg18", manifest_dir);
    if is_valid_light_e2e_package_dir(&default_path) {
        return default_path;
    }

    panic!(
        "Valid Linux light-E2E extension package directory not found.\n\
         Expected packaged artifacts under usr/share/postgresql/18/extension and\n\
         usr/lib/postgresql/18/lib.\n\
         Run `bash ./scripts/run_light_e2e_tests.sh --package-only` first,\n\
         or set PGT_EXTENSION_DIR to a valid Linux package output directory."
    );
}

fn shared_db_name(prefix: &str) -> String {
    let sequence = SHARED_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{}", std::process::id(), sequence)
}

fn connection_string(port: u16, db_name: &str) -> String {
    format!("postgres://postgres:postgres@127.0.0.1:{port}/{db_name}")
}

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

async fn drop_database_if_exists(admin_cs: &str, db_name: &str) {
    let Ok(pool) = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_cs)
        .await
    else {
        return;
    };
    // Terminate any lingering connections so DROP DATABASE succeeds.
    let _ = sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(db_name)
    .execute(&pool)
    .await;
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS \"{db_name}\""
    )))
    .execute(&pool)
    .await;
    pool.close().await;
}

/// Create a database named `db_name` as a file-system clone of `template`.
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

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE \"{db_name}\" TEMPLATE \"{template}\""
    )))
    .execute(&admin_pool)
    .await
    .unwrap_or_else(|e| panic!("Failed to CREATE DATABASE {db_name} from template: {e}"));

    admin_pool.close().await;
}

/// Install pg_trickle once into a dedicated template database so that each
/// per-test database can be cloned cheaply via `CREATE DATABASE … TEMPLATE`.
///
/// Returns the name of the created template database.
async fn create_extension_template(admin_connection_string: &str, port: u16) -> String {
    // CREATE EXTENSION also creates cluster-wide predefined roles.  The
    // light-E2E shards share one PostgreSQL instance, so serialize template
    // installation across test-binary processes.
    let lock_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_connection_string)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect for template lock: {e}"));
    let mut template_lock = lock_pool
        .acquire()
        .await
        .unwrap_or_else(|e| panic!("Failed to acquire template lock: {e}"));
    sqlx::query("SELECT pg_advisory_lock($1::bigint)")
        .bind(7_102_018_i64)
        .execute(&mut *template_lock)
        .await
        .unwrap_or_else(|e| panic!("Failed to acquire template lock: {e}"));

    // Use a PID-scoped name so that multiple test binary processes sharing
    // the same PostgreSQL server (e.g. the light-E2E shared container where
    // PGT_LIGHT_E2E_PORT is set) each get their own template without
    // conflicting with each other.
    let template_name = format!("pgt_ext_template_{}", std::process::id());
    let template_name = template_name.as_str();

    create_database(admin_connection_string, template_name).await;

    let template_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&connection_string(port, template_name))
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to template DB for extension init: {e}"));

    sqlx::query("CREATE EXTENSION pg_trickle CASCADE")
        .execute(&template_pool)
        .await
        .unwrap_or_else(|e| panic!("Failed to CREATE EXTENSION on template DB: {e}"));

    template_pool.close().await;
    sqlx::query("SELECT pg_advisory_unlock($1::bigint)")
        .bind(7_102_018_i64)
        .execute(&mut *template_lock)
        .await
        .unwrap_or_else(|e| panic!("Failed to release template lock: {e}"));
    drop(template_lock);
    lock_pool.close().await;

    template_name.to_string()
}

async fn shared_container() -> &'static SharedContainer {
    SHARED_CONTAINER
        .get_or_init(|| async {
            // ── Fast path: shell script pre-started a single container ────────
            // PGT_LIGHT_E2E_PORT is set by run_light_e2e_tests.sh before cargo
            // nextest is invoked.  All 48+ test binary processes share this one
            // container instead of each spawning their own.  This prevents
            // Docker resource exhaustion (StartupTimeout / PortNotExposed).
            if let Ok(port_str) = std::env::var("PGT_LIGHT_E2E_PORT") {
                let port: u16 = port_str
                    .parse()
                    .expect("PGT_LIGHT_E2E_PORT must be a valid port number");
                let container_id = std::env::var("PGT_LIGHT_E2E_CONTAINER_ID")
                    .unwrap_or_else(|_| "external".to_string());
                let admin_connection_string = connection_string(port, "postgres");
                let template_db_name =
                    create_extension_template(&admin_connection_string, port).await;
                return SharedContainer {
                    admin_connection_string,
                    template_db_name,
                    port,
                    container_id,
                    _container: None,
                };
            }

            // ── Fallback: per-binary container (direct cargo test invocations) ─
            let ext_dir = find_extension_dir();
            let run_id = std::env::var("PGT_LIGHT_E2E_RUN_ID").ok();

            let mut image = GenericImage::new("postgres", "18.3")
                .with_exposed_port(5432_u16.tcp())
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_PASSWORD", "postgres")
                .with_env_var("POSTGRES_DB", "postgres")
                .with_cmd(["postgres", "-c", "track_commit_timestamp=on"])
                .with_mount(Mount::bind_mount(ext_dir, "/tmp/pg_ext"))
                .with_label("com.pgtrickle.test", "true")
                .with_label("com.pgtrickle.suite", "light-e2e")
                .with_label("com.pgtrickle.repo", "pg-stream");

            if let Some(run_id) = run_id {
                image = image.with_label("com.pgtrickle.run-id", run_id);
            }

            let container = image.start().await.expect(
                "Failed to start shared light-e2e container.\n\
                     Ensure Docker is running and postgres:18.3 is available.",
            );

            container
                .exec(ExecCommand::new(vec![
                    "sh",
                    "-c",
                    "cp /tmp/pg_ext/usr/share/postgresql/18/extension/pg_trickle* \
                        /usr/share/postgresql/18/extension/ && \
                     cp /tmp/pg_ext/usr/lib/postgresql/18/lib/pg_trickle* \
                        /usr/lib/postgresql/18/lib/",
                ]))
                .await
                .expect("Failed to copy extension files into shared light-e2e container");

            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("Failed to get mapped port");
            let admin_connection_string = connection_string(port, "postgres");

            // Pre-seed a template database with the extension installed once.
            let template_db_name = create_extension_template(&admin_connection_string, port).await;

            SharedContainer {
                admin_connection_string,
                template_db_name,
                port,
                container_id: container.id().to_string(),
                _container: Some(Mutex::new(container)),
            }
        })
        .await
}

/// A test database backed by a stock PostgreSQL 18.3 container with
/// the compiled pg_trickle extension bind-mounted and installed
/// **without** `shared_preload_libraries`.
///
/// The extension is loaded on-demand when `CREATE EXTENSION` is called.
/// Background worker, scheduler, and shared-memory features are NOT
/// available.
pub struct E2eDb {
    pub pool: PgPool,
    connection_string: String,
    admin_connection_string: String,
    db_name: String,
    container_id: String,
    _container: ContainerLease,
}

impl Drop for E2eDb {
    fn drop(&mut self) {
        let admin_cs = self.admin_connection_string.clone();
        let db_name = self.db_name.clone();
        // Clean up the test database in a background OS thread so cleanup is
        // decoupled from any tokio runtime that may be shutting down.  Tests
        // can run in parallel (each has its own database), so we just fire and
        // forget; the count of live databases at any moment stays small.
        std::thread::spawn(move || {
            if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                rt.block_on(drop_database_if_exists(&admin_cs, &db_name));
            }
        });
    }
}

#[allow(dead_code)]
impl E2eDb {
    /// Start a fresh PostgreSQL 18.3 container, install the extension
    /// artifacts via bind-mount, and create the extension.
    pub async fn new() -> Self {
        let shared = shared_container().await;
        let db_name = shared_db_name("pgt_light_e2e");
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
            admin_connection_string: shared.admin_connection_string.clone(),
            db_name: db_name.clone(),
            container_id: shared.container_id.clone(),
            _container: ContainerLease::Shared { _shared: shared },
        }
    }

    /// Start a fresh database WITHOUT the extension pre-installed.
    ///
    /// Unlike [`Self::new`] (which clones from the pre-seeded template), this
    /// creates a plain empty database.  Use this for upgrade tests that need
    /// to run `CREATE EXTENSION pg_trickle VERSION '<old_version>'` themselves.
    pub async fn new_without_extension() -> Self {
        let shared = shared_container().await;
        let db_name = shared_db_name("pgt_upgrade_light_e2e");
        create_database(&shared.admin_connection_string, &db_name).await;
        let connection_string = connection_string(shared.port, &db_name);
        let pool = Self::connect_with_retry(&connection_string, 15).await;

        E2eDb {
            pool,
            connection_string,
            admin_connection_string: shared.admin_connection_string.clone(),
            db_name: db_name.clone(),
            container_id: shared.container_id.clone(),
            _container: ContainerLease::Shared { _shared: shared },
        }
    }

    /// Light harness does not support the background worker.
    /// Falls back to `new()` (connects to `pg_trickle_test` database).
    pub async fn new_on_postgres_db() -> Self {
        panic!(
            "new_on_postgres_db() requires shared_preload_libraries.\n\
             This test needs the full E2E harness (just test-e2e)."
        );
    }

    /// Light harness does not support bench-tuned containers.
    pub async fn new_bench() -> Self {
        panic!(
            "new_bench() requires shared_preload_libraries and SHM tuning.\n\
             This test needs the full E2E harness (just test-e2e)."
        );
    }

    /// Get the Docker container ID.
    pub fn container_id(&self) -> &str {
        &self.container_id
    }

    /// Get the connection string for this database.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Retry connection with backoff.
    async fn connect_with_retry(url: &str, max_attempts: u32) -> PgPool {
        for attempt in 1..=max_attempts {
            match PgPool::connect(url).await {
                Ok(pool) => match sqlx::query("SELECT 1").execute(&pool).await {
                    Ok(_) => return pool,
                    Err(e) if attempt < max_attempts => {
                        eprintln!(
                            "Light-E2E connect attempt {}/{}: ping failed: {}",
                            attempt, max_attempts, e
                        );
                    }
                    Err(e) => {
                        panic!(
                            "Light-E2E: Failed to ping after {} attempts: {}",
                            max_attempts, e
                        );
                    }
                },
                Err(e) if attempt < max_attempts => {
                    eprintln!(
                        "Light-E2E connect attempt {}/{}: {}",
                        attempt, max_attempts, e
                    );
                }
                Err(e) => {
                    panic!(
                        "Light-E2E: Failed to connect after {} attempts: {}",
                        max_attempts, e
                    );
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        unreachable!()
    }

    /// Install the extension (`CREATE EXTENSION pg_trickle`).
    pub async fn with_extension(self) -> Self {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trickle CASCADE")
            .execute(&self.pool)
            .await
            .expect("Failed to CREATE EXTENSION pg_trickle");
        self
    }

    // ── SQL Execution Helpers ──────────────────────────────────────────

    /// Reload PostgreSQL configuration and wait briefly for SIGHUP settings to apply.
    pub async fn reload_config_and_wait(&self) {
        self.execute("SELECT pg_reload_conf()").await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    /// Read a GUC value after forcing the extension to load on the same backend.
    pub async fn show_setting(&self, setting: &str) -> String {
        let mut conn =
            self.pool.acquire().await.unwrap_or_else(|e| {
                panic!("Failed to acquire DB connection for SHOW {setting}: {e}")
            });

        let _: String = sqlx::query_scalar("SELECT pgtrickle.version()")
            .fetch_one(&mut *conn)
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to load pg_trickle on backend before SHOW {setting}: {e}")
            });

        let show_sql = format!("SHOW {setting}");
        sqlx::query_scalar(sqlx::AssertSqlSafe(show_sql.as_str()))
            .fetch_one(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("Scalar query failed: {}\nSQL: {}", e, show_sql))
    }

    /// Wait until `SHOW <setting>` reports the expected value.
    pub async fn wait_for_setting(&self, setting: &str, expected: &str) {
        for _ in 0..30 {
            let current = self.show_setting(setting).await;
            if current == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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

    /// Wait for any pg_trickle scheduler background worker to appear.
    ///
    /// **Not supported in light-e2e mode** — always returns `false`
    /// because background worker is not running.
    pub async fn wait_for_scheduler(&self, _timeout: std::time::Duration) -> bool {
        false
    }

    /// Wait for the background scheduler to auto-refresh a ST.
    ///
    /// **Not supported in light-e2e mode** — always returns `false`
    /// because background worker is not running.
    pub async fn wait_for_auto_refresh(
        &self,
        _pgt_name: &str,
        _timeout: std::time::Duration,
    ) -> bool {
        false
    }
}
