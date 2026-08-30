//! Shared test helpers for integration tests using Testcontainers.
#![allow(dead_code)]

use sqlx::PgPool;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// SQL to create the pgtrickle catalog schema and tables.
/// Generated from `sql/archive/pg_trickle--<version>.sql` by `scripts/gen_test_schema.py`.
/// Regenerate: `python3 scripts/gen_test_schema.py > tests/generated/schema.rs`
#[allow(dead_code)]
pub const CATALOG_DDL: &str = include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/generated/schema.rs"
));

/// A test database backed by a Testcontainers PostgreSQL 18.3 instance.
///
/// The container is automatically cleaned up when `TestDb` is dropped.
pub struct TestDb {
    pub pool: PgPool,
    _container: ContainerAsync<Postgres>,
}

#[allow(dead_code)]
impl TestDb {
    /// Start a fresh PostgreSQL 18.3 container and connect to it.
    pub async fn new() -> Self {
        let container = Postgres::default()
            .with_tag("18.3-alpine")
            .start()
            .await
            .expect("Failed to start PostgreSQL 18.3 container");

        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get mapped port");

        let connection_string = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);

        let pool = PgPool::connect(&connection_string)
            .await
            .expect("Failed to connect to test database");

        // Standalone DVM tests execute generated SQL without loading the
        // extension shared library. Keep the test identity contract typed so
        // those fixtures exercise BYTEA transport and exact probe matching.
        sqlx::raw_sql(
            r#"
CREATE SCHEMA IF NOT EXISTS pgtrickle;
CREATE OR REPLACE FUNCTION pgtrickle.encode_row_id_v2(domain TEXT, value ANYELEMENT)
RETURNS BYTEA
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
AS $$ SELECT decode(md5(domain || ':' || value::text), 'hex') $$;
CREATE OR REPLACE FUNCTION pgtrickle.row_probe_v1(value BYTEA)
RETURNS BYTEA
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
AS $$ SELECT substring(value FROM 1 FOR 128) $$;
CREATE OR REPLACE FUNCTION pgtrickle.test_int_to_row_id(value INTEGER)
RETURNS BYTEA
LANGUAGE SQL IMMUTABLE STRICT
AS $$ SELECT decode(md5('SCAN_KEY:(' || value::text || ')'), 'hex') $$;
CREATE OR REPLACE FUNCTION pgtrickle.test_bigint_to_row_id(value BIGINT)
RETURNS BYTEA
LANGUAGE SQL IMMUTABLE STRICT
AS $$ SELECT decode(md5('SCAN_KEY:(' || value::text || ')'), 'hex') $$;
DO $$ BEGIN
    CREATE CAST (INTEGER AS BYTEA)
        WITH FUNCTION pgtrickle.test_int_to_row_id(INTEGER) AS IMPLICIT;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    CREATE CAST (BIGINT AS BYTEA)
        WITH FUNCTION pgtrickle.test_bigint_to_row_id(BIGINT) AS IMPLICIT;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
"#,
        )
        .execute(&pool)
        .await
        .expect("Failed to create standalone test identity helpers");

        TestDb {
            pool,
            _container: container,
        }
    }

    /// Start a fresh container with the pg_trickle catalog schema pre-created.
    pub async fn with_catalog() -> Self {
        let db = Self::new().await;
        // Use raw_sql to execute multiple DDL statements in one call
        sqlx::raw_sql(CATALOG_DDL)
            .execute(&db.pool)
            .await
            .expect("Failed to create pg_trickle catalog schema");
        db
    }

    /// Execute a SQL statement.
    pub async fn execute(&self, sql: &str) {
        sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .execute(&self.pool)
            .await
            .unwrap_or_else(|e| panic!("SQL execution failed: {}\nSQL: {}", e, sql));
    }

    /// Execute a SQL statement, returning Ok/Err instead of panicking.
    pub async fn try_execute(&self, sql: &str) -> Result<(), sqlx::Error> {
        sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
            .execute(&self.pool)
            .await
            .map(|_| ())
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

    /// Assert that two tables/subqueries contain exactly the same multiset of rows.
    ///
    /// Uses the symmetric set-difference pattern:
    /// ```sql
    /// SELECT NOT EXISTS (
    ///   (SELECT cols FROM a EXCEPT ALL SELECT cols FROM b)
    ///   UNION ALL
    ///   (SELECT cols FROM b EXCEPT ALL SELECT cols FROM a)
    /// )
    /// ```
    /// This catches: missing rows, extra rows, duplicate discrepancies, and column
    /// value mutations. Both `table_a` and `table_b` can be table names or
    /// parenthesized subqueries.
    pub async fn assert_sets_equal(&self, table_a: &str, table_b: &str, cols: &[&str]) {
        let col_list = cols.join(", ");
        let sql = format!(
            "SELECT NOT EXISTS (
                (SELECT {col_list} FROM {a} EXCEPT ALL SELECT {col_list} FROM {b})
                UNION ALL
                (SELECT {col_list} FROM {b} EXCEPT ALL SELECT {col_list} FROM {a})
            )",
            col_list = col_list,
            a = table_a,
            b = table_b
        );
        let matches: bool = self.query_scalar(&sql).await;
        assert!(
            matches,
            "Set mismatch between {} and {} (columns: {})",
            table_a, table_b, col_list
        );
    }

    /// Assert that two table names expose exactly the same column names **and**
    /// data types (in declaration order).
    ///
    /// When `assert_types_match` is `false` this is a no-op, so callers can
    /// keep the parameter in their call-sites while deferring the type check.
    pub async fn assert_column_types_match(
        &self,
        table_a: &str,
        table_b: &str,
        assert_types_match: bool,
    ) {
        if !assert_types_match {
            return;
        }
        let type_sql = |table: &str| {
            // Strip optional schema prefix so the WHERE clause works for
            // both `public.my_st` and plain `my_st`.
            let (schema_filter, name_filter) = if let Some(dot) = table.rfind('.') {
                (
                    format!("table_schema = '{}'", &table[..dot]),
                    format!("table_name = '{}'", &table[dot + 1..]),
                )
            } else {
                (
                    "table_schema NOT IN ('pg_catalog','information_schema')".to_string(),
                    format!("table_name = '{table}'"),
                )
            };
            format!(
                "SELECT column_name, data_type \
                 FROM information_schema.columns \
                 WHERE {schema_filter} AND {name_filter} \
                   AND column_name NOT LIKE '__pgt_%' \
                 ORDER BY ordinal_position"
            )
        };

        let cols_a: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(type_sql(table_a)))
            .fetch_all(&self.pool)
            .await
            .unwrap_or_else(|e| panic!("type query for {table_a} failed: {e}"));
        let cols_b: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(type_sql(table_b)))
            .fetch_all(&self.pool)
            .await
            .unwrap_or_else(|e| panic!("type query for {table_b} failed: {e}"));

        assert_eq!(
            cols_a, cols_b,
            "Column type mismatch between {table_a} and {table_b}"
        );
    }
}

// ── A42-9: State-polling helpers (replace fixed sleeps) ──────────────────────

use std::time::Duration;

/// Poll until the refresh history for `st_name` has at least `min_count` rows,
/// or until `timeout` elapses.
///
/// Returns `true` if the condition was met before the timeout.
///
/// Use this instead of `tokio::time::sleep(...)` in tests that wait for the
/// background worker to complete at least one refresh cycle.
pub async fn wait_for_refresh_history(
    pool: &sqlx::PgPool,
    st_name: &str,
    min_count: i64,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    let (schema, name) = if let Some(dot) = st_name.rfind('.') {
        (&st_name[..dot], &st_name[dot + 1..])
    } else {
        ("public", st_name)
    };
    loop {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables s ON s.pgt_id = h.pgt_id \
             WHERE s.pgt_schema = $1 AND s.pgt_name = $2 \
               AND h.status = 'COMPLETED'",
        )
        .bind(schema)
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap_or(0);

        if count >= min_count {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll until the stream table `st_name` has `last_refresh_at IS NOT NULL`
/// (i.e., at least one refresh has completed), or until `timeout` elapses.
///
/// Returns `true` if the condition was met before the timeout.
pub async fn wait_for_first_refresh(pool: &sqlx::PgPool, st_name: &str, timeout: Duration) -> bool {
    wait_for_refresh_history(pool, st_name, 1, timeout).await
}

/// Poll until `last_refresh_at` for `st_name` is strictly after `after_ts`,
/// indicating a *new* refresh cycle completed after the caller's operation.
///
/// Pass the current `last_refresh_at` timestamp before triggering the operation
/// to ensure you wait for a fresh cycle rather than seeing a stale one.
/// Pass `None` to wait for the first refresh from scratch.
///
/// Returns `true` if the condition was met before the timeout.
pub async fn wait_for_refresh_after(
    pool: &sqlx::PgPool,
    st_name: &str,
    after_ts: Option<&str>,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    let (schema, name) = if let Some(dot) = st_name.rfind('.') {
        (&st_name[..dot], &st_name[dot + 1..])
    } else {
        ("public", st_name)
    };
    loop {
        let ts: Option<String> = sqlx::query_scalar(
            "SELECT last_refresh_at::text \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = $1 AND pgt_name = $2",
        )
        .bind(schema)
        .bind(name)
        .fetch_optional(pool)
        .await
        .unwrap_or(None)
        .flatten();

        let refreshed = match (ts.as_deref(), after_ts) {
            (Some(_), None) => true,
            (Some(t), Some(after)) => t > after,
            _ => false,
        };
        if refreshed {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll until the CDC mode for `st_name` matches `expected_mode`,
/// or until `timeout` elapses.
///
/// Returns `true` if the condition was met before the timeout.
pub async fn wait_for_cdc_mode(
    pool: &sqlx::PgPool,
    st_name: &str,
    expected_mode: &str,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    let (schema, name) = if let Some(dot) = st_name.rfind('.') {
        (&st_name[..dot], &st_name[dot + 1..])
    } else {
        ("public", st_name)
    };
    loop {
        // CDC mode is stored per-dependency source; check the effective mode
        // via pgt_change_tracking joined to pgt_dependencies.
        let mode: Option<String> = sqlx::query_scalar(
            "SELECT ct.cdc_mode \
             FROM pgtrickle.pgt_change_tracking ct \
             JOIN pgtrickle.pgt_dependencies d ON d.source_relid = ct.source_relid \
             JOIN pgtrickle.pgt_stream_tables s ON s.pgt_id = d.pgt_id \
             WHERE s.pgt_schema = $1 AND s.pgt_name = $2 \
             LIMIT 1",
        )
        .bind(schema)
        .bind(name)
        .fetch_optional(pool)
        .await
        .unwrap_or(None)
        .flatten();

        if mode.as_deref() == Some(expected_mode) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll until the scheduler watermark (last_tick_at) has advanced at least
/// `min_ticks` times beyond the current value when this function is called,
/// or until `timeout` elapses.
///
/// Returns `true` if the condition was met before the timeout.
pub async fn wait_for_scheduler_tick(
    pool: &sqlx::PgPool,
    min_ticks: u32,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    // Capture baseline tick count
    let baseline: i64 = sqlx::query_scalar(
        "SELECT COALESCE(COUNT(*), 0) \
         FROM pgtrickle.pgt_refresh_history \
         WHERE status = 'COMPLETED'",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let target = baseline + min_ticks as i64;
    loop {
        let count: i64 = sqlx::query_scalar(
            "SELECT COALESCE(COUNT(*), 0) \
             FROM pgtrickle.pgt_refresh_history \
             WHERE status = 'COMPLETED'",
        )
        .fetch_one(pool)
        .await
        .unwrap_or(0);

        if count >= target {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Poll until `st_name` has status `expected_status`, or until `timeout` elapses.
///
/// Returns `true` if the condition was met before the timeout.
pub async fn wait_for_stream_table_status(
    pool: &sqlx::PgPool,
    st_name: &str,
    expected_status: &str,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    let (schema, name) = if let Some(dot) = st_name.rfind('.') {
        (&st_name[..dot], &st_name[dot + 1..])
    } else {
        ("public", st_name)
    };
    loop {
        let status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = $1 AND pgt_name = $2",
        )
        .bind(schema)
        .bind(name)
        .fetch_optional(pool)
        .await
        .unwrap_or(None)
        .flatten();

        if status.as_deref() == Some(expected_status) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll until a SQL query returning a count reaches at least `min_count`,
/// or until `timeout` elapses.
///
/// Returns `true` if the condition was met before the timeout.
///
/// This helper replaces common polling loops like:
/// ```ignore
/// loop {
///     if start.elapsed() > timeout { break; }
///     tokio::time::sleep(Duration::from_millis(500)).await;
///     if db.count("table").await >= N { break; }
/// }
/// ```
pub async fn wait_for_query_count(
    pool: &sqlx::PgPool,
    sql: &str,
    min_count: i64,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned()))
            .fetch_one(pool)
            .await
            .unwrap_or(0);
        if count >= min_count {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}
