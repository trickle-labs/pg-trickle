//! Shared TPC-H test helpers.
//!
//! P2.11: Extracted from `e2e_tpch_tests.rs` and `e2e_tpch_dag_tests.rs` to
//! eliminate ~120 lines of duplication.  Both integration test binaries include
//! this module via `mod tpch;`.
//!
//! Because each integration-test file is its own compilation unit, the tpch
//! module is compiled independently for each binary.  `super::e2e::E2eDb`
//! refers to the `e2e` module declared by the parent test file.

#![allow(dead_code)] // some helpers are only used by one of the two test files

// ── Configuration ───────────────────────────────────────────────────────

/// Scale factor for TPC-H data generation.
pub fn scale_factor() -> f64 {
    std::env::var("TPCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.01)
}

/// Number of refresh cycles per query.
pub fn cycles() -> usize {
    std::env::var("TPCH_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
}

// ── Scale-factor dimensions ─────────────────────────────────────────────

pub fn sf_orders() -> usize {
    ((scale_factor() * 150_000.0) as usize).max(1_500)
}
pub fn sf_customers() -> usize {
    ((scale_factor() * 15_000.0) as usize).max(150)
}
pub fn sf_suppliers() -> usize {
    ((scale_factor() * 1_000.0) as usize).max(10)
}
pub fn sf_parts() -> usize {
    ((scale_factor() * 20_000.0) as usize).max(200)
}

/// Number of rows per RF cycle (INSERT/DELETE/UPDATE batch size).
pub fn rf_count() -> usize {
    let sf = scale_factor();
    let orders = ((sf * 150_000.0) as usize).max(1_500);
    (orders / 100).max(10)
}

// ── SQL file embedding ──────────────────────────────────────────────────
//
// Paths are relative to this file (tests/tpch/mod.rs), so bare filenames
// refer to files in tests/tpch/.

pub const SCHEMA_SQL: &str = include_str!("schema.sql");
pub const DATAGEN_SQL: &str = include_str!("datagen.sql");
pub const RF1_SQL: &str = include_str!("rf1.sql");
pub const RF2_SQL: &str = include_str!("rf2.sql");
pub const RF3_SQL: &str = include_str!("rf3.sql");

// ── Token substitution ──────────────────────────────────────────────────

/// Replace scale-factor tokens in a SQL template.
pub fn substitute_sf(sql: &str) -> String {
    sql.replace("__SF_ORDERS__", &sf_orders().to_string())
        .replace("__SF_CUSTOMERS__", &sf_customers().to_string())
        .replace("__SF_SUPPLIERS__", &sf_suppliers().to_string())
        .replace("__SF_PARTS__", &sf_parts().to_string())
}

/// Replace RF tokens in a mutation SQL template.
pub fn substitute_rf(sql: &str, next_orderkey: usize) -> String {
    sql.replace("__RF_COUNT__", &rf_count().to_string())
        .replace("__NEXT_ORDERKEY__", &next_orderkey.to_string())
        .replace("__SF_CUSTOMERS__", &sf_customers().to_string())
        .replace("__SF_PARTS__", &sf_parts().to_string())
        .replace("__SF_SUPPLIERS__", &sf_suppliers().to_string())
}

// ── Database helpers ────────────────────────────────────────────────────
//
// These functions reference `super::e2e::E2eDb` which resolves to the `e2e`
// module declared by the test file that includes this module.

use super::e2e::E2eDb;

/// Execute each semicolon-delimited SQL statement in `sql`, skipping blank
/// and comment-only segments.  Panics on the first error.
pub async fn exec_sql(db: &E2eDb, sql: &str) {
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.execute(stmt).await;
        }
    }
}

/// Execute each semicolon-delimited SQL statement, returning the first error.
pub async fn try_exec_sql(db: &E2eDb, sql: &str) -> Result<(), String> {
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.try_execute(stmt).await.map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// Load the TPC-H schema into the database.
pub async fn load_schema(db: &E2eDb) {
    exec_sql(db, SCHEMA_SQL).await;
}

/// Load TPC-H data at the configured scale factor.
pub async fn load_data(db: &E2eDb) {
    let sql = substitute_sf(DATAGEN_SQL);
    exec_sql(db, &sql).await;
}

/// Return the current maximum `o_orderkey` value (used to generate non-conflicting RF1 keys).
pub async fn max_orderkey(db: &E2eDb) -> usize {
    let max: i64 = db
        .query_scalar("SELECT COALESCE(MAX(o_orderkey), 0)::bigint FROM orders")
        .await;
    max as usize
}

/// Apply RF1 (bulk INSERT into orders + lineitem).
pub async fn apply_rf1(db: &E2eDb, next_orderkey: usize) {
    let sql = substitute_rf(RF1_SQL, next_orderkey);
    exec_sql(db, &sql).await;
}

/// Apply RF2 (bulk DELETE from orders + lineitem).
pub async fn apply_rf2(db: &E2eDb) {
    let sql = RF2_SQL.replace("__RF_COUNT__", &rf_count().to_string());
    exec_sql(db, &sql).await;
}

/// Apply RF3 (targeted UPDATEs).
pub async fn apply_rf3(db: &E2eDb) {
    let sql = RF3_SQL.replace("__RF_COUNT__", &rf_count().to_string());
    exec_sql(db, &sql).await;
}

/// Assert that a stream table is multiset-equal to its defining query.
///
/// Returns `Ok(())` on match; `Err(description)` with diagnostic details on
/// mismatch so the caller can decide whether to soft-skip or hard-fail.
///
/// Also checks for negative `__pgt_count` (over-retraction bug).
pub async fn assert_invariant(
    db: &E2eDb,
    st_name: &str,
    query: &str,
    qname: &str,
    cycle: usize,
) -> Result<(), String> {
    let st_table = format!("public.{st_name}");

    let cols: String = db
        .query_scalar(&format!(
            "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) \
             FROM information_schema.columns \
             WHERE (table_schema || '.' || table_name = 'public.{st_name}' \
                OR table_name = '{st_name}') \
             AND left(column_name, 6) <> '__pgt_'"
        ))
        .await;

    // Negative __pgt_count guard (over-retraction).
    let has_pgt_count: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS ( \
                SELECT 1 FROM information_schema.columns \
                WHERE table_name = '{st_name}' \
                  AND column_name = '__pgt_count' \
            )"
        ))
        .await;
    if has_pgt_count {
        let neg_count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {st_table} WHERE __pgt_count < 0"
            ))
            .await;
        if neg_count > 0 {
            return Err(format!(
                "NEGATIVE __pgt_count: {qname} cycle {cycle} — \
                 {neg_count} rows with __pgt_count < 0 (over-retraction bug)"
            ));
        }
    }

    // Multiset equality via symmetric EXCEPT ALL.
    let matches: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS ( \
                (SELECT {cols} FROM {st_table} EXCEPT ALL ({query})) \
                UNION ALL \
                (({query}) EXCEPT ALL SELECT {cols} FROM {st_table}) \
            )"
        ))
        .await;

    if matches {
        return Ok(());
    }

    // Collect diagnostics.
    let st_count: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM {st_table}"))
        .await;
    let q_count: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM ({query}) _q"))
        .await;
    let extra: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM \
             (SELECT {cols} FROM {st_table} EXCEPT ALL ({query})) _x"
        ))
        .await;
    let missing: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM \
             (({query}) EXCEPT ALL SELECT {cols} FROM {st_table}) _x"
        ))
        .await;

    let extra_rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT row_to_json(x)::text FROM \
         (SELECT {cols} FROM {st_table} EXCEPT ALL ({query})) x \
         LIMIT 5"
    )))
    .fetch_all(&db.pool)
    .await
    .unwrap_or_default();

    let missing_rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT row_to_json(x)::text FROM \
         (({query}) EXCEPT ALL SELECT {cols} FROM {st_table}) x \
         LIMIT 5"
    )))
    .fetch_all(&db.pool)
    .await
    .unwrap_or_default();

    if !extra_rows.is_empty() {
        println!("    EXTRA rows in ST (not in query):");
        for (row,) in &extra_rows {
            println!("      {row}");
        }
    }
    if !missing_rows.is_empty() {
        println!("    MISSING rows from ST (in query, not in ST):");
        for (row,) in &missing_rows {
            println!("      {row}");
        }
    }

    Err(format!(
        "INVARIANT VIOLATION: {qname} cycle {cycle} — \
         ST rows: {st_count}, Q rows: {q_count}, \
         extra: {extra}, missing: {missing}"
    ))
}
