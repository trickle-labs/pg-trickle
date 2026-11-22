//! E2E tests for `pgtrickle.create_or_replace_stream_table()`.
//!
//! Validates idempotent DDL: create when absent, no-op when identical,
//! config-only alter, query replacement, and combined changes.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Helper ─────────────────────────────────────────────────────────────

/// Call `create_or_replace_stream_table` with the most common parameters.
async fn create_or_replace(
    db: &E2eDb,
    name: &str,
    query: &str,
    schedule: &str,
    refresh_mode: &str,
) {
    let sql = format!(
        "SELECT pgtrickle.create_or_replace_stream_table(\
         '{name}', $${query}$$, '{schedule}', '{refresh_mode}')"
    );
    db.execute(&sql).await;
}

// ── 1. Creates when not exists ─────────────────────────────────────────

#[tokio::test]
async fn test_cor_creates_when_not_exists() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO orders VALUES (1, 'US', 100), (2, 'EU', 200)")
        .await;

    create_or_replace(
        &db,
        "order_totals",
        "SELECT region, SUM(amount) AS total FROM orders GROUP BY region",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Verify creation
    let (status, mode, populated, errors) = db.pgt_status("order_totals").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "DIFFERENTIAL");
    assert!(populated);
    assert_eq!(errors, 0);

    let count = db.count("public.order_totals").await;
    assert_eq!(count, 2, "Should have 2 rows (US and EU)");
}

// ── 2. No-op when identical ────────────────────────────────────────────

#[tokio::test]
async fn test_cor_noop_when_identical() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price NUMERIC)")
        .await;
    db.execute("INSERT INTO items VALUES (1, 'Widget', 10), (2, 'Gadget', 20)")
        .await;

    let query = "SELECT id, name, price FROM items";

    // First call — creates
    create_or_replace(&db, "items_snap", query, "1m", "DIFFERENTIAL").await;

    let count_before = db.count("public.items_snap").await;
    assert_eq!(count_before, 2);

    // Capture data_timestamp
    let ts_before: String = db
        .query_scalar(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'items_snap'",
        )
        .await;

    // Second call with identical definition — should be no-op
    create_or_replace(&db, "items_snap", query, "1m", "DIFFERENTIAL").await;

    // data_timestamp should not change (no refresh was triggered)
    let ts_after: String = db
        .query_scalar(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'items_snap'",
        )
        .await;
    assert_eq!(ts_before, ts_after, "No-op should not trigger a refresh");

    let count_after = db.count("public.items_snap").await;
    assert_eq!(count_after, 2);
}

// ── 3. Alters config only (schedule) ───────────────────────────────────

#[tokio::test]
async fn test_cor_alters_config_schedule() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE events (id INT PRIMARY KEY, ts TIMESTAMPTZ DEFAULT now())")
        .await;
    db.execute("INSERT INTO events VALUES (1), (2), (3)").await;

    let query = "SELECT id, ts FROM events";

    // Create with 1m schedule
    create_or_replace(&db, "events_snap", query, "1m", "DIFFERENTIAL").await;

    let sched_before: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'events_snap'",
        )
        .await;
    assert_eq!(sched_before, "1m");

    // Replace with 5m schedule — same query, different config
    create_or_replace(&db, "events_snap", query, "5m", "DIFFERENTIAL").await;

    let sched_after: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'events_snap'",
        )
        .await;
    assert_eq!(sched_after, "5m");

    // Status should still be active
    let (status, _, _, _) = db.pgt_status("events_snap").await;
    assert_eq!(status, "ACTIVE");
}

// ── 4. Replaces query (same output schema) ─────────────────────────────

#[tokio::test]
async fn test_cor_replaces_query_same_schema() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO products VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30), (4, 'D', 40)",
    )
    .await;

    // Create with query filtering price > 15
    create_or_replace(
        &db,
        "expensive_products",
        "SELECT id, name, price FROM products WHERE price > 15",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count1 = db.count("public.expensive_products").await;
    assert_eq!(count1, 3, "Should have 3 rows with price > 15");

    // Replace with query filtering price > 25
    create_or_replace(
        &db,
        "expensive_products",
        "SELECT id, name, price FROM products WHERE price > 25",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count2 = db.count("public.expensive_products").await;
    assert_eq!(count2, 2, "Should have 2 rows with price > 25");

    let (status, _, populated, _) = db.pgt_status("expensive_products").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
}

// ── 5. Replaces query with new columns (compatible schema) ─────────────

#[tokio::test]
async fn test_cor_replaces_query_new_columns() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO employees VALUES \
         (1, 'Alice', 'Eng', 100000), (2, 'Bob', 'Sales', 90000)",
    )
    .await;

    // Create with id and name only
    create_or_replace(
        &db,
        "emp_summary",
        "SELECT id, name FROM employees",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count1 = db.count("public.emp_summary").await;
    assert_eq!(count1, 2);

    // Replace with id, name, dept (added column)
    create_or_replace(
        &db,
        "emp_summary",
        "SELECT id, name, dept FROM employees",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count2 = db.count("public.emp_summary").await;
    assert_eq!(count2, 2);

    // Verify new column exists and has data
    let dept: String = db
        .query_scalar("SELECT dept FROM public.emp_summary WHERE name = 'Alice'")
        .await;
    assert_eq!(dept, "Eng");
}

// ── 6. Replaces query AND config simultaneously ────────────────────────

#[tokio::test]
async fn test_cor_replaces_query_and_config() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE metrics (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO metrics VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    // Create: all rows, 1m schedule
    create_or_replace(
        &db,
        "metrics_snap",
        "SELECT id, val FROM metrics",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count1 = db.count("public.metrics_snap").await;
    assert_eq!(count1, 3);

    // Replace: filtered query + changed schedule
    create_or_replace(
        &db,
        "metrics_snap",
        "SELECT id, val FROM metrics WHERE val > 15",
        "10m",
        "DIFFERENTIAL",
    )
    .await;

    let count2 = db.count("public.metrics_snap").await;
    assert_eq!(count2, 2, "Should have 2 rows with val > 15");

    let sched: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'metrics_snap'",
        )
        .await;
    assert_eq!(sched, "10m");
}

// ── 7. FULL refresh mode ───────────────────────────────────────────────

#[tokio::test]
async fn test_cor_full_refresh_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE data (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO data VALUES (1, 'a'), (2, 'b')")
        .await;

    create_or_replace(&db, "data_snap", "SELECT id, val FROM data", "1m", "FULL").await;

    let (_, mode, _, _) = db.pgt_status("data_snap").await;
    assert_eq!(mode, "FULL");

    let count = db.count("public.data_snap").await;
    assert_eq!(count, 2);
}

// ── 8. Whitespace-only query difference = no-op ────────────────────────

#[tokio::test]
async fn test_cor_whitespace_noop() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ws_test (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO ws_test VALUES (1, 'x'), (2, 'y')")
        .await;

    let query1 = "SELECT id, val FROM ws_test";
    let query2 = "SELECT  id,  val  FROM  ws_test";

    create_or_replace(&db, "ws_snap", query1, "1m", "DIFFERENTIAL").await;

    let ts_before: String = db
        .query_scalar(
            "SELECT updated_at::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ws_snap'",
        )
        .await;

    // Whitespace-different query should be no-op
    create_or_replace(&db, "ws_snap", query2, "1m", "DIFFERENTIAL").await;

    let ts_after: String = db
        .query_scalar(
            "SELECT updated_at::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ws_snap'",
        )
        .await;

    assert_eq!(
        ts_before, ts_after,
        "Whitespace-only difference should be a no-op"
    );
}

// ── 9. Mode switch: DIFFERENTIAL → FULL ────────────────────────────────

#[tokio::test]
async fn test_cor_mode_switch_differential_to_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mode_test (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO mode_test VALUES (1, 100), (2, 200)")
        .await;

    let query = "SELECT id, val FROM mode_test";

    create_or_replace(&db, "mode_snap", query, "1m", "DIFFERENTIAL").await;

    let (_, mode1, _, _) = db.pgt_status("mode_snap").await;
    assert_eq!(mode1, "DIFFERENTIAL");

    // Same query, different mode
    create_or_replace(&db, "mode_snap", query, "1m", "FULL").await;

    let (_, mode2, _, _) = db.pgt_status("mode_snap").await;
    assert_eq!(mode2, "FULL");

    // Data should still be present
    let count = db.count("public.mode_snap").await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_cor_refresh_mode_auto_over_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cor_auto_src (id INT PRIMARY KEY, k INT)")
        .await;
    db.execute("INSERT INTO cor_auto_src SELECT g, g % 4 FROM generate_series(1, 20) g")
        .await;

    let query = "SELECT k, count(*) AS n FROM cor_auto_src GROUP BY k";
    db.create_st("cor_auto_oracle", query, "1m", "AUTO").await;
    db.create_st("cor_auto_explicit", query, "1m", "FULL").await;
    create_or_replace(&db, "cor_auto_explicit", query, "1m", "AUTO").await;

    let oracle: String = db
        .query_scalar(
            "SELECT refresh_mode || '/' || requested_refresh_mode \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'cor_auto_oracle'",
        )
        .await;
    let actual: String = db
        .query_scalar(
            "SELECT refresh_mode || '/' || requested_refresh_mode \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'cor_auto_explicit'",
        )
        .await;

    assert_eq!(oracle, "DIFFERENTIAL/AUTO");
    assert_eq!(actual, oracle);
    db.assert_st_matches_query("cor_auto_explicit", query).await;
}

#[tokio::test]
async fn test_cor_default_refresh_mode_auto_over_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cor_default_auto_src (id INT PRIMARY KEY, k INT)")
        .await;
    db.execute("INSERT INTO cor_default_auto_src SELECT g, g % 4 FROM generate_series(1, 20) g")
        .await;

    let query = "SELECT k, count(*) AS n FROM cor_default_auto_src GROUP BY k";
    db.create_st("cor_default_auto_oracle", query, "1m", "AUTO")
        .await;
    db.create_st("cor_default_auto", query, "1m", "FULL").await;
    db.execute(&format!(
        "SELECT pgtrickle.create_or_replace_stream_table(\
         'cor_default_auto', $${query}$$, '1m')"
    ))
    .await;

    let oracle: String = db
        .query_scalar(
            "SELECT refresh_mode || '/' || requested_refresh_mode \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'cor_default_auto_oracle'",
        )
        .await;
    let actual: String = db
        .query_scalar(
            "SELECT refresh_mode || '/' || requested_refresh_mode \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'cor_default_auto'",
        )
        .await;

    assert_eq!(oracle, "DIFFERENTIAL/AUTO");
    assert_eq!(actual, oracle);
    db.assert_st_matches_query("cor_default_auto", query).await;
}

// ── 10. Incompatible schema change → full rebuild ──────────────────────

#[tokio::test]
async fn test_cor_replaces_query_incompatible() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE incompat (id INT PRIMARY KEY, val TEXT, num NUMERIC)")
        .await;
    db.execute("INSERT INTO incompat VALUES (1, 'hello', 42), (2, 'world', 99)")
        .await;

    // Create with TEXT column
    create_or_replace(
        &db,
        "incompat_snap",
        "SELECT id, val FROM incompat",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count1 = db.count("public.incompat_snap").await;
    assert_eq!(count1, 2);

    // Capture OID before the replace
    let oid_before: i64 = db
        .query_scalar(
            "SELECT pgt_relid::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'incompat_snap'",
        )
        .await;

    // Replace with val cast to INTEGER — truly incompatible type change
    // (text → integer has no implicit cast) on the same column name
    create_or_replace(
        &db,
        "incompat_snap",
        "SELECT id, num::integer AS val FROM incompat",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count2 = db.count("public.incompat_snap").await;
    assert_eq!(count2, 2);

    // Verify the data is correct after rebuild
    let val: i64 = db
        .query_scalar("SELECT val::bigint FROM public.incompat_snap WHERE id = 1")
        .await;
    assert_eq!(val, 42);

    // pgt_id should be preserved (catalog entry not dropped)
    let pgt_id_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'incompat_snap'",
        )
        .await;
    assert_eq!(pgt_id_count, 1, "Catalog entry should be preserved");

    // OID changes for incompatible rebuilds (same column name, incompatible type)
    let oid_after: i64 = db
        .query_scalar(
            "SELECT pgt_relid::bigint FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'incompat_snap'",
        )
        .await;
    assert_ne!(
        oid_before, oid_after,
        "Incompatible schema change should rebuild storage (new OID)"
    );
}

// ── 11. IMMEDIATE mode ─────────────────────────────────────────────────

#[tokio::test]
async fn test_cor_immediate_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE imm_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO imm_src VALUES (1, 10), (2, 20)")
        .await;

    // Create with IMMEDIATE mode (no schedule needed)
    let sql = "SELECT pgtrickle.create_or_replace_stream_table(\
               'imm_snap', $$SELECT id, val FROM imm_src$$, \
               'calculated', 'IMMEDIATE')";
    db.execute(sql).await;

    let (status, mode, populated, _) = db.pgt_status("imm_snap").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "IMMEDIATE");
    assert!(populated);

    let count = db.count("public.imm_snap").await;
    assert_eq!(count, 2);

    // Verify IMMEDIATE semantics: INSERT into source is reflected immediately
    db.execute("INSERT INTO imm_src VALUES (3, 30)").await;
    let count_after = db.count("public.imm_snap").await;
    assert_eq!(
        count_after, 3,
        "IMMEDIATE mode should reflect INSERTs immediately"
    );
}

// ── 12. create_stream_table_if_not_exists — creates ────────────────────

#[tokio::test]
async fn test_if_not_exists_creates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ifne_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO ifne_src VALUES (1, 'a'), (2, 'b')")
        .await;

    // First call — should create
    db.execute(
        "SELECT pgtrickle.create_stream_table_if_not_exists(\
         'ifne_snap', $$SELECT id, val FROM ifne_src$$, '1m', 'DIFFERENTIAL')",
    )
    .await;

    let (status, mode, populated, _) = db.pgt_status("ifne_snap").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "DIFFERENTIAL");
    assert!(populated);

    let count = db.count("public.ifne_snap").await;
    assert_eq!(count, 2);
}

// ── 13. create_stream_table_if_not_exists — noop ───────────────────────

#[tokio::test]
async fn test_if_not_exists_noop() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ifne2_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO ifne2_src VALUES (1, 'x'), (2, 'y')")
        .await;

    // Create the stream table normally first
    db.create_st(
        "ifne2_snap",
        "SELECT id, val FROM ifne2_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let sched_before: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ifne2_snap'",
        )
        .await;
    assert_eq!(sched_before, "1m");

    // Call if_not_exists with DIFFERENT schedule — should be no-op
    db.execute(
        "SELECT pgtrickle.create_stream_table_if_not_exists(\
         'ifne2_snap', $$SELECT id, val FROM ifne2_src$$, '99m', 'FULL')",
    )
    .await;

    // Schedule and mode should NOT have changed
    let (_, mode_after, _, _) = db.pgt_status("ifne2_snap").await;
    assert_eq!(mode_after, "DIFFERENTIAL", "Mode should not change");

    let sched_after: String = db
        .query_scalar(
            "SELECT schedule FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ifne2_snap'",
        )
        .await;
    assert_eq!(
        sched_after, "1m",
        "Schedule should not change on if_not_exists"
    );
}
#[tokio::test]
async fn test_cor_invalid_query_fails() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE source_tbl (id INT PRIMARY KEY, val TEXT)")
        .await;

    create_or_replace(
        &db,
        "test_st_1",
        "SELECT id, val FROM source_tbl",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let res = sqlx::query("SELECT pgtrickle.create_or_replace_stream_table('test_st_1', $$SELECT non_existent FROM source_tbl$$, '1m', 'DIFFERENTIAL')")
        .execute(&db.pool)
        .await;

    assert!(res.is_err(), "Replacing with invalid query should fail");
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains("column \"non_existent\" does not exist"),
        "Unexpected error: {}",
        err_msg
    );
}

// Regression for #1463: inspect an aggregate-list change before detaching a
// publication, without changing the storage or publication binding.
#[tokio::test]
async fn test_cor_published_aggregate_preflight_and_guarded_replace() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE pub1463_events (id BIGINT PRIMARY KEY, grp TEXT NOT NULL, value NUMERIC NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO pub1463_events SELECT g, 'g' || (g % 5), g FROM generate_series(1, 100) g",
    )
    .await;

    let original =
        "SELECT grp, sum(value) AS total, count(*) AS n FROM pub1463_events GROUP BY grp";
    let with_peak = "SELECT grp, sum(value) AS total, count(*) AS n, max(value) AS peak FROM pub1463_events GROUP BY grp";
    create_or_replace(&db, "pub1463_totals", original, "1h", "DIFFERENTIAL").await;
    db.execute("CREATE UNIQUE INDEX pub1463_totals_grp ON pub1463_totals (grp)")
        .await;
    db.execute("ALTER TABLE pub1463_totals REPLICA IDENTITY FULL")
        .await;
    db.execute("SELECT pgtrickle.stream_table_to_publication('pub1463_totals')")
        .await;

    let storage_oid_before = db.table_oid("pub1463_totals").await;
    let publication_oid_before: i64 = db
        .query_scalar(
            "SELECT oid::bigint FROM pg_publication WHERE pubname = 'pgt_pub_pub1463_totals'",
        )
        .await;
    let updated_before: String = db
        .query_scalar(
            "SELECT updated_at::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'pub1463_totals'",
        )
        .await;

    let same: bool = db
        .query_scalar(&format!(
            "SELECT pgtrickle.would_replace_stream_table_storage('pub1463_totals', $q${original}$q$)"
        ))
        .await;
    assert!(!same, "identical definition must be a no-op");
    let cosmetic: bool = db
        .query_scalar(
            "SELECT pgtrickle.would_replace_stream_table_storage('public.pub1463_totals', $q$SELECT  grp,  sum(value) AS total, count(*) AS n FROM pub1463_events  GROUP BY grp$q$)",
        )
        .await;
    assert!(
        !cosmetic,
        "whitespace-only changes must not replace storage"
    );

    let new_aggregate: bool = db
        .query_scalar(&format!(
            "SELECT pgtrickle.would_replace_stream_table_storage('pub1463_totals', $q${with_peak}$q$)"
        ))
        .await;
    assert!(new_aggregate, "the extra aggregate requires new storage");

    let semantic: bool = db
        .query_scalar(
            "SELECT pgtrickle.would_replace_stream_table_storage('pub1463_totals', $q$SELECT grp, sum(value) AS total, count(*) AS n FROM pub1463_events WHERE value > 10 GROUP BY grp$q$)",
        )
        .await;
    assert!(
        semantic,
        "semantic changes with unchanged columns also rebuild"
    );

    assert_eq!(storage_oid_before, db.table_oid("pub1463_totals").await);
    let updated_after: String = db
        .query_scalar(
            "SELECT updated_at::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'pub1463_totals'",
        )
        .await;
    assert_eq!(updated_before, updated_after);
    let publication_oid_after: i64 = db
        .query_scalar(
            "SELECT oid::bigint FROM pg_publication WHERE pubname = 'pgt_pub_pub1463_totals'",
        )
        .await;
    assert_eq!(publication_oid_before, publication_oid_after);

    let replace_sql = format!(
        "SELECT pgtrickle.create_or_replace_stream_table('pub1463_totals', $q${with_peak}$q$, '1h', 'DIFFERENTIAL')"
    );
    let refused = sqlx::query(&replace_sql).execute(&db.pool).await;
    assert!(refused.is_err(), "unguarded replacement must fail closed");
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("drop the publication first"),
        "published replacement should explain why it was blocked"
    );
    assert_eq!(storage_oid_before, db.table_oid("pub1463_totals").await);
    assert_eq!(db.count("pub1463_totals").await, 5);

    // Config-only changes do not replace storage and can retain the publication.
    create_or_replace(&db, "pub1463_totals", original, "2h", "DIFFERENTIAL").await;
    assert_eq!(storage_oid_before, db.table_oid("pub1463_totals").await);

    db.execute("SELECT pgtrickle.drop_stream_table_publication('pub1463_totals')")
        .await;
    create_or_replace(&db, "pub1463_totals", with_peak, "2h", "DIFFERENTIAL").await;
    assert_ne!(storage_oid_before, db.table_oid("pub1463_totals").await);
    let peak: String = db
        .query_scalar("SELECT peak::text FROM pub1463_totals WHERE grp = 'g0'")
        .await;
    assert_eq!(peak, "100");

    let now_noop: bool = db
        .query_scalar(&format!(
            "SELECT pgtrickle.would_replace_stream_table_storage('pub1463_totals', $q${with_peak}$q$)"
        ))
        .await;
    assert!(!now_noop);
    db.execute("SELECT pgtrickle.stream_table_to_publication('pub1463_totals')")
        .await;
    let rebound: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_publication_bindings b \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid = b.publication_oid \
             WHERE st.pgt_name = 'pub1463_totals' AND b.stream_relid = st.pgt_relid \
               AND pr.prrelid = st.pgt_relid)",
        )
        .await;
    assert!(rebound, "publication must bind to replacement storage");
}

#[tokio::test]
async fn test_cor_storage_preflight_missing_and_invalid_queries() {
    let db = E2eDb::new().await.with_extension().await;
    let missing: bool = db
        .query_scalar(
            "SELECT pgtrickle.would_replace_stream_table_storage('pub1463_missing', 'SELECT 1')",
        )
        .await;
    assert!(
        !missing,
        "creating a missing table does not replace storage"
    );

    db.execute("CREATE TABLE pub1463_source (id INT PRIMARY KEY, value INT)")
        .await;
    db.execute("INSERT INTO pub1463_source VALUES (1, 10)")
        .await;
    create_or_replace(
        &db,
        "pub1463_snapshot",
        "SELECT id, value FROM pub1463_source",
        "1m",
        "FULL",
    )
    .await;
    let original_oid = db.table_oid("pub1463_snapshot").await;
    let invalid = sqlx::query_scalar::<_, bool>(
        "SELECT pgtrickle.would_replace_stream_table_storage('pub1463_snapshot', 'SELECT missing_column FROM pub1463_source')",
    )
    .fetch_one(&db.pool)
    .await;
    assert!(
        invalid.is_err(),
        "invalid changed queries must raise an error"
    );
    assert_eq!(original_oid, db.table_oid("pub1463_snapshot").await);
    assert_eq!(db.count("pub1463_snapshot").await, 1);
}

#[tokio::test]
async fn test_cor_storage_preflight_rejects_non_owner() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE pub1463_source (id INT PRIMARY KEY)")
        .await;
    create_or_replace(
        &db,
        "pub1463_private",
        "SELECT id FROM pub1463_source",
        "1m",
        "FULL",
    )
    .await;

    db.execute("CREATE ROLE pub1463_nonowner LOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO pub1463_nonowner")
        .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.would_replace_stream_table_storage(text, text) TO pub1463_nonowner",
    )
    .await;
    let result = db
        .try_execute_with_role(
            "SET ROLE pub1463_nonowner",
            "SELECT pgtrickle.would_replace_stream_table_storage('pub1463_private', 'SELECT id FROM pub1463_source')",
            "RESET ROLE",
        )
        .await;
    assert!(result.is_err(), "preflight must enforce stream ownership");
    assert!(
        result.unwrap_err().to_string().contains("must be owner"),
        "the owner check, not catalog privilege, should reject this call"
    );
}
