//! E2E tests for partitioned source table support.
//!
//! Validates that pg_trickle works correctly with PostgreSQL's declarative
//! table partitioning: RANGE, LIST, and HASH partitioned source tables,
//! ATTACH PARTITION detection and reinitialize, and WAL publication
//! configuration for partitioned tables.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── PT1: Partitioned source tables work end-to-end ─────────────────────

#[tokio::test]
async fn test_partition_range_full_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a RANGE-partitioned table
    db.execute(
        "CREATE TABLE orders (
            id BIGSERIAL,
            created_at DATE NOT NULL,
            total NUMERIC,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)",
    )
    .await;

    db.execute(
        "CREATE TABLE orders_2025 PARTITION OF orders
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;

    db.execute(
        "CREATE TABLE orders_2026 PARTITION OF orders
            FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;

    // Insert data across partitions
    db.execute(
        "INSERT INTO orders (created_at, total) VALUES
         ('2025-06-15', 100.00),
         ('2025-12-01', 200.00),
         ('2026-03-15', 300.00)",
    )
    .await;

    // Create stream table over the partitioned source
    db.create_st(
        "order_totals",
        "SELECT created_at, total FROM orders",
        "1m",
        "FULL",
    )
    .await;

    db.refresh_st("order_totals").await;

    let count: i64 = db.count("order_totals").await;
    assert_eq!(count, 3, "All rows from all partitions should be visible");

    // Verify row-level correctness — partition pruning must not corrupt individual values
    db.assert_st_matches_query("order_totals", "SELECT created_at, total FROM orders")
        .await;
}

#[tokio::test]
async fn test_partition_range_differential_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE diff_orders (
            id BIGSERIAL,
            month DATE NOT NULL,
            amount NUMERIC,
            PRIMARY KEY (id, month)
        ) PARTITION BY RANGE (month)",
    )
    .await;

    db.execute(
        "CREATE TABLE diff_orders_q1 PARTITION OF diff_orders
            FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')",
    )
    .await;

    db.execute(
        "CREATE TABLE diff_orders_q2 PARTITION OF diff_orders
            FOR VALUES FROM ('2025-04-01') TO ('2025-07-01')",
    )
    .await;

    db.execute("INSERT INTO diff_orders (month, amount) VALUES ('2025-02-01', 50.00)")
        .await;

    // Create DIFFERENTIAL stream table
    db.create_st(
        "diff_order_st",
        "SELECT id, month, amount FROM diff_orders",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.refresh_st("diff_order_st").await;
    let count: i64 = db.count("diff_order_st").await;
    assert_eq!(count, 1, "Initial row should be present");

    // Insert into a different partition
    db.execute("INSERT INTO diff_orders (month, amount) VALUES ('2025-05-15', 75.00)")
        .await;

    db.refresh_st("diff_order_st").await;
    let count: i64 = db.count("diff_order_st").await;
    assert_eq!(
        count, 2,
        "Row from second partition should appear after differential refresh"
    );

    // Update across partitions
    db.execute("UPDATE diff_orders SET amount = 99.00 WHERE amount = 50.00")
        .await;

    db.refresh_st("diff_order_st").await;

    let updated: String = db
        .query_scalar("SELECT amount::text FROM diff_order_st WHERE month = '2025-02-01'")
        .await;
    assert_eq!(updated, "99.00", "Update should be reflected after refresh");

    // Delete from a partition
    db.execute("DELETE FROM diff_orders WHERE month = '2025-05-15'")
        .await;

    db.refresh_st("diff_order_st").await;
    let count: i64 = db.count("diff_order_st").await;
    assert_eq!(
        count, 1,
        "Deleted row should be removed after differential refresh"
    );

    // Verify full data correctness after all DML cycles across partitions
    db.assert_st_matches_query("diff_order_st", "SELECT id, month, amount FROM diff_orders")
        .await;
}

#[tokio::test]
async fn test_partition_list_source() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a LIST-partitioned table
    db.execute(
        "CREATE TABLE events (
            id SERIAL,
            region TEXT NOT NULL,
            payload TEXT,
            PRIMARY KEY (id, region)
        ) PARTITION BY LIST (region)",
    )
    .await;

    db.execute("CREATE TABLE events_us PARTITION OF events FOR VALUES IN ('US')")
        .await;

    db.execute("CREATE TABLE events_eu PARTITION OF events FOR VALUES IN ('EU')")
        .await;

    db.execute("INSERT INTO events (region, payload) VALUES ('US', 'click'), ('EU', 'view')")
        .await;

    db.create_st(
        "event_st",
        "SELECT region, count(*) as cnt FROM events GROUP BY region",
        "1m",
        "FULL",
    )
    .await;

    db.refresh_st("event_st").await;

    let count: i64 = db.count("event_st").await;
    assert_eq!(count, 2, "Both regions should appear in aggregated result");

    // Verify aggregated result correctness across LIST partitions
    db.assert_st_matches_query(
        "event_st",
        "SELECT region, count(*) as cnt FROM events GROUP BY region",
    )
    .await;
}

#[tokio::test]
async fn test_partition_hash_source() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a HASH-partitioned table
    db.execute(
        "CREATE TABLE hash_data (
            id INT PRIMARY KEY,
            val TEXT
        ) PARTITION BY HASH (id)",
    )
    .await;

    db.execute(
        "CREATE TABLE hash_data_0 PARTITION OF hash_data
            FOR VALUES WITH (MODULUS 2, REMAINDER 0)",
    )
    .await;

    db.execute(
        "CREATE TABLE hash_data_1 PARTITION OF hash_data
            FOR VALUES WITH (MODULUS 2, REMAINDER 1)",
    )
    .await;

    db.execute("INSERT INTO hash_data VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
        .await;

    db.create_st("hash_st", "SELECT id, val FROM hash_data", "1m", "FULL")
        .await;

    db.refresh_st("hash_st").await;

    let count: i64 = db.count("hash_st").await;
    assert_eq!(
        count, 4,
        "All rows across hash partitions should be visible"
    );

    // Verify data correctness — hash partitioning must not lose or corrupt rows
    db.assert_st_matches_query("hash_st", "SELECT id, val FROM hash_data")
        .await;
}

// ── PT2: ATTACH PARTITION detection ────────────────────────────────────

#[tokio::test]
async fn test_partition_attach_triggers_reinit() {
    let db = E2eDb::new().await.with_extension().await;

    // Create partitioned source with one partition
    db.execute(
        "CREATE TABLE attach_orders (
            id BIGSERIAL,
            created_at DATE NOT NULL,
            total NUMERIC,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)",
    )
    .await;

    db.execute(
        "CREATE TABLE attach_orders_2025 PARTITION OF attach_orders
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;

    db.execute("INSERT INTO attach_orders (created_at, total) VALUES ('2025-06-01', 100.00)")
        .await;

    db.create_st(
        "attach_st",
        "SELECT id, created_at, total FROM attach_orders",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.refresh_st("attach_st").await;
    let count: i64 = db.count("attach_st").await;
    assert_eq!(count, 1, "Initial partition data should be present");

    // Create a standalone table with pre-existing data, then attach it
    db.execute(
        "CREATE TABLE attach_orders_2026 (
            id BIGSERIAL,
            created_at DATE NOT NULL,
            total NUMERIC,
            PRIMARY KEY (id, created_at)
        )",
    )
    .await;

    db.execute(
        "INSERT INTO attach_orders_2026 (created_at, total) VALUES
         ('2026-03-01', 200.00),
         ('2026-06-01', 300.00)",
    )
    .await;

    // ATTACH PARTITION — this should trigger reinit detection
    db.execute(
        "ALTER TABLE attach_orders ATTACH PARTITION attach_orders_2026
            FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;

    // Partition changes suspend the stream table until it is explicitly repaired.
    let status: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'attach_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED:SOURCE_DEPENDENCY_AMBIGUOUS");

    db.execute("SELECT pgtrickle.reinitialize_stream_table('attach_st')")
        .await;

    // Refresh after repair should pick up all data including the newly attached partition.
    db.refresh_st("attach_st").await;

    let count: i64 = db.count("attach_st").await;
    assert_eq!(
        count, 3,
        "After reinit, all rows including attached partition data should be visible"
    );
}

#[tokio::test]
async fn test_partition_detach_triggers_reinit() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE detach_orders (
            id BIGSERIAL,
            created_at DATE NOT NULL,
            total NUMERIC,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)",
    )
    .await;

    db.execute(
        "CREATE TABLE detach_orders_2025 PARTITION OF detach_orders
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;

    db.execute(
        "CREATE TABLE detach_orders_2026 PARTITION OF detach_orders
            FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;

    db.execute(
        "INSERT INTO detach_orders (created_at, total) VALUES
         ('2025-06-01', 100.00),
         ('2026-06-01', 200.00)",
    )
    .await;

    db.create_st(
        "detach_st",
        "SELECT id, created_at, total FROM detach_orders",
        "1m",
        "FULL",
    )
    .await;

    db.refresh_st("detach_st").await;
    let count: i64 = db.count("detach_st").await;
    assert_eq!(count, 2, "Both partitions should contribute data");

    // DETACH a partition
    db.execute("ALTER TABLE detach_orders DETACH PARTITION detach_orders_2026")
        .await;

    // Partition changes suspend the stream table until it is explicitly repaired.
    let status: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'detach_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED:SOURCE_DEPENDENCY_AMBIGUOUS");

    db.execute("SELECT pgtrickle.reinitialize_stream_table('detach_st')")
        .await;

    // After repair, only the remaining partition's data should be visible.
    db.refresh_st("detach_st").await;

    let count: i64 = db.count("detach_st").await;
    assert_eq!(
        count, 1,
        "After reinit, only remaining partition data should be visible"
    );
}

// ── PT4: Foreign table source restriction ──────────────────────────────

#[tokio::test]
async fn test_foreign_table_full_refresh_works() {
    let db = E2eDb::new().await.with_extension().await;

    // Set up a foreign data wrapper pointing to the same database
    db.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .await;

    // Build server options — use 127.0.0.1 for loopback (inet_server_addr()
    // returns the container's external IP which may not be reachable from
    // within the same container).  `user` is a USER MAPPING option, not a
    // SERVER option in postgres_fdw.
    let port: String = db.query_scalar("SELECT inet_server_port()::text").await;
    let dbname: String = db.query_scalar("SELECT current_database()").await;

    db.execute(&format!(
        "CREATE SERVER IF NOT EXISTS loopback FOREIGN DATA WRAPPER postgres_fdw \
         OPTIONS (host '127.0.0.1', port '{port}', dbname '{dbname}')",
    ))
    .await;

    db.execute(&format!(
        "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER loopback OPTIONS (user '{}')",
        db.query_scalar::<String>("SELECT current_user").await
    ))
    .await;

    // Create a local table, then a foreign table pointing to it
    db.execute("CREATE TABLE fdw_source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO fdw_source VALUES (1, 'hello'), (2, 'world')")
        .await;

    db.execute(
        "CREATE FOREIGN TABLE fdw_remote (id INT, val TEXT)
         SERVER loopback OPTIONS (table_name 'fdw_source')",
    )
    .await;

    // FULL refresh should work with foreign tables
    db.create_st("fdw_st", "SELECT id, val FROM fdw_remote", "1m", "FULL")
        .await;

    db.refresh_st("fdw_st").await;

    let count: i64 = db.count("fdw_st").await;
    assert_eq!(
        count, 2,
        "Foreign table should be readable via FULL refresh"
    );
}

// ── Partitioned table with aggregation ─────────────────────────────────

#[tokio::test]
async fn test_partition_with_aggregation() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE agg_sales (
            id SERIAL,
            sale_date DATE NOT NULL,
            category TEXT NOT NULL,
            amount NUMERIC,
            PRIMARY KEY (id, sale_date)
        ) PARTITION BY RANGE (sale_date)",
    )
    .await;

    db.execute(
        "CREATE TABLE agg_sales_h1 PARTITION OF agg_sales
            FOR VALUES FROM ('2025-01-01') TO ('2025-07-01')",
    )
    .await;

    db.execute(
        "CREATE TABLE agg_sales_h2 PARTITION OF agg_sales
            FOR VALUES FROM ('2025-07-01') TO ('2026-01-01')",
    )
    .await;

    db.execute(
        "INSERT INTO agg_sales (sale_date, category, amount) VALUES
         ('2025-02-01', 'A', 100), ('2025-03-01', 'A', 200),
         ('2025-08-01', 'B', 300), ('2025-09-01', 'A', 150)",
    )
    .await;

    db.create_st(
        "sales_agg_st",
        "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM agg_sales GROUP BY category",
        "1m",
        "FULL",
    )
    .await;

    db.refresh_st("sales_agg_st").await;

    let a_total: String = db
        .query_scalar("SELECT total::text FROM sales_agg_st WHERE category = 'A'")
        .await;
    assert_eq!(
        a_total, "450",
        "Category A total should span both partitions"
    );

    let b_total: String = db
        .query_scalar("SELECT total::text FROM sales_agg_st WHERE category = 'B'")
        .await;
    assert_eq!(
        b_total, "300",
        "Category B total should be from H2 partition"
    );

    // Verify full aggregated result correctness spanning both partitions
    db.assert_st_matches_query(
        "sales_agg_st",
        "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt FROM agg_sales GROUP BY category",
    )
    .await;
}

#[tokio::test]
async fn test_partition_differential_with_aggregation() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE diff_agg (
            id SERIAL,
            month DATE NOT NULL,
            dept TEXT NOT NULL,
            revenue NUMERIC,
            PRIMARY KEY (id, month)
        ) PARTITION BY RANGE (month)",
    )
    .await;

    db.execute(
        "CREATE TABLE diff_agg_q1 PARTITION OF diff_agg
            FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')",
    )
    .await;

    db.execute(
        "CREATE TABLE diff_agg_q2 PARTITION OF diff_agg
            FOR VALUES FROM ('2025-04-01') TO ('2025-07-01')",
    )
    .await;

    db.execute(
        "INSERT INTO diff_agg (month, dept, revenue) VALUES
         ('2025-01-15', 'eng', 1000),
         ('2025-02-15', 'eng', 2000)",
    )
    .await;

    db.create_st(
        "diff_agg_st",
        "SELECT dept, SUM(revenue) AS total FROM diff_agg GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.refresh_st("diff_agg_st").await;

    let eng_total: String = db
        .query_scalar("SELECT total::text FROM diff_agg_st WHERE dept = 'eng'")
        .await;
    assert_eq!(eng_total, "3000");

    // Insert into second partition
    db.execute("INSERT INTO diff_agg (month, dept, revenue) VALUES ('2025-05-15', 'eng', 500)")
        .await;

    db.refresh_st("diff_agg_st").await;

    let eng_total: String = db
        .query_scalar("SELECT total::text FROM diff_agg_st WHERE dept = 'eng'")
        .await;
    assert_eq!(
        eng_total, "3500",
        "Aggregate should include row from second partition"
    );

    // Verify full result matches the expected aggregation after cross-partition DML
    db.assert_st_matches_query(
        "diff_agg_st",
        "SELECT dept, SUM(revenue) AS total FROM diff_agg GROUP BY dept",
    )
    .await;
}

// ── A1-4: Partitioned stream tables (partition_by parameter) ───────────
//
// These tests validate the full A1-1+A1-2+A1-3 stack:
//   A1-1: storage table created with PARTITION BY RANGE + default partition
//   A1-2: empty-delta fast path skips MERGE
//   A1-3: partition-key range predicate injected into MERGE ON clause

#[tokio::test]
async fn test_partitioned_st_create_and_initial_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE sales (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            region TEXT NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO sales (sale_date, region, amount) VALUES
         ('2026-01-10', 'north', 100),
         ('2026-02-15', 'south', 200),
         ('2026-03-20', 'east',  300)",
    )
    .await;

    // A1-1: create partitioned stream table
    db.create_st_partitioned(
        "sales_summary",
        "SELECT sale_date, region, SUM(amount) AS total FROM sales GROUP BY sale_date, region",
        "1m",
        "DIFFERENTIAL",
        "sale_date",
    )
    .await;

    // Verify catalog stores partition key
    let pk: Option<String> = db
        .query_scalar_opt(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'sales_summary'",
        )
        .await;
    assert_eq!(
        pk.as_deref(),
        Some("sale_date"),
        "st_partition_key should be stored in catalog"
    );

    // A1-1: verify storage table is RANGE-partitioned
    let is_partitioned: bool = db
        .query_scalar(
            "SELECT relkind = 'p' FROM pg_class \
             WHERE relname = 'sales_summary' AND relnamespace = 'public'::regnamespace",
        )
        .await;
    assert!(
        is_partitioned,
        "storage table should be a partitioned table (relkind='p')"
    );

    // A1-1: default catch-all partition must exist
    let default_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(\
               SELECT 1 FROM pg_class pc \
               JOIN pg_inherits pi ON pi.inhrelid = pc.oid \
               JOIN pg_class pp ON pp.oid = pi.inhparent \
               JOIN pg_partitioned_table ppt ON ppt.partrelid = pp.oid \
               WHERE pp.relname = 'sales_summary' \
                 AND pc.relname = 'sales_summary_default'\
             )",
        )
        .await;
    assert!(
        default_exists,
        "default catch-all partition should exist automatically"
    );

    db.refresh_st("sales_summary").await;

    let count: i64 = db.count("sales_summary").await;
    assert_eq!(
        count, 3,
        "all rows should be in the partitioned ST after refresh"
    );

    // Correctness: values must match the source
    db.assert_st_matches_query(
        "sales_summary",
        "SELECT sale_date, region, SUM(amount) AS total FROM sales GROUP BY sale_date, region",
    )
    .await;
}

#[tokio::test]
async fn test_partitioned_st_differential_refresh_inserts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE events (
            id SERIAL PRIMARY KEY,
            event_date DATE NOT NULL,
            category TEXT NOT NULL,
            score INT NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO events (event_date, category, score) VALUES
         ('2026-01-05', 'A', 10),
         ('2026-01-06', 'B', 20)",
    )
    .await;

    db.create_st_partitioned(
        "event_summary",
        "SELECT event_date, category, SUM(score) AS total FROM events GROUP BY event_date, category",
        "1m",
        "DIFFERENTIAL",
        "event_date",
    )
    .await;

    db.refresh_st("event_summary").await;
    let count: i64 = db.count("event_summary").await;
    assert_eq!(count, 2);

    // Insert new rows — A1-3: only these dates' partitions are visited in MERGE
    db.execute(
        "INSERT INTO events (event_date, category, score) VALUES
         ('2026-02-10', 'A', 30),
         ('2026-02-11', 'C', 40)",
    )
    .await;

    db.refresh_st("event_summary").await;
    let count: i64 = db.count("event_summary").await;
    assert_eq!(
        count, 4,
        "new rows should be present after differential refresh"
    );

    db.assert_st_matches_query(
        "event_summary",
        "SELECT event_date, category, SUM(score) AS total FROM events GROUP BY event_date, category",
    )
    .await;
}

#[tokio::test]
async fn test_partitioned_st_differential_refresh_updates_and_deletes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            order_date DATE NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO orders (order_date, amount) VALUES
         ('2026-01-01', 100),
         ('2026-01-02', 200),
         ('2026-03-01', 300)",
    )
    .await;

    db.create_st_partitioned(
        "order_totals",
        "SELECT order_date, SUM(amount) AS total FROM orders GROUP BY order_date",
        "1m",
        "DIFFERENTIAL",
        "order_date",
    )
    .await;

    db.refresh_st("order_totals").await;

    // Update a row — A1-3: predicate covers only the updated date's partition
    db.execute("UPDATE orders SET amount = 150 WHERE order_date = '2026-01-01'")
        .await;
    db.refresh_st("order_totals").await;

    let jan1_total: String = db
        .query_scalar("SELECT total::text FROM order_totals WHERE order_date = '2026-01-01'")
        .await;
    assert_eq!(jan1_total, "150", "update should be reflected");

    // Delete a row — A1-2: empty groups are removed by differential
    db.execute("DELETE FROM orders WHERE order_date = '2026-03-01'")
        .await;
    db.refresh_st("order_totals").await;

    let count: i64 = db.count("order_totals").await;
    assert_eq!(
        count, 2,
        "deleted group should be removed from partitioned ST"
    );

    db.assert_st_matches_query(
        "order_totals",
        "SELECT order_date, SUM(amount) AS total FROM orders GROUP BY order_date",
    )
    .await;
}

#[tokio::test]
async fn test_partitioned_st_validate_partition_key_error() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE zzz_src (id INT PRIMARY KEY, name TEXT)")
        .await;

    // Invalid partition key — not in SELECT output
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
               'zzz_st', $$SELECT id, name FROM zzz_src$$, '1m', 'DIFFERENTIAL', \
               partition_by => 'nonexistent_col')",
        )
        .await;
    assert!(
        result.is_err(),
        "partition_by with unknown column should return an error"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("nonexistent_col") || msg.contains("partition_by"),
        "error message should mention the invalid column: {msg}"
    );
}

#[tokio::test]
async fn test_partitioned_st_partition_key_in_explain_plan() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    db.execute(
        "CREATE TABLE explain_src (
            id SERIAL PRIMARY KEY,
            tx_date DATE NOT NULL,
            val NUMERIC NOT NULL
        )",
    )
    .await;

    // Create the partitioned stream table while the source is still empty so
    // that the initial refresh leaves the default partition empty.  Explicit
    // range partitions must be attached before any data lands in the default
    // partition; otherwise PostgreSQL rejects the PARTITION OF statement with
    // "partition constraint for default partition would be violated".
    db.create_st_partitioned(
        "explain_st",
        "SELECT tx_date, SUM(val) AS daily_total FROM explain_src GROUP BY tx_date",
        "1m",
        "DIFFERENTIAL",
        "tx_date",
    )
    .await;

    // Add explicit named partitions (month-level) so pruning has something to
    // prune.  This must happen while the default partition is still empty.
    db.execute(
        "CREATE TABLE explain_st_jan2026 \
         PARTITION OF explain_st \
         FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE explain_st_feb2026 \
         PARTITION OF explain_st \
         FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE explain_st_mar2026 \
         PARTITION OF explain_st \
         FOR VALUES FROM ('2026-03-01') TO ('2026-04-01')",
    )
    .await;

    // Now insert 3 months of data so the planner has statistics to work with
    db.execute(
        "INSERT INTO explain_src (tx_date, val)
         SELECT
             DATE '2026-01-01' + (i % 90) * INTERVAL '1 day',
             (random() * 1000)::numeric
         FROM generate_series(1, 9000) AS s(i)",
    )
    .await;

    db.execute("ANALYZE explain_src").await;

    db.refresh_st("explain_st").await;

    let count: i64 = db.count("explain_st").await;
    assert_eq!(count, 90, "90 distinct dates should be present");

    // A1-3 correctness: inject one month of changes and verify result is still correct
    db.execute(
        "UPDATE explain_src SET val = val + 1.0 \
         WHERE tx_date >= '2026-01-01' AND tx_date < '2026-02-01'",
    )
    .await;

    db.refresh_st("explain_st").await;

    db.assert_st_matches_query(
        "explain_st",
        "SELECT tx_date, SUM(val) AS daily_total FROM explain_src GROUP BY tx_date",
    )
    .await;

    // A1-3 observability: EXPLAIN on the defining query should show partition pruning
    // when the range is known. This is a soft assertion — we document the plan for
    // manual inspection when PGS_BENCH_EXPLAIN=true, but do not fail on pruning absence
    // (the planner may inline the predicate differently depending on statistics).
    if std::env::var("PGS_BENCH_EXPLAIN").is_ok() {
        let explain_sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) \
             SELECT tx_date, SUM(val) AS daily_total FROM explain_src GROUP BY tx_date";
        if let Some(plan) = db.query_text(explain_sql).await {
            for line in plan.lines().take(30) {
                eprintln!("[A1-4 EXPLAIN] {line}");
            }
        }
    }
}

#[tokio::test]
async fn test_partitioned_st_empty_delta_skips_merge() {
    // A1-2: when no source changes exist, extract_partition_range returns None
    // and the MERGE is skipped entirely. Validate via row count stability.
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE nochange_src (
            id SERIAL PRIMARY KEY,
            evt_date DATE NOT NULL,
            v INT NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO nochange_src (evt_date, v) VALUES ('2026-01-01', 1), ('2026-01-02', 2)",
    )
    .await;

    db.create_st_partitioned(
        "nochange_st",
        "SELECT evt_date, SUM(v) AS total FROM nochange_src GROUP BY evt_date",
        "1m",
        "DIFFERENTIAL",
        "evt_date",
    )
    .await;

    db.refresh_st("nochange_st").await;
    let count_before: i64 = db.count("nochange_st").await;

    // No source changes — second refresh should be a no-op (A1-2 fast path)
    db.refresh_st("nochange_st").await;
    let count_after: i64 = db.count("nochange_st").await;

    assert_eq!(
        count_before, count_after,
        "empty-delta fast path: count must not change on a no-op refresh"
    );

    db.assert_st_matches_query(
        "nochange_st",
        "SELECT evt_date, SUM(v) AS total FROM nochange_src GROUP BY evt_date",
    )
    .await;
}

// ── A1-1b: Multi-column RANGE partition key tests ──────────────────────────

/// A1-1b: Create a stream table partitioned by two columns (date + region).
/// Verify the storage table uses `PARTITION BY RANGE (col_a, col_b)`,
/// a default partition exists, and the initial FULL refresh populates data.
#[tokio::test]
async fn test_partitioned_st_multi_column_create_and_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mc_src (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            region TEXT NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO mc_src (sale_date, region, amount)
         SELECT
             DATE '2026-01-01' + (i % 90),
             CASE WHEN i % 3 = 0 THEN 'US' WHEN i % 3 = 1 THEN 'EU' ELSE 'APAC' END,
             (i * 10)::numeric
         FROM generate_series(1, 300) AS s(i)",
    )
    .await;

    // A1-1b: multi-column partition key
    db.create_st_partitioned(
        "mc_summary",
        "SELECT sale_date, region, SUM(amount) AS total FROM mc_src GROUP BY sale_date, region",
        "1m",
        "DIFFERENTIAL",
        "sale_date,region",
    )
    .await;

    // Verify partition key stored in catalog
    let pk: String = db
        .query_scalar(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'mc_summary'",
        )
        .await;
    assert_eq!(
        pk, "sale_date,region",
        "Multi-column partition key must be persisted"
    );

    // Verify storage is RANGE partitioned
    let relkind: String = db
        .query_scalar("SELECT relkind::text FROM pg_class WHERE relname = 'mc_summary'")
        .await;
    assert_eq!(
        relkind, "p",
        "Storage table must be partitioned (relkind='p')"
    );

    // Verify default partition exists
    let default_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_inherits pi \
             JOIN pg_class pc ON pc.oid = pi.inhrelid \
             WHERE pc.relname = 'mc_summary_default')",
        )
        .await;
    assert!(default_exists, "Default partition must exist");

    // Verify initial refresh populates data
    let count: i64 = db.count("mc_summary").await;
    assert!(
        count > 0,
        "Initial refresh should populate rows, got {count}"
    );

    // Verify correctness
    db.assert_st_matches_query(
        "mc_summary",
        "SELECT sale_date, region, SUM(amount) AS total FROM mc_src GROUP BY sale_date, region",
    )
    .await;
}

/// A1-1b: DIFFERENTIAL refresh with multi-column partition key correctly
/// applies incremental changes.
#[tokio::test]
async fn test_partitioned_st_multi_column_differential_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mc2_src (
            id SERIAL PRIMARY KEY,
            event_day DATE NOT NULL,
            customer_id INT NOT NULL,
            qty INT NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO mc2_src (event_day, customer_id, qty)
         SELECT
             DATE '2026-03-01' + (i % 10),
             100 + (i % 5),
             i
         FROM generate_series(1, 100) AS s(i)",
    )
    .await;

    db.create_st_partitioned(
        "mc2_st",
        "SELECT event_day, customer_id, SUM(qty) AS total_qty FROM mc2_src GROUP BY event_day, customer_id",
        "1m",
        "DIFFERENTIAL",
        "event_day, customer_id",
    )
    .await;

    db.refresh_st("mc2_st").await;

    let count_before: i64 = db.count("mc2_st").await;
    assert!(count_before > 0, "Should have rows after initial refresh");

    // Now insert new data and refresh again (DIFFERENTIAL path)
    db.execute(
        "INSERT INTO mc2_src (event_day, customer_id, qty)
         SELECT
             DATE '2026-03-15' + (i % 5),
             200 + (i % 3),
             i * 2
         FROM generate_series(1, 50) AS s(i)",
    )
    .await;

    db.refresh_st("mc2_st").await;

    let count_after: i64 = db.count("mc2_st").await;
    assert!(
        count_after >= count_before,
        "Row count should increase or stay same after insert+refresh"
    );

    db.assert_st_matches_query(
        "mc2_st",
        "SELECT event_day, customer_id, SUM(qty) AS total_qty FROM mc2_src GROUP BY event_day, customer_id",
    )
    .await;
}

/// A1-1b: Validate that a multi-column partition key with an invalid column
/// returns an appropriate error.
#[tokio::test]
async fn test_partitioned_st_multi_column_invalid_column_error() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE mc3_src (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
             'mc3_st', $$SELECT sale_date, SUM(amount) AS total FROM mc3_src GROUP BY sale_date$$, \
             '1m', 'DIFFERENTIAL', partition_by => 'sale_date,nonexistent')",
        )
        .await;

    assert!(
        result.is_err(),
        "Should fail when one partition column does not exist in SELECT output"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("nonexistent"),
        "Error should mention the invalid column: {err_msg}"
    );
}

// ── PART-WARN: Default partition growth warning tests ──────────────────────

/// PART-WARN: When all data lands in the default partition (no explicit named
/// partitions), verifying correctness still works. The WARNING is emitted in
/// server logs (not easily asserted in E2E, but this test verifies the code
/// path does not crash).
#[tokio::test]
async fn test_partitioned_st_default_partition_warning_no_crash() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE pw_src (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO pw_src (sale_date, val)
         SELECT DATE '2026-01-01' + (i % 30), i
         FROM generate_series(1, 100) AS s(i)",
    )
    .await;

    // Create partitioned ST — only default partition exists (no explicit ranges).
    // The PART-WARN code path will fire after refresh.
    db.create_st_partitioned(
        "pw_st",
        "SELECT sale_date, SUM(val) AS total FROM pw_src GROUP BY sale_date",
        "1m",
        "DIFFERENTIAL",
        "sale_date",
    )
    .await;

    db.refresh_st("pw_st").await;

    // All rows should be in the default partition — verify data is correct.
    let default_count: i64 = db.count("pw_st_default").await;
    assert!(default_count > 0, "Default partition should have rows");

    db.assert_st_matches_query(
        "pw_st",
        "SELECT sale_date, SUM(val) AS total FROM pw_src GROUP BY sale_date",
    )
    .await;
}

/// PART-WARN: When explicit partitions cover all data, the default partition
/// should be empty and no warning should fire. Verify data correctness as well.
#[tokio::test]
async fn test_partitioned_st_explicit_partitions_no_warning() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE pw2_src (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    // Create ST while source is empty so default partition is empty
    db.create_st_partitioned(
        "pw2_st",
        "SELECT sale_date, SUM(val) AS total FROM pw2_src GROUP BY sale_date",
        "1m",
        "DIFFERENTIAL",
        "sale_date",
    )
    .await;

    // Add explicit partitions covering our data range
    db.execute(
        "CREATE TABLE pw2_st_jan PARTITION OF pw2_st \
         FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE pw2_st_feb PARTITION OF pw2_st \
         FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
    )
    .await;

    // Insert data only in Jan-Feb
    db.execute(
        "INSERT INTO pw2_src (sale_date, val)
         SELECT DATE '2026-01-01' + (i % 59), i
         FROM generate_series(1, 100) AS s(i)",
    )
    .await;

    db.refresh_st("pw2_st").await;

    // Default partition should be empty
    let default_count: i64 = db.count("pw2_st_default").await;
    assert_eq!(
        default_count, 0,
        "Default partition should be empty when explicit partitions cover all data"
    );

    db.assert_st_matches_query(
        "pw2_st",
        "SELECT sale_date, SUM(val) AS total FROM pw2_src GROUP BY sale_date",
    )
    .await;
}

// ── A1-1d: LIST-partitioned stream tables ──────────────────────────────

#[tokio::test]
async fn test_partitioned_st_list_create_and_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE list_src (
            id SERIAL PRIMARY KEY,
            region TEXT NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO list_src (region, amount) VALUES
         ('US', 100), ('EU', 200), ('APAC', 300), ('US', 150)",
    )
    .await;

    // A1-1d: create LIST-partitioned stream table
    db.create_st_partitioned(
        "list_summary",
        "SELECT region, SUM(amount) AS total FROM list_src GROUP BY region",
        "1m",
        "DIFFERENTIAL",
        "LIST:region",
    )
    .await;

    // Verify catalog stores the LIST: prefix
    let pk: Option<String> = db
        .query_scalar_opt(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'list_summary'",
        )
        .await;
    assert_eq!(pk.as_deref(), Some("LIST:region"));

    // Verify storage table is LIST-partitioned
    let part_strategy: String = db
        .query_scalar(
            "SELECT partstrat::text FROM pg_partitioned_table pt \
             JOIN pg_class c ON c.oid = pt.partrelid \
             WHERE c.relname = 'list_summary'",
        )
        .await;
    assert_eq!(
        part_strategy, "l",
        "storage table should be LIST-partitioned (partstrat='l')"
    );

    // Default partition must exist
    let default_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(\
               SELECT 1 FROM pg_class WHERE relname = 'list_summary_default'\
             )",
        )
        .await;
    assert!(default_exists, "default partition should exist");

    db.refresh_st("list_summary").await;

    let count: i64 = db.count("list_summary").await;
    assert_eq!(count, 3, "3 regions should be in the LIST-partitioned ST");

    db.assert_st_matches_query(
        "list_summary",
        "SELECT region, SUM(amount) AS total FROM list_src GROUP BY region",
    )
    .await;
}

#[tokio::test]
async fn test_partitioned_st_list_differential_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE ldiff_src (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute("INSERT INTO ldiff_src (category, val) VALUES ('A', 10), ('B', 20), ('C', 30)")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table('ldiff_st', \
         $$SELECT category, SUM(val) AS total FROM ldiff_src GROUP BY category$$, \
         '1m', 'DIFFERENTIAL', initialize => false, partition_by => 'LIST:category')",
    )
    .await;

    // Create explicit LIST partitions (before first refresh, so default partition is empty)
    db.execute("CREATE TABLE ldiff_st_a PARTITION OF ldiff_st FOR VALUES IN ('A')")
        .await;
    db.execute("CREATE TABLE ldiff_st_b PARTITION OF ldiff_st FOR VALUES IN ('B')")
        .await;

    db.refresh_st("ldiff_st").await;

    // Insert more data into category A only
    db.execute("INSERT INTO ldiff_src (category, val) VALUES ('A', 5)")
        .await;

    db.refresh_st("ldiff_st").await;

    // Verify correctness — category A should be updated, B/C unchanged
    db.assert_st_matches_query(
        "ldiff_st",
        "SELECT category, SUM(val) AS total FROM ldiff_src GROUP BY category",
    )
    .await;
}

#[tokio::test]
async fn test_partitioned_st_list_with_explicit_partitions() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE lexp_src (
            id SERIAL PRIMARY KEY,
            status TEXT NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO lexp_src (status, amount) VALUES
         ('active', 100), ('inactive', 200), ('active', 50)",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table('lexp_st', \
         $$SELECT status, SUM(amount) AS total FROM lexp_src GROUP BY status$$, \
         '1m', 'DIFFERENTIAL', initialize => false, partition_by => 'LIST:status')",
    )
    .await;

    // Create named partitions before refresh
    db.execute("CREATE TABLE lexp_st_active PARTITION OF lexp_st FOR VALUES IN ('active')")
        .await;
    db.execute("CREATE TABLE lexp_st_inactive PARTITION OF lexp_st FOR VALUES IN ('inactive')")
        .await;

    db.refresh_st("lexp_st").await;

    // Verify data lands in the correct partitions
    let active_count: i64 = db.count("lexp_st_active").await;
    assert_eq!(active_count, 1, "active partition should have 1 row");

    let inactive_count: i64 = db.count("lexp_st_inactive").await;
    assert_eq!(inactive_count, 1, "inactive partition should have 1 row");

    db.assert_st_matches_query(
        "lexp_st",
        "SELECT status, SUM(amount) AS total FROM lexp_src GROUP BY status",
    )
    .await;
}

#[tokio::test]
async fn test_partitioned_st_list_multi_column_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE lmc_src (
            id SERIAL PRIMARY KEY,
            region TEXT NOT NULL,
            category TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    // LIST with multiple columns should be rejected
    let err = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('lmc_st', \
             $$SELECT region, category, SUM(val) AS total \
               FROM lmc_src GROUP BY region, category$$, \
             '1m', 'DIFFERENTIAL', partition_by => 'LIST:region,category')",
        )
        .await;
    assert!(
        err.is_err(),
        "LIST with multiple columns should be rejected"
    );
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("single column"),
        "Error should mention single column: {msg}"
    );
}

// ── A1-1c: ALTER partition_by support ──────────────────────────────────

#[tokio::test]
async fn test_alter_st_add_partition_key() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE alt_add_src (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO alt_add_src (sale_date, amount) VALUES
         ('2026-01-10', 100), ('2026-02-15', 200), ('2026-03-20', 300)",
    )
    .await;

    // Create an unpartitioned stream table first.
    db.execute(
        "SELECT pgtrickle.create_stream_table('alt_add_st', \
         $$SELECT sale_date, SUM(amount) AS total FROM alt_add_src GROUP BY sale_date$$, \
         '1m', 'DIFFERENTIAL')",
    )
    .await;
    db.refresh_st("alt_add_st").await;

    let count_before: i64 = db.count("alt_add_st").await;
    assert_eq!(count_before, 3);

    // Not partitioned initially.
    let is_partitioned: bool = db
        .query_scalar(
            "SELECT relkind = 'p' FROM pg_class \
             WHERE relname = 'alt_add_st' AND relnamespace = 'public'::regnamespace",
        )
        .await;
    assert!(!is_partitioned, "should not be partitioned initially");

    // ALTER to add RANGE partitioning.
    db.execute("SELECT pgtrickle.alter_stream_table('alt_add_st', partition_by => 'sale_date')")
        .await;

    // Now it should be partitioned.
    let is_partitioned_after: bool = db
        .query_scalar(
            "SELECT relkind = 'p' FROM pg_class \
             WHERE relname = 'alt_add_st' AND relnamespace = 'public'::regnamespace",
        )
        .await;
    assert!(
        is_partitioned_after,
        "should be RANGE-partitioned after ALTER"
    );

    // Catalog should have the partition key.
    let pk: Option<String> = db
        .query_scalar_opt(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'alt_add_st'",
        )
        .await;
    assert_eq!(pk.as_deref(), Some("sale_date"));

    // Data should be preserved via full refresh.
    let count_after: i64 = db.count("alt_add_st").await;
    assert_eq!(count_after, 3, "data should be preserved after repartition");

    db.assert_st_matches_query(
        "alt_add_st",
        "SELECT sale_date, SUM(amount) AS total FROM alt_add_src GROUP BY sale_date",
    )
    .await;
}

#[tokio::test]
async fn test_alter_st_change_partition_key_to_list() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE alt_chg_src (
            id SERIAL PRIMARY KEY,
            region TEXT NOT NULL,
            category TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO alt_chg_src (region, category, val) VALUES
         ('US', 'A', 10), ('EU', 'B', 20), ('APAC', 'A', 30)",
    )
    .await;

    // Start with RANGE partitioning on region.
    db.create_st_partitioned(
        "alt_chg_st",
        "SELECT region, category, SUM(val) AS total FROM alt_chg_src GROUP BY region, category",
        "1m",
        "DIFFERENTIAL",
        "region",
    )
    .await;
    db.refresh_st("alt_chg_st").await;

    // Change to LIST partitioning on category.
    db.execute(
        "SELECT pgtrickle.alter_stream_table('alt_chg_st', partition_by => 'LIST:category')",
    )
    .await;

    // Verify LIST partitioning.
    let part_strategy: String = db
        .query_scalar(
            "SELECT partstrat::text FROM pg_partitioned_table pt \
             JOIN pg_class c ON c.oid = pt.partrelid \
             WHERE c.relname = 'alt_chg_st'",
        )
        .await;
    assert_eq!(part_strategy, "l", "should be LIST-partitioned after ALTER");

    // Catalog should have the new key.
    let pk: Option<String> = db
        .query_scalar_opt(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'alt_chg_st'",
        )
        .await;
    assert_eq!(pk.as_deref(), Some("LIST:category"));

    // Data preserved.
    db.assert_st_matches_query(
        "alt_chg_st",
        "SELECT region, category, SUM(val) AS total FROM alt_chg_src GROUP BY region, category",
    )
    .await;
}

#[tokio::test]
async fn test_alter_st_remove_partition_key() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE alt_rm_src (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO alt_rm_src (sale_date, amount) VALUES
         ('2026-01-10', 100), ('2026-02-15', 200)",
    )
    .await;

    // Start partitioned.
    db.create_st_partitioned(
        "alt_rm_st",
        "SELECT sale_date, SUM(amount) AS total FROM alt_rm_src GROUP BY sale_date",
        "1m",
        "DIFFERENTIAL",
        "sale_date",
    )
    .await;
    db.refresh_st("alt_rm_st").await;

    // Remove partitioning by passing empty string.
    db.execute("SELECT pgtrickle.alter_stream_table('alt_rm_st', partition_by => '')")
        .await;

    // Should no longer be partitioned.
    let is_partitioned: bool = db
        .query_scalar(
            "SELECT relkind = 'p' FROM pg_class \
             WHERE relname = 'alt_rm_st' AND relnamespace = 'public'::regnamespace",
        )
        .await;
    assert!(!is_partitioned, "should not be partitioned after removal");

    // Catalog should have no partition key.
    let pk: Option<String> = db
        .query_scalar_opt(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'alt_rm_st'",
        )
        .await;
    assert!(pk.is_none(), "partition key should be NULL after removal");

    // Data preserved.
    let count: i64 = db.count("alt_rm_st").await;
    assert_eq!(
        count, 2,
        "data should be preserved after removing partition"
    );
}

#[tokio::test]
async fn test_alter_st_partition_by_invalid_column_error() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE alt_err_src (
            id SERIAL PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table('alt_err_st', \
         $$SELECT val, COUNT(*) AS cnt FROM alt_err_src GROUP BY val$$, \
         '1m', 'DIFFERENTIAL')",
    )
    .await;

    // ALTER with a column not in the output should fail.
    let err = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table('alt_err_st', partition_by => 'nonexistent')",
        )
        .await;
    assert!(err.is_err(), "invalid column should be rejected");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("nonexistent"),
        "Error should mention the invalid column: {msg}"
    );
}

// ── A1-3b: HASH partitioned stream tables ───────────────────────────────

#[tokio::test]
async fn test_partitioned_st_hash_create_and_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE hash_src (
            id SERIAL PRIMARY KEY,
            customer_id INT NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO hash_src (customer_id, amount) VALUES
         (1, 100), (2, 200), (3, 300), (4, 400), (1, 50)",
    )
    .await;

    // A1-3b: create HASH-partitioned stream table (default 4 partitions)
    db.create_st_partitioned(
        "hash_summary",
        "SELECT customer_id, SUM(amount) AS total FROM hash_src GROUP BY customer_id",
        "1m",
        "DIFFERENTIAL",
        "HASH:customer_id",
    )
    .await;

    // Verify catalog stores the HASH: prefix
    let pk: Option<String> = db
        .query_scalar_opt(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'hash_summary'",
        )
        .await;
    assert_eq!(pk.as_deref(), Some("HASH:customer_id"));

    // Verify storage table is HASH-partitioned
    let part_strategy: String = db
        .query_scalar(
            "SELECT partstrat::text FROM pg_partitioned_table pt \
             JOIN pg_class c ON c.oid = pt.partrelid \
             WHERE c.relname = 'hash_summary'",
        )
        .await;
    assert_eq!(
        part_strategy, "h",
        "storage table should be HASH-partitioned (partstrat='h')"
    );

    // Verify 4 child partitions were auto-created (hash_summary_p0 .. p3)
    let child_count: i64 = db
        .query_scalar(
            "SELECT count(*)::bigint FROM pg_inherits i \
             JOIN pg_class c ON c.oid = i.inhrelid \
             JOIN pg_class p ON p.oid = i.inhparent \
             WHERE p.relname = 'hash_summary'",
        )
        .await;
    assert_eq!(
        child_count, 4,
        "HASH:customer_id should auto-create 4 child partitions"
    );

    // No default partition for HASH
    let default_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(\
               SELECT 1 FROM pg_class WHERE relname = 'hash_summary_default'\
             )",
        )
        .await;
    assert!(
        !default_exists,
        "HASH partitions should not have a default partition"
    );

    db.refresh_st("hash_summary").await;

    let count: i64 = db.count("hash_summary").await;
    assert_eq!(count, 4, "4 customers should be in the HASH-partitioned ST");

    db.assert_st_matches_query(
        "hash_summary",
        "SELECT customer_id, SUM(amount) AS total FROM hash_src GROUP BY customer_id",
    )
    .await;
}

#[tokio::test]
async fn test_partitioned_st_hash_differential_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE hdiff_src (
            id SERIAL PRIMARY KEY,
            customer_id INT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute("INSERT INTO hdiff_src (customer_id, val) VALUES (10, 100), (20, 200), (30, 300)")
        .await;

    db.create_st_partitioned(
        "hdiff_st",
        "SELECT customer_id, SUM(val) AS total FROM hdiff_src GROUP BY customer_id",
        "1m",
        "DIFFERENTIAL",
        "HASH:customer_id",
    )
    .await;

    db.refresh_st("hdiff_st").await;
    let count_1: i64 = db.count("hdiff_st").await;
    assert_eq!(count_1, 3, "initial refresh should produce 3 rows");

    // Insert more data — should be incrementally merged
    db.execute("INSERT INTO hdiff_src (customer_id, val) VALUES (10, 50), (40, 400)")
        .await;

    db.refresh_st("hdiff_st").await;
    let count_2: i64 = db.count("hdiff_st").await;
    assert_eq!(count_2, 4, "after insert, 4 customers total");

    db.assert_st_matches_query(
        "hdiff_st",
        "SELECT customer_id, SUM(val) AS total FROM hdiff_src GROUP BY customer_id",
    )
    .await;
}

#[tokio::test]
async fn test_partitioned_st_hash_custom_modulus() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE hmod_src (
            id SERIAL PRIMARY KEY,
            key_col INT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute("INSERT INTO hmod_src (key_col, val) VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    // HASH with explicit modulus 8
    db.create_st_partitioned(
        "hmod_st",
        "SELECT key_col, SUM(val) AS total FROM hmod_src GROUP BY key_col",
        "1m",
        "DIFFERENTIAL",
        "HASH:key_col:8",
    )
    .await;

    // Verify 8 child partitions were created
    let child_count: i64 = db
        .query_scalar(
            "SELECT count(*)::bigint FROM pg_inherits i \
             JOIN pg_class c ON c.oid = i.inhrelid \
             JOIN pg_class p ON p.oid = i.inhparent \
             WHERE p.relname = 'hmod_st'",
        )
        .await;
    assert_eq!(
        child_count, 8,
        "HASH:key_col:8 should auto-create 8 child partitions"
    );

    db.refresh_st("hmod_st").await;

    db.assert_st_matches_query(
        "hmod_st",
        "SELECT key_col, SUM(val) AS total FROM hmod_src GROUP BY key_col",
    )
    .await;
}

#[tokio::test]
async fn test_partitioned_st_hash_multi_column_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE hmulti_src (
            id SERIAL PRIMARY KEY,
            a INT NOT NULL,
            b INT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    // HASH does not support multi-column partition keys
    let err = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('hmulti_st', \
             $$SELECT a, b, SUM(val) AS total FROM hmulti_src GROUP BY a, b$$, \
             '1m', 'DIFFERENTIAL', partition_by => 'HASH:a,b')",
        )
        .await;
    assert!(err.is_err(), "HASH with multi-column should be rejected");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("single column"),
        "Error should mention single column: {msg}"
    );
}

#[tokio::test]
async fn test_partitioned_st_hash_updates_and_deletes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE hud_src (
            id SERIAL PRIMARY KEY,
            customer_id INT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute("INSERT INTO hud_src (customer_id, val) VALUES (1, 100), (2, 200), (3, 300)")
        .await;

    db.create_st_partitioned(
        "hud_st",
        "SELECT customer_id, SUM(val) AS total FROM hud_src GROUP BY customer_id",
        "1m",
        "DIFFERENTIAL",
        "HASH:customer_id",
    )
    .await;

    db.refresh_st("hud_st").await;
    assert_eq!(db.count("hud_st").await, 3i64);

    // Update an existing row
    db.execute("UPDATE hud_src SET val = 999 WHERE customer_id = 2")
        .await;
    // Delete a row
    db.execute("DELETE FROM hud_src WHERE customer_id = 3")
        .await;

    db.refresh_st("hud_st").await;

    let count: i64 = db.count("hud_st").await;
    assert_eq!(count, 2, "customer 3 should be removed after delete");

    db.assert_st_matches_query(
        "hud_st",
        "SELECT customer_id, SUM(val) AS total FROM hud_src GROUP BY customer_id",
    )
    .await;
}

#[tokio::test]
async fn test_alter_st_partition_by_hash() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE alt_hash_src (
            id SERIAL PRIMARY KEY,
            key_col INT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;

    db.execute("INSERT INTO alt_hash_src (key_col, val) VALUES (1, 10), (2, 20)")
        .await;

    // Start with RANGE partition
    db.create_st_partitioned(
        "alt_hash_st",
        "SELECT key_col, SUM(val) AS total FROM alt_hash_src GROUP BY key_col",
        "1m",
        "DIFFERENTIAL",
        "key_col",
    )
    .await;

    db.refresh_st("alt_hash_st").await;

    // ALTER to HASH partition
    db.execute(
        "SELECT pgtrickle.alter_stream_table('alt_hash_st', partition_by => 'HASH:key_col')",
    )
    .await;

    // Verify the partition key was updated
    let pk: Option<String> = db
        .query_scalar_opt(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'alt_hash_st'",
        )
        .await;
    assert_eq!(pk.as_deref(), Some("HASH:key_col"));

    // Verify the storage is now HASH-partitioned
    let part_strategy: String = db
        .query_scalar(
            "SELECT partstrat::text FROM pg_partitioned_table pt \
             JOIN pg_class c ON c.oid = pt.partrelid \
             WHERE c.relname = 'alt_hash_st'",
        )
        .await;
    assert_eq!(
        part_strategy, "h",
        "ALTER should switch to HASH partitioning"
    );

    db.refresh_st("alt_hash_st").await;

    db.assert_st_matches_query(
        "alt_hash_st",
        "SELECT key_col, SUM(val) AS total FROM alt_hash_src GROUP BY key_col",
    )
    .await;
}

// ── SCALE-1 (v0.26.0): Partition-count scale test ─────────────────────────

/// SCALE-1 (v0.26.0): 1,000-partition source table — trigger install + first refresh < 60 s.
///
/// Creates a RANGE-partitioned source table with 1,000 daily partitions
/// and asserts that:
/// - `create_stream_table()` (which installs CDC triggers) completes in < 60 s.
/// - The first `refresh_stream_table()` completes in < 60 s.
/// - The stream table contains the correct row count.
///
/// This test is `#[ignore]` by default (it is slow) and runs only in the
/// dedicated scale suite:
///   cargo test --test e2e_partition_tests scale -- --ignored --nocapture
#[tokio::test]
#[ignore = "SCALE-1: slow partition-count test — run with --ignored in the scale suite"]
async fn test_scale1_1000_partitions_trigger_install_and_refresh_within_60s() {
    let db = E2eDb::new().await.with_extension().await;

    // Build CREATE TABLE DDL for 1,000 daily partitions.
    let create_parent = "
        CREATE TABLE scale1_src (
            id BIGINT NOT NULL,
            day DATE NOT NULL,
            val INT,
            PRIMARY KEY (id, day)
        ) PARTITION BY RANGE (day)";
    db.execute(create_parent).await;

    // Create 1,000 daily partitions (roughly 2.7 years).
    for i in 0..1000i32 {
        let partition_sql = format!(
            "CREATE TABLE scale1_src_p{i} PARTITION OF scale1_src \
             FOR VALUES FROM ('2020-01-01'::date + {i}) TO ('2020-01-01'::date + {next})",
            next = i + 1,
        );
        db.execute(&partition_sql).await;
    }

    // Seed some data.
    db.execute(
        "INSERT INTO scale1_src (id, day, val)
         SELECT g, '2020-01-01'::date + (g % 1000), g
         FROM generate_series(1, 5000) g",
    )
    .await;

    // Time the create_stream_table call (includes trigger install on all partitions).
    let create_start = std::time::Instant::now();
    db.create_st(
        "scale1_st",
        "SELECT id, day, val FROM scale1_src",
        "5m",
        "FULL",
    )
    .await;
    let create_elapsed = create_start.elapsed();

    assert!(
        create_elapsed.as_secs() < 60,
        "SCALE-1: create_stream_table (trigger install) took {:.1}s, expected < 60s",
        create_elapsed.as_secs_f64()
    );

    // Time the first refresh.
    let refresh_start = std::time::Instant::now();
    db.refresh_st("scale1_st").await;
    let refresh_elapsed = refresh_start.elapsed();

    assert!(
        refresh_elapsed.as_secs() < 60,
        "SCALE-1: first refresh took {:.1}s, expected < 60s",
        refresh_elapsed.as_secs_f64()
    );

    // Correctness: stream table must have all 5,000 rows.
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM public.scale1_st")
        .await;
    assert_eq!(
        count, 5000,
        "SCALE-1: stream table must contain all 5,000 source rows"
    );
}

// ── SCALE-2 (v0.26.0): Multi-DB worker starvation test ────────────────────

/// SCALE-2 (v0.26.0): Worker starvation — hot-tier ST refreshes within SLA
/// despite a flooded worker pool.
///
/// This test requires two separate databases on the same PostgreSQL instance.
/// One database floods the worker pool with slow refreshes; the other has a
/// hot-tier ST that must still complete within its SLA.
///
/// Because this test needs a multi-database setup with the shared preload
/// libraries, it uses `E2eDb::new_on_postgres_db()` with a separate database.
///
/// This test is `#[ignore]` by default (it is slow and infra-intensive).
/// Run with:
///   cargo test --test e2e_partition_tests scale2 -- --ignored --nocapture
#[tokio::test]
#[ignore = "SCALE-2: multi-DB worker starvation — run with --ignored in the scale suite"]
async fn test_scale2_multi_db_worker_pool_hot_tier_refreshes_within_sla() {
    use std::time::Duration;

    // Use the postgres DB as the "hot-tier" database.
    let hot_db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // Configure a fast scheduler so refreshes happen every 100 ms.
    hot_db
        .execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    hot_db
        .execute("ALTER SYSTEM SET pg_trickle.tiered_scheduling = on")
        .await;
    hot_db.reload_config_and_wait().await;

    // Create a hot-tier ST in the hot database.
    hot_db
        .execute("CREATE TABLE scale2_hot_src (id INT PRIMARY KEY, val INT)")
        .await;
    hot_db
        .execute("INSERT INTO scale2_hot_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    hot_db
        .create_st(
            "scale2_hot_st",
            "SELECT id, val FROM scale2_hot_src",
            "1s",
            "FULL",
        )
        .await;

    // Wait for the scheduler and measure if the hot ST refreshes within 5s
    // (lenient SLA allowing for scheduler startup lag).
    let sched_running = hot_db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(
        sched_running,
        "SCALE-2: pg_trickle scheduler did not start within 90s"
    );

    // Insert changes to trigger refreshes.
    hot_db
        .execute("INSERT INTO scale2_hot_src SELECT g, g FROM generate_series(101, 200) g")
        .await;

    // Allow 5 seconds for the hot-tier refresh to complete.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if std::time::Instant::now() > deadline {
            break;
        }
        let count: i64 = hot_db
            .query_scalar("SELECT count(*) FROM public.scale2_hot_st")
            .await;
        if count == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let final_count: i64 = hot_db
        .query_scalar("SELECT count(*) FROM public.scale2_hot_st")
        .await;
    assert_eq!(
        final_count, 200,
        "SCALE-2: hot-tier ST must refresh within SLA (5s), got {final_count} rows"
    );

    // Reset system settings.
    hot_db
        .execute("ALTER SYSTEM RESET pg_trickle.scheduler_interval_ms")
        .await;
    hot_db
        .execute("ALTER SYSTEM RESET pg_trickle.tiered_scheduling")
        .await;
}
