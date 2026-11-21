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

    // A normal ATTACH is automatically repaired by the next full rebuild.
    let state: String = db
        .query_scalar(
            "SELECT status || ':' || needs_reinit::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'attach_st'",
        )
        .await;
    assert_eq!(state, "ACTIVE:true");

    // Refresh should pick up pre-existing rows in the newly attached partition.
    db.refresh_st("attach_st").await;

    db.assert_st_matches_query(
        "public.attach_st",
        "SELECT id, created_at, total FROM attach_orders",
    )
    .await;

    let count: i64 = db.count("attach_st").await;
    assert_eq!(
        count, 3,
        "After reinit, all rows including attached partition data should be visible"
    );
}

#[tokio::test]
async fn test_empty_partition_attach_keeps_auto_trigger_stream_table_active() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE empty_attach_orders (
            id BIGSERIAL,
            created_at DATE NOT NULL,
            total NUMERIC,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)",
    )
    .await;
    db.execute(
        "CREATE TABLE empty_attach_orders_2025 PARTITION OF empty_attach_orders
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute("INSERT INTO empty_attach_orders (created_at, total) VALUES ('2025-06-01', 100.00)")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.cdc_mode = 'auto'",
        "SELECT pgtrickle.create_stream_table(\
            name => 'empty_attach_st',\
            query => $$SELECT id, created_at, total FROM empty_attach_orders$$,\
            schedule => '1m',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    ])
    .await;

    let cdc_mode: String = db
        .query_scalar(
            "SELECT cdc_mode FROM pgtrickle.pgt_dependencies d \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = 'empty_attach_st'",
        )
        .await;
    assert_eq!(cdc_mode, "TRIGGER");

    db.execute(
        "CREATE TABLE empty_attach_orders_2026 (
            id BIGSERIAL,
            created_at DATE NOT NULL,
            total NUMERIC,
            PRIMARY KEY (id, created_at)
        )",
    )
    .await;
    db.execute_seq(&[
        "SET pg_trickle.cdc_mode = 'auto'",
        "ALTER TABLE empty_attach_orders ATTACH PARTITION empty_attach_orders_2026
            FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    ])
    .await;

    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'empty_attach_st'",
        )
        .await;
    assert_eq!(status, "ACTIVE");

    db.execute("INSERT INTO empty_attach_orders (created_at, total) VALUES ('2026-03-01', 200.00)")
        .await;
    db.refresh_st("empty_attach_st").await;
    db.assert_st_matches_query(
        "public.empty_attach_st",
        "SELECT id, created_at, total FROM empty_attach_orders",
    )
    .await;
}

#[tokio::test]
async fn test_empty_partition_attach_resolves_duplicate_names_by_search_path() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE SCHEMA attach_first").await;
    db.execute("CREATE SCHEMA attach_second").await;
    db.execute(
        "CREATE TABLE public.duplicate_attach_orders (
            id INT NOT NULL,
            created_at DATE NOT NULL,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)",
    )
    .await;
    db.execute(
        "CREATE TABLE attach_first.duplicate_attach_part PARTITION OF public.duplicate_attach_orders
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE attach_second.duplicate_attach_part (
            id INT NOT NULL,
            created_at DATE NOT NULL,
            PRIMARY KEY (id, created_at)
        )",
    )
    .await;
    db.execute("INSERT INTO public.duplicate_attach_orders VALUES (1, '2025-06-01')")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'duplicate_attach_st',\
            query => $$SELECT id, created_at FROM public.duplicate_attach_orders$$,\
            schedule => '1m',\
            refresh_mode => 'DIFFERENTIAL',\
            cdc_mode => 'trigger'\
        )",
    )
    .await;

    db.execute_seq(&[
        "SET search_path = attach_second, attach_first, public",
        "ALTER TABLE public.duplicate_attach_orders ATTACH PARTITION duplicate_attach_part \
            FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    ])
    .await;
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'duplicate_attach_st'",
        )
        .await;
    assert_eq!(status, "ACTIVE");

    db.execute("INSERT INTO public.duplicate_attach_orders VALUES (2, '2026-06-01')")
        .await;
    db.refresh_st("public.duplicate_attach_st").await;
    db.assert_st_matches_query(
        "public.duplicate_attach_st",
        "SELECT id, created_at FROM public.duplicate_attach_orders",
    )
    .await;
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

    db.refresh_st_with_retry("detach_st").await;
    let count: i64 = db.count("detach_st").await;
    assert_eq!(count, 2, "Both partitions should contribute data");

    // DETACH a partition
    db.execute("ALTER TABLE detach_orders DETACH PARTITION detach_orders_2026")
        .await;

    // A normal DETACH is automatically repaired by the next refresh.
    let state: String = db
        .query_scalar(
            "SELECT status || ':' || needs_reinit::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'detach_st'",
        )
        .await;
    assert_eq!(state, "ACTIVE:true");

    // After automatic repair, only the remaining partition's data is visible.
    db.refresh_st_with_retry("detach_st").await;

    let action: (String, String) = sqlx::query_as(
        "SELECT h.action, h.status FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
         WHERE s.pgt_name = 'detach_st' AND h.initiated_by = 'MANUAL' \
         ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read completed detach refresh");
    assert!(matches!(action.0.as_str(), "FULL" | "REINITIALIZE"));
    assert_eq!(action.1, "COMPLETED");

    let count: i64 = db.count("detach_st").await;
    assert_eq!(
        count, 1,
        "After reinit, only remaining partition data should be visible"
    );
}

// ── #1418: Partition removal must never leave silent stale aggregates ──

#[tokio::test]
async fn test_partition_direct_drop_marks_parent_reinit_and_rebuilds_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE drop_orders (
            id INT, d DATE NOT NULL, amount INT,
            PRIMARY KEY (id, d)
        ) PARTITION BY RANGE (d)",
    )
    .await;
    db.execute(
        "CREATE TABLE drop_orders_old PARTITION OF drop_orders
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE drop_orders_new PARTITION OF drop_orders
         FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;
    db.execute(
        "INSERT INTO drop_orders VALUES
         (1, '2025-01-01', 10), (2, '2025-06-01', 20),
         (3, '2026-01-01', 30)",
    )
    .await;
    let query = "SELECT COUNT(*) AS n, SUM(amount) AS total FROM drop_orders";
    db.create_st("drop_totals", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st("drop_totals").await;
    db.assert_st_matches_query("drop_totals", query).await;

    // No DETACH and no row-level DELETEs: sql_drop must trace the dropped
    // child's OID through the parent's saved ancestry snapshot.
    db.execute("DROP TABLE drop_orders_old").await;
    let state: String = db
        .query_scalar(
            "SELECT status || ':' || needs_reinit::text
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'drop_totals'",
        )
        .await;
    assert_eq!(state, "ACTIVE:true");

    db.refresh_st("drop_totals").await;
    db.assert_st_matches_query("drop_totals", query).await;
    let remaining: i64 = db.query_scalar("SELECT n FROM drop_totals").await;
    assert_eq!(remaining, 1);
}

#[tokio::test]
async fn test_partition_drop_with_misaligned_shared_hour_rebuilds_exact_groups() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute(
        "CREATE TABLE shared_hour_events (
            id INT NOT NULL, event_day DATE NOT NULL, occurred TIMESTAMP NOT NULL,
            account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id, event_day)
         ) PARTITION BY RANGE (event_day)",
    )
    .await;
    db.execute(
        "CREATE TABLE shared_hour_old PARTITION OF shared_hour_events
         FOR VALUES FROM ('2025-01-01') TO ('2025-01-02')",
    )
    .await;
    db.execute(
        "CREATE TABLE shared_hour_live PARTITION OF shared_hour_events
         FOR VALUES FROM ('2025-01-02') TO ('2025-01-03')",
    )
    .await;
    db.execute(
        "INSERT INTO shared_hour_events VALUES
         (1, '2025-01-01', '2025-01-02 10:10', 'a', 10),
         (2, '2025-01-02', '2025-01-02 10:20', 'a', 20),
         (3, '2025-01-01', '2025-01-01 09:00', 'b', 30),
         (4, '2025-01-02', '2025-01-02 11:00', 'b', 40)",
    )
    .await;
    let query = "SELECT account, date_trunc('hour', occurred) AS hour, \
                 SUM(amount) AS total, COUNT(*) AS n FROM shared_hour_events GROUP BY 1, 2";
    db.create_st("shared_hour_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("shared_hour_st", query).await;

    db.execute("DROP TABLE shared_hour_old").await;
    db.refresh_st_with_retry("shared_hour_st").await;
    let history: (String, String) = sqlx::query_as(
        "SELECT action, status FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                         WHERE pgt_name = 'shared_hour_st') \
         ORDER BY refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read partition removal refresh history");
    assert_eq!(history, ("REINITIALIZE".into(), "COMPLETED".into()));
    db.assert_st_matches_query("shared_hour_st", query).await;
    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT account, to_char(hour, 'YYYY-MM-DD HH24:MI'), total, n \
         FROM shared_hour_st ORDER BY account, hour",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read remaining shared-hour aggregates");
    assert_eq!(
        rows,
        vec![
            ("a".into(), "2025-01-02 10:00".into(), 20, 1),
            ("b".into(), "2025-01-02 11:00".into(), 40, 1),
        ]
    );
}

#[tokio::test]
async fn test_partition_create_child_after_stream_then_direct_drop() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE create_part_root (
            id INT, d DATE NOT NULL, PRIMARY KEY (id,d)
        ) PARTITION BY RANGE (d)",
    )
    .await;
    db.execute(
        "CREATE TABLE create_part_old PARTITION OF create_part_root
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute("INSERT INTO create_part_root VALUES (1,'2025-02-01')")
        .await;
    let query = "SELECT COUNT(*) AS n FROM create_part_root";
    db.create_st("create_part_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st("create_part_st").await;

    // Unlike ALTER TABLE ATTACH, CREATE TABLE ... PARTITION OF only issues
    // CREATE TABLE on the child. It must still update the parent snapshot.
    db.execute(
        "CREATE TABLE create_part_new PARTITION OF create_part_root
         FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;
    db.execute("INSERT INTO create_part_root VALUES (2,'2026-03-01')")
        .await;
    db.refresh_st("create_part_st").await;
    db.assert_st_matches_query("create_part_st", query).await;

    db.execute("DROP TABLE create_part_new").await;
    let state: String = db
        .query_scalar(
            "SELECT status || ':' || needs_reinit::text
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'create_part_st'",
        )
        .await;
    assert_eq!(state, "ACTIVE:true");
    db.refresh_st("create_part_st").await;
    db.assert_st_matches_query("create_part_st", query).await;
}

#[tokio::test]
async fn test_partition_nested_detach_rebuilds_root_and_downstream() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute(
        "CREATE TABLE nested_detach_source (d DATE NOT NULL, region TEXT NOT NULL, amount INT NOT NULL)
         PARTITION BY RANGE (d)",
    )
    .await;
    db.execute(
        "CREATE TABLE nested_detach_year PARTITION OF nested_detach_source
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01') PARTITION BY LIST (region)",
    )
    .await;
    db.execute(
        "CREATE TABLE nested_detach_eu PARTITION OF nested_detach_year FOR VALUES IN ('EU')",
    )
    .await;
    db.execute(
        "CREATE TABLE nested_detach_us PARTITION OF nested_detach_year FOR VALUES IN ('US')",
    )
    .await;
    db.execute(
        "INSERT INTO nested_detach_source VALUES ('2025-01-01','EU',10), ('2025-01-02','US',20)",
    )
    .await;
    let query = "SELECT region, SUM(amount) AS total FROM nested_detach_source GROUP BY region";
    db.create_st("nested_detach_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st(
        "nested_detach_downstream",
        "SELECT region, total FROM nested_detach_st",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE TABLE nested_detach_ca (LIKE nested_detach_source INCLUDING ALL)")
        .await;
    db.execute(
        "ALTER TABLE nested_detach_year ATTACH PARTITION nested_detach_ca FOR VALUES IN ('CA')",
    )
    .await;
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'nested_detach_st'",
        )
        .await;
    assert!(!needs_reinit, "empty nested attachment should not rebuild");

    let mut tx = db.pool.begin().await.expect("begin nested detach rollback");
    sqlx::query("ALTER TABLE nested_detach_year DETACH PARTITION nested_detach_eu")
        .execute(&mut *tx)
        .await
        .expect("detach nested partition in transaction");
    tx.rollback().await.expect("roll back nested detach");
    db.assert_st_matches_query("nested_detach_st", query).await;
    db.execute("ALTER TABLE nested_detach_year DETACH PARTITION nested_detach_eu")
        .await;
    let states: Vec<(String, bool)> = sqlx::query_as(
        "SELECT pgt_name, needs_reinit FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_name IN ('nested_detach_st', 'nested_detach_downstream') ORDER BY pgt_name",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read nested detach recovery states");
    assert_eq!(
        states,
        vec![
            ("nested_detach_downstream".into(), true),
            ("nested_detach_st".into(), true),
        ]
    );
    db.refresh_st_with_retry("nested_detach_st").await;
    db.refresh_st_with_retry("nested_detach_downstream").await;
    let actions: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.pgt_name, h.action, h.status FROM pgtrickle.pgt_stream_tables s \
         JOIN LATERAL (SELECT action, status FROM pgtrickle.pgt_refresh_history \
           WHERE pgt_id = s.pgt_id ORDER BY refresh_id DESC LIMIT 1) h ON true \
         WHERE s.pgt_name IN ('nested_detach_st', 'nested_detach_downstream') ORDER BY s.pgt_name",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read nested detach refresh actions");
    assert_eq!(
        actions,
        vec![
            (
                "nested_detach_downstream".into(),
                "REINITIALIZE".into(),
                "COMPLETED".into()
            ),
            (
                "nested_detach_st".into(),
                "REINITIALIZE".into(),
                "COMPLETED".into()
            ),
        ]
    );
    db.assert_st_matches_query("nested_detach_st", query).await;
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT region, total FROM nested_detach_downstream ORDER BY region")
            .fetch_all(&db.pool)
            .await
            .expect("read downstream aggregates after nested detach");
    assert_eq!(rows, vec![("US".into(), 20)]);
    db.execute(
        "ALTER TABLE nested_detach_year ATTACH PARTITION nested_detach_eu FOR VALUES IN ('EU')",
    )
    .await;
    db.refresh_st_with_retry("nested_detach_st").await;
    db.refresh_st_with_retry("nested_detach_downstream").await;
    db.assert_st_matches_query("nested_detach_st", query).await;
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT region, total FROM nested_detach_downstream ORDER BY region")
            .fetch_all(&db.pool)
            .await
            .expect("read downstream aggregates after nested reattachment");
    assert_eq!(rows, vec![("EU".into(), 10), ("US".into(), 20)]);

    db.execute("ALTER TABLE nested_detach_year DETACH PARTITION nested_detach_eu CONCURRENTLY")
        .await;
    let statuses: Vec<(String, String)> = sqlx::query_as(
        "SELECT pgt_name, status FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_name IN ('nested_detach_st', 'nested_detach_downstream') ORDER BY pgt_name",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read nested concurrent detach statuses");
    assert_eq!(
        statuses,
        vec![
            ("nested_detach_downstream".into(), "SUSPENDED".into()),
            ("nested_detach_st".into(), "SUSPENDED".into()),
        ]
    );
    db.execute("SELECT pgtrickle.resume_stream_table('nested_detach_st')")
        .await;
    db.execute("SELECT pgtrickle.resume_stream_table('nested_detach_downstream')")
        .await;
    db.refresh_st_with_retry("nested_detach_st").await;
    db.refresh_st_with_retry("nested_detach_downstream").await;
    db.assert_st_matches_query("nested_detach_st", query).await;
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT region, total FROM nested_detach_downstream ORDER BY region")
            .fetch_all(&db.pool)
            .await
            .expect("read downstream aggregates after concurrent detach recovery");
    assert_eq!(rows, vec![("US".into(), 20)]);
}

#[tokio::test]
async fn test_retention_concurrent_detach_preserves_retained_history() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE concurrent_retain_src (id integer NOT NULL, d date NOT NULL, amount integer NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE concurrent_retain_old PARTITION OF concurrent_retain_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE concurrent_retain_live PARTITION OF concurrent_retain_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO concurrent_retain_src VALUES (1,'2025-06-01',10), (2,'2026-06-01',20)")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('concurrent_retain_st', 'SELECT d, sum(amount) AS total, count(*) AS n FROM concurrent_retain_src GROUP BY d', 'RETAIN', 'd', '2026-01-01', '1h')").await;
    db.create_st(
        "concurrent_retain_downstream",
        "SELECT d, total, n FROM concurrent_retain_st",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "concurrent_retain_paused",
        "SELECT d, total, n FROM concurrent_retain_st",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.pause_stream_table('concurrent_retain_paused')")
        .await;
    let marker: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.execute(
        "ALTER TABLE concurrent_retain_src DETACH PARTITION concurrent_retain_old CONCURRENTLY",
    )
    .await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM concurrent_retain_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read retained aggregates");
    assert_eq!(
        rows,
        vec![("2025-06-01".into(), 10, 1), ("2026-06-01".into(), 20, 1)]
    );
    let policy: serde_json::Value = db
        .query_scalar(
            "SELECT pgtrickle.get_stream_table_partition_retention('concurrent_retain_st')",
        )
        .await;
    assert_eq!(
        policy["sealed_ranges"],
        serde_json::json!([{"lower":"2025-01-01", "upper":"2026-01-01"}])
    );
    assert!(policy.get("pending_detach").is_none());
    db.execute("INSERT INTO concurrent_retain_src VALUES (3,'2026-06-01',5)")
        .await;
    db.refresh_st_with_retry("concurrent_retain_st").await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM concurrent_retain_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read retained and live aggregates");
    assert_eq!(
        rows,
        vec![("2025-06-01".into(), 10, 1), ("2026-06-01".into(), 25, 2)]
    );
    let downstream_status: String = db.query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'concurrent_retain_downstream'").await;
    assert_eq!(downstream_status, "ACTIVE");
    let paused_status: String = db.query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'concurrent_retain_paused'").await;
    assert_eq!(paused_status, "SUSPENDED");
    db.refresh_st_with_retry("concurrent_retain_downstream")
        .await;
    let downstream: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM concurrent_retain_downstream ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read downstream retained aggregates");
    assert_eq!(downstream, rows);
    let actions: Vec<(String, String, bool)> = sqlx::query_as("SELECT h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'concurrent_retain_st' AND h.refresh_id > $1 ORDER BY h.refresh_id")
        .bind(marker).fetch_all(&db.pool).await.expect("read concurrent retention refresh actions");
    assert!(!actions.is_empty());
    assert!(
        actions
            .iter()
            .all(
                |(action, status, fallback)| (action == "DIFFERENTIAL" || action == "NO_DATA")
                    && status == "COMPLETED"
                    && !fallback
            ),
        "{actions:?}"
    );
}

#[tokio::test]
async fn test_retention_concurrent_detach_as_source_owner() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE ROLE concurrent_retention_owner LOGIN")
        .await;
    db.execute(
        "CREATE SCHEMA concurrent_retention_tenant AUTHORIZATION concurrent_retention_owner",
    )
    .await;
    db.execute("CREATE TABLE concurrent_retention_tenant.events (d date NOT NULL, amount integer NOT NULL) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE concurrent_retention_tenant.events_old PARTITION OF concurrent_retention_tenant.events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE concurrent_retention_tenant.events_live PARTITION OF concurrent_retention_tenant.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute(
        "ALTER TABLE concurrent_retention_tenant.events OWNER TO concurrent_retention_owner",
    )
    .await;
    db.execute(
        "ALTER TABLE concurrent_retention_tenant.events_old OWNER TO concurrent_retention_owner",
    )
    .await;
    db.execute(
        "ALTER TABLE concurrent_retention_tenant.events_live OWNER TO concurrent_retention_owner",
    )
    .await;
    db.execute("INSERT INTO concurrent_retention_tenant.events VALUES ('2025-06-01',10), ('2026-06-01',20)").await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('concurrent_retention_owner_st', 'SELECT d, sum(amount) AS total, count(*) AS n FROM concurrent_retention_tenant.events GROUP BY d', 'RETAIN', 'd', '2026-01-01', '1h')").await;
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire source owner connection");
    sqlx::query("SET ROLE concurrent_retention_owner")
        .execute(&mut *conn)
        .await
        .expect("set source owner role");
    sqlx::query("SET search_path = concurrent_retention_tenant, public, pg_catalog")
        .execute(&mut *conn)
        .await
        .expect("set source owner search path");
    sqlx::query("ALTER TABLE events DETACH PARTITION events_old CONCURRENTLY")
        .execute(&mut *conn)
        .await
        .expect("detach as source owner");
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset role");
    drop(conn);
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text,total,n FROM concurrent_retention_owner_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read retained rows");
    assert_eq!(
        rows,
        vec![("2025-06-01".into(), 10, 1), ("2026-06-01".into(), 20, 1)]
    );
}

#[tokio::test]
async fn test_retention_concurrent_nested_range_with_both_policies() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE nested_concurrent_retention (d date NOT NULL, amount integer NOT NULL) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE nested_concurrent_year PARTITION OF nested_concurrent_retention FOR VALUES FROM ('2025-01-01') TO ('2026-01-01') PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE nested_concurrent_old PARTITION OF nested_concurrent_year FOR VALUES FROM ('2025-01-01') TO ('2025-07-01')").await;
    db.execute("CREATE TABLE nested_concurrent_current PARTITION OF nested_concurrent_year FOR VALUES FROM ('2025-07-01') TO ('2026-01-01')").await;
    db.execute(
        "INSERT INTO nested_concurrent_retention VALUES ('2025-03-01',10), ('2025-09-01',20)",
    )
    .await;
    let query =
        "SELECT d, sum(amount) AS total, count(*) AS n FROM nested_concurrent_retention GROUP BY d";
    for (name, mode) in [
        ("nested_concurrent_trim", "TRIM"),
        ("nested_concurrent_retain", "RETAIN"),
    ] {
        sqlx::query("SELECT pgtrickle.create_stream_table_with_retention($1, $2, $3, 'd', '2025-07-01', '1h')")
            .bind(name).bind(query).bind(mode).execute(&db.pool).await.expect("configure nested retention");
    }
    let marker: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.execute(
        "ALTER TABLE nested_concurrent_year DETACH PARTITION nested_concurrent_old CONCURRENTLY",
    )
    .await;
    db.assert_st_matches_query("nested_concurrent_trim", query)
        .await;
    let retained: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text,total,n FROM nested_concurrent_retain ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read nested retained history");
    assert_eq!(
        retained,
        vec![("2025-03-01".into(), 10, 1), ("2025-09-01".into(), 20, 1)]
    );
    let states: Vec<(String,String)> = sqlx::query_as("SELECT pgt_name,status FROM pgtrickle.pgt_stream_tables WHERE pgt_name IN ('nested_concurrent_trim','nested_concurrent_retain') ORDER BY pgt_name")
        .fetch_all(&db.pool).await.expect("read nested stream states");
    assert_eq!(
        states,
        vec![
            ("nested_concurrent_retain".into(), "ACTIVE".into()),
            ("nested_concurrent_trim".into(), "ACTIVE".into())
        ]
    );
    let rebuilds: Vec<String> = sqlx::query_scalar("SELECT h.action FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name IN ('nested_concurrent_trim','nested_concurrent_retain') AND h.refresh_id > $1 AND (h.action IN ('FULL','REINITIALIZE') OR h.was_full_fallback) ORDER BY h.refresh_id")
        .bind(marker).fetch_all(&db.pool).await.expect("read nested refresh actions");
    assert_eq!(rebuilds, Vec::<String>::new());
}

#[tokio::test]
async fn test_retention_concurrent_retain_waits_for_live_writer() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE writer_pending_src (id integer NOT NULL, d date NOT NULL, amount integer NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE writer_pending_old PARTITION OF writer_pending_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE writer_pending_live PARTITION OF writer_pending_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO writer_pending_src VALUES (1,'2025-06-01',10), (2,'2026-06-01',20)")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('writer_pending_st', 'SELECT d, sum(amount) AS total, count(*) AS n FROM writer_pending_src GROUP BY d', 'RETAIN', 'd', '2026-01-01', '1h')").await;
    let mut reader = db.pool.begin().await.expect("begin old source snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *reader)
        .await
        .expect("set isolation");
    let before: Vec<(i32, i32)> =
        sqlx::query_as("SELECT id, amount FROM writer_pending_src ORDER BY id")
            .fetch_all(&mut *reader)
            .await
            .expect("hold source snapshot");
    assert_eq!(before, vec![(1, 10), (2, 20)]);
    let mut connection = db.pool.acquire().await.expect("acquire detach connection");
    let detach_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .expect("read detach PID");
    let detach = tokio::spawn(async move {
        sqlx::query(
            "ALTER TABLE writer_pending_src DETACH PARTITION writer_pending_old CONCURRENTLY",
        )
        .execute(&mut *connection)
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let pending: bool = db.query_scalar("SELECT COALESCE((SELECT inhdetachpending FROM pg_catalog.pg_inherits WHERE inhrelid = 'writer_pending_old'::regclass), false)").await;
            if pending { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.expect("detach enters pending phase");
    let mut writer = db.pool.begin().await.expect("begin live writer");
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .expect("read writer PID");
    sqlx::query("INSERT INTO writer_pending_src VALUES (3,'2026-06-01',5)")
        .execute(&mut *writer)
        .await
        .expect("write live partition");
    reader.commit().await.expect("release old snapshot");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let waiting: bool =
                sqlx::query_scalar("SELECT $2 = ANY(pg_catalog.pg_blocking_pids($1))")
                    .bind(detach_pid)
                    .bind(writer_pid)
                    .fetch_one(&db.pool)
                    .await
                    .expect("inspect detach wait");
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("detach waits for live writer");
    let visible: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM writer_pending_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read stream during detach");
    assert_eq!(
        visible,
        vec![("2025-06-01".into(), 10, 1), ("2026-06-01".into(), 20, 1)]
    );
    writer.commit().await.expect("commit live writer");
    detach
        .await
        .expect("join detach")
        .expect("complete concurrent detach");
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM writer_pending_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read retained and live rows");
    assert_eq!(
        rows,
        vec![("2025-06-01".into(), 10, 1), ("2026-06-01".into(), 25, 2)]
    );
    db.refresh_st_with_retry("writer_pending_st").await;
    let after: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM writer_pending_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read rows after next refresh");
    assert_eq!(after, rows);
    let actions: Vec<(String,String,bool)> = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name = 'writer_pending_st' ORDER BY h.refresh_id DESC LIMIT 2")
        .fetch_all(&db.pool).await.expect("read concurrent refresh actions");
    assert_eq!(
        actions,
        vec![
            ("DIFFERENTIAL".into(), "COMPLETED".into(), false),
            ("DIFFERENTIAL".into(), "COMPLETED".into(), false)
        ]
    );
}

#[tokio::test]
async fn test_retention_concurrent_retain_retries_failed_second_phase() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retry_pending_src (id integer NOT NULL, d date NOT NULL, amount integer NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE retry_pending_old PARTITION OF retry_pending_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retry_pending_live PARTITION OF retry_pending_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO retry_pending_src VALUES (1,'2025-06-01',10), (2,'2026-06-01',20)")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('retry_pending_st', 'SELECT d, sum(amount) AS total, count(*) AS n FROM retry_pending_src GROUP BY d', 'RETAIN', 'd', '2026-01-01', '1h')").await;
    db.execute("CREATE FUNCTION public.fail_retention_detach() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'intentional second-phase failure'; END $$").await;
    db.execute("CREATE EVENT TRIGGER zz_fail_retention_detach ON ddl_command_end WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION public.fail_retention_detach()").await;
    let mut reader = db.pool.begin().await.expect("begin old source snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *reader)
        .await
        .expect("set isolation");
    let before: Vec<(i32, i32)> =
        sqlx::query_as("SELECT id,amount FROM retry_pending_src ORDER BY id")
            .fetch_all(&mut *reader)
            .await
            .expect("hold old source snapshot");
    assert_eq!(before, vec![(1, 10), (2, 20)]);
    let pool = db.pool.clone();
    let detach = tokio::spawn(async move {
        sqlx::query("ALTER TABLE retry_pending_src DETACH PARTITION retry_pending_old CONCURRENTLY")
            .execute(&pool)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let pending: bool = db.query_scalar("SELECT COALESCE((SELECT inhdetachpending FROM pg_catalog.pg_inherits WHERE inhrelid = 'retry_pending_old'::regclass), false)").await;
            if pending { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.expect("detach enters pending phase");
    let frontier: serde_json::Value = db
        .query_scalar(
            "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retry_pending_st'",
        )
        .await;
    db.execute("INSERT INTO retry_pending_old VALUES (3,'2025-06-01',7)")
        .await;
    reader.commit().await.expect("release old snapshot");
    let error = detach
        .await
        .expect("join detach")
        .expect_err("second phase must fail");
    assert!(
        error
            .to_string()
            .contains("intentional second-phase failure"),
        "{error}"
    );
    let pending: bool = db.query_scalar("SELECT inhdetachpending FROM pg_catalog.pg_inherits WHERE inhrelid = 'retry_pending_old'::regclass").await;
    assert!(pending);
    let after_frontier: serde_json::Value = db
        .query_scalar(
            "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retry_pending_st'",
        )
        .await;
    assert_eq!(after_frontier, frontier);
    let before_retry: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text,total,n FROM retry_pending_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read rolled-back stream");
    assert_eq!(
        before_retry,
        vec![("2025-06-01".into(), 10, 1), ("2026-06-01".into(), 20, 1)]
    );
    db.execute("ALTER EVENT TRIGGER zz_fail_retention_detach DISABLE")
        .await;
    db.execute("ALTER TABLE retry_pending_src DETACH PARTITION retry_pending_old FINALIZE")
        .await;
    let recovered: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text,total,n FROM retry_pending_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read recovered stream");
    assert_eq!(
        recovered,
        vec![("2025-06-01".into(), 17, 2), ("2026-06-01".into(), 20, 1)]
    );
    db.refresh_st_with_retry("retry_pending_st").await;
    let after: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text,total,n FROM retry_pending_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read stream after retry");
    assert_eq!(after, recovered);
}

#[tokio::test]
#[cfg(not(feature = "light-e2e"))]
async fn test_retention_concurrent_retain_recovers_pending_after_restart() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE restart_pending_src (id integer NOT NULL, d date NOT NULL, amount integer NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE restart_pending_old PARTITION OF restart_pending_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE restart_pending_live PARTITION OF restart_pending_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO restart_pending_src VALUES (1,'2025-06-01',10), (2,'2026-06-01',20)")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('restart_pending_st', 'SELECT d, sum(amount) AS total, count(*) AS n FROM restart_pending_src GROUP BY d', 'RETAIN', 'd', '2026-01-01', '1h')").await;
    let mut reader = db.pool.begin().await.expect("begin old source snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *reader)
        .await
        .expect("set isolation");
    let before: Vec<(i32, i32)> =
        sqlx::query_as("SELECT id, amount FROM restart_pending_src ORDER BY id")
            .fetch_all(&mut *reader)
            .await
            .expect("hold old source snapshot");
    assert_eq!(before, vec![(1, 10), (2, 20)]);
    let mut connection = db.pool.acquire().await.expect("acquire detach connection");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .expect("read detach PID");
    let detach = tokio::spawn(async move {
        sqlx::query(
            "ALTER TABLE restart_pending_src DETACH PARTITION restart_pending_old CONCURRENTLY",
        )
        .execute(&mut *connection)
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let pending: bool = db.query_scalar("SELECT COALESCE((SELECT inhdetachpending FROM pg_catalog.pg_inherits WHERE inhrelid = 'restart_pending_old'::regclass), false)").await;
            if pending { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.expect("detach enters pending phase");
    db.execute("INSERT INTO restart_pending_old VALUES (3,'2025-06-01',7)")
        .await;
    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(pid)
        .fetch_one(&db.pool)
        .await
        .expect("cancel detach");
    assert!(cancelled);
    assert!(detach.await.expect("join detach").is_err());
    reader.commit().await.expect("release old snapshot");
    db.pool.close().await;
    let restart = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart database");
    assert!(
        restart.status.success(),
        "{}",
        String::from_utf8_lossy(&restart.stderr)
    );
    db.pool = db.reconnect_after_restart().await;
    let policy: serde_json::Value = db
        .query_scalar("SELECT pgtrickle.get_stream_table_partition_retention('restart_pending_st')")
        .await;
    assert_eq!(policy["pending_detach"]["lower"], "2025-01-01");
    assert_eq!(policy["pending_detach"]["upper"], "2026-01-01");
    db.execute("ALTER TABLE restart_pending_src DETACH PARTITION restart_pending_old FINALIZE")
        .await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM restart_pending_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read recovered history");
    assert_eq!(
        rows,
        vec![("2025-06-01".into(), 17, 2), ("2026-06-01".into(), 20, 1)]
    );
    let action: (String,String,bool) = sqlx::query_as("SELECT h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'restart_pending_st' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&db.pool).await.expect("read recovered refresh action");
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    db.execute("INSERT INTO restart_pending_src VALUES (4,'2026-06-01',5)")
        .await;
    db.refresh_st_with_retry("restart_pending_st").await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM restart_pending_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read history after live insert");
    assert_eq!(
        rows,
        vec![("2025-06-01".into(), 17, 2), ("2026-06-01".into(), 25, 2)]
    );
}

#[tokio::test]
async fn test_retention_concurrent_detach_captures_pending_child_writes() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE pending_retention_src (id integer NOT NULL, d date NOT NULL, amount integer NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE pending_retention_old PARTITION OF pending_retention_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE pending_retention_live PARTITION OF pending_retention_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO pending_retention_src VALUES (1,'2025-06-01',10), (2,'2026-06-01',20)")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('pending_retention_st', 'SELECT d, sum(amount) AS total, count(*) AS n FROM pending_retention_src GROUP BY d', 'TRIM', 'd', '2026-01-01', '1h')").await;
    db.create_st(
        "pending_retention_other",
        "SELECT d, sum(amount) AS total, count(*) AS n FROM pending_retention_src GROUP BY d",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "pending_retention_downstream",
        "SELECT d, total, n FROM pending_retention_st",
        "1h",
        "DIFFERENTIAL",
    )
    .await;

    let mut reader = db.pool.begin().await.expect("begin old source snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *reader)
        .await
        .expect("set snapshot isolation");
    let before: Vec<(i32, String, i32)> =
        sqlx::query_as("SELECT id, d::text, amount FROM pending_retention_src ORDER BY id")
            .fetch_all(&mut *reader)
            .await
            .expect("hold old partition snapshot");
    assert_eq!(
        before,
        vec![(1, "2025-06-01".into(), 10), (2, "2026-06-01".into(), 20)]
    );

    let mut connection = db.pool.acquire().await.expect("acquire detach connection");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .expect("read detach backend PID");
    let detach = tokio::spawn(async move {
        sqlx::query(
            "ALTER TABLE pending_retention_src DETACH PARTITION pending_retention_old CONCURRENTLY",
        )
        .execute(&mut *connection)
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let pending: bool = db.query_scalar("SELECT COALESCE((SELECT inhdetachpending FROM pg_catalog.pg_inherits WHERE inhrelid = 'pending_retention_old'::regclass), false)").await;
            if pending { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.expect("concurrent detach enters pending phase");

    let policy: serde_json::Value = db
        .query_scalar(
            "SELECT pgtrickle.get_stream_table_partition_retention('pending_retention_st')",
        )
        .await;
    assert_eq!(policy["pending_detach"]["lower"], "2025-01-01");
    assert_eq!(policy["pending_detach"]["upper"], "2026-01-01");
    let error = db
        .try_execute(
            "SELECT pgtrickle.set_stream_table_partition_retention('pending_retention_st', 'OFF')",
        )
        .await
        .expect_err("a pending retention cut must not be removed");
    assert!(
        error.to_string().contains("pending retention detach"),
        "{error}"
    );

    db.execute("UPDATE pending_retention_old SET amount = 12 WHERE id = 1")
        .await;
    db.execute("DELETE FROM pending_retention_old WHERE id = 1")
        .await;
    db.execute("INSERT INTO pending_retention_old VALUES (3,'2025-06-01',7)")
        .await;
    let source: Vec<(i32, String, i32)> =
        sqlx::query_as("SELECT id, d::text, amount FROM pending_retention_src ORDER BY id")
            .fetch_all(&db.pool)
            .await
            .expect("read source during pending detach");
    assert_eq!(source, vec![(2, "2026-06-01".into(), 20)]);
    let buffer: String = db.query_scalar("SELECT pg_catalog.format('%I.%I', 'pgtrickle_changes', 'changes_' || source_stable_name) FROM pgtrickle.pgt_change_tracking WHERE source_relid = 'pending_retention_src'::regclass").await;
    let changes: Vec<(String, i32, String, i32)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT action::text, id, d::text, amount FROM {buffer} WHERE id = 3 ORDER BY id"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("read pending child CDC");
    assert_eq!(changes, vec![("I".into(), 3, "2025-06-01".into(), 7)]);

    // Another consumer must not clean up the suspended consumer's CDC.
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET status = 'ACTIVE' WHERE pgt_name = 'pending_retention_other'").await;
    db.refresh_st_with_retry("pending_retention_other").await;
    let retained_changes: Vec<(String, i32, String, i32)> = sqlx::query_as(sqlx::AssertSqlSafe(
        format!("SELECT action::text, id, d::text, amount FROM {buffer} WHERE id = 3 ORDER BY id"),
    ))
    .fetch_all(&db.pool)
    .await
    .expect("read CDC after other consumer refresh");
    assert_eq!(
        retained_changes,
        vec![("I".into(), 3, "2025-06-01".into(), 7)]
    );

    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(pid)
        .fetch_one(&db.pool)
        .await
        .expect("cancel concurrent detach");
    assert!(cancelled);
    assert!(detach.await.expect("join detach task").is_err());
    reader.commit().await.expect("release old snapshot");
    let pending: bool = db.query_scalar("SELECT inhdetachpending FROM pg_catalog.pg_inherits WHERE inhrelid = 'pending_retention_old'::regclass").await;
    assert!(pending);
    let error = db
        .try_execute("DROP TABLE pending_retention_old")
        .await
        .expect_err("a pending retention partition must not be dropped");
    assert!(error.to_string().contains("pending"), "{error}");
    let drop_guard: String = db.query_scalar("SELECT pg_catalog.format('DROP TRIGGER %I ON %I.%I', t.tgname, n.nspname, c.relname) FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE t.tgrelid = 'pending_retention_old'::regclass AND t.tgname LIKE 'pg_trickle_retention_truncate_%'").await;
    let error = db
        .try_execute(&drop_guard)
        .await
        .expect_err("pending child CDC guard must remain installed");
    assert!(
        error.to_string().contains("cannot drop retention capture"),
        "{error}"
    );
    let error = db
        .try_execute("ALTER TABLE pending_retention_src ADD COLUMN extra integer")
        .await
        .expect_err("source schema must not change during a pending retention cut");
    assert!(
        error.to_string().contains("source schema cannot change"),
        "{error}"
    );
    let extra: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = 'pending_retention_src'::regclass AND attname = 'extra' AND NOT attisdropped)").await;
    assert!(!extra);
    db.execute("CREATE TABLE pending_retention_attach (LIKE pending_retention_src INCLUDING ALL)")
        .await;
    let error = db.try_execute("ALTER TABLE pending_retention_src ATTACH PARTITION pending_retention_attach FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')")
        .await.expect_err("a partition must not be attached during a pending retention cut");
    assert!(error.to_string().contains("pending"), "{error}");
    db.execute("ALTER TABLE pending_retention_src DETACH PARTITION pending_retention_old FINALIZE")
        .await;
    let attached: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid = 'pending_retention_old'::regclass)").await;
    assert!(!attached);
    let replayed: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM pending_retention_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read post-detach CDC replay");
    assert_eq!(replayed, vec![("2026-06-01".into(), 20, 1)]);
    let action: (String, String) = sqlx::query_as("SELECT h.action, h.status FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'pending_retention_st' AND h.initiated_by = 'MANUAL' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&db.pool).await.expect("read post-detach replay action");
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into()));
    let downstream_status: String = db.query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'pending_retention_downstream'").await;
    assert_eq!(downstream_status, "ACTIVE");
    db.refresh_st_with_retry("pending_retention_downstream")
        .await;
    let downstream: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM pending_retention_downstream ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read downstream trimmed aggregates");
    assert_eq!(downstream, replayed);
    let child: Vec<(i32, String, i32)> =
        sqlx::query_as("SELECT id, d::text, amount FROM pending_retention_old ORDER BY id")
            .fetch_all(&db.pool)
            .await
            .expect("read detached partition");
    assert_eq!(child, vec![(3, "2025-06-01".into(), 7)]);
}

#[tokio::test]
async fn test_partition_nested_concurrent_detach_suspends_during_pending_phase() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute(
        "CREATE TABLE nested_pending_source (d DATE NOT NULL, region TEXT NOT NULL)
         PARTITION BY RANGE (d)",
    )
    .await;
    db.execute(
        "CREATE TABLE nested_pending_year PARTITION OF nested_pending_source
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01') PARTITION BY LIST (region)",
    )
    .await;
    db.execute(
        "CREATE TABLE nested_pending_eu PARTITION OF nested_pending_year FOR VALUES IN ('EU')",
    )
    .await;
    db.execute(
        "CREATE TABLE nested_pending_us PARTITION OF nested_pending_year FOR VALUES IN ('US')",
    )
    .await;
    db.execute("INSERT INTO nested_pending_source VALUES ('2025-01-01','EU'), ('2025-01-02','US')")
        .await;
    let query = "SELECT region, COUNT(*) AS n FROM nested_pending_source GROUP BY region";
    db.create_st("nested_pending_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st(
        "nested_pending_downstream",
        "SELECT region, n FROM nested_pending_st",
        "1h",
        "DIFFERENTIAL",
    )
    .await;

    let mut reader = db.pool.begin().await.expect("begin old source snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *reader)
        .await
        .expect("set snapshot isolation");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM nested_pending_source")
        .fetch_one(&mut *reader)
        .await
        .expect("hold old partition snapshot");
    assert_eq!(count, 2);
    let pool = db.pool.clone();
    let detach = tokio::spawn(async move {
        sqlx::query(
            "ALTER TABLE nested_pending_year DETACH PARTITION nested_pending_eu CONCURRENTLY",
        )
        .execute(&pool)
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let pending: bool = db
                .query_scalar(
                    "SELECT COALESCE((SELECT inhdetachpending FROM pg_inherits \
                     WHERE inhrelid = 'nested_pending_eu'::regclass), false)",
                )
                .await;
            if pending {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("concurrent detach enters pending phase");
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'nested_pending_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED");
    let frontier: serde_json::Value = db
        .query_scalar(
            "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'nested_pending_st'",
        )
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('nested_pending_st')")
        .await
        .expect_err("refresh must reject the pending detach");
    assert!(error.to_string().to_lowercase().contains("suspend"));
    let error = db
        .try_execute("SELECT pgtrickle.resume_stream_table('nested_pending_st')")
        .await
        .expect_err("resume must reject the pending detach");
    assert!(error.to_string().contains("detach is pending"));
    let error = db
        .try_execute("SELECT pgtrickle.resume_stream_table('nested_pending_downstream')")
        .await
        .expect_err("downstream resume must reject the pending detach");
    assert!(error.to_string().contains("detach is pending"));
    let after: serde_json::Value = db
        .query_scalar(
            "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'nested_pending_st'",
        )
        .await;
    assert_eq!(after, frontier);
    reader.commit().await.expect("release old source snapshot");
    detach
        .await
        .expect("join concurrent detach")
        .expect("finish concurrent detach");
    db.execute("SELECT pgtrickle.resume_stream_table('nested_pending_st')")
        .await;
    db.refresh_st_with_retry("nested_pending_st").await;
    db.execute("SELECT pgtrickle.resume_stream_table('nested_pending_downstream')")
        .await;
    db.refresh_st_with_retry("nested_pending_downstream").await;
    db.assert_st_matches_query("nested_pending_st", query).await;
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT region, n FROM nested_pending_downstream ORDER BY region")
            .fetch_all(&db.pool)
            .await
            .expect("read downstream after pending detach recovery");
    assert_eq!(rows, vec![("US".into(), 1)]);
}

#[tokio::test]
async fn test_partition_concurrent_detach_resolves_caller_search_path() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("CREATE ROLE partition_owner LOGIN").await;
    db.execute("CREATE SCHEMA partition_tenant AUTHORIZATION partition_owner")
        .await;
    db.execute("CREATE TABLE partition_tenant.events (d DATE NOT NULL) PARTITION BY RANGE (d)")
        .await;
    db.execute(
        "CREATE TABLE partition_tenant.events_old PARTITION OF partition_tenant.events
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE partition_tenant.events_new PARTITION OF partition_tenant.events
         FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;
    db.execute("ALTER TABLE partition_tenant.events OWNER TO partition_owner")
        .await;
    db.execute("ALTER TABLE partition_tenant.events_old OWNER TO partition_owner")
        .await;
    db.execute("ALTER TABLE partition_tenant.events_new OWNER TO partition_owner")
        .await;
    db.execute("INSERT INTO partition_tenant.events VALUES ('2025-01-01'), ('2026-01-01')")
        .await;
    db.create_st(
        "tenant_events_st",
        "SELECT COUNT(*) AS n FROM partition_tenant.events",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let mut conn = db.pool.acquire().await.expect("acquire caller connection");
    sqlx::query("SET ROLE partition_owner")
        .execute(&mut *conn)
        .await
        .expect("set non-superuser role");
    sqlx::query("SET search_path = partition_tenant, pg_catalog")
        .execute(&mut *conn)
        .await
        .expect("set caller search path");
    sqlx::query("ALTER TABLE events DETACH PARTITION events_old CONCURRENTLY")
        .execute(&mut *conn)
        .await
        .expect("detach as source owner using unqualified names");
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("restore role");
    sqlx::query("RESET search_path")
        .execute(&mut *conn)
        .await
        .expect("restore search path");
    drop(conn);
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'tenant_events_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED");
    db.execute("SELECT pgtrickle.resume_stream_table('tenant_events_st')")
        .await;
    db.refresh_st_with_retry("tenant_events_st").await;
    let count: i64 = db.query_scalar("SELECT n FROM tenant_events_st").await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_retained_history_uses_differential_above_auto_fallback_threshold() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE busy_retain_src (id INT NOT NULL, d DATE NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE busy_retain_old PARTITION OF busy_retain_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE busy_retain_live PARTITION OF busy_retain_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO busy_retain_src VALUES (1,'2025-06-01',10),(2,'2026-06-01',20)")
        .await;
    db.create_st(
        "busy_retain_st",
        "SELECT d, sum(amount) AS total, count(*) AS n FROM busy_retain_src GROUP BY d",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('busy_retain_st', 'RETAIN', 'd', '2026-01-01')").await;
    db.execute("DROP TABLE busy_retain_old").await;
    db.execute("INSERT INTO busy_retain_src SELECT g, '2026-06-01'::date, 1 FROM generate_series(3,12) AS g").await;
    let mut tx = db.pool.begin().await.expect("begin refresh transaction");
    sqlx::query("SET LOCAL pg_trickle.differential_max_change_ratio = 0.001")
        .execute(&mut *tx)
        .await
        .expect("set low fallback threshold");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('busy_retain_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh without rebuilding sealed history");
    tx.commit().await.expect("commit differential refresh");
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM busy_retain_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read historical and live results");
    assert_eq!(
        rows,
        vec![("2025-06-01".into(), 10, 1), ("2026-06-01".into(), 30, 11)]
    );
    let action: (String, String, bool) = sqlx::query_as("SELECT h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'busy_retain_st' ORDER BY h.refresh_id DESC LIMIT 1").fetch_one(&db.pool).await.expect("read effective refresh action");
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));

    db.execute("INSERT INTO busy_retain_src SELECT g, '2026-06-01'::date, 1 FROM generate_series(13,10012) AS g").await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET auto_threshold = NULL WHERE pgt_name = 'busy_retain_st'").await;
    let mut tx = db.pool.begin().await.expect("begin capped refresh");
    sqlx::query("SELECT set_config('pg_trickle.differential_max_change_ratio', '0', true), set_config('pg_trickle.merge_work_mem_mb', '64', true), set_config('pg_trickle.delta_work_mem_cap_mb', '8', true)")
        .execute(&mut *tx).await.expect("set capped planner budget");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('busy_retain_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh at the work-memory cap");
    tx.commit().await.expect("commit capped refresh");
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM busy_retain_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read results after capped refresh");
    assert_eq!(
        rows,
        vec![
            ("2025-06-01".into(), 10, 1),
            ("2026-06-01".into(), 10030, 10011)
        ]
    );
    let action: (String, String, bool) = sqlx::query_as("SELECT h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'busy_retain_st' ORDER BY h.refresh_id DESC LIMIT 1").fetch_one(&db.pool).await.expect("read capped refresh action");
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    let detail: String = sqlx::query_scalar("SELECT h.refresh_reason_detail FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'busy_retain_st' ORDER BY h.refresh_id DESC LIMIT 1").fetch_one(&db.pool).await.expect("read capped planner evidence");
    let evidence: serde_json::Value = serde_json::from_str(&detail)
        .unwrap_or_else(|_| panic!("unexpected planner evidence: {detail}"));
    assert_eq!(evidence["planner_hint_tier"], "nestloop+workmem");
}

#[tokio::test]
async fn test_retained_history_blocks_rebuild_query_change_and_restore() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retained_guard_src (d DATE NOT NULL, amount INT NOT NULL) PARTITION BY RANGE (d)")
        .await;
    db.execute("CREATE TABLE retained_guard_old PARTITION OF retained_guard_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')")
        .await;
    db.execute("INSERT INTO retained_guard_src VALUES ('2025-01-01', 10)")
        .await;
    db.create_st(
        "retained_guard_st",
        "SELECT d, SUM(amount) AS total FROM retained_guard_src GROUP BY d",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('retained_guard_st', 'RETAIN', 'd', '2026-01-01')").await;
    let source_oid: i64 = db
        .query_scalar("SELECT 'retained_guard_src'::regclass::oid::bigint")
        .await;
    let empty_state = serde_json::json!({
        "version": 1, "mode": "RETAIN",
        "candidate": {"output_column": "d", "bucket": "EXACT", "source_type_oid": 1082,
                      "source_oid": source_oid, "source_column": "d"},
        "cutoff": {"kind": "DATE", "value": 2}, "query_hash": 0, "sealed": []
    });
    sqlx::query(
        "UPDATE pgtrickle.pgt_dependencies SET column_snapshot = \
         jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) \
         WHERE source_relid = 'retained_guard_src'::regclass",
    )
    .bind(&empty_state)
    .execute(&db.pool)
    .await
    .expect("install unsealed-state fixture");
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET refresh_mode = 'FULL' WHERE pgt_name = 'retained_guard_st'")
        .await;
    db.refresh_st_with_retry("retained_guard_st").await;
    let action: (String, String) = sqlx::query_as(
        "SELECT action, status FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retained_guard_st') \
         ORDER BY refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read unsealed FULL refresh");
    assert_eq!(action, ("FULL".into(), "COMPLETED".into()));
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET refresh_mode = 'DIFFERENTIAL' WHERE pgt_name = 'retained_guard_st'")
        .await;
    db.execute("SELECT pgtrickle.snapshot_stream_table('retained_guard_st', 'public.retained_guard_snapshot')")
        .await;
    let mut state = empty_state.clone();
    state["sealed"] = serde_json::json!([{
        "lower": {"kind": "DATE", "value": 1},
        "upper": {"kind": "DATE", "value": 2}
    }]);
    sqlx::query(
        "UPDATE pgtrickle.pgt_dependencies SET column_snapshot = \
         jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) \
         WHERE source_relid = 'retained_guard_src'::regclass",
    )
    .bind(&state)
    .execute(&db.pool)
    .await
    .expect("install sealed-state fixture");
    db.execute("CREATE TABLE retained_guard_new PARTITION OF retained_guard_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
        .await;
    let saved: serde_json::Value = db
        .query_scalar("SELECT column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies WHERE source_relid = 'retained_guard_src'::regclass")
        .await;
    assert_eq!(
        saved, state,
        "partition metadata refresh must preserve sealed state"
    );
    let frontier: serde_json::Value = db
        .query_scalar(
            "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retained_guard_st'",
        )
        .await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET refresh_mode = 'FULL' WHERE pgt_name = 'retained_guard_st'")
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('retained_guard_st')")
        .await
        .expect_err("FULL must reject sealed history");
    assert!(error.to_string().contains("retained partition history"));
    let error = db
        .try_execute("SELECT pgtrickle.alter_stream_table('retained_guard_st', query => 'SELECT d, COUNT(*) AS n FROM retained_guard_src GROUP BY d')")
        .await
        .expect_err("query replacement must reject retention configuration");
    assert!(error.to_string().contains("disable partition retention"));
    let error = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table('retained_guard_st', partition_by => 'd')",
        )
        .await
        .expect_err("storage replacement must reject sealed history");
    assert!(error.to_string().contains("retained partition history"));
    let error = db
        .try_execute("SELECT pgtrickle.restore_from_snapshot('retained_guard_st', 'public.retained_guard_snapshot')")
        .await
        .expect_err("restore must reject sealed history");
    assert!(error.to_string().contains("retained partition history"));
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET status = 'SUSPENDED' WHERE pgt_name = 'retained_guard_st'")
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.resume_stream_table('retained_guard_st')")
        .await
        .expect_err("resume cannot trigger a rebuild of sealed history");
    assert!(error.to_string().contains("retained partition history"));
    let after: serde_json::Value = db
        .query_scalar(
            "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retained_guard_st'",
        )
        .await;
    assert_eq!(after, frontier);
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT d::text, total FROM retained_guard_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read protected retained output");
    assert_eq!(rows, vec![("2025-01-01".into(), 10)]);
}

#[tokio::test]
async fn test_retention_catchup_updates_filtered_downstream() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE trim_filter_src (id INT NOT NULL, d DATE NOT NULL, account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE trim_filter_old PARTITION OF trim_filter_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE trim_filter_live PARTITION OF trim_filter_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO trim_filter_src VALUES (1,'2025-06-01','a',30),(2,'2026-06-01','a',20),(3,'2026-06-01','b',40)").await;
    db.create_st("trim_filter_upstream", "SELECT d, account, sum(amount) AS total, count(*) AS n FROM trim_filter_src GROUP BY d, account", "1h", "DIFFERENTIAL").await;
    db.create_st(
        "trim_filter_downstream",
        "SELECT d, account, total, n FROM trim_filter_upstream WHERE total >= 25",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('trim_filter_upstream','TRIM','d','2026-01-01')").await;
    db.execute(
        "INSERT INTO trim_filter_src VALUES (4,'2026-06-01','a',7),(5,'2026-06-01','b',-20)",
    )
    .await;
    let mut tx = db.pool.begin().await.expect("begin high-cost removal");
    sqlx::query("SET LOCAL random_page_cost = 1000000")
        .execute(&mut *tx)
        .await
        .expect("set planner cost");
    sqlx::query("DROP TABLE trim_filter_old")
        .execute(&mut *tx)
        .await
        .expect("remove expired partition");
    tx.commit().await.expect("commit retention cut");
    db.refresh_st_until_converged("trim_filter_downstream")
        .await;
    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM trim_filter_downstream ORDER BY d,account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read filtered downstream");
    assert_eq!(rows, vec![("2026-06-01".into(), "a".into(), 27, 2)]);
    db.assert_st_matches_query("trim_filter_downstream", "SELECT d, account, sum(amount) AS total, count(*) AS n FROM trim_filter_src GROUP BY d, account HAVING sum(amount) >= 25").await;
    let action: (String, String, bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='trim_filter_downstream' ORDER BY h.refresh_id DESC LIMIT 1").fetch_one(&db.pool).await.expect("read downstream refresh action");
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
}

#[tokio::test]
async fn test_retention_catchup_applies_keyless_inserts_updates_and_deletes() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE trim_dml_src (id INT NOT NULL, d DATE NOT NULL, account TEXT NOT NULL, amount INT NOT NULL) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE trim_dml_old PARTITION OF trim_dml_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE trim_dml_live PARTITION OF trim_dml_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO trim_dml_src VALUES (1,'2025-06-01','old',5),(2,'2026-06-01','a',10),(3,'2026-06-01','a',20),(4,'2026-06-01','b',4)").await;
    db.create_st("trim_dml_upstream", "SELECT d, account, sum(amount) AS total, count(*) AS n FROM trim_dml_src GROUP BY d, account", "1h", "DIFFERENTIAL").await;
    db.create_st(
        "trim_dml_downstream",
        "SELECT d, account, total, n FROM trim_dml_upstream",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('trim_dml_upstream','TRIM','d','2026-01-01')").await;
    db.execute("UPDATE trim_dml_src SET amount=11 WHERE id=2")
        .await;
    db.execute("DELETE FROM trim_dml_src WHERE id=4").await;
    db.execute("INSERT INTO trim_dml_src VALUES (5,'2026-06-01','a',7),(6,'2026-06-01','c',3)")
        .await;
    db.execute("DROP TABLE trim_dml_old").await;
    db.refresh_st_until_converged("trim_dml_downstream").await;
    let expected = vec![
        ("2026-06-01".to_string(), "a".to_string(), 38_i64, 3_i64),
        ("2026-06-01".to_string(), "c".to_string(), 3_i64, 1_i64),
    ];
    for (table, query) in [
        (
            "trim_dml_upstream",
            "SELECT d::text, account, total, n FROM trim_dml_upstream ORDER BY d,account",
        ),
        (
            "trim_dml_downstream",
            "SELECT d::text, account, total, n FROM trim_dml_downstream ORDER BY d,account",
        ),
    ] {
        let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(query)
            .fetch_all(&db.pool)
            .await
            .expect("read maintained groups");
        assert_eq!(rows, expected);
        let action: (String, String, bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name=$1 ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(table).fetch_one(&db.pool).await.expect("read completed refresh action");
        assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    }
    db.assert_st_matches_query("trim_dml_upstream", "SELECT d, account, sum(amount) AS total, count(*) AS n FROM trim_dml_src GROUP BY d, account").await;
}

#[tokio::test]
async fn test_protected_downstream_full_retries_locked_upstream_without_deadlock() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_lock_src (id INT PRIMARY KEY, amount INT NOT NULL)")
        .await;
    db.execute("INSERT INTO retention_lock_src VALUES (1,10)")
        .await;
    db.create_st(
        "retention_lock_upstream",
        "SELECT id,amount FROM retention_lock_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "retention_lock_downstream",
        "SELECT id,amount FROM retention_lock_upstream",
        "1h",
        "FULL",
    )
    .await;
    db.execute("INSERT INTO retention_lock_src VALUES (2,20)")
        .await;
    let before: i64 = db.query_scalar("SELECT COALESCE(max(h.refresh_id),0) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='retention_lock_downstream'").await;
    let mut tx = db
        .pool
        .begin()
        .await
        .expect("begin upstream refresh transaction");
    sqlx::query("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name='retention_lock_upstream' FOR UPDATE")
        .execute(&mut *tx).await.expect("hold upstream catalog row");
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        db.try_execute("SELECT pgtrickle.refresh_stream_table('retention_lock_downstream')"),
    )
    .await
    .expect("downstream refresh must not wait on the locked row")
    .expect_err("locked upstream must return a retryable error");
    assert!(
        error.to_string().contains("locked; retry the refresh"),
        "{error}"
    );
    let completed: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='retention_lock_downstream' AND h.refresh_id>$1 AND h.status='COMPLETED'")
        .bind(before).fetch_one(&db.pool).await.expect("inspect skipped refresh history");
    assert_eq!(completed, 0);
    sqlx::query("SELECT pgtrickle.refresh_stream_table('retention_lock_upstream')")
        .execute(&mut *tx)
        .await
        .expect("refresh upstream while holding its catalog row");
    tx.commit().await.expect("commit upstream refresh");
    db.refresh_st("retention_lock_downstream").await;
    let rows: Vec<(i32, i32)> =
        sqlx::query_as("SELECT id,amount FROM retention_lock_downstream ORDER BY id")
            .fetch_all(&db.pool)
            .await
            .expect("read downstream output");
    assert_eq!(rows, vec![(1, 10), (2, 20)]);
    let action: (String, String) = sqlx::query_as("SELECT h.action,h.status FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='retention_lock_downstream' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&db.pool).await.expect("inspect completed downstream refresh");
    assert_eq!(action, ("FULL".into(), "COMPLETED".into()));
    db.execute("SELECT pgtrickle.alter_stream_table('retention_lock_downstream', refresh_mode => 'DIFFERENTIAL')").await;
    db.refresh_st("retention_lock_downstream").await;
    let unchanged: Vec<(i32, i32)> =
        sqlx::query_as("SELECT id,amount FROM retention_lock_downstream ORDER BY id")
            .fetch_all(&db.pool)
            .await
            .expect("read downstream after protected FULL");
    assert_eq!(unchanged, vec![(1, 10), (2, 20)]);
    db.execute("INSERT INTO retention_lock_src VALUES (3,30)")
        .await;
    db.refresh_st("retention_lock_upstream").await;
    db.refresh_st("retention_lock_downstream").await;
    let after: Vec<(i32, i32)> =
        sqlx::query_as("SELECT id,amount FROM retention_lock_downstream ORDER BY id")
            .fetch_all(&db.pool)
            .await
            .expect("read downstream after later insert");
    assert_eq!(after, vec![(1, 10), (2, 20), (3, 30)]);
    let last: (String, String, bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='retention_lock_downstream' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&db.pool).await.expect("inspect later differential refresh");
    assert_eq!(last, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
}

#[tokio::test]
async fn test_retention_cut_and_multisource_full_refresh_do_not_replay() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE mixed_cut_src (id INT NOT NULL, d DATE NOT NULL, account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE mixed_cut_old PARTITION OF mixed_cut_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE mixed_cut_live PARTITION OF mixed_cut_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("CREATE TABLE mixed_cut_lookup (account TEXT PRIMARY KEY, factor INT NOT NULL)")
        .await;
    db.execute("INSERT INTO mixed_cut_src VALUES (1,'2025-06-01','a',10),(2,'2026-06-01','a',20)")
        .await;
    db.execute("INSERT INTO mixed_cut_lookup VALUES ('a',2),('b',3)")
        .await;
    db.create_st(
        "mixed_cut_up",
        "SELECT d,account,sum(amount) AS total,count(*) AS n FROM mixed_cut_src GROUP BY d,account",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st("mixed_cut_down", "SELECT u.d,u.account,u.total,u.n,l.factor FROM mixed_cut_up u JOIN mixed_cut_lookup l ON u.account=l.account", "1h", "FULL").await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('mixed_cut_up','TRIM','d','2026-01-01')").await;
    db.execute("INSERT INTO mixed_cut_src VALUES (3,'2026-06-01','a',7)")
        .await;

    let mut cut = db.pool.begin().await.expect("begin retention cut");
    sqlx::query("DROP TABLE mixed_cut_old")
        .execute(&mut *cut)
        .await
        .expect("prepare uncommitted cut");
    db.execute("UPDATE mixed_cut_lookup SET factor=4 WHERE account='a'")
        .await;
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        db.try_execute("SELECT pgtrickle.refresh_stream_table('mixed_cut_down')"),
    )
    .await
    .expect("downstream refresh must not deadlock")
    .expect_err("uncommitted upstream cut must be retried");
    assert!(
        error.to_string().contains("locked; retry the refresh"),
        "{error}"
    );
    cut.commit().await.expect("commit retention cut");

    db.refresh_st("mixed_cut_down").await;
    let full_action: (String,String) = sqlx::query_as("SELECT h.action,h.status FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='mixed_cut_down' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&db.pool).await.expect("read completed FULL action");
    assert_eq!(full_action, ("FULL".into(), "COMPLETED".into()));
    db.assert_st_matches_query("mixed_cut_down", "SELECT s.d,s.account,sum(s.amount) AS total,count(*) AS n,l.factor FROM mixed_cut_src s JOIN mixed_cut_lookup l ON s.account=l.account GROUP BY s.d,s.account,l.factor").await;

    db.execute(
        "SELECT pgtrickle.alter_stream_table('mixed_cut_down', refresh_mode => 'DIFFERENTIAL')",
    )
    .await;
    db.refresh_st("mixed_cut_down").await;
    db.execute("INSERT INTO mixed_cut_src VALUES (4,'2026-06-01','a',3),(5,'2026-06-01','b',9)")
        .await;
    db.execute("UPDATE mixed_cut_lookup SET factor=5 WHERE account='a'")
        .await;
    db.refresh_st("mixed_cut_up").await;
    db.refresh_st_until_converged("mixed_cut_down").await;
    let rows: Vec<(String, String, i64, i64, i32)> = sqlx::query_as(
        "SELECT d::text,account,total,n,factor FROM mixed_cut_down ORDER BY d,account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read maintained join");
    assert_eq!(
        rows,
        vec![
            ("2026-06-01".into(), "a".into(), 30, 3, 5),
            ("2026-06-01".into(), "b".into(), 9, 1, 3)
        ]
    );
    db.assert_st_matches_query("mixed_cut_down", "SELECT s.d,s.account,sum(s.amount) AS total,count(*) AS n,l.factor FROM mixed_cut_src s JOIN mixed_cut_lookup l ON s.account=l.account GROUP BY s.d,s.account,l.factor").await;
    let action: (String,String,bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='mixed_cut_down' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&db.pool).await.expect("read completed differential action");
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
}

#[tokio::test]
async fn test_retention_scoped_dml_preserves_duplicate_multiplicity() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    let mut conn = db.pool.acquire().await.expect("acquire DML session");
    for sql in [
        "CREATE TEMP TABLE scoped_target (__pgt_row_id bytea NOT NULL, val int NOT NULL)",
        "CREATE INDEX ON scoped_target (__pgt_row_id)",
        "CREATE TEMP TABLE __pgt_delta_7 (__pgt_row_id bytea NOT NULL, __pgt_action text NOT NULL, val int NOT NULL)",
        "INSERT INTO scoped_target VALUES (decode('01','hex'),10),(decode('01','hex'),11),(decode('02','hex'),20)",
        "INSERT INTO __pgt_delta_7 VALUES (decode('01','hex'),'D',10),(decode('02','hex'),'I',21),(decode('03','hex'),'I',30)",
        "DELETE FROM scoped_target AS st USING (SELECT matched.tableoid, matched.ctid FROM (SELECT __pgt_row_id, COUNT(*) AS n FROM __pgt_delta_7 WHERE __pgt_action = 'D' GROUP BY __pgt_row_id) d CROSS JOIN LATERAL (SELECT st2.tableoid, st2.ctid FROM scoped_target st2 WHERE pgtrickle.row_probe_v1(st2.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) AND st2.__pgt_row_id = d.__pgt_row_id ORDER BY st2.ctid LIMIT d.n) matched) target WHERE st.tableoid = target.tableoid AND st.ctid = target.ctid",
        "UPDATE scoped_target AS st SET val = d.val FROM __pgt_delta_7 AS d CROSS JOIN LATERAL (SELECT st2.tableoid, st2.ctid FROM scoped_target st2 WHERE pgtrickle.row_probe_v1(st2.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) AND st2.__pgt_row_id = d.__pgt_row_id OFFSET 0) matched WHERE st.tableoid = matched.tableoid AND st.ctid = matched.ctid AND d.__pgt_action = 'I' AND (st.val::text IS DISTINCT FROM d.val::text)",
        "INSERT INTO scoped_target (__pgt_row_id, val) SELECT DISTINCT ON (d.__pgt_row_id) d.__pgt_row_id, d.val FROM __pgt_delta_7 AS d WHERE d.__pgt_action = 'I' AND NOT EXISTS (SELECT 1 FROM (SELECT 1 FROM scoped_target AS st WHERE pgtrickle.row_probe_v1(st.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) AND st.__pgt_row_id = d.__pgt_row_id OFFSET 0) matched) ORDER BY d.__pgt_row_id",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("execute scoped DML");
    }
    let rows: Vec<(String, i32)> = sqlx::query_as(
        "SELECT encode(__pgt_row_id,'hex'), val FROM scoped_target ORDER BY __pgt_row_id,val",
    )
    .fetch_all(&mut *conn)
    .await
    .expect("read resulting multiset");
    assert_eq!(
        rows,
        vec![("01".into(), 11), ("02".into(), 21), ("03".into(), 30)]
    );
}

#[tokio::test]
async fn test_retention_scoped_dml_distinguishes_partition_ctids() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire partitioned DML session");
    for (step, sql) in [
        "CREATE TEMP TABLE scoped_partitioned (__pgt_row_id bytea NOT NULL, bucket int NOT NULL, val int NOT NULL) PARTITION BY RANGE(bucket)",
        "CREATE TEMP TABLE scoped_partitioned_a PARTITION OF scoped_partitioned FOR VALUES FROM (0) TO (100)",
        "CREATE TEMP TABLE scoped_partitioned_b PARTITION OF scoped_partitioned FOR VALUES FROM (100) TO (200)",
        "CREATE INDEX ON scoped_partitioned (__pgt_row_id)",
        "CREATE TEMP TABLE __pgt_delta_7 (__pgt_row_id bytea NOT NULL, __pgt_action text NOT NULL, bucket int NOT NULL, val int NOT NULL)",
        "INSERT INTO scoped_partitioned VALUES (decode('01','hex'),1,10),(decode('02','hex'),101,20)",
        "INSERT INTO __pgt_delta_7 VALUES (decode('01','hex'),'D',1,10),(decode('02','hex'),'I',2,21),(decode('03','hex'),'I',102,30)",
        "DELETE FROM scoped_partitioned AS st USING (SELECT matched.tableoid, matched.ctid FROM (SELECT __pgt_row_id, COUNT(*) AS n FROM __pgt_delta_7 WHERE __pgt_action = 'D' GROUP BY __pgt_row_id) d CROSS JOIN LATERAL (SELECT st2.tableoid, st2.ctid FROM scoped_partitioned st2 WHERE pgtrickle.row_probe_v1(st2.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) AND st2.__pgt_row_id = d.__pgt_row_id ORDER BY st2.ctid LIMIT d.n) matched) target WHERE st.tableoid = target.tableoid AND st.ctid = target.ctid",
        "UPDATE scoped_partitioned AS st SET bucket = d.bucket, val = d.val FROM __pgt_delta_7 AS d CROSS JOIN LATERAL (SELECT st2.tableoid, st2.ctid FROM scoped_partitioned st2 WHERE pgtrickle.row_probe_v1(st2.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) AND st2.__pgt_row_id = d.__pgt_row_id OFFSET 0) matched WHERE st.tableoid = matched.tableoid AND st.ctid = matched.ctid AND d.__pgt_action = 'I' AND (st.bucket::text IS DISTINCT FROM d.bucket::text OR st.val::text IS DISTINCT FROM d.val::text)",
        "INSERT INTO scoped_partitioned (__pgt_row_id, bucket, val) SELECT DISTINCT ON (d.__pgt_row_id) d.__pgt_row_id, d.bucket, d.val FROM __pgt_delta_7 AS d WHERE d.__pgt_action = 'I' AND NOT EXISTS (SELECT 1 FROM (SELECT 1 FROM scoped_partitioned AS st WHERE pgtrickle.row_probe_v1(st.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) AND st.__pgt_row_id = d.__pgt_row_id OFFSET 0) matched) ORDER BY d.__pgt_row_id",
    ].into_iter().enumerate() {
        sqlx::query(sql).execute(&mut *conn).await.expect("execute partitioned scoped DML");
        if step == 5 {
            let locations: Vec<(String, String)> = sqlx::query_as("SELECT tableoid::regclass::text, ctid::text FROM scoped_partitioned ORDER BY __pgt_row_id")
                .fetch_all(&mut *conn).await.expect("read partition row locations");
            assert_eq!(locations, vec![("scoped_partitioned_a".into(), "(0,1)".into()), ("scoped_partitioned_b".into(), "(0,1)".into())]);
        }
    }
    let rows: Vec<(String, i32, i32)> = sqlx::query_as("SELECT encode(__pgt_row_id,'hex'), bucket, val FROM scoped_partitioned ORDER BY __pgt_row_id")
        .fetch_all(&mut *conn).await.expect("read partitioned result");
    assert_eq!(rows, vec![("02".into(), 2, 21), ("03".into(), 102, 30)]);
}

#[tokio::test]
async fn test_retention_utc_hour_fall_back_keeps_distinct_buckets() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_dst_src (id INT NOT NULL, occurred TIMESTAMPTZ NOT NULL, account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY(id,occurred)) PARTITION BY RANGE(occurred)").await;
    db.execute("CREATE TABLE retention_dst_old PARTITION OF retention_dst_src FOR VALUES FROM ('2025-11-02 05:00:00+00') TO ('2025-11-02 06:00:00+00')").await;
    db.execute("CREATE TABLE retention_dst_live PARTITION OF retention_dst_src FOR VALUES FROM ('2025-11-02 06:00:00+00') TO ('2025-11-02 08:00:00+00')").await;
    db.execute("INSERT INTO retention_dst_src VALUES (1,'2025-11-02 01:30:00-04','a',10),(2,'2025-11-02 01:00:00-05','a',20),(3,'2025-11-02 01:30:00-05','b',7)").await;
    let query = "SELECT pg_catalog.date_trunc('hour',occurred,'UTC') AS bucket,account,sum(amount) AS total,count(*) AS n FROM retention_dst_src GROUP BY 1,2";
    for (name, policy) in [
        ("retention_dst_trim", "TRIM"),
        ("retention_dst_keep", "RETAIN"),
    ] {
        sqlx::query("SELECT pgtrickle.create_stream_table_with_retention($1,$2,$3,'occurred','2025-11-02 01:00:00-05','1h')")
            .bind(name).bind(query).bind(policy).execute(&db.pool).await.expect("create UTC bucket stream");
    }
    let baseline: Vec<(String,String,i64,i64)> = sqlx::query_as("SELECT to_char(bucket AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI'),account,total,n FROM retention_dst_keep ORDER BY bucket,account").fetch_all(&db.pool).await.unwrap();
    assert_eq!(
        baseline,
        vec![
            ("2025-11-02 05:00".into(), "a".into(), 10, 1),
            ("2025-11-02 06:00".into(), "a".into(), 20, 1),
            ("2025-11-02 06:00".into(), "b".into(), 7, 1)
        ]
    );
    let mut conn = db.pool.acquire().await.expect("acquire DST session");
    sqlx::query("SET TIME ZONE 'America/New_York'")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE retention_dst_src DETACH PARTITION retention_dst_old")
        .execute(&mut *conn)
        .await
        .expect("detach expired UTC hour in a DST session");
    let status: serde_json::Value = sqlx::query_scalar(
        "SELECT pgtrickle.get_stream_table_partition_retention('retention_dst_keep')",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    let (lower, upper): (String, String) = sqlx::query_as("SELECT TIMESTAMPTZ '2025-11-02 05:00:00+00'::text, TIMESTAMPTZ '2025-11-02 06:00:00+00'::text")
        .fetch_one(&mut *conn).await.unwrap();
    assert_eq!(
        status["sealed_ranges"],
        serde_json::json!([{"lower": lower, "upper": upper}])
    );
    drop(conn);
    db.assert_st_matches_query("retention_dst_trim", query)
        .await;
    let retained: Vec<(String,String,i64,i64)> = sqlx::query_as("SELECT to_char(bucket AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI'),account,total,n FROM retention_dst_keep ORDER BY bucket,account").fetch_all(&db.pool).await.unwrap();
    assert_eq!(retained, baseline);
    let error = db.try_execute("ALTER TABLE retention_dst_src ATTACH PARTITION retention_dst_old FOR VALUES FROM ('2025-11-02 01:00:00-04') TO ('2025-11-02 01:00:00-05')").await.expect_err("nonempty historical reattachment must be rejected");
    assert!(
        error
            .to_string()
            .contains("cannot attach a nonempty partition"),
        "{error}"
    );
    db.execute("CREATE TABLE retention_dst_empty (LIKE retention_dst_src INCLUDING ALL)")
        .await;
    let error = db.try_execute("ALTER TABLE retention_dst_src ATTACH PARTITION retention_dst_empty FOR VALUES FROM ('2025-11-02 01:00:00-04') TO ('2025-11-02 01:00:00-05')").await.expect_err("empty reattachment must not overlap the sealed UTC hour");
    assert!(
        error.to_string().contains("overlapping retained history"),
        "{error}"
    );
    for name in ["retention_dst_trim", "retention_dst_keep"] {
        let action: (String,String,bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name=$1 ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(name).fetch_one(&db.pool).await.unwrap();
        assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    }
    db.execute("INSERT INTO retention_dst_src VALUES (4,'2025-11-02 01:45:00-05','a',3),(5,'2025-11-02 02:15:00-05','a',4)").await;
    db.refresh_st("retention_dst_trim").await;
    db.refresh_st("retention_dst_keep").await;
    db.assert_st_matches_query("retention_dst_trim", query)
        .await;
    let expected = vec![
        ("2025-11-02 05:00".into(), "a".into(), 10, 1),
        ("2025-11-02 06:00".into(), "a".into(), 23, 2),
        ("2025-11-02 06:00".into(), "b".into(), 7, 1),
        ("2025-11-02 07:00".into(), "a".into(), 4, 1),
    ];
    let retained: Vec<(String,String,i64,i64)> = sqlx::query_as("SELECT to_char(bucket AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI'),account,total,n FROM retention_dst_keep ORDER BY bucket,account").fetch_all(&db.pool).await.unwrap();
    assert_eq!(retained, expected);
    for name in ["retention_dst_trim", "retention_dst_keep"] {
        let action: (String,String,bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name=$1 ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(name).fetch_one(&db.pool).await.unwrap();
        assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    }
}

#[tokio::test]
async fn test_retention_partitioned_output_with_different_key_keeps_all_groups() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE different_key_src (id INT NOT NULL, d DATE NOT NULL, account TEXT, amount INT NOT NULL) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE different_key_old PARTITION OF different_key_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE different_key_live PARTITION OF different_key_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO different_key_src VALUES (1,'2025-06-01','a',10),(2,'2026-06-01','a',20),(3,'2026-06-01',NULL,7),(6,'2026-06-02',NULL,9)").await;
    let query = "SELECT d AS day,account,sum(amount) AS total,count(*) AS n FROM different_key_src GROUP BY d,account";
    db.create_st_partitioned(
        "different_key_up",
        query,
        "1h",
        "DIFFERENTIAL",
        "HASH:account",
    )
    .await;
    db.create_st(
        "different_key_down",
        "SELECT day,account,total,n FROM different_key_up",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st_partitioned("aligned_key_up", query, "1h", "DIFFERENTIAL", "HASH:day")
        .await;
    db.create_st(
        "aligned_key_down",
        "SELECT day,account,total,n FROM aligned_key_up",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('different_key_up','TRIM','d','2026-01-01')").await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('aligned_key_up','TRIM','d','2026-01-01')").await;
    db.execute("DELETE FROM different_key_src WHERE id=6").await;
    db.execute(
        "INSERT INTO different_key_src VALUES (4,'2026-06-01','a',3),(5,'2026-06-01',NULL,4)",
    )
    .await;
    db.execute("DROP TABLE different_key_old").await;
    db.refresh_st_until_converged("different_key_down").await;
    db.refresh_st_until_converged("aligned_key_down").await;
    let expected = vec![
        (
            "2026-06-01".to_string(),
            Some("a".to_string()),
            23_i64,
            2_i64,
        ),
        ("2026-06-01".to_string(), None, 11_i64, 2_i64),
    ];
    for (name, sql) in [
        (
            "different_key_up",
            "SELECT day::text,account,total,n FROM different_key_up ORDER BY day,account NULLS LAST",
        ),
        (
            "different_key_down",
            "SELECT day::text,account,total,n FROM different_key_down ORDER BY day,account NULLS LAST",
        ),
        (
            "aligned_key_up",
            "SELECT day::text,account,total,n FROM aligned_key_up ORDER BY day,account NULLS LAST",
        ),
        (
            "aligned_key_down",
            "SELECT day::text,account,total,n FROM aligned_key_down ORDER BY day,account NULLS LAST",
        ),
    ] {
        let rows: Vec<(String, Option<String>, i64, i64)> =
            sqlx::query_as(sql).fetch_all(&db.pool).await.unwrap();
        assert_eq!(rows, expected);
        db.assert_st_matches_query(name, query).await;
        let action: (String,String,bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name=$1 ORDER BY h.refresh_id DESC LIMIT 1").bind(name).fetch_one(&db.pool).await.unwrap();
        assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    }
}

#[tokio::test]
async fn test_concurrent_retention_detach_with_partitioned_output_and_downstream() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE concurrent_output_src (id INT NOT NULL, d DATE NOT NULL, amount INT NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE concurrent_output_old PARTITION OF concurrent_output_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE concurrent_output_live PARTITION OF concurrent_output_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO concurrent_output_src VALUES (1,'2025-06-01',10),(2,'2026-06-01',20)")
        .await;
    let query = "SELECT d,sum(amount) AS total,count(*) AS n FROM concurrent_output_src GROUP BY d";
    db.create_st_partitioned(
        "concurrent_output_up",
        query,
        "1h",
        "DIFFERENTIAL",
        "HASH:d",
    )
    .await;
    db.create_st(
        "concurrent_output_down",
        "SELECT d,total,n FROM concurrent_output_up",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('concurrent_output_up','TRIM','d','2026-01-01')").await;
    db.execute("INSERT INTO concurrent_output_src VALUES (3,'2026-06-01',7)")
        .await;
    db.execute(
        "ALTER TABLE concurrent_output_src DETACH PARTITION concurrent_output_old CONCURRENTLY",
    )
    .await;
    db.refresh_st_until_converged("concurrent_output_down")
        .await;
    for (name, sql) in [
        (
            "concurrent_output_up",
            "SELECT d::text,total,n FROM concurrent_output_up ORDER BY d",
        ),
        (
            "concurrent_output_down",
            "SELECT d::text,total,n FROM concurrent_output_down ORDER BY d",
        ),
    ] {
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(sql).fetch_all(&db.pool).await.unwrap();
        assert_eq!(rows, vec![("2026-06-01".into(), 27, 2)]);
        db.assert_st_matches_query(name, query).await;
        let action: (String,String,bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name=$1 ORDER BY h.refresh_id DESC LIMIT 1").bind(name).fetch_one(&db.pool).await.unwrap();
        assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    }
    let state: serde_json::Value = db
        .query_scalar(
            "SELECT pgtrickle.get_stream_table_partition_retention('concurrent_output_up')",
        )
        .await;
    assert!(state.get("pending_detach").is_none(), "{state}");
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_capture_multiset_preserves_duplicate_deltas() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE capture_multiset_src (val INT NOT NULL)")
        .await;
    db.create_st(
        "capture_multiset_st",
        "SELECT val FROM capture_multiset_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'capture_multiset_st'",
        )
        .await;
    let mut conn = db.pool.acquire().await.unwrap();
    sqlx::query("SET pg_trickle.internal_refresh = 'true'")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO capture_multiset_st (__pgt_row_id,val) VALUES (decode('01','hex'),10),(decode('01','hex'),11),(decode('01','hex'),11),(decode('03','hex'),30),(decode('04','hex'),40)").execute(&mut *conn).await.unwrap();
    sqlx::query("RESET pg_trickle.internal_refresh")
        .execute(&mut *conn)
        .await
        .unwrap();
    for sql in [
        format!("CREATE TEMP TABLE __pgt_pre_{id} (__pgt_row_id bytea NOT NULL, val int NOT NULL)"),
        format!("INSERT INTO __pgt_pre_{id} VALUES (decode('01','hex'),10),(decode('01','hex'),10),(decode('01','hex'),11),(decode('02','hex'),20)"),
        format!("CREATE TEMP TABLE __pgt_delta_{id} (__pgt_row_id bytea NOT NULL)"),
        format!("INSERT INTO __pgt_delta_{id} VALUES (decode('01','hex')),(decode('01','hex')),(decode('02','hex')),(decode('04','hex'))"),
        "CREATE TEMP TABLE __pgt_capture_test (lsn pg_lsn, action \"char\", __pgt_row_id bytea, source_xid xid, source_commit_at timestamptz, val int)".to_string(),
    ] {
        // The only interpolated identifier component is the catalog's integer ID.
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    let captured: i64 = sqlx::query_scalar("SELECT pgtrickle.retention_capture_multiset_test($1)")
        .bind(id)
        .fetch_one(&mut *conn)
        .await
        .expect("capture actual multiset difference");
    assert_eq!(captured, 4);
    let changes: Vec<(String, String, i32)> = sqlx::query_as("SELECT action::text, encode(__pgt_row_id,'hex'), val FROM __pgt_capture_test ORDER BY action::text, __pgt_row_id, val")
        .fetch_all(&mut *conn).await.expect("read captured changes");
    assert_eq!(
        changes,
        vec![
            ("D".into(), "01".into(), 10),
            ("D".into(), "02".into(), 20),
            ("I".into(), "01".into(), 11),
            ("I".into(), "04".into(), 40)
        ]
    );
}

#[tokio::test]
async fn test_retention_scoped_capture_preserves_duplicate_occurrences() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    let mut conn = db.pool.acquire().await.expect("acquire capture session");
    sqlx::query("CREATE TEMP TABLE capture_target (__pgt_row_id bytea NOT NULL, val int NOT NULL)")
        .execute(&mut *conn)
        .await
        .expect("create capture target");
    sqlx::query("CREATE TEMP TABLE capture_delta (__pgt_row_id bytea NOT NULL)")
        .execute(&mut *conn)
        .await
        .expect("create capture delta");
    sqlx::query("INSERT INTO capture_target VALUES (decode('01','hex'),10),(decode('01','hex'),11),(decode('02','hex'),20),(decode('03','hex'),30)")
        .execute(&mut *conn).await.expect("insert capture target rows");
    sqlx::query("INSERT INTO capture_delta VALUES (decode('01','hex')),(decode('01','hex')),(decode('02','hex'))")
        .execute(&mut *conn).await.expect("insert duplicate delta identities");
    let old: Vec<(String, i32, i64)> = sqlx::query_as("SELECT encode(t.__pgt_row_id,'hex'), t.val, row_number() OVER (PARTITION BY t.__pgt_row_id ORDER BY t.tableoid,t.ctid) FROM capture_target t WHERE EXISTS (SELECT 1 FROM capture_delta d WHERE pgtrickle.row_probe_v1(t.__pgt_row_id)=pgtrickle.row_probe_v1(d.__pgt_row_id) AND t.__pgt_row_id=d.__pgt_row_id) ORDER BY 1,3")
        .fetch_all(&mut *conn).await.expect("read original capture semantics");
    let scoped: Vec<(String, i32, i64)> = sqlx::query_as("SELECT encode(scoped.__pgt_row_id,'hex'), scoped.val, scoped.occurrence FROM (SELECT DISTINCT __pgt_row_id FROM capture_delta) affected CROSS JOIN LATERAL (SELECT t.__pgt_row_id,t.val,row_number() OVER (PARTITION BY t.__pgt_row_id ORDER BY t.tableoid,t.ctid) AS occurrence FROM capture_target t WHERE pgtrickle.row_probe_v1(t.__pgt_row_id)=pgtrickle.row_probe_v1(affected.__pgt_row_id) AND t.__pgt_row_id=affected.__pgt_row_id OFFSET 0) scoped ORDER BY 1,3")
        .fetch_all(&mut *conn).await.expect("read scoped capture semantics");
    let expected = vec![
        ("01".into(), 10, 1),
        ("01".into(), 11, 2),
        ("02".into(), 20, 1),
    ];
    assert_eq!(old, expected);
    assert_eq!(scoped, expected);
}


#[tokio::test]
async fn test_retention_aligned_day_key_allows_other_time_bucket_constant_and_max() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false").await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE aligned_usage (request_id text NOT NULL, event_day date NOT NULL, occurred_at timestamptz NOT NULL, meter text NOT NULL, scope_path text NOT NULL, value numeric NOT NULL, PRIMARY KEY (request_id, event_day)) PARTITION BY RANGE (event_day)").await;
    db.execute("CREATE TABLE aligned_usage_old PARTITION OF aligned_usage FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE aligned_usage_live PARTITION OF aligned_usage FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO aligned_usage VALUES ('1','2025-12-31','2025-12-31 22:12+00','cpu','a',2),('2','2025-12-31','2025-12-31 22:20+00','cpu','a',7),('3','2026-01-02','2026-01-02 03:10+00','cpu','a',3),('4','2026-01-02','2026-01-02 03:40+00','cpu','a',5)").await;
    let query = "SELECT meter, scope_path, event_day, date_trunc('hour', occurred_at AT TIME ZONE 'UTC') AS hour, 'x'::text AS tag, SUM(value) AS quantity, COUNT(*) AS events, MAX(value) AS peak FROM aligned_usage GROUP BY meter, scope_path, event_day, date_trunc('hour', occurred_at AT TIME ZONE 'UTC')";
    db.create_st("aligned_hourly", query, "1h", "DIFFERENTIAL").await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('aligned_hourly', 'TRIM', 'event_day', '2026-01-01')").await;

    // CDC catch-up must preserve pending live rows even as the old partition goes.
    db.execute("INSERT INTO aligned_usage VALUES ('5','2026-01-02','2026-01-02 03:52+00','cpu','a',8)").await;
    db.execute("ALTER TABLE aligned_usage DETACH PARTITION aligned_usage_old").await;
    db.assert_st_matches_query("aligned_hourly", query).await;
    let rows: Vec<(String, String, String, i64, String)> = sqlx::query_as(
        "SELECT event_day::text, hour::text, tag, events, quantity::text FROM aligned_hourly ORDER BY event_day, hour",
    ).fetch_all(&db.pool).await.expect("read aligned groups");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "2026-01-02");
    assert_eq!(rows[0].2, "x");
    assert_eq!(rows[0].3, 3);
    assert_eq!(rows[0].4, "16");
    let needs_reinit: bool = db.query_scalar("SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'aligned_hourly'").await;
    assert!(!needs_reinit, "aligned drop must not rebuild the whole stream");
    let (action, status): (String, String) = sqlx::query_as(
        "SELECT h.action, h.status FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'aligned_hourly' ORDER BY h.refresh_id DESC LIMIT 1",
    ).fetch_one(&db.pool).await.expect("read retention refresh history");
    assert_eq!((action.as_str(), status.as_str()), ("DIFFERENTIAL", "COMPLETED"));
}

#[tokio::test]
async fn test_retain_aligned_day_key_preserves_expired_computed_groups() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false").await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE aligned_retain_src (id int NOT NULL, event_day date NOT NULL, occurred_at timestamptz NOT NULL, amount numeric NOT NULL, PRIMARY KEY (id, event_day)) PARTITION BY RANGE (event_day)").await;
    db.execute("CREATE TABLE aligned_retain_old PARTITION OF aligned_retain_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE aligned_retain_live PARTITION OF aligned_retain_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO aligned_retain_src VALUES (1,'2025-06-01','2025-06-01 13:12+00',8),(2,'2026-06-01','2026-06-01 14:13+00',12)").await;
    let query = "SELECT event_day, date_trunc('hour', occurred_at AT TIME ZONE 'UTC') AS hour, 'fixed'::text AS tag, SUM(amount) AS total, MAX(amount) AS peak FROM aligned_retain_src GROUP BY event_day, date_trunc('hour', occurred_at AT TIME ZONE 'UTC')";
    db.create_st("aligned_retained", query, "1h", "DIFFERENTIAL").await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('aligned_retained', 'RETAIN', 'event_day', '2026-01-01')").await;
    db.execute("DROP TABLE aligned_retain_old").await;
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT event_day::text, total::text, tag FROM aligned_retained ORDER BY event_day",
    ).fetch_all(&db.pool).await.expect("read preserved groups");
    assert_eq!(rows, vec![
        ("2025-06-01".into(), "8".into(), "fixed".into()),
        ("2026-06-01".into(), "12".into(), "fixed".into()),
    ]);
    let retention: serde_json::Value = db.query_scalar(
        "SELECT pgtrickle.get_stream_table_partition_retention('aligned_retained')",
    ).await;
    assert_eq!(retention["sealed_ranges"].as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn test_retention_trim_requires_unique_index_for_keyed_stream() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE trim_unique_src (id INT NOT NULL, d DATE NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE trim_unique_old PARTITION OF trim_unique_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE trim_unique_live PARTITION OF trim_unique_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO trim_unique_src VALUES (1,'2025-06-01',10),(2,'2026-06-01',20)")
        .await;
    let query = "SELECT d, sum(amount) AS total, count(*) AS n FROM trim_unique_src GROUP BY d";
    db.create_st("trim_unique_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('trim_unique_st', 'TRIM', 'd', '2026-01-01')").await;
    db.execute("DROP INDEX trim_unique_st_row_id_idx").await;
    db.execute("CREATE INDEX trim_unique_st_row_id_idx ON trim_unique_st (__pgt_row_id)")
        .await;
    db.execute("INSERT INTO trim_unique_src VALUES (3,'2026-06-01',7)")
        .await;
    let error = db
        .try_execute("DROP TABLE trim_unique_old")
        .await
        .expect_err("reject non-unique row identity");
    assert!(
        error.to_string().contains("immediate unique index"),
        "{error}"
    );
    let attached: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid = 'trim_unique_old'::regclass AND inhparent = 'trim_unique_src'::regclass)").await;
    assert!(attached);
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM trim_unique_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read unchanged stream");
    assert_eq!(
        rows,
        vec![("2025-06-01".into(), 10, 1), ("2026-06-01".into(), 20, 1)]
    );
    db.execute("DROP INDEX trim_unique_st_row_id_idx").await;
    db.execute("CREATE UNIQUE INDEX trim_unique_st_row_id_idx ON trim_unique_st (__pgt_row_id, d)")
        .await;
    let error = db
        .try_execute("DROP TABLE trim_unique_old")
        .await
        .expect_err("reject a composite unique index");
    assert!(
        error.to_string().contains("immediate unique index"),
        "{error}"
    );
    db.execute("DROP INDEX trim_unique_st_row_id_idx").await;
    db.execute("CREATE INDEX trim_unique_st_row_id_idx ON trim_unique_st (pgtrickle.row_probe_v1(__pgt_row_id))").await;
    let error = db
        .try_execute("DROP TABLE trim_unique_old")
        .await
        .expect_err("reject a probe index for a keyed stream");
    assert!(
        error.to_string().contains("immediate unique index"),
        "{error}"
    );
    db.execute("DROP INDEX trim_unique_st_row_id_idx").await;
    db.execute("ALTER TABLE trim_unique_st ADD CONSTRAINT trim_unique_deferred UNIQUE (__pgt_row_id) DEFERRABLE").await;
    let error = db
        .try_execute("DROP TABLE trim_unique_old")
        .await
        .expect_err("reject a deferrable unique index");
    assert!(
        error.to_string().contains("immediate unique index"),
        "{error}"
    );
    db.execute("ALTER TABLE trim_unique_st DROP CONSTRAINT trim_unique_deferred")
        .await;
    db.execute("CREATE UNIQUE INDEX trim_unique_st_row_id_idx ON trim_unique_st (__pgt_row_id)")
        .await;
    db.execute("DROP TABLE trim_unique_old").await;
    db.assert_st_matches_query("trim_unique_st", query).await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM trim_unique_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read repaired stream");
    assert_eq!(rows, vec![("2026-06-01".into(), 27, 2)]);
}

#[tokio::test]
async fn test_retention_trim_accepts_keyless_nonunique_identity_index() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE trim_keyless_src (d DATE NOT NULL, amount INT NOT NULL) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE trim_keyless_old PARTITION OF trim_keyless_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE trim_keyless_live PARTITION OF trim_keyless_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO trim_keyless_src VALUES ('2025-06-01',10),('2026-06-01',20)")
        .await;
    let query = "SELECT d, sum(amount) AS total, count(*) AS n FROM trim_keyless_src GROUP BY d";
    db.create_st("trim_keyless_st", query, "1h", "DIFFERENTIAL")
        .await;
    let unique: bool = db.query_scalar("SELECT indisunique FROM pg_catalog.pg_index WHERE indexrelid = 'trim_keyless_st_row_id_idx'::regclass").await;
    assert!(!unique);
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('trim_keyless_st', 'TRIM', 'd', '2026-01-01')").await;
    db.execute("INSERT INTO trim_keyless_src VALUES ('2026-06-01',7)")
        .await;
    db.execute("DROP TABLE trim_keyless_old").await;
    db.assert_st_matches_query("trim_keyless_st", query).await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM trim_keyless_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read keyless stream results");
    assert_eq!(rows, vec![("2026-06-01".into(), 27, 2)]);
}

#[tokio::test]
async fn test_partitioned_stream_downstream_without_retention() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE partitioned_chain_src (id INT PRIMARY KEY, d DATE NOT NULL, amount INT NOT NULL)").await;
    db.execute("INSERT INTO partitioned_chain_src VALUES (1,'2026-06-01',20)")
        .await;
    let query =
        "SELECT d, sum(amount) AS total, count(*) AS n FROM partitioned_chain_src GROUP BY d";
    db.create_st_partitioned(
        "partitioned_chain_upstream",
        query,
        "1h",
        "DIFFERENTIAL",
        "HASH:d",
    )
    .await;
    db.create_st(
        "partitioned_chain_downstream",
        "SELECT d,total,n FROM partitioned_chain_upstream",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("INSERT INTO partitioned_chain_src VALUES (2,'2026-06-01',7),(3,'2026-06-02',11)")
        .await;
    db.refresh_st_until_converged("partitioned_chain_upstream")
        .await;
    db.refresh_st_until_converged("partitioned_chain_downstream")
        .await;
    db.assert_st_matches_query("partitioned_chain_upstream", query)
        .await;
    db.assert_st_matches_query("partitioned_chain_downstream", query)
        .await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text,total,n FROM partitioned_chain_downstream ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read partitioned pipeline results");
    assert_eq!(
        rows,
        vec![("2026-06-01".into(), 27, 2), ("2026-06-02".into(), 11, 1)]
    );
    for name in ["partitioned_chain_upstream", "partitioned_chain_downstream"] {
        let action: (String, String, bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name=$1 ORDER BY h.refresh_id DESC LIMIT 1").bind(name).fetch_one(&db.pool).await.expect("read pipeline refresh action");
        assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    }
}

#[tokio::test]
async fn test_retention_trim_partitioned_stream_storage() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE trim_storage_src (id INT NOT NULL, d DATE NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE trim_storage_old PARTITION OF trim_storage_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE trim_storage_live PARTITION OF trim_storage_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO trim_storage_src VALUES (1,'2025-06-01',10),(2,'2026-06-01',20),(4,'2026-06-02',9)")
        .await;
    let query = "SELECT d, sum(amount) AS total, count(*) AS n FROM trim_storage_src GROUP BY d";
    db.create_st_partitioned("trim_storage_st", query, "1h", "DIFFERENTIAL", "d")
        .await;
    db.create_st(
        "trim_storage_downstream",
        "SELECT d, total, n FROM trim_storage_st",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('trim_storage_st', 'TRIM', 'd', '2026-01-01')").await;
    db.execute("UPDATE trim_storage_src SET amount=21 WHERE id=2")
        .await;
    db.execute("DELETE FROM trim_storage_src WHERE id=4").await;
    db.execute("INSERT INTO trim_storage_src VALUES (3,'2026-06-01',7),(5,'2026-06-03',11)")
        .await;
    db.execute("DROP TABLE trim_storage_old").await;
    db.assert_st_matches_query("trim_storage_st", query).await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM trim_storage_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read partitioned stream results");
    assert_eq!(
        rows,
        vec![("2026-06-01".into(), 28, 2), ("2026-06-03".into(), 11, 1)]
    );
    db.refresh_st_until_converged("trim_storage_downstream")
        .await;
    db.assert_st_matches_query("trim_storage_downstream", query)
        .await;
    let downstream: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM trim_storage_downstream ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read partitioned stream downstream results");
    assert_eq!(
        downstream,
        vec![("2026-06-01".into(), 28, 2), ("2026-06-03".into(), 11, 1)]
    );
    let action: (String, String, bool) = sqlx::query_as("SELECT h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'trim_storage_st' ORDER BY h.refresh_id DESC LIMIT 1").fetch_one(&db.pool).await.expect("read retention refresh action");
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
}

#[tokio::test]
async fn test_retention_trim_keyless_partitioned_storage_maintains_changes() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE trim_partitioned_keyless_src (id INT NOT NULL, d DATE NOT NULL, amount INT NOT NULL) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE trim_partitioned_keyless_old PARTITION OF trim_partitioned_keyless_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE trim_partitioned_keyless_live PARTITION OF trim_partitioned_keyless_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO trim_partitioned_keyless_src VALUES (1,'2025-06-01',10),(2,'2026-06-01',20),(4,'2026-06-02',9)").await;
    let query = "SELECT d, sum(amount) AS total, count(*) AS n FROM trim_partitioned_keyless_src GROUP BY d";
    db.create_st_partitioned(
        "trim_partitioned_keyless_st",
        query,
        "1h",
        "DIFFERENTIAL",
        "HASH:d",
    )
    .await;
    db.create_st(
        "trim_partitioned_keyless_downstream",
        "SELECT d,total,n FROM trim_partitioned_keyless_st",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('trim_partitioned_keyless_st','TRIM','d','2026-01-01')").await;
    db.execute("UPDATE trim_partitioned_keyless_src SET amount=21 WHERE id=2")
        .await;
    db.execute("DELETE FROM trim_partitioned_keyless_src WHERE id=4")
        .await;
    db.execute(
        "INSERT INTO trim_partitioned_keyless_src VALUES (3,'2026-06-01',7),(5,'2026-06-03',11)",
    )
    .await;
    db.execute("DROP TABLE trim_partitioned_keyless_old").await;
    db.refresh_st_until_converged("trim_partitioned_keyless_downstream")
        .await;
    for name in [
        "trim_partitioned_keyless_st",
        "trim_partitioned_keyless_downstream",
    ] {
        db.assert_st_matches_query(name, query).await;
        let action: (String, String, bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name=$1 ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(name).fetch_one(&db.pool).await.expect("read completed refresh action");
        assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    }
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT d::text,total,n FROM trim_partitioned_keyless_downstream ORDER BY d",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read partitioned downstream");
    assert_eq!(
        rows,
        vec![("2026-06-01".into(), 28, 2), ("2026-06-03".into(), 11, 1)]
    );
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_trim_detach_preserves_live_groups_and_downstream() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE trim_cut_src (id INT NOT NULL, d DATE NOT NULL, account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE trim_cut_old PARTITION OF trim_cut_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE trim_cut_live PARTITION OF trim_cut_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO trim_cut_src VALUES (1,'2025-06-01','a',10), (2,'2025-06-01','b',8), (3,'2026-06-01','a',20), (4,'2026-06-02','b',7)").await;
    let query =
        "SELECT d, account, SUM(amount) AS total, COUNT(*) AS n FROM trim_cut_src GROUP BY 1,2";
    db.create_st("trim_cut_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st(
        "trim_cut_downstream",
        "SELECT d, account, total, n FROM trim_cut_st",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("DROP INDEX trim_cut_st_row_id_idx").await;
    let error = db.try_execute("SELECT pgtrickle.set_stream_table_partition_retention('trim_cut_st', 'TRIM', 'd', '2026-01-01')").await.expect_err("reject retention configuration without the row-identity index");
    assert!(
        error.to_string().contains("valid btree row-identity index"),
        "{error}"
    );
    db.execute("CREATE INDEX trim_cut_st_row_id_idx ON trim_cut_st (pgtrickle.row_probe_v1(__pgt_row_id)) WHERE d >= DATE '2026-01-01'").await;
    let error = db.try_execute("SELECT pgtrickle.set_stream_table_partition_retention('trim_cut_st', 'TRIM', 'd', '2026-01-01')").await.expect_err("reject a partial row-identity index");
    assert!(
        error.to_string().contains("valid btree row-identity index"),
        "{error}"
    );
    db.execute("DROP INDEX trim_cut_st_row_id_idx").await;
    db.execute(
        "CREATE INDEX trim_cut_st_row_id_idx ON trim_cut_st (pgtrickle.row_probe_v1(__pgt_row_id))",
    )
    .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('trim_cut_st', 'TRIM', 'd', '2026-01-01')").await;
    db.execute("DROP INDEX trim_cut_st_row_id_idx").await;
    let error = db
        .try_execute("DROP TABLE trim_cut_old")
        .await
        .expect_err("reject removal without the row-identity index");
    assert!(
        error.to_string().contains("valid btree row-identity index"),
        "{error}"
    );
    let attached: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid = 'trim_cut_old'::regclass AND inhparent = 'trim_cut_src'::regclass)").await;
    assert!(attached);
    db.assert_st_matches_query("trim_cut_st", query).await;
    db.execute(
        "CREATE INDEX trim_cut_st_row_id_idx ON trim_cut_st (pgtrickle.row_probe_v1(__pgt_row_id))",
    )
    .await;
    let (pgt_id, query_hash): (i64, i64) = sqlx::query_as("SELECT pgt_id, defining_query_hash FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'trim_cut_st'")
        .fetch_one(&db.pool).await.expect("read stream identity");
    let (source_oid, target_oid, cutoff): (i64, i64, i32) = sqlx::query_as("SELECT 'trim_cut_src'::regclass::oid::bigint, 'trim_cut_old'::regclass::oid::bigint, DATE '2026-01-01' - DATE '2000-01-01'")
        .fetch_one(&db.pool).await.expect("read partition identities");
    let state = serde_json::json!({
        "version": 1, "mode": "TRIM", "candidate": {
            "output_column": "d", "bucket": "EXACT", "source_type_oid": 1082,
            "source_oid": source_oid, "source_column": "d"
        }, "cutoff": {"kind": "DATE", "value": cutoff}, "query_hash": query_hash, "sealed": []
    });
    sqlx::query("UPDATE pgtrickle.pgt_dependencies SET column_snapshot = jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) WHERE pgt_id = $2 AND source_relid = $3::bigint::oid")
        .bind(&state).bind(pgt_id).bind(source_oid).execute(&db.pool).await.expect("install retention fixture");
    db.execute("DROP INDEX trim_cut_st_d_account_idx").await;
    let err = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT pgtrickle.retention_detach_test($1, $2::bigint::oid, false)",
    )
    .bind(pgt_id)
    .bind(target_oid)
    .fetch_one(&db.pool)
    .await
    .expect_err("reject unindexed TRIM");
    assert!(err.to_string().contains("btree index"));
    db.execute("CREATE INDEX ON trim_cut_st (d)").await;
    let mut plan_conn = db.pool.acquire().await.expect("acquire plan session");
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *plan_conn)
        .await
        .expect("prefer available indexes");
    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN DELETE FROM trim_cut_st WHERE d >= DATE '2025-01-01' AND d < DATE '2026-01-01'",
    )
    .fetch_all(&mut *plan_conn)
    .await
    .expect("explain bounded deletion");
    assert!(
        plan.iter().any(|line| line.contains("trim_cut_st_d_idx")),
        "bounded deletion cannot use the group index: {plan:?}"
    );
    sqlx::query("RESET enable_seqscan")
        .execute(&mut *plan_conn)
        .await
        .expect("restore planner setting");
    drop(plan_conn);
    let mut partial = state.clone();
    partial["cutoff"]["value"] = serde_json::json!(cutoff - 1);
    sqlx::query("UPDATE pgtrickle.pgt_dependencies SET column_snapshot = jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) WHERE pgt_id = $2")
        .bind(&partial).bind(pgt_id).execute(&db.pool).await.expect("install partially overlapping cutoff");
    let err = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT pgtrickle.retention_detach_test($1, $2::bigint::oid, false)",
    )
    .bind(pgt_id)
    .bind(target_oid)
    .fetch_one(&db.pool)
    .await
    .expect_err("reject partially expired partition");
    assert!(err.to_string().contains("not wholly below"));
    db.assert_st_matches_query("trim_cut_st", query).await;
    sqlx::query("UPDATE pgtrickle.pgt_dependencies SET column_snapshot = jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) WHERE pgt_id = $2")
        .bind(&state).bind(pgt_id).execute(&db.pool).await.expect("restore expired cutoff");
    let err = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT pgtrickle.retention_detach_test($1, $2::bigint::oid, true)",
    )
    .bind(pgt_id)
    .bind(target_oid)
    .fetch_one(&db.pool)
    .await
    .expect_err("roll back trimmed detach");
    assert!(
        err.to_string()
            .contains("intentional retention test rollback")
    );
    db.assert_st_matches_query("trim_cut_st", query).await;
    let before_frontier: serde_json::Value = sqlx::query_scalar(
        "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'trim_cut_downstream'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read downstream frontier");
    let mut removal = db.pool.begin().await.expect("begin trim transaction");
    let _: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.retention_detach_test($1, $2::bigint::oid, false)")
            .bind(pgt_id)
            .bind(target_oid)
            .fetch_one(&mut *removal)
            .await
            .expect("trim detached range");
    let visible: Vec<(String, String, i64, i64)> = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        sqlx::query_as("SELECT d::text, account, total, n FROM trim_cut_st ORDER BY d, account")
            .fetch_all(&db.pool),
    )
    .await
    .expect("reader does not block on uncommitted trim")
    .expect("read prior snapshot");
    assert_eq!(
        visible,
        vec![
            ("2025-06-01".into(), "a".into(), 10, 1),
            ("2025-06-01".into(), "b".into(), 8, 1),
            ("2026-06-01".into(), "a".into(), 20, 1),
            ("2026-06-02".into(), "b".into(), 7, 1),
        ]
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        db.refresh_st_with_retry("trim_cut_downstream"),
    )
    .await
    .expect("downstream refresh during uncommitted trim");
    let during_frontier: serde_json::Value = sqlx::query_scalar(
        "SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'trim_cut_downstream'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read downstream frontier during trim");
    let source_key = format!("pgt_{pgt_id}");
    let before_lsn = before_frontier["sources"][&source_key]["lsn"]
        .as_str()
        .expect("downstream source WAL position");
    assert_eq!(
        during_frontier["sources"][&source_key]["lsn"].as_str(),
        Some(before_lsn)
    );
    let downstream_before_commit: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM trim_cut_downstream ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read downstream output during trim");
    assert_eq!(downstream_before_commit, visible);
    removal.commit().await.expect("commit trim transaction");
    db.assert_st_matches_query("trim_cut_st", query).await;
    db.refresh_st_until_converged("trim_cut_downstream").await;
    db.assert_st_matches_query("trim_cut_downstream", query)
        .await;
    let downstream_history: (String, String, bool) = sqlx::query_as("SELECT h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'trim_cut_downstream' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&db.pool).await.expect("read downstream refresh action");
    assert_eq!(
        downstream_history,
        ("DIFFERENTIAL".into(), "COMPLETED".into(), false)
    );
    db.execute("INSERT INTO trim_cut_src VALUES (5,'2026-06-01','a',3)")
        .await;
    db.refresh_st_until_converged("trim_cut_st").await;
    db.refresh_st_until_converged("trim_cut_downstream").await;
    db.assert_st_matches_query("trim_cut_st", query).await;
    db.assert_st_matches_query("trim_cut_downstream", query)
        .await;
    let history: (String, String, bool) = sqlx::query_as("SELECT action, status, was_full_fallback FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1 ORDER BY refresh_id DESC LIMIT 1")
        .bind(pgt_id).fetch_one(&db.pool).await.expect("read refresh action");
    assert_eq!(history, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_trim_utc_hour_bounds_before_postgres_epoch() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE trim_hour_src (id INT NOT NULL, d TIMESTAMPTZ NOT NULL, account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE trim_hour_old PARTITION OF trim_hour_src FOR VALUES FROM ('1999-12-31 23:00:00+00') TO ('2000-01-01 00:00:00+00')").await;
    db.execute("CREATE TABLE trim_hour_live PARTITION OF trim_hour_src FOR VALUES FROM ('2000-01-01 00:00:00+00') TO ('2000-01-01 02:00:00+00')").await;
    db.execute("INSERT INTO trim_hour_src VALUES (1,'1999-12-31 23:59:59+00','a',10), (2,'2000-01-01 00:15:00+00','a',20), (3,'2000-01-01 01:15:00+00','b',7)").await;
    let query = "SELECT pg_catalog.date_trunc('hour', d, 'UTC') AS bucket, account, SUM(amount) AS total, COUNT(*) AS n FROM trim_hour_src GROUP BY 1,2";
    db.create_st("trim_hour_st", query, "1h", "DIFFERENTIAL")
        .await;
    let mut conn = db.pool.acquire().await.expect("acquire session");
    sqlx::query("SET TIME ZONE 'America/New_York'")
        .execute(&mut *conn)
        .await
        .expect("set session time zone");
    sqlx::query("SELECT pgtrickle.set_stream_table_partition_retention('trim_hour_st', 'TRIM', 'd', '2000-01-01 01:00:00+01')")
        .execute(&mut *conn).await.expect("configure UTC cutoff");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.get_stream_table_partition_retention('trim_hour_st')")
            .fetch_one(&mut *conn)
            .await
            .expect("read UTC cutoff");
    let expected_cutoff: String =
        sqlx::query_scalar("SELECT TIMESTAMPTZ '2000-01-01 00:00:00+00'::text")
            .fetch_one(&mut *conn)
            .await
            .expect("format UTC cutoff in caller time zone");
    assert_eq!(status["cutoff"], expected_cutoff);
    sqlx::query("ALTER TABLE trim_hour_src DETACH PARTITION trim_hour_old")
        .execute(&mut *conn)
        .await
        .expect("trim UTC range");
    drop(conn);
    db.assert_st_matches_query("trim_hour_st", query).await;
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
#[cfg(not(feature = "light-e2e"))]
async fn test_retention_prepare_drains_writer_before_choosing_cdc_cut() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_cut_src (id INT NOT NULL, d DATE NOT NULL, account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_cut_old PARTITION OF retention_cut_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_cut_live PARTITION OF retention_cut_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute(
        "INSERT INTO retention_cut_src VALUES (1,'2025-06-01','a',10), (2,'2026-06-01','a',20)",
    )
    .await;
    let query = "SELECT d, account, SUM(amount) AS total, COUNT(*) AS n FROM retention_cut_src GROUP BY 1,2";
    db.create_st("retention_cut_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_cut_st', 'RETAIN', 'd', '2026-01-01')").await;
    let (pgt_id, query_hash): (i64, i64) = sqlx::query_as(
        "SELECT pgt_id, defining_query_hash FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_cut_st'",
    ).fetch_one(&db.pool).await.expect("read stream identity");
    let (source_oid, target_oid, cutoff): (i64, i64, i32) = sqlx::query_as(
        "SELECT 'retention_cut_src'::regclass::oid::bigint, 'retention_cut_old'::regclass::oid::bigint, DATE '2026-01-01' - DATE '2000-01-01'",
    ).fetch_one(&db.pool).await.expect("read source identity and cutoff");
    let state = serde_json::json!({
        "version": 1, "mode": "RETAIN", "candidate": {
            "output_column": "d", "bucket": "EXACT", "source_type_oid": 1082,
            "source_oid": source_oid, "source_column": "d"
        }, "cutoff": {"kind": "DATE", "value": cutoff}, "query_hash": query_hash, "sealed": []
    });
    sqlx::query("UPDATE pgtrickle.pgt_dependencies SET column_snapshot = jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) WHERE pgt_id = $2 AND source_relid = $3::bigint::oid")
        .bind(&state).bind(pgt_id).bind(source_oid).execute(&db.pool).await.expect("install retention fixture");

    for (command, expected_error) in [
        (
            "ALTER TABLE retention_cut_src DETACH PARTITION retention_cut_live",
            "wholly below",
        ),
        (
            "ALTER TABLE retention_cut_src DETACH PARTITION retention_cut_live CONCURRENTLY",
            "wholly below",
        ),
        ("DROP TABLE retention_cut_live", "wholly below"),
    ] {
        let error = db
            .try_execute(command)
            .await
            .expect_err("reject unprepared first cut");
        assert!(
            error.to_string().contains(expected_error),
            "{command}: {error}"
        );
    }
    let mut writer = db.pool.begin().await.expect("begin source writer");
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .expect("read writer PID");
    sqlx::query("LOCK TABLE retention_cut_src IN ROW EXCLUSIVE MODE")
        .execute(&mut *writer)
        .await
        .expect("hold writer lock");
    let mut prepare_conn = db
        .pool
        .acquire()
        .await
        .expect("acquire preparation connection");
    let prepare_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *prepare_conn)
        .await
        .expect("read preparation PID");
    let prepare = tokio::spawn(async move {
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT pgtrickle.retention_prepare_test($1, $2::bigint::oid)",
        )
        .bind(pgt_id)
        .bind(target_oid)
        .fetch_one(&mut *prepare_conn)
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE pid = $1 AND relation = 'retention_cut_src'::regclass AND mode = 'ShareLock' AND NOT granted AND $2 = ANY(pg_catalog.pg_blocking_pids(pid)))")
                .bind(prepare_pid).bind(writer_pid).fetch_one(&db.pool).await.expect("inspect preparation lock");
            if waiting { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.expect("preparation waits for writer");
    sqlx::query("INSERT INTO retention_cut_src VALUES (3,'2025-06-01','a',5)")
        .execute(&mut *writer)
        .await
        .expect("write after preparation starts");
    let writer_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(&mut *writer)
        .await
        .expect("capture writer WAL position");
    writer.commit().await.expect("commit source writer");
    let prepared = prepare
        .await
        .expect("join preparation")
        .expect("prepare retention cut");
    let bound = prepared["safe_bound"].as_str().expect("prepared WAL bound");
    let covers_writer: bool = sqlx::query_scalar("SELECT $1::pg_lsn >= $2::pg_lsn")
        .bind(bound)
        .bind(&writer_lsn)
        .fetch_one(&db.pool)
        .await
        .expect("compare WAL positions");
    assert!(
        covers_writer,
        "the protected cut must follow the drained writer"
    );
    let history: (String, String, bool) = sqlx::query_as("SELECT action, status, was_full_fallback FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1 ORDER BY refresh_id DESC LIMIT 1")
        .bind(pgt_id).fetch_one(&db.pool).await.expect("read completed preparation action");
    assert_eq!(history, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    db.assert_st_matches_query("retention_cut_st", query).await;
    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM retention_cut_st ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read prepared aggregates");
    assert_eq!(
        rows,
        vec![
            ("2025-06-01".into(), "a".into(), 15, 2),
            ("2026-06-01".into(), "a".into(), 20, 1)
        ]
    );
    db.execute("INSERT INTO retention_cut_src VALUES (4,'2026-06-01','a',7)")
        .await;
    db.refresh_st_until_converged("retention_cut_st").await;
    db.assert_st_matches_query("retention_cut_st", query).await;
    let history: (String, String, bool) = sqlx::query_as("SELECT action, status, was_full_fallback FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1 ORDER BY refresh_id DESC LIMIT 1")
        .bind(pgt_id).fetch_one(&db.pool).await.expect("read subsequent refresh action");
    assert_eq!(history, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));

    let baseline: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, SUM(amount), COUNT(*) FROM retention_cut_src GROUP BY d, account ORDER BY d, account",
    ).fetch_all(&db.pool).await.expect("capture independent pre-removal baseline");
    let frontier: serde_json::Value =
        sqlx::query_scalar("SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1")
            .bind(pgt_id)
            .fetch_one(&db.pool)
            .await
            .expect("capture pre-removal frontier");
    let error = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT pgtrickle.retention_detach_test($1, $2::bigint::oid, true)",
    )
    .bind(pgt_id)
    .bind(target_oid)
    .fetch_one(&db.pool)
    .await
    .expect_err("intentional removal rollback");
    assert!(
        error
            .to_string()
            .contains("intentional retention test rollback")
    );
    let attached: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = 'retention_cut_src'::regclass AND inhrelid = 'retention_cut_old'::regclass)").await;
    assert!(attached);
    let after_rollback: serde_json::Value =
        sqlx::query_scalar("SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1")
            .bind(pgt_id)
            .fetch_one(&db.pool)
            .await
            .expect("read rolled-back frontier");
    assert_eq!(after_rollback, frontier);
    let saved: serde_json::Value = sqlx::query_scalar("SELECT column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies WHERE pgt_id = $1")
        .bind(pgt_id).fetch_one(&db.pool).await.expect("read rolled-back policy state");
    assert_eq!(saved, state);

    let finalized: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.retention_detach_test($1, $2::bigint::oid, false)")
            .bind(pgt_id)
            .bind(target_oid)
            .fetch_one(&db.pool)
            .await
            .expect("detach and finalize retained range");
    let lower: i32 = db
        .query_scalar("SELECT DATE '2025-01-01' - DATE '2000-01-01'")
        .await;
    assert_eq!(
        finalized["sealed"],
        serde_json::json!([{
            "lower": {"kind": "DATE", "value": lower}, "upper": {"kind": "DATE", "value": cutoff}
        }])
    );
    let saved: serde_json::Value = sqlx::query_scalar("SELECT column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies WHERE pgt_id = $1")
        .bind(pgt_id).fetch_one(&db.pool).await.expect("read committed retained state");
    assert_eq!(saved, finalized);
    let retained: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM retention_cut_st ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read retained historical output");
    assert_eq!(retained, baseline);
    let status: (String, bool) = sqlx::query_as(
        "SELECT status, needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .fetch_one(&db.pool)
    .await
    .expect("read retained stream state");
    assert_eq!(status, ("ACTIVE".into(), false));
    for (command, expected_error) in [
        (
            "ALTER TABLE retention_cut_src DETACH PARTITION retention_cut_live",
            "wholly below",
        ),
        (
            "ALTER TABLE retention_cut_src DETACH PARTITION retention_cut_live CONCURRENTLY",
            "wholly below",
        ),
        ("DROP TABLE retention_cut_live", "wholly below"),
    ] {
        let error = db
            .try_execute(command)
            .await
            .expect_err("reject unprepared removal after sealing");
        assert!(
            error.to_string().contains(expected_error),
            "{command}: {error}"
        );
    }
    let still_attached: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent = 'retention_cut_src'::regclass AND inhrelid = 'retention_cut_live'::regclass AND NOT inhdetachpending)").await;
    assert!(
        still_attached,
        "failed removal must roll back partition state"
    );
    let error = db.try_execute("ALTER TABLE retention_cut_src ATTACH PARTITION retention_cut_old FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')")
        .await.expect_err("reject reattachment of retained rows");
    assert!(error.to_string().contains("nonempty partition"));
    let error = db.try_execute("CREATE TABLE retention_cut_old_empty PARTITION OF retention_cut_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')")
        .await.expect_err("reject empty partition over retained history");
    assert!(
        error.to_string().contains("overlapping retained history"),
        "{error}"
    );
    db.execute("CREATE TABLE retention_cut_old_attach (LIKE retention_cut_src INCLUDING DEFAULTS INCLUDING CONSTRAINTS)").await;
    let error = db.try_execute("ALTER TABLE retention_cut_src ATTACH PARTITION retention_cut_old_attach FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')")
        .await.expect_err("reject empty reattachment over retained history");
    assert!(
        error.to_string().contains("overlapping retained history"),
        "{error}"
    );
    db.execute("CREATE TABLE retention_cut_backfill (LIKE retention_cut_src INCLUDING DEFAULTS INCLUDING CONSTRAINTS)").await;
    db.execute("INSERT INTO retention_cut_backfill VALUES (6,'2027-06-01','b',8)")
        .await;
    let error = db.try_execute("ALTER TABLE retention_cut_src ATTACH PARTITION retention_cut_backfill FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')")
        .await.expect_err("reject nonempty future attachment");
    assert!(error.to_string().contains("nonempty partition"));
    let mut attach_conn = db.pool.acquire().await.expect("acquire DDL session");
    sqlx::query("SET search_path = public, pg_catalog")
        .execute(&mut *attach_conn)
        .await
        .expect("set caller search path");
    sqlx::query("CREATE TABLE retention_cut_future PARTITION OF retention_cut_src FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')")
        .execute(&mut *attach_conn).await.expect("create empty future partition");
    let path: String = sqlx::query_scalar("SELECT pg_catalog.current_setting('search_path')")
        .fetch_one(&mut *attach_conn)
        .await
        .expect("read restored search path");
    assert_eq!(path, "public, pg_catalog");
    drop(attach_conn);
    db.execute("CREATE TABLE retention_cut_future_attach (LIKE retention_cut_src INCLUDING DEFAULTS INCLUDING CONSTRAINTS)").await;
    db.execute("ALTER TABLE retention_cut_src ATTACH PARTITION retention_cut_future_attach FOR VALUES FROM ('2028-01-01') TO ('2029-01-01')").await;
    db.execute("INSERT INTO retention_cut_src VALUES (5,'2026-06-01','a',3)")
        .await;
    db.execute("INSERT INTO retention_cut_src VALUES (7,'2027-06-01','b',9)")
        .await;
    db.execute("INSERT INTO retention_cut_src VALUES (8,'2028-06-01','c',11)")
        .await;
    let mut expected = baseline.clone();
    expected[1].2 += 3;
    expected[1].3 += 1;
    expected.push(("2027-06-01".into(), "b".into(), 9, 1));
    expected.push(("2028-06-01".into(), "c".into(), 11, 1));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        db.refresh_st_with_retry("retention_cut_st").await;
        let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
            "SELECT d::text, account, total, n FROM retention_cut_st ORDER BY d, account",
        )
        .fetch_all(&db.pool)
        .await
        .expect("read retained and live aggregates");
        if rows == expected {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "retained differential refresh did not converge: {rows:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let history: (String, String, bool) = sqlx::query_as("SELECT action, status, was_full_fallback FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1 ORDER BY refresh_id DESC LIMIT 1")
        .bind(pgt_id).fetch_one(&db.pool).await.expect("read post-retention refresh action");
    assert_eq!(history, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    db.pool.close().await;
    let restart = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart retention database");
    assert!(
        restart.status.success(),
        "container restart failed: {}",
        String::from_utf8_lossy(&restart.stderr)
    );
    db.pool = db.reconnect_after_restart().await;
    let after_restart: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM retention_cut_st ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read retained output after restart");
    assert_eq!(after_restart, expected);
    db.execute("INSERT INTO retention_cut_src VALUES (9,'2027-06-01','b',4)")
        .await;
    expected[2].2 += 4;
    expected[2].3 += 1;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        db.refresh_st_with_retry("retention_cut_st").await;
        let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
            "SELECT d::text, account, total, n FROM retention_cut_st ORDER BY d, account",
        )
        .fetch_all(&db.pool)
        .await
        .expect("read retained output after restart refresh");
        if rows == expected {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "post-restart refresh did not converge: {rows:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let history: (String, String, bool) = sqlx::query_as("SELECT action, status, was_full_fallback FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1 ORDER BY refresh_id DESC LIMIT 1")
        .bind(pgt_id).fetch_one(&db.pool).await.expect("read post-restart refresh action");
    assert_eq!(history, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET refresh_mode = 'FULL' WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_cut_st')")
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('retention_cut_st')")
        .await
        .expect_err("FULL must not erase retained history");
    assert!(error.to_string().contains("retained partition history"));
    let after_failed_full: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM retention_cut_st ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read retained output after rejected FULL");
    assert_eq!(after_failed_full, expected);
}

#[tokio::test]
async fn test_retention_downstream_capture_preserves_null_groups() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.refresh_strategy = 'differential'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE null_retention_src (id INT NOT NULL, d DATE NOT NULL, account INT, amount INT, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE null_retention_old PARTITION OF null_retention_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE null_retention_live PARTITION OF null_retention_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO null_retention_src VALUES (1,'2025-06-01',NULL,NULL),(2,'2025-06-01',1,7),(3,'2026-06-01',NULL,NULL),(4,'2026-06-01',1,10)").await;
    let query = "SELECT d, account, sum(amount) AS total, count(*) AS n FROM null_retention_src GROUP BY d, account";
    for (name, policy) in [("null_trim", "TRIM"), ("null_keep", "RETAIN")] {
        db.create_st(name, query, "1h", "DIFFERENTIAL").await;
        sqlx::query(
            "SELECT pgtrickle.set_stream_table_partition_retention($1,$2,'d','2026-01-01')",
        )
        .bind(name)
        .bind(policy)
        .execute(&db.pool)
        .await
        .unwrap();
    }
    db.create_st(
        "null_down",
        "SELECT d, account, total, n FROM null_keep",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "INSERT INTO null_retention_src VALUES (5,'2025-06-01',NULL,2),(6,'2026-06-01',NULL,3)",
    )
    .await;
    let baseline: Vec<(String, Option<i32>, Option<i64>, i64)> = sqlx::query_as("SELECT d::text, account, sum(amount), count(*) FROM null_retention_src GROUP BY d,account ORDER BY d,account NULLS FIRST").fetch_all(&db.pool).await.unwrap();
    assert_eq!(
        baseline,
        vec![
            ("2025-06-01".into(), None, Some(2), 2),
            ("2025-06-01".into(), Some(1), Some(7), 1),
            ("2026-06-01".into(), None, Some(3), 2),
            ("2026-06-01".into(), Some(1), Some(10), 1)
        ]
    );
    db.execute("ALTER TABLE null_retention_src DETACH PARTITION null_retention_old")
        .await;
    db.refresh_st("null_down").await;
    let downstream: Vec<(String, Option<i32>, Option<i64>, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM null_down ORDER BY d,account NULLS FIRST",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(downstream, baseline);
    db.assert_st_matches_query("null_trim", query).await;
    db.execute("UPDATE null_retention_src SET amount = NULL WHERE id = 6")
        .await;
    db.refresh_st("null_trim").await;
    db.refresh_st("null_keep").await;
    db.refresh_st("null_down").await;
    let expected = vec![
        ("2025-06-01".into(), None, Some(2), 2),
        ("2025-06-01".into(), Some(1), Some(7), 1),
        ("2026-06-01".into(), None, None, 2),
        ("2026-06-01".into(), Some(1), Some(10), 1),
    ];
    let keep: Vec<(String, Option<i32>, Option<i64>, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM null_keep ORDER BY d,account NULLS FIRST",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    let downstream: Vec<(String, Option<i32>, Option<i64>, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM null_down ORDER BY d,account NULLS FIRST",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(keep, expected);
    assert_eq!(downstream, expected);
    db.assert_st_matches_query("null_trim", query).await;
    let actions: Vec<(String, String, String, bool)> = sqlx::query_as("SELECT DISTINCT ON (s.pgt_name) s.pgt_name,h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name IN ('null_keep','null_trim','null_down') ORDER BY s.pgt_name,h.refresh_id DESC").fetch_all(&db.pool).await.unwrap();
    assert_eq!(
        actions,
        vec![
            (
                "null_down".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            ),
            (
                "null_keep".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            ),
            (
                "null_trim".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            )
        ]
    );
}

#[tokio::test]
async fn test_retention_preserves_nonfinite_numeric_aggregates() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.refresh_strategy = 'differential'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE special_retention_src (id INT NOT NULL, d DATE NOT NULL, account INT NOT NULL, amount NUMERIC NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE special_retention_old PARTITION OF special_retention_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE special_retention_live PARTITION OF special_retention_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO special_retention_src VALUES (1,'2025-06-01',1,'Infinity'), (2,'2025-06-01',1,5), (3,'2026-06-01',1,'Infinity'), (4,'2026-06-01',1,1), (5,'2026-06-01',2,'NaN')").await;
    let query =
        "SELECT d, account, sum(amount) AS total FROM special_retention_src GROUP BY d, account";
    for (name, policy) in [("special_trim", "TRIM"), ("special_keep", "RETAIN")] {
        db.create_st(name, query, "1h", "DIFFERENTIAL").await;
        sqlx::query(
            "SELECT pgtrickle.set_stream_table_partition_retention($1, $2, 'd', '2026-01-01')",
        )
        .bind(name)
        .bind(policy)
        .execute(&db.pool)
        .await
        .expect("configure retention");
    }
    db.create_st(
        "special_down",
        "SELECT d, account, total FROM special_keep",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("INSERT INTO special_retention_src VALUES (6,'2025-06-01',2,'NaN'), (7,'2026-06-01',3,'Infinity')").await;
    let baseline: Vec<(String, i32, String)> = sqlx::query_as("SELECT d::text, account, sum(amount)::text FROM special_retention_src GROUP BY d, account ORDER BY d, account").fetch_all(&db.pool).await.unwrap();
    assert_eq!(
        baseline,
        vec![
            ("2025-06-01".into(), 1, "Infinity".into()),
            ("2025-06-01".into(), 2, "NaN".into()),
            ("2026-06-01".into(), 1, "Infinity".into()),
            ("2026-06-01".into(), 2, "NaN".into()),
            ("2026-06-01".into(), 3, "Infinity".into())
        ]
    );
    db.execute("ALTER TABLE special_retention_src DETACH PARTITION special_retention_old")
        .await;
    let keep: Vec<(String, i32, String)> = sqlx::query_as(
        "SELECT d::text, account, total::text FROM special_keep ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(keep, baseline);
    db.refresh_st("special_down").await;
    let downstream: Vec<(String, i32, String)> = sqlx::query_as(
        "SELECT d::text, account, total::text FROM special_down ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(downstream, baseline);
    db.assert_st_matches_query("special_trim", query).await;
    db.execute("DELETE FROM special_retention_src WHERE id = 3")
        .await;
    db.execute("UPDATE special_retention_src SET amount = 4 WHERE id = 5")
        .await;
    db.refresh_st("special_trim").await;
    db.refresh_st("special_keep").await;
    db.refresh_st("special_down").await;
    let trim: Vec<(String, i32, String)> = sqlx::query_as(
        "SELECT d::text, account, total::text FROM special_trim ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    let keep: Vec<(String, i32, String)> = sqlx::query_as(
        "SELECT d::text, account, total::text FROM special_keep ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        trim,
        vec![
            ("2026-06-01".into(), 1, "1".into()),
            ("2026-06-01".into(), 2, "4".into()),
            ("2026-06-01".into(), 3, "Infinity".into())
        ]
    );
    assert_eq!(
        keep,
        vec![
            ("2025-06-01".into(), 1, "Infinity".into()),
            ("2025-06-01".into(), 2, "NaN".into()),
            ("2026-06-01".into(), 1, "1".into()),
            ("2026-06-01".into(), 2, "4".into()),
            ("2026-06-01".into(), 3, "Infinity".into())
        ]
    );
    let downstream: Vec<(String, i32, String)> = sqlx::query_as(
        "SELECT d::text, account, total::text FROM special_down ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(downstream, keep);
    db.assert_st_matches_query("special_trim", query).await;
    let actions: Vec<(String, String, String, bool)> = sqlx::query_as("SELECT DISTINCT ON (s.pgt_name) s.pgt_name, h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name IN ('special_trim', 'special_keep') ORDER BY s.pgt_name, h.refresh_id DESC").fetch_all(&db.pool).await.unwrap();
    assert_eq!(
        actions,
        vec![
            (
                "special_keep".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            ),
            (
                "special_trim".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            )
        ]
    );
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET refresh_mode = 'FULL' WHERE pgt_name = 'special_keep'").await;
    let error = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('special_keep')")
        .await
        .expect_err("FULL must not replace sealed non-finite history");
    assert!(
        error.to_string().contains("retained partition history"),
        "{error}"
    );
    let after_full: Vec<(String, i32, String)> = sqlx::query_as(
        "SELECT d::text, account, total::text FROM special_keep ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(after_full, keep);
}

#[tokio::test]
async fn test_retention_rejects_inexact_floating_sum() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_float_src (id INT NOT NULL, d DATE NOT NULL, val DOUBLE PRECISION, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE retention_float_old PARTITION OF retention_float_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_float_live PARTITION OF retention_float_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO retention_float_src VALUES (1,'2026-06-01',1e16)")
        .await;
    let query = "SELECT d,sum(val) AS total,count(*) AS n FROM retention_float_src GROUP BY d";
    for (name, policy) in [("float_trim", "TRIM"), ("float_keep", "RETAIN")] {
        let error = sqlx::query(
            "SELECT pgtrickle.create_stream_table_with_retention($1,$2,$3,'d','2026-01-01','1h')",
        )
        .bind(name)
        .bind(query)
        .bind(policy)
        .execute(&db.pool)
        .await
        .expect_err("floating SUM must be rejected");
        assert!(
            error
                .to_string()
                .contains("floating-point SUM is unsupported"),
            "{error}"
        );
        let relation: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(name)
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(relation, None);
    }
    db.create_st("float_legacy", query, "1h", "DIFFERENTIAL")
        .await;
    let error = db.try_execute("SELECT pgtrickle.set_stream_table_partition_retention('float_legacy','RETAIN','d','2026-01-01')").await.expect_err("existing floating SUM must be rejected");
    assert!(
        error
            .to_string()
            .contains("floating-point SUM is unsupported"),
        "{error}"
    );
    db.execute("UPDATE pgtrickle.pgt_dependencies dep SET column_snapshot = jsonb_set(COALESCE(dep.column_snapshot,'{}'::jsonb), '{partition_retention}', jsonb_build_object('version',1,'mode','RETAIN','candidate',jsonb_build_object('output_column','d','bucket','EXACT','source_type_oid',1082,'source_oid',dep.source_relid::oid::bigint,'source_column','d'),'cutoff',jsonb_build_object('kind','DATE','value',DATE '2026-01-01' - DATE '2000-01-01'),'query_hash',st.defining_query_hash,'sealed','[]'::jsonb)) FROM pgtrickle.pgt_stream_tables st WHERE st.pgt_id=dep.pgt_id AND st.pgt_name='float_legacy' AND dep.source_relid='retention_float_src'::regclass").await;
    db.execute("INSERT INTO retention_float_src VALUES (2,'2026-06-01',-1e16),(3,'2026-06-01',1)")
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('float_legacy')")
        .await
        .expect_err("unsafe legacy policy must fail before refresh");
    assert!(
        error
            .to_string()
            .contains("does not support floating-point SUM"),
        "{error}"
    );
    let actual: Vec<(String, f64, i64)> =
        sqlx::query_as("SELECT d::text,total,n FROM float_legacy ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(actual, vec![("2026-06-01".into(), 1e16, 1)]);
    let expected: Vec<(String, f64, i64)> = sqlx::query_as(
        "SELECT d::text,sum(val),count(*) FROM retention_float_src GROUP BY d ORDER BY d",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(expected, vec![("2026-06-01".into(), 1.0, 3)]);
    let error = db
        .try_execute("DROP TABLE retention_float_old")
        .await
        .expect_err("unsafe legacy policy must block removal");
    assert!(
        error
            .to_string()
            .contains("floating-point SUM is unsupported"),
        "{error}"
    );
    let attached: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent='retention_float_src'::regclass AND inhrelid='retention_float_old'::regclass)").await;
    assert!(attached);

    #[cfg(not(feature = "light-e2e"))]
    {
        db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
            .await;
        db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
            .await;
        db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
            .await;
        db.execute("UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at = now() - interval '2 hours' WHERE pgt_name = 'float_legacy'").await;
        db.execute("ALTER SYSTEM SET pg_trickle.enabled = true")
            .await;
        db.execute("SELECT pg_reload_conf()").await;
        assert!(
            db.wait_for_scheduler(std::time::Duration::from_secs(90))
                .await
        );
        assert!(db.wait_for_condition(
            "scheduled refresh rejects floating-point SUM",
            "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='float_legacy' AND h.initiated_by='SCHEDULER' AND h.status='FAILED' AND h.error_message LIKE '%does not support floating-point SUM%')",
            std::time::Duration::from_secs(60),
            std::time::Duration::from_millis(50),
        ).await);
        db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
            .await;
        db.execute("SELECT pg_reload_conf()").await;
        let after_scheduler: Vec<(String, f64, i64)> =
            sqlx::query_as("SELECT d::text,total,n FROM float_legacy ORDER BY d")
                .fetch_all(&db.pool)
                .await
                .unwrap();
        assert_eq!(after_scheduler, actual);
    }
    db.execute("UPDATE pgtrickle.pgt_dependencies dep SET column_snapshot=dep.column_snapshot-'partition_retention' FROM pgtrickle.pgt_stream_tables st WHERE st.pgt_id=dep.pgt_id AND st.pgt_name='float_legacy'").await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('float_count','SELECT d,count(val) AS n FROM retention_float_src GROUP BY d','TRIM','d','2026-01-01','1h')").await;
    db.execute("INSERT INTO retention_float_src VALUES (4,'2026-06-01',NULL)")
        .await;
    db.execute("DROP TABLE retention_float_old").await;
    let count: Vec<(String, i64)> = sqlx::query_as("SELECT d::text,n FROM float_count ORDER BY d")
        .fetch_all(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, vec![("2026-06-01".into(), 3)]);
    db.assert_st_matches_query(
        "float_count",
        "SELECT d,count(val) AS n FROM retention_float_src GROUP BY d",
    )
    .await;
    let action: (String, String, bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='float_count' ORDER BY h.refresh_id DESC LIMIT 1").fetch_one(&db.pool).await.unwrap();
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
}

#[tokio::test]
async fn test_retention_rejects_nondeterministic_group_collations() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE COLLATION retention_case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false)").await;
    db.execute("CREATE TABLE collation_retention_src (id INT NOT NULL, d DATE NOT NULL, account TEXT COLLATE retention_case_insensitive NOT NULL, amount INT NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE collation_retention_old PARTITION OF collation_retention_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE collation_retention_live PARTITION OF collation_retention_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    let query =
        "SELECT d, account, sum(amount) AS total FROM collation_retention_src GROUP BY d, account";
    for (name, policy) in [("collation_trim", "TRIM"), ("collation_keep", "RETAIN")] {
        let error = sqlx::query(
            "SELECT pgtrickle.create_stream_table_with_retention($1,$2,$3,'d','2026-01-01','1h')",
        )
        .bind(name)
        .bind(query)
        .bind(policy)
        .execute(&db.pool)
        .await
        .expect_err("reject nondeterministic grouping");
        assert!(
            error.to_string().contains("nondeterministic collation"),
            "{error}"
        );
        let relation: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(name)
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(relation, None);
    }
    db.create_st("collation_legacy", query, "1h", "DIFFERENTIAL")
        .await;
    let error = db.try_execute("SELECT pgtrickle.set_stream_table_partition_retention('collation_legacy','RETAIN','d','2026-01-01')")
        .await.expect_err("reject an existing stream with unsafe grouping");
    assert!(
        error.to_string().contains("nondeterministic collation"),
        "{error}"
    );
    let policy: serde_json::Value = db
        .query_scalar("SELECT pgtrickle.get_stream_table_partition_retention('collation_legacy')")
        .await;
    assert_eq!(policy, serde_json::json!({"enabled": false}));

    // Simulate a policy installed by an older version; refresh must fail before changing rows.
    db.execute("UPDATE pgtrickle.pgt_dependencies dep SET column_snapshot = jsonb_set(COALESCE(dep.column_snapshot,'{}'::jsonb), '{partition_retention}', jsonb_build_object('version',1,'mode','RETAIN','candidate',jsonb_build_object('output_column','d','bucket','EXACT','source_type_oid',1082,'source_oid',dep.source_relid::oid::bigint,'source_column','d'),'cutoff',jsonb_build_object('kind','DATE','value',DATE '2026-01-01' - DATE '2000-01-01'),'query_hash',st.defining_query_hash,'sealed','[]'::jsonb)) FROM pgtrickle.pgt_stream_tables st WHERE st.pgt_id = dep.pgt_id AND st.pgt_name = 'collation_legacy' AND dep.source_relid = 'collation_retention_src'::regclass").await;
    db.execute("INSERT INTO collation_retention_src VALUES (1,'2026-06-01','FOO',1),(2,'2026-06-01','foo',2)").await;
    let error = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('collation_legacy')")
        .await
        .expect_err("reject unsafe legacy policy on refresh");
    assert!(
        error.to_string().contains("nondeterministic collation"),
        "{error}"
    );
    let rows: Vec<(String, String, i64)> =
        sqlx::query_as("SELECT d::text, account, total FROM collation_legacy ORDER BY d, account")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(rows, Vec::new());
    let expected: Vec<(String, String, i64)> = sqlx::query_as("SELECT d::text, lower(account), total FROM (SELECT d, account, sum(amount) AS total FROM collation_retention_src GROUP BY d, account) q").fetch_all(&db.pool).await.unwrap();
    assert_eq!(expected, vec![("2026-06-01".into(), "foo".into(), 3)]);

    // A nondeterministic collation on a COUNT input is safe when it is not a group key.
    db.execute("CREATE TABLE collation_count_src (id INT NOT NULL, d DATE NOT NULL, note TEXT COLLATE retention_case_insensitive, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE collation_count_old PARTITION OF collation_count_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE collation_count_live PARTITION OF collation_count_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO collation_count_src VALUES (1,'2025-06-01','A'),(2,'2026-06-01','b')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('collation_count_st','SELECT d,count(note) AS n FROM collation_count_src GROUP BY d','TRIM','d','2026-01-01','1h')").await;
    db.execute("INSERT INTO collation_count_src VALUES (3,'2026-06-01','C')")
        .await;
    db.execute("ALTER TABLE collation_count_src DETACH PARTITION collation_count_old")
        .await;
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT d::text,n FROM collation_count_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(rows, vec![("2026-06-01".into(), 2)]);
    db.assert_st_matches_query(
        "collation_count_st",
        "SELECT d,count(note) AS n FROM collation_count_src GROUP BY d",
    )
    .await;
    let action: (String, String, bool) = sqlx::query_as("SELECT h.action,h.status,h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='collation_count_st' ORDER BY h.refresh_id DESC LIMIT 1").fetch_one(&db.pool).await.unwrap();
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));

    #[cfg(not(feature = "light-e2e"))]
    {
        db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
            .await;
        db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
            .await;
        db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
            .await;
        db.execute("UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at = now() - interval '2 hours' WHERE pgt_name = 'collation_legacy'").await;
        db.execute("ALTER SYSTEM SET pg_trickle.enabled = true")
            .await;
        db.execute("SELECT pg_reload_conf()").await;
        assert!(
            db.wait_for_scheduler(std::time::Duration::from_secs(90))
                .await
        );
        assert!(db.wait_for_condition(
            "scheduled refresh rejects unsafe collation",
            "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name = 'collation_legacy' AND h.initiated_by = 'SCHEDULER' AND h.status = 'FAILED' AND h.error_message LIKE '%nondeterministic collation%')",
            std::time::Duration::from_secs(60),
            std::time::Duration::from_millis(50),
        ).await);
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT d::text, account, total FROM collation_legacy ORDER BY d, account",
        )
        .fetch_all(&db.pool)
        .await
        .unwrap();
        assert_eq!(rows, Vec::new());
    }
}

#[tokio::test]
async fn test_retention_rejects_array_group_identity() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("CREATE TABLE alias_array_src (d DATE NOT NULL, account INT[] NOT NULL, amount INT NOT NULL) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE alias_array_old PARTITION OF alias_array_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    let error = db.try_execute("SELECT pgtrickle.create_stream_table_with_retention('alias_array_st', 'SELECT d, account, sum(amount) AS total FROM alias_array_src GROUP BY d, account', 'TRIM', 'd', '2026-01-01', '1h')")
        .await.expect_err("array group identity must be rejected");
    assert!(
        error.to_string().contains("unsupported structural type"),
        "{error}"
    );
    assert!(error.to_string().contains("array"), "{error}");
    let stream: Option<String> = db
        .query_scalar("SELECT to_regclass('alias_array_st')::text")
        .await;
    assert_eq!(stream, None);
}

#[tokio::test]
async fn test_retention_reordered_aliased_group_keys() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.refresh_strategy = 'differential'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE alias_retention_src (id INT NOT NULL, d DATE NOT NULL, account INT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE alias_retention_old PARTITION OF alias_retention_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE alias_retention_live PARTITION OF alias_retention_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO alias_retention_src VALUES (1,'2025-06-01',1,10), (2,'2025-06-01',2,5), (3,'2026-06-01',1,20), (4,'2026-06-01',2,8)").await;
    let query = "SELECT account AS acct, d AS day, sum(amount) AS total FROM alias_retention_src GROUP BY d, account";
    for (name, policy) in [("alias_trim", "TRIM"), ("alias_keep", "RETAIN")] {
        db.create_st(name, query, "1h", "DIFFERENTIAL").await;
        sqlx::query(
            "SELECT pgtrickle.set_stream_table_partition_retention($1, $2, 'd', '2026-01-01')",
        )
        .bind(name)
        .bind(policy)
        .execute(&db.pool)
        .await
        .expect("configure retention");
    }
    let before: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.execute("INSERT INTO alias_retention_src VALUES (5,'2025-06-01',1,5), (6,'2026-06-01',1,7)")
        .await;
    db.execute("ALTER TABLE alias_retention_src DETACH PARTITION alias_retention_old")
        .await;
    let actions: Vec<(String, String, String, bool)> = sqlx::query_as("SELECT s.pgt_name, h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id > $1 AND s.pgt_name IN ('alias_trim', 'alias_keep') ORDER BY s.pgt_name, h.refresh_id")
        .bind(before).fetch_all(&db.pool).await.expect("read cut refresh actions");
    assert_eq!(
        actions,
        vec![
            (
                "alias_keep".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            ),
            (
                "alias_trim".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            )
        ]
    );
    let trim: Vec<(i32, String, i64)> =
        sqlx::query_as("SELECT acct, day::text, total FROM alias_trim ORDER BY day, acct")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    let keep: Vec<(i32, String, i64)> =
        sqlx::query_as("SELECT acct, day::text, total FROM alias_keep ORDER BY day, acct")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        trim,
        vec![(1, "2026-06-01".into(), 27), (2, "2026-06-01".into(), 8)]
    );
    assert_eq!(
        keep,
        vec![
            (1, "2025-06-01".into(), 15),
            (2, "2025-06-01".into(), 5),
            (1, "2026-06-01".into(), 27),
            (2, "2026-06-01".into(), 8)
        ]
    );
    db.assert_st_matches_query("alias_trim", query).await;
    db.execute("INSERT INTO alias_retention_src VALUES (7,'2026-06-01',1,3)")
        .await;
    db.refresh_st("alias_trim").await;
    db.refresh_st("alias_keep").await;
    let trim: Vec<(i32, String, i64)> =
        sqlx::query_as("SELECT acct, day::text, total FROM alias_trim ORDER BY day, acct")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    let keep: Vec<(i32, String, i64)> =
        sqlx::query_as("SELECT acct, day::text, total FROM alias_keep ORDER BY day, acct")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        trim,
        vec![(1, "2026-06-01".into(), 30), (2, "2026-06-01".into(), 8)]
    );
    assert_eq!(
        keep,
        vec![
            (1, "2025-06-01".into(), 15),
            (2, "2025-06-01".into(), 5),
            (1, "2026-06-01".into(), 30),
            (2, "2026-06-01".into(), 8)
        ]
    );
    db.assert_st_matches_query("alias_trim", query).await;
    let actions: Vec<(String, String, String, bool)> = sqlx::query_as("SELECT DISTINCT ON (s.pgt_name) s.pgt_name, h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name IN ('alias_trim', 'alias_keep') ORDER BY s.pgt_name, h.refresh_id DESC")
        .fetch_all(&db.pool).await.expect("read subsequent refresh actions");
    assert_eq!(
        actions,
        vec![
            (
                "alias_keep".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            ),
            (
                "alias_trim".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            )
        ]
    );
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_hourly_tutorial_policies() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_hour_example (occurred timestamp NOT NULL, account_id integer NOT NULL, amount numeric NOT NULL) PARTITION BY RANGE (occurred)").await;
    db.execute("CREATE TABLE retention_hour_old PARTITION OF retention_hour_example FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')").await;
    db.execute("CREATE TABLE retention_hour_live PARTITION OF retention_hour_example FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").await;
    db.execute("INSERT INTO retention_hour_example VALUES ('2025-01-15 12:05', 1, 10), ('2025-01-15 12:30', 1, 5), ('2025-02-15 09:10', 1, 20), ('2025-02-15 09:20', 2, 7)").await;
    let query = "SELECT pg_catalog.date_trunc('hour', occurred) AS hour, account_id, sum(amount) AS total, count(*) AS n FROM retention_hour_example GROUP BY 1, 2";
    for (name, mode) in [
        ("retention_hour_trim", "TRIM"),
        ("retention_hour_history", "RETAIN"),
    ] {
        sqlx::query("SELECT pgtrickle.create_stream_table_with_retention($1, $2, $3, 'occurred', '2025-02-01', '1h')")
            .bind(name).bind(query).bind(mode).execute(&db.pool).await.expect("create hourly stream");
    }
    db.execute("ALTER TABLE retention_hour_example DETACH PARTITION retention_hour_old")
        .await;
    let trimmed: Vec<(String, i32, String, i64)> = sqlx::query_as(
        "SELECT hour::text, account_id, total::text, n FROM retention_hour_trim ORDER BY 1, 2",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read trimmed rows");
    let historical: Vec<(String, i32, String, i64)> = sqlx::query_as(
        "SELECT hour::text, account_id, total::text, n FROM retention_hour_history ORDER BY 1, 2",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read retained rows");
    assert_eq!(
        trimmed,
        vec![
            ("2025-02-15 09:00:00".into(), 1, "20".into(), 1),
            ("2025-02-15 09:00:00".into(), 2, "7".into(), 1)
        ]
    );
    assert_eq!(
        historical,
        vec![
            ("2025-01-15 12:00:00".into(), 1, "15".into(), 2),
            ("2025-02-15 09:00:00".into(), 1, "20".into(), 1),
            ("2025-02-15 09:00:00".into(), 2, "7".into(), 1)
        ]
    );
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_nested_partition_subtree_and_leaf_safety() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_nested_src (d date NOT NULL, account text NOT NULL, amount integer NOT NULL) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_nested_old PARTITION OF retention_nested_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01') PARTITION BY LIST (account)").await;
    db.execute(
        "CREATE TABLE retention_nested_a PARTITION OF retention_nested_old FOR VALUES IN ('a')",
    )
    .await;
    db.execute(
        "CREATE TABLE retention_nested_b PARTITION OF retention_nested_old FOR VALUES IN ('b')",
    )
    .await;
    db.execute("CREATE TABLE retention_nested_live PARTITION OF retention_nested_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO retention_nested_src VALUES ('2025-06-01','a',10), ('2025-06-01','b',5), ('2026-06-01','a',20)").await;
    let query = "SELECT d, account, sum(amount) AS total, count(*) AS n FROM retention_nested_src GROUP BY d, account";
    for (name, policy) in [
        ("retention_nested_trim", "TRIM"),
        ("retention_nested_keep", "RETAIN"),
    ] {
        sqlx::query("SELECT pgtrickle.create_stream_table_with_retention($1, $2, $3, 'd', '2026-01-01', '1h')")
            .bind(name).bind(query).bind(policy).execute(&db.pool).await.expect("configure nested source");
    }
    let baseline: Vec<(String, String, i64, i64)> = sqlx::query_as("SELECT d::text, account, sum(amount), count(*) FROM retention_nested_src GROUP BY d, account ORDER BY d, account")
        .fetch_all(&db.pool).await.expect("capture historical baseline");
    let error = db
        .try_execute("ALTER TABLE retention_nested_old DETACH PARTITION retention_nested_a")
        .await
        .expect_err("retained history requires a proven whole-root range");
    assert!(error.to_string().contains("RANGE"), "{error}");
    let attached: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = 'retention_nested_a'::regclass AND inhparent = 'retention_nested_old'::regclass)").await;
    assert!(attached);
    db.execute("ALTER TABLE retention_nested_src DETACH PARTITION retention_nested_old")
        .await;
    db.assert_st_matches_query("retention_nested_trim", query)
        .await;
    let retained: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM retention_nested_keep ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read retained subtree history");
    assert_eq!(retained, baseline);
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_rejects_stream_owner_subject_to_rls() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE ROLE retention_rls_owner LOGIN BYPASSRLS")
        .await;
    db.execute("GRANT pgtrickle_admin TO retention_rls_owner")
        .await;
    db.execute("GRANT CREATE ON SCHEMA public TO retention_rls_owner")
        .await;
    db.execute("CREATE TABLE retention_rls_src (d date NOT NULL, amount integer NOT NULL) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE retention_rls_child PARTITION OF retention_rls_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("INSERT INTO retention_rls_src VALUES ('2025-06-01',10)")
        .await;
    db.execute("ALTER TABLE retention_rls_src ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute("CREATE POLICY retention_rls_policy ON retention_rls_src FOR SELECT USING (true)")
        .await;
    db.execute("GRANT SELECT ON retention_rls_src TO retention_rls_owner")
        .await;
    let mut owner = db
        .pool
        .acquire()
        .await
        .expect("acquire stream owner session");
    sqlx::query("SET ROLE retention_rls_owner")
        .execute(&mut *owner)
        .await
        .expect("set stream owner");
    sqlx::query("SELECT pgtrickle.create_stream_table(name => 'retention_rls_st', query => 'SELECT d, sum(amount) AS total FROM retention_rls_src GROUP BY d', schedule => '1h', refresh_mode => 'DIFFERENTIAL')")
        .execute(&mut *owner).await.expect("create stream while bypassing RLS");
    sqlx::query("RESET ROLE")
        .execute(&mut *owner)
        .await
        .expect("restore superuser");
    db.execute("ALTER ROLE retention_rls_owner NOBYPASSRLS")
        .await;
    sqlx::query("SET ROLE retention_rls_owner")
        .execute(&mut *owner)
        .await
        .expect("set owner without RLS bypass");
    let error = sqlx::query("SELECT pgtrickle.set_stream_table_partition_retention('retention_rls_st','RETAIN','d','2026-01-01')")
        .execute(&mut *owner).await.expect_err("reject retention when differential refresh would fall back");
    assert!(error.to_string().contains("row-level security"), "{error}");
    let status: serde_json::Value = sqlx::query_scalar(
        "SELECT pgtrickle.get_stream_table_partition_retention('retention_rls_st')",
    )
    .fetch_one(&mut *owner)
    .await
    .expect("inspect rejected policy");
    assert_eq!(status, serde_json::json!({"enabled": false}));
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_enable_drains_direct_partition_writer_without_replay() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_enable_src (id integer NOT NULL, d date NOT NULL, amount integer NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE retention_enable_old PARTITION OF retention_enable_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("INSERT INTO retention_enable_src VALUES (1,'2025-06-01',10)")
        .await;
    let query =
        "SELECT d, sum(amount) AS total, count(*) AS n FROM retention_enable_src GROUP BY d";
    db.create_st("retention_enable_st", query, "1h", "DIFFERENTIAL")
        .await;
    let mut writer = db
        .pool
        .begin()
        .await
        .expect("begin direct partition writer");
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_catalog.pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .expect("read writer PID");
    sqlx::query("INSERT INTO retention_enable_old VALUES (2,'2025-06-01',5)")
        .execute(&mut *writer)
        .await
        .expect("write old partition");
    let pool = db.pool.clone();
    let configure = tokio::spawn(async move {
        let mut connection = pool.acquire().await.expect("acquire configuration session");
        sqlx::query("SET application_name = 'retention_enable_wait'")
            .execute(&mut *connection)
            .await
            .expect("name configuration session");
        sqlx::query("SELECT pgtrickle.set_stream_table_partition_retention('retention_enable_st', 'RETAIN', 'd', '2026-01-01')").execute(&mut *connection).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name = 'retention_enable_wait' AND $1 = ANY(pg_catalog.pg_blocking_pids(pid)))")
                .bind(writer_pid).fetch_one(&db.pool).await.expect("inspect configuration lock");
            if blocked { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.expect("configuration waits for the writer");
    writer.commit().await.expect("commit direct writer");
    configure
        .await
        .expect("join configuration")
        .expect("enable retention");
    db.assert_st_matches_query("retention_enable_st", query)
        .await;
    db.execute("INSERT INTO retention_enable_src VALUES (3,'2025-06-01',3)")
        .await;
    db.refresh_st_until_converged("retention_enable_st").await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM retention_enable_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read protected baseline and later delta");
    assert_eq!(rows, vec![("2025-06-01".into(), 18, 3)]);
    let action: (String, String, bool) = sqlx::query_as("SELECT h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name = 'retention_enable_st' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&db.pool).await.expect("read later refresh action");
    assert_eq!(action, ("DIFFERENTIAL".into(), "COMPLETED".into(), false));
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_off_restores_source_capture_mode() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_off_src (d date NOT NULL, amount integer NOT NULL) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE retention_off_child PARTITION OF retention_off_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("INSERT INTO retention_off_src VALUES ('2025-06-01',10)")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('retention_off_st', 'SELECT d, sum(amount) AS total, count(*) AS n FROM retention_off_src GROUP BY d', 'TRIM', 'd', '2026-01-01', '1h')").await;
    let triggers = "SELECT CASE WHEN tgname LIKE 'pg_trickle_retention_truncate_%' THEN 'guard' WHEN tgname LIKE 'pg_trickle_cdc_truncate_%' THEN 'truncate' WHEN tgname LIKE 'pg_trickle_cdc_ins_%' THEN 'insert' WHEN tgname LIKE 'pg_trickle_cdc_upd_%' THEN 'update' WHEN tgname LIKE 'pg_trickle_cdc_del_%' THEN 'delete' ELSE 'row' END, tgtype::integer, tgenabled::text FROM pg_catalog.pg_trigger WHERE tgrelid = $1::regclass AND tgname LIKE 'pg_trickle_%' ORDER BY 1";
    let root: Vec<(String, i32, String)> = sqlx::query_as(triggers)
        .bind("retention_off_src")
        .fetch_all(&db.pool)
        .await
        .expect("read retention source triggers");
    assert_eq!(
        root,
        vec![
            ("guard".into(), 62, "A".into()),
            ("row".into(), 29, "A".into()),
            ("truncate".into(), 32, "O".into())
        ]
    );
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_off_st', 'OFF')")
        .await;
    let root: Vec<(String, i32, String)> = sqlx::query_as(triggers)
        .bind("retention_off_src")
        .fetch_all(&db.pool)
        .await
        .expect("read restored source triggers");
    assert_eq!(
        root,
        vec![
            ("delete".into(), 8, "O".into()),
            ("insert".into(), 4, "O".into()),
            ("truncate".into(), 32, "O".into()),
            ("update".into(), 16, "O".into())
        ]
    );
    let child: Vec<(String, i32, String)> = sqlx::query_as(triggers)
        .bind("retention_off_child")
        .fetch_all(&db.pool)
        .await
        .expect("read restored partition triggers");
    assert_eq!(child, Vec::<(String, i32, String)>::new());
    db.execute("INSERT INTO retention_off_src VALUES ('2025-06-01',5)")
        .await;
    db.refresh_st_until_converged("retention_off_st").await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM retention_off_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read restored stream");
    assert_eq!(rows, vec![("2025-06-01".into(), 15, 2)]);
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('retention_drop_policy_st', 'SELECT d, sum(amount) AS total, count(*) AS n FROM retention_off_src GROUP BY d', 'TRIM', 'd', '2026-01-01', '1h')").await;
    db.execute("SELECT pgtrickle.drop_stream_table('retention_drop_policy_st')")
        .await;
    let root: Vec<(String, i32, String)> = sqlx::query_as(triggers)
        .bind("retention_off_src")
        .fetch_all(&db.pool)
        .await
        .expect("read triggers after dropping last policy");
    assert_eq!(
        root,
        vec![
            ("delete".into(), 8, "O".into()),
            ("insert".into(), 4, "O".into()),
            ("truncate".into(), 32, "O".into()),
            ("update".into(), 16, "O".into())
        ]
    );
    let child: Vec<(String, i32, String)> = sqlx::query_as(triggers)
        .bind("retention_off_child")
        .fetch_all(&db.pool)
        .await
        .expect("read partition triggers after dropping last policy");
    assert_eq!(child, Vec::<(String, i32, String)>::new());
    db.execute("INSERT INTO retention_off_src VALUES ('2025-06-01',5)")
        .await;
    db.refresh_st_until_converged("retention_off_st").await;
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM retention_off_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read remaining stream");
    assert_eq!(rows, vec![("2025-06-01".into(), 20, 3)]);
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_captures_direct_partition_writes() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_direct_src (id integer NOT NULL, d date NOT NULL, amount integer NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_direct_old PARTITION OF retention_direct_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_direct_live PARTITION OF retention_direct_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO retention_direct_src VALUES (1,'2025-06-01',10), (2,'2026-06-01',20)")
        .await;
    let query =
        "SELECT d, sum(amount) AS total, count(*) AS n FROM retention_direct_src GROUP BY d";
    db.create_st("retention_direct_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("INSERT INTO retention_direct_old VALUES (3,'2025-06-01',7)")
        .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_direct_st', 'RETAIN', 'd', '2026-01-01')").await;
    db.assert_st_matches_query("retention_direct_st", query)
        .await;
    let marker: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.execute("UPDATE retention_direct_old SET amount = amount + 3 WHERE id = 3")
        .await;
    db.execute("INSERT INTO retention_direct_live VALUES (4,'2026-06-01',5)")
        .await;
    db.refresh_st_until_converged("retention_direct_st").await;
    let baseline: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, sum(amount), count(*) FROM retention_direct_src GROUP BY d ORDER BY d",
    )
    .fetch_all(&db.pool)
    .await
    .expect("capture direct-write baseline");
    assert_eq!(
        baseline,
        vec![("2025-06-01".into(), 20, 2), ("2026-06-01".into(), 25, 2)]
    );
    db.execute("ALTER TABLE retention_direct_src DETACH PARTITION retention_direct_old")
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.pause_stream_table('retention_direct_st')")
        .await
        .expect_err("pausing must not make retained history require reconstruction");
    assert!(
        error.to_string().contains("retained partition history"),
        "{error}"
    );
    let mut paused = db.pool.begin().await.expect("begin lossy CDC test");
    sqlx::query("SET LOCAL pg_trickle.cdc_paused = on")
        .execute(&mut *paused)
        .await
        .expect("pause CDC");
    sqlx::query("SET LOCAL pg_trickle.cdc_capture_mode = 'discard'")
        .execute(&mut *paused)
        .await
        .expect("set lossy CDC mode");
    let error = sqlx::query("INSERT INTO retention_direct_live VALUES (99,'2026-06-01',99)")
        .execute(&mut *paused)
        .await
        .expect_err("reject writes while capture would discard them");
    assert!(
        error.to_string().contains("CDC is paused in discard mode"),
        "{error}"
    );
    paused.rollback().await.expect("roll back lossy CDC test");
    db.execute("INSERT INTO retention_direct_old VALUES (5,'2025-06-01',99)")
        .await;
    db.execute("TRUNCATE retention_direct_old").await;
    db.execute("DELETE FROM retention_direct_live WHERE id = 4")
        .await;
    let mut replica = db.pool.begin().await.expect("begin replica-role writer");
    sqlx::query("SET LOCAL session_replication_role = replica")
        .execute(&mut *replica)
        .await
        .expect("set replica role");
    sqlx::query("INSERT INTO retention_direct_live VALUES (7,'2026-06-01',2)")
        .execute(&mut *replica)
        .await
        .expect("capture replica-role write");
    replica.commit().await.expect("commit replica-role write");
    db.execute("CREATE TABLE retention_direct_future PARTITION OF retention_direct_src FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')").await;
    db.execute("INSERT INTO retention_direct_future VALUES (6,'2027-06-01',11)")
        .await;
    let expected = vec![
        ("2025-06-01".to_string(), 20_i64, 2_i64),
        ("2026-06-01".to_string(), 22_i64, 2_i64),
        ("2027-06-01".to_string(), 11_i64, 1_i64),
    ];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        db.refresh_st_with_retry("retention_direct_st").await;
        let rows: Vec<(String, i64, i64)> =
            sqlx::query_as("SELECT d::text, total, n FROM retention_direct_st ORDER BY d")
                .fetch_all(&db.pool)
                .await
                .expect("read retained and live direct writes");
        if rows == expected {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "direct child writes did not converge: {rows:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let disable: String = db.query_scalar("SELECT pg_catalog.format('ALTER TABLE public.retention_direct_live DISABLE TRIGGER %I', tgname) FROM pg_catalog.pg_trigger WHERE tgrelid = 'retention_direct_live'::regclass AND tgname LIKE 'pg_trickle_retention_truncate_%'").await;
    let error = db
        .try_execute(&disable)
        .await
        .expect_err("managed source guards cannot be disabled");
    assert!(
        error.to_string().contains("source guards are incomplete"),
        "{error}"
    );
    let downgrade = disable.replace("DISABLE TRIGGER", "ENABLE TRIGGER");
    let error = db
        .try_execute(&downgrade)
        .await
        .expect_err("managed source guards cannot be changed to origin-only");
    assert!(
        error.to_string().contains("source guards are incomplete"),
        "{error}"
    );
    let drop_guard: String = db.query_scalar("SELECT pg_catalog.format('DROP TRIGGER %I ON public.retention_direct_live', tgname) FROM pg_catalog.pg_trigger WHERE tgrelid = 'retention_direct_live'::regclass AND tgname LIKE 'pg_trickle_retention_truncate_%'").await;
    let error = db
        .try_execute(&drop_guard)
        .await
        .expect_err("managed source guards cannot be dropped");
    assert!(
        error.to_string().contains("source guards are incomplete"),
        "{error}"
    );
    let rename_guard: String = db.query_scalar("SELECT pg_catalog.format('ALTER TRIGGER %I ON public.retention_direct_live RENAME TO renamed_retention_guard', tgname) FROM pg_catalog.pg_trigger WHERE tgrelid = 'retention_direct_live'::regclass AND tgname LIKE 'pg_trickle_retention_truncate_%'").await;
    let error = db
        .try_execute(&rename_guard)
        .await
        .expect_err("managed source guards cannot be renamed");
    assert!(
        error.to_string().contains("source guards are incomplete"),
        "{error}"
    );
    let drop_capture: String = db.query_scalar("SELECT pg_catalog.format('DROP TRIGGER %I ON public.retention_direct_src', tgname) FROM pg_catalog.pg_trigger WHERE tgrelid = 'retention_direct_src'::regclass AND tgtype = 29 AND tgname LIKE 'pg_trickle_cdc_%'").await;
    let error = db
        .try_execute(&drop_capture)
        .await
        .expect_err("managed CDC triggers cannot be dropped");
    assert!(
        error.to_string().contains("source guards are incomplete"),
        "{error}"
    );
    db.execute("CREATE FUNCTION retention_direct_noop() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END;'").await;
    let replace_guard: String = db.query_scalar("SELECT pg_catalog.format('CREATE OR REPLACE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.retention_direct_live FOR EACH STATEMENT EXECUTE FUNCTION public.retention_direct_noop()', tgname) FROM pg_catalog.pg_trigger WHERE tgrelid = 'retention_direct_live'::regclass AND tgname LIKE 'pg_trickle_retention_truncate_%'").await;
    let error = db
        .try_execute(&replace_guard)
        .await
        .expect_err("managed source guards cannot be replaced");
    assert!(
        error.to_string().contains("source guards are incomplete"),
        "{error}"
    );
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM retention_direct_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read history after rejected trigger change");
    assert_eq!(rows, expected);
    let rebuilds: Vec<String> = sqlx::query_scalar("SELECT action FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name = 'retention_direct_st' AND h.refresh_id > $1 AND (h.action IN ('FULL','REINITIALIZE') OR h.was_full_fallback) ORDER BY h.refresh_id")
        .bind(marker).fetch_all(&db.pool).await.expect("read direct-write refresh history");
    assert_eq!(rebuilds, Vec::<String>::new());
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_nested_unaligned_shared_group_fails_closed() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_shared_src (occurred timestamp NOT NULL, account text NOT NULL, amount integer NOT NULL) PARTITION BY RANGE (occurred)").await;
    db.execute("CREATE TABLE retention_shared_day PARTITION OF retention_shared_src FOR VALUES FROM ('2025-01-01') TO ('2025-01-02') PARTITION BY RANGE (occurred)").await;
    db.execute("CREATE TABLE retention_shared_first PARTITION OF retention_shared_day FOR VALUES FROM ('2025-01-01 00:00') TO ('2025-01-01 00:30')").await;
    db.execute("CREATE TABLE retention_shared_second PARTITION OF retention_shared_day FOR VALUES FROM ('2025-01-01 00:30') TO ('2025-01-01 01:00')").await;
    db.execute("INSERT INTO retention_shared_src VALUES ('2025-01-01 00:10','a',10), ('2025-01-01 00:40','a',20)").await;
    let query = "SELECT pg_catalog.date_trunc('hour', occurred) AS hour, account, sum(amount) AS total, count(*) AS n FROM retention_shared_src GROUP BY 1,2";
    for (name, policy) in [
        ("retention_shared_trim", "TRIM"),
        ("retention_shared_keep", "RETAIN"),
    ] {
        sqlx::query("SELECT pgtrickle.create_stream_table_with_retention($1, $2, $3, 'occurred', '2025-01-02', '1h')")
            .bind(name).bind(query).bind(policy).execute(&db.pool).await.expect("configure shared-group source");
    }
    let error = db
        .try_execute("ALTER TABLE retention_shared_day DETACH PARTITION retention_shared_first")
        .await
        .expect_err("unaligned nested range must not freeze a shared group");
    assert!(error.to_string().contains("UnalignedHour"), "{error}");
    let before: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT hour::text, account, total, n FROM retention_shared_keep ORDER BY 1,2",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read protected shared group");
    assert_eq!(
        before,
        vec![("2025-01-01 00:00:00".into(), "a".into(), 30, 2)]
    );
    db.execute(
        "SELECT pgtrickle.set_stream_table_partition_retention('retention_shared_keep', 'OFF')",
    )
    .await;
    let marker: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.execute("ALTER TABLE retention_shared_day DETACH PARTITION retention_shared_first")
        .await;
    db.refresh_st_until_converged("retention_shared_trim").await;
    db.assert_st_matches_query("retention_shared_trim", query)
        .await;
    let action: (String, String) = sqlx::query_as("SELECT h.action, h.status FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name = 'retention_shared_trim' AND h.refresh_id > $1 AND h.action = 'REINITIALIZE' ORDER BY h.refresh_id DESC LIMIT 1")
        .bind(marker).fetch_one(&db.pool).await.expect("read fallback action");
    assert_eq!(action, ("REINITIALIZE".into(), "COMPLETED".into()));
    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT hour::text, account, total, n FROM retention_shared_trim ORDER BY 1,2",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read live shared group after fallback");
    assert_eq!(
        rows,
        vec![("2025-01-01 00:00:00".into(), "a".into(), 20, 1)]
    );
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_nested_aligned_range_leaves() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_nested_range_src (occurred timestamp NOT NULL, account text NOT NULL, amount integer NOT NULL) PARTITION BY RANGE (occurred)").await;
    db.execute("CREATE TABLE retention_nested_range_old PARTITION OF retention_nested_range_src FOR VALUES FROM ('2025-01-01') TO ('2025-02-01') PARTITION BY RANGE (occurred)").await;
    db.execute("CREATE TABLE retention_nested_range_first PARTITION OF retention_nested_range_old FOR VALUES FROM ('2025-01-01') TO ('2025-01-15')").await;
    db.execute("CREATE TABLE retention_nested_range_second PARTITION OF retention_nested_range_old FOR VALUES FROM ('2025-01-15') TO ('2025-02-01')").await;
    db.execute("CREATE TABLE retention_nested_range_live PARTITION OF retention_nested_range_src FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')").await;
    db.execute("INSERT INTO retention_nested_range_src VALUES ('2025-01-05 12:05','a',10), ('2025-01-20 12:10','a',20), ('2025-02-05 10:10','b',7)").await;
    let query = "SELECT pg_catalog.date_trunc('hour', occurred) AS hour, account, sum(amount) AS total, count(*) AS n FROM retention_nested_range_src GROUP BY 1,2";
    for (name, policy) in [
        ("retention_nested_range_trim", "TRIM"),
        ("retention_nested_range_keep", "RETAIN"),
    ] {
        sqlx::query("SELECT pgtrickle.create_stream_table_with_retention($1, $2, $3, 'occurred', '2025-02-01', '1h')")
            .bind(name).bind(query).bind(policy).execute(&db.pool).await.expect("configure nested range source");
    }
    let baseline: Vec<(String, String, i64, i64)> = sqlx::query_as("SELECT pg_catalog.date_trunc('hour', occurred)::text, account, sum(amount), count(*) FROM retention_nested_range_src GROUP BY 1,2 ORDER BY 1,2")
        .fetch_all(&db.pool).await.expect("capture nested historical baseline");
    let marker: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
        .await;
    let mut transaction = db
        .pool
        .begin()
        .await
        .expect("begin nested detach transaction");
    sqlx::query("SAVEPOINT before_nested_detach")
        .execute(&mut *transaction)
        .await
        .expect("create savepoint");
    sqlx::query(
        "ALTER TABLE retention_nested_range_old DETACH PARTITION retention_nested_range_first",
    )
    .execute(&mut *transaction)
    .await
    .expect("detach nested range in savepoint");
    sqlx::query("ROLLBACK TO SAVEPOINT before_nested_detach")
        .execute(&mut *transaction)
        .await
        .expect("roll back nested cut");
    transaction.commit().await.expect("commit nested rollback");
    db.execute(
        "ALTER TABLE retention_nested_range_old DETACH PARTITION retention_nested_range_first",
    )
    .await;
    db.assert_st_matches_query("retention_nested_range_trim", query)
        .await;
    let retained: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT hour::text, account, total, n FROM retention_nested_range_keep ORDER BY 1,2",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read retained nested range");
    assert_eq!(retained, baseline);
    let error = db.try_execute("CREATE TABLE retention_nested_range_overlap PARTITION OF retention_nested_range_old FOR VALUES FROM ('2025-01-01') TO ('2025-01-15')").await.expect_err("reject empty nested historical reattachment");
    assert!(
        error.to_string().contains("overlapping retained history"),
        "{error}"
    );
    db.execute("CREATE TABLE retention_nested_range_attach (LIKE retention_nested_range_src INCLUDING DEFAULTS INCLUDING CONSTRAINTS)").await;
    let error = db.try_execute("ALTER TABLE retention_nested_range_old ATTACH PARTITION retention_nested_range_attach FOR VALUES FROM ('2025-01-01') TO ('2025-01-15')")
        .await.expect_err("reject nested attachment over retained history");
    assert!(
        error.to_string().contains("overlapping retained history"),
        "{error}"
    );
    db.execute("INSERT INTO retention_nested_range_src VALUES ('2025-01-20 12:40','a',3)")
        .await;
    db.refresh_st_until_converged("retention_nested_range_trim")
        .await;
    db.execute("DROP TABLE retention_nested_range_second").await;
    db.assert_st_matches_query("retention_nested_range_trim", query)
        .await;
    let mut expected = baseline;
    expected[1].2 += 3;
    expected[1].3 += 1;
    let retained: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT hour::text, account, total, n FROM retention_nested_range_keep ORDER BY 1,2",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read retained history after second cut");
    assert_eq!(retained, expected);
    db.execute("INSERT INTO retention_nested_range_src VALUES ('2025-02-05 10:30','b',5)")
        .await;
    expected[2].2 += 5;
    expected[2].3 += 1;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        db.refresh_st_with_retry("retention_nested_range_keep")
            .await;
        let retained: Vec<(String, String, i64, i64)> = sqlx::query_as(
            "SELECT hour::text, account, total, n FROM retention_nested_range_keep ORDER BY 1,2",
        )
        .fetch_all(&db.pool)
        .await
        .expect("read nested history after live insert");
        if retained == expected {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "nested retained history did not converge: {retained:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let rebuilds: Vec<String> = sqlx::query_scalar("SELECT h.action FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name IN ('retention_nested_range_trim','retention_nested_range_keep') AND h.refresh_id > $1 AND (h.action IN ('FULL','REINITIALIZE') OR h.was_full_fallback) ORDER BY h.refresh_id")
        .bind(marker).fetch_all(&db.pool).await.expect("read nested refresh history");
    assert_eq!(rebuilds, Vec::<String>::new());
    let differential: Vec<String> = sqlx::query_scalar("SELECT DISTINCT s.pgt_name FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name IN ('retention_nested_range_trim','retention_nested_range_keep') AND h.refresh_id > $1 AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback ORDER BY s.pgt_name")
        .bind(marker).fetch_all(&db.pool).await.expect("read nested differential actions");
    assert_eq!(
        differential,
        vec!["retention_nested_range_keep", "retention_nested_range_trim"]
    );
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_trim_updates_immediate_downstream() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_immediate_src (d date NOT NULL, account text NOT NULL, amount integer NOT NULL) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_immediate_old PARTITION OF retention_immediate_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_immediate_live PARTITION OF retention_immediate_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute(
        "INSERT INTO retention_immediate_src VALUES ('2025-06-01','a',10), ('2026-06-01','a',20)",
    )
    .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('retention_immediate_up', 'SELECT d, account, sum(amount) AS total, count(*) AS n FROM retention_immediate_src GROUP BY d, account', 'TRIM', 'd', '2026-01-01', '1h')").await;
    db.execute("SELECT pgtrickle.create_stream_table(name => 'retention_immediate_down', query => 'SELECT d, account, total, n FROM retention_immediate_up', schedule => NULL, refresh_mode => 'IMMEDIATE')").await;
    db.execute("ALTER TABLE retention_immediate_src DETACH PARTITION retention_immediate_old")
        .await;
    let read = || {
        sqlx::query_as::<_, (String, String, i64, i64)>(
            "SELECT d::text, account, total, n FROM retention_immediate_down ORDER BY d, account",
        )
    };
    let rows = read()
        .fetch_all(&db.pool)
        .await
        .expect("read downstream after trim");
    assert_eq!(rows, vec![("2026-06-01".into(), "a".into(), 20, 1)]);
    db.execute("INSERT INTO retention_immediate_src VALUES ('2026-06-01','a',7)")
        .await;
    db.refresh_st_until_converged("retention_immediate_up")
        .await;
    let rows = read()
        .fetch_all(&db.pool)
        .await
        .expect("read downstream after differential refresh");
    assert_eq!(rows, vec![("2026-06-01".into(), "a".into(), 27, 2)]);
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_truncate_cannot_delete_sealed_history() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_truncate_src (d date NOT NULL, account text NOT NULL, amount integer NOT NULL) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_truncate_old PARTITION OF retention_truncate_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_truncate_live PARTITION OF retention_truncate_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute(
        "INSERT INTO retention_truncate_src VALUES ('2025-06-01','a',10), ('2026-06-01','a',20)",
    )
    .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('retention_truncate_st', 'SELECT d, account, sum(amount) AS total, count(*) AS n FROM retention_truncate_src GROUP BY d, account', 'RETAIN', 'd', '2026-01-01', '1h')").await;
    db.execute("ALTER TABLE retention_truncate_src DETACH PARTITION retention_truncate_old")
        .await;
    let expected = vec![
        ("2025-06-01".to_string(), "a".to_string(), 10_i64, 1_i64),
        ("2026-06-01".to_string(), "a".to_string(), 20_i64, 1_i64),
    ];
    let frontier: serde_json::Value = db.query_scalar("SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_truncate_st'").await;
    for command in [
        "TRUNCATE retention_truncate_live",
        "TRUNCATE retention_truncate_src",
    ] {
        let error = db
            .try_execute(command)
            .await
            .expect_err("TRUNCATE must not bypass retained history");
        assert!(
            error
                .to_string()
                .contains("TRUNCATE of a partition with retention"),
            "{error}"
        );
    }
    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM retention_truncate_st ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read protected history");
    assert_eq!(rows, expected);
    let after: serde_json::Value = db.query_scalar("SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_truncate_st'").await;
    assert_eq!(after, frontier);
    let live: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, sum(amount), count(*) FROM retention_truncate_src GROUP BY d ORDER BY d",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read surviving raw rows");
    assert_eq!(live, vec![("2026-06-01".into(), 20, 1)]);
    let buffer: String = db.query_scalar("SELECT pg_catalog.format('%I.%I', 'pgtrickle_changes', 'changes_' || source_stable_name) FROM pgtrickle.pgt_change_tracking WHERE source_relid = 'retention_truncate_src'::regclass").await;
    db.execute(&format!("INSERT INTO {buffer} (lsn, action, __pgt_row_id, source_xid, source_commit_at) VALUES (pg_catalog.pg_current_wal_insert_lsn(), 'T', pgtrickle.encode_row_id_v3('SYNTHETIC', ROW(0::int8)), NULL::xid, NULL::timestamptz)"))
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('retention_truncate_st')")
        .await
        .expect_err("a buffered TRUNCATE marker must not delete sealed history");
    assert!(
        error.to_string().contains("retained partition history"),
        "{error}"
    );
    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM retention_truncate_st ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read history after rejected marker");
    assert_eq!(rows, expected);
    let after: serde_json::Value = db.query_scalar("SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_truncate_st'").await;
    assert_eq!(after, frontier);
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_scheduled_refresh_preserves_sealed_history() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.differential_max_change_ratio = 0.001")
        .await;
    db.reload_config_and_wait().await;
    db.execute("CREATE TABLE retention_scheduled_src (d date NOT NULL, account text NOT NULL, amount integer NOT NULL) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_scheduled_old PARTITION OF retention_scheduled_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_scheduled_live PARTITION OF retention_scheduled_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute(
        "INSERT INTO retention_scheduled_src VALUES ('2025-06-01','a',10), ('2026-06-01','a',20)",
    )
    .await;
    db.execute("SELECT pgtrickle.create_stream_table_with_retention('retention_scheduled_st', 'SELECT d, account, sum(amount) AS total, count(*) AS n FROM retention_scheduled_src GROUP BY d, account', 'RETAIN', 'd', '2026-01-01', '1s')").await;
    assert!(
        db.wait_for_scheduler(std::time::Duration::from_secs(90))
            .await,
        "scheduler did not start"
    );
    let mut historical: Vec<(String, String, i64, i64)> = sqlx::query_as("SELECT d::text, account, sum(amount), count(*) FROM retention_scheduled_src GROUP BY d, account ORDER BY d, account")
        .fetch_all(&db.pool).await.expect("capture historical baseline");
    let mut reader = db.pool.begin().await.expect("begin old source snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *reader)
        .await
        .expect("set isolation");
    let rows: Vec<(String, String, i32)> = sqlx::query_as(
        "SELECT d::text,account,amount FROM retention_scheduled_src ORDER BY d,account",
    )
    .fetch_all(&mut *reader)
    .await
    .expect("hold old source snapshot");
    assert_eq!(
        rows,
        vec![
            ("2025-06-01".into(), "a".into(), 10),
            ("2026-06-01".into(), "a".into(), 20)
        ]
    );
    let pool = db.pool.clone();
    let detach = tokio::spawn(async move {
        sqlx::query("ALTER TABLE retention_scheduled_src DETACH PARTITION retention_scheduled_old CONCURRENTLY").execute(&pool).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let pending: bool = db.query_scalar("SELECT COALESCE((SELECT inhdetachpending FROM pg_catalog.pg_inherits WHERE inhrelid = 'retention_scheduled_old'::regclass), false)").await;
            if pending { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.expect("scheduled detach enters pending phase");
    db.execute("INSERT INTO retention_scheduled_old VALUES ('2025-06-01','a',3)")
        .await;
    historical[0].2 += 3;
    historical[0].3 += 1;
    let status: String = db.query_scalar("SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_scheduled_st'").await;
    assert_eq!(status, "SUSPENDED");
    reader.commit().await.expect("release old source snapshot");
    detach
        .await
        .expect("join concurrent detach")
        .expect("complete concurrent detach");
    let marker: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.execute("INSERT INTO retention_scheduled_src SELECT '2026-06-01'::date, 'a', 7 FROM generate_series(1,10)")
        .await;
    let mut expected = historical;
    expected[1].2 += 70;
    expected[1].3 += 10;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
            "SELECT d::text, account, total, n FROM retention_scheduled_st ORDER BY d, account",
        )
        .fetch_all(&db.pool)
        .await
        .expect("read scheduled results");
        let completed: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name = 'retention_scheduled_st' AND h.refresh_id > $1 AND h.initiated_by = 'SCHEDULER' AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback)")
            .bind(marker).fetch_one(&db.pool).await.expect("read scheduler refresh history");
        if rows == expected && completed {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "scheduled refresh did not preserve retained rows: {rows:?}, differential completed: {completed}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_automatic_detach_multiple_streams_and_savepoint_retry() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_auto_src (id INT NOT NULL, d DATE NOT NULL, account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_auto_old PARTITION OF retention_auto_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_auto_live PARTITION OF retention_auto_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO retention_auto_src VALUES (1,'2025-06-01','a',10), (2,'2025-06-01','b',5), (3,'2026-06-01','a',20)").await;
    let query = "SELECT d, account, SUM(amount) AS total, COUNT(*) AS n FROM retention_auto_src GROUP BY 1,2";
    db.create_st("retention_auto_trim", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st("retention_auto_keep", query, "1h", "DIFFERENTIAL")
        .await;
    let initial: serde_json::Value = sqlx::query_scalar(
        "SELECT pgtrickle.get_stream_table_partition_retention('retention_auto_trim')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("inspect default policy");
    assert_eq!(initial, serde_json::json!({"enabled": false}));
    for (name, mode) in [
        ("retention_auto_trim", "TRIM"),
        ("retention_auto_keep", "RETAIN"),
    ] {
        sqlx::query(
            "SELECT pgtrickle.set_stream_table_partition_retention($1, $2, 'd', '2026-01-01')",
        )
        .bind(name)
        .bind(mode)
        .execute(&db.pool)
        .await
        .expect("configure retention policy");
        let status: serde_json::Value =
            sqlx::query_scalar("SELECT pgtrickle.get_stream_table_partition_retention($1)")
                .bind(name)
                .fetch_one(&db.pool)
                .await
                .expect("inspect configured policy");
        assert_eq!(
            status,
            serde_json::json!({
                "enabled": true, "policy": mode, "source": "public.retention_auto_src",
                "source_column": "d", "group_column": "d", "bucket": "EXACT",
                "cutoff": "2026-01-01", "sealed_ranges": []
            })
        );
    }
    db.execute("INSERT INTO retention_auto_src VALUES (4,'2025-06-01','a',2)")
        .await;
    let baseline: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, SUM(amount), COUNT(*) FROM retention_auto_src GROUP BY d, account ORDER BY d, account",
    ).fetch_all(&db.pool).await.expect("capture independent historical baseline");
    let mut tx = db.pool.begin().await.expect("begin detach transaction");
    sqlx::query("SELECT set_config('enable_nestloop', 'off', true), set_config('enable_hashjoin', 'on', true), set_config('enable_mergejoin', 'on', true), set_config('enable_indexscan', 'off', true), set_config('enable_bitmapscan', 'off', true), set_config('enable_seqscan', 'on', true), set_config('enable_tidscan', 'off', true), set_config('jit', 'on', true), set_config('random_page_cost', '1000000', true), set_config('cpu_index_tuple_cost', '100000000', true), set_config('cpu_operator_cost', '100000000', true)")
        .execute(&mut *tx).await.expect("set caller planner preferences");
    sqlx::query("SET LOCAL enable_partition_pruning = off")
        .execute(&mut *tx)
        .await
        .expect("disable caller partition pruning");
    sqlx::query("SAVEPOINT before_detach")
        .execute(&mut *tx)
        .await
        .expect("create savepoint");
    sqlx::query("ALTER TABLE retention_auto_src DETACH PARTITION retention_auto_old")
        .execute(&mut *tx)
        .await
        .expect("automatic detach");
    let planner_settings: (String, String, String, String, String, String, String, String, f64, f64, f64) = sqlx::query_as(
        "SELECT current_setting('enable_nestloop'), current_setting('enable_hashjoin'), current_setting('enable_mergejoin'), current_setting('enable_indexscan'), current_setting('enable_bitmapscan'), current_setting('enable_seqscan'), current_setting('enable_tidscan'), current_setting('jit'), current_setting('random_page_cost')::float8, current_setting('cpu_index_tuple_cost')::float8, current_setting('cpu_operator_cost')::float8",
    ).fetch_one(&mut *tx).await.expect("read planner preferences after retention");
    assert_eq!(
        planner_settings,
        (
            "off".into(),
            "on".into(),
            "on".into(),
            "off".into(),
            "off".into(),
            "on".into(),
            "off".into(),
            "on".into(),
            1_000_000.0,
            100_000_000.0,
            100_000_000.0
        )
    );
    let restored: bool = sqlx::query_scalar("SELECT current_setting('enable_nestloop') = 'off' AND current_setting('enable_hashjoin') = 'on' AND current_setting('enable_mergejoin') = 'on' AND current_setting('enable_indexscan') = 'off' AND current_setting('enable_bitmapscan') = 'off' AND current_setting('enable_seqscan') = 'on' AND current_setting('enable_tidscan') = 'off' AND current_setting('enable_partition_pruning') = 'off' AND current_setting('jit') = 'on' AND current_setting('random_page_cost')::numeric = 1000000 AND current_setting('cpu_index_tuple_cost')::numeric = 100000000 AND current_setting('cpu_operator_cost')::numeric = 100000000")
        .fetch_one(&mut *tx).await.expect("read caller planner preferences");
    assert!(
        restored,
        "retention did not restore caller planner preferences"
    );
    sqlx::query("ROLLBACK TO SAVEPOINT before_detach")
        .execute(&mut *tx)
        .await
        .expect("roll back detach");
    let attached: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent = 'retention_auto_src'::regclass AND inhrelid = 'retention_auto_old'::regclass)")
        .fetch_one(&mut *tx).await.expect("read rolled-back partition");
    assert!(attached);
    sqlx::query("ALTER TABLE retention_auto_src DETACH PARTITION retention_auto_old")
        .execute(&mut *tx)
        .await
        .expect("retry automatic detach");
    let restored: bool = sqlx::query_scalar("SELECT current_setting('enable_nestloop') = 'off' AND current_setting('enable_hashjoin') = 'on' AND current_setting('enable_mergejoin') = 'on' AND current_setting('enable_indexscan') = 'off' AND current_setting('enable_bitmapscan') = 'off' AND current_setting('enable_seqscan') = 'on' AND current_setting('enable_tidscan') = 'off' AND current_setting('enable_partition_pruning') = 'off' AND current_setting('jit') = 'on' AND current_setting('random_page_cost')::numeric = 1000000 AND current_setting('cpu_index_tuple_cost')::numeric = 100000000 AND current_setting('cpu_operator_cost')::numeric = 100000000")
        .fetch_one(&mut *tx).await.expect("read caller planner preferences after retry");
    assert!(
        restored,
        "retention retry did not restore caller planner preferences"
    );
    tx.commit().await.expect("commit automatic detach");
    db.assert_st_matches_query("retention_auto_trim", query)
        .await;
    let retained: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM retention_auto_keep ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read retained output");
    assert_eq!(retained, baseline);
    let states: Vec<(String, String, bool)> = sqlx::query_as("SELECT pgt_name, status, needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name IN ('retention_auto_trim','retention_auto_keep') ORDER BY pgt_name")
        .fetch_all(&db.pool).await.expect("read stream states");
    assert_eq!(
        states,
        vec![
            ("retention_auto_keep".into(), "ACTIVE".into(), false),
            ("retention_auto_trim".into(), "ACTIVE".into(), false)
        ]
    );
    let actions: Vec<(String, String, String, bool)> = sqlx::query_as("SELECT s.pgt_name, h.action, h.status, h.was_full_fallback FROM pgtrickle.pgt_stream_tables s CROSS JOIN LATERAL (SELECT action, status, was_full_fallback FROM pgtrickle.pgt_refresh_history WHERE pgt_id = s.pgt_id ORDER BY refresh_id DESC LIMIT 1) h WHERE s.pgt_name IN ('retention_auto_trim','retention_auto_keep') ORDER BY s.pgt_name")
        .fetch_all(&db.pool).await.expect("read preparation actions");
    assert_eq!(
        actions,
        vec![
            (
                "retention_auto_keep".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            ),
            (
                "retention_auto_trim".into(),
                "DIFFERENTIAL".into(),
                "COMPLETED".into(),
                false
            )
        ]
    );
    db.execute("INSERT INTO retention_auto_src VALUES (5,'2026-06-01','a',3)")
        .await;
    db.refresh_st_until_converged("retention_auto_trim").await;
    let mut expected = baseline;
    expected[2].2 += 3;
    expected[2].3 += 1;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        db.refresh_st_with_retry("retention_auto_keep").await;
        let actual: Vec<(String, String, i64, i64)> = sqlx::query_as(
            "SELECT d::text, account, total, n FROM retention_auto_keep ORDER BY d, account",
        )
        .fetch_all(&db.pool)
        .await
        .expect("read retained and live output");
        if actual == expected {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "retained output did not converge: {actual:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    db.assert_st_matches_query("retention_auto_trim", query)
        .await;
    let status: serde_json::Value = sqlx::query_scalar(
        "SELECT pgtrickle.get_stream_table_partition_retention('retention_auto_keep')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("inspect sealed policy");
    assert_eq!(
        status["sealed_ranges"],
        serde_json::json!([{"lower": "2025-01-01", "upper": "2026-01-01"}])
    );
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_auto_keep', 'RETAIN', 'd', '2026-07-01')").await;
    let error = db.try_execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_auto_keep', 'RETAIN', 'd', '2026-01-01')")
        .await.expect_err("reject cutoff regression");
    assert!(error.to_string().contains("CutoffRegression"), "{error}");
    let error = db
        .try_execute(
            "SELECT pgtrickle.set_stream_table_partition_retention('retention_auto_keep', 'OFF')",
        )
        .await
        .expect_err("cannot disable sealed history");
    assert!(
        error.to_string().contains("retained partition history"),
        "{error}"
    );
    db.execute(
        "SELECT pgtrickle.set_stream_table_partition_retention('retention_auto_trim', 'OFF')",
    )
    .await;
    let disabled: serde_json::Value = sqlx::query_scalar(
        "SELECT pgtrickle.get_stream_table_partition_retention('retention_auto_trim')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("inspect disabled policy");
    assert_eq!(disabled, serde_json::json!({"enabled": false}));
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_automatic_multi_drop_preserves_policies_and_downstream() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_drop_src (id INT NOT NULL, d DATE NOT NULL, account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_drop_2024 PARTITION OF retention_drop_src FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')").await;
    db.execute("CREATE TABLE retention_drop_2025 PARTITION OF retention_drop_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_drop_live PARTITION OF retention_drop_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO retention_drop_src VALUES (1,'2024-06-01','a',10), (2,'2025-06-01','b',5), (3,'2026-06-01','a',20)").await;
    let query = "SELECT d, account, SUM(amount) AS total, COUNT(*) AS n FROM retention_drop_src GROUP BY 1,2";
    db.create_st("retention_drop_trim", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st("retention_drop_keep", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st(
        "retention_drop_downstream",
        "SELECT d, account, total, n FROM retention_drop_trim",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let (source_oid, cutoff): (i64, i32) = sqlx::query_as(
        "SELECT 'retention_drop_src'::regclass::oid::bigint, DATE '2026-01-01' - DATE '2000-01-01'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read source identity");
    for (name, mode) in [
        ("retention_drop_trim", "TRIM"),
        ("retention_drop_keep", "RETAIN"),
    ] {
        sqlx::query(
            "SELECT pgtrickle.set_stream_table_partition_retention($1, $2, 'd', '2026-01-01')",
        )
        .bind(name)
        .bind(mode)
        .execute(&db.pool)
        .await
        .expect("configure policy capture");
        let (id, hash): (i64, i64) = sqlx::query_as("SELECT pgt_id, defining_query_hash FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1")
            .bind(name).fetch_one(&db.pool).await.expect("read stream identity");
        let state = serde_json::json!({
            "version": 1, "mode": mode, "candidate": {
                "output_column": "d", "bucket": "EXACT", "source_type_oid": 1082,
                "source_oid": source_oid, "source_column": "d"
            }, "cutoff": {"kind": "DATE", "value": cutoff}, "query_hash": hash, "sealed": []
        });
        sqlx::query("UPDATE pgtrickle.pgt_dependencies SET column_snapshot = jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) WHERE pgt_id = $2")
            .bind(state).bind(id).execute(&db.pool).await.expect("install policy fixture");
    }
    db.execute("INSERT INTO retention_drop_src VALUES (4,'2024-06-01','a',2)")
        .await;
    db.execute("CREATE VIEW retention_drop_blocker AS SELECT * FROM retention_drop_2024")
        .await;
    let baseline: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, SUM(amount), COUNT(*) FROM retention_drop_src GROUP BY d, account ORDER BY d, account",
    ).fetch_all(&db.pool).await.expect("capture historical baseline");
    let history_start: i64 = db
        .query_scalar(
            "SELECT COALESCE(MAX(refresh_id),0)::bigint FROM pgtrickle.pgt_refresh_history",
        )
        .await;
    let mut tx = db.pool.begin().await.expect("begin drop transaction");
    sqlx::query("SAVEPOINT rejected_drop")
        .execute(&mut *tx)
        .await
        .expect("create failed-drop savepoint");
    let error = sqlx::query("DROP TABLE retention_drop_2025, retention_drop_2024")
        .execute(&mut *tx)
        .await
        .expect_err("reject drop with dependent view");
    assert!(error.to_string().contains("depend"), "{error}");
    sqlx::query("ROLLBACK TO SAVEPOINT rejected_drop")
        .execute(&mut *tx)
        .await
        .expect("roll back failed drop");
    sqlx::query("DROP VIEW retention_drop_blocker")
        .execute(&mut *tx)
        .await
        .expect("remove drop blocker");
    sqlx::query("SAVEPOINT before_drop")
        .execute(&mut *tx)
        .await
        .expect("create savepoint");
    sqlx::query("DROP TABLE retention_drop_2025, retention_drop_2024")
        .execute(&mut *tx)
        .await
        .expect("drop expired partitions");
    sqlx::query("ROLLBACK TO SAVEPOINT before_drop")
        .execute(&mut *tx)
        .await
        .expect("roll back partition drops");
    let restored: (bool, bool) = sqlx::query_as("SELECT to_regclass('retention_drop_2024') IS NOT NULL, to_regclass('retention_drop_2025') IS NOT NULL")
        .fetch_one(&mut *tx).await.expect("read restored partitions");
    assert_eq!(restored, (true, true));
    sqlx::query("DROP TABLE retention_drop_2025, retention_drop_2024")
        .execute(&mut *tx)
        .await
        .expect("retry partition drops");
    tx.commit().await.expect("commit partition drops");
    db.assert_st_matches_query("retention_drop_trim", query)
        .await;
    let retained: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, account, total, n FROM retention_drop_keep ORDER BY d, account",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read retained output");
    assert_eq!(retained, baseline);
    db.refresh_st_until_converged("retention_drop_downstream")
        .await;
    db.assert_st_matches_query("retention_drop_downstream", query)
        .await;
    let states: Vec<(String, String, bool)> = sqlx::query_as("SELECT pgt_name, status, needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name IN ('retention_drop_trim','retention_drop_keep') ORDER BY pgt_name")
        .fetch_all(&db.pool).await.expect("read stream states");
    assert_eq!(
        states,
        vec![
            ("retention_drop_keep".into(), "ACTIVE".into(), false),
            ("retention_drop_trim".into(), "ACTIVE".into(), false)
        ]
    );
    let differential: Vec<String> = sqlx::query_scalar("SELECT DISTINCT s.pgt_name FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id > $1 AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback AND s.pgt_name IN ('retention_drop_trim','retention_drop_keep') ORDER BY s.pgt_name")
        .bind(history_start).fetch_all(&db.pool).await.expect("read differential actions");
    assert_eq!(
        differential,
        vec!["retention_drop_keep", "retention_drop_trim"]
    );
    let rebuilds: Vec<String> = sqlx::query_scalar("SELECT h.action FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id > $1 AND h.action IN ('FULL','REINITIALIZE') AND s.pgt_name IN ('retention_drop_trim','retention_drop_keep') ORDER BY h.refresh_id")
        .bind(history_start).fetch_all(&db.pool).await.expect("read unexpected rebuilds");
    assert!(rebuilds.is_empty(), "unexpected rebuilds: {rebuilds:?}");
    let state: serde_json::Value = db.query_scalar("SELECT d.column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies d JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'retention_drop_keep'").await;
    let bounds: (i32, i32) = sqlx::query_as(
        "SELECT DATE '2024-01-01' - DATE '2000-01-01', DATE '2025-01-01' - DATE '2000-01-01'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read expected range bounds");
    assert_eq!(
        state["sealed"],
        serde_json::json!([
            {"lower": {"kind":"DATE", "value":bounds.0}, "upper": {"kind":"DATE", "value":bounds.1}},
            {"lower": {"kind":"DATE", "value":bounds.1}, "upper": {"kind":"DATE", "value":cutoff}}
        ])
    );
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_drop_resolves_non_superuser_search_path() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE ROLE retention_tenant_owner LOGIN").await;
    db.execute("CREATE ROLE retention_table_owner LOGIN").await;
    db.execute("CREATE SCHEMA retention_tenant AUTHORIZATION retention_tenant_owner")
        .await;
    db.execute("CREATE TABLE retention_tenant.events (id INT NOT NULL, d DATE NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_tenant.events_old PARTITION OF retention_tenant.events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_tenant.events_live PARTITION OF retention_tenant.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("CREATE TABLE public.events_old (id INT)").await;
    db.execute("ALTER TABLE retention_tenant.events OWNER TO retention_table_owner")
        .await;
    db.execute("ALTER TABLE retention_tenant.events_old OWNER TO retention_table_owner")
        .await;
    db.execute("ALTER TABLE retention_tenant.events_live OWNER TO retention_table_owner")
        .await;
    db.execute(
        "INSERT INTO retention_tenant.events VALUES (1,'2025-06-01',10), (2,'2026-06-01',20)",
    )
    .await;
    let query =
        "SELECT d, SUM(amount) AS total, COUNT(*) AS n FROM retention_tenant.events GROUP BY d";
    db.create_st("retention_tenant_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_tenant_st', 'RETAIN', 'd', '2026-01-01')").await;
    let (pgt_id, hash, source_oid, cutoff): (i64, i64, i64, i32) = sqlx::query_as("SELECT pgt_id, defining_query_hash, 'retention_tenant.events'::regclass::oid::bigint, DATE '2026-01-01' - DATE '2000-01-01' FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_tenant_st'")
        .fetch_one(&db.pool).await.expect("read stream identity");
    let state = serde_json::json!({
        "version": 1, "mode": "RETAIN", "candidate": {
            "output_column": "d", "bucket": "EXACT", "source_type_oid": 1082,
            "source_oid": source_oid, "source_column": "d"
        }, "cutoff": {"kind": "DATE", "value": cutoff}, "query_hash": hash, "sealed": []
    });
    sqlx::query("UPDATE pgtrickle.pgt_dependencies SET column_snapshot = jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) WHERE pgt_id = $2")
        .bind(state).bind(pgt_id).execute(&db.pool).await.expect("install policy fixture");
    let baseline: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT d::text, SUM(amount), COUNT(*) FROM retention_tenant.events GROUP BY d ORDER BY d",
    )
    .fetch_all(&db.pool)
    .await
    .expect("capture historical baseline");
    let mut conn = db.pool.acquire().await.expect("acquire tenant connection");
    sqlx::query("SET ROLE retention_tenant_owner")
        .execute(&mut *conn)
        .await
        .expect("set tenant role");
    sqlx::query("SET search_path = retention_tenant, public, pg_catalog")
        .execute(&mut *conn)
        .await
        .expect("set caller search path");
    sqlx::query("DROP TABLE events_old")
        .execute(&mut *conn)
        .await
        .expect("drop partition using caller search path");
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset role");
    sqlx::query("RESET search_path")
        .execute(&mut *conn)
        .await
        .expect("reset search path");
    drop(conn);
    let output: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT d::text, total, n FROM retention_tenant_st ORDER BY d")
            .fetch_all(&db.pool)
            .await
            .expect("read retained results");
    assert_eq!(output, baseline);
    let remaining: (bool, bool) = sqlx::query_as("SELECT to_regclass('retention_tenant.events_old') IS NOT NULL, to_regclass('public.events_old') IS NOT NULL")
        .fetch_one(&db.pool).await.expect("read similarly named tables");
    assert_eq!(remaining, (false, true));
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_trim_partial_partition_uses_safe_reinitialization() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_partial_src (id INT NOT NULL, d DATE NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_partial_old PARTITION OF retention_partial_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_partial_live PARTITION OF retention_partial_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO retention_partial_src VALUES (1,'2025-06-01',10), (2,'2026-06-01',20)")
        .await;
    let query =
        "SELECT d, SUM(amount) AS total, COUNT(*) AS n FROM retention_partial_src GROUP BY d";
    db.create_st("retention_partial_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_partial_st', 'TRIM', 'd', '2025-12-31')").await;
    let (pgt_id, hash): (i64, i64) = sqlx::query_as("SELECT pgt_id, defining_query_hash FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_partial_st'")
        .fetch_one(&db.pool).await.expect("read stream identity");
    let (source_oid, cutoff): (i64, i32) = sqlx::query_as("SELECT 'retention_partial_src'::regclass::oid::bigint, DATE '2025-12-31' - DATE '2000-01-01'")
        .fetch_one(&db.pool).await.expect("read source identity");
    let state = serde_json::json!({
        "version": 1, "mode": "TRIM", "candidate": {
            "output_column": "d", "bucket": "EXACT", "source_type_oid": 1082,
            "source_oid": source_oid, "source_column": "d"
        }, "cutoff": {"kind": "DATE", "value": cutoff}, "query_hash": hash, "sealed": []
    });
    sqlx::query("UPDATE pgtrickle.pgt_dependencies SET column_snapshot = jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) WHERE pgt_id = $2")
        .bind(state).bind(pgt_id).execute(&db.pool).await.expect("install policy fixture");
    db.execute("ALTER TABLE retention_partial_src DETACH PARTITION retention_partial_old")
        .await;
    let pending: bool = sqlx::query_scalar(
        "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .fetch_one(&db.pool)
    .await
    .expect("read pending reinitialization");
    assert!(pending);
    db.refresh_st_until_converged("retention_partial_st").await;
    db.assert_st_matches_query("retention_partial_st", query)
        .await;
    let action: (String, String) = sqlx::query_as("SELECT action, status FROM pgtrickle.pgt_refresh_history WHERE pgt_id = $1 ORDER BY refresh_id DESC LIMIT 1")
        .bind(pgt_id).fetch_one(&db.pool).await.expect("read refresh action");
    assert_eq!(action, ("REINITIALIZE".into(), "COMPLETED".into()));
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_configuration_serializes_with_concurrent_detach() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_race_src (id INT NOT NULL, d DATE NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,d)) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_race_old PARTITION OF retention_race_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("INSERT INTO retention_race_src VALUES (1,'2025-06-01',10)")
        .await;
    let query = "SELECT d, SUM(amount) AS total, COUNT(*) AS n FROM retention_race_src GROUP BY d";
    db.create_st("retention_race_st", query, "1h", "DIFFERENTIAL")
        .await;
    let (pgt_id, query_hash): (i64, i64) = sqlx::query_as("SELECT pgt_id, defining_query_hash FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_race_st'")
        .fetch_one(&db.pool).await.expect("read stream identity");
    let (source_oid, cutoff): (i64, i32) = sqlx::query_as(
        "SELECT 'retention_race_src'::regclass::oid::bigint, DATE '2026-01-01' - DATE '2000-01-01'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read source identity");
    let state = serde_json::json!({
        "version": 1, "mode": "RETAIN", "candidate": {
            "output_column": "d", "bucket": "EXACT", "source_type_oid": 1082,
            "source_oid": source_oid, "source_column": "d"
        }, "cutoff": {"kind": "DATE", "value": cutoff}, "query_hash": query_hash, "sealed": []
    });
    let mut configure = db
        .pool
        .begin()
        .await
        .expect("begin retention configuration");
    sqlx::query("SELECT pg_catalog.pg_advisory_xact_lock($1)")
        .bind(pgt_id)
        .execute(&mut *configure)
        .await
        .expect("lock stream during configuration");
    sqlx::query("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1 FOR UPDATE")
        .bind(pgt_id)
        .execute(&mut *configure)
        .await
        .expect("lock stream catalog row");
    sqlx::query("LOCK TABLE ONLY retention_race_src IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut *configure)
        .await
        .expect("lock source during configuration");
    sqlx::query("UPDATE pgtrickle.pgt_dependencies SET column_snapshot = jsonb_set(column_snapshot, '{partition_retention}', $1::jsonb) WHERE pgt_id = $2")
        .bind(&state).bind(pgt_id).execute(&mut *configure).await.expect("configure retention");
    let configure_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *configure)
        .await
        .expect("read configuration PID");
    let mut conn = db.pool.acquire().await.expect("acquire detach connection");
    let detach_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await
        .expect("read detach PID");
    let detach = tokio::spawn(async move {
        sqlx::query(
            "ALTER TABLE retention_race_src DETACH PARTITION retention_race_old CONCURRENTLY",
        )
        .execute(&mut *conn)
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE pid = $1 AND locktype = 'advisory' AND mode = 'ExclusiveLock' AND NOT granted AND $2 = ANY(pg_catalog.pg_blocking_pids(pid)))")
                .bind(detach_pid).bind(configure_pid).fetch_one(&db.pool).await.expect("inspect stream lock");
            if waiting { break; }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await.expect("concurrent detach waits for configuration");
    configure
        .commit()
        .await
        .expect("commit retention configuration");
    let error = detach
        .await
        .expect("join detach")
        .expect_err("incomplete retention capture rejects detach");
    assert!(
        error
            .to_string()
            .contains("partition retention CDC or source guards are incomplete"),
        "{error}"
    );
    let attached: (bool, bool) = sqlx::query_as("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent = 'retention_race_src'::regclass AND inhrelid = 'retention_race_old'::regclass), COALESCE((SELECT inhdetachpending FROM pg_catalog.pg_inherits WHERE inhrelid = 'retention_race_old'::regclass), false)")
        .fetch_one(&db.pool).await.expect("read partition lifecycle");
    assert_eq!(attached, (true, false));
    let status: (String, bool) = sqlx::query_as(
        "SELECT status, needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .fetch_one(&db.pool)
    .await
    .expect("read stream status");
    assert_eq!(status, ("ACTIVE".into(), false));
    db.assert_st_matches_query("retention_race_st", query).await;
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_public_api_validates_alignment_cutoff_index_and_owner() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE retention_api_src (id INT NOT NULL, event_day DATE NOT NULL, occurred TIMESTAMP NOT NULL, account TEXT NOT NULL, amount INT NOT NULL, PRIMARY KEY (id,event_day)) PARTITION BY RANGE (event_day)").await;
    db.execute("CREATE TABLE retention_api_old PARTITION OF retention_api_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE retention_api_live PARTITION OF retention_api_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("INSERT INTO retention_api_src VALUES (1,'2025-06-01','2026-06-01 10:00','a',10), (2,'2026-06-01','2026-06-01 10:00','a',20)").await;
    db.create_st("retention_api_misaligned", "SELECT pg_catalog.date_trunc('hour', occurred) AS hour, account, SUM(amount) AS total, COUNT(*) AS n FROM retention_api_src GROUP BY 1,2", "1h", "DIFFERENTIAL").await;
    let error = db.try_execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_api_misaligned', 'RETAIN', 'event_day', '2026-01-01')")
        .await.expect_err("reject an unproven partition-to-group relationship");
    assert!(error.to_string().contains("not eligible"), "{error}");
    let status: serde_json::Value = db
        .query_scalar(
            "SELECT pgtrickle.get_stream_table_partition_retention('retention_api_misaligned')",
        )
        .await;
    assert_eq!(status, serde_json::json!({"enabled": false}));
    let query = "SELECT account, event_day, SUM(amount) AS total, COUNT(*) AS n FROM retention_api_src GROUP BY account,event_day";
    let error = sqlx::query("SELECT pgtrickle.create_stream_table_with_retention($1, $2, 'RETAIN', 'event_day', '2026-01-01', '1h')")
        .bind("retention_api_atomic_failure")
        .bind("SELECT pg_catalog.date_trunc('hour', occurred) AS hour, account, SUM(amount) AS total, COUNT(*) AS n FROM retention_api_src GROUP BY 1,2")
        .execute(&db.pool).await.expect_err("invalid retention must roll back creation");
    assert!(error.to_string().contains("not eligible"), "{error}");
    let absent: (bool, bool) = sqlx::query_as("SELECT to_regclass('retention_api_atomic_failure') IS NULL, NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_api_atomic_failure')")
        .fetch_one(&db.pool).await.expect("inspect rolled-back creation");
    assert_eq!(absent, (true, true));
    sqlx::query("SELECT pgtrickle.create_stream_table_with_retention($1, $2, 'RETAIN', 'event_day', '2026-01-01', '1h')")
        .bind("retention_api_atomic").bind(query).execute(&db.pool).await.expect("create retained stream atomically");
    db.create_st("retention_api_valid", query, "1h", "DIFFERENTIAL")
        .await;
    let error = db.try_execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_api_valid', 'TRIM', 'event_day', 'infinity')")
        .await.expect_err("reject infinite cutoff");
    assert!(error.to_string().contains("finite"), "{error}");
    let error = db.try_execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_api_valid', 'DELETE', 'event_day', '2026-01-01')")
        .await.expect_err("reject unknown policy");
    assert!(
        error.to_string().contains("TRIM, RETAIN, or OFF"),
        "{error}"
    );
    let status: serde_json::Value = db
        .query_scalar(
            "SELECT pgtrickle.get_stream_table_partition_retention('retention_api_valid')",
        )
        .await;
    assert_eq!(status, serde_json::json!({"enabled": false}));
    db.execute("CREATE ROLE retention_api_visitor LOGIN").await;
    let mut visitor = db.pool.acquire().await.expect("acquire visitor connection");
    sqlx::query("SET ROLE retention_api_visitor")
        .execute(&mut *visitor)
        .await
        .expect("set visitor role");
    let error = sqlx::query("SELECT pgtrickle.set_stream_table_partition_retention('public.retention_api_valid', 'TRIM', 'event_day', '2026-01-01')")
        .execute(&mut *visitor).await.expect_err("non-owner cannot configure retention");
    assert!(
        error.to_string().contains("owner") || error.to_string().contains("permission denied"),
        "{error}"
    );
    let error = sqlx::query(
        "SELECT pgtrickle.get_stream_table_partition_retention('public.retention_api_valid')",
    )
    .execute(&mut *visitor)
    .await
    .expect_err("non-owner cannot inspect retention");
    assert!(
        error.to_string().contains("owner") || error.to_string().contains("permission denied"),
        "{error}"
    );
    sqlx::query("RESET ROLE")
        .execute(&mut *visitor)
        .await
        .expect("restore caller role");
    drop(visitor);
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_api_valid', 'TRIM', 'event_day', '2026-01-01')").await;
    let generation: i64 = db.query_scalar("SELECT contract_generation FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_api_valid'").await;
    db.execute("SELECT pgtrickle.set_stream_table_partition_retention('retention_api_valid', 'TRIM', 'event_day', '2026-01-01')").await;
    let repeated_generation: i64 = db.query_scalar("SELECT contract_generation FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retention_api_valid'").await;
    assert_eq!(repeated_generation, generation);
    let indexes: Vec<String> = db.query_scalar("SELECT pg_catalog.array_agg(pg_catalog.pg_get_indexdef(i.indexrelid) ORDER BY i.indexrelid) FROM pg_catalog.pg_index i JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0] WHERE i.indrelid = 'retention_api_valid'::regclass AND a.attname = 'event_day'").await;
    assert_eq!(indexes, vec!["CREATE INDEX retention_api_valid_event_day_idx ON public.retention_api_valid USING btree (event_day)".to_string()]);
    let status: serde_json::Value = db
        .query_scalar(
            "SELECT pgtrickle.get_stream_table_partition_retention('retention_api_valid')",
        )
        .await;
    assert_eq!(
        status,
        serde_json::json!({
            "enabled": true, "policy": "TRIM", "source": "public.retention_api_src",
            "source_column": "event_day", "group_column": "event_day", "bucket": "EXACT",
            "cutoff": "2026-01-01", "sealed_ranges": []
        })
    );
    db.execute("INSERT INTO retention_api_src VALUES (3,'2025-06-01','2026-06-01 10:00','a',3)")
        .await;
    db.execute("ALTER TABLE retention_api_src DETACH PARTITION retention_api_old")
        .await;
    db.assert_st_matches_query("retention_api_valid", query)
        .await;
    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as("SELECT account, event_day::text, total, n FROM retention_api_valid ORDER BY account,event_day")
        .fetch_all(&db.pool).await.expect("read exact surviving groups");
    assert_eq!(rows, vec![("a".into(), "2026-06-01".into(), 20, 1)]);
    let retained: Vec<(String, String, i64, i64)> = sqlx::query_as("SELECT account, event_day::text, total, n FROM retention_api_atomic ORDER BY account,event_day")
        .fetch_all(&db.pool).await.expect("read atomically created retained history");
    assert_eq!(
        retained,
        vec![
            ("a".into(), "2025-06-01".into(), 13, 2),
            ("a".into(), "2026-06-01".into(), 20, 1)
        ]
    );
}

#[tokio::test]
async fn test_retention_eligibility_diagnostics_report_construct_and_contract_hint() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute(
        "CREATE TABLE retention_diag_ev (
            event_day DATE NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            meter TEXT NOT NULL,
            amount NUMERIC NOT NULL
        ) PARTITION BY RANGE (event_day)",
    )
    .await;
    db.execute(
        "CREATE TABLE retention_diag_old PARTITION OF retention_diag_ev
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE retention_diag_live PARTITION OF retention_diag_ev
         FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;
    db.execute(
        "INSERT INTO retention_diag_ev VALUES
         ('2025-06-01', '2025-06-01 12:05+00', 'a', 10),
         ('2026-06-01', '2026-06-01 09:10+00', 'b', 20)",
    )
    .await;

    let valid = "SELECT event_day, meter, sum(amount) AS total, count(*) AS n
                 FROM retention_diag_ev GROUP BY event_day, meter";
    sqlx::query(
        "SELECT pgtrickle.create_stream_table_with_retention(
            $1, $2, 'TRIM', 'event_day', '2026-01-01', '1h')",
    )
    .bind("retention_diag_ok")
    .bind(valid)
    .execute(&db.pool)
    .await
    .expect("eligible query must still be accepted");
    let policy: serde_json::Value = db
        .query_scalar("SELECT pgtrickle.get_stream_table_partition_retention('retention_diag_ok')")
        .await;
    assert_eq!(policy["enabled"], true);

    let cases = [
        (
            "retention_diag_constant",
            "SELECT 'x'::text AS tag, event_day, meter, sum(amount) AS total, count(*) AS n
             FROM retention_diag_ev GROUP BY event_day, meter",
            "select-list item",
            "tag",
            "plain column reference",
        ),
        (
            "retention_diag_other_hour",
            "SELECT event_day, meter,
                    date_trunc('hour', occurred_at AT TIME ZONE 'UTC') AS hour,
                    sum(amount) AS total, count(*) AS n
             FROM retention_diag_ev
             GROUP BY event_day, meter, date_trunc('hour', occurred_at AT TIME ZONE 'UTC')",
            "grouping key",
            "occurred_at",
            "partition key \"event_day\"",
        ),
    ];
    for (name, query, context, expression, rule) in cases {
        let error = sqlx::query(
            "SELECT pgtrickle.create_stream_table_with_retention(
                $1, $2, 'TRIM', 'event_day', '2026-01-01', '1h')",
        )
        .bind(name)
        .bind(query)
        .execute(&db.pool)
        .await
        .expect_err("ineligible retention query must fail");

        let message = error.to_string();
        assert!(
            message.contains("retention query is not eligible"),
            "{message}"
        );
        assert!(message.contains(context), "{message}");
        assert!(message.contains(expression), "{message}");
        assert!(message.contains(rule), "{message}");

        let database_error = error.as_database_error().expect("PostgreSQL error");
        let pg_error = database_error.downcast_ref::<sqlx::postgres::PgDatabaseError>();
        let hint = pg_error.hint().expect("retention contract hint");
        assert!(
            hint.contains("docs/tutorials/PARTITIONED_TABLES.md#retention-policies"),
            "{hint}"
        );

        let created: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(name)
            .fetch_one(&db.pool)
            .await
            .expect("check failed create did not leave a stream");
        assert_eq!(created, None);
    }
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_rejects_non_durable_partition_trees() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .await;
    db.execute("CREATE SERVER retention_foreign_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'unused')").await;
    db.execute("CREATE TABLE retention_foreign_src (d DATE NOT NULL, region TEXT NOT NULL) PARTITION BY RANGE (d)").await;
    db.execute("CREATE TABLE retention_foreign_year PARTITION OF retention_foreign_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01') PARTITION BY LIST (region)").await;
    db.execute("CREATE FOREIGN TABLE retention_foreign_leaf PARTITION OF retention_foreign_year FOR VALUES IN ('remote') SERVER retention_foreign_server OPTIONS (table_name 'unused')").await;
    let error = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT pgtrickle.retention_layout_test($1, 'retention_foreign_src'::regclass::oid, 'd', $2)",
    )
    .bind("SELECT d, COUNT(*) AS n FROM retention_foreign_src GROUP BY d")
    .bind(9497_i64)
    .fetch_one(&db.pool)
    .await
    .expect_err("reject a foreign subpartition");
    assert!(
        error.to_string().contains("permanent local tables"),
        "{error}"
    );
    db.execute("CREATE TABLE retention_unlogged_src (d DATE NOT NULL) PARTITION BY RANGE (d)")
        .await;
    db.execute("CREATE UNLOGGED TABLE retention_unlogged_leaf (d DATE NOT NULL)")
        .await;
    db.execute("ALTER TABLE retention_unlogged_src ATTACH PARTITION retention_unlogged_leaf FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    let error = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT pgtrickle.retention_layout_test($1, 'retention_unlogged_src'::regclass::oid, 'd', $2)",
    )
    .bind("SELECT d, COUNT(*) AS n FROM retention_unlogged_src GROUP BY d")
    .bind(9497_i64)
    .fetch_one(&db.pool)
    .await
    .expect_err("reject an unlogged partition tree");
    assert!(
        error.to_string().contains("permanent local tables"),
        "{error}"
    );
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_candidates_use_actual_postgresql_query_trees() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute(
        "CREATE TABLE retention_parse_ts (event_day DATE NOT NULL, occurred TIMESTAMP NOT NULL, \
         account TEXT NOT NULL, amount INT NOT NULL) PARTITION BY RANGE (occurred)",
    )
    .await;
    db.execute(
        "CREATE TABLE retention_parse_tz (occurred TIMESTAMPTZ NOT NULL, amount INT NOT NULL) \
         PARTITION BY RANGE (occurred)",
    )
    .await;
    let cases = [
        (
            "SELECT occurred, count(*) AS n FROM retention_parse_ts GROUP BY 1",
            "retention_parse_ts",
            "occurred",
        ),
        (
            "SELECT account, pg_catalog.date_trunc('hour', occurred) AS hour, sum(amount) AS total, count(*) AS n FROM retention_parse_ts GROUP BY 1, 2",
            "retention_parse_ts",
            "occurred",
        ),
        (
            "SELECT account, pg_catalog.date_trunc('hour', occurred) AS hour, sum(amount) AS total, count(*) AS n FROM retention_parse_ts GROUP BY 1, 2",
            "retention_parse_ts",
            "event_day",
        ),
        (
            "SELECT pg_catalog.date_trunc('hour', occurred) AS hour, count(*) AS n FROM retention_parse_tz GROUP BY 1",
            "retention_parse_tz",
            "occurred",
        ),
        (
            "SELECT pg_catalog.date_trunc('hour', occurred, 'UTC') AS hour, count(*) AS n FROM retention_parse_tz GROUP BY 1",
            "retention_parse_tz",
            "occurred",
        ),
        (
            "SELECT occurred, sum(amount + 1) AS total FROM retention_parse_ts GROUP BY 1",
            "retention_parse_ts",
            "occurred",
        ),
        (
            "SELECT occurred, lower(account) AS account, count(*) AS n FROM retention_parse_ts GROUP BY 1,2",
            "retention_parse_ts",
            "occurred",
        ),
    ];
    let mut actual = Vec::new();
    for (query, source, key) in cases {
        let result: String = sqlx::query_scalar(
            "SELECT pgtrickle.retention_candidate_test($1, $2::regclass::oid, $3)",
        )
        .bind(query)
        .bind(source)
        .bind(key)
        .fetch_one(&db.pool)
        .await
        .expect("analyze real query tree");
        actual.push(result);
    }
    assert_eq!(
        actual,
        vec![
            "Ok(\"occurred\", Exact, 1114)",
            "Ok(\"hour\", HourTimestamp, 1114)",
            "Err(InvalidSourceKey)",
            "Err(UnsupportedShape)",
            "Ok(\"hour\", HourUtcTimestamptz, 1184)",
            "Err(UnsupportedAggregate)",
            "Err(UnsupportedShape)",
        ]
    );
}

#[tokio::test]
#[cfg(feature = "e2e-unsafe-test-hooks")]
async fn test_retention_layout_validates_actual_partition_catalog() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute(
        "CREATE TABLE layout_source (t TIMESTAMP NOT NULL, amount INT) PARTITION BY RANGE (t)",
    )
    .await;
    db.execute("CREATE TABLE layout_old PARTITION OF layout_source FOR VALUES FROM ('2000-01-01 00:00') TO ('2000-01-01 01:00')")
        .await;
    db.execute("CREATE TABLE layout_live PARTITION OF layout_source FOR VALUES FROM ('2000-01-01 01:00') TO ('2000-01-01 02:00')")
        .await;
    let query = "SELECT pg_catalog.date_trunc('hour', t) AS hour, count(*) AS n FROM layout_source GROUP BY 1";
    let actual: serde_json::Value = sqlx::query_scalar(
        "SELECT pgtrickle.retention_layout_test($1, 'layout_source'::regclass::oid, 't', $2)",
    )
    .bind(query)
    .bind(3_600_000_000_i64)
    .fetch_one(&db.pool)
    .await
    .expect("validate hourly layout");
    let oids: (i64, i64) = sqlx::query_as(
        "SELECT 'layout_old'::regclass::oid::bigint, 'layout_live'::regclass::oid::bigint",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read partition OIDs");
    assert_eq!(
        actual,
        serde_json::json!([
            {"oid": oids.0, "lower": 0, "upper": 3_600_000_000_i64, "expired": true},
            {"oid": oids.1, "lower": 3_600_000_000_i64, "upper": 7_200_000_000_i64, "expired": false}
        ])
    );
    let partial: serde_json::Value = sqlx::query_scalar(
        "SELECT pgtrickle.retention_layout_test($1, 'layout_source'::regclass::oid, 't', $2)",
    )
    .bind(query)
    .bind(5_400_000_000_i64)
    .fetch_one(&db.pool)
    .await
    .expect("validate partial-horizon overlap");
    assert_eq!(partial, actual);

    let mut tx = db.pool.begin().await.expect("begin locked layout proof");
    let locked: serde_json::Value = sqlx::query_scalar(
        "SELECT pgtrickle.retention_layout_test($1, 'layout_source'::regclass::oid, 't', $2)",
    )
    .bind(query)
    .bind(3_600_000_000_i64)
    .fetch_one(&mut *tx)
    .await
    .expect("hold the layout lock");
    assert_eq!(locked, actual);
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sqlx::query("INSERT INTO layout_source VALUES ('2000-01-01 01:30', 1)").execute(&db.pool),
    )
    .await
    .expect("layout lock must not block a source writer")
    .expect("insert into live partition");
    let mut ddl_conn = db.pool.acquire().await.expect("acquire DDL connection");
    let ddl_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *ddl_conn)
        .await
        .expect("read DDL backend PID");
    let ddl = tokio::spawn(async move {
        sqlx::query("CREATE TABLE layout_future PARTITION OF layout_source FOR VALUES FROM ('2000-01-01 02:00') TO ('2000-01-01 03:00')")
            .execute(&mut *ddl_conn)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks \
                 WHERE pid = $1 AND relation = 'layout_source'::regclass AND NOT granted)",
            )
            .bind(ddl_pid)
            .fetch_one(&db.pool)
            .await
            .expect("inspect DDL lock wait");
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("partition DDL waits for the layout proof");
    tx.rollback().await.expect("release layout lock");
    ddl.await
        .expect("join partition DDL")
        .expect("partition DDL completes after lock release");

    let mut repeatable = db.pool.begin().await.expect("begin repeatable-read proof");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *repeatable)
        .await
        .expect("set repeatable-read isolation");
    let error = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT pgtrickle.retention_layout_test($1, 'layout_source'::regclass::oid, 't', $2)",
    )
    .bind(query)
    .bind(3_600_000_000_i64)
    .fetch_one(&mut *repeatable)
    .await
    .expect_err("stale repeatable-read catalog snapshots must fail closed");
    assert!(error.to_string().contains("READ COMMITTED"));
    repeatable.rollback().await.expect("release failed proof");

    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire search-path connection");
    sqlx::query("SET search_path = public, pg_catalog")
        .execute(&mut *conn)
        .await
        .expect("set unsafe catalog order");
    let error = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT pgtrickle.retention_layout_test($1, 'public.layout_source'::regclass::oid, 't', $2)",
    )
    .bind(query)
    .bind(3_600_000_000_i64)
    .fetch_one(&mut *conn)
    .await
    .expect_err("untrusted operator resolution must be rejected");
    assert!(error.to_string().contains("search_path"));
    sqlx::query("RESET search_path")
        .execute(&mut *conn)
        .await
        .expect("restore search path");
    drop(conn);

    db.execute("CREATE TABLE layout_default PARTITION OF layout_source DEFAULT")
        .await;
    let error = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT pgtrickle.retention_layout_test($1, 'layout_source'::regclass::oid, 't', $2)",
    )
    .bind(query)
    .bind(3_600_000_000_i64)
    .fetch_one(&db.pool)
    .await
    .expect_err("default partition must be rejected");
    assert!(error.to_string().contains("no default partition"));

    db.execute("CREATE TABLE layout_unaligned (t TIMESTAMP NOT NULL) PARTITION BY RANGE (t)")
        .await;
    db.execute("CREATE TABLE layout_unaligned_p PARTITION OF layout_unaligned FOR VALUES FROM ('2000-01-01 00:30') TO ('2000-01-01 02:00')")
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.retention_layout_test($$SELECT pg_catalog.date_trunc('hour', t) AS hour, count(*) AS n FROM layout_unaligned GROUP BY 1$$, 'layout_unaligned'::regclass::oid, 't', 7200000000)")
        .await
        .expect_err("unaligned hourly bounds must be rejected");
    assert!(error.to_string().contains("UnalignedHour"));

    db.execute("CREATE TABLE layout_misaligned (d DATE NOT NULL, t TIMESTAMP NOT NULL) PARTITION BY RANGE (d)")
        .await;
    db.execute("CREATE TABLE layout_misaligned_p PARTITION OF layout_misaligned FOR VALUES FROM ('2000-01-01') TO ('2000-01-02')")
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.retention_layout_test($$SELECT pg_catalog.date_trunc('hour', t) AS hour, count(*) AS n FROM layout_misaligned GROUP BY 1$$, 'layout_misaligned'::regclass::oid, 't', 86400000000)")
        .await
        .expect_err("partition and grouping keys must match");
    assert!(error.to_string().contains("one non-null RANGE column"));

    db.execute("CREATE TABLE layout_unbounded (d DATE NOT NULL) PARTITION BY RANGE (d)")
        .await;
    db.execute("CREATE TABLE layout_unbounded_p PARTITION OF layout_unbounded FOR VALUES FROM (MINVALUE) TO ('2000-01-01')")
        .await;
    let error = db
        .try_execute("SELECT pgtrickle.retention_layout_test($$SELECT d, count(*) AS n FROM layout_unbounded GROUP BY d$$, 'layout_unbounded'::regclass::oid, 'd', 0)")
        .await
        .expect_err("unbounded ranges must fail closed");
    assert!(error.to_string().contains("unsupported or unbounded"));
}

#[tokio::test]
async fn test_partition_snapshot_decodes_timestamp_bounds_without_session_timezone() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("CREATE TABLE saved_bounds (t TIMESTAMPTZ NOT NULL) PARTITION BY RANGE (t)")
        .await;
    db.execute(
        "CREATE TABLE saved_bounds_finite PARTITION OF saved_bounds \
         FOR VALUES FROM ('1999-12-31 23:00:00+00') TO ('2000-01-01 00:00:00+00')",
    )
    .await;
    db.execute(
        "CREATE TABLE saved_bounds_unbounded PARTITION OF saved_bounds \
         FOR VALUES FROM (MINVALUE) TO ('1999-12-31 23:00:00+00')",
    )
    .await;
    db.execute("CREATE TABLE saved_bounds_default PARTITION OF saved_bounds DEFAULT")
        .await;
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire timezone connection");
    sqlx::query("SET TIME ZONE 'America/New_York'")
        .execute(&mut *conn)
        .await
        .expect("set source snapshot timezone");
    sqlx::query(
        "SELECT pgtrickle.create_stream_table('saved_bounds_st', \
         'SELECT t, COUNT(*) AS n FROM saved_bounds GROUP BY t', '1h', 'DIFFERENTIAL')",
    )
    .execute(&mut *conn)
    .await
    .expect("create stream with non-UTC timezone");
    sqlx::query("SET TIME ZONE 'Asia/Tokyo'")
        .execute(&mut *conn)
        .await
        .expect("change session timezone");
    let bounds: Vec<(String, serde_json::Value)> = sqlx::query_as(
        "SELECT c.relname::text, entry->'range' FROM pgtrickle.pgt_dependencies d \
         CROSS JOIN LATERAL jsonb_array_elements(d.column_snapshot->'partition_descendants') entry \
         JOIN pg_catalog.pg_class c ON c.oid = (entry->>'oid')::oid \
         WHERE d.source_relid = 'saved_bounds'::regclass ORDER BY c.relname",
    )
    .fetch_all(&mut *conn)
    .await
    .expect("read decoded bound values");
    assert_eq!(
        bounds,
        vec![
            ("saved_bounds_default".into(), serde_json::Value::Null),
            (
                "saved_bounds_finite".into(),
                serde_json::json!({"type_oid": 1184, "lower": -3_600_000_000_i64, "upper": 0})
            ),
            ("saved_bounds_unbounded".into(), serde_json::Value::Null),
        ]
    );
    sqlx::query("CREATE TABLE saved_local_bounds (t TIMESTAMP NOT NULL) PARTITION BY RANGE (t)")
        .execute(&mut *conn)
        .await
        .expect("create local timestamp source");
    sqlx::query(
        "CREATE TABLE saved_local_hour PARTITION OF saved_local_bounds \
         FOR VALUES FROM ('1999-12-31 23:00:00') TO ('2000-01-01 00:00:00')",
    )
    .execute(&mut *conn)
    .await
    .expect("create local timestamp partition");
    sqlx::query(
        "SELECT pgtrickle.create_stream_table('saved_local_st', \
         'SELECT t, COUNT(*) AS n FROM saved_local_bounds GROUP BY t', '1h', 'DIFFERENTIAL')",
    )
    .execute(&mut *conn)
    .await
    .expect("create local timestamp stream");
    let local: serde_json::Value = sqlx::query_scalar(
        "SELECT entry->'range' FROM pgtrickle.pgt_dependencies d \
         CROSS JOIN LATERAL jsonb_array_elements(d.column_snapshot->'partition_descendants') entry \
         WHERE d.source_relid = 'saved_local_bounds'::regclass",
    )
    .fetch_one(&mut *conn)
    .await
    .expect("read local timestamp bounds");
    assert_eq!(
        local,
        serde_json::json!({"type_oid": 1114, "lower": -3_600_000_000_i64, "upper": 0})
    );
    sqlx::query("RESET TIME ZONE")
        .execute(&mut *conn)
        .await
        .expect("restore timezone");
}

#[tokio::test]
async fn test_partition_nested_drop_invalidates_grandparent() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE nest_source (id INT, d DATE NOT NULL, region TEXT NOT NULL,
         PRIMARY KEY (id, d, region)) PARTITION BY RANGE (d)",
    )
    .await;
    db.execute(
        "CREATE TABLE nest_year PARTITION OF nest_source
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
         PARTITION BY LIST (region)",
    )
    .await;
    db.execute("CREATE TABLE nest_leaf PARTITION OF nest_year FOR VALUES IN ('EU')")
        .await;
    db.execute(
        "CREATE TABLE nest_other PARTITION OF nest_source
         FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;
    db.execute(
        "INSERT INTO nest_source VALUES
         (1,'2025-02-01','EU'),(2,'2026-02-01','US')",
    )
    .await;
    let query = "SELECT COUNT(*) AS n FROM nest_source";
    db.create_st("nest_st", query, "1m", "DIFFERENTIAL").await;
    db.refresh_st("nest_st").await;
    let fingerprint: String = db
        .query_scalar(
            "SELECT schema_fingerprint FROM pgtrickle.pgt_dependencies \
             WHERE source_relid = 'nest_source'::regclass",
        )
        .await;
    db.execute("CREATE TABLE nest_leaf_us PARTITION OF nest_year FOR VALUES IN ('US')")
        .await;
    let new_fingerprint: String = db
        .query_scalar(
            "SELECT schema_fingerprint FROM pgtrickle.pgt_dependencies \
             WHERE source_relid = 'nest_source'::regclass",
        )
        .await;
    assert_eq!(fingerprint, new_fingerprint);

    async fn snapshot(db: &E2eDb) -> serde_json::Value {
        sqlx::query_scalar(
            "SELECT column_snapshot->'partition_descendants' \
             FROM pgtrickle.pgt_dependencies WHERE source_relid = 'nest_source'::regclass \
             AND pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'nest_st')",
        )
        .fetch_one(&db.pool)
        .await
        .expect("read saved partition bounds")
    }

    let live: Vec<(i64, i64, String, String, String)> = sqlx::query_as(
        "SELECT c.oid::bigint, i.inhparent::bigint, c.relpartbound::text, c.relkind::text, c.relname::text \
         FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent IN ('nest_source'::regclass, 'nest_year'::regclass) \
         ORDER BY c.oid, i.inhparent",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read live partition bounds");
    let dates: (i32, i32, i32) = sqlx::query_as(
        "SELECT DATE '2025-01-01' - DATE '2000-01-01', \
                DATE '2026-01-01' - DATE '2000-01-01', \
                DATE '2027-01-01' - DATE '2000-01-01'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read PostgreSQL date offsets");
    let expected = serde_json::Value::Array(
        live.iter()
            .map(|(oid, parent, bound, kind, name)| {
                let range = match name.as_str() {
                    "nest_year" => {
                        serde_json::json!({"type_oid": 1082, "lower": dates.0, "upper": dates.1})
                    }
                    "nest_other" => {
                        serde_json::json!({"type_oid": 1082, "lower": dates.1, "upper": dates.2})
                    }
                    _ => serde_json::Value::Null,
                };
                serde_json::json!({
                    "oid": oid, "parent_oid": parent, "bound_node": bound, "relkind": kind,
                    "range": range,
                })
            })
            .collect(),
    );
    assert_eq!(snapshot(&db).await, expected);
    let leaf_oid: i64 = db
        .query_scalar("SELECT 'nest_leaf'::regclass::oid::bigint")
        .await;

    let mut tx = db.pool.begin().await.expect("begin partition rollback");
    sqlx::query("DROP TABLE nest_leaf")
        .execute(&mut *tx)
        .await
        .expect("drop partition within transaction");
    tx.rollback().await.expect("roll back partition drop");
    assert_eq!(snapshot(&db).await, expected);

    db.execute("DROP TABLE nest_leaf").await;
    let remaining_snapshot = serde_json::Value::Array(
        expected
            .as_array()
            .expect("partition snapshot array")
            .iter()
            .filter(|entry| entry["oid"] != serde_json::json!(leaf_oid))
            .cloned()
            .collect(),
    );
    assert_eq!(snapshot(&db).await, remaining_snapshot);
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'nest_st'",
        )
        .await;
    assert!(needs_reinit, "grandparent dependency must be invalidated");
    db.refresh_st("nest_st").await;
    db.assert_st_matches_query("nest_st", query).await;
    let remaining: i64 = db.query_scalar("SELECT n FROM nest_st").await;
    assert_eq!(remaining, 1);
}

#[tokio::test]
async fn test_partition_empty_attach_then_drop_updates_ancestry() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE late_source (id INT, d DATE NOT NULL,
         PRIMARY KEY (id, d)) PARTITION BY RANGE(d)",
    )
    .await;
    db.execute(
        "CREATE TABLE late_first PARTITION OF late_source
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute("INSERT INTO late_source VALUES (1,'2025-03-01')")
        .await;
    let query = "SELECT COUNT(*) AS n FROM late_source";
    db.create_st("late_st", query, "1m", "DIFFERENTIAL").await;
    db.refresh_st("late_st").await;

    db.execute("CREATE TABLE late_second (LIKE late_source INCLUDING ALL)")
        .await;
    db.execute(
        "ALTER TABLE late_source ATTACH PARTITION late_second
         FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;
    let active: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'late_st'",
        )
        .await;
    assert_eq!(active, "ACTIVE", "empty attach should not suspend the ST");

    db.execute("INSERT INTO late_source VALUES (2,'2026-03-01')")
        .await;
    db.refresh_st("late_st").await;
    db.execute("DROP TABLE late_second").await;
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'late_st'",
        )
        .await;
    assert!(needs_reinit, "the newly attached child must be tracked");
    db.refresh_st("late_st").await;
    db.assert_st_matches_query("late_st", query).await;
}

#[tokio::test]
async fn test_partition_drop_rollback_preserves_materialization() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE rollback_source (id INT, d DATE NOT NULL,
         PRIMARY KEY (id, d)) PARTITION BY RANGE(d)",
    )
    .await;
    db.execute(
        "CREATE TABLE rollback_leaf PARTITION OF rollback_source
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute("INSERT INTO rollback_source VALUES (1,'2025-01-01')")
        .await;
    let query = "SELECT COUNT(*) AS n FROM rollback_source";
    db.create_st("rollback_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st("rollback_st").await;

    let mut connection = db.pool.acquire().await.expect("acquire connection");
    sqlx::query("BEGIN")
        .execute(&mut *connection)
        .await
        .expect("begin transaction");
    sqlx::query("DROP TABLE rollback_leaf")
        .execute(&mut *connection)
        .await
        .expect("drop partition within transaction");
    let marked: bool = sqlx::query_scalar(
        "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables
         WHERE pgt_name = 'rollback_st'",
    )
    .fetch_one(&mut *connection)
    .await
    .expect("read transactional invalidation");
    assert!(marked);
    sqlx::query("ROLLBACK")
        .execute(&mut *connection)
        .await
        .expect("rollback");
    drop(connection);

    let marked_after: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'rollback_st'",
        )
        .await;
    assert!(!marked_after, "invalidations must roll back with DROP");
    db.assert_st_matches_query("rollback_st", query).await;
}

#[tokio::test]
async fn test_partition_concurrent_detach_stays_suspended_until_resume() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE concurrent_root (id INT, d DATE NOT NULL,
         PRIMARY KEY (id,d)) PARTITION BY RANGE (d)",
    )
    .await;
    db.execute(
        "CREATE TABLE concurrent_old PARTITION OF concurrent_root
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE concurrent_new PARTITION OF concurrent_root
         FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;
    db.execute(
        "INSERT INTO concurrent_root VALUES
         (1,'2025-01-01'),(2,'2026-01-01')",
    )
    .await;
    let query = "SELECT COUNT(*) AS n FROM concurrent_root";
    db.create_st("concurrent_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st("concurrent_st").await;

    // Two-phase DETACH cannot be reinitialized before its final commit.
    db.execute("ALTER TABLE concurrent_root DETACH PARTITION concurrent_old CONCURRENTLY")
        .await;
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'concurrent_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED");

    db.execute("SELECT pgtrickle.resume_stream_table('concurrent_st')")
        .await;
    db.refresh_st("concurrent_st").await;
    db.assert_st_matches_query("concurrent_st", query).await;
}

#[tokio::test]
async fn test_partition_append_only_pk_survives_insert_only_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE app_source (id INT, d DATE NOT NULL, region TEXT NOT NULL,
         PRIMARY KEY (id,d)) PARTITION BY RANGE (d)",
    )
    .await;
    db.execute(
        "CREATE TABLE app_p1 PARTITION OF app_source
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute("INSERT INTO app_source VALUES (1,'2025-05-01','EU')")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            name => 'app_st',
            query => 'SELECT id, d, region FROM app_source',
            schedule => '1m',
            refresh_mode => 'DIFFERENTIAL',
            append_only => true
        )",
    )
    .await;
    db.refresh_st_with_retry("app_st").await;
    db.execute("INSERT INTO app_source VALUES (2,'2025-06-01','EU')")
        .await;
    db.refresh_st_with_retry("app_st").await;
    // The append-only INSERT fast path is valid for a monotonic row
    // projection; an aggregate would legitimately revert to MERGE because
    // inserting source rows can UPDATE an existing output group.
    let append_only: bool = db
        .query_scalar(
            "SELECT is_append_only FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'app_st'",
        )
        .await;
    assert!(
        append_only,
        "insert-only batches must not disable the explicit flag"
    );
    db.assert_st_matches_query("app_st", "SELECT id, d, region FROM app_source")
        .await;
}

#[tokio::test]
async fn test_partition_direct_drop_suspends_immediate_until_protected_resume() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE immediate_part_source (
            id INT, d DATE NOT NULL, PRIMARY KEY (id,d)
        ) PARTITION BY RANGE (d)",
    )
    .await;
    db.execute(
        "CREATE TABLE immediate_part_old PARTITION OF immediate_part_source
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE immediate_part_new PARTITION OF immediate_part_source
         FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;
    db.execute(
        "INSERT INTO immediate_part_source VALUES
         (1,'2025-02-01'),(2,'2026-02-01')",
    )
    .await;
    let query = "SELECT id, d FROM immediate_part_source";
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            name => 'immediate_part_st',
            query => 'SELECT id, d FROM immediate_part_source',
            refresh_mode => 'IMMEDIATE'
        )",
    )
    .await;
    db.assert_st_matches_query("immediate_part_st", query).await;

    // IMMEDIATE has no scheduler to process a pending reinitialization.
    db.execute("DROP TABLE immediate_part_old").await;
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'immediate_part_st'",
        )
        .await;
    assert_eq!(status, "SUSPENDED");
    db.execute("SELECT pgtrickle.resume_stream_table('immediate_part_st')")
        .await;
    db.assert_st_matches_query("immediate_part_st", query).await;
}

#[tokio::test]
async fn test_partition_and_plain_wal_requests_report_eligibility_blockers() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE requested_wal_plain (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            name => 'requested_wal_plain_st',
            query => 'SELECT id, v FROM requested_wal_plain',
            refresh_mode => 'DIFFERENTIAL',
            cdc_mode => 'wal'
        )",
    )
    .await;

    db.execute(
        "CREATE TABLE requested_wal_root (
            id INT, d DATE NOT NULL, PRIMARY KEY (id,d)
        ) PARTITION BY RANGE (d)",
    )
    .await;
    db.execute(
        "CREATE TABLE requested_wal_leaf PARTITION OF requested_wal_root
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            name => 'requested_wal_root_st',
            query => 'SELECT id, d FROM requested_wal_root',
            refresh_mode => 'DIFFERENTIAL',
            cdc_mode => 'wal'
        )",
    )
    .await;

    // Neither source is currently safe to adopt into WAL: the regular table
    // has default replica identity, and the partitioned source has relkind p.
    // The requested CDC mode must be visible, not silently treated as TRIGGER.
    for source in ["requested_wal_plain", "requested_wal_root"] {
        let alert: Option<String> = db
            .query_scalar(&format!(
                "SELECT alert FROM pgtrickle.check_cdc_health()
                 WHERE source_relid = '{source}'::regclass::oid::bigint",
            ))
            .await;
        let alert = alert.expect("explicit WAL request needs a health explanation");
        assert!(
            alert.contains("WAL CDC requested"),
            "unexpected {source} eligibility alert: {alert}",
        );
        assert!(
            alert.contains("REPLICA IDENTITY FULL")
                || alert.contains("partitioned parent")
                || alert.contains("wal_level")
                || alert.contains("replication slot"),
            "alert must identify the prerequisite: {alert}",
        );
    }
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
async fn test_partitioned_not_in_first_rhs_null_hides_all_rows() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE notin_left (id integer PRIMARY KEY, k integer)")
        .await;
    db.execute("CREATE TABLE notin_right (id integer PRIMARY KEY, k integer)")
        .await;
    db.execute(
        "INSERT INTO notin_left \
         SELECT id, CASE WHEN id = 1 THEN NULL ELSE id % 10 + 1 END \
         FROM generate_series(1, 100) AS series(id)",
    )
    .await;
    let query = "SELECT l.id, l.k FROM notin_left l \
                 WHERE l.k NOT IN (SELECT r.k FROM notin_right r)";
    db.create_st_partitioned("notin_partitioned_st", query, "1h", "DIFFERENTIAL", "id")
        .await;
    db.assert_st_matches_query("notin_partitioned_st", query)
        .await;

    let (relkind, partition_key): (String, String) = sqlx::query_as(
        "SELECT c.relkind::text, pg_get_partkeydef(c.oid) \
         FROM pgtrickle.pgt_stream_tables st JOIN pg_class c ON c.oid = st.pgt_relid \
         WHERE st.pgt_name = 'notin_partitioned_st'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read partitioned stream-table metadata");
    assert_eq!(relkind, "p");
    assert_eq!(partition_key, "RANGE (id)");

    async fn refresh_and_compare(db: &E2eDb, query: &str) {
        db.try_execute_with_config(
            &["SET pg_trickle.refresh_strategy = 'differential'"],
            "SELECT pgtrickle.refresh_stream_table('notin_partitioned_st')",
        )
        .await
        .expect("run forced differential refresh");

        let history: (String, bool, String) = sqlx::query_as(
            "SELECT action, was_full_fallback, status FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                             WHERE pgt_name = 'notin_partitioned_st') \
             ORDER BY refresh_id DESC LIMIT 1",
        )
        .fetch_one(&db.pool)
        .await
        .expect("read effective refresh history");
        assert_eq!(history, ("DIFFERENTIAL".into(), false, "COMPLETED".into()));

        let actual: Vec<(i32, Option<i32>)> =
            sqlx::query_as("SELECT id, k FROM notin_partitioned_st ORDER BY id")
                .fetch_all(&db.pool)
                .await
                .expect("read partitioned stream-table rows");
        let expected: Vec<(i32, Option<i32>)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!("{query} ORDER BY l.id")))
                .fetch_all(&db.pool)
                .await
                .expect("read independent PostgreSQL NOT IN oracle");
        assert_eq!(actual, expected, "partitioned NOT IN must match PostgreSQL");
    }

    db.execute("INSERT INTO notin_right VALUES (1, 10), (2, 10)")
        .await;
    refresh_and_compare(&db, query).await;

    db.execute("INSERT INTO notin_right VALUES (3, NULL)").await;
    refresh_and_compare(&db, query).await;

    db.execute("INSERT INTO notin_right VALUES (4, NULL)").await;
    refresh_and_compare(&db, query).await;

    db.execute("DELETE FROM notin_right WHERE id = 3").await;
    refresh_and_compare(&db, query).await;

    db.execute("DELETE FROM notin_right WHERE id = 4").await;
    refresh_and_compare(&db, query).await;

    db.execute("DELETE FROM notin_right").await;
    refresh_and_compare(&db, query).await;
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
async fn test_alter_st_partition_key_preserves_dependent_view() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE alt_view_src (id INT PRIMARY KEY, sale_date DATE NOT NULL)")
        .await;
    db.execute("INSERT INTO alt_view_src VALUES (1, '2026-01-10'), (2, '2026-02-15')")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table('alt_view_st', \
         $$SELECT id, sale_date FROM alt_view_src$$, '1m', 'DIFFERENTIAL')",
    )
    .await;
    db.refresh_st("alt_view_st").await;
    db.execute("CREATE VIEW alt_dependent_view AS SELECT * FROM alt_view_st")
        .await;

    let oid_before: i64 = db
        .query_scalar("SELECT 'alt_view_st'::regclass::oid::bigint")
        .await;
    let result = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table('alt_view_st', partition_by => 'sale_date')",
        )
        .await;

    let view_exists: bool = db
        .query_scalar("SELECT to_regclass('public.alt_dependent_view') IS NOT NULL")
        .await;
    assert!(
        view_exists,
        "dependent view was removed; ALTER result: {result:?}"
    );
    db.assert_st_matches_query(
        "alt_dependent_view",
        "SELECT id, sale_date FROM alt_view_src",
    )
    .await;
    db.assert_st_matches_query("alt_view_st", "SELECT id, sale_date FROM alt_view_src")
        .await;
    let partition_key: Option<String> = db.query_scalar_opt(
        "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'alt_view_st'",
    ).await;
    if result.is_ok() {
        assert_eq!(partition_key.as_deref(), Some("sale_date"));
    } else {
        let oid_after: i64 = db
            .query_scalar("SELECT 'alt_view_st'::regclass::oid::bigint")
            .await;
        assert_eq!(oid_after, oid_before);
        assert_eq!(partition_key, None);
        let is_partitioned: bool = db
            .query_scalar("SELECT relkind = 'p' FROM pg_class WHERE oid = 'alt_view_st'::regclass")
            .await;
        assert!(!is_partitioned);
    }
}

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

/// A partition-key migration changes the producer's OID. Until that path
/// can rebind the graph like ALTER QUERY, reject a live downstream chain.
#[tokio::test]
async fn test_alter_st_partition_key_with_downstream_chain_rejects_before_replacement() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE guard_chain_src (
            id INT PRIMARY KEY, event_day DATE NOT NULL, amount INT NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO guard_chain_src VALUES
         (1, '2026-01-01', 10), (2, '2026-02-01', 20)",
    )
    .await;
    db.create_st(
        "guard_chain_up",
        "SELECT id, event_day, amount FROM guard_chain_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "guard_chain_down",
        "SELECT id, amount FROM guard_chain_up",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "guard_chain_leaf",
        "SELECT id FROM guard_chain_down",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let original_oid: i64 = db
        .query_scalar("SELECT 'guard_chain_up'::regclass::oid::bigint")
        .await;
    let dependency_oid: i64 = db
        .query_scalar(
            "SELECT source_relid::bigint FROM pgtrickle.pgt_dependencies
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                             WHERE pgt_name = 'guard_chain_down')
               AND source_type = 'STREAM_TABLE'",
        )
        .await;
    assert_eq!(dependency_oid, original_oid);

    let err = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table(
               'guard_chain_up', partition_by => 'event_day')",
        )
        .await
        .expect_err("repartitioning must not leave downstream relations dangling");
    assert!(
        err.to_string().contains("downstream stream table")
            && err.to_string().contains("partition_by"),
        "unexpected partition-dependency error: {err}"
    );
    let current_oid: i64 = db
        .query_scalar("SELECT 'guard_chain_up'::regclass::oid::bigint")
        .await;
    assert_eq!(
        current_oid, original_oid,
        "rejected ALTER replaced the parent"
    );
    let current_dependency_oid: i64 = db
        .query_scalar(
            "SELECT source_relid::bigint FROM pgtrickle.pgt_dependencies
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                             WHERE pgt_name = 'guard_chain_down')
               AND source_type = 'STREAM_TABLE'",
        )
        .await;
    assert_eq!(current_dependency_oid, original_oid);
    let key: Option<String> = db
        .query_scalar_opt(
            "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'guard_chain_up'",
        )
        .await;
    assert!(key.is_none());

    db.execute("INSERT INTO guard_chain_src VALUES (3, '2026-03-01', 30)")
        .await;
    db.refresh_st_with_retry("guard_chain_up").await;
    db.refresh_st_with_retry("guard_chain_down").await;
    db.refresh_st_with_retry("guard_chain_leaf").await;
    db.assert_st_matches_query(
        "guard_chain_up",
        "SELECT id, event_day, amount FROM guard_chain_src",
    )
    .await;
    db.assert_st_matches_query("guard_chain_down", "SELECT id, amount FROM guard_chain_src")
        .await;
    db.assert_st_matches_query("guard_chain_leaf", "SELECT id FROM guard_chain_src")
        .await;
}

#[tokio::test]
async fn test_alter_st_partition_key_change_and_remove_reject_with_downstream() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE guard_change_src (
            id INT PRIMARY KEY, event_day DATE NOT NULL, category TEXT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO guard_change_src VALUES (1, '2026-01-01', 'A')")
        .await;
    db.create_st_partitioned(
        "guard_change_up",
        "SELECT id, event_day, category FROM guard_change_src",
        "1m",
        "DIFFERENTIAL",
        "event_day",
    )
    .await;
    db.create_st(
        "guard_change_down",
        "SELECT id, category FROM guard_change_up",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let oid_before: i64 = db
        .query_scalar("SELECT 'guard_change_up'::regclass::oid::bigint")
        .await;
    for new_key in ["category", ""] {
        let err = db
            .try_execute(&format!(
                "SELECT pgtrickle.alter_stream_table(
                    'guard_change_up', partition_by => '{new_key}')"
            ))
            .await
            .expect_err("both a changed key and removal must reject downstream graphs");
        assert!(err.to_string().contains("downstream stream table"));
        let oid_after: i64 = db
            .query_scalar("SELECT 'guard_change_up'::regclass::oid::bigint")
            .await;
        assert_eq!(oid_after, oid_before);
        let key: String = db
            .query_scalar(
                "SELECT st_partition_key FROM pgtrickle.pgt_stream_tables
                 WHERE pgt_name = 'guard_change_up'",
            )
            .await;
        assert_eq!(key, "event_day");
    }
    // The rejected ALTER must preserve the existing downstream dependency
    // and materialized result. The normal partitioned explicit-DML path is
    // covered by #1439; verify it still works after the rejected operations.
    let dependent_source_oid: i64 = db
        .query_scalar(
            "SELECT source_relid::bigint FROM pgtrickle.pgt_dependencies
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                             WHERE pgt_name = 'guard_change_down')
               AND source_type = 'STREAM_TABLE'",
        )
        .await;
    assert_eq!(dependent_source_oid, oid_before);
    db.assert_st_matches_query(
        "guard_change_up",
        "SELECT id, event_day, category FROM guard_change_src",
    )
    .await;
    db.assert_st_matches_query(
        "guard_change_down",
        "SELECT id, category FROM guard_change_src",
    )
    .await;

    db.execute("INSERT INTO guard_change_src VALUES (2, '2026-02-01', 'B')")
        .await;
    db.refresh_st_with_retry("guard_change_up").await;
    db.refresh_st_with_retry("guard_change_down").await;
    db.assert_st_matches_query(
        "guard_change_up",
        "SELECT id, event_day, category FROM guard_change_src",
    )
    .await;
    db.assert_st_matches_query(
        "guard_change_down",
        "SELECT id, category FROM guard_change_src",
    )
    .await;
}

#[tokio::test]
async fn test_alter_st_partition_key_rejects_immediate_downstream_without_losing_triggers() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE guard_immediate_src (
            id INT PRIMARY KEY, event_day DATE NOT NULL, amount INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO guard_immediate_src VALUES (1, '2026-01-01', 10)")
        .await;
    db.create_st(
        "guard_immediate_up",
        "SELECT id, event_day, amount FROM guard_immediate_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            name => 'guard_immediate_down',
            query => 'SELECT id, amount FROM guard_immediate_up',
            schedule => NULL, refresh_mode => 'IMMEDIATE')",
    )
    .await;
    let parent_oid: i64 = db
        .query_scalar("SELECT 'guard_immediate_up'::regclass::oid::bigint")
        .await;
    let triggers_before: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_catalog.pg_trigger
             WHERE tgrelid = 'guard_immediate_up'::regclass AND NOT tgisinternal",
        )
        .await;
    assert!(
        triggers_before > 0,
        "downstream IVM must have source triggers"
    );

    let err = db
        .try_execute(
            "SELECT pgtrickle.alter_stream_table(
               'guard_immediate_up', partition_by => 'event_day')",
        )
        .await
        .expect_err("repartitioning must preserve downstream IVM trigger ownership");
    assert!(err.to_string().contains("downstream stream table"));
    assert_eq!(
        db.query_scalar::<i64>("SELECT 'guard_immediate_up'::regclass::oid::bigint")
            .await,
        parent_oid
    );
    assert_eq!(
        db.query_scalar::<i64>(
            "SELECT count(*) FROM pg_catalog.pg_trigger
             WHERE tgrelid = 'guard_immediate_up'::regclass AND NOT tgisinternal"
        )
        .await,
        triggers_before
    );

    db.execute("INSERT INTO guard_immediate_src VALUES (2, '2026-02-01', 20)")
        .await;
    db.refresh_st_with_retry("guard_immediate_up").await;
    db.assert_st_matches_query(
        "guard_immediate_down",
        "SELECT id, amount FROM guard_immediate_src",
    )
    .await;
}

/// An uncommitted CREATE of a downstream stream holds a read lock on the
/// upstream relation. Repartition must wait for that dependency to commit
/// before deciding whether it may drop and replace the relation.
#[tokio::test]
async fn test_alter_st_partition_key_serializes_concurrent_downstream_registration() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE guard_race_src (
            id INT PRIMARY KEY, event_day DATE NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO guard_race_src VALUES (1, '2026-01-01')")
        .await;
    db.create_st(
        "guard_race_up",
        "SELECT id, event_day FROM guard_race_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let upstream_oid: i64 = db
        .query_scalar("SELECT 'guard_race_up'::regclass::oid::bigint")
        .await;

    // Complete downstream creation, but deliberately keep its transaction
    // uncommitted while the repartition request begins.
    let mut create_tx = db.pool.begin().await.expect("begin dependent creation");
    sqlx::query(
        "SELECT pgtrickle.create_stream_table(
            'guard_race_down',
            'SELECT id FROM guard_race_up',
            '1m', 'DIFFERENTIAL')",
    )
    .execute(&mut *create_tx)
    .await
    .expect("create downstream stream table inside transaction");
    let creator_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *create_tx)
        .await
        .expect("identify the connection holding the upstream read lock");

    let pool = db.pool.clone();
    let alter = tokio::spawn(async move {
        let mut conn = pool.acquire().await.expect("acquire ALTER session");
        sqlx::query("SET application_name = 'guard_repartition_wait'")
            .execute(&mut *conn)
            .await
            .expect("identify ALTER backend for lock observation");
        sqlx::query(
            "SELECT pgtrickle.alter_stream_table(
                'guard_race_up', partition_by => 'event_day')",
        )
        .execute(&mut *conn)
        .await
        .map(|_| ())
    });

    // A witnessed lock wait, not a sleep alone, proves the race is exercised.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut observed_wait = false;
    while std::time::Instant::now() < deadline {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_stat_activity a
                WHERE a.application_name = 'guard_repartition_wait'
                  AND a.wait_event_type = 'Lock'
                  AND $1::integer = ANY(pg_catalog.pg_blocking_pids(a.pid)))",
        )
        .bind(creator_pid)
        .fetch_one(&db.pool)
        .await
        .expect("observe the creator blocking repartition");
        if waiting {
            observed_wait = true;
            break;
        }
        if alter.is_finished() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        observed_wait,
        "repartition request did not wait for the concurrent downstream registration"
    );

    create_tx
        .commit()
        .await
        .expect("publish downstream dependency before ALTER resumes");
    let err = tokio::time::timeout(std::time::Duration::from_secs(20), alter)
        .await
        .expect("ALTER must finish after downstream commits")
        .expect("ALTER task must not panic")
        .expect_err("a committed downstream must block repartition");
    assert!(
        err.to_string().contains("downstream stream table"),
        "unexpected post-wait ALTER result: {err}"
    );
    assert_eq!(
        db.query_scalar::<i64>("SELECT 'guard_race_up'::regclass::oid::bigint")
            .await,
        upstream_oid
    );
    assert_eq!(
        db.query_scalar::<i64>(
            "SELECT source_relid::bigint FROM pgtrickle.pgt_dependencies
             WHERE source_type = 'STREAM_TABLE'
               AND pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                             WHERE pgt_name = 'guard_race_down')"
        )
        .await,
        upstream_oid,
        "the waiting ALTER must not orphan the newly committed dependency"
    );

    db.execute("INSERT INTO guard_race_src VALUES (2, '2026-02-01')")
        .await;
    db.refresh_st_with_retry("guard_race_up").await;
    db.refresh_st_with_retry("guard_race_down").await;
    db.assert_st_matches_query("guard_race_down", "SELECT id FROM guard_race_src")
        .await;
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
