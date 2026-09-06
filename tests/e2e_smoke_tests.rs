//! E2E smoke tests — verify the test harness and extension load correctly.
//!
//! These are the most basic E2E tests: start a container with the compiled
//! extension, run CREATE EXTENSION, and verify core functionality works.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Container & Extension Loading ──────────────────────────────────────────

#[tokio::test]
async fn test_container_starts_with_extension_image() {
    let db = E2eDb::new().await;

    // Verify PostgreSQL 18.3 is running
    let version: String = db.query_scalar("SELECT version()").await;
    assert!(
        version.contains("PostgreSQL 18"),
        "Expected PostgreSQL 18, got: {}",
        version
    );
}

#[tokio::test]
async fn test_create_extension_succeeds() {
    let db = E2eDb::new().await.with_extension().await;

    // Verify the extension is installed
    let exists: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_trickle')")
        .await;
    assert!(exists, "Extension should be installed");
}

#[tokio::test]
async fn test_pg_trickle_schema_created() {
    let db = E2eDb::new().await.with_extension().await;

    let pg_trickle_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
        )
        .await;
    assert!(pg_trickle_exists, "pg_trickle schema should exist");

    let changes_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle_changes')",
        )
        .await;
    assert!(changes_exists, "pgtrickle_changes schema should exist");
}

#[tokio::test]
async fn test_catalog_tables_created() {
    let db = E2eDb::new().await.with_extension().await;

    let tables = [
        ("pgtrickle", "pgt_stream_tables"),
        ("pgtrickle", "pgt_dependencies"),
        ("pgtrickle", "pgt_refresh_history"),
        ("pgtrickle", "pgt_change_tracking"),
    ];

    for (schema, table) in tables {
        let exists = db.table_exists(schema, table).await;
        assert!(exists, "Table {}.{} should exist", schema, table);
    }
}

#[tokio::test]
async fn test_pgt_status_returns_empty_initially() {
    let db = E2eDb::new().await.with_extension().await;

    // pgt_status() should return 0 rows when no STs exist
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_status()")
        .await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_create_and_query_stream_table() {
    let db = E2eDb::new().await.with_extension().await;

    // Set up source data
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50), (4, 2, 150), (5, 3, 300)",
    )
    .await;

    // Create a stream table with aggregation
    db.create_st(
        "order_totals",
        "SELECT customer_id, sum(amount) as total_amount FROM orders GROUP BY customer_id",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Verify catalog entry
    let (status, mode, populated, errors) = db.pgt_status("order_totals").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "DIFFERENTIAL");
    assert!(populated);
    assert_eq!(errors, 0);

    // Verify materialized data
    let row_count = db.count("public.order_totals").await;
    assert_eq!(row_count, 3, "Should have 3 customer groups");

    // Verify aggregate values — use explicit cast to avoid type ambiguity
    let total_1: i64 = db
        .query_scalar("SELECT total_amount::bigint FROM public.order_totals WHERE customer_id = 1")
        .await;
    assert_eq!(total_1, 300); // 100 + 200

    let total_2: i64 = db
        .query_scalar("SELECT total_amount::bigint FROM public.order_totals WHERE customer_id = 2")
        .await;
    assert_eq!(total_2, 200); // 50 + 150

    let total_3: i64 = db
        .query_scalar("SELECT total_amount::bigint FROM public.order_totals WHERE customer_id = 3")
        .await;
    assert_eq!(total_3, 300);
}

#[tokio::test]
async fn test_refresh_picks_up_new_data() {
    let db = E2eDb::new().await.with_extension().await;

    // Create source and ST
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, qty INT)")
        .await;
    db.execute("INSERT INTO items VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "item_mirror",
        "SELECT id, qty FROM items",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let initial_count = db.count("public.item_mirror").await;
    assert_eq!(initial_count, 2);

    // Insert new data into source
    db.execute("INSERT INTO items VALUES (3, 30), (4, 40)")
        .await;

    // Refresh the ST
    db.refresh_st("item_mirror").await;

    let refreshed_count = db.count("public.item_mirror").await;
    assert_eq!(refreshed_count, 4);
}

#[tokio::test]
async fn test_drop_cleans_up() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src VALUES (1, 'hello')").await;

    db.create_st("temp_st", "SELECT id, val FROM src", "1m", "FULL")
        .await;

    // Verify it exists
    let exists = db.table_exists("public", "temp_st").await;
    assert!(exists, "ST storage table should exist before drop");

    // Drop it
    db.drop_st("temp_st").await;

    // Verify cleanup
    let exists = db.table_exists("public", "temp_st").await;
    assert!(!exists, "ST storage table should be gone after drop");

    let catalog_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'temp_st'")
        .await;
    assert_eq!(catalog_count, 0, "Catalog entry should be removed");
}

#[tokio::test]
async fn test_monitoring_views_exist() {
    let db = E2eDb::new().await.with_extension().await;

    // Both monitoring views should be queryable
    let info_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.stream_tables_info")
        .await;
    assert_eq!(info_count, 0);

    let stat_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pg_stat_stream_tables")
        .await;
    assert_eq!(stat_count, 0);
}

#[tokio::test]
async fn test_event_triggers_installed() {
    let db = E2eDb::new().await.with_extension().await;

    let ddl_trigger: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtname = 'pg_trickle_ddl_tracker')",
        )
        .await;
    assert!(ddl_trigger, "DDL event trigger should exist");

    let drop_trigger: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtname = 'pg_trickle_drop_tracker')",
        )
        .await;
    assert!(drop_trigger, "Drop event trigger should exist");
}

#[tokio::test]
async fn test_v093_capabilities_are_independent() {
    let db = E2eDb::new().await.with_extension().await;

    let capability_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.integration_capabilities()")
        .await;
    assert_eq!(capability_count, 2);

    let graph_enabled: bool = db
        .query_scalar(
            "SELECT enabled FROM pgtrickle.integration_capabilities() \
             WHERE capability = 'external_graph_refresh'",
        )
        .await;
    let delta_status: String = db
        .query_scalar(
            "SELECT details->>'status' FROM pgtrickle.integration_capabilities() \
             WHERE capability = 'output_delta_consumer'",
        )
        .await;
    assert!(!graph_enabled);
    assert_eq!(delta_status, "absent");
}

#[tokio::test]
async fn test_v093_external_ownership_blocks_managed_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE v093_source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'v093_external',\
             query => 'SELECT id, val FROM v093_source',\
             schedule => '1m',\
             refresh_mode => 'FULL',\
             initialize => false,\
             orchestration_mode => 'EXTERNAL'\
         )",
    )
    .await;

    let mode: String = db
        .query_scalar(
            "SELECT orchestration_mode FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'v093_external'",
        )
        .await;
    assert_eq!(mode, "EXTERNAL");

    let refresh = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('v093_external')")
        .await;
    assert!(
        refresh.is_err(),
        "managed refresh must reject EXTERNAL tables"
    );

    db.execute(
        "SELECT pgtrickle.set_orchestration_mode(\
             'public.v093_external'::regclass, 'MANAGED'\
         )",
    )
    .await;
}

#[tokio::test]
async fn test_v093_contracts_include_digest_and_canonical_graph() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE v093_graph_source (id INT PRIMARY KEY, val TEXT)")
        .await;
    for name in ["v093_graph_a", "v093_graph_b"] {
        let query = if name.ends_with('a') {
            "SELECT id, val FROM v093_graph_source"
        } else {
            "SELECT id, val FROM public.v093_graph_a"
        };
        db.execute(&format!(
            "SELECT pgtrickle.create_stream_table(\
                 name => '{name}', query => '{query}', schedule => '1m',\
                 refresh_mode => 'FULL', initialize => false,\
                 orchestration_mode => 'EXTERNAL'\
             )"
        ))
        .await;
    }

    let digest_length: i64 = db
        .query_scalar(
            "SELECT octet_length(contract_digest)::bigint \
             FROM pgtrickle.stream_table_contract('public.v093_graph_a'::regclass)",
        )
        .await;
    let member_count: i64 = db
        .query_scalar(
            "SELECT jsonb_array_length(contract->'members')::bigint \
             FROM pgtrickle.graph_contract(ARRAY['public.v093_graph_b'::regclass])",
        )
        .await;
    assert_eq!(digest_length, 32);
    assert_eq!(member_count, 2);

    let duplicate_roots = db
        .try_execute(
            "SELECT * FROM pgtrickle.graph_contract(ARRAY[\
                 'public.v093_graph_b'::regclass, 'public.v093_graph_b'::regclass\
             ])",
        )
        .await;
    assert!(duplicate_roots.is_err(), "duplicate roots must be rejected");
}
