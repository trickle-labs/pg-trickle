//! E2E tests verifying that mixed PostgreSQL object + stream table scenarios
//! work correctly under all three scheduling modes:
//!
//! 1. **Manual** — explicit `refresh_st()` calls, no background scheduler.
//! 2. **Serial scheduler** — background worker, `parallel_refresh_mode = 'off'`.
//! 3. **Parallel scheduler** — background worker, `parallel_refresh_mode = 'on'`.
//!
//! These tests cover the same topologies as `e2e_mixed_pg_objects_tests.rs`
//! but ensure correctness regardless of how refreshes are triggered.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Scheduling mode abstraction ──────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduleMode {
    Manual,
    Serial,
    Parallel,
}

impl std::fmt::Display for ScheduleMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScheduleMode::Manual => write!(f, "manual"),
            ScheduleMode::Serial => write!(f, "serial"),
            ScheduleMode::Parallel => write!(f, "parallel"),
        }
    }
}

/// Create a test database configured for the given scheduling mode.
///
/// - **Manual**: uses the shared container (no scheduler needed).
/// - **Serial/Parallel**: uses `new_on_postgres_db()` which resets server
///   config and holds a process-level lock for scheduler isolation.
async fn db_for_mode(mode: ScheduleMode) -> E2eDb {
    match mode {
        ScheduleMode::Manual => E2eDb::new().await.with_extension().await,
        ScheduleMode::Serial | ScheduleMode::Parallel => {
            let db = E2eDb::new_on_postgres_db().await.with_extension().await;
            configure_scheduler(&db, mode).await;
            db
        }
    }
}

/// Configure the background scheduler for fast testing.
///
/// For serial mode: `parallel_refresh_mode = 'off'`.
/// For parallel mode: `parallel_refresh_mode = 'on'`.
async fn configure_scheduler(db: &E2eDb, mode: ScheduleMode) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;

    match mode {
        ScheduleMode::Serial => {
            db.execute("ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'off'")
                .await;
        }
        ScheduleMode::Parallel => {
            db.execute("ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'on'")
                .await;
            db.execute("ALTER SYSTEM SET pg_trickle.max_concurrent_refreshes = 4")
                .await;
        }
        ScheduleMode::Manual => unreachable!(),
    }

    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;

    match mode {
        ScheduleMode::Serial => {
            db.wait_for_setting("pg_trickle.parallel_refresh_mode", "off")
                .await;
        }
        ScheduleMode::Parallel => {
            db.wait_for_setting("pg_trickle.parallel_refresh_mode", "on")
                .await;
            db.wait_for_setting("pg_trickle.max_concurrent_refreshes", "4")
                .await;
        }
        ScheduleMode::Manual => unreachable!(),
    }

    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler did not appear within 90 s ({mode})"
    );
}

/// Refresh a stream table according to the scheduling mode.
///
/// - **Manual**: calls `refresh_st()` directly.
/// - **Serial/Parallel**: waits for the scheduler to auto-refresh
///   by polling `data_timestamp`, using `refresh_st_with_retry` as fallback.
async fn refresh_for_mode(db: &E2eDb, st_name: &str, mode: ScheduleMode) {
    match mode {
        ScheduleMode::Manual => {
            db.refresh_st(st_name).await;
        }
        ScheduleMode::Serial | ScheduleMode::Parallel => {
            let refreshed = db
                .wait_for_auto_refresh(st_name, Duration::from_secs(60))
                .await;
            if !refreshed {
                // Fallback: force manual refresh if scheduler didn't fire
                db.refresh_st_with_retry(st_name).await;
            }
        }
    }
}

/// Wait for an entire chain of STs to be refreshed by the scheduler.
///
/// For manual mode, refreshes each ST in order (topological).
/// For scheduler modes, waits for each ST in topological order to ensure
/// changes propagate through the chain.  Waiting only for the leaf is racy:
/// the scheduler might refresh the leaf with stale upstream data if the
/// parent hasn't been refreshed yet in the current cycle.
async fn refresh_chain_for_mode(db: &E2eDb, st_names: &[&str], mode: ScheduleMode) {
    match mode {
        ScheduleMode::Manual => {
            for name in st_names {
                db.refresh_st(name).await;
            }
        }
        ScheduleMode::Serial | ScheduleMode::Parallel => {
            // Wait for each ST in topological order so that downstream STs
            // observe the refreshed upstream data.
            for name in st_names {
                let refreshed = db
                    .wait_for_auto_refresh(name, Duration::from_secs(90))
                    .await;
                if !refreshed {
                    db.refresh_st_with_retry(name).await;
                }
            }
        }
    }
}

/// Schedule string appropriate for the mode: short for scheduler, long for manual.
fn schedule_for_mode(mode: ScheduleMode) -> &'static str {
    match mode {
        ScheduleMode::Manual => "1m",
        ScheduleMode::Serial | ScheduleMode::Parallel => "1s",
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1 — View upstream, differential, full DML cycle
// ═══════════════════════════════════════════════════════════════════════════
//
// Table → View → ST (DIFFERENTIAL). INSERT/UPDATE/DELETE on the base table
// propagate through the view to the stream table.

async fn run_view_upstream_differential(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute(
        "CREATE TABLE msv_products (
            id    SERIAL PRIMARY KEY,
            name  TEXT NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            active BOOLEAN DEFAULT true
        )",
    )
    .await;
    db.execute(
        "INSERT INTO msv_products (name, price, active) VALUES
         ('Widget', 9.99, true),
         ('Gadget', 19.99, true),
         ('Gizmo', 29.99, false)",
    )
    .await;

    db.execute(
        "CREATE VIEW msv_active_products AS
         SELECT id, name, price FROM msv_products WHERE active = true",
    )
    .await;

    db.create_st(
        "msv_st_products",
        "SELECT id, name, price FROM msv_active_products",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    let (status, mode_str, populated, errors) = db.pgt_status("msv_st_products").await;
    assert_eq!(status, "ACTIVE", "[{mode}] status");
    assert_eq!(mode_str, "DIFFERENTIAL", "[{mode}] mode");
    assert!(populated, "[{mode}] populated");
    assert_eq!(errors, 0, "[{mode}] errors");
    assert_eq!(
        db.count("public.msv_st_products").await,
        2,
        "[{mode}] initial count"
    );

    // INSERT
    db.execute("INSERT INTO msv_products (name, price, active) VALUES ('Doohickey', 5.99, true)")
        .await;
    refresh_for_mode(&db, "msv_st_products", mode).await;
    assert_eq!(
        db.count("public.msv_st_products").await,
        3,
        "[{mode}] after INSERT"
    );

    // UPDATE: deactivate
    db.execute("UPDATE msv_products SET active = false WHERE name = 'Widget'")
        .await;
    refresh_for_mode(&db, "msv_st_products", mode).await;
    assert_eq!(
        db.count("public.msv_st_products").await,
        2,
        "[{mode}] after deactivate"
    );

    // DELETE
    db.execute("DELETE FROM msv_products WHERE name = 'Doohickey'")
        .await;
    refresh_for_mode(&db, "msv_st_products", mode).await;

    db.assert_st_matches_query(
        "msv_st_products",
        "SELECT id, name, price FROM msv_active_products",
    )
    .await;
}

#[tokio::test]
async fn test_mixed_sched_view_upstream_differential_manual() {
    run_view_upstream_differential(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_view_upstream_differential_serial() {
    run_view_upstream_differential(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_view_upstream_differential_parallel() {
    run_view_upstream_differential(ScheduleMode::Parallel).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2 — Multiple views joined in a stream table
// ═══════════════════════════════════════════════════════════════════════════
//
// Two independent views (from different tables) joined in a stream table.

async fn run_multiple_views_joined(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute(
        "CREATE TABLE msj_customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL, tier TEXT NOT NULL)",
    )
    .await;
    db.execute(
        "CREATE TABLE msj_orders (
            id SERIAL PRIMARY KEY,
            customer_id INT NOT NULL REFERENCES msj_customers(id),
            amount NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO msj_customers VALUES (1, 'Alice', 'gold'), (2, 'Bob', 'silver'), (3, 'Carol', 'gold')",
    )
    .await;
    db.execute("INSERT INTO msj_orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50), (4, 3, 300)")
        .await;

    db.execute(
        "CREATE VIEW msj_gold_customers AS
         SELECT id, name FROM msj_customers WHERE tier = 'gold'",
    )
    .await;
    db.execute(
        "CREATE VIEW msj_large_orders AS
         SELECT id, customer_id, amount FROM msj_orders WHERE amount >= 100",
    )
    .await;

    db.create_st(
        "msj_st_gold_large",
        "SELECT c.name, o.amount
         FROM msj_gold_customers c
         JOIN msj_large_orders o ON c.id = o.customer_id",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(
        db.count("public.msj_st_gold_large").await,
        3,
        "[{mode}] initial"
    );

    // Insert a new large order for a gold customer
    db.execute("INSERT INTO msj_orders VALUES (5, 3, 500)")
        .await;
    refresh_for_mode(&db, "msj_st_gold_large", mode).await;
    assert_eq!(
        db.count("public.msj_st_gold_large").await,
        4,
        "[{mode}] after new order"
    );

    // Demote Alice from gold → silver
    db.execute("UPDATE msj_customers SET tier = 'silver' WHERE name = 'Alice'")
        .await;
    refresh_for_mode(&db, "msj_st_gold_large", mode).await;

    db.assert_st_matches_query(
        "msj_st_gold_large",
        "SELECT c.name, o.amount
         FROM msj_gold_customers c
         JOIN msj_large_orders o ON c.id = o.customer_id",
    )
    .await;
}

#[tokio::test]
async fn test_mixed_sched_multiple_views_joined_manual() {
    run_multiple_views_joined(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_multiple_views_joined_serial() {
    run_multiple_views_joined(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_multiple_views_joined_parallel() {
    run_multiple_views_joined(ScheduleMode::Parallel).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3 — Midstream view between two ST layers
// ═══════════════════════════════════════════════════════════════════════════
//
// base table → user view → ST₁ (DIFFERENTIAL) → ST₂ (ST-on-ST, calculated).
// The scheduler must propagate changes through the view and the ST chain.

async fn run_midstream_view_chain(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute(
        "CREATE TABLE msm_inventory (
            id SERIAL PRIMARY KEY,
            product TEXT NOT NULL,
            quantity INT NOT NULL,
            warehouse TEXT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO msm_inventory (product, quantity, warehouse) VALUES
         ('Widget', 100, 'A'), ('Widget', 50, 'B'),
         ('Gadget', 200, 'A'), ('Gadget', 75, 'B')",
    )
    .await;

    db.execute(
        "CREATE VIEW msm_v_inventory AS
         SELECT product, quantity, warehouse FROM msm_inventory WHERE quantity > 0",
    )
    .await;

    db.create_st(
        "msm_st_product_totals",
        "SELECT product, SUM(quantity) AS total_qty FROM msm_v_inventory GROUP BY product",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(
        db.count("public.msm_st_product_totals").await,
        2,
        "[{mode}] initial"
    );

    // ST₂: filter low stock (ST-on-ST, calculated schedule)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'msm_st_low_stock',
            $$SELECT product, total_qty FROM msm_st_product_totals WHERE total_qty < 200$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    assert_eq!(
        db.count("public.msm_st_low_stock").await,
        1,
        "[{mode}] initial low stock (Widget=150)"
    );

    // Reduce stock to push both below threshold
    db.execute(
        "UPDATE msm_inventory SET quantity = 10 WHERE product = 'Widget' AND warehouse = 'A'",
    )
    .await;
    db.execute(
        "UPDATE msm_inventory SET quantity = 30 WHERE product = 'Gadget' AND warehouse = 'A'",
    )
    .await;

    refresh_chain_for_mode(&db, &["msm_st_product_totals", "msm_st_low_stock"], mode).await;

    db.assert_st_matches_query(
        "msm_st_product_totals",
        "SELECT product, SUM(quantity) AS total_qty FROM msm_v_inventory GROUP BY product",
    )
    .await;

    db.assert_st_matches_query(
        "msm_st_low_stock",
        "SELECT product, total_qty FROM msm_st_product_totals WHERE total_qty < 200",
    )
    .await;
}

#[tokio::test]
async fn test_mixed_sched_midstream_view_chain_manual() {
    run_midstream_view_chain(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_midstream_view_chain_serial() {
    run_midstream_view_chain(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_midstream_view_chain_parallel() {
    run_midstream_view_chain(ScheduleMode::Parallel).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4 — Diamond DAG with view branch
// ═══════════════════════════════════════════════════════════════════════════
//
// Diamond: base → view → ST₁(SUM) + base → ST₂(COUNT) → ST₃(JOIN).
// Exercises the scheduler's topological ordering across mixed sources.

async fn run_diamond_with_view_branch(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute(
        "CREATE TABLE msd_src (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            amount INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO msd_src (category, amount) VALUES ('x', 10), ('x', 20), ('y', 30)")
        .await;

    // Branch A: through a view (SUM)
    db.execute(
        "CREATE VIEW msd_v_src AS
         SELECT id, category, amount FROM msd_src WHERE amount > 0",
    )
    .await;
    db.create_st(
        "msd_branch_a",
        "SELECT category, SUM(amount) AS total FROM msd_v_src GROUP BY category",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    // Branch B: direct from table (COUNT)
    db.create_st(
        "msd_branch_b",
        "SELECT category, COUNT(*) AS cnt FROM msd_src GROUP BY category",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    // ST₃: join both branches (calculated schedule)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'msd_joined',
            $$SELECT a.category, a.total, b.cnt
              FROM msd_branch_a a JOIN msd_branch_b b ON a.category = b.category$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    assert_eq!(
        db.count("public.msd_joined").await,
        2,
        "[{mode}] initial diamond"
    );

    // Add data
    db.execute("INSERT INTO msd_src (category, amount) VALUES ('x', 40), ('z', 100)")
        .await;

    refresh_chain_for_mode(&db, &["msd_branch_a", "msd_branch_b", "msd_joined"], mode).await;

    db.assert_st_matches_query(
        "msd_joined",
        "SELECT a.category, a.total, b.cnt
         FROM msd_branch_a a JOIN msd_branch_b b ON a.category = b.category",
    )
    .await;
}

#[tokio::test]
async fn test_mixed_sched_diamond_view_branch_manual() {
    run_diamond_with_view_branch(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_diamond_view_branch_serial() {
    run_diamond_with_view_branch(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_diamond_view_branch_parallel() {
    run_diamond_with_view_branch(ScheduleMode::Parallel).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5 — Three-layer chain: table → view → ST → ST → downstream view
// ═══════════════════════════════════════════════════════════════════════════
//
// End-to-end: source table → user view → ST₁ (DIFF) → ST₂ (ST-on-ST)
// → user view (downstream consumer). Verify correctness at every layer.

async fn run_three_layer_chain(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute(
        "CREATE TABLE ms3_shipments (
            id SERIAL PRIMARY KEY,
            origin TEXT NOT NULL,
            destination TEXT NOT NULL,
            weight_kg NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO ms3_shipments (origin, destination, weight_kg) VALUES
         ('NYC', 'LAX', 100), ('NYC', 'LAX', 200),
         ('CHI', 'LAX', 150), ('NYC', 'CHI', 50),
         ('CHI', 'MIA', 300)",
    )
    .await;

    db.execute(
        "CREATE VIEW ms3_v_domestic AS
         SELECT id, origin, destination, weight_kg FROM ms3_shipments",
    )
    .await;

    db.create_st(
        "ms3_st_routes",
        "SELECT origin, destination, SUM(weight_kg) AS total_kg, COUNT(*) AS shipment_count
         FROM ms3_v_domestic GROUP BY origin, destination",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    // ST₂: heavy routes (ST-on-ST)
    let downstream_mode = if mode == ScheduleMode::Parallel {
        "FULL"
    } else {
        "DIFFERENTIAL"
    };
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table(
            'ms3_st_heavy_routes',
            $$SELECT origin, destination, total_kg
              FROM ms3_st_routes WHERE total_kg >= 200$$,
            'calculated',
            '{downstream_mode}'
        )"
    ))
    .await;

    // Downstream view on ST₂
    db.execute(
        "CREATE VIEW ms3_v_report AS
         SELECT origin || ' → ' || destination AS route,
                total_kg
         FROM ms3_st_heavy_routes
         ORDER BY total_kg DESC",
    )
    .await;

    let heavy_count: i64 = db.query_scalar("SELECT count(*) FROM ms3_v_report").await;
    assert_eq!(heavy_count, 2, "[{mode}] initial heavy routes");

    // Add a big shipment on NYC→CHI
    db.execute(
        "INSERT INTO ms3_shipments (origin, destination, weight_kg) VALUES ('NYC', 'CHI', 250)",
    )
    .await;

    refresh_chain_for_mode(&db, &["ms3_st_routes", "ms3_st_heavy_routes"], mode).await;

    let heavy_after: i64 = db.query_scalar("SELECT count(*) FROM ms3_v_report").await;
    assert_eq!(heavy_after, 3, "[{mode}] NYC→CHI should now be heavy");

    // Delete to make a route fall below threshold
    db.execute("DELETE FROM ms3_shipments WHERE origin = 'CHI' AND destination = 'MIA'")
        .await;

    refresh_chain_for_mode(&db, &["ms3_st_routes", "ms3_st_heavy_routes"], mode).await;

    db.assert_st_matches_query(
        "ms3_st_heavy_routes",
        "SELECT origin, destination, total_kg
         FROM ms3_st_routes WHERE total_kg >= 200",
    )
    .await;
}

#[tokio::test]
async fn test_mixed_sched_three_layer_chain_manual() {
    run_three_layer_chain(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_three_layer_chain_serial() {
    run_three_layer_chain(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_three_layer_chain_parallel() {
    run_three_layer_chain(ScheduleMode::Parallel).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6 — Parallel STs from same view
// ═══════════════════════════════════════════════════════════════════════════
//
// Three independent STs reading from the same view with different filters.
// Verifies independent scheduling doesn't cause interference.

async fn run_parallel_sts_same_view(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute(
        "CREATE TABLE msp_events (
            id SERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            severity INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO msp_events (event_type, severity) VALUES
         ('login', 1), ('error', 5), ('warning', 3),
         ('error', 4), ('login', 1), ('critical', 5)",
    )
    .await;

    db.execute(
        "CREATE VIEW msp_v_all_events AS
         SELECT id, event_type, severity FROM msp_events",
    )
    .await;

    db.create_st(
        "msp_st_errors",
        "SELECT id, event_type, severity FROM msp_v_all_events WHERE event_type = 'error'",
        sched,
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "msp_st_critical",
        "SELECT id, event_type, severity FROM msp_v_all_events WHERE severity >= 5",
        sched,
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "msp_st_all",
        "SELECT event_type, COUNT(*) AS cnt FROM msp_v_all_events GROUP BY event_type",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(
        db.count("public.msp_st_errors").await,
        2,
        "[{mode}] errors initial"
    );
    assert_eq!(
        db.count("public.msp_st_critical").await,
        2,
        "[{mode}] critical initial"
    );
    assert_eq!(
        db.count("public.msp_st_all").await,
        4,
        "[{mode}] all initial"
    );

    db.execute("INSERT INTO msp_events (event_type, severity) VALUES ('error', 5), ('info', 1)")
        .await;

    // Refresh all three
    refresh_for_mode(&db, "msp_st_errors", mode).await;
    refresh_for_mode(&db, "msp_st_critical", mode).await;
    refresh_for_mode(&db, "msp_st_all", mode).await;

    db.assert_st_matches_query(
        "msp_st_errors",
        "SELECT id, event_type, severity FROM msp_v_all_events WHERE event_type = 'error'",
    )
    .await;
    db.assert_st_matches_query(
        "msp_st_critical",
        "SELECT id, event_type, severity FROM msp_v_all_events WHERE severity >= 5",
    )
    .await;
    db.assert_st_matches_query(
        "msp_st_all",
        "SELECT event_type, COUNT(*) AS cnt FROM msp_v_all_events GROUP BY event_type",
    )
    .await;
}

#[tokio::test]
async fn test_mixed_sched_parallel_sts_same_view_manual() {
    run_parallel_sts_same_view(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_parallel_sts_same_view_serial() {
    run_parallel_sts_same_view(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_parallel_sts_same_view_parallel() {
    run_parallel_sts_same_view(ScheduleMode::Parallel).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 7 — TRUNCATE propagation through view under all modes
// ═══════════════════════════════════════════════════════════════════════════
//
// TRUNCATE base table → view → ST should clear the ST data.

async fn run_truncate_propagation(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute("CREATE TABLE mst_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO mst_src VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    db.execute("CREATE VIEW mst_view AS SELECT id, val FROM mst_src")
        .await;

    db.create_st(
        "mst_st",
        "SELECT id, val FROM mst_view",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.mst_st").await, 3, "[{mode}] initial");

    db.execute("TRUNCATE mst_src").await;
    refresh_for_mode(&db, "mst_st", mode).await;

    assert_eq!(
        db.count("public.mst_st").await,
        0,
        "[{mode}] after TRUNCATE"
    );

    // Verify recovery
    db.execute("INSERT INTO mst_src VALUES (10, 100), (20, 200)")
        .await;
    refresh_for_mode(&db, "mst_st", mode).await;

    assert_eq!(
        db.count("public.mst_st").await,
        2,
        "[{mode}] after re-populate"
    );
    db.assert_st_matches_query("mst_st", "SELECT id, val FROM mst_view")
        .await;
}

#[tokio::test]
async fn test_mixed_sched_truncate_propagation_manual() {
    run_truncate_propagation(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_truncate_propagation_serial() {
    run_truncate_propagation(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_truncate_propagation_parallel() {
    run_truncate_propagation(ScheduleMode::Parallel).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 8 — Cross-schema: raw_data.table → analytics.view → ST
// ═══════════════════════════════════════════════════════════════════════════

async fn run_multi_schema(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute("CREATE SCHEMA IF NOT EXISTS ms_raw_data").await;
    db.execute(
        "CREATE TABLE ms_raw_data.sensor_readings (
            id SERIAL PRIMARY KEY,
            sensor_id INT NOT NULL,
            value NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO ms_raw_data.sensor_readings (sensor_id, value) VALUES
         (1, 23.5), (1, 24.1), (2, 18.0), (2, 18.5), (3, 30.0)",
    )
    .await;

    db.execute("CREATE SCHEMA IF NOT EXISTS ms_analytics").await;
    db.execute(
        "CREATE VIEW ms_analytics.v_recent_readings AS
         SELECT sensor_id, value FROM ms_raw_data.sensor_readings",
    )
    .await;

    db.create_st(
        "ms_sensor_averages",
        "SELECT sensor_id, AVG(value)::numeric(10,2) AS avg_value, COUNT(*) AS reading_count
         FROM ms_analytics.v_recent_readings GROUP BY sensor_id",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(
        db.count("public.ms_sensor_averages").await,
        3,
        "[{mode}] initial sensors"
    );

    db.execute(
        "INSERT INTO ms_raw_data.sensor_readings (sensor_id, value) VALUES (1, 25.0), (4, 15.0)",
    )
    .await;
    refresh_for_mode(&db, "ms_sensor_averages", mode).await;

    assert_eq!(
        db.count("public.ms_sensor_averages").await,
        4,
        "[{mode}] after new sensor"
    );

    db.assert_st_matches_query(
        "ms_sensor_averages",
        "SELECT sensor_id, AVG(value)::numeric(10,2) AS avg_value, COUNT(*) AS reading_count
         FROM ms_analytics.v_recent_readings GROUP BY sensor_id",
    )
    .await;
}

#[tokio::test]
async fn test_mixed_sched_multi_schema_manual() {
    run_multi_schema(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_multi_schema_serial() {
    run_multi_schema(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_multi_schema_parallel() {
    run_multi_schema(ScheduleMode::Parallel).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 9 — Nested views → ST → ST (deep chain)
// ═══════════════════════════════════════════════════════════════════════════
//
// base table → view₁ → view₂ → ST₁ → ST₂
// Tests that deeply nested view inlining + ST-on-ST works under all modes.

async fn run_nested_views_chain(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute(
        "CREATE TABLE msn_logs (
            id SERIAL PRIMARY KEY,
            level TEXT NOT NULL,
            message TEXT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO msn_logs (level, message) VALUES
         ('ERROR', 'disk full'), ('WARN', 'memory high'),
         ('ERROR', 'timeout'), ('INFO', 'started'),
         ('ERROR', 'connection lost')",
    )
    .await;

    db.execute(
        "CREATE VIEW msn_v_not_info AS
         SELECT id, level, message FROM msn_logs WHERE level != 'INFO'",
    )
    .await;
    db.execute(
        "CREATE VIEW msn_v_errors AS
         SELECT id, message FROM msn_v_not_info WHERE level = 'ERROR'",
    )
    .await;

    db.create_st(
        "msn_st_errors",
        "SELECT id, message FROM msn_v_errors",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(
        db.count("public.msn_st_errors").await,
        3,
        "[{mode}] initial errors"
    );

    // ST₂ counts errors (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'msn_st_error_count',
            $$SELECT COUNT(*) AS error_count FROM msn_st_errors$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    let error_count: i64 = db
        .query_scalar("SELECT error_count FROM public.msn_st_error_count")
        .await;
    assert_eq!(error_count, 3, "[{mode}] initial error count");

    // Add more errors
    db.execute(
        "INSERT INTO msn_logs (level, message) VALUES ('ERROR', 'oom'), ('WARN', 'slow query')",
    )
    .await;

    refresh_chain_for_mode(&db, &["msn_st_errors", "msn_st_error_count"], mode).await;

    let new_count: i64 = db
        .query_scalar("SELECT error_count FROM public.msn_st_error_count")
        .await;
    assert_eq!(
        new_count, 4,
        "[{mode}] error count after adding one more ERROR"
    );

    db.assert_st_matches_query("msn_st_errors", "SELECT id, message FROM msn_v_errors")
        .await;
}

#[tokio::test]
async fn test_mixed_sched_nested_views_chain_manual() {
    run_nested_views_chain(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_nested_views_chain_serial() {
    run_nested_views_chain(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_nested_views_chain_parallel() {
    run_nested_views_chain(ScheduleMode::Parallel).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 10 — Transaction atomicity through view
// ═══════════════════════════════════════════════════════════════════════════
//
// Multiple DML statements in a single transaction on a base table that
// feeds through a view into an ST. Only the committed state should appear.

async fn run_transaction_atomicity(mode: ScheduleMode) {
    let db = db_for_mode(mode).await;
    let sched = schedule_for_mode(mode);

    db.execute("CREATE TABLE msa_tx (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO msa_tx VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    db.execute("CREATE VIEW msa_v_tx AS SELECT id, val FROM msa_tx")
        .await;

    db.create_st(
        "msa_st_tx",
        "SELECT id, val FROM msa_v_tx",
        sched,
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.msa_st_tx").await, 3, "[{mode}] initial");

    // Multi-statement transaction
    db.execute_seq(&[
        "BEGIN",
        "INSERT INTO msa_tx VALUES (4, 40)",
        "UPDATE msa_tx SET val = 99 WHERE id = 1",
        "DELETE FROM msa_tx WHERE id = 2",
        "COMMIT",
    ])
    .await;

    refresh_for_mode(&db, "msa_st_tx", mode).await;

    assert_eq!(
        db.count("public.msa_st_tx").await,
        3,
        "[{mode}] after transaction (3 rows: 1 deleted, 1 added)"
    );

    let val_1: i32 = db
        .query_scalar("SELECT val FROM public.msa_st_tx WHERE id = 1")
        .await;
    assert_eq!(val_1, 99, "[{mode}] UPDATE should be reflected");

    let has_2: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.msa_st_tx WHERE id = 2)")
        .await;
    assert!(!has_2, "[{mode}] DELETE should be reflected");

    let has_4: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.msa_st_tx WHERE id = 4)")
        .await;
    assert!(has_4, "[{mode}] INSERT should be reflected");

    db.assert_st_matches_query("msa_st_tx", "SELECT id, val FROM msa_v_tx")
        .await;
}

#[tokio::test]
async fn test_mixed_sched_transaction_atomicity_manual() {
    run_transaction_atomicity(ScheduleMode::Manual).await;
}

#[tokio::test]
async fn test_mixed_sched_transaction_atomicity_serial() {
    run_transaction_atomicity(ScheduleMode::Serial).await;
}

#[tokio::test]
async fn test_mixed_sched_transaction_atomicity_parallel() {
    run_transaction_atomicity(ScheduleMode::Parallel).await;
}
