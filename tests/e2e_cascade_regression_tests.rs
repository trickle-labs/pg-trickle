//! Regression tests for stream-table-on-stream-table (ST-on-ST) cascade bugs
//! discovered while following the GETTING_STARTED.md walkthrough.
//!
//! These tests cover:
//!
//! 1. **CDC trigger exclusion** — `has_user_triggers` must not count
//!    pg_trickle's own CDC triggers as user triggers (Bug: `pg_trickle_%`
//!    triggers were not excluded, causing incorrect DML path selection).
//!
//! 2. **ST-on-ST cascade propagation** — Changes to a base table must
//!    propagate through a chain of stream tables via manual refresh calls
//!    in topological (source-to-sink) order.
//!
//! 3. **0-row DIFFERENTIAL must not bump data_timestamp** — A DIFFERENTIAL
//!    refresh that finds no rows in the current frontier window (stale
//!    change buffer rows not yet cleaned up) must not advance
//!    `data_timestamp`, because downstream STs compare
//!    `upstream.data_timestamp > us.data_timestamp` to detect changes.
//!    Bumping it would trigger spurious FULL refreshes downstream.
//!
//! 4. **FULL refresh for ST-upstream changes** — When a STREAM_TABLE
//!    upstream has newer data than the downstream ST, a manual refresh
//!    must pick up those changes (since there is no CDC change buffer
//!    between stream tables).
//!
//! 5. **ST-on-ST must use DIFFERENTIAL, not FULL** — When a downstream
//!    ST has `refresh_mode = DIFFERENTIAL` and its upstream ST has changes,
//!    the scheduler must use DIFFERENTIAL refresh via ST change buffers
//!    (`changes_pgt_{id}`), not force FULL. Guards against the Phase 8
//!    regression where code restructuring re-introduced a force-FULL
//!    override.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Set up a simple two-layer cascade:
///   base_table `orders` → ST `order_summary` → ST `order_report`
///
/// `order_summary` uses a `'calculated'` schedule (CALCULATED mode).
/// `order_report` uses a '1m' schedule.
async fn setup_two_layer_cascade(db: &E2eDb) {
    db.execute(
        "CREATE TABLE orders (
            id    SERIAL PRIMARY KEY,
            region TEXT NOT NULL,
            amount NUMERIC(10,2) NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO orders (region, amount) VALUES
            ('East', 100), ('East', 200), ('West', 300)",
    )
    .await;

    // Layer 1: order_summary — aggregates per region from base table
    db.create_st(
        "order_summary",
        "SELECT region, COUNT(*) AS order_count, SUM(amount) AS total_amount
         FROM orders GROUP BY region",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Layer 2: order_report — reads from order_summary (ST-on-ST)
    // Uses 'calculated' schedule (CALCULATED mode)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'order_report',
            $$SELECT region, total_amount FROM order_summary WHERE order_count > 0$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;
}

// ── Test 1: CDC trigger exclusion ────────────────────────────────────────────

/// Regression: `has_user_triggers` must exclude `pg_trickle_%` CDC triggers.
///
/// Before the fix, pg_trickle's own CDC triggers (named `pg_trickle_cdc_<oid>`)
/// were counted as user triggers because the exclusion filter only matched
/// `pgt_%`. This caused the refresh executor to use the slower explicit-DML
/// path unnecessarily.
///
/// This test creates a stream table on a source table that has NO user
/// triggers and verifies that the pg_trickle CDC triggers do not appear
/// as user triggers.
#[tokio::test]
async fn test_cdc_triggers_not_counted_as_user_triggers() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cdc_excl_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cdc_excl_src VALUES (1, 'a')").await;

    db.create_st(
        "cdc_excl_st",
        "SELECT id, val FROM cdc_excl_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Verify that CDC triggers exist on the source table
    let cdc_trigger_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_trigger t
             JOIN pg_class c ON t.tgrelid = c.oid
             WHERE c.relname = 'cdc_excl_src'
               AND t.tgname LIKE 'pg_trickle_%'",
        )
        .await;
    assert!(
        cdc_trigger_count > 0,
        "CDC triggers should exist on the source table"
    );

    // The stream table itself should have zero user triggers
    // (pg_trickle CDC triggers are on the *source* table, not the ST).
    // But more importantly: the source table should also report zero
    // user triggers when we exclude pg_trickle's own triggers.
    let source_oid: i32 = db
        .query_scalar("SELECT 'cdc_excl_src'::regclass::oid::int")
        .await;
    let has_user_triggers: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS(
                SELECT 1 FROM pg_trigger
                WHERE tgrelid = {}::oid
                  AND tgisinternal = false
                  AND tgname NOT LIKE 'pgt_%'
                  AND tgname NOT LIKE 'pg_trickle_%'
            )",
            source_oid
        ))
        .await;
    assert!(
        !has_user_triggers,
        "Source table should have no user triggers (CDC triggers must be excluded)"
    );

    // Now add an actual user trigger and verify it IS detected
    db.execute(
        "CREATE OR REPLACE FUNCTION user_noop() RETURNS TRIGGER AS $$
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .await;
    db.execute(
        "CREATE TRIGGER my_user_trigger AFTER INSERT ON cdc_excl_src
         FOR EACH ROW EXECUTE FUNCTION user_noop()",
    )
    .await;

    let has_user_triggers_after: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS(
                SELECT 1 FROM pg_trigger
                WHERE tgrelid = {}::oid
                  AND tgisinternal = false
                  AND tgname NOT LIKE 'pgt_%'
                  AND tgname NOT LIKE 'pg_trickle_%'
            )",
            source_oid
        ))
        .await;
    assert!(
        has_user_triggers_after,
        "Real user trigger should be detected after exclusion"
    );
}

// ── Test 2: ST-on-ST cascade — manual refresh propagation ────────────────────

/// Regression: Changes to a base table must propagate through a chain of
/// stream tables when refreshed manually in topological order.
///
/// Before the fix, `check_upstream_changes` only checked CDC change buffers
/// (TABLE sources). When the only upstream source was a STREAM_TABLE,
/// `determine_refresh_action` always returned NO_DATA because the change
/// buffer didn't exist.
#[tokio::test]
async fn test_st_on_st_cascade_propagates_insert() {
    let db = E2eDb::new().await.with_extension().await;
    setup_two_layer_cascade(&db).await;

    // Verify initial state
    let east_amount: String = db
        .query_scalar("SELECT total_amount::text FROM order_report WHERE region = 'East'")
        .await;
    assert_eq!(east_amount, "300.00", "East: 100 + 200 = 300");

    // Insert a new order
    db.execute("INSERT INTO orders (region, amount) VALUES ('East', 150)")
        .await;

    // Refresh in topological order (upstream first).
    // Use retry variant: background scheduler may race for the advisory lock.
    db.refresh_st_until_converged("order_summary").await;
    db.refresh_st_until_converged("order_report").await;

    // Verify the change cascaded
    let post_east_amount: String = db
        .query_scalar("SELECT total_amount::text FROM order_report WHERE region = 'East'")
        .await;
    assert_eq!(
        post_east_amount, "450.00",
        "East should be 100 + 200 + 150 = 450 after cascade"
    );

    // West should be untouched
    let west_amount: String = db
        .query_scalar("SELECT total_amount::text FROM order_report WHERE region = 'West'")
        .await;
    assert_eq!(west_amount, "300.00", "West should be unaffected");

    // Verify row-level correctness of the cascaded state
    db.assert_st_matches_query(
        "order_report",
        "SELECT region, total_amount FROM order_summary WHERE order_count > 0",
    )
    .await;
}

/// Regression: DELETE on a base table cascades through ST-on-ST chain.
#[tokio::test]
async fn test_st_on_st_cascade_propagates_delete() {
    let db = E2eDb::new().await.with_extension().await;
    setup_two_layer_cascade(&db).await;

    // Delete the West order
    db.execute("DELETE FROM orders WHERE region = 'West'").await;

    db.refresh_st_until_converged("order_summary").await;
    db.refresh_st_until_converged("order_report").await;

    // West had order_count=1; after delete it's 0, so the WHERE clause
    // in order_report (order_count > 0) will exclude it
    let west_exists: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM order_report WHERE region = 'West')")
        .await;
    assert!(
        !west_exists,
        "West should vanish from order_report after its only order is deleted"
    );

    // Verify full multiset: order_report must exactly match its defining query
    // applied to the current (post-delete, post-refresh) state of order_summary.
    db.assert_st_matches_query(
        "order_report",
        "SELECT region, total_amount FROM order_summary WHERE order_count > 0",
    )
    .await;
}

// ── Helper: fast scheduler ───────────────────────────────────────────────────

/// Configure the scheduler for fast testing:
/// - `pg_trickle.scheduler_interval_ms = 100` (wake every 100ms)
/// - `pg_trickle.min_schedule_seconds = 1` (allow 1-second schedule)
///
/// Also waits for the pg_trickle scheduler BGW to appear in pg_stat_activity.
/// The wait helper periodically bumps the launcher rescan signal and sends
/// SIGHUP so stale `last_attempt` entries are retried promptly.
async fn configure_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    // Disable auto-backoff so 1-second schedules never get stretched in slow
    // CI containers — the default (true since v0.10.0) would double the
    // effective interval once a refresh takes > 950 ms.
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.auto_backoff", "off").await;

    let sched_running = db
        .wait_for_scheduler(std::time::Duration::from_secs(90))
        .await;

    assert!(
        sched_running,
        "pg_trickle scheduler did not appear in pg_stat_activity within 90 s. \
         Possible causes: \
         (1) the launcher never re-probed the fresh test database after CREATE EXTENSION, \
         despite periodic launcher rescan nudges; \
         (2) launcher retry back-off (retry_ttl=15 s + poll=10 s = 25 s) exceeded \
         the timeout; \
         (3) pg_trickle.enabled GUC is false; \
         (4) max_worker_processes exhausted — E2E image sets it to 128."
    );
}

// ── Test 3: 0-row DIFFERENTIAL must not bump data_timestamp ─────────────────

/// Regression: A DIFFERENTIAL refresh that finds zero rows in the current
/// frontier window must NOT advance `data_timestamp`.
///
/// Before the fix, every DIFFERENTIAL refresh — even one that produced 0
/// inserts and 0 deletes — called `update_after_refresh` which bumped
/// `data_timestamp`. This caused downstream stream tables (which compare
/// `upstream.data_timestamp > us.data_timestamp`) to see a false "upstream
/// changed" signal, triggering unnecessary FULL refreshes every cycle.
///
/// This fix is in the **scheduler** path (`execute_scheduled_refresh`), so
/// the test uses auto-refresh via a short schedule rather than manual refresh.
#[tokio::test]
async fn test_zero_row_differential_preserves_data_timestamp() {
    // new_on_postgres_db() now creates an isolated per-test database while
    // still resetting server-level scheduler GUCs before the test starts.
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    // configure_fast_scheduler also waits for the scheduler BGW to appear.
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE ts_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO ts_src VALUES (1, 'a'), (2, 'b')")
        .await;

    // create_st with initialize=true populates the ST and sets timestamps.
    // The source rows were inserted before the CDC trigger was installed, so
    // there are no stale change-buffer entries after initialization.
    db.create_st("ts_st", "SELECT id, val FROM ts_src", "1s", "DIFFERENTIAL")
        .await;

    // Record last_refresh_at immediately after creation. Use COALESCE so the
    // comparison is safe even if the column is NULL for any reason.
    let initial_last_refresh: String = db
        .query_scalar(
            "SELECT COALESCE(last_refresh_at::text, 'never') \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ts_st'",
        )
        .await;

    // Wait for the first scheduler cycle (last_refresh_at will advance when
    // the scheduler runs, even if it finds no new rows — NoData path).
    // Loaded CI runners can need a launcher retry and scheduler poll before
    // the first cycle. Keep the progress assertion, but allow that bounded
    // retry window before declaring a scheduler liveness failure.
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > std::time::Duration::from_secs(90) {
            panic!(
                "Timed out waiting for first scheduler cycle \
                 (initial last_refresh_at = {})",
                initial_last_refresh
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let lr: String = db
            .query_scalar(
                "SELECT COALESCE(last_refresh_at::text, 'never') \
                 FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = 'ts_st'",
            )
            .await;
        if lr != initial_last_refresh {
            break;
        }
    }

    // Record data_timestamp and last_refresh_at after the first scheduler cycle.
    let ts_after_first_cycle: String = db
        .query_scalar(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'ts_st'",
        )
        .await;
    let lr_after_first_cycle: String = db
        .query_scalar(
            "SELECT COALESCE(last_refresh_at::text, 'never') \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ts_st'",
        )
        .await;

    // Wait for a SECOND scheduler cycle (no changes in source this time)
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > std::time::Duration::from_secs(30) {
            panic!(
                "Timed out waiting for second scheduler cycle \
                 (lr_after_first_cycle = {})",
                lr_after_first_cycle
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let lr: String = db
            .query_scalar(
                "SELECT COALESCE(last_refresh_at::text, 'never') \
                 FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = 'ts_st'",
            )
            .await;
        if lr != lr_after_first_cycle {
            break;
        }
    }

    // data_timestamp must NOT have advanced on the second (noop) cycle
    let ts_after_second_cycle: String = db
        .query_scalar(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'ts_st'",
        )
        .await;
    assert_eq!(
        ts_after_first_cycle, ts_after_second_cycle,
        "data_timestamp must not change when scheduler refresh finds no new data"
    );

    // Sanity check: insert data and verify data_timestamp DOES advance
    db.execute("INSERT INTO ts_src VALUES (3, 'c')").await;
    let refreshed = db
        .wait_for_auto_refresh("ts_st", std::time::Duration::from_secs(30))
        .await;
    assert!(refreshed, "ts_st should pick up the new row");

    let ts_after_real_change: String = db
        .query_scalar(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'ts_st'",
        )
        .await;
    assert_ne!(
        ts_after_first_cycle, ts_after_real_change,
        "data_timestamp must advance when scheduler refresh writes rows"
    );
}

// ── Test 4: No spurious downstream cascades after 0-row refresh ─────────────

/// Regression: A 0-row refresh of an upstream ST must not trigger a
/// downstream refresh cascade.
///
/// This is the end-to-end version of test 3: when upstream ST refreshes
/// with 0 rows (NO_DATA or 0-row DIFFERENTIAL), its `data_timestamp`
/// stays unchanged, so downstream STs that compare timestamps should
/// NOT see "upstream changed" and should NOT perform a FULL refresh.
///
/// Uses auto-refresh via short schedules to exercise the scheduler path
/// where the 0-row DIFFERENTIAL fix lives.
#[tokio::test]
async fn test_no_spurious_cascade_after_noop_upstream_refresh() {
    // new_on_postgres_db() now creates an isolated per-test database while
    // still resetting server-level scheduler GUCs before the test starts.
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Build a two-layer cascade with short schedules for auto-refresh
    db.execute(
        "CREATE TABLE cascade_noop (
            id    SERIAL PRIMARY KEY,
            region TEXT NOT NULL,
            amount NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO cascade_noop (region, amount) VALUES
            ('East', 100), ('West', 200)",
    )
    .await;

    // Layer 1: short schedule
    db.create_st(
        "noop_summary",
        "SELECT region, SUM(amount) AS total FROM cascade_noop GROUP BY region",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Layer 2: ST-on-ST, also short schedule
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'noop_report',
            $$SELECT region, total FROM noop_summary$$,
            '1s',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Both STs are populated from create_st/create_stream_table.
    // Wait for first scheduler cycle on noop_report. Use COALESCE so the
    // comparison is safe even if last_refresh_at is NULL for any reason.

    // Wait for first scheduler cycle on noop_report
    let lr_initial: String = db
        .query_scalar(
            "SELECT COALESCE(last_refresh_at::text, 'never') \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'noop_report'",
        )
        .await;
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > std::time::Duration::from_secs(30) {
            panic!(
                "Timed out waiting for 1st noop_report scheduler cycle \
                 (lr_initial = {})",
                lr_initial
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let lr: String = db
            .query_scalar(
                "SELECT COALESCE(last_refresh_at::text, 'never') \
                 FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = 'noop_report'",
            )
            .await;
        if lr != lr_initial {
            break;
        }
    }

    // Record timestamps after first scheduler cycle (buffer is now clean)
    let report_ts_after_cycle1: String = db
        .query_scalar(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'noop_report'",
        )
        .await;
    let lr_after_cycle1: String = db
        .query_scalar(
            "SELECT COALESCE(last_refresh_at::text, 'never') \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'noop_report'",
        )
        .await;

    // Wait for second scheduler cycle (no DML changes)
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > std::time::Duration::from_secs(30) {
            panic!(
                "Timed out waiting for 2nd noop_report scheduler cycle \
                 (lr_after_cycle1 = {})",
                lr_after_cycle1
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let lr: String = db
            .query_scalar(
                "SELECT COALESCE(last_refresh_at::text, 'never') \
                 FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = 'noop_report'",
            )
            .await;
        if lr != lr_after_cycle1 {
            break;
        }
    }

    // data_timestamp must be stable — no spurious cascade
    let report_ts_after_cycle2: String = db
        .query_scalar(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'noop_report'",
        )
        .await;
    assert_eq!(
        report_ts_after_cycle1, report_ts_after_cycle2,
        "noop_report data_timestamp should not advance when upstream has no changes"
    );

    let report_rows = db.count("public.noop_report").await;
    assert_eq!(
        report_rows, 2,
        "noop_report should have 2 rows (East + West)"
    );
}

// ── Test 5: Three-layer cascade ─────────────────────────────────────────────

/// Regression: A three-layer STREAM_TABLE cascade (base → ST₁ → ST₂ → ST₃)
/// correctly propagates DML through all layers.
///
/// This mirrors the GETTING_STARTED.md `department_tree` → `department_stats`
/// → `department_report` pipeline but uses a simpler schema to isolate the
/// cascade mechanism from the recursive CTE logic.
#[tokio::test]
async fn test_three_layer_cascade_insert_propagates() {
    let db = E2eDb::new().await.with_extension().await;

    // Base table
    db.execute(
        "CREATE TABLE items (
            id       SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            price    NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO items (category, price) VALUES
            ('A', 10), ('A', 20), ('B', 30)",
    )
    .await;

    // Layer 1: category_totals (reads from base)
    db.create_st(
        "category_totals",
        "SELECT category, COUNT(*) AS cnt, SUM(price) AS total
         FROM items GROUP BY category",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Layer 2: category_flags (reads from ST layer 1)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'category_flags',
            $$SELECT category, total,
                     CASE WHEN total > 25 THEN true ELSE false END AS is_big
              FROM category_totals$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Layer 3: big_categories (reads from ST layer 2)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'big_categories',
            $$SELECT category, total FROM category_flags WHERE is_big = true$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Initial state: A total=30 (>25, big), B total=30 (>25, big)
    assert_eq!(
        db.count("public.big_categories").await,
        2,
        "Both A(30) and B(30) qualify as big initially"
    );

    // Insert a new item in category C
    db.execute("INSERT INTO items (category, price) VALUES ('C', 50)")
        .await;

    // Refresh all three layers in topological order.
    // Each layer must consume its pending changes before refreshing its downstream.
    db.refresh_st_until_converged("category_totals").await;
    db.refresh_st_until_converged("category_flags").await;
    db.refresh_st_until_converged("big_categories").await;

    // C has total=50 > 25, so it should appear in big_categories
    assert_eq!(
        db.count("public.big_categories").await,
        3,
        "A(30), B(30), C(50) should all be big"
    );

    let c_total: String = db
        .query_scalar("SELECT total::text FROM big_categories WHERE category = 'C'")
        .await;
    assert_eq!(c_total, "50.00");

    // Verify row-level correctness of the full 3-layer cascade via multiset comparison.
    // big_categories defines: SELECT category, total FROM category_flags WHERE is_big = true
    // category_flags has been refreshed, so this evaluates the ST's live state correctly.
    db.assert_st_matches_query(
        "big_categories",
        "SELECT category, total FROM category_flags WHERE is_big = true",
    )
    .await;
}

/// Regression: UPDATE on a base table cascades correctly through three layers.
#[tokio::test]
async fn test_three_layer_cascade_update_propagates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE products (
            id SERIAL PRIMARY KEY, category TEXT NOT NULL, price NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO products (category, price) VALUES ('X', 100), ('X', 200), ('Y', 50)")
        .await;

    // Layer 1
    db.create_st(
        "prod_summary",
        "SELECT category, SUM(price) AS total FROM products GROUP BY category",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Layer 2 (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'prod_labels',
            $$SELECT category, total,
                     CASE WHEN total >= 200 THEN 'premium' ELSE 'basic' END AS tier
              FROM prod_summary$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Layer 3 (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'premium_categories',
            $$SELECT category, total FROM prod_labels WHERE tier = 'premium'$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Initial: X total=300 (premium), Y total=50 (basic)
    assert_eq!(db.count("public.premium_categories").await, 1);
    let initial_cat: String = db
        .query_scalar("SELECT category FROM premium_categories")
        .await;
    assert_eq!(initial_cat, "X");

    // Update: increase Y's price so its total becomes 250 (premium)
    db.execute("UPDATE products SET price = 250 WHERE category = 'Y' AND price = 50")
        .await;

    db.refresh_st_until_converged("prod_summary").await;
    db.refresh_st_until_converged("prod_labels").await;
    db.refresh_st_until_converged("premium_categories").await;

    // Both X (300) and Y (250) should now be premium
    assert_eq!(
        db.count("public.premium_categories").await,
        2,
        "Both X and Y should be premium after update"
    );

    let cats: Vec<String> =
        sqlx::query_scalar("SELECT category FROM premium_categories ORDER BY category")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(cats, vec!["X", "Y"]);

    // Verify row-level correctness of the full 3-layer cascade
    db.assert_st_matches_query(
        "premium_categories",
        "SELECT category, total FROM prod_labels WHERE tier = 'premium'",
    )
    .await;
}

// ── Test 6: Dependencies catalog for ST-on-ST ───────────────────────────────

/// Regression: When a ST reads from another ST (not a base table), the
/// dependency must be recorded as `source_type = 'STREAM_TABLE'` in
/// `pgtrickle.pgt_dependencies`.
///
/// This is critical because `check_upstream_changes` dispatches to
/// different detection strategies based on source_type:
/// - TABLE → check CDC change buffer
/// - STREAM_TABLE → compare data_timestamp
#[tokio::test]
async fn test_st_on_st_dependency_is_stream_table_type() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE dep_base (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO dep_base VALUES (1, 10)").await;

    // Layer 1: reads from base table
    db.create_st(
        "dep_layer1",
        "SELECT id, val FROM dep_base",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Layer 2: reads from layer 1 (a stream table)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'dep_layer2',
            $$SELECT id, val * 2 AS doubled FROM dep_layer1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Check that dep_layer2 depends on dep_layer1 as STREAM_TABLE
    let source_type: String = db
        .query_scalar(
            "SELECT d.source_type
             FROM pgtrickle.pgt_dependencies d
             JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
             WHERE st.pgt_name = 'dep_layer2'",
        )
        .await;
    assert_eq!(
        source_type, "STREAM_TABLE",
        "Dependency on another stream table must be recorded as STREAM_TABLE"
    );

    // dep_layer1 should depend on dep_base as TABLE
    let base_source_type: String = db
        .query_scalar(
            "SELECT d.source_type
             FROM pgtrickle.pgt_dependencies d
             JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
             WHERE st.pgt_name = 'dep_layer1'",
        )
        .await;
    assert_eq!(
        base_source_type, "TABLE",
        "Dependency on a base table must be recorded as TABLE"
    );
}

// ── Test: ST-on-ST must use DIFFERENTIAL, never force FULL ──────────────────

/// Regression guard: When an ST-on-ST downstream has `refresh_mode =
/// DIFFERENTIAL` and its upstream ST has fresh changes in the change buffer
/// (`changes_pgt_{id}`), the scheduler must use DIFFERENTIAL — not force
/// FULL.
///
/// Phase 8 (v0.11.0) built full infrastructure for differential ST-on-ST
/// refresh (change buffers, pgt_ placeholders, scan operator routing,
/// frontier tracking).  A subsequent merge (Phase 6 code restructuring)
/// accidentally re-introduced a `has_stream_table_changes → Full` override
/// in several scheduler paths.  This test prevents that regression from
/// recurring.
#[tokio::test]
async fn test_st_on_st_uses_differential_not_full() {
    let db = E2eDb::new().await.with_extension().await;

    // Source table
    db.execute(
        "CREATE TABLE ssd_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO ssd_src (grp, val) VALUES
            ('a', 10), ('a', 20), ('b', 30)",
    )
    .await;

    // L1: aggregate ST reading from base table
    db.create_st(
        "ssd_upstream",
        "SELECT grp, SUM(val) AS total FROM ssd_src GROUP BY grp",
        "calculated",
        "DIFFERENTIAL",
    )
    .await;

    // L2: downstream ST reading from the upstream ST (ST-on-ST)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'ssd_downstream',
            $$SELECT grp, total * 2 AS doubled FROM ssd_upstream$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Populate both STs in topological order
    db.refresh_st_until_converged("ssd_upstream").await;
    db.refresh_st_until_converged("ssd_downstream").await;

    // Enable fast scheduler
    configure_fast_scheduler(&db).await;

    // Insert delta into source table — triggers CDC
    db.execute("INSERT INTO ssd_src (grp, val) VALUES ('a', 40), ('b', 50)")
        .await;

    // Wait for the downstream ST to reflect the new data.
    //
    // The scheduler may perform a spurious NO_DATA + DIFFERENTIAL cycle
    // (triggered by the NS-8 last_refresh_at fallback) before the CDC
    // changes from the INSERT are actually processed.  Waiting only for
    // "a new COMPLETED refresh" can break too early on such a spurious
    // cycle.  Instead, poll until the upstream data has been refreshed
    // (SUM(total) reflects the new rows) AND the downstream has
    // propagated it (SUM(doubled) = 2 × SUM(total)).
    let timeout = std::time::Duration::from_secs(60);
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for ssd_downstream to receive correct data \
                 after delta INSERT (upstream may not have been refreshed)"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // grp 'a': 10+20+40 = 70, grp 'b': 30+50 = 80 → total = 150
        let upstream_total: i64 = db
            .query_scalar("SELECT COALESCE(SUM(total), 0)::bigint FROM ssd_upstream")
            .await;
        if upstream_total < 150 {
            continue; // upstream not refreshed yet
        }

        // doubled total should be 300 (2 × 150)
        let downstream_total: i64 = db
            .query_scalar("SELECT COALESCE(SUM(doubled), 0)::bigint FROM ssd_downstream")
            .await;
        if downstream_total >= 300 {
            break;
        }
    }

    // The downstream ST must have used at least one DIFFERENTIAL refresh
    // (not FULL) to propagate the upstream changes. Check the most recent
    // non-NO_DATA refresh — that's the one that carried the actual delta.
    let action: String = db
        .query_scalar(
            "SELECT h.action
             FROM pgtrickle.pgt_refresh_history h
             JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
             WHERE st.pgt_name = 'ssd_downstream'
               AND h.status = 'COMPLETED'
               AND h.action <> 'NO_DATA'
             ORDER BY h.refresh_id DESC
             LIMIT 1",
        )
        .await;

    assert_eq!(
        action, "DIFFERENTIAL",
        "ST-on-ST downstream must use DIFFERENTIAL refresh when upstream has \
         changes in the change buffer — force-FULL is a Phase 8 regression. \
         Got action='{action}'"
    );

    // Verify data correctness: doubled values should reflect the new totals
    let doubled_a: i64 = db
        .query_scalar("SELECT doubled FROM ssd_downstream WHERE grp = 'a'")
        .await;
    // grp 'a' has vals 10+20+40 = 70, doubled = 140
    assert_eq!(
        doubled_a, 140,
        "Expected grp 'a' doubled = 140, got {doubled_a}"
    );

    let doubled_b: i64 = db
        .query_scalar("SELECT doubled FROM ssd_downstream WHERE grp = 'b'")
        .await;
    // grp 'b' has vals 30+50 = 80, doubled = 160
    assert_eq!(
        doubled_b, 160,
        "Expected grp 'b' doubled = 160, got {doubled_b}"
    );
}

// ── STST-3: Multi-Level ST-on-ST Testing ─────────────────────────────────────

/// STST-3: DELETE on a base table propagates correctly through a three-layer
/// DIFFERENTIAL cascade.
#[tokio::test]
async fn test_three_layer_cascade_delete_propagates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE stst3_del_src (
            id       SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            price    NUMERIC(10,2) NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO stst3_del_src (category, price) VALUES
            ('A', 10), ('A', 20), ('B', 30), ('C', 50)",
    )
    .await;

    // Layer 1
    db.create_st(
        "stst3_del_l1",
        "SELECT category, COUNT(*) AS cnt, SUM(price) AS total
         FROM stst3_del_src GROUP BY category",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Layer 2
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_del_l2',
            $$SELECT category, total,
                     CASE WHEN total > 25 THEN true ELSE false END AS is_big
              FROM stst3_del_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Layer 3
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_del_l3',
            $$SELECT category, total FROM stst3_del_l2 WHERE is_big = true$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Initial: A=30 (big), B=30 (big), C=50 (big) → 3 rows
    assert_eq!(db.count("public.stst3_del_l3").await, 3);

    // Delete all rows for category A → A total drops to 0 (removed by GROUP BY)
    db.execute("DELETE FROM stst3_del_src WHERE category = 'A'")
        .await;

    db.refresh_st_until_converged("stst3_del_l1").await;
    db.refresh_st_until_converged("stst3_del_l2").await;
    db.refresh_st_until_converged("stst3_del_l3").await;

    // A should be gone entirely
    assert_eq!(
        db.count("public.stst3_del_l3").await,
        2,
        "Only B(30) and C(50) should remain after deleting all A rows"
    );

    let a_exists: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM stst3_del_l3 WHERE category = 'A')")
        .await;
    assert!(!a_exists, "Category A should be gone from layer 3");

    db.assert_st_matches_query(
        "stst3_del_l3",
        "SELECT category, total FROM stst3_del_l2 WHERE is_big = true",
    )
    .await;
}

/// STST-3: Four-layer cascade propagates INSERT correctly through all levels.
#[tokio::test]
async fn test_four_layer_cascade_insert_propagates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE stst3_4l_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO stst3_4l_src (grp, val) VALUES
            ('x', 10), ('x', 20), ('y', 30)",
    )
    .await;

    // Layer 1: aggregate
    db.create_st(
        "stst3_4l_l1",
        "SELECT grp, SUM(val) AS total FROM stst3_4l_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Layer 2: add label
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_4l_l2',
            $$SELECT grp, total, total * 2 AS doubled FROM stst3_4l_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Layer 3: filter
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_4l_l3',
            $$SELECT grp, doubled FROM stst3_4l_l2 WHERE doubled > 50$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Layer 4: final projection
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_4l_l4',
            $$SELECT grp, doubled AS final_val FROM stst3_4l_l3$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Initial: x total=30, doubled=60 (>50, passes); y total=30, doubled=60 (>50, passes)
    assert_eq!(
        db.count("public.stst3_4l_l4").await,
        2,
        "x(60) and y(60) both pass the doubled>50 filter"
    );

    // Insert: new group z with val=100 → total=100, doubled=200
    db.execute("INSERT INTO stst3_4l_src (grp, val) VALUES ('z', 100)")
        .await;

    // Refresh all 4 layers
    db.refresh_st_until_converged("stst3_4l_l1").await;
    db.refresh_st_until_converged("stst3_4l_l2").await;
    db.refresh_st_until_converged("stst3_4l_l3").await;
    db.refresh_st_until_converged("stst3_4l_l4").await;

    assert_eq!(
        db.count("public.stst3_4l_l4").await,
        3,
        "x(60), y(60), z(200) should all be in layer 4"
    );

    let z_val: i64 = db
        .query_scalar("SELECT final_val FROM stst3_4l_l4 WHERE grp = 'z'")
        .await;
    assert_eq!(z_val, 200, "z: total=100, doubled=200");

    db.assert_st_matches_query(
        "stst3_4l_l4",
        "SELECT grp, doubled AS final_val FROM stst3_4l_l3",
    )
    .await;
}

/// STST-3: Four-layer cascade propagates DELETE through all levels, including
/// a delete that removes a group entirely.
#[tokio::test]
async fn test_four_layer_cascade_delete_propagates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE stst3_4ld_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO stst3_4ld_src (grp, val) VALUES
            ('a', 50), ('b', 60), ('c', 70)",
    )
    .await;

    db.create_st(
        "stst3_4ld_l1",
        "SELECT grp, SUM(val) AS total FROM stst3_4ld_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_4ld_l2',
            $$SELECT grp, total, total + 10 AS adjusted FROM stst3_4ld_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_4ld_l3',
            $$SELECT grp, adjusted FROM stst3_4ld_l2 WHERE adjusted >= 70$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_4ld_l4',
            $$SELECT grp FROM stst3_4ld_l3$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Initial: a=50+10=60 (excluded), b=60+10=70 (included), c=70+10=80 (included)
    assert_eq!(db.count("public.stst3_4ld_l4").await, 2);

    // Delete c entirely
    db.execute("DELETE FROM stst3_4ld_src WHERE grp = 'c'")
        .await;

    db.refresh_st_until_converged("stst3_4ld_l1").await;
    db.refresh_st_until_converged("stst3_4ld_l2").await;
    db.refresh_st_until_converged("stst3_4ld_l3").await;
    db.refresh_st_until_converged("stst3_4ld_l4").await;

    assert_eq!(
        db.count("public.stst3_4ld_l4").await,
        1,
        "Only b(70) should remain after deleting c"
    );

    let b_exists: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM stst3_4ld_l4 WHERE grp = 'b')")
        .await;
    assert!(b_exists, "Group b should still be present");

    db.assert_st_matches_query("stst3_4ld_l4", "SELECT grp FROM stst3_4ld_l3")
        .await;
}

/// STST-3: Mixed refresh modes — DIFFERENTIAL at some levels, FULL at others.
/// Verifies correctness when a FULL-mode ST is between two DIFFERENTIAL STs.
#[tokio::test]
async fn test_mixed_refresh_modes_in_cascade() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE stst3_mix_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO stst3_mix_src (grp, val) VALUES
            ('a', 10), ('a', 20), ('b', 30)",
    )
    .await;

    // Layer 1: DIFFERENTIAL
    db.create_st(
        "stst3_mix_l1",
        "SELECT grp, SUM(val) AS total FROM stst3_mix_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Layer 2: FULL (intentionally different mode)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_mix_l2',
            $$SELECT grp, total * 3 AS tripled FROM stst3_mix_l1$$,
            'calculated',
            'FULL'
        )",
    )
    .await;

    // Layer 3: DIFFERENTIAL (back to differential after a FULL layer)
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_mix_l3',
            $$SELECT grp, tripled FROM stst3_mix_l2 WHERE tripled > 50$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Initial: a=30*3=90 (>50), b=30*3=90 (>50)
    assert_eq!(db.count("public.stst3_mix_l3").await, 2);

    // Insert: make group c with small value (won't pass filter)
    db.execute("INSERT INTO stst3_mix_src (grp, val) VALUES ('c', 5)")
        .await;
    // Update: increase b so tripled changes
    db.execute("UPDATE stst3_mix_src SET val = 100 WHERE grp = 'b' AND val = 30")
        .await;

    db.refresh_st_until_converged("stst3_mix_l1").await;
    db.refresh_st_until_converged("stst3_mix_l2").await;
    db.refresh_st_until_converged("stst3_mix_l3").await;

    // a: 30*3=90 (>50, still in); b: 100*3=300 (>50, in); c: 5*3=15 (<50, out)
    assert_eq!(
        db.count("public.stst3_mix_l3").await,
        2,
        "a(90) and b(300) pass; c(15) does not"
    );

    let b_tripled: i64 = db
        .query_scalar("SELECT tripled FROM stst3_mix_l3 WHERE grp = 'b'")
        .await;
    assert_eq!(b_tripled, 300, "b: 100*3=300");

    db.assert_st_matches_query(
        "stst3_mix_l3",
        "SELECT grp, tripled FROM stst3_mix_l2 WHERE tripled > 50",
    )
    .await;
}

/// STST-3: Concurrent DML at multiple levels — INSERT into base table while
/// an intermediate ST also has pending changes from a previous cycle.
#[tokio::test]
async fn test_cascade_concurrent_dml_at_multiple_levels() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE stst3_conc_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO stst3_conc_src (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    db.create_st(
        "stst3_conc_l1",
        "SELECT grp, SUM(val) AS total FROM stst3_conc_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_conc_l2',
            $$SELECT grp, total FROM stst3_conc_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_conc_l3',
            $$SELECT grp, total FROM stst3_conc_l2$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Step 1: Refresh only L1 (L2, L3 still stale from initial)
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['stst3_conc_l1', 'stst3_conc_l2', 'stst3_conc_l3'])")
        .await;
    db.execute("INSERT INTO stst3_conc_src (grp, val) VALUES ('a', 30)")
        .await;
    let mut writer = db
        .pool
        .begin()
        .await
        .expect("begin controlled source writer");
    sqlx::query("LOCK TABLE stst3_conc_src IN ROW EXCLUSIVE MODE")
        .execute(&mut *writer)
        .await
        .expect("hold source writer lock");
    let last_refresh: i64 = db
        .query_scalar("SELECT COALESCE(MAX(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    tokio::join!(db.refresh_st_until_converged("stst3_conc_l1"), async {
        assert!(
            db.wait_for_condition(
                "manual refresh held back by a source writer",
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                     JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
                     WHERE st.pgt_name = 'stst3_conc_l1' AND h.refresh_id > {last_refresh} \
                       AND h.initiated_by = 'MANUAL' AND h.status = 'COMPLETED')"
                ),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_millis(50),
            )
            .await,
            "manual refresh must complete a held-back attempt before the writer is released"
        );
        let held_total: i64 = db
            .query_scalar("SELECT total FROM stst3_conc_l1 WHERE grp = 'a'")
            .await;
        assert_eq!(
            held_total, 10,
            "writer holdback must preserve the old result"
        );
        writer
            .rollback()
            .await
            .expect("release controlled source writer");
    });
    let l1_total: i64 = db
        .query_scalar("SELECT total FROM stst3_conc_l1 WHERE grp = 'a'")
        .await;
    assert_eq!(
        l1_total, 40,
        "L1 must consume the first insert before the second DML"
    );
    let l2_total: i64 = db
        .query_scalar("SELECT total FROM stst3_conc_l2 WHERE grp = 'a'")
        .await;
    assert_eq!(
        l2_total, 10,
        "L2 must retain pending changes from the first cycle"
    );

    // Step 2: More DML before L2/L3 have caught up
    db.execute("INSERT INTO stst3_conc_src (grp, val) VALUES ('b', 80)")
        .await;

    // Refresh all in order — L1 picks up second insert, L2 catches up to both
    db.refresh_st_until_converged("stst3_conc_l1").await;
    db.refresh_st_until_converged("stst3_conc_l2").await;
    db.refresh_st_until_converged("stst3_conc_l3").await;

    // a: 10+30 = 40, b: 20+80 = 100
    let a_total: i64 = db
        .query_scalar("SELECT total FROM stst3_conc_l3 WHERE grp = 'a'")
        .await;
    assert_eq!(a_total, 40, "a: 10+30 = 40");

    let b_total: i64 = db
        .query_scalar("SELECT total FROM stst3_conc_l3 WHERE grp = 'b'")
        .await;
    assert_eq!(b_total, 100, "b: 20+80 = 100");

    db.assert_st_matches_query("stst3_conc_l3", "SELECT grp, total FROM stst3_conc_l2")
        .await;
}

/// STST-3: DROP of an intermediate-level ST and verify downstream behavior.
/// After dropping the intermediate ST, the downstream should error on refresh.
#[tokio::test]
async fn test_drop_intermediate_st_in_cascade() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE stst3_drop_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO stst3_drop_src (grp, val) VALUES ('a', 10)")
        .await;

    db.create_st(
        "stst3_drop_l1",
        "SELECT grp, SUM(val) AS total FROM stst3_drop_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_drop_l2',
            $$SELECT grp, total FROM stst3_drop_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_drop_l3',
            $$SELECT grp, total FROM stst3_drop_l2$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Verify initial data flows through all 3 layers
    assert_eq!(db.count("public.stst3_drop_l3").await, 1);

    // Drop the intermediate ST (L2)
    db.execute("SELECT pgtrickle.drop_stream_table('stst3_drop_l2', cascade => true)")
        .await;

    // L3 should also be dropped by CASCADE
    let l3_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM pgtrickle.pgt_stream_tables
                WHERE pgt_name = 'stst3_drop_l3'
            )",
        )
        .await;
    assert!(
        !l3_exists,
        "L3 should be dropped by CASCADE when L2 is dropped"
    );

    // L1 should still exist
    let l1_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM pgtrickle.pgt_stream_tables
                WHERE pgt_name = 'stst3_drop_l1'
            )",
        )
        .await;
    assert!(l1_exists, "L1 should still exist after dropping L2");
}

/// STST-3: UPDATE on base table propagates through a four-layer DIFFERENTIAL
/// cascade with correct intermediate values at each level.
#[tokio::test]
async fn test_four_layer_cascade_update_propagates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE stst3_4lu_src (
            id  SERIAL PRIMARY KEY,
            grp TEXT NOT NULL,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO stst3_4lu_src (grp, val) VALUES
            ('p', 10), ('p', 20), ('q', 50)",
    )
    .await;

    db.create_st(
        "stst3_4lu_l1",
        "SELECT grp, SUM(val) AS total FROM stst3_4lu_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_4lu_l2',
            $$SELECT grp, total, total * 2 AS doubled FROM stst3_4lu_l1$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_4lu_l3',
            $$SELECT grp, doubled + 5 AS adjusted FROM stst3_4lu_l2$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'stst3_4lu_l4',
            $$SELECT grp, adjusted FROM stst3_4lu_l3 WHERE adjusted > 100$$,
            'calculated',
            'DIFFERENTIAL'
        )",
    )
    .await;

    // Initial: p=30, doubled=60, adjusted=65 (<100, excluded);
    //          q=50, doubled=100, adjusted=105 (>100, included)
    assert_eq!(db.count("public.stst3_4lu_l4").await, 1);

    // Update: increase p's val so it qualifies
    db.execute("UPDATE stst3_4lu_src SET val = 80 WHERE grp = 'p' AND val = 10")
        .await;

    db.refresh_st_until_converged("stst3_4lu_l1").await;
    db.refresh_st_until_converged("stst3_4lu_l2").await;
    db.refresh_st_until_converged("stst3_4lu_l3").await;
    db.refresh_st_until_converged("stst3_4lu_l4").await;

    // p: 80+20=100, doubled=200, adjusted=205 (>100, now included)
    // q: 50, doubled=100, adjusted=105 (>100, still included)
    assert_eq!(
        db.count("public.stst3_4lu_l4").await,
        2,
        "Both p(205) and q(105) should pass the filter after update"
    );

    let p_adjusted: i64 = db
        .query_scalar("SELECT adjusted FROM stst3_4lu_l4 WHERE grp = 'p'")
        .await;
    assert_eq!(p_adjusted, 205, "p: (80+20)*2+5 = 205");

    db.assert_st_matches_query(
        "stst3_4lu_l4",
        "SELECT grp, adjusted FROM stst3_4lu_l3 WHERE adjusted > 100",
    )
    .await;
}
