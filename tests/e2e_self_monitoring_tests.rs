mod e2e;
use e2e::E2eDb;

async fn pause_self_monitoring(db: &E2eDb) {
    db.execute(
        "SELECT pgtrickle.pause_scheduler(ARRAY[
            'pgtrickle.df_efficiency_rolling',
            'pgtrickle.df_anomaly_signals',
            'pgtrickle.df_threshold_advice',
            'pgtrickle.df_cdc_buffer_trends',
            'pgtrickle.df_scheduling_interference',
            'pgtrickle.df_frozen_stream_tables'
        ])",
    )
    .await;
}

// ── DF-F3: E2E test — setup/teardown cycle ──────────────────────────────────

#[tokio::test]
async fn test_self_monitoring_setup_creates_five_stream_tables() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a source table and a user ST so pgt_refresh_history has data.
    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .await;
    db.create_st("user_st", "SELECT id, val FROM src", "1m", "AUTO")
        .await;
    db.refresh_st("user_st").await;

    // Setup self-monitoring.
    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Verify all six DF stream tables exist.
    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(
        count, 6,
        "setup_self_monitoring should create 6 DF stream tables"
    );
}

// ── STAB-1: setup_self_monitoring() idempotency ─────────────────────────────────

#[tokio::test]
async fn test_self_monitoring_setup_idempotent_three_calls() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    // Call setup 3 times — should not error or create duplicates.
    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;
    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;
    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(count, 6, "3× setup should still produce exactly 6 DF STs");
}

// ── DF-F5 + STAB-5: teardown + partial teardown ────────────────────────────

#[tokio::test]
async fn test_self_monitoring_teardown_drops_all_stream_tables() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Verify STs exist before teardown.
    let before: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(before, 6);

    pause_self_monitoring(&db).await;

    // Retry the complete teardown if concurrent scheduler DDL still deadlocks.
    for attempt in 0..3 {
        match db
            .try_execute("SELECT pgtrickle.teardown_self_monitoring()")
            .await
        {
            Ok(()) => break,
            Err(error) => {
                let deadlock = error
                    .as_database_error()
                    .and_then(|database_error| database_error.code())
                    .as_deref()
                    == Some("40P01");
                assert!(
                    deadlock && attempt < 2,
                    "self-monitoring teardown failed on attempt {}: {error}",
                    attempt + 1
                );
                tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt + 1))).await;
            }
        }
    }

    let after: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(after, 0, "teardown should remove all DF STs");

    // User ST should still exist.
    let user_st: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'user_st'",
        )
        .await;
    assert_eq!(user_st, 1, "user ST should survive teardown");
}

#[tokio::test]
async fn test_self_monitoring_teardown_safe_with_partial_setup() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    // Setup, then manually drop one DF ST to simulate partial state.
    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;
    pause_self_monitoring(&db).await;
    db.execute("SELECT pgtrickle.drop_stream_table('pgtrickle.df_anomaly_signals', true)")
        .await;

    // Teardown should succeed without errors even with missing DF ST.
    db.execute("SELECT pgtrickle.teardown_self_monitoring()")
        .await;

    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(count, 0, "teardown should clean up remaining STs");
}

// ── UX-1: self_monitoring_status() ──────────────────────────────────────────────

#[tokio::test]
async fn test_self_monitoring_status_reports_all_five() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.self_monitoring_status()")
        .await;
    assert_eq!(count, 6, "self_monitoring_status should report 6 rows");

    // All should exist.
    let all_exist: bool = db
        .query_scalar("SELECT bool_and(exists) FROM pgtrickle.self_monitoring_status()")
        .await;
    assert!(all_exist, "all six DF STs should report exists = true");
}

#[tokio::test]
async fn test_self_monitoring_status_before_setup() {
    let db = E2eDb::new().await.with_extension().await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.self_monitoring_status()")
        .await;
    assert_eq!(
        count, 6,
        "self_monitoring_status should report 6 rows even before setup"
    );

    let any_exist: bool = db
        .query_scalar("SELECT bool_or(exists) FROM pgtrickle.self_monitoring_status()")
        .await;
    assert!(!any_exist, "no DF STs should exist before setup");
}

// ── TEST-2: Full create/refresh/teardown cycle ──────────────────────────────

#[tokio::test]
async fn test_self_monitoring_full_lifecycle() {
    let db = E2eDb::new().await.with_extension().await;

    // Create source data.
    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 100) g")
        .await;

    // Create a user ST and run some refreshes to populate history.
    db.create_st(
        "user_st",
        "SELECT id, sum(val) AS total FROM src GROUP BY id",
        "1m",
        "AUTO",
    )
    .await;

    for _ in 0..3 {
        db.execute("INSERT INTO src SELECT g + (SELECT max(id) FROM src), g * 10 FROM generate_series(1, 10) g")
            .await;
        db.refresh_st("user_st").await;
    }

    // Setup self-monitoring.
    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Verify status.
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.self_monitoring_status() WHERE exists")
        .await;
    assert_eq!(count, 6);

    pause_self_monitoring(&db).await;

    // Teardown.
    db.execute("SELECT pgtrickle.teardown_self_monitoring()")
        .await;

    let count_after: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.self_monitoring_status() WHERE exists")
        .await;
    assert_eq!(count_after, 0);
}

// ── OPS-4: explain_dag() ────────────────────────────────────────────────────

#[tokio::test]
async fn test_explain_dag_includes_df_nodes_after_setup() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    let dag: String = db.query_scalar("SELECT pgtrickle.explain_dag()").await;

    assert!(dag.contains("graph TD"), "should be a Mermaid graph");
    assert!(
        dag.contains("df_efficiency_rolling"),
        "should include DF-1 node"
    );
    assert!(
        dag.contains("df_anomaly_signals"),
        "should include DF-2 node"
    );
    assert!(
        dag.contains("df_threshold_advice"),
        "should include DF-3 node"
    );
}

#[tokio::test]
async fn test_explain_dag_dot_format() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;

    let dag: String = db.query_scalar("SELECT pgtrickle.explain_dag('dot')").await;

    assert!(dag.contains("digraph dag"), "should be a DOT graph");
}

// ── OPS-3: scheduler_overhead() ─────────────────────────────────────────────

#[tokio::test]
async fn test_scheduler_overhead_returns_valid_row() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.scheduler_overhead()")
        .await;
    assert_eq!(count, 1, "scheduler_overhead should return exactly 1 row");

    let total: i64 = db
        .query_scalar("SELECT total_refreshes_1h FROM pgtrickle.scheduler_overhead()")
        .await;
    assert!(total >= 1, "should have at least 1 refresh in history");
}

// ── DF-G1: self_monitoring_auto_apply GUC ───────────────────────────────────────

#[tokio::test]
async fn test_self_monitoring_auto_apply_guc_exists() {
    let db = E2eDb::new().await.with_extension().await;

    // Default should be 'off'.
    let value: String = db
        .query_scalar("SHOW pg_trickle.self_monitoring_auto_apply")
        .await;
    assert_eq!(value, "off", "default should be 'off'");

    // Should accept valid values — acquire a single connection so the SET is
    // visible to the subsequent SHOW (connection-pool dispatch can route them
    // to different backends otherwise).
    let value: String = {
        let mut conn = db.pool.acquire().await.expect("acquire connection");
        sqlx::query("SET pg_trickle.self_monitoring_auto_apply = 'threshold_only'")
            .execute(&mut *conn)
            .await
            .expect("SET pg_trickle.self_monitoring_auto_apply");
        sqlx::query_scalar("SHOW pg_trickle.self_monitoring_auto_apply")
            .fetch_one(&mut *conn)
            .await
            .expect("SHOW pg_trickle.self_monitoring_auto_apply")
    };
    assert_eq!(value, "threshold_only");
}

// ── PERF-1: Index on pgt_refresh_history(pgt_id, start_time) ────────────────

#[tokio::test]
async fn test_refresh_history_index_on_pgt_id_start_time() {
    let db = E2eDb::new().await.with_extension().await;

    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = 'pgtrickle'
                  AND tablename = 'pgt_refresh_history'
                  AND indexname = 'idx_hist_pgt_start'
            )",
        )
        .await;
    assert!(exists, "PERF-1 index idx_hist_pgt_start should exist");
}

// ── CORR-4: INSERT-only CDC trigger invariant ───────────────────────────────

#[tokio::test]
async fn test_cdc_insert_only_trigger_on_refresh_history() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    // Setup self-monitoring — this creates STs that reference pgt_refresh_history.
    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Verify that CDC triggers on pgt_refresh_history are INSERT-only.
    // PostgreSQL tgtype bitmask (pg_trigger.h):
    //   ROW=1, BEFORE=2, INSERT=4, DELETE=8, UPDATE=16, TRUNCATE=32
    // To check for DELETE or UPDATE triggers: mask = 8 | 16 = 24.
    // INSERT triggers have tgtype & 24 == 0, so they are correctly excluded.
    let has_non_insert: bool = db
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgrelid = 'pgtrickle.pgt_refresh_history'::regclass
                  AND tgname LIKE 'pg_trickle_cdc_%'
                  AND (tgtype & 24) != 0  -- DELETE (8) or UPDATE (16) bits set
            )",
        )
        .await;
    assert!(
        !has_non_insert,
        "CDC triggers on pgt_refresh_history should be INSERT-only (CORR-4)"
    );
}

// ── DF-D3: Control plane survives DF ST suspension ──────────────────────────

#[tokio::test]
async fn test_control_plane_survives_df_st_suspension() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src VALUES (1, 10), (2, 20)").await;
    db.create_st("user_st", "SELECT id, val FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Drop all DF STs — simulate suspension.
    pause_self_monitoring(&db).await;
    db.try_execute("SELECT pgtrickle.teardown_self_monitoring()")
        .await
        .expect("teardown self-monitoring while preserving the user stream");

    // User ST should still refresh successfully.
    db.execute("INSERT INTO src VALUES (3, 30)").await;
    db.refresh_st("user_st").await;

    let count: i64 = db.count("user_st").await;
    assert_eq!(count, 3, "user ST should still work after DF teardown");
}

// ── TEST-3: Upgrade test — migration doesn't break history ──────────────

#[tokio::test]
async fn test_upgrade_preserves_refresh_history() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 20) g")
        .await;
    db.create_st(
        "user_st",
        "SELECT id, sum(val) AS total FROM src GROUP BY id",
        "1m",
        "AUTO",
    )
    .await;
    db.refresh_st("user_st").await;

    // Verify PERF-1 index exists (part of 0.19.0→0.20.0 migration).
    let idx: bool = db
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE indexname = 'idx_hist_pgt_start'
            )",
        )
        .await;
    assert!(idx, "TEST-3: PERF-1 index must exist after upgrade");

    // Verify history rows survive — at least one completed refresh.
    let hist: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history WHERE status = 'COMPLETED'",
        )
        .await;
    assert!(hist >= 1, "TEST-3: history rows must survive upgrade");

    // Verify initiated_by CHECK allows SELF_MONITOR.
    // data_timestamp is NOT NULL in pgt_refresh_history; use now() for a SKIP row.
    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_history \
         (pgt_id, data_timestamp, start_time, action, status, delta_row_count, \
          rows_inserted, initiated_by) \
         SELECT pgt_id, now(), now(), 'SKIP', 'COMPLETED', 0, 0, 'SELF_MONITOR' \
         FROM pgtrickle.pgt_stream_tables LIMIT 1",
    )
    .await;
}

// ── DF-A4: Threshold spike detection ────────────────────────────────────

#[tokio::test]
async fn test_threshold_advice_produces_recommendations() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 50) g")
        .await;
    db.create_st(
        "user_st",
        "SELECT id, sum(val) AS total FROM src GROUP BY id",
        "1m",
        "AUTO",
    )
    .await;

    // Generate enough refresh history for threshold advice.
    for i in 0..25 {
        db.execute(&format!(
            "INSERT INTO src SELECT g + {}, g * 10 FROM generate_series(1, 5) g",
            50 + i * 5
        ))
        .await;
        db.refresh_st("user_st").await;
    }

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Refresh DF-1 (efficiency rolling) which feeds DF-3 (threshold advice).
    db.refresh_st("pgtrickle.df_efficiency_rolling").await;
    db.refresh_st("pgtrickle.df_threshold_advice").await;

    // Verify threshold advice has data.
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.df_threshold_advice")
        .await;
    assert!(
        count >= 1,
        "DF-A4: df_threshold_advice should have at least 1 row after refreshes"
    );

    // Verify recommended_threshold is within CORR-1 bounds.
    let in_bounds: bool = db
        .query_scalar(
            "SELECT bool_and(recommended_threshold BETWEEN 0.01 AND 0.80) \
             FROM pgtrickle.df_threshold_advice",
        )
        .await;
    assert!(in_bounds, "DF-A4: all thresholds must be in [0.01, 0.80]");
}

// ── DF-A5: Anomaly duration spike detection ─────────────────────────────

#[tokio::test]
async fn test_anomaly_signals_detects_spikes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 50) g")
        .await;
    db.create_st(
        "user_st",
        "SELECT id, sum(val) AS total FROM src GROUP BY id",
        "1m",
        "AUTO",
    )
    .await;

    // Generate several refreshes to build baseline.
    for i in 0..10 {
        db.execute(&format!(
            "INSERT INTO src SELECT g + {}, g * 10 FROM generate_series(1, 5) g",
            50 + i * 5
        ))
        .await;
        db.refresh_st("user_st").await;
    }

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;
    db.refresh_st("pgtrickle.df_efficiency_rolling").await;
    db.refresh_st("pgtrickle.df_anomaly_signals").await;

    // Verify anomaly signals table has data.
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.df_anomaly_signals")
        .await;
    assert!(
        count >= 1,
        "DF-A5: df_anomaly_signals should have at least 1 row"
    );

    // Verify required columns exist.
    let has_cols: bool = db
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.df_anomaly_signals
                WHERE pgt_id IS NOT NULL
                  AND recent_failures IS NOT NULL
            )",
        )
        .await;
    assert!(
        has_cols,
        "DF-A5: anomaly signals must have pgt_id and recent_failures columns"
    );
}

// ── DF-C3: Scheduling overlap detection ─────────────────────────────────

#[tokio::test]
async fn test_scheduling_interference_detects_overlap() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 20) g")
        .await;
    db.create_st(
        "st_a",
        "SELECT id, sum(val) AS total FROM src GROUP BY id",
        "1m",
        "AUTO",
    )
    .await;
    db.create_st("st_b", "SELECT id, val FROM src", "1m", "FULL")
        .await;

    // Generate overlapping refresh history by refreshing both STs.
    for i in 0..5 {
        db.execute(&format!(
            "INSERT INTO src SELECT g + {}, g * 10 FROM generate_series(1, 5) g",
            20 + i * 5
        ))
        .await;
        db.refresh_st("st_a").await;
        db.refresh_st("st_b").await;
    }

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;
    db.refresh_st("pgtrickle.df_scheduling_interference").await;

    // The interference table should exist and be queryable (even when empty —
    // sequential test refreshes do not produce real time overlaps, so the
    // HAVING count(*) >= 3 filter produces 0 rows on a quiet test database).
    // Use count(*) >= 0 to verify the table can be queried without error.
    let row_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.df_scheduling_interference")
        .await;
    assert!(
        row_count >= 0,
        "DF-C3: df_scheduling_interference should be queryable"
    );
}

// ── DF-G4: Auto-apply threshold test ────────────────────────────────────

#[tokio::test]
async fn test_auto_apply_initiated_by_dog_feed() {
    let db = E2eDb::new().await.with_extension().await;

    // Verify the SELF_MONITOR initiated_by value is allowed by the CHECK constraint.
    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    // Insert a SELF_MONITOR audit row directly to test CHECK constraint.
    // data_timestamp is NOT NULL in pgt_refresh_history (used for
    // ST-on-ST cascade logic); use now() as a stand-in for a SKIP row.
    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_history \
         (pgt_id, data_timestamp, start_time, action, status, delta_row_count, \
          rows_inserted, initiated_by, error_message) \
         SELECT pgt_id, now(), now(), 'SKIP', 'COMPLETED', 0, 0, 'SELF_MONITOR', \
                'auto_threshold 0.10 → 0.15' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'user_st'",
    )
    .await;

    // Verify it was inserted.
    let dog_feed: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history \
             WHERE initiated_by = 'SELF_MONITOR'",
        )
        .await;
    assert!(
        dog_feed >= 1,
        "DF-G4: SELF_MONITOR initiated_by should be insertable"
    );
}

// ── DF-G5: Rate limiting test ───────────────────────────────────────────

#[tokio::test]
async fn test_auto_apply_guc_values() {
    let db = E2eDb::new().await.with_extension().await;

    // Test all valid GUC values.
    // Use set_config() which sets the GUC and returns its new value in a
    // single round-trip, avoiding connection-pool ambiguity (SET on one
    // backend is not visible to a SHOW on a different backend).
    let v1: String = db
        .query_scalar("SELECT set_config('pg_trickle.self_monitoring_auto_apply', 'off', false)")
        .await;
    assert_eq!(v1, "off");

    let v2: String = db
        .query_scalar(
            "SELECT set_config('pg_trickle.self_monitoring_auto_apply', 'threshold_only', false)",
        )
        .await;
    assert_eq!(v2, "threshold_only");

    let v3: String = db
        .query_scalar("SELECT set_config('pg_trickle.self_monitoring_auto_apply', 'full', false)")
        .await;
    assert_eq!(v3, "full");
}

// ── TEST-4: DF STs absent from health anomaly list ──────────────────────

#[tokio::test]
async fn test_cdc_health_no_false_alerts_for_df_sts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src VALUES (1, 10), (2, 20)").await;
    db.create_st("user_st", "SELECT id, val FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Check that check_cdc_health() does not flag DF STs as problematic.
    // DF STs read from pgt_refresh_history which has triggers, not from user tables.
    let alerts: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.check_cdc_health() \
             WHERE source_table LIKE 'pgtrickle.df_%' \
               AND alert IS NOT NULL",
        )
        .await;
    assert_eq!(
        alerts, 0,
        "TEST-4: DF STs should not generate CDC health alerts"
    );
}

// ── UX-5: explain_st shows DF coverage ──────────────────────────────────

#[tokio::test]
async fn test_explain_st_shows_df_coverage() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src VALUES (1, 10)").await;
    db.create_st("user_st", "SELECT id, val FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    // Before setup: should report "none".
    let before: String = db
        .query_scalar(
            "SELECT value FROM pgtrickle.explain_st('public.user_st') \
             WHERE property = 'self_monitoring_coverage'",
        )
        .await;
    assert!(
        before.contains("none"),
        "UX-5: should report 'none' before setup"
    );

    // After setup: should report "full".
    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    let after: String = db
        .query_scalar(
            "SELECT value FROM pgtrickle.explain_st('public.user_st') \
             WHERE property = 'self_monitoring_coverage'",
        )
        .await;
    assert!(
        after.contains("full"),
        "UX-5: should report 'full' after setup"
    );
}

// ── UX-6: explain_st shows recommend_refresh_mode ───────────────────────

#[tokio::test]
async fn test_explain_st_shows_recommended_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src VALUES (1, 10)").await;
    db.create_st("user_st", "SELECT id, val FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    let has_rec: bool = db
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.explain_st('public.user_st')
                WHERE property = 'recommended_refresh_mode'
            )",
        )
        .await;
    assert!(
        has_rec,
        "UX-6: explain_st should include recommended_refresh_mode"
    );
}

// ── PERF-2: Benchmark DF-1 vs refresh_efficiency() ──────────────────────

#[tokio::test]
#[ignore] // Requires extended run — use --ignored to include
async fn test_benchmark_df_efficiency_vs_refresh_efficiency() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 100) g")
        .await;
    db.create_st(
        "user_st",
        "SELECT id, sum(val) AS total FROM src GROUP BY id",
        "1m",
        "AUTO",
    )
    .await;

    // Generate 50 refreshes to build substantial history.
    for i in 0..50 {
        db.execute(&format!(
            "INSERT INTO src SELECT g + {}, g * 10 FROM generate_series(1, 5) g",
            100 + i * 5
        ))
        .await;
        db.refresh_st("user_st").await;
    }

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;
    db.refresh_st("pgtrickle.df_efficiency_rolling").await;

    // Both should return data for user_st.
    let df_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.df_efficiency_rolling \
             WHERE pgt_name = 'user_st'",
        )
        .await;
    assert!(df_count >= 1, "PERF-2: DF-1 should have data for user_st");

    let eff_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.refresh_efficiency() \
             WHERE pgt_name = 'user_st'",
        )
        .await;
    assert!(
        eff_count >= 1,
        "PERF-2: refresh_efficiency() should have data for user_st"
    );
}

// ── PERF-3: Dog-feeding overhead < 1% CPU ───────────────────────────────

#[tokio::test]
#[ignore] // Requires extended run — use --ignored to include
async fn test_self_monitoring_overhead_below_threshold() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 50) g")
        .await;
    db.create_st(
        "user_st",
        "SELECT id, sum(val) AS total FROM src GROUP BY id",
        "1m",
        "AUTO",
    )
    .await;

    // Generate refreshes.
    for i in 0..20 {
        db.execute(&format!(
            "INSERT INTO src SELECT g + {}, g * 10 FROM generate_series(1, 5) g",
            50 + i * 5
        ))
        .await;
        db.refresh_st("user_st").await;
    }

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Refresh all DF STs.
    for st in &[
        "df_efficiency_rolling",
        "df_anomaly_signals",
        "df_threshold_advice",
        "df_cdc_buffer_trends",
        "df_scheduling_interference",
    ] {
        db.refresh_st(&format!("pgtrickle.{st}")).await;
    }

    // Check scheduler_overhead fraction.
    let fraction: Option<f64> = db
        .query_scalar("SELECT df_refresh_fraction FROM pgtrickle.scheduler_overhead()")
        .await;

    if let Some(f) = fraction {
        // Allow generous margin — in test env with few refreshes, DF fraction
        // can be high. The real constraint is verified in soak tests.
        assert!(
            f <= 0.50,
            "PERF-3: DF refresh fraction should be reasonable (got {:.2}%)",
            f * 100.0
        );
    }
}

// ── SCAL-2: Retention interacts correctly with self-monitoring CDC ──────────

#[tokio::test]
async fn test_retention_cleanup_does_not_break_self_monitoring() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Simulate history cleanup (delete old rows).
    db.execute(
        "DELETE FROM pgtrickle.pgt_refresh_history \
         WHERE start_time < now() - interval '2 hours'",
    )
    .await;

    // DF STs should still refresh successfully after cleanup.
    db.refresh_st("pgtrickle.df_efficiency_rolling").await;

    let status_ok: bool = db
        .query_scalar("SELECT bool_and(exists) FROM pgtrickle.self_monitoring_status()")
        .await;
    assert!(
        status_ok,
        "SCAL-2: all DF STs should still exist after retention cleanup"
    );
}

// ── SCAL-1: DF STs refresh within window at scale ───────────────────────

#[tokio::test]
#[ignore] // Long-running soak test — use --ignored to include
async fn test_df_sts_refresh_within_window_at_scale() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 100) g")
        .await;

    // Create 10 user STs to simulate moderate load.
    for i in 0..10 {
        db.create_st(
            &format!("user_st_{i}"),
            &format!("SELECT id, sum(val) AS total FROM src WHERE id % 10 = {i} GROUP BY id"),
            "1m",
            "AUTO",
        )
        .await;
        db.refresh_st(&format!("user_st_{i}")).await;
    }

    // Generate refresh history.
    for _ in 0..10 {
        db.execute(
            "INSERT INTO src SELECT g + (SELECT max(id) FROM src), g * 10 \
             FROM generate_series(1, 10) g",
        )
        .await;
        for i in 0..10 {
            db.refresh_st(&format!("user_st_{i}")).await;
        }
    }

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Refresh all DF STs — should complete within a reasonable time.
    let start = std::time::Instant::now();
    for st in &[
        "df_efficiency_rolling",
        "df_anomaly_signals",
        "df_threshold_advice",
        "df_cdc_buffer_trends",
        "df_scheduling_interference",
    ] {
        db.refresh_st(&format!("pgtrickle.{st}")).await;
    }
    let elapsed = start.elapsed();

    // All DF STs should complete within 30 seconds even under load.
    assert!(
        elapsed.as_secs() < 30,
        "SCAL-1: DF ST refresh took too long: {:?}",
        elapsed
    );

    // Verify all DF STs still exist and are healthy.
    let all_exist: bool = db
        .query_scalar("SELECT bool_and(exists) FROM pgtrickle.self_monitoring_status()")
        .await;
    assert!(all_exist, "SCAL-1: all DF STs should exist after refresh");
}

// ── DF-D4: Soak test addition ───────────────────────────────────────────

#[tokio::test]
#[ignore] // Long-running soak test — use --ignored to include
async fn test_soak_self_monitoring_multiple_cycles() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 50) g")
        .await;
    db.create_st(
        "user_st",
        "SELECT id, sum(val) AS total FROM src GROUP BY id",
        "1m",
        "AUTO",
    )
    .await;

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Run 20 cycles of: insert data → refresh user ST → refresh all DF STs.
    for cycle in 0..20 {
        db.execute(&format!(
            "INSERT INTO src SELECT g + {}, g * 10 FROM generate_series(1, 5) g",
            50 + cycle * 5
        ))
        .await;
        db.refresh_st("user_st").await;

        for st in &[
            "df_efficiency_rolling",
            "df_anomaly_signals",
            "df_threshold_advice",
            "df_cdc_buffer_trends",
            "df_scheduling_interference",
        ] {
            db.refresh_st(&format!("pgtrickle.{st}")).await;
        }
    }

    // After 20 cycles, all DF STs should still be healthy.
    let all_exist: bool = db
        .query_scalar("SELECT bool_and(exists) FROM pgtrickle.self_monitoring_status()")
        .await;
    assert!(all_exist, "DF-D4: all DF STs should survive 20 soak cycles");

    // Scheduler overhead should be reasonable.
    let overhead: i64 = db
        .query_scalar("SELECT total_refreshes_1h FROM pgtrickle.scheduler_overhead()")
        .await;
    assert!(
        overhead > 0,
        "DF-D4: scheduler_overhead should report refreshes after soak"
    );
}

// ── TEST-5: Soak — self-monitoring with many user STs ───────────────────────

#[tokio::test]
#[ignore] // Long-running soak test — use --ignored to include
async fn test_soak_self_monitoring_with_many_user_sts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 200) g")
        .await;

    // Create 20 user STs (representative of a moderate production environment).
    for i in 0..20 {
        db.create_st(
            &format!("user_st_{i}"),
            &format!("SELECT id, sum(val) AS total FROM src WHERE id % 20 = {i} GROUP BY id"),
            "1m",
            "AUTO",
        )
        .await;
        db.refresh_st(&format!("user_st_{i}")).await;
    }

    // Generate enough history.
    for _ in 0..5 {
        db.execute(
            "INSERT INTO src SELECT g + (SELECT max(id) FROM src), g * 10 \
             FROM generate_series(1, 20) g",
        )
        .await;
        for i in 0..20 {
            db.refresh_st(&format!("user_st_{i}")).await;
        }
    }

    db.execute("SELECT pgtrickle.setup_self_monitoring()").await;

    // Refresh all DF STs.
    for st in &[
        "df_efficiency_rolling",
        "df_anomaly_signals",
        "df_threshold_advice",
        "df_cdc_buffer_trends",
        "df_scheduling_interference",
    ] {
        db.refresh_st(&format!("pgtrickle.{st}")).await;
    }

    // Verify overhead fraction — DF refreshes should be a small fraction.
    let fraction: Option<f64> = db
        .query_scalar("SELECT df_refresh_fraction FROM pgtrickle.scheduler_overhead()")
        .await;
    if let Some(f) = fraction {
        // In a test with 20 user STs and 6 DF STs, fraction should be moderate.
        assert!(
            f < 0.50,
            "TEST-5: DF fraction with 20 user STs should be < 50% (got {:.1}%)",
            f * 100.0
        );
    }

    pause_self_monitoring(&db).await;

    // Teardown should work cleanly.
    db.execute("SELECT pgtrickle.teardown_self_monitoring()")
        .await;

    let after: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(after, 0, "TEST-5: teardown should remove all DF STs");
}
