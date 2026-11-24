//! v0.62.0: Full E2E tests for scheduler PERF-1 fanout cache and
//! API-1/2 per-node pause/resume control.
//!
//! These tests require the background scheduler (`shared_preload_libraries`)
//! and can only run in the full E2E harness (just test-e2e).

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helpers ────────────────────────────────────────────────────────────────

async fn configure_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(
        sched_running,
        "pg_trickle scheduler did not appear within 90 s"
    );
}

#[tokio::test]
async fn test_scheduler_retries_full_when_upstream_is_locked() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.enable_fused_refresh", "off", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.load_shed_threshold", "0", "0")
        .await;
    db.execute("CREATE TABLE lock_src (id INT PRIMARY KEY, amount INT NOT NULL)")
        .await;
    db.execute("INSERT INTO lock_src VALUES (1, 10)").await;
    db.create_st(
        "lock_up",
        "SELECT id, amount FROM lock_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st("lock_down", "SELECT id, amount FROM lock_up", "1h", "FULL")
        .await;
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['lock_up', 'lock_down'])")
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_phase",
        "'before_upstream_lock'",
        "before_upstream_lock",
    )
    .await;
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_for_table",
        "'lock_down'",
        "lock_down",
    )
    .await;

    let down_id: i32 = db
        .query_scalar(
            "SELECT pgt_id::int FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'lock_down'",
        )
        .await;
    let before: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    let mut barrier = db.pool.acquire().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *barrier)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_lock($1, 5)")
        .bind(down_id)
        .execute(&mut *barrier)
        .await
        .unwrap();
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = true WHERE pgt_name = 'lock_down'",
    )
    .await;
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['lock_down'])")
        .await;
    let blocked = format!(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name LIKE 'pgt_atomicity:{down_id}:before_upstream_lock:%' AND wait_event_type = 'Lock' AND {blocker_pid} = ANY(pg_blocking_pids(pid)))"
    );
    assert!(
        db.wait_for_condition(
            "scheduler upstream lock barrier",
            &blocked,
            Duration::from_secs(60),
            Duration::from_millis(50)
        )
        .await
    );
    let scheduler_pid: i32 = db.query_scalar(&format!("SELECT pid FROM pg_stat_activity WHERE application_name LIKE 'pgt_atomicity:{down_id}:before_upstream_lock:%' AND {blocker_pid} = ANY(pg_blocking_pids(pid)) LIMIT 1")).await;
    db.execute("INSERT INTO lock_src VALUES (2, 20)").await;
    let mut upstream = db.pool.begin().await.unwrap();
    sqlx::query(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'lock_up' FOR UPDATE",
    )
    .execute(&mut *upstream)
    .await
    .unwrap();
    sqlx::query("SELECT pg_advisory_unlock($1, 5)")
        .bind(down_id)
        .execute(&mut *barrier)
        .await
        .unwrap();
    let cleared = format!(
        "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = {scheduler_pid} AND application_name LIKE 'pgt_atomicity:{down_id}:before_upstream_lock:%')"
    );
    assert!(
        db.wait_for_condition(
            "scheduler skips locked upstream",
            &cleared,
            Duration::from_secs(30),
            Duration::from_millis(50)
        )
        .await
    );
    let completed: i64 = db.query_scalar(&format!("SELECT count(*) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'lock_down' AND h.refresh_id > {before} AND h.status = 'COMPLETED'")).await;
    assert_eq!(completed, 0);
    sqlx::query("SELECT pgtrickle.refresh_stream_table('lock_up')")
        .execute(&mut *upstream)
        .await
        .unwrap();
    upstream.commit().await.unwrap();
    db.alter_system_set_and_wait("pg_trickle.test_chaos_for_table", "''", "")
        .await;
    let completed = format!(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'lock_down' AND h.refresh_id > {before} AND h.initiated_by = 'SCHEDULER' AND h.action = 'REINITIALIZE' AND h.status = 'COMPLETED')"
    );
    assert!(
        db.wait_for_condition(
            "scheduled reinitialization after upstream commit",
            &completed,
            Duration::from_secs(60),
            Duration::from_millis(50)
        )
        .await
    );
    let rows: Vec<(i32, i32)> = sqlx::query_as("SELECT id, amount FROM lock_down ORDER BY id")
        .fetch_all(&db.pool)
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_phase",
        "'before_apply'",
        "before_apply",
    )
    .await;
}

async fn assert_clock_dependent_refresh(mode: &str, schedule_kind: &str) {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;
    db.execute("CREATE TABLE clock_src (id INT PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL)")
        .await;
    db.execute("INSERT INTO clock_src VALUES (1, clock_timestamp() + interval '5 seconds')")
        .await;
    let query = "SELECT id FROM clock_src WHERE expires_at > CURRENT_TIMESTAMP";
    let schedule = if schedule_kind == "cron" {
        "0 * * * *"
    } else {
        "1h"
    };
    db.create_st("clock_st", query, schedule, mode).await;
    db.create_st("clock_control", "SELECT id FROM clock_src", "1h", "FULL")
        .await;
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['clock_st', 'clock_control'])")
        .await;
    if schedule_kind == "interval" {
        db.alter_st("clock_st", "target_freshness => '1 hour'")
            .await;
    }
    assert_eq!(db.count("clock_st").await, 1);
    assert_eq!(db.pgt_status("clock_st").await.1, "FULL");
    db.execute("DELETE FROM pgtrickle.pgt_refresh_history")
        .await;
    assert!(
        db.wait_for_condition(
            "source row expired without writes",
            "SELECT clock_timestamp() >= expires_at FROM clock_src",
            Duration::from_secs(15),
            Duration::from_millis(50),
        )
        .await
    );
    // Make every schedule observably due after expiry, without source writes.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at = now() - interval '2 hours'",
    )
    .await;
    if schedule_kind == "interval" {
        db.execute("UPDATE pgtrickle.pgt_freshness_controller_state SET next_due_at = now() - interval '1 second'")
            .await;
    }
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['clock_st', 'clock_control'])")
        .await;
    assert!(db.wait_for_condition(
        "scheduler due-time witness",
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'clock_control' AND h.status = 'COMPLETED')",
        Duration::from_secs(90),
        Duration::from_millis(50),
    ).await);
    let refreshed = db
        .wait_for_condition(
            "clock-dependent scheduled expiry",
            "SELECT NOT EXISTS(SELECT 1 FROM clock_st)",
            Duration::from_secs(10),
            Duration::from_millis(50),
        )
        .await;
    if refreshed && schedule_kind == "interval" {
        // Witness another tick while the controller's original due time is past.
        db.execute("UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at = now() - interval '2 hours' WHERE pgt_name = 'clock_control'")
            .await;
        assert!(db.wait_for_condition(
            "second scheduler tick before next clock interval",
            "SELECT count(*) >= 2 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'clock_control' AND h.status = 'COMPLETED'",
            Duration::from_secs(90),
            Duration::from_millis(50),
        ).await);
        let refreshes: i64 = db.query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'clock_st' AND h.status = 'COMPLETED'").await;
        assert_eq!(
            refreshes, 1,
            "clock reevaluation must wait for its next interval"
        );
    }
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['clock_st', 'clock_control'])")
        .await;
    let witness: String = db.query_scalar(
        "SELECT jsonb_build_object('now', clock_timestamp(), 'source', (SELECT jsonb_agg(s) FROM clock_src s), 'materialized', (SELECT count(*) FROM clock_st), 'expected', (SELECT count(*) FROM clock_src WHERE expires_at > CURRENT_TIMESTAMP), 'metadata', (SELECT jsonb_agg(jsonb_build_object('name', pgt_name, 'mode', refresh_mode, 'schedule', schedule, 'target', target_freshness_mode, 'last_refresh_at', last_refresh_at)) FROM pgtrickle.pgt_stream_tables), 'history', (SELECT jsonb_agg(jsonb_build_object('action', action, 'status', status)) FROM pgtrickle.pgt_refresh_history))::text"
    ).await;
    eprintln!("clock refresh witness ({mode}, {schedule_kind}): {witness}");
    assert_eq!(db.count("clock_src").await, 1);
    let control_action: String = db.query_scalar("SELECT h.action FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'clock_control' AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert_eq!(
        control_action, "NO_DATA",
        "source-only FULL must stay cheap"
    );
    assert!(refreshed, "scheduled output stayed stale: {witness}");
    db.assert_st_matches_query("clock_st", query).await;
    let action: String = db.query_scalar("SELECT h.action FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'clock_st' AND h.status = 'COMPLETED' ORDER BY h.refresh_id LIMIT 1").await;
    assert_eq!(action, "FULL", "{witness}");
}

#[tokio::test]
async fn test_scheduler_clock_auto_duration_expires_without_writes() {
    assert_clock_dependent_refresh("AUTO", "duration").await;
}

#[tokio::test]
async fn test_scheduler_clock_full_duration_expires_without_writes() {
    assert_clock_dependent_refresh("FULL", "duration").await;
}

#[tokio::test]
async fn test_scheduler_clock_auto_cron_expires_without_writes() {
    assert_clock_dependent_refresh("AUTO", "cron").await;
}

#[tokio::test]
async fn test_scheduler_clock_auto_interval_expires_without_writes() {
    assert_clock_dependent_refresh("AUTO", "interval").await;
}

// Keep schedules long and explicitly make each pass due so observations cannot
// race another refresh. Backdating the leaf also forces a real DIFFERENTIAL
// pass via the upstream-refresh timestamp, even when no deltas remain.
async fn assert_scheduler_baseline_does_not_replay(baseline: &str, change_upstream: bool) {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    // Keep quiet upstream work in the same tick as load-shed-exempt reinitialization.
    db.alter_system_set_and_wait("pg_trickle.load_shed_threshold", "0", "0")
        .await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.refresh_strategy",
        "differential",
        "differential",
    )
    .await;

    db.execute("CREATE TABLE replay_src (id INT PRIMARY KEY, grp INT NOT NULL)")
        .await;
    db.execute("INSERT INTO replay_src VALUES (1, 1), (2, 1)")
        .await;
    db.create_st("replay_up", "SELECT id, grp FROM replay_src", "1h", "FULL")
        .await;
    let query = "SELECT grp, COUNT(*) AS n FROM replay_up GROUP BY grp";
    for leaf in ["replay_leaf", "replay_sibling"] {
        db.create_st(leaf, query, "1h", "DIFFERENTIAL").await;
    }
    db.execute(
        "SELECT pgtrickle.pause_scheduler(ARRAY['replay_up', 'replay_leaf', 'replay_sibling'])",
    )
    .await;
    db.execute("DELETE FROM pgtrickle.pgt_refresh_history")
        .await;
    if baseline == "FALLBACK" {
        // Put one eligible delta below the next tick bound so differential
        // execution reaches the configured FULL fallback instead of no-oping.
        db.execute("INSERT INTO replay_src VALUES (4, 1)").await;
        db.refresh_st("replay_up").await;
    }
    if change_upstream {
        db.execute("INSERT INTO replay_src VALUES (3, 1)").await;
    }
    match baseline {
        "FULL" => {
            db.alter_system_set_and_wait("pg_trickle.force_full_refresh", "on", "on")
                .await;
        }
        "FALLBACK" => {
            db.alter_system_set_and_wait("pg_trickle.refresh_strategy", "full", "full")
                .await;
        }
        "REINITIALIZE" => {
            db.execute("UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = true WHERE pgt_name = 'replay_leaf'")
                .await;
        }
        "FRONTIER_MISSING" => {
            db.execute("UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_name = 'replay_leaf'")
                .await;
        }
        _ => panic!("unknown baseline {baseline}"),
    }
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at = now() - interval '2 hours' WHERE pgt_name IN ('replay_up', 'replay_leaf')")
        .await;
    // A missing-frontier leaf can run its FULL baseline before the upstream
    // refresh has published any CDC rows. If both are resumed together, the
    // test may see only the buffer's 0/0 sentinel instead of the change it
    // intends to verify. Materialize the upstream first in this case; the
    // other baselines still cover same-tick upstream/leaf execution.
    if baseline == "FRONTIER_MISSING" && change_upstream {
        db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['replay_up'])")
            .await;
        let upstream_id: i64 = db
            .query_scalar(
                "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'replay_up'",
            )
            .await;
        let captured = format!(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle_changes.changes_pgt_{upstream_id}
                WHERE action = 'I' AND lsn > '0/0'::pg_lsn
            )"
        );
        assert!(
            db.wait_for_condition(
                "upstream CDC before missing-frontier baseline",
                &captured,
                Duration::from_secs(60),
                Duration::from_millis(50),
            )
            .await,
            "upstream FULL must publish its change before the leaf baseline"
        );
        db.assert_st_matches_query("replay_up", "SELECT id, grp FROM replay_src")
            .await;
        db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['replay_leaf'])")
            .await;
    } else {
        db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['replay_up', 'replay_leaf'])")
            .await;
    }
    assert!(
        db.wait_for_condition(
            "scheduled baseline",
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'replay_leaf' AND h.status = 'COMPLETED')",
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .await
    );
    db.assert_st_matches_query("replay_leaf", query).await;
    let upstream_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'replay_up'")
        .await;
    let buffer = format!("pgtrickle_changes.changes_pgt_{upstream_id}");
    let diagnostics: String = db
        .query_scalar(&format!(
            "SELECT jsonb_build_object(
                    'baseline', '{baseline}', 'action', h.action,
                    'strategy', h.merge_strategy_used, 'fallback', h.was_full_fallback,
                    'tick_bound', h.tick_watermark_lsn, 'frontier', st.frontier,
                    'buffer_max', (SELECT MAX(lsn)::text FROM {buffer}),
                    'same_transaction', h.visibility_xid = (
                        SELECT visibility_xid FROM pgtrickle.pgt_refresh_history
                        WHERE pgt_id = {upstream_id} AND status = 'COMPLETED'
                        ORDER BY refresh_id DESC LIMIT 1))::text
           FROM pgtrickle.pgt_refresh_history h
           JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
          WHERE st.pgt_name = 'replay_leaf' AND h.status = 'COMPLETED'
          ORDER BY h.refresh_id DESC LIMIT 1"
        ))
        .await;
    eprintln!("scheduler baseline witness: {diagnostics}");
    let witness: serde_json::Value = serde_json::from_str(&diagnostics).expect("baseline witness");
    assert_eq!(
        witness["action"],
        if baseline == "REINITIALIZE" {
            "REINITIALIZE"
        } else {
            "FULL"
        }
    );
    assert_eq!(witness["strategy"], "FULL", "{diagnostics}");
    // visibility_xid is optional freshness telemetry. A baseline can be
    // correct without either side having a recorded visibility XID; in that
    // case SQL equality evaluates to NULL, not false. Assert transaction
    // equality when both IDs are available, and keep the frontier/buffer
    // and exact-result checks below as the mandatory no-replay evidence.
    if change_upstream && baseline != "FRONTIER_MISSING" && !witness["same_transaction"].is_null() {
        assert_eq!(witness["same_transaction"], true, "{diagnostics}");
    }
    if baseline == "FALLBACK" {
        assert_eq!(witness["fallback"], true, "{diagnostics}");
    }
    if change_upstream {
        // The missing-frontier case starts the leaf on a later tick, after the
        // upstream capture was witnessed. Other baselines must still prove
        // that their same-tick capture exceeds the leaf's original tick bound.
        if baseline != "FRONTIER_MISSING" {
            assert!(db.query_scalar::<bool>(&format!(
                "SELECT MAX(lsn) > (SELECT tick_watermark_lsn FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'replay_leaf' AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) FROM {buffer}"
            )).await, "upstream capture must exceed the tick bound: {diagnostics}");
        }
        assert_eq!(
            db.query_scalar::<i64>(&format!("SELECT COUNT(*) FROM {buffer} WHERE action = 'I'"))
                .await,
            if baseline == "FALLBACK" { 2 } else { 1 },
            "FULL must retain the delta for the paused sibling"
        );
    }

    db.alter_system_set_and_wait("pg_trickle.force_full_refresh", "off", "off")
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.refresh_strategy",
        "differential",
        "differential",
    )
    .await;
    // No further source writes: consume only the window following the baseline.
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at = now() - interval '2 hours' WHERE pgt_name = 'replay_leaf'")
        .await;
    assert!(db.wait_for_condition(
        "differential after baseline",
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'replay_leaf' AND h.status = 'COMPLETED' AND h.action = 'DIFFERENTIAL' AND h.merge_strategy_used <> 'FULL' AND NOT h.was_full_fallback)",
        Duration::from_secs(60),
        Duration::from_millis(50),
    ).await, "a real differential pass must follow the baseline: {diagnostics}");
    db.assert_st_matches_query("replay_leaf", query).await;
    assert_eq!(
        db.query_scalar::<i64>("SELECT __pgt_count FROM replay_leaf")
            .await,
        if baseline == "FALLBACK" {
            4
        } else if change_upstream {
            3
        } else {
            2
        }
    );
    assert!(db.query_scalar::<bool>(&format!(
        "SELECT (frontier #>> '{{sources,pgt_{upstream_id},lsn}}')::pg_lsn >= (SELECT COALESCE(MAX(lsn), '0/0'::pg_lsn) FROM {buffer}) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'replay_leaf'"
    )).await, "the baseline frontier must cover all materialized upstream changes");

    db.execute("UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at = now() - interval '2 hours' WHERE pgt_name = 'replay_sibling'")
        .await;
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['replay_sibling'])")
        .await;
    assert!(db.wait_for_condition(
        "sibling catch-up",
        "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'replay_sibling' AND h.status = 'COMPLETED' AND h.action = 'DIFFERENTIAL' AND NOT h.was_full_fallback)",
        Duration::from_secs(120),
        Duration::from_millis(50),
    ).await);
    db.assert_st_matches_query("replay_sibling", query).await;
}

/// #1417: a scheduled REINITIALIZE must not reuse the pre-lock tick LSN.
/// A pending source writer blocks the scheduler's SHARE lock, forcing its FULL
/// snapshot to include a commit that happened *after* the scheduler tick.  The
/// next DIFFERENTIAL must not count that writer's rows a second time.
#[tokio::test]
async fn test_scheduler_reinitialize_after_blocked_writer_does_not_replay() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.load_shed_threshold", "0", "0")
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.refresh_strategy",
        "differential",
        "differential",
    )
    .await;

    db.execute(
        "CREATE TABLE scheduler_cut_src (
            id BIGINT NOT NULL,
            day DATE NOT NULL,
            grp INT NOT NULL,
            qty BIGINT NOT NULL,
            PRIMARY KEY (id, day)
        ) PARTITION BY RANGE (day)",
    )
    .await;
    db.execute(
        "CREATE TABLE scheduler_cut_active PARTITION OF scheduler_cut_src
         FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
    )
    .await;
    db.execute(
        "CREATE TABLE scheduler_cut_empty PARTITION OF scheduler_cut_src
         FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
    )
    .await;
    db.execute(
        "INSERT INTO scheduler_cut_src (id, day, grp, qty)
         SELECT i, '2026-11-15', i % 3, 10 FROM generate_series(1, 9) AS i",
    )
    .await;
    let query = "SELECT grp, SUM(qty) AS total, COUNT(*) AS n FROM scheduler_cut_src GROUP BY grp";
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            name => 'scheduler_cut_st',
            query => 'SELECT grp, SUM(qty) AS total, COUNT(*) AS n FROM scheduler_cut_src GROUP BY grp',
            schedule => '1s',
            refresh_mode => 'DIFFERENTIAL',
            cdc_mode => 'trigger'
         )",
    )
    .await;
    db.assert_st_matches_query("scheduler_cut_st", query).await;

    // Keep scheduler work paused while a normal DETACH + DROP requests a
    // protected REINITIALIZE. Partition removal now leaves managed streams
    // ACTIVE with SCHEMA_CHANGED (rather than suspending them); the scheduler
    // must perform the rebuild after it resumes, not a manual refresh.
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['scheduler_cut_st'])")
        .await;
    db.execute("ALTER TABLE scheduler_cut_src DETACH PARTITION scheduler_cut_empty")
        .await;
    db.execute("DROP TABLE scheduler_cut_empty").await;
    let state: String = db
        .query_scalar(
            "SELECT status || ':' || refresh_reason FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'scheduler_cut_st'",
        )
        .await;
    assert_eq!(state, "ACTIVE:SCHEMA_CHANGED");
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'scheduler_cut_st'",
        )
        .await;
    assert!(
        needs_reinit,
        "normal partition removal must request REINITIALIZE"
    );

    let source_oid: i64 = db
        .query_scalar("SELECT 'scheduler_cut_src'::regclass::oid::bigint")
        .await;
    let source_buffer: String = db
        .query_scalar(&format!(
            "SELECT format('%I.%I', 'pgtrickle_changes',
                           'changes_' || COALESCE(source_stable_name, {source_oid}::text))
             FROM pgtrickle.pgt_change_tracking
             WHERE source_relid = {source_oid}::oid"
        ))
        .await;
    let mut writer = db.pool.begin().await.expect("open source writer");
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .expect("get source writer PID");
    // Acquire the conflicting source lock without writing yet.  The scheduler
    // must capture its tick and start waiting before the CDC LSN is minted.
    sqlx::query("LOCK TABLE scheduler_cut_src IN ROW EXCLUSIVE MODE")
        .execute(&mut *writer)
        .await
        .expect("hold source lock before scheduler tick");

    // Resume only after the writer has its source RowExclusiveLock.  The
    // scheduled REINITIALIZE must wait on this transaction's source lock.
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['scheduler_cut_st'])")
        .await;
    let blocked = format!(
        "SELECT EXISTS (
            SELECT 1 FROM pg_locks l
            WHERE l.relation = 'scheduler_cut_src'::regclass
              AND l.mode = 'ShareLock' AND NOT l.granted
              AND {writer_pid} = ANY(pg_blocking_pids(l.pid))
        )"
    );
    assert!(
        db.wait_for_condition(
            "scheduled REINITIALIZE blocked by the source writer",
            &blocked,
            Duration::from_secs(45),
            Duration::from_millis(50),
        )
        .await,
        "the scheduler must capture its tick before the writer commits"
    );
    // This write occurs strictly *after* the scheduler recorded its tick.
    sqlx::query(
        "INSERT INTO scheduler_cut_src (id, day, grp, qty)
         SELECT i, '2026-11-15', i % 3, 100
         FROM generate_series(10, 109) AS i",
    )
    .execute(&mut *writer)
    .await
    .expect("insert rows after the scheduler was blocked");
    // The relation is catalog-derived and SQL-identifier quoted by PostgreSQL
    // format('%I.%I'); no user string is interpolated into this query.
    let delta_sql = format!("SELECT MAX(lsn)::text FROM {source_buffer}");
    let writer_delta_lsn: String = sqlx::query_scalar(sqlx::AssertSqlSafe(delta_sql))
        .fetch_one(&mut *writer)
        .await
        .expect("read uncommitted source CDC LSN");
    writer
        .commit()
        .await
        .expect("commit the blocked source writer");

    assert!(
        db.wait_for_condition(
            "scheduled REINITIALIZE completed after the writer committed",
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                WHERE st.pgt_name = 'scheduler_cut_st'
                  AND h.action = 'REINITIALIZE' AND h.status = 'COMPLETED'
            )",
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .await,
        "the scheduler must complete a real REINITIALIZE"
    );
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['scheduler_cut_st'])")
        .await;
    db.assert_st_matches_query("scheduler_cut_st", query).await;

    // These two witnesses distinguish a repaired frontier from merely having
    // a correct FULL output: the tick was older than the writer's CDC row, but
    // the new per-source frontier must cover that row.
    let tick_is_older: bool = sqlx::query_scalar(
        "SELECT h.tick_watermark_lsn < $1::pg_lsn
         FROM pgtrickle.pgt_refresh_history h
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
         WHERE st.pgt_name = 'scheduler_cut_st'
           AND h.action = 'REINITIALIZE' AND h.status = 'COMPLETED'
         ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .bind(&writer_delta_lsn)
    .fetch_one(&db.pool)
    .await
    .expect("read scheduler tick watermark");
    assert!(
        tick_is_older,
        "the test must exercise the stale-tick interleaving"
    );
    let frontier_covers_writer: bool = sqlx::query_scalar(
        "SELECT (frontier #>> ARRAY['sources', $1::text, 'lsn'])::pg_lsn >= $2::pg_lsn
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'scheduler_cut_st'",
    )
    .bind(source_oid.to_string())
    .bind(&writer_delta_lsn)
    .fetch_one(&db.pool)
    .await
    .expect("read protected source frontier");
    assert!(
        frontier_covers_writer,
        "the baseline frontier must cover every change included in the FULL snapshot"
    );

    // Force a genuine differential refresh with one new write; an old frontier
    // would replay the previous 100 inserts as well as this one.
    db.execute(
        "INSERT INTO scheduler_cut_src (id, day, grp, qty)
         VALUES (110, '2026-11-15', 1, 7)",
    )
    .await;
    let before_diff: i64 = db
        .query_scalar("SELECT COALESCE(MAX(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.refresh_st("scheduler_cut_st").await;
    let was_differential: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                WHERE st.pgt_name = 'scheduler_cut_st'
                  AND h.refresh_id > {before_diff}
                  AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED'
                  AND NOT h.was_full_fallback
            )"
        ))
        .await;
    assert!(
        was_differential,
        "the post-rebuild correctness check must actually apply a differential delta"
    );
    db.assert_st_matches_query("scheduler_cut_st", query).await;
}

/// #1455: pause/resume and scheduler REINITIALIZE with a multi-statement
/// writer against a day-partitioned source must preserve SUM, COUNT and MAX.
/// The first writer commits *after* the scheduler's tick while its SHARE lock
/// is blocked.  A second two-statement transaction must then be applied by
/// an actual DIFFERENTIAL refresh, not a skipped call or another FULL.
#[tokio::test]
async fn test_scheduler_reinitialize_partitioned_multistatement_writer_exact() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.load_shed_threshold", "0", "0")
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.refresh_strategy",
        "differential",
        "differential",
    )
    .await;

    db.execute(
        "CREATE TABLE issue1455_src (
            id BIGINT NOT NULL,
            day DATE NOT NULL,
            meter INT NOT NULL,
            scope_path TEXT NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            value BIGINT NOT NULL,
            PRIMARY KEY (id, day)
        ) PARTITION BY RANGE (day)",
    )
    .await;
    db.execute(
        "CREATE TABLE issue1455_day PARTITION OF issue1455_src
         FOR VALUES FROM ('2026-11-20') TO ('2026-11-21')",
    )
    .await;
    db.execute(
        "CREATE TABLE issue1455_empty PARTITION OF issue1455_src
         FOR VALUES FROM ('2026-11-19') TO ('2026-11-20')",
    )
    .await;
    db.execute(
        "INSERT INTO issue1455_src (id, day, meter, scope_path, occurred_at, value)
         SELECT i, '2026-11-20', i % 2, 'tenant',
                TIMESTAMPTZ '2026-11-20 00:00:00+00' + (i % 12) * INTERVAL '1 hour',
                (i % 7 + 1) * 10
         FROM generate_series(1, 24) AS i",
    )
    .await;
    let query = "SELECT meter, scope_path,
                date_trunc('hour', occurred_at AT TIME ZONE 'UTC') AS hour,
                sum(value) AS quantity, count(*) AS events, max(value) AS max_value
         FROM issue1455_src
         GROUP BY meter, scope_path,
                  date_trunc('hour', occurred_at AT TIME ZONE 'UTC')";
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            name => 'issue1455_hourly',
            query => $q$SELECT meter, scope_path,
                               date_trunc('hour', occurred_at AT TIME ZONE 'UTC') AS hour,
                               sum(value) AS quantity, count(*) AS events,
                               max(value) AS max_value
                        FROM issue1455_src
                        GROUP BY meter, scope_path,
                                 date_trunc('hour', occurred_at AT TIME ZONE 'UTC')$q$,
            schedule => '1s',
            refresh_mode => 'DIFFERENTIAL',
            cdc_mode => 'trigger'
        )",
    )
    .await;
    db.assert_st_matches_query("issue1455_hourly", query).await;

    // Pause/resume can preserve CDC and legitimately avoid a rebuild. Detach an
    // empty source partition while the scheduler is paused to explicitly require
    // a protected REINITIALIZE without changing the live source data. This also
    // exercises a protected FULL baseline while a multi-statement source writer
    // holds the lock.
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['issue1455_hourly'])")
        .await;
    db.execute("SELECT pgtrickle.pause_stream_table('issue1455_hourly')")
        .await;
    db.execute("SELECT pgtrickle.resume_stream_table('issue1455_hourly')")
        .await;
    db.execute("ALTER TABLE issue1455_src DETACH PARTITION issue1455_empty")
        .await;
    db.execute("DROP TABLE issue1455_empty").await;
    let needs_reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'issue1455_hourly'",
        )
        .await;
    assert!(
        needs_reinit,
        "detaching a partition must require a protected REINITIALIZE"
    );

    let source_oid: i64 = db
        .query_scalar("SELECT 'issue1455_src'::regclass::oid::bigint")
        .await;
    let source_buffer: String = db
        .query_scalar(&format!(
            "SELECT format('%I.%I', 'pgtrickle_changes',
                           'changes_' || COALESCE(source_stable_name, {source_oid}::text))
             FROM pgtrickle.pgt_change_tracking
             WHERE source_relid = {source_oid}::oid"
        ))
        .await;
    let mut writer = db.pool.begin().await.expect("open source writer");
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .expect("read writer PID");
    sqlx::query("LOCK TABLE issue1455_src IN ROW EXCLUSIVE MODE")
        .execute(&mut *writer)
        .await
        .expect("hold source lock before the tick");

    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['issue1455_hourly'])")
        .await;
    let blocked = format!(
        "SELECT EXISTS (
            SELECT 1 FROM pg_locks l
            WHERE l.relation = 'issue1455_src'::regclass
              AND l.mode = 'ShareLock' AND NOT l.granted
              AND {writer_pid} = ANY(pg_blocking_pids(l.pid))
        )"
    );
    assert!(
        db.wait_for_condition(
            "scheduled #1455 REINITIALIZE blocked behind source writer",
            &blocked,
            Duration::from_secs(45),
            Duration::from_millis(50),
        )
        .await,
        "a real scheduler SHARE-lock wait is required"
    );

    // Two statements, one transaction, overlapping unique keys.  All 12
    // hourly groups must be present in the baseline despite the old tick.
    sqlx::query(
        "INSERT INTO issue1455_src (id, day, meter, scope_path, occurred_at, value)
         SELECT i, '2026-11-20', i % 2, 'tenant',
                TIMESTAMPTZ '2026-11-20 00:00:00+00' + (i % 12) * INTERVAL '1 hour',
                (i % 7 + 1) * 10
         FROM generate_series(1000, 1119) AS i ON CONFLICT DO NOTHING",
    )
    .execute(&mut *writer)
    .await
    .expect("first insert statement");
    sqlx::query(
        "INSERT INTO issue1455_src (id, day, meter, scope_path, occurred_at, value)
         SELECT i, '2026-11-20', i % 2, 'tenant',
                TIMESTAMPTZ '2026-11-20 00:00:00+00' + (i % 12) * INTERVAL '1 hour',
                (i % 7 + 1) * 10
         FROM generate_series(1100, 1259) AS i ON CONFLICT DO NOTHING",
    )
    .execute(&mut *writer)
    .await
    .expect("second insert statement");
    // The buffer relation name is derived and identifier-quoted by PostgreSQL.
    let writer_delta_lsn: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT MAX(lsn)::text FROM {source_buffer}"
    )))
    .fetch_one(&mut *writer)
    .await
    .expect("read captured multi-statement LSN");
    writer.commit().await.expect("commit source transaction");

    assert!(
        db.wait_for_condition(
            "scheduled #1455 REINITIALIZE completed",
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                WHERE st.pgt_name = 'issue1455_hourly'
                  AND h.action = 'REINITIALIZE' AND h.status = 'COMPLETED'
            )",
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .await,
        "the scheduler must perform the REINITIALIZE"
    );
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['issue1455_hourly'])")
        .await;
    db.assert_st_matches_query("issue1455_hourly", query).await;
    assert_eq!(
        db.query_scalar::<i64>("SELECT COUNT(*) FROM issue1455_hourly")
            .await,
        12,
        "all 12 hourly groups must survive the rebuild"
    );

    let tick_is_older: bool = sqlx::query_scalar(
        "SELECT h.tick_watermark_lsn < $1::pg_lsn
         FROM pgtrickle.pgt_refresh_history h
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
         WHERE st.pgt_name = 'issue1455_hourly'
           AND h.action = 'REINITIALIZE' AND h.status = 'COMPLETED'
         ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .bind(&writer_delta_lsn)
    .fetch_one(&db.pool)
    .await
    .expect("read scheduler watermark");
    assert!(tick_is_older, "the test must witness the post-tick writer");
    let frontier_covers_writer: bool = sqlx::query_scalar(
        "SELECT (frontier #>> ARRAY['sources', $1::text, 'lsn'])::pg_lsn >= $2::pg_lsn
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'issue1455_hourly'",
    )
    .bind(source_oid.to_string())
    .bind(&writer_delta_lsn)
    .fetch_one(&db.pool)
    .await
    .expect("read source frontier");
    assert!(
        frontier_covers_writer,
        "the FULL baseline must cover the committed multi-statement CDC images"
    );

    // Another multi-statement transaction exercises real DIFFERENTIAL catch-up
    // after the reinitialize.  A skipped manual API call must not pass.
    let mut next_writer = db.pool.begin().await.expect("open catch-up writer");
    for (start, end) in [(2000, 2059), (2040, 2099)] {
        sqlx::query(
            "INSERT INTO issue1455_src (id, day, meter, scope_path, occurred_at, value)
             SELECT i, '2026-11-20', i % 2, 'tenant',
                    TIMESTAMPTZ '2026-11-20 00:00:00+00' + (i % 12) * INTERVAL '1 hour',
                    (i % 7 + 1) * 10
             FROM generate_series($1::int, $2::int) AS i ON CONFLICT DO NOTHING",
        )
        .bind(start)
        .bind(end)
        .execute(&mut *next_writer)
        .await
        .expect("catch-up statement");
    }
    next_writer.commit().await.expect("commit catch-up writer");
    let before_diff: i64 = db
        .query_scalar("SELECT COALESCE(MAX(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.refresh_st("issue1455_hourly").await;
    let real_differential: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                WHERE st.pgt_name = 'issue1455_hourly'
                  AND h.refresh_id > {before_diff}
                  AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED'
                  AND NOT h.was_full_fallback
            )"
        ))
        .await;
    assert!(
        real_differential,
        "the post-reinitialize update must be a completed DIFFERENTIAL refresh"
    );
    db.assert_st_matches_query("issue1455_hourly", query).await;
}

/// #1417: a DIFFERENTIAL-to-FULL fallback also needs to replace its original
/// tick-bounded base-table frontier after the FULL acquires source locks.
#[tokio::test]
async fn test_scheduler_full_fallback_after_blocked_writer_does_not_replay() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;
    db.alter_system_set_and_wait("pg_trickle.load_shed_threshold", "0", "0")
        .await;

    db.execute("CREATE TABLE scheduler_fallback_src (id INT PRIMARY KEY, grp INT, qty BIGINT)")
        .await;
    db.execute(
        "INSERT INTO scheduler_fallback_src
         SELECT i, i % 2, 10 FROM generate_series(1, 9) AS i",
    )
    .await;
    let query =
        "SELECT grp, SUM(qty) AS total, COUNT(*) AS n FROM scheduler_fallback_src GROUP BY grp";
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            name => 'scheduler_fallback_st',
            query => 'SELECT grp, SUM(qty) AS total, COUNT(*) AS n FROM scheduler_fallback_src GROUP BY grp',
            schedule => '1s',
            refresh_mode => 'DIFFERENTIAL',
            cdc_mode => 'trigger'
        )",
    )
    .await;
    db.assert_st_matches_query("scheduler_fallback_st", query)
        .await;
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['scheduler_fallback_st'])")
        .await;

    // One committed change guarantees that a normal differential run is due.
    // Force that run to choose FULL, then block its source lock with another
    // writer whose changes the old scheduler tick cannot safely cover.
    db.execute("INSERT INTO scheduler_fallback_src VALUES (10, 0, 10)")
        .await;
    db.alter_system_set_and_wait("pg_trickle.refresh_strategy", "full", "full")
        .await;
    let source_oid: i64 = db
        .query_scalar("SELECT 'scheduler_fallback_src'::regclass::oid::bigint")
        .await;
    let source_buffer: String = db
        .query_scalar(&format!(
            "SELECT format('%I.%I', 'pgtrickle_changes',
                           'changes_' || COALESCE(source_stable_name, {source_oid}::text))
             FROM pgtrickle.pgt_change_tracking
             WHERE source_relid = {source_oid}::oid"
        ))
        .await;
    // A persisted INTERVAL freshness target is eligible only after its
    // controller's next_due_at, even when the source has pending CDC rows.
    // Make both the controller and legacy timestamp due while the scheduler
    // is paused, then establish the conflicting writer lock. Without this
    // step the test can time out before attempting the protected FULL at all.
    let source_has_pending_cdc: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM {source_buffer} WHERE action <> 'S')"
        ))
        .await;
    assert!(
        source_has_pending_cdc,
        "the committed source update must be present in its trigger buffer"
    );
    let mut due = db
        .pool
        .begin()
        .await
        .expect("begin fallback scheduler due setup");
    sqlx::query(
        "UPDATE pgtrickle.pgt_stream_tables
         SET last_refresh_at = clock_timestamp() - interval '2 hours'
         WHERE pgt_name = 'scheduler_fallback_st'",
    )
    .execute(&mut *due)
    .await
    .expect("make fallback stream overdue");
    sqlx::query(
        "UPDATE pgtrickle.pgt_freshness_controller_state
         SET next_due_at = clock_timestamp() - interval '1 second'
         WHERE pgt_id = (
             SELECT pgt_id FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'scheduler_fallback_st'
         )",
    )
    .execute(&mut *due)
    .await
    .expect("make fallback freshness controller due");
    due.commit().await.expect("commit fallback due setup");

    let mut writer = db.pool.begin().await.expect("open fallback source writer");
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .expect("get fallback writer PID");
    sqlx::query("LOCK TABLE scheduler_fallback_src IN ROW EXCLUSIVE MODE")
        .execute(&mut *writer)
        .await
        .expect("hold fallback source lock before scheduler tick");

    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['scheduler_fallback_st'])")
        .await;
    let blocked = format!(
        "SELECT EXISTS (
            SELECT 1 FROM pg_locks l
            WHERE l.relation = 'scheduler_fallback_src'::regclass
              AND l.mode = 'ShareLock' AND NOT l.granted
              AND {writer_pid} = ANY(pg_blocking_pids(l.pid))
        )"
    );
    assert!(
        db.wait_for_condition(
            "fallback FULL waiting for the writer's source lock",
            &blocked,
            Duration::from_secs(45),
            Duration::from_millis(50),
        )
        .await,
        "the fallback must reach the FULL source lock under an old tick"
    );
    // Mint the CDC LSN only after the scheduler has captured its old tick.
    sqlx::query(
        "INSERT INTO scheduler_fallback_src
         SELECT i, i % 2, 100 FROM generate_series(11, 110) AS i",
    )
    .execute(&mut *writer)
    .await
    .expect("insert fallback source rows");
    // The relation is catalog-derived and SQL-identifier quoted by PostgreSQL
    // format('%I.%I'); no user string is interpolated into this query.
    let delta_sql = format!("SELECT MAX(lsn)::text FROM {source_buffer}");
    let writer_delta_lsn: String = sqlx::query_scalar(sqlx::AssertSqlSafe(delta_sql))
        .fetch_one(&mut *writer)
        .await
        .expect("read fallback writer's CDC LSN");
    writer
        .commit()
        .await
        .expect("commit the blocked fallback writer");

    assert!(
        db.wait_for_condition(
            "scheduled differential FULL fallback completed",
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                WHERE st.pgt_name = 'scheduler_fallback_st'
                  AND h.action = 'FULL' AND h.was_full_fallback
                  AND h.status = 'COMPLETED'
            )",
            Duration::from_secs(60),
            Duration::from_millis(50),
        )
        .await,
        "scheduler must complete the intended FULL fallback"
    );
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['scheduler_fallback_st'])")
        .await;
    db.assert_st_matches_query("scheduler_fallback_st", query)
        .await;

    let frontier_covers_writer: bool = sqlx::query_scalar(
        "SELECT (frontier #>> ARRAY['sources', $1::text, 'lsn'])::pg_lsn >= $2::pg_lsn
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'scheduler_fallback_st'",
    )
    .bind(source_oid.to_string())
    .bind(&writer_delta_lsn)
    .fetch_one(&db.pool)
    .await
    .expect("read fallback source frontier");
    assert!(
        frontier_covers_writer,
        "FULL fallback must replace the tick-bounded base-source frontier"
    );

    db.alter_system_set_and_wait(
        "pg_trickle.refresh_strategy",
        "differential",
        "differential",
    )
    .await;
    db.execute("INSERT INTO scheduler_fallback_src VALUES (111, 1, 7)")
        .await;
    let before_diff: i64 = db
        .query_scalar("SELECT COALESCE(MAX(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.refresh_st("scheduler_fallback_st").await;
    let was_differential: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                WHERE st.pgt_name = 'scheduler_fallback_st'
                  AND h.refresh_id > {before_diff}
                  AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED'
                  AND NOT h.was_full_fallback
            )"
        ))
        .await;
    assert!(
        was_differential,
        "post-fallback refresh must be differential"
    );
    db.assert_st_matches_query("scheduler_fallback_st", query)
        .await;
}

/// #1417: manual DIFFERENTIAL-to-FULL fallback must not retain a bound taken
/// before its source lock.  The input-boundary advisory barrier lets new source
/// rows commit after the differential frontier but before the FULL snapshot.
#[tokio::test]
async fn test_manual_full_fallback_after_new_commit_does_not_replay() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.execute("CREATE TABLE manual_cut_src (id INT PRIMARY KEY, grp INT, qty BIGINT)")
        .await;
    db.execute(
        "INSERT INTO manual_cut_src
         SELECT i, i % 3, 10 FROM generate_series(1, 9) AS i",
    )
    .await;
    let query = "SELECT grp, SUM(qty) AS total, COUNT(*) AS n FROM manual_cut_src GROUP BY grp";
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            name => 'manual_cut_st',
            query => 'SELECT grp, SUM(qty) AS total, COUNT(*) AS n FROM manual_cut_src GROUP BY grp',
            schedule => '1h',
            refresh_mode => 'DIFFERENTIAL',
            cdc_mode => 'trigger'
        )",
    )
    .await;
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['manual_cut_st'])")
        .await;
    db.assert_st_matches_query("manual_cut_st", query).await;
    db.execute("INSERT INTO manual_cut_src VALUES (10, 1, 10)")
        .await;
    db.alter_system_set_and_wait("pg_trickle.refresh_strategy", "full", "full")
        .await;

    let st_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'manual_cut_st'",
        )
        .await;
    let source_oid: i64 = db
        .query_scalar("SELECT 'manual_cut_src'::regclass::oid::bigint")
        .await;
    let source_buffer: String = db
        .query_scalar(&format!(
            "SELECT format('%I.%I', 'pgtrickle_changes',
                           'changes_' || COALESCE(source_stable_name, {source_oid}::text))
             FROM pgtrickle.pgt_change_tracking
             WHERE source_relid = {source_oid}::oid"
        ))
        .await;

    // The extension pauses at its real differential input boundary after the
    // pre-lock frontier is captured.  It must subsequently do a FULL fallback.
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_phase",
        "'input_boundary'",
        "input_boundary",
    )
    .await;
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_for_table",
        "'manual_cut_st'",
        "manual_cut_st",
    )
    .await;
    let mut barrier = db.pool.acquire().await.expect("acquire manual barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *barrier)
        .await
        .expect("get manual barrier PID");
    sqlx::query("SELECT pg_advisory_lock($1::int, 1)")
        .bind(st_id as i32)
        .execute(&mut *barrier)
        .await
        .expect("hold input-boundary advisory lock");

    let pool = db.pool.clone();
    let refresh = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('manual_cut_st')")
            .execute(&pool)
            .await
    });
    let blocked = format!(
        "SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity a
            WHERE a.datname = current_database()
              AND a.application_name LIKE 'pgt_atomicity:{st_id}:input_boundary:%'
              AND a.wait_event_type = 'Lock'
              AND {blocker_pid} = ANY(pg_blocking_pids(a.pid))
        )"
    );
    assert!(
        db.wait_for_condition(
            "manual differential blocked after its frontier snapshot",
            &blocked,
            Duration::from_secs(30),
            Duration::from_millis(50),
        )
        .await,
        "manual refresh must reach the witnessed input boundary"
    );
    db.execute(
        "INSERT INTO manual_cut_src
         SELECT i, i % 3, 100 FROM generate_series(11, 110) AS i",
    )
    .await;
    // The relation is catalog-derived and SQL-identifier quoted by PostgreSQL
    // format('%I.%I'); no user string is interpolated into this query.
    let delta_sql = format!("SELECT MAX(lsn)::text FROM {source_buffer}");
    let writer_delta_lsn: String = db.query_scalar(&delta_sql).await;

    let unlocked: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1::int, 1)")
        .bind(st_id as i32)
        .fetch_one(&mut *barrier)
        .await
        .expect("release the manual barrier");
    assert!(unlocked, "the manual test barrier must be released");
    tokio::time::timeout(Duration::from_secs(90), refresh)
        .await
        .expect("manual FULL fallback timed out")
        .expect("manual refresh task panicked")
        .expect("manual FULL fallback failed");
    db.alter_system_set_and_wait("pg_trickle.test_chaos_for_table", "''", "")
        .await;
    db.alter_system_set_and_wait(
        "pg_trickle.test_chaos_phase",
        "'before_apply'",
        "before_apply",
    )
    .await;

    let fell_back: bool = db
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                WHERE st.pgt_name = 'manual_cut_st'
                  AND h.action = 'FULL' AND h.was_full_fallback
                  AND h.status = 'COMPLETED'
            )",
        )
        .await;
    assert!(
        fell_back,
        "manual differential must actually fall back to FULL"
    );
    db.assert_st_matches_query("manual_cut_st", query).await;

    let frontier_covers_writer: bool = sqlx::query_scalar(
        "SELECT (frontier #>> ARRAY['sources', $1::text, 'lsn'])::pg_lsn >= $2::pg_lsn
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'manual_cut_st'",
    )
    .bind(source_oid.to_string())
    .bind(&writer_delta_lsn)
    .fetch_one(&db.pool)
    .await
    .expect("read the manually rebuilt source frontier");
    assert!(
        frontier_covers_writer,
        "manual FULL fallback must cover rows committed after its pre-lock cut"
    );

    db.alter_system_set_and_wait(
        "pg_trickle.refresh_strategy",
        "differential",
        "differential",
    )
    .await;
    db.execute("INSERT INTO manual_cut_src VALUES (111, 0, 7)")
        .await;
    let before_diff: i64 = db
        .query_scalar("SELECT COALESCE(MAX(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.refresh_st("manual_cut_st").await;
    let actual_diff: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                WHERE st.pgt_name = 'manual_cut_st'
                  AND h.refresh_id > {before_diff}
                  AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED'
                  AND NOT h.was_full_fallback
            )"
        ))
        .await;
    assert!(
        actual_diff,
        "manual post-FULL refresh must really be differential"
    );
    db.assert_st_matches_query("manual_cut_st", query).await;
}

#[tokio::test]
async fn test_scheduler_full_baseline_does_not_replay_upstream_changes() {
    assert_scheduler_baseline_does_not_replay("FULL", true).await;
}

#[tokio::test]
async fn test_scheduler_fallback_baseline_does_not_replay_upstream_changes() {
    assert_scheduler_baseline_does_not_replay("FALLBACK", true).await;
}

#[tokio::test]
async fn test_scheduler_reinitialize_baseline_does_not_replay_upstream_changes() {
    assert_scheduler_baseline_does_not_replay("REINITIALIZE", true).await;
}

#[tokio::test]
async fn test_scheduler_missing_frontier_baseline_does_not_replay_upstream_changes() {
    assert_scheduler_baseline_does_not_replay("FRONTIER_MISSING", true).await;
}

#[tokio::test]
async fn test_scheduler_unchanged_upstream_baseline_preserves_counts() {
    assert_scheduler_baseline_does_not_replay("REINITIALIZE", false).await;
}

/// A lossless CDC pause keeps trigger capture active while refresh consumption
/// is stopped, then lets differential refresh catch up after resume.
#[tokio::test]
async fn test_cdc_hold_pause_preserves_dml_until_resume() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "trigger", "trigger")
        .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_capture_mode", "hold", "hold")
        .await;
    db.alter_system_set_and_wait("pg_trickle.cdc_paused", "on", "on")
        .await;

    db.execute("CREATE TABLE cdc_hold_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO cdc_hold_src VALUES (1, 'one')")
        .await;
    db.create_st(
        "cdc_hold_st",
        "SELECT id, val FROM cdc_hold_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = db.table_oid("cdc_hold_src").await;
    let buffer = db.change_buffer_table(source_oid as i64).await;
    db.execute("INSERT INTO cdc_hold_src VALUES (2, 'two')")
        .await;
    db.execute("UPDATE cdc_hold_src SET val = 'updated' WHERE id = 1")
        .await;
    db.execute("DELETE FROM cdc_hold_src WHERE id = 2").await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(db.count("public.cdc_hold_st").await, 1);
    assert_eq!(
        db.query_scalar::<String>("SELECT val FROM public.cdc_hold_st WHERE id = 1")
            .await,
        "one"
    );
    assert!(
        db.count(&buffer).await >= 4,
        "hold mode must retain INSERT/UPDATE/DELETE changes in the buffer"
    );

    db.alter_system_set_and_wait("pg_trickle.cdc_paused", "off", "off")
        .await;
    assert!(
        db.wait_for_condition(
            "CDC hold resume catch-up",
            "SELECT count(*) = 1 AND bool_and(val = 'updated') FROM public.cdc_hold_st",
            Duration::from_secs(30),
            Duration::from_millis(100),
        )
        .await,
        "resuming CDC must let the scheduler apply the buffered changes"
    );

    db.alter_system_reset_and_wait("pg_trickle.cdc_capture_mode", "discard")
        .await;
}

/// Wait until `pgt_name` has at least `min_count` COMPLETED refresh records.
async fn wait_for_n_refreshes(
    db: &E2eDb,
    pgt_name: &str,
    min_count: i64,
    timeout: Duration,
) -> i64 {
    let start = std::time::Instant::now();
    loop {
        let count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
                 WHERE d.pgt_name = '{pgt_name}' AND h.status = 'COMPLETED'"
            ))
            .await;
        if count >= min_count {
            return count;
        }
        if start.elapsed() > timeout {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Snapshot-polled sources must be polled before the fanout cache freezes
/// change visibility, even when their buffers were initially empty.
#[tokio::test]
async fn test_scheduler_fanout_shared_matview_changes_refresh_both_consumers() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;
    // This test asserts scheduled refreshes, so host load must not defer them.
    db.alter_system_set_and_wait("pg_trickle.load_shed_threshold", "0", "0")
        .await;
    db.alter_system_set_and_wait("pg_trickle.enable_change_buffer_fanout", "on", "on")
        .await;
    db.execute("CREATE TABLE fanout_mv_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO fanout_mv_src VALUES (1, 10)").await;
    db.execute("CREATE MATERIALIZED VIEW fanout_mv AS SELECT * FROM fanout_mv_src")
        .await;
    for name in ["fanout_mv_a", "fanout_mv_b"] {
        db.create_st(name, "SELECT id, val FROM fanout_mv", "1s", "FULL")
            .await;
    }

    // A failed remote poll must not starve the independent MATVIEW group.
    db.execute("CREATE EXTENSION postgres_fdw").await;
    let db_name: String = db.query_scalar("SELECT current_database()").await;
    // Keep the deliberately failed poll bounded so the test measures scheduler
    // isolation instead of the runner's TCP connect timeout.
    db.execute(&format!(
        "CREATE SERVER fanout_remote FOREIGN DATA WRAPPER postgres_fdw \
         OPTIONS (dbname '{db_name}', host '127.0.0.1', port '5432', connect_timeout '1')"
    ))
    .await;
    db.execute(
        "CREATE USER MAPPING FOR CURRENT_USER SERVER fanout_remote OPTIONS (user 'postgres')",
    )
    .await;
    db.execute("CREATE TABLE fanout_remote_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO fanout_remote_src VALUES (1, 10)")
        .await;
    db.execute("CREATE FOREIGN TABLE fanout_ft (id INT, val INT) SERVER fanout_remote OPTIONS (table_name 'fanout_remote_src')")
        .await;
    db.create_st(
        "fanout_ft_st",
        "SELECT id, val FROM fanout_ft",
        "1s",
        "FULL",
    )
    .await;
    db.execute("ALTER SERVER fanout_remote OPTIONS (SET port '1')")
        .await;
    db.execute("UPDATE fanout_remote_src SET val = 30").await;

    // Updating and then deleting the only row verifies both nonempty and
    // empty snapshots; no manual ST refresh may supply the missing polling.
    for mutation in [
        "UPDATE fanout_mv_src SET val = 20",
        "DELETE FROM fanout_mv_src",
    ] {
        db.execute(mutation).await;
        db.execute("REFRESH MATERIALIZED VIEW fanout_mv").await;
        assert!(
            db.wait_for_condition(
                "both fanout consumers match the materialized view",
                "SELECT NOT EXISTS (
                    (SELECT id, val FROM fanout_mv_a EXCEPT ALL SELECT id, val FROM fanout_mv)
                    UNION ALL
                    (SELECT id, val FROM fanout_mv EXCEPT ALL SELECT id, val FROM fanout_mv_a)
                    UNION ALL
                    (SELECT id, val FROM fanout_mv_b EXCEPT ALL SELECT id, val FROM fanout_mv)
                    UNION ALL
                    (SELECT id, val FROM fanout_mv EXCEPT ALL SELECT id, val FROM fanout_mv_b)
                )",
                Duration::from_secs(30),
                Duration::from_millis(100),
            )
            .await,
            "fanout polling must refresh both consumers after {mutation}"
        );
    }
    // The failed source remains retryable and catches up after recovery.
    db.execute("ALTER SERVER fanout_remote OPTIONS (SET port '5432')")
        .await;
    assert!(
        db.wait_for_condition(
            "foreign source recovers after polling failure",
            "SELECT EXISTS (SELECT 1 FROM fanout_ft_st WHERE val = 30)",
            Duration::from_secs(30),
            Duration::from_millis(100),
        )
        .await
    );
}

/// PERF-1: The change-buffer fanout cache must not suppress legitimate refresh
/// triggers.
///
/// Two stream tables share the same source table.  After a DML change, both
/// STs must be refreshed within a reasonable time.  This verifies that the
/// per-tick batched EXISTS query correctly identifies both STs as having
/// pending changes (deduplication of the _source scan_, not the refresh).
#[tokio::test]
async fn test_scheduler_fanout_deduplicates_buffer_scan() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Enable fanout explicitly (it should be on by default, but let's be sure).
    db.execute("ALTER SYSTEM SET pg_trickle.enable_change_buffer_fanout = on")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.enable_change_buffer_fanout", "on")
        .await;

    // Create one source table and two stream tables that read from it.
    db.execute("CREATE TABLE fanout_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO fanout_src VALUES (1, 100)").await;

    db.create_st(
        "fanout_st_a",
        "SELECT id, val FROM fanout_src",
        "1s",
        "FULL",
    )
    .await;
    db.create_st(
        "fanout_st_b",
        "SELECT id, val * 2 AS val2 FROM fanout_src",
        "1s",
        "FULL",
    )
    .await;

    // Wait for the initial refresh of both STs.
    let cnt_a = wait_for_n_refreshes(&db, "fanout_st_a", 1, Duration::from_secs(30)).await;
    let cnt_b = wait_for_n_refreshes(&db, "fanout_st_b", 1, Duration::from_secs(30)).await;
    assert!(
        cnt_a >= 1,
        "fanout_st_a must refresh at least once initially"
    );
    assert!(
        cnt_b >= 1,
        "fanout_st_b must refresh at least once initially"
    );

    // Verify initial content.
    assert_eq!(db.count("public.fanout_st_a").await, 1);
    assert_eq!(db.count("public.fanout_st_b").await, 1);

    // Insert a second row to create a CDC event.
    db.execute("INSERT INTO fanout_src VALUES (2, 200)").await;

    // Both STs must pick up the new row within a few seconds.
    let got_a = wait_for_n_refreshes(&db, "fanout_st_a", cnt_a + 1, Duration::from_secs(30)).await;
    let got_b = wait_for_n_refreshes(&db, "fanout_st_b", cnt_b + 1, Duration::from_secs(30)).await;

    assert!(
        got_a > cnt_a,
        "fanout_st_a must refresh after source insert (got {got_a}, expected ≥ {})",
        cnt_a + 1
    );
    assert!(
        got_b > cnt_b,
        "fanout_st_b must refresh after source insert (got {got_b}, expected ≥ {})",
        cnt_b + 1
    );

    assert_eq!(
        db.count("public.fanout_st_a").await,
        2,
        "fanout_st_a must contain 2 rows after refresh"
    );
    assert_eq!(
        db.count("public.fanout_st_b").await,
        2,
        "fanout_st_b must contain 2 rows after refresh"
    );
}

#[tokio::test]
async fn test_scheduler_records_and_cleans_up_effective_full_fallback() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.refresh_strategy", "full", "full")
        .await;

    db.execute("CREATE TABLE effective_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO effective_src VALUES (1, 'initial')")
        .await;
    db.create_st(
        "effective_st",
        "SELECT id, val FROM effective_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let source_oid = i64::from(db.table_oid("effective_src").await);
    let buffer = db.change_buffer_table(source_oid).await;
    db.execute("INSERT INTO effective_src VALUES (2, 'fallback')")
        .await;
    let pending_changes = format!("SELECT count(*) FROM {buffer} WHERE action IN ('I', 'D')");
    assert!(
        db.query_scalar::<i64>(&pending_changes).await > 0,
        "CDC buffer should contain the insert"
    );

    let completed = db
        .wait_for_condition(
            "scheduler effective FULL fallback",
            "SELECT EXISTS(SELECT 1
               FROM pgtrickle.pgt_refresh_history h
               JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
              WHERE st.pgt_name = 'effective_st'
                AND h.status = 'COMPLETED'
                AND h.refresh_reason = 'CONFIGURED_FULL')",
            Duration::from_secs(60),
            Duration::from_millis(300),
        )
        .await;
    assert!(
        completed,
        "scheduler should complete the configured FULL fallback"
    );

    let (action, strategy, was_fallback): (String, String, bool) = sqlx::query_as(
        "SELECT h.action, h.merge_strategy_used, h.was_full_fallback
           FROM pgtrickle.pgt_refresh_history h
           JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
          WHERE st.pgt_name = 'effective_st'
            AND h.refresh_reason = 'CONFIGURED_FULL'
          ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read scheduler fallback history");
    assert_eq!(action, "FULL");
    assert_eq!(strategy, "FULL");
    assert!(was_fallback);

    assert_eq!(
        db.query_scalar::<i64>(&pending_changes).await,
        0,
        "effective FULL finalization should remove consumed CDC rows"
    );
    let (full_count, diff_count): (i64, i64) = sqlx::query_as(
        "SELECT total_full_refreshes, total_diff_refreshes
           FROM pgtrickle.pgt_refresh_summary
          WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                           WHERE pgt_name = 'effective_st')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read refresh summary");
    assert!(full_count >= 1);
    assert_eq!(diff_count, 0);

    db.alter_system_reset_and_wait("pg_trickle.refresh_strategy", "auto")
        .await;
}

/// API-1/2: `pause_scheduler` stops the scheduler from dispatching a node;
/// `resume_scheduler` re-enables it.
#[tokio::test]
async fn test_pause_resume_scheduler_basic() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("CREATE TABLE pause_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO pause_src VALUES (1, 10)").await;
    db.create_st("pause_st", "SELECT id, val FROM pause_src", "1s", "FULL")
        .await;

    // Wait for the initial refresh.
    let baseline = wait_for_n_refreshes(&db, "pause_st", 1, Duration::from_secs(30)).await;
    assert!(
        baseline >= 1,
        "pause_st must refresh at least once initially"
    );
    assert_eq!(db.count("public.pause_st").await, 1);

    // Pause the node.
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.pause_st'])")
        .await;

    // Insert new data — the scheduler should NOT dispatch the node.
    db.execute("INSERT INTO pause_src VALUES (2, 20)").await;

    // Wait two scheduler intervals to give the scheduler time to skip the node.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let after_pause: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'pause_st' AND h.status = 'COMPLETED'",
        )
        .await;

    assert_eq!(
        after_pause, baseline,
        "pause_st should not refresh while paused (baseline={baseline}, after={after_pause})"
    );
    // Data should still be stale.
    assert_eq!(
        db.count("public.pause_st").await,
        1,
        "pause_st must still have 1 row while paused"
    );

    // Resume and wait for the scheduler to pick it up.
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['public.pause_st'])")
        .await;

    let after_resume =
        wait_for_n_refreshes(&db, "pause_st", baseline + 1, Duration::from_secs(30)).await;
    assert!(
        after_resume > baseline,
        "pause_st must refresh after resume (got {after_resume}, expected ≥ {})",
        baseline + 1
    );
    assert_eq!(
        db.count("public.pause_st").await,
        2,
        "pause_st must contain 2 rows after resume refresh"
    );
}

#[tokio::test]
async fn test_scheduler_paused_atomic_member_stays_stale_until_resume() {
    for mode in ["off", "dry_run"] {
        let db = E2eDb::new_dedicated().await.with_extension().await;
        configure_fast_scheduler(&db).await;
        db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", mode, mode)
            .await;
        db.alter_system_set_and_wait("pg_trickle.load_shed_threshold", "0", "0")
            .await;

        db.execute("CREATE TABLE pause_group_src (id INT PRIMARY KEY, val INT)")
            .await;
        db.execute("INSERT INTO pause_group_src VALUES (1, 10), (2, 20), (3, 30)")
            .await;
        // The 1h schedule leaves an idle window before pause; members become due afterward.
        for (name, query) in [
            (
                "pause_group_b",
                "SELECT id, val FROM pause_group_src WHERE val <= 20",
            ),
            (
                "pause_group_c",
                "SELECT id, val FROM pause_group_src WHERE val >= 20",
            ),
            (
                "pause_group_d",
                "SELECT b.id AS b_id, c.id AS c_id, b.val + c.val AS total \
                 FROM pause_group_b b JOIN pause_group_c c ON b.id = c.id",
            ),
        ] {
            db.execute(&format!(
                "SELECT pgtrickle.create_stream_table('{name}', $${query}$$, \
                 '1h', 'FULL', true, 'atomic')"
            ))
            .await;
        }
        db.create_st(
            "pause_group_control",
            "SELECT id, val FROM pause_group_src",
            "1s",
            "DIFFERENTIAL",
        )
        .await;

        for name in [
            "pause_group_b",
            "pause_group_c",
            "pause_group_d",
            "pause_group_control",
        ] {
            let count = wait_for_n_refreshes(&db, name, 1, Duration::from_secs(60)).await;
            assert!(count >= 1, "{name} did not populate in {mode} mode");
        }
        assert!(
            db.query_scalar::<bool>(
                "SELECT count(*) = 3 AND count(DISTINCT group_id) = 1 \
                   FROM pgtrickle.diamond_groups() \
                  WHERE member_schema = 'public' \
                    AND member_name IN ('pause_group_b', 'pause_group_c', 'pause_group_d')"
            )
            .await,
            "B, C, and D must be members of one atomic diamond group in {mode} mode"
        );

        let group_refresh_baselines = sqlx::query_as::<_, (String, i64, i64)>(
            "SELECT st.pgt_name::text, \
                    count(*) FILTER (WHERE h.status = 'COMPLETED')::bigint, \
                    count(*)::bigint \
               FROM pgtrickle.pgt_stream_tables st \
               LEFT JOIN pgtrickle.pgt_refresh_history h USING (pgt_id) \
              WHERE st.pgt_name IN ('pause_group_b', 'pause_group_c', 'pause_group_d') \
              GROUP BY st.pgt_name",
        )
        .fetch_all(&db.pool)
        .await
        .expect("read atomic group refresh baselines");
        assert_eq!(group_refresh_baselines.len(), 3);
        let group_refreshes: i64 = group_refresh_baselines.iter().map(|row| row.2).sum();
        let control_refresh_baseline =
            wait_for_n_refreshes(&db, "pause_group_control", 1, Duration::from_secs(30)).await;

        assert_eq!(
            db.query_scalar::<String>("SELECT current_setting('pg_trickle.parallel_refresh_mode')")
                .await,
            mode,
            "scheduler must use the requested sequential path"
        );
        assert!(
            db.query_scalar::<bool>(
                "SELECT bool_and(st.last_refresh_at > now() - interval '55 minutes') \
                        AND bool_and(st.status = 'ACTIVE' AND st.consecutive_errors = 0 \
                                     AND st.fuse_mode = 'off' AND st.fuse_state = 'armed') \
                        AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_source_gates g \
                                        WHERE g.source_relid = 'pause_group_src'::regclass) \
                        AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                                        JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
                                        WHERE st.pgt_name IN ('pause_group_b', 'pause_group_c', \
                                                              'pause_group_d') \
                                          AND h.status = 'RUNNING') \
                   FROM pgtrickle.pgt_stream_tables st \
                  WHERE st.pgt_name IN ('pause_group_b', 'pause_group_c', 'pause_group_d')"
            )
            .await,
            "group must be idle, healthy, ungated, and not due before pause in {mode} mode"
        );
        assert_eq!(
            db.query_scalar::<String>(
                "SELECT pgtrickle.pause_scheduler(ARRAY['public.pause_group_b'])"
            )
            .await,
            "OK"
        );
        db.execute("INSERT INTO pause_group_src VALUES (4, 20)")
            .await;
        let source_oid = i64::from(db.table_oid("pause_group_src").await);
        let change_buffer = db.change_buffer_table(source_oid).await;
        assert!(
            db.wait_for_condition(
                "control source insert capture",
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM {change_buffer} WHERE id = 4 AND action = 'I')"
                ),
                Duration::from_secs(30),
                Duration::from_millis(50),
            )
            .await,
            "source insert was not captured before scheduler progress in {mode} mode"
        );
        db.execute(
            "UPDATE pgtrickle.pgt_stream_tables \
                SET last_refresh_at = now() - interval '2 hours' \
              WHERE pgt_name IN ('pause_group_b', 'pause_group_c', 'pause_group_d', \
                                 'pause_group_control')",
        )
        .await;

        // A completed refresh can have taken its source snapshot before the
        // committed insert. Wait for the observable row as well as new history
        // so scheduler progress proves this source change was applied.
        let control_caught_up = db
            .wait_for_condition(
                "unpaused control consumes committed source row",
                &format!(
                    "SELECT (SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
                              JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
                             WHERE st.pgt_name = 'pause_group_control' \
                               AND h.status = 'COMPLETED') > {control_refresh_baseline} \
                        AND EXISTS (SELECT 1 FROM pause_group_control \
                                    WHERE id = 4 AND val = 20)"
                ),
                Duration::from_secs(30),
                Duration::from_millis(50),
            )
            .await;
        assert!(
            control_caught_up,
            "unpaused source consumer did not apply the committed row in {mode} mode"
        );
        db.assert_st_matches_query("pause_group_control", "SELECT id, val FROM pause_group_src")
            .await;

        tokio::time::sleep(Duration::from_secs(2)).await;
        let after_pause: i64 = db
            .query_scalar(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
                   JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
                  WHERE st.pgt_name IN ('pause_group_b', 'pause_group_c', 'pause_group_d')",
            )
            .await;
        assert_eq!(
            after_pause, group_refreshes,
            "atomic group must stay deferred while B is paused in {mode} mode"
        );
        for table in ["pause_group_b", "pause_group_c"] {
            assert_eq!(
                db.query_scalar::<i64>(&format!("SELECT count(*) FROM {table} WHERE id = 4"))
                    .await,
                0,
                "{table} changed while B was paused in {mode} mode"
            );
        }
        assert_eq!(
            db.query_scalar::<i64>("SELECT count(*) FROM pause_group_d WHERE b_id = 4")
                .await,
            0,
            "pause_group_d changed while B was paused in {mode} mode"
        );

        db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['public.pause_group_b'])")
            .await;
        for (name, completed, _) in group_refresh_baselines {
            assert!(
                wait_for_n_refreshes(&db, &name, completed + 1, Duration::from_secs(30)).await
                    > completed,
                "{name} did not refresh after resume in {mode} mode"
            );
        }
        db.assert_st_matches_query(
            "pause_group_b",
            "SELECT id, val FROM pause_group_src WHERE val <= 20",
        )
        .await;
        db.assert_st_matches_query(
            "pause_group_c",
            "SELECT id, val FROM pause_group_src WHERE val >= 20",
        )
        .await;
        db.assert_st_matches_query(
            "pause_group_d",
            "SELECT b.id AS b_id, c.id AS c_id, b.val + c.val AS total \
             FROM pause_group_b b JOIN pause_group_c c ON b.id = c.id",
        )
        .await;
    }
}

/// API-1: `pause_scheduler` reports a bounded timeout while an active refresh
/// cannot drain, and the worker can finish after the blocking condition clears.
#[tokio::test]
async fn test_pause_scheduler_drain_timeout() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "'on'", "on")
        .await;
    db.alter_system_set_and_wait("pg_trickle.scheduler_drain_timeout", "1", "1")
        .await;

    db.execute("CREATE TABLE drain_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO drain_src VALUES (1, 1)").await;
    db.execute(
        "CREATE FUNCTION public.drain_refresh_gate() RETURNS boolean
         LANGUAGE plpgsql VOLATILE AS $$
         BEGIN
             -- Keep the test barrier beyond pause_scheduler's drain deadline.
             PERFORM set_config('lock_timeout', '30s', true);
             PERFORM pg_advisory_xact_lock(1219001::bigint);
             RETURN true;
         END
         $$",
    )
    .await;
    db.create_st(
        "drain_st",
        "SELECT id, val, public.drain_refresh_gate() AS gate FROM drain_src",
        "1s",
        "FULL",
    )
    .await;
    db.refresh_st("drain_st").await;

    let baseline = wait_for_n_refreshes(&db, "drain_st", 1, Duration::from_secs(30)).await;
    assert!(
        baseline >= 1,
        "drain_st must have a completed baseline refresh"
    );

    let mut barrier = db
        .pool
        .acquire()
        .await
        .expect("failed to acquire scheduler barrier connection");
    let barrier_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *barrier)
        .await
        .expect("failed to get scheduler barrier PID");
    sqlx::query("SELECT pg_advisory_lock(1219001::bigint)")
        .execute(&mut *barrier)
        .await
        .expect("failed to hold scheduler barrier");

    // The 1s schedule reaches this gate while the test holds its advisory lock.
    let blocked_refresh = format!(
        "SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity a
            WHERE a.datname = current_database()
              AND a.backend_type = 'pg_trickle refresh worker'
              AND a.wait_event_type = 'Lock'
              AND {barrier_pid} = ANY(pg_blocking_pids(a.pid))
        )"
    );
    assert!(
        db.wait_for_condition(
            "scheduled refresh blocked on the test barrier",
            &blocked_refresh,
            Duration::from_secs(30),
            Duration::from_millis(50),
        )
        .await,
        "the scheduler must reach the blocked refresh query before pause"
    );

    let start = std::time::Instant::now();
    let error = db
        .try_execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.drain_st'])")
        .await;
    let error = error.expect_err("pause_scheduler must time out while the refresh remains blocked");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "pause_scheduler must complete within 10 s with a 1 s drain_timeout (took {elapsed:?})"
    );
    assert!(
        error.to_string().contains("pause_scheduler timed out"),
        "pause_scheduler returned an unexpected error: {error}"
    );

    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock(1219001::bigint)")
        .fetch_one(&mut *barrier)
        .await
        .expect("failed to release scheduler barrier");
    assert!(released, "test must release the scheduler barrier");

    let after_timeout =
        wait_for_n_refreshes(&db, "drain_st", baseline + 1, Duration::from_secs(30)).await;
    assert!(
        after_timeout > baseline,
        "the in-flight refresh must finish after releasing the barrier"
    );

    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['public.drain_st'])")
        .await;
    db.alter_system_reset_and_wait("pg_trickle.parallel_refresh_mode", "on")
        .await;
    db.alter_system_reset_and_wait("pg_trickle.scheduler_drain_timeout", "30")
        .await;
}

#[tokio::test]
async fn test_scheduled_reinitialize_arms_automatic_partition_retention() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;
    db.execute("CREATE TABLE arm1499_src (id int NOT NULL, d date NOT NULL, amount int NOT NULL, PRIMARY KEY(id,d)) PARTITION BY RANGE(d)").await;
    db.execute("CREATE TABLE arm1499_old PARTITION OF arm1499_src FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')").await;
    db.execute("CREATE TABLE arm1499_live PARTITION OF arm1499_src FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')").await;
    db.execute("CREATE TABLE arm1499_default PARTITION OF arm1499_src DEFAULT")
        .await;
    db.execute("INSERT INTO arm1499_src VALUES (1,'2025-06-01',4),(2,'2026-06-01',8)")
        .await;
    let query = "SELECT d, SUM(amount) AS total, COUNT(*) AS n FROM arm1499_src GROUP BY d";
    db.create_st("arm1499_st", query, "1h", "DIFFERENTIAL")
        .await;
    let policy: serde_json::Value = db
        .query_scalar("SELECT pgtrickle.get_stream_table_partition_retention('arm1499_st')")
        .await;
    assert_ne!(policy["automatic_delete_with_source"], true);

    db.execute("DROP TABLE arm1499_default").await;
    db.execute("SELECT pgtrickle.reinitialize_stream_table('arm1499_st')")
        .await;
    assert!(
        db.wait_for_condition(
            "scheduler protected reinitialization completed",
            "SELECT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_refresh_history h
                JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                WHERE st.pgt_name = 'arm1499_st' AND h.action = 'REINITIALIZE'
                  AND h.status = 'COMPLETED' AND h.initiated_by = 'SCHEDULER'
                  AND st.is_populated AND NOT st.needs_reinit
            )",
            Duration::from_secs(90),
            Duration::from_millis(100),
        )
        .await
    );
    let policy: serde_json::Value = db
        .query_scalar("SELECT pgtrickle.get_stream_table_partition_retention('arm1499_st')")
        .await;
    assert_eq!(policy["automatic_delete_with_source"], true, "{policy}");
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.arm1499_st'])")
        .await;
    db.execute("DROP TABLE arm1499_old").await;
    db.assert_st_matches_query("arm1499_st", query).await;
    assert!(
        !db.query_scalar::<bool>(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'arm1499_st'",
        )
        .await
    );
}
