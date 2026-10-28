//! v0.62.0: Full E2E tests for scheduler PERF-1 fanout cache and
//! API-1/2 per-node pause/resume control.
//!
//! These tests require the background scheduler (`shared_preload_libraries`)
//! and can only run in the full E2E harness (just test-e2e).

mod e2e;

use e2e::E2eDb;
use sqlx::AssertSqlSafe;
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
    // Manual FULL is a control, evaluated only after recording scheduled output.
    db.refresh_st("clock_st").await;
    db.assert_st_matches_query("clock_st", query).await;
    assert!(refreshed, "scheduled output stayed stale: {witness}");
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
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['replay_up', 'replay_leaf'])")
        .await;
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
    if change_upstream {
        assert_eq!(witness["same_transaction"], true, "{diagnostics}");
    }
    if baseline == "FALLBACK" {
        assert_eq!(witness["fallback"], true, "{diagnostics}");
    }
    if change_upstream {
        assert!(db.query_scalar::<bool>(&format!(
            "SELECT MAX(lsn) > (SELECT tick_watermark_lsn FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'replay_leaf' AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) FROM {buffer}"
        )).await, "upstream capture must exceed the tick bound: {diagnostics}");
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
    db.execute(&format!(
        "CREATE SERVER fanout_remote FOREIGN DATA WRAPPER postgres_fdw \
         OPTIONS (dbname '{db_name}', host '127.0.0.1', port '5432')"
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

/// API-1/2: pausing after dispatch waits for the in-flight refresh to finish,
/// then prevents later due work until the node is resumed.
#[tokio::test]
async fn test_pause_scheduler_drains_dispatched_refresh_before_returning() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    db.alter_system_set_and_wait("pg_trickle.parallel_refresh_mode", "off", "off")
        .await;

    db.execute("CREATE TABLE pause_race_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO pause_race_src VALUES (1, 10)")
        .await;
    db.create_st(
        "pause_race_st",
        "SELECT id, val FROM public.pause_race_src",
        "1h",
        "FULL",
    )
    .await;
    let baseline = wait_for_n_refreshes(&db, "pause_race_st", 1, Duration::from_secs(30)).await;
    assert!(baseline >= 1, "pause_race_st must refresh initially");

    // Hold the output relation so the scheduled refresh is visibly in flight.
    let target_rel: String = db
        .query_scalar(
            "SELECT pgt_relid::regclass::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'pause_race_st'",
        )
        .await;
    let mut table_lock = db
        .pool
        .begin()
        .await
        .expect("begin output lock transaction");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *table_lock)
        .await
        .expect("get output lock backend PID");
    // The name comes from this test-created relation's regclass, not caller input.
    sqlx::query(AssertSqlSafe(format!(
        "LOCK TABLE {target_rel} IN ACCESS EXCLUSIVE MODE"
    )))
    .execute(&mut *table_lock)
    .await
    .expect("hold output table lock");
    let lock_held: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_locks \
         WHERE pid = $1 AND relation = $2::regclass AND mode = 'AccessExclusiveLock' AND granted)",
    )
    .bind(blocker_pid)
    .bind(&target_rel)
    .fetch_one(&db.pool)
    .await
    .expect("inspect output table lock");
    assert!(lock_held, "output lock was not held on {target_rel}");
    db.execute("INSERT INTO pause_race_src VALUES (2, 20)")
        .await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables
            SET last_refresh_at = now() - interval '2 hours'
          WHERE pgt_name = 'pause_race_st'",
    )
    .await;

    let blocked_refresh = db
        .wait_for_condition(
            "dispatched refresh blocked by output table lock",
            &format!(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                 WHERE wait_event_type = 'Lock' AND {blocker_pid} = ANY(pg_blocking_pids(pid)))"
            ),
            Duration::from_secs(30),
            Duration::from_millis(20),
        )
        .await;
    if !blocked_refresh {
        let state: String = db
            .query_scalar(
                "SELECT jsonb_build_object(
                     'stream', (SELECT jsonb_build_object(
                         'status', status, 'mode', refresh_mode, 'schedule', schedule,
                         'last_refresh_at', last_refresh_at, 'relid', pgt_relid::regclass::text
                     ) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'pause_race_st'),
                     'counts', jsonb_build_object(
                         'source', (SELECT count(*) FROM public.pause_race_src),
                         'output', (SELECT count(*) FROM public.pause_race_st)
                     ),
                     'history', (SELECT jsonb_agg(jsonb_build_object(
                         'status', h.status, 'action', h.action, 'initiated_by', h.initiated_by
                     ) ORDER BY h.refresh_id) FROM pgtrickle.pgt_refresh_history h
                         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id)
                         WHERE st.pgt_name = 'pause_race_st'),
                     'jobs', (SELECT jsonb_agg(jsonb_build_object(
                         'status', j.status, 'members', j.member_pgt_ids
                     )) FROM pgtrickle.pgt_scheduler_jobs j
                         WHERE j.member_pgt_ids && ARRAY[(SELECT pgt_id
                             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'pause_race_st')]
                     )
                 )::text",
            )
            .await;
        let blocked: String = sqlx::query_scalar(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object(
                 'pid', pid, 'query', query, 'state', state,
                 'wait_event_type', wait_event_type, 'wait_event', wait_event
             )), '[]'::jsonb)::text FROM pg_stat_activity
             WHERE $1 = ANY(pg_blocking_pids(pid))",
        )
        .bind(blocker_pid)
        .fetch_one(&db.pool)
        .await
        .expect("inspect scheduler activity blocked by the output lock");
        panic!(
            "scheduler did not dispatch a refresh blocked by the output lock; state={state}; blocked={blocked}"
        );
    }

    let pause_pool = db.pool.clone();
    let pause_task = tokio::spawn(async move {
        sqlx::query_scalar::<_, String>(
            "SELECT pgtrickle.pause_scheduler(ARRAY['public.pause_race_st'])",
        )
        .fetch_one(&pause_pool)
        .await
    });
    let pause_started = db
        .wait_for_condition(
            "pause_scheduler entered its drain wait",
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
             WHERE state = 'active' AND query LIKE '%pause_scheduler%pause_race_st%')",
            Duration::from_secs(10),
            Duration::from_millis(20),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let pause_waited_for_refresh = !pause_task.is_finished();

    // Always release the witness before asserting or awaiting the pause task.
    table_lock
        .commit()
        .await
        .expect("release output table lock");
    let pause_result = tokio::time::timeout(Duration::from_secs(20), pause_task)
        .await
        .expect("pause_scheduler did not finish after the refresh drained")
        .expect("pause_scheduler task panicked")
        .expect("pause_scheduler failed");
    assert!(
        pause_started,
        "pause_scheduler never entered its drain wait"
    );
    assert!(
        pause_waited_for_refresh,
        "pause_scheduler returned while the dispatched refresh was blocked"
    );
    assert_eq!(pause_result, "OK");

    let after_drain =
        wait_for_n_refreshes(&db, "pause_race_st", baseline + 1, Duration::from_secs(30)).await;
    assert!(after_drain > baseline, "in-flight refresh did not complete");
    assert_eq!(db.count("public.pause_race_st").await, 2);

    // Make another change due while paused; it must wait until resume.
    db.execute("INSERT INTO pause_race_src VALUES (3, 30)")
        .await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables
            SET last_refresh_at = now() - interval '2 hours'
          WHERE pgt_name = 'pause_race_st'",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let while_paused =
        wait_for_n_refreshes(&db, "pause_race_st", after_drain, Duration::from_secs(1)).await;
    assert_eq!(while_paused, after_drain, "paused work was dispatched");
    assert_eq!(db.count("public.pause_race_st").await, 2);

    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['public.pause_race_st'])")
        .await;
    let after_resume = wait_for_n_refreshes(
        &db,
        "pause_race_st",
        after_drain + 1,
        Duration::from_secs(30),
    )
    .await;
    assert!(
        after_resume > after_drain,
        "resume did not dispatch due work"
    );
    assert_eq!(db.count("public.pause_race_st").await, 3);
    db.assert_st_matches_query("pause_race_st", "SELECT id, val FROM public.pause_race_src")
        .await;
}

/// API-1: `pause_scheduler` returns within `scheduler_drain_timeout` seconds
/// even when there are active refresh workers.
///
/// This test verifies the drain-timeout warning path by reducing the timeout
/// to 1 second and pausing a fast-refreshing node.
#[tokio::test]
async fn test_pause_scheduler_drain_timeout() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Set a short drain timeout to exercise the timeout code path.
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_drain_timeout = 1")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_drain_timeout", "1")
        .await;

    db.execute("CREATE TABLE drain_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO drain_src VALUES (1, 1)").await;
    db.create_st("drain_st", "SELECT id, val FROM drain_src", "1s", "FULL")
        .await;

    // Wait for the first refresh.
    let _ = wait_for_n_refreshes(&db, "drain_st", 1, Duration::from_secs(30)).await;

    // The call must complete (not hang) regardless of worker state.
    // We verify the call succeeds (doesn't error) and returns within a
    // generous wall-clock budget.
    let start = std::time::Instant::now();
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.drain_st'])")
        .await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(10),
        "pause_scheduler must complete within 10 s with a 1 s drain_timeout (took {elapsed:?})"
    );

    // Restore drain timeout to default.
    db.execute("ALTER SYSTEM RESET pg_trickle.scheduler_drain_timeout")
        .await;
    db.reload_config_and_wait().await;

    // Resume the node so it doesn't stay paused and affect other tests.
    db.execute("SELECT pgtrickle.resume_scheduler(ARRAY['public.drain_st'])")
        .await;
}
