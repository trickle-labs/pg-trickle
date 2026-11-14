//! E2E tests for frontier visibility holdback (Issue #536).
//!
//! These tests verify that the `pg_trickle.frontier_holdback_mode` GUC
//! prevents silent data loss when a long-running transaction inserts into
//! a tracked source table while the scheduler is advancing the frontier.
//!
//! # Background
//!
//! The CDC trigger path records changes with `lsn = pg_current_wal_insert_lsn()`
//! at trigger-fire time.  If a transaction inserts a row at LSN 100 but does
//! not commit before the scheduler captures `write_lsn = 500` and sets the
//! frontier to 500, the next tick queries `lsn > 500` and permanently misses
//! the row at LSN 100.
//!
//! With `frontier_holdback_mode = 'xmin'` (the default), the scheduler probes
//! `pg_stat_activity` + `pg_prepared_xacts` and refuses to advance the frontier
//! past the last safe LSN while any in-progress transaction exists.
//!
//! **Test matrix:**
//! 1. `test_holdback_gucs_registered` — GUC defaults are correct.
//! 2. `test_holdback_read_committed_long_txn` — READ COMMITTED txn spanning a tick.
//! 3. `test_holdback_repeatable_read_long_txn` — REPEATABLE READ txn spanning ticks.
//! 4. `test_holdback_prepared_transaction` — 2PC PREPARE spanning many ticks.
//! 5. `test_holdback_none_mode_regression_guard` — demonstrates data loss with
//!    `frontier_holdback_mode = 'none'` (regression guard).
//!
//! These tests require the full E2E Docker image (scheduler background worker).

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

/// #1412: require repeated scheduled progress before the write load stops.
#[tokio::test]
async fn test_holdback_concurrent_writers_make_progress() {
    concurrent_writers_make_progress(false).await;
}

/// A missing activity timestamp is a deterministic witness for the otherwise
/// timing-dependent PGPROC/activity-status race under concurrent writes.
#[tokio::test]
async fn test_holdback_concurrent_writers_without_activity_timestamps_make_progress() {
    concurrent_writers_make_progress(true).await;
}

async fn concurrent_writers_make_progress(disable_activity_tracking: bool) {
    use std::collections::VecDeque;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;
    // Exercise the incremental frontier, not a source-locking FULL fallback.
    db.alter_system_set_and_wait(
        "pg_trickle.refresh_strategy",
        "'differential'",
        "differential",
    )
    .await;
    db.execute("CREATE TABLE busy_orders (id SERIAL PRIMARY KEY, region TEXT NOT NULL, amount NUMERIC NOT NULL)").await;
    db.execute("INSERT INTO busy_orders (region, amount) VALUES ('US',100),('EU',200),('US',300),('APAC',50)").await;
    let query = "SELECT region, SUM(amount) AS total, COUNT(*) AS order_count FROM busy_orders GROUP BY region";
    db.create_st("busy_revenue", query, "1s", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("busy_revenue", query).await;

    // Eight open writers plus one handoff connection. Begin the replacement
    // before committing its predecessor so the probe never gets an accidental
    // writer-free interval. Every individual transaction remains short-lived.
    let writers = sqlx::postgres::PgPoolOptions::new()
        .max_connections(9)
        .after_connect(move |conn, _| {
            Box::pin(async move {
                sqlx::query(if disable_activity_tracking {
                    "SET track_activities = off"
                } else {
                    "SET track_activities = on"
                })
                .execute(&mut *conn)
                .await?;
                sqlx::query("SET statement_timeout = '10s'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(db.connection_string())
        .await
        .unwrap();
    let mut open = VecDeque::new();
    for _ in 0..8 {
        let mut txn = writers.begin().await.unwrap();
        sqlx::query("INSERT INTO busy_orders (region, amount) VALUES ('US',999)")
            .execute(&mut *txn)
            .await
            .unwrap();
        open.push_back(txn);
    }
    if disable_activity_tracking {
        let missing: i64 = db.query_scalar(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND backend_xid IS NOT NULL AND xact_start IS NULL"
        ).await;
        assert!(
            missing >= 8,
            "fixture must expose active XIDs with NULL ages"
        );
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = stop.clone();
    let load = tokio::spawn(async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        let mut committed = 0_i64;
        while !writer_stop.load(Ordering::Relaxed) && std::time::Instant::now() < deadline {
            let mut next = writers.begin().await.unwrap();
            sqlx::query("INSERT INTO busy_orders (region, amount) VALUES ('US',999)")
                .execute(&mut *next)
                .await
                .unwrap();
            open.pop_front().unwrap().commit().await.unwrap();
            committed += 1;
            open.push_back(next);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for txn in open {
            txn.commit().await.unwrap();
            committed += 1;
        }
        writers.close().await;
        committed
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut last_count = 2_i64;
    let mut advances = 0;
    while std::time::Instant::now() < deadline && !load.is_finished() && advances < 3 {
        let count: i64 = db
            .query_scalar("SELECT order_count FROM busy_revenue WHERE region = 'US'")
            .await;
        if count > last_count {
            advances += 1;
            last_count = count;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let progressed_during_load = advances >= 3 && !load.is_finished();
    stop.store(true, Ordering::Relaxed);
    let committed = load.await.expect("writer task must succeed");
    assert!(
        progressed_during_load,
        "scheduled SUM/COUNT must advance repeatedly while writers remain active; advances={advances}, last_count={last_count}, committed={committed}"
    );
    assert!(db.wait_for_condition(
        "all concurrent inserts consumed",
        "SELECT (SELECT SUM(order_count) FROM busy_revenue) = (SELECT COUNT(*) FROM busy_orders)",
        Duration::from_secs(60), Duration::from_millis(100),
    ).await, "scheduler must consume every committed insert");
    db.assert_st_matches_query("busy_revenue", query).await;
}

// ── Shared setup helper ────────────────────────────────────────────────────

/// Set up a fast scheduler (100 ms tick, 1 s minimum schedule) and wait for
/// the pg_trickle scheduler BGW to appear in pg_stat_activity.
async fn configure_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    // Force trigger-based CDC so the holdback logic is exercised.
    // WAL CDC (the default when wal_level=logical) is already immune
    // to this race and would make these tests pass trivially.
    db.execute("ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger'")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;
    db.wait_for_setting("pg_trickle.cdc_mode", "trigger").await;

    let ok = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(ok, "pg_trickle scheduler did not start within 90 s");
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Verify that the holdback GUCs are registered with the correct defaults.
#[tokio::test]
async fn test_holdback_gucs_registered() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    let mode = db.show_setting("pg_trickle.frontier_holdback_mode").await;
    assert_eq!(
        mode, "xmin",
        "frontier_holdback_mode default should be 'xmin'"
    );

    let warn_secs = db
        .show_setting("pg_trickle.frontier_holdback_warn_seconds")
        .await;
    assert_eq!(
        warn_secs, "60",
        "frontier_holdback_warn_seconds default should be '60'"
    );
}

/// Verify that a READ COMMITTED transaction straddling a scheduler tick
/// does NOT cause its row to be permanently skipped.
///
/// Scenario:
/// 1. Begin transaction A on a second connection, insert a row (CDC records
///    the change at some LSN X).  Do NOT commit.
/// 2. Wait for 3+ scheduler ticks (300+ ms with 100 ms interval).
///    With holdback enabled, the scheduler should NOT advance the frontier
///    past X because transaction A is still in-progress.
/// 3. Commit transaction A.
/// 4. Wait for the next tick to consume the row.
/// 5. Assert the stream table contains the inserted row.
#[tokio::test]
async fn test_holdback_read_committed_long_txn() {
    read_committed_long_txn(false).await;
}

#[tokio::test]
async fn test_holdback_unknown_age_writer_preserves_late_commit() {
    read_committed_long_txn(true).await;
}

async fn read_committed_long_txn(disable_activity_tracking: bool) {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Ensure holdback mode is enabled (the default).
    db.execute("ALTER SYSTEM SET pg_trickle.frontier_holdback_mode = 'xmin'")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.frontier_holdback_mode", "xmin")
        .await;

    // Create source table + stream table.
    db.execute("CREATE TABLE ltv_rc_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO ltv_rc_src VALUES (1, 'initial')")
        .await;
    db.create_st(
        "ltv_rc_st",
        "SELECT id, val FROM ltv_rc_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Wait for the ST to be initialized (populated with initial row).
    let ok = db
        .wait_for_condition(
            "ltv_rc_st initial population",
            "SELECT is_populated FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ltv_rc_st'",
            Duration::from_secs(30),
            Duration::from_millis(200),
        )
        .await;
    assert!(ok, "ST should be populated within 30 s");
    assert_eq!(db.count("public.ltv_rc_st").await, 1);

    // --- Phase 1: begin transaction, insert row (do NOT commit yet) ---
    let pool = db.pool.clone();
    let mut long_txn_conn = pool.acquire().await.expect("acquire long-txn connection");
    if disable_activity_tracking {
        sqlx::query("SET track_activities = off")
            .execute(&mut *long_txn_conn)
            .await
            .unwrap();
    }
    sqlx::query("BEGIN")
        .execute(&mut *long_txn_conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO ltv_rc_src VALUES (2, 'long_txn_row')")
        .execute(&mut *long_txn_conn)
        .await
        .unwrap();

    // --- Phase 2: wait for 3+ scheduler ticks while the txn is open ---
    // With frontier_holdback_mode = 'xmin', the scheduler must NOT advance
    // the frontier past the row's LSN.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stream table should still have only 1 row (txn not committed).
    let count_before_commit: i64 = db.count("public.ltv_rc_st").await;
    assert_eq!(
        count_before_commit, 1,
        "uncommitted row must not appear in stream table"
    );

    // --- Phase 3: commit the transaction ---
    sqlx::query("COMMIT")
        .execute(&mut *long_txn_conn)
        .await
        .unwrap();
    drop(long_txn_conn);

    // --- Phase 4: wait for the row to appear ---
    let ok = db
        .wait_for_condition(
            "ltv_rc_st long-txn row",
            "SELECT count(*) = 2 FROM public.ltv_rc_st",
            Duration::from_secs(15),
            Duration::from_millis(200),
        )
        .await;
    assert!(
        ok,
        "committed row from long-running READ COMMITTED transaction \
         must appear in stream table within 15 s (holdback must not lose it)"
    );
    assert_eq!(db.count("public.ltv_rc_st").await, 2);
}

/// Same as the READ COMMITTED test but uses REPEATABLE READ isolation,
/// which holds the snapshot open for longer and exercises the xmin-tracking
/// code path across multiple ticks.
#[tokio::test]
async fn test_holdback_repeatable_read_long_txn() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    db.execute("ALTER SYSTEM SET pg_trickle.frontier_holdback_mode = 'xmin'")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.frontier_holdback_mode", "xmin")
        .await;

    db.execute("CREATE TABLE ltv_rr_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO ltv_rr_src VALUES (1, 'initial')")
        .await;
    db.create_st(
        "ltv_rr_st",
        "SELECT id, val FROM ltv_rr_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let ok = db
        .wait_for_condition(
            "ltv_rr_st initial population",
            "SELECT is_populated FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ltv_rr_st'",
            Duration::from_secs(30),
            Duration::from_millis(200),
        )
        .await;
    assert!(ok, "ST should be populated within 30 s");
    assert_eq!(db.count("public.ltv_rr_st").await, 1);

    // Open REPEATABLE READ transaction — holds xmin open for the full duration.
    let pool = db.pool.clone();
    let mut long_txn_conn = pool.acquire().await.expect("acquire long-txn connection");
    sqlx::query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *long_txn_conn)
        .await
        .unwrap();
    // Touch the table to materialise the xmin.
    sqlx::query("SELECT count(*) FROM ltv_rr_src")
        .execute(&mut *long_txn_conn)
        .await
        .unwrap();
    // On the MAIN connection, insert the row that the long txn would cause to race.
    db.execute("INSERT INTO ltv_rr_src VALUES (2, 'rr_race_row')")
        .await;

    // Let 4+ ticks pass with the RR transaction still holding its snapshot.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Commit the REPEATABLE READ transaction (no DML needed; just releasing the xmin).
    sqlx::query("COMMIT")
        .execute(&mut *long_txn_conn)
        .await
        .unwrap();
    drop(long_txn_conn);

    // Row inserted on the main connection should appear within a few ticks.
    let ok = db
        .wait_for_condition(
            "ltv_rr_st race row",
            "SELECT count(*) = 2 FROM public.ltv_rr_st",
            Duration::from_secs(15),
            Duration::from_millis(200),
        )
        .await;
    assert!(
        ok,
        "row inserted while REPEATABLE READ transaction held xmin \
         must appear in stream table after txn commits"
    );
}

/// Verify that a 2PC PREPARE TRANSACTION spanning many ticks does not
/// cause data loss once the transaction is committed.
///
/// Requires `max_prepared_transactions > 0` (set via ALTER SYSTEM).
#[tokio::test]
async fn test_holdback_prepared_transaction() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    // 2PC requires max_prepared_transactions > 0; restart-safe setting.
    db.execute("ALTER SYSTEM SET max_prepared_transactions = 10")
        .await;
    // Need a server restart — use pg_reload_conf for GUC-level changes
    // (max_prepared_transactions is a postmaster GUC, so we skip if not
    // effective; the test will be a no-op if 2PC is unavailable).
    db.reload_config_and_wait().await;

    let mpt: i32 = db
        .query_scalar("SELECT current_setting('max_prepared_transactions')::int")
        .await;
    if mpt == 0 {
        // Skip: max_prepared_transactions requires a server restart to take
        // effect and cannot be changed online. This test is a best-effort
        // check; full validation is done in the nightly soak suite.
        eprintln!(
            "test_holdback_prepared_transaction: skipping — \
             max_prepared_transactions = 0 (requires server restart to change)"
        );
        return;
    }

    configure_fast_scheduler(&db).await;
    db.execute("ALTER SYSTEM SET pg_trickle.frontier_holdback_mode = 'xmin'")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.frontier_holdback_mode", "xmin")
        .await;

    db.execute("CREATE TABLE ltv_2pc_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO ltv_2pc_src VALUES (1, 'initial')")
        .await;
    db.create_st(
        "ltv_2pc_st",
        "SELECT id, val FROM ltv_2pc_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let ok = db
        .wait_for_condition(
            "ltv_2pc_st initial population",
            "SELECT is_populated FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ltv_2pc_st'",
            Duration::from_secs(30),
            Duration::from_millis(200),
        )
        .await;
    assert!(ok, "ST should be populated within 30 s");
    assert_eq!(db.count("public.ltv_2pc_st").await, 1);

    // Prepare a 2PC transaction — this holds xmin in pg_prepared_xacts.
    let pool = db.pool.clone();
    let mut prep_conn = pool.acquire().await.expect("acquire 2pc connection");
    sqlx::query("BEGIN").execute(&mut *prep_conn).await.unwrap();
    sqlx::query("INSERT INTO ltv_2pc_src VALUES (2, '2pc_row')")
        .execute(&mut *prep_conn)
        .await
        .unwrap();
    sqlx::query("PREPARE TRANSACTION 'ltv_holdback_test_2pc'")
        .execute(&mut *prep_conn)
        .await
        .unwrap();
    drop(prep_conn);

    // Wait for several scheduler ticks while the prepared transaction holds xmin.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Commit the prepared transaction.
    db.execute("COMMIT PREPARED 'ltv_holdback_test_2pc'").await;

    // The row should appear after the next tick.
    let ok = db
        .wait_for_condition(
            "ltv_2pc_st committed row",
            "SELECT count(*) = 2 FROM public.ltv_2pc_st",
            Duration::from_secs(15),
            Duration::from_millis(200),
        )
        .await;
    assert!(
        ok,
        "row from COMMIT PREPARED transaction must appear in stream table \
         after the 2PC transaction commits"
    );
}

/// Regression guard: with `frontier_holdback_mode = 'none'`, the bug
/// described in Issue #536 (silent data loss) can still occur.
///
/// This test demonstrates the unsafe behaviour so the fix can be compared
/// against the baseline:
/// - With `mode = 'none'`, a long-running transaction that spans a tick
///   causes its change-buffer row to be silently skipped.
///
/// **This test is expected to detect data loss and FAIL when holdback is
/// working.**  It is kept as a regression guard to verify that `mode = 'none'`
/// truly reverts to the old unsafe behaviour (the unsafe escape hatch must
/// remain unsafe so that benchmark operators know what they're opting into).
///
/// The test is marked `#[ignore]` so it does not run in normal CI.  Run it
/// explicitly to confirm the unsafe mode still behaves as documented:
///
/// ```bash
/// cargo test --test e2e_long_txn_visibility_tests test_holdback_none_mode_regression_guard -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore]
async fn test_holdback_none_mode_regression_guard() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Disable holdback — revert to the pre-fix unsafe behaviour.
    db.execute("ALTER SYSTEM SET pg_trickle.frontier_holdback_mode = 'none'")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.frontier_holdback_mode", "none")
        .await;

    db.execute("CREATE TABLE ltv_none_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO ltv_none_src VALUES (1, 'initial')")
        .await;
    db.create_st(
        "ltv_none_st",
        "SELECT id, val FROM ltv_none_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let ok = db
        .wait_for_condition(
            "ltv_none_st initial population",
            "SELECT is_populated FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ltv_none_st'",
            Duration::from_secs(30),
            Duration::from_millis(200),
        )
        .await;
    assert!(ok, "ST should be populated within 30 s");
    assert_eq!(db.count("public.ltv_none_st").await, 1);

    // Begin transaction, insert row (CDC fires, records LSN X), hold open.
    let pool = db.pool.clone();
    let mut long_txn_conn = pool.acquire().await.expect("acquire long-txn connection");
    sqlx::query("BEGIN")
        .execute(&mut *long_txn_conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO ltv_none_src VALUES (2, 'lost_row')")
        .execute(&mut *long_txn_conn)
        .await
        .unwrap();

    // With mode = 'none', the scheduler fires multiple ticks here and
    // advances the frontier PAST the row's LSN (the row is still uncommitted
    // so MVCC hides it from the delta query).
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Commit the transaction.
    sqlx::query("COMMIT")
        .execute(&mut *long_txn_conn)
        .await
        .unwrap();
    drop(long_txn_conn);

    // Wait for a couple more ticks.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // With mode = 'none', the row may be permanently skipped (data loss).
    // This assertion documents the UNSAFE behaviour — it may pass OR fail
    // depending on exact timing, but it demonstrates the risk.
    let final_count: i64 = db.count("public.ltv_none_st").await;
    // If the row was lost (expected with mode=none), count = 1.
    // If timing was lucky and no tick happened during the window, count = 2.
    eprintln!(
        "test_holdback_none_mode_regression_guard: final row count = {final_count} \
         (expected 1 with data loss, 2 if timing allowed capture)"
    );
    // We don't hard-assert here because the race is timing-dependent.
    // The intent is to show operators what 'none' mode implies.
}
