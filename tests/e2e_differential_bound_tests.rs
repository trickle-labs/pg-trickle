//! Regression for #1437: the CDC upper LSN and every live source read
//! must represent the same logical input boundary.
//!
//! The existing input_boundary test barrier pauses a manual refresh *after*
//! it chooses the new frontier and *before* its staged CDC / MERGE. Writers
//! commit in that gap. The first refresh must exclude those changes, and
//! subsequent bounded refreshes must converge without FULL fallback.
//!
//! Run: cargo test --test e2e_differential_bound_tests -- --nocapture

mod e2e;

use e2e::E2eDb;
use sqlx::{PgPool, Postgres, pool::PoolConnection};
use std::time::Duration;

const INPUT_BOUNDARY: i32 = 1;

async fn refresh_with_optional_barrier(pool: &PgPool, st: &'static str, barrier: bool) {
    let mut connection = pool.acquire().await.expect("acquire refresh session");
    sqlx::query("SET pg_trickle.refresh_strategy = 'differential'")
        .execute(&mut *connection)
        .await
        .expect("force differential strategy");
    if st == "bound_poll_st" {
        sqlx::query("SET pg_trickle.matview_polling = on")
            .execute(&mut *connection)
            .await
            .expect("enable materialized-view polling in refresh session");
    }
    if barrier {
        sqlx::query("SET pg_trickle.test_chaos_phase = 'input_boundary'")
            .execute(&mut *connection)
            .await
            .expect("set input boundary test phase");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET pg_trickle.test_chaos_for_table = '{st}'"
        )))
        .execute(&mut *connection)
        .await
        .expect("enable input boundary test barrier");
    }
    // Keep the transaction open after the function returns so a regression
    // can inspect the exact pg_temp bounded source used by its MERGE.
    if barrier {
        sqlx::query("BEGIN")
            .execute(&mut *connection)
            .await
            .expect("begin inspectable bounded refresh");
    }
    sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
        .bind(st)
        .execute(&mut *connection)
        .await
        .expect("differential refresh");

    if matches!(st, "bound_delete_st" | "bound_update_st") && barrier {
        let stage: String = sqlx::query_scalar(
            "SELECT relname::text FROM pg_catalog.pg_class
             WHERE relnamespace = pg_my_temp_schema()
               AND relname LIKE '__pgt_bound_%'
             ORDER BY relname LIMIT 1",
        )
        .fetch_one(&mut *connection)
        .await
        .expect("find frontier-bounded source stage");
        let stages: Vec<(i32, i32)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id, v FROM pg_temp.\"{stage}\" WHERE id = 27",
        )))
        .fetch_all(&mut *connection)
        .await
        .expect("inspect bounded source row");
        assert_eq!(
            stages,
            vec![(27, 27)],
            "post-bound D image must restore the pre-frontier row in {stage}",
        );
    }
    if barrier {
        sqlx::query("COMMIT")
            .execute(&mut *connection)
            .await
            .expect("commit inspected bounded refresh");
    }
}

async fn pgt_id(db: &E2eDb, st: &'static str) -> i32 {
    let id: i64 =
        sqlx::query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1")
            .bind(st)
            .fetch_one(&db.pool)
            .await
            .expect("read stream pgt id");
    i32::try_from(id).expect("fixture pgt id fits advisory lock key")
}

async fn hold_boundary(db: &E2eDb, id: i32) -> (PoolConnection<Postgres>, i32) {
    let mut connection = db.pool.acquire().await.expect("acquire barrier connection");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *connection)
        .await
        .expect("read blocker backend pid");
    sqlx::query("SELECT pg_advisory_lock($1, $2)")
        .bind(id)
        .bind(INPUT_BOUNDARY)
        .execute(&mut *connection)
        .await
        .expect("hold input boundary");
    (connection, pid)
}

async fn wait_for_input_boundary(db: &E2eDb, id: i32, blocker_pid: i32) {
    let pattern = format!("pgt_atomicity:{id}:input_boundary:%");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let observed: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity a \
                 WHERE a.application_name LIKE $1 AND a.wait_event_type = 'Lock' \
                   AND $2 = ANY(pg_blocking_pids(a.pid)))",
            )
            .bind(&pattern)
            .bind(blocker_pid)
            .fetch_one(&db.pool)
            .await
            .expect("inspect advisory barrier");
            if observed {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("manual refresh did not stop after its input LSN bound");
}

async fn release_boundary(connection: &mut PoolConnection<Postgres>, id: i32) {
    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1, $2)")
        .bind(id)
        .bind(INPUT_BOUNDARY)
        .fetch_one(&mut **connection)
        .await
        .expect("release input boundary");
    assert!(released, "advisory barrier must have been held");
}

async fn start_bounded_refresh(
    db: &E2eDb,
    st: &'static str,
) -> (PoolConnection<Postgres>, i32, tokio::task::JoinHandle<()>) {
    let id = pgt_id(db, st).await;
    let (blocker, pid) = hold_boundary(db, id).await;
    let pool = db.pool.clone();
    let task = tokio::spawn(async move {
        refresh_with_optional_barrier(&pool, st, true).await;
    });
    wait_for_input_boundary(db, id, pid).await;
    (blocker, id, task)
}

async fn finish_bounded_refresh(
    blocker: &mut PoolConnection<Postgres>,
    id: i32,
    task: tokio::task::JoinHandle<()>,
) {
    release_boundary(blocker, id).await;
    tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .expect("bounded refresh timed out")
        .expect("bounded refresh task panicked");
}

async fn refresh_normally(db: &E2eDb, st: &'static str) {
    refresh_with_optional_barrier(&db.pool, st, false).await;
}

async fn assert_no_full_fallback(db: &E2eDb, st: &'static str) {
    let fallbacks: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_refresh_history \
         WHERE was_full_fallback = true \
           AND pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1)",
    )
    .bind(st)
    .fetch_one(&db.pool)
    .await
    .expect("read refresh history");
    assert_eq!(fallbacks, 0, "race must not be hidden by a FULL fallback");
}

async fn extremum_case(minimum: bool) {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE bound_src(id int PRIMARY KEY, g int, v numeric)")
        .await;
    db.execute("INSERT INTO bound_src SELECT i, i % 3, i FROM generate_series(1, 30) i")
        .await;
    let function = if minimum { "min" } else { "max" };
    let query = format!(
        "SELECT g, {function}(v) AS ex, sum(v) AS s, count(*) AS n \
         FROM bound_src GROUP BY g"
    );
    db.create_st("bound_ext_st", &query, "1h", "DIFFERENTIAL")
        .await;
    // First window deletes a real old extremum (the rescan is required).
    if minimum {
        db.execute("DELETE FROM bound_src WHERE id = 3").await;
    } else {
        db.execute("DELETE FROM bound_src WHERE id = 30").await;
    }

    let (mut blocker, id, refresh) = start_bounded_refresh(&db, "bound_ext_st").await;
    // Committed after the new frontier but before DVM executes.
    let future = if minimum { -1000 } else { 1000 };
    sqlx::query("INSERT INTO bound_src VALUES (9999, 0, $1)")
        .bind(future)
        .execute(&db.pool)
        .await
        .expect("commit future extreme");
    finish_bounded_refresh(&mut blocker, id, refresh).await;

    // Even though the source now contains a future extreme, this materialized
    // result corresponds to the earlier NEW frontier, not the live source.
    let ex: i64 = sqlx::query_scalar("SELECT ex::bigint FROM bound_ext_st WHERE g = 0")
        .fetch_one(&db.pool)
        .await
        .expect("read bounded extreme");
    assert_eq!(ex, if minimum { 6 } else { 27 });

    // In the next window that temporary row is both inserted and deleted.
    // No further delta will repair a wrongly included extremum.
    db.execute("DELETE FROM bound_src WHERE id = 9999").await;
    refresh_normally(&db, "bound_ext_st").await;
    refresh_normally(&db, "bound_ext_st").await;
    db.assert_st_matches_query("bound_ext_st", &query).await;
    assert_no_full_fallback(&db, "bound_ext_st").await;
}

#[tokio::test]
async fn max_excludes_future_insert_and_converges() {
    extremum_case(false).await;
}

#[tokio::test]
async fn min_excludes_future_insert_and_converges() {
    extremum_case(true).await;
}

#[tokio::test]
async fn max_restores_future_deleted_row_in_current_window() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE bound_del_src(id int PRIMARY KEY, g int, v int)")
        .await;
    db.execute("INSERT INTO bound_del_src SELECT i, i % 3, i FROM generate_series(1, 30) i")
        .await;
    let query = "SELECT g, max(v) AS ex, sum(v) AS s, count(*) AS n \
                 FROM bound_del_src GROUP BY g";
    db.create_st("bound_delete_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("DELETE FROM bound_del_src WHERE id = 30").await;
    let (mut blocker, id, refresh) = start_bounded_refresh(&db, "bound_delete_st").await;

    // The next largest value is deleted *after* the new frontier. A bounded
    // rescan must restore its D image even though the live table lacks it.
    db.execute("DELETE FROM bound_del_src WHERE id = 27").await;
    finish_bounded_refresh(&mut blocker, id, refresh).await;
    let ex: i32 = sqlx::query_scalar("SELECT ex FROM bound_delete_st WHERE g = 0")
        .fetch_one(&db.pool)
        .await
        .expect("read bounded max after future delete");
    assert_eq!(ex, 27);

    refresh_normally(&db, "bound_delete_st").await;
    db.assert_st_matches_query("bound_delete_st", query).await;
    assert_no_full_fallback(&db, "bound_delete_st").await;
}

#[tokio::test]
async fn max_undoes_future_update_before_replaying_it() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE bound_upd_src(id int PRIMARY KEY, g int, v int)")
        .await;
    db.execute("INSERT INTO bound_upd_src SELECT i, i % 3, i FROM generate_series(1, 30) i")
        .await;
    let query = "SELECT g, max(v) AS ex, sum(v) AS s, count(*) AS n \
                 FROM bound_upd_src GROUP BY g";
    db.create_st("bound_update_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("DELETE FROM bound_upd_src WHERE id = 30").await;
    let (mut blocker, id, refresh) = start_bounded_refresh(&db, "bound_update_st").await;
    db.execute("UPDATE bound_upd_src SET v = 1000 WHERE id = 27")
        .await;
    finish_bounded_refresh(&mut blocker, id, refresh).await;
    let max_before: i32 = sqlx::query_scalar("SELECT ex FROM bound_update_st WHERE g = 0")
        .fetch_one(&db.pool)
        .await
        .expect("read as-of max before update");
    assert_eq!(max_before, 27);
    refresh_normally(&db, "bound_update_st").await;
    db.assert_st_matches_query("bound_update_st", query).await;
    let max_after: i32 = sqlx::query_scalar("SELECT ex FROM bound_update_st WHERE g = 0")
        .fetch_one(&db.pool)
        .await
        .expect("read max after update replay");
    assert_eq!(max_after, 1000);
    assert_no_full_fallback(&db, "bound_update_st").await;
}

#[tokio::test]
async fn join_counts_future_partner_only_in_next_window() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE bound_a(id int PRIMARY KEY, k int, g int)")
        .await;
    db.execute("CREATE TABLE bound_b(id int PRIMARY KEY, k int, w int)")
        .await;
    db.execute("INSERT INTO bound_a SELECT i, i, i % 3 FROM generate_series(1, 30) i")
        .await;
    db.execute("INSERT INTO bound_b SELECT i, i, 1 FROM generate_series(1, 30) i")
        .await;
    let query = "SELECT a.g, count(*) AS n, sum(b.w) AS sw \
                 FROM bound_a a JOIN bound_b b ON a.k = b.k GROUP BY a.g";
    db.create_st("bound_join_st", query, "1h", "DIFFERENTIAL")
        .await;

    db.execute("INSERT INTO bound_a VALUES (100, 100, 0)").await;
    let (mut blocker, id, refresh) = start_bounded_refresh(&db, "bound_join_st").await;
    db.execute("INSERT INTO bound_b VALUES (100, 100, 5)").await;
    finish_bounded_refresh(&mut blocker, id, refresh).await;

    // A had no partner at new_bound. Its partner is not counted early.
    let before: (i64, i64) = sqlx::query_as("SELECT n, sw::bigint FROM bound_join_st WHERE g = 0")
        .fetch_one(&db.pool)
        .await
        .expect("read as-of join");
    assert_eq!(before, (10, 10));

    refresh_normally(&db, "bound_join_st").await;
    refresh_normally(&db, "bound_join_st").await;
    let after: (i64, i64) = sqlx::query_as("SELECT n, sw::bigint FROM bound_join_st WHERE g = 0")
        .fetch_one(&db.pool)
        .await
        .expect("read final join");
    assert_eq!(after, (11, 15));
    db.assert_st_matches_query("bound_join_st", query).await;
    assert_no_full_fallback(&db, "bound_join_st").await;
}

#[tokio::test]
async fn exists_grouped_count_excludes_future_partner_until_next_window() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE bound_q04_orders(\
           o_orderkey int PRIMARY KEY, o_orderdate date NOT NULL, o_orderpriority text NOT NULL)",
    )
    .await;
    db.execute(
        "CREATE TABLE bound_q04_lineitem(\
           l_orderkey int NOT NULL, l_commitdate date NOT NULL, l_receiptdate date NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO bound_q04_orders VALUES \
                (1, DATE '1993-08-01', '1-URGENT'), \
                (2, DATE '1993-08-01', '1-URGENT')",
    )
    .await;
    db.execute(
        "INSERT INTO bound_q04_lineitem VALUES \
                (1, DATE '1993-09-01', DATE '1993-09-02')",
    )
    .await;

    let query = "SELECT o.o_orderpriority, count(*) AS order_count \
                 FROM bound_q04_orders o \
                 WHERE o.o_orderdate >= DATE '1993-07-01' \
                   AND o.o_orderdate < DATE '1993-07-01' + INTERVAL '3 months' \
                   AND EXISTS (SELECT 1 FROM bound_q04_lineitem l \
                               WHERE l.l_orderkey = o.o_orderkey \
                                 AND l.l_commitdate < l.l_receiptdate) \
                 GROUP BY o.o_orderpriority";
    db.create_st("bound_q04_st", query, "1h", "DIFFERENTIAL")
        .await;

    // The new order is in this window, but its matching EXISTS partner is not.
    db.execute(
        "INSERT INTO bound_q04_orders VALUES \
                (3, DATE '1993-08-01', '1-URGENT')",
    )
    .await;
    let (mut blocker, id, refresh) = start_bounded_refresh(&db, "bound_q04_st").await;
    db.execute(
        "INSERT INTO bound_q04_lineitem VALUES \
                (3, DATE '1993-09-01', DATE '1993-09-02')",
    )
    .await;
    finish_bounded_refresh(&mut blocker, id, refresh).await;

    let before: i64 = sqlx::query_scalar(
        "SELECT order_count FROM bound_q04_st WHERE o_orderpriority = '1-URGENT'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read q04 count at the captured frontier");
    assert_eq!(before, 1, "future EXISTS partner must not count early");

    refresh_normally(&db, "bound_q04_st").await;
    refresh_normally(&db, "bound_q04_st").await;
    let after: i64 = sqlx::query_scalar(
        "SELECT order_count FROM bound_q04_st WHERE o_orderpriority = '1-URGENT'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read q04 count after replaying the future partner");
    assert_eq!(after, 2);
    db.assert_st_matches_query("bound_q04_st", query).await;
    assert_no_full_fallback(&db, "bound_q04_st").await;
}

// Regression for #1453: a normal DIFFERENTIAL refresh must ANALYZE the
// materialized CDC window read by the delta query, even without planner hints.
// Inspect the staged table before COMMIT (it is ON COMMIT DROP). The negative
// control also verifies that analyze_before_delta = off still disables ANALYZE.
async fn inspect_issue_1453_stage_statistics(db: &E2eDb, analyze: bool) {
    let mut tx = db
        .pool
        .begin()
        .await
        .expect("begin inspectable CDC refresh");
    sqlx::query("SET LOCAL pg_trickle.refresh_strategy = 'differential'")
        .execute(&mut *tx)
        .await
        .expect("force differential refresh");
    sqlx::query("SET LOCAL pg_trickle.planner_aggressive = off")
        .execute(&mut *tx)
        .await
        .expect("disable planner hints");
    sqlx::query(if analyze {
        "SET LOCAL pg_trickle.analyze_before_delta = on"
    } else {
        "SET LOCAL pg_trickle.analyze_before_delta = off"
    })
    .execute(&mut *tx)
    .await
    .expect("configure pre-delta ANALYZE");

    sqlx::query("SELECT pgtrickle.refresh_stream_table('analyze1453_hourly')")
        .execute(&mut *tx)
        .await
        .expect("refresh staged CDC aggregate");

    let (stage_name, lsn_stats): (String, i64) = sqlx::query_as(
        "SELECT c.relname::text,
                (SELECT count(*)::bigint
                 FROM pg_catalog.pg_statistic s
                 JOIN pg_catalog.pg_attribute a
                   ON a.attrelid = s.starelid AND a.attnum = s.staattnum
                 WHERE s.starelid = c.oid AND a.attname = 'lsn')
         FROM pg_catalog.pg_class c
         WHERE c.relnamespace = pg_catalog.pg_my_temp_schema()
           AND c.relname LIKE '__pgt_cdc_%'",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("find the ordinary refresh's staged CDC table");

    assert!(stage_name.starts_with("__pgt_cdc_"));
    let window_rows: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*)::bigint FROM pg_temp.\"{stage_name}\""
    )))
    .fetch_one(&mut *tx)
    .await
    .expect("count the bounded CDC rows");
    assert!(window_rows > 0, "test must exercise a nonempty CDC window");

    assert_eq!(
        lsn_stats > 0,
        analyze,
        "staged table {stage_name} ({window_rows} rows) has {lsn_stats} lsn statistics"
    );
    let action: String = sqlx::query_scalar(
        "SELECT h.action::text
         FROM pgtrickle.pgt_refresh_history h
         JOIN pgtrickle.pgt_stream_tables t USING (pgt_id)
         WHERE t.pgt_name = 'analyze1453_hourly'
         ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("read refresh action");
    assert_eq!(
        action, "DIFFERENTIAL",
        "the regression must not fall back to FULL"
    );

    tx.commit().await.expect("commit inspected refresh");
}

#[tokio::test]
async fn test_issue_1453_staged_cdc_analyzed_in_ordinary_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE analyze1453_usage (
            request_id text NOT NULL,
            event_day date NOT NULL,
            occurred_at timestamptz NOT NULL,
            scope_path text NOT NULL,
            meter text NOT NULL,
            value numeric NOT NULL,
            PRIMARY KEY (request_id, meter, event_day)
         ) PARTITION BY RANGE (event_day)",
    )
    .await;
    db.execute(
        "CREATE TABLE analyze1453_usage_all PARTITION OF analyze1453_usage
         FOR VALUES FROM ('2020-01-01') TO ('2100-01-01')",
    )
    .await;
    let query = "SELECT meter, scope_path,
                        date_trunc('hour', occurred_at AT TIME ZONE 'UTC') AS hour,
                        sum(value) AS quantity, count(*) AS events
                 FROM analyze1453_usage
                 GROUP BY meter, scope_path,
                          date_trunc('hour', occurred_at AT TIME ZONE 'UTC')";
    db.create_st("analyze1453_hourly", query, "1h", "DIFFERENTIAL")
        .await;
    #[cfg(not(feature = "light-e2e"))]
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.analyze1453_hourly'])")
        .await;

    db.execute(
        "INSERT INTO analyze1453_usage
         SELECT 'on-' || g, DATE '2025-01-01',
                TIMESTAMPTZ '2025-01-01 10:00+00',
                'scope-' || (g % 20), 'm' || (g % 5), (1 + g % 5)::numeric
         FROM generate_series(1, 1200) g",
    )
    .await;
    inspect_issue_1453_stage_statistics(&db, true).await;
    db.refresh_st_until_converged("analyze1453_hourly").await;
    db.assert_st_matches_query("analyze1453_hourly", query)
        .await;

    // A new CDC copy must not inherit statistics from the previous window.
    db.execute(
        "INSERT INTO analyze1453_usage
         SELECT 'off-' || g, DATE '2025-01-01',
                TIMESTAMPTZ '2025-01-01 10:00+00',
                'scope-' || (g % 20), 'm' || (g % 5), (1 + g % 5)::numeric
         FROM generate_series(1, 120) g",
    )
    .await;
    inspect_issue_1453_stage_statistics(&db, false).await;
    db.refresh_st_until_converged("analyze1453_hourly").await;
    db.assert_st_matches_query("analyze1453_hourly", query)
        .await;
}

#[tokio::test]
async fn polling_rescan_uses_the_captured_matview_image() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE bound_poll_src(id int PRIMARY KEY, v int NOT NULL)")
        .await;
    db.execute("INSERT INTO bound_poll_src VALUES (1, 10), (2, 20), (3, 30)")
        .await;
    db.execute("CREATE MATERIALIZED VIEW bound_poll_mv AS SELECT * FROM bound_poll_src")
        .await;
    db.execute("CREATE UNIQUE INDEX bound_poll_mv_id ON bound_poll_mv(id)")
        .await;
    let query = "SELECT max(v) AS ex, count(*) AS n FROM bound_poll_mv";
    db.try_execute_with_config(
        &["SET pg_trickle.matview_polling = on"],
        &format!(
            "SELECT pgtrickle.create_stream_table('bound_poll_st', $${query}$$, \
             '1h', 'DIFFERENTIAL')"
        ),
    )
    .await
    .expect("create stream table with materialized-view polling enabled");
    db.execute("DELETE FROM bound_poll_src WHERE id = 3").await;
    db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY bound_poll_mv")
        .await;
    let (mut blocker, id, refresh) = start_bounded_refresh(&db, "bound_poll_st").await;
    // The local poll has completed. A new matview image is visible, but its
    // insert has not been published to polling CDC.
    db.execute("INSERT INTO bound_poll_src VALUES (4, 1000)")
        .await;
    db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY bound_poll_mv")
        .await;
    finish_bounded_refresh(&mut blocker, id, refresh).await;
    let (ex, n): (i32, i64) = sqlx::query_as("SELECT ex, n FROM bound_poll_st")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!((ex, n), (20, 2), "rescan must read the durable poll image");
    refresh_normally(&db, "bound_poll_st").await;
    db.assert_st_matches_query("bound_poll_st", query).await;
    assert_no_full_fallback(&db, "bound_poll_st").await;
}

#[tokio::test]
async fn phantom_reconciliation_does_not_publish_rows_past_its_cut() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE bound_recon_left(k int, v int)")
        .await;
    db.execute("CREATE TABLE bound_recon_right(k int, v int)")
        .await;
    db.execute("INSERT INTO bound_recon_left VALUES (1, 10), (1, 10)")
        .await;
    db.execute("INSERT INTO bound_recon_right VALUES (1, 20)")
        .await;
    let query = "SELECT l.k, l.v AS lv, r.v AS rv FROM bound_recon_left l JOIN bound_recon_right r ON l.k = r.k";
    db.create_st("bound_recon_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("INSERT INTO bound_recon_left VALUES (1, 11)")
        .await;
    let (mut blocker, id, refresh) = start_bounded_refresh(&db, "bound_recon_st").await;
    db.execute("INSERT INTO bound_recon_right VALUES (1, 21)")
        .await;
    finish_bounded_refresh(&mut blocker, id, refresh).await;
    let fallback: bool = db.query_scalar(
        "SELECT was_full_fallback FROM pgtrickle.pgt_refresh_history WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'bound_recon_st') AND status = 'COMPLETED' ORDER BY refresh_id DESC LIMIT 1",
    ).await;
    assert!(
        fallback,
        "live reconciliation must move to a protected baseline"
    );
    db.assert_st_matches_query("bound_recon_st", query).await;
    refresh_normally(&db, "bound_recon_st").await;
    db.assert_st_matches_query("bound_recon_st", query).await;
}

/// A plain view has no CDC buffer/frontier of its own. Its physical join
/// sources must be fenced without rejecting the ordinary differential path.
#[tokio::test]
async fn phantom_reconciliation_accepts_view_over_synchronous_sources() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE bound_view_left(k int, v int)")
        .await;
    db.execute("CREATE TABLE bound_view_right(k int, v int)")
        .await;
    db.execute("INSERT INTO bound_view_left VALUES (1, 10), (1, 10)")
        .await;
    db.execute("INSERT INTO bound_view_right VALUES (1, 20)")
        .await;
    db.execute(
        "CREATE VIEW bound_join_view AS SELECT l.k, l.v AS lv, r.v AS rv \
         FROM bound_view_left l JOIN bound_view_right r ON l.k = r.k",
    )
    .await;
    let query = "SELECT * FROM bound_join_view";
    db.create_st("bound_view_join_st", query, "1h", "DIFFERENTIAL")
        .await;
    let view_and_leaves: bool = db
        .query_scalar(
            "SELECT count(*) FILTER (WHERE d.source_type = 'VIEW') = 1 \
                 AND count(*) FILTER (WHERE d.source_type = 'TABLE' \
                     AND d.cdc_mode = 'TRIGGER') = 2 \
             FROM pgtrickle.pgt_dependencies d \
             JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
             WHERE s.pgt_name = 'bound_view_join_st'",
        )
        .await;
    assert!(
        view_and_leaves,
        "fixture must register a view and both physical sources"
    );
    db.execute("INSERT INTO bound_view_left VALUES (1, 11)")
        .await;
    refresh_normally(&db, "bound_view_join_st").await;
    db.assert_st_matches_query("bound_view_join_st", query)
        .await;
    assert_no_full_fallback(&db, "bound_view_join_st").await;
}
