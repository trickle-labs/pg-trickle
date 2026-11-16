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
        let stages: Vec<(i32, i32)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
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
