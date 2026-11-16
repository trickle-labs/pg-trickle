//! Regression tests for issue #1437: live DVM sources must be at the
//! exact NEW LSN bound, not the later MERGE statement's MVCC snapshot.
//!
//! A SHARE lock on the stream table forces the refresh to pause between
//! taking its WAL bound and applying its MERGE. A source writer commits
//! after the bound. The refresh must neither use the future row early nor
//! leave it in materialized state after subsequent bounded windows.
//!
//! Run:
//! cargo test --test e2e_differential_bound_tests -- --nocapture

mod e2e;

use e2e::E2eDb;
use sqlx::{PgPool, Postgres, pool::PoolConnection};
use std::time::Duration;

async fn refresh_differential(pool: &PgPool, st: &'static str) {
    let mut conn = pool.acquire().await.expect("refresh connection");
    sqlx::query("SET pg_trickle.refresh_strategy = 'differential'")
        .execute(&mut *conn)
        .await
        .expect("force differential strategy");
    let sql = format!("SELECT pgtrickle.refresh_stream_table('{st}')");
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(&mut *conn)
        .await
        .expect("bounded differential refresh");
}

async fn hold_stream_share_lock(db: &E2eDb, stream: &'static str) -> PoolConnection<Postgres> {
    let mut conn = db.pool.acquire().await.expect("stream lock connection");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin blocker");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE {stream} IN SHARE MODE"
    )))
    .execute(&mut *conn)
    .await
    .expect("lock stream table");
    conn
}

async fn wait_until_refresh_blocked(db: &E2eDb) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS( \
                   SELECT 1 FROM pg_stat_activity \
                   WHERE state = 'active' \
                     AND wait_event_type = 'Lock' \
                     AND query LIKE '%pgtrickle.refresh_stream_table%' \
                     AND pid <> pg_backend_pid())",
            )
            .fetch_one(&db.pool)
            .await
            .expect("query blocked refresh");
            if waiting {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("refresh did not block at the stream-table lock");
}

async fn assert_no_full_fallback(db: &E2eDb, st: &'static str) {
    let full_fallbacks: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_refresh_history \
         WHERE was_full_fallback = true \
           AND pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1)",
    )
    .bind(st)
    .fetch_one(&db.pool)
    .await
    .expect("read refresh history");
    assert_eq!(full_fallbacks, 0, "reproduction must stay on differential path");
}

async fn run_extremum_test(minimum: bool) {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE bound_src (id integer PRIMARY KEY, g integer, v numeric)")
        .await;
    db.execute(
        "INSERT INTO bound_src \
         SELECT i, i % 3, i FROM generate_series(1, 30) i",
    )
    .await;
    let aggregate = if minimum { "min" } else { "max" };
    let defining_query = format!(
        "SELECT g, {aggregate}(v) AS ex, sum(v) AS s, count(*) AS n \
         FROM bound_src GROUP BY g"
    );
    db.create_st("bound_ext_st", &defining_query, "1h", "DIFFERENTIAL")
        .await;

    // Delete the old extremum in the first window, then pause its MERGE.
    if minimum {
        db.execute("DELETE FROM bound_src WHERE id = 3").await;
    } else {
        db.execute("DELETE FROM bound_src WHERE id = 30").await;
    }
    let mut blocker = hold_stream_share_lock(&db, "bound_ext_st").await;
    let refresh_pool = db.pool.clone();
    let refresh = tokio::spawn(async move {
        refresh_differential(&refresh_pool, "bound_ext_st").await;
    });
    wait_until_refresh_blocked(&db).await;

    // This row is AFTER the first window bound. It must never appear in
    // the rescan for the first window, even when visible to a later snapshot.
    let writer_pool = db.pool.clone();
    let future = if minimum { -1000 } else { 1000 };
    let writer = tokio::spawn(async move {
        sqlx::query("INSERT INTO bound_src VALUES (9999, 0, $1)")
            .bind(future)
            .execute(&writer_pool)
            .await
            .expect("insert future extreme");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    sqlx::query("COMMIT")
        .execute(&mut *blocker)
        .await
        .expect("release blocker");
    tokio::time::timeout(Duration::from_secs(30), async {
        refresh.await.expect("refresh task");
        writer.await.expect("writer task");
    })
    .await
    .expect("concurrency reproduction timed out");

    // The future extremum is transient: INSERT and DELETE cancel in the
    // next bounded window. A stale rescan value would persist indefinitely.
    db.execute("DELETE FROM bound_src WHERE id = 9999").await;
    refresh_differential(&db.pool, "bound_ext_st").await;
    refresh_differential(&db.pool, "bound_ext_st").await;
    db.assert_st_matches_query("bound_ext_st", &defining_query)
        .await;
    assert_no_full_fallback(&db, "bound_ext_st").await;
}

#[tokio::test]
async fn max_does_not_retain_future_deleted_extreme() {
    run_extremum_test(false).await;
}

#[tokio::test]
async fn min_does_not_retain_future_deleted_extreme() {
    run_extremum_test(true).await;
}

#[tokio::test]
async fn join_counts_future_partner_once() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE bound_a(id integer PRIMARY KEY, k integer, g integer)")
        .await;
    db.execute("CREATE TABLE bound_b(id integer PRIMARY KEY, k integer, w integer)")
        .await;
    db.execute("INSERT INTO bound_a SELECT i, i, i % 3 FROM generate_series(1, 30) i")
        .await;
    db.execute("INSERT INTO bound_b SELECT i, i, 1 FROM generate_series(1, 30) i")
        .await;
    let query = "SELECT a.g, count(*) AS n, sum(b.w) AS sw \
                 FROM bound_a a JOIN bound_b b ON a.k = b.k GROUP BY a.g";
    db.create_st("bound_join_st", query, "1h", "DIFFERENTIAL")
        .await;

    // A joins no row in the first window; B is committed after its bound.
    db.execute("INSERT INTO bound_a VALUES (100, 100, 0)").await;
    let mut blocker = hold_stream_share_lock(&db, "bound_join_st").await;
    let refresh_pool = db.pool.clone();
    let refresh = tokio::spawn(async move {
        refresh_differential(&refresh_pool, "bound_join_st").await;
    });
    wait_until_refresh_blocked(&db).await;
    let writer_pool = db.pool.clone();
    let writer = tokio::spawn(async move {
        sqlx::query("INSERT INTO bound_b VALUES (100, 100, 5)")
            .execute(&writer_pool)
            .await
            .expect("insert future join partner");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    sqlx::query("COMMIT")
        .execute(&mut *blocker)
        .await
        .expect("release blocker");
    tokio::time::timeout(Duration::from_secs(30), async {
        refresh.await.expect("refresh task");
        writer.await.expect("writer task");
    })
    .await
    .expect("join concurrency reproduction timed out");

    refresh_differential(&db.pool, "bound_join_st").await;
    refresh_differential(&db.pool, "bound_join_st").await;
    db.assert_st_matches_query("bound_join_st", query).await;
    assert_no_full_fallback(&db, "bound_join_st").await;
}
