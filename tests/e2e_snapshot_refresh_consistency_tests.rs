//! Snapshot rows and frontiers must be captured under the refresh lock.

mod e2e;

use e2e::E2eDb;
use sqlx::{PgPool, Postgres, pool::PoolConnection};
use std::time::Duration;

const LOCK_CLASS: i32 = 1183;
const LOCK_OBJECT: i32 = 1183;

async fn hold_snapshot_barrier(db: &E2eDb) -> PoolConnection<Postgres> {
    let mut connection = db.pool.acquire().await.expect("acquire snapshot barrier");
    sqlx::query("SELECT pg_advisory_lock($1, $2)")
        .bind(LOCK_CLASS)
        .bind(LOCK_OBJECT)
        .execute(&mut *connection)
        .await
        .expect("hold snapshot barrier");
    connection
}

async fn wait_for_snapshot_barrier(db: &E2eDb, blocker_pid: i32) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                 WHERE wait_event = 'advisory' AND $1 = ANY(pg_blocking_pids(pid)))",
            )
            .bind(blocker_pid)
            .fetch_one(&db.pool)
            .await
            .expect("inspect snapshot barrier");
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("snapshot did not reach the deterministic DDL barrier");
}

async fn snapshot_as_role(pool: PgPool, role: &'static str) -> Result<(), sqlx::Error> {
    let mut connection = pool.acquire().await.expect("acquire snapshot caller");
    sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE {role}")))
        .execute(&mut *connection)
        .await?;
    let result = sqlx::query("SELECT pgtrickle.snapshot_stream_table('v1183_stream')")
        .execute(&mut *connection)
        .await
        .map(|_| ());
    let reset = sqlx::query("RESET ROLE").execute(&mut *connection).await;
    result.and(reset.map(|_| ()))
}

async fn run_snapshot_during_refresh(db: &E2eDb, use_default_target: bool) {
    let old_frontier: String = sqlx::query_scalar(
        "SELECT frontier::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'v1183_stream'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read pre-snapshot frontier");
    let mut barrier = hold_snapshot_barrier(db).await;
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *barrier)
        .await
        .expect("read barrier backend");

    let snapshot = if use_default_target {
        let pool = db.pool.clone();
        tokio::spawn(async move { snapshot_as_role(pool, "v1183_owner").await })
    } else {
        let pool = db.pool.clone();
        tokio::spawn(async move {
            sqlx::query("SELECT pgtrickle.snapshot_stream_table('v1183_stream', 'v1183_target')")
                .execute(&pool)
                .await
                .map(|_| ())
        })
    };
    wait_for_snapshot_barrier(db, blocker_pid).await;

    let notices = db
        .try_execute_with_notices("SELECT pgtrickle.refresh_stream_table('v1183_stream')")
        .await
        .expect("concurrent refresh call failed");
    assert!(
        notices
            .iter()
            .all(|notice| !notice.contains("refresh skipped")),
        "baseline reproduction requires the differential refresh to run: {notices:?}"
    );

    sqlx::query("SELECT pg_advisory_unlock($1, $2)")
        .bind(LOCK_CLASS)
        .bind(LOCK_OBJECT)
        .execute(&mut *{ barrier })
        .await
        .expect("release snapshot barrier");
    snapshot
        .await
        .expect("snapshot task panicked")
        .expect("snapshot failed");

    let (snapshot_schema, snapshot_table, snapshot_frontier): (String, String, String) =
        sqlx::query_as(
            "SELECT snapshot_schema::text, snapshot_table::text, frontier::text \
             FROM pgtrickle.pgt_snapshots \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                             WHERE pgt_name = 'v1183_stream') \
             ORDER BY snapshot_id DESC LIMIT 1",
        )
        .fetch_one(&db.pool)
        .await
        .expect("read snapshot catalog entry");
    let current_frontier: String = sqlx::query_scalar(
        "SELECT frontier::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'v1183_stream'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read post-refresh frontier");
    let current_rows: Vec<i32> = sqlx::query_scalar("SELECT id FROM v1183_stream ORDER BY id")
        .fetch_all(&db.pool)
        .await
        .expect("read post-refresh stream-table rows");
    let source_rows: Vec<i32> = sqlx::query_scalar("SELECT id FROM v1183_source ORDER BY id")
        .fetch_all(&db.pool)
        .await
        .expect("read independent source oracle");
    assert_ne!(
        current_frontier, old_frontier,
        "refresh must commit in the gap"
    );
    assert_eq!(
        current_rows, source_rows,
        "refresh must reach source parity"
    );
    assert_eq!(
        snapshot_frontier, old_frontier,
        "snapshot saved pre-refresh frontier"
    );
    let snapshot_name = if snapshot_schema == "pgtrickle" {
        format!("pgtrickle.{snapshot_table}")
    } else {
        format!("{snapshot_schema}.{snapshot_table}")
    };
    let snapshot_rows: Vec<i32> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {snapshot_name} ORDER BY id"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("read snapshot rows");
    assert_eq!(
        snapshot_rows, current_rows,
        "snapshot copied post-refresh rows"
    );
    assert_ne!(
        snapshot_frontier, current_frontier,
        "reproduction observed a mixed pair"
    );
}

#[tokio::test]
async fn test_snapshot_refresh_race_keeps_frontier_and_rows_coherent() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE v1183_source (id integer primary key)")
        .await;
    db.execute("INSERT INTO v1183_source VALUES (1)").await;
    db.execute("CREATE ROLE v1183_owner LOGIN").await;
    db.execute("GRANT SELECT ON v1183_source TO v1183_owner")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO v1183_owner")
        .await;
    db.execute("GRANT CREATE ON SCHEMA public TO v1183_owner")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table(text, text, text, text, boolean, text, text, text, boolean, boolean, text, integer, double precision, text, boolean, text, integer, text, text) TO v1183_owner")
        .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.snapshot_stream_table(text, text) TO v1183_owner",
    )
    .await;
    db.try_execute_with_role(
        "SET ROLE v1183_owner",
        "SELECT pgtrickle.create_stream_table('v1183_stream', 'SELECT id FROM v1183_source', schedule => '1h', refresh_mode => 'DIFFERENTIAL')",
        "RESET ROLE",
    )
    .await
    .expect("create owner stream table");
    db.execute("CREATE OR REPLACE FUNCTION v1183_snapshot_barrier() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN IF position('v1183_target' in current_query()) > 0 OR position('snapshot_v1183_stream_' in current_query()) > 0 THEN PERFORM pg_advisory_lock(1183, 1183); PERFORM pg_advisory_unlock(1183, 1183); END IF; END $$")
        .await;
    db.execute("CREATE EVENT TRIGGER v1183_snapshot_barrier ON ddl_command_start EXECUTE FUNCTION v1183_snapshot_barrier()")
        .await;

    db.execute("INSERT INTO v1183_source VALUES (2)").await;
    run_snapshot_during_refresh(&db, false).await;
    db.execute("INSERT INTO v1183_source VALUES (3)").await;
    run_snapshot_during_refresh(&db, true).await;

    db.execute("DROP EVENT TRIGGER v1183_snapshot_barrier")
        .await;
    db.execute("DROP FUNCTION v1183_snapshot_barrier()").await;
    db.execute("SELECT pgtrickle.create_stream_table('v1183_empty', 'SELECT id FROM v1183_source WHERE false', schedule => '1h', refresh_mode => 'DIFFERENTIAL')")
        .await;
    let empty_frontier: String = sqlx::query_scalar(
        "SELECT frontier::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'v1183_empty'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read empty stream-table frontier");
    db.execute("SELECT pgtrickle.snapshot_stream_table('v1183_empty', 'v1183_empty_snapshot')")
        .await;
    let empty_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM v1183_empty_snapshot")
        .fetch_one(&db.pool)
        .await
        .expect("read empty snapshot row count");
    let saved_empty_frontier: String = sqlx::query_scalar(
        "SELECT frontier::text FROM pgtrickle.pgt_snapshots \
         WHERE snapshot_table = 'v1183_empty_snapshot'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("read empty snapshot frontier");
    assert_eq!(empty_rows, 0);
    assert_eq!(saved_empty_frontier, empty_frontier);
}
