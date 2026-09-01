//! Physical restart coverage for private window-state lifecycle.

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_window_state_and_frontier_survive_restart_then_differential_converges() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TABLE ws_restart_source (id bigint PRIMARY KEY, value int NOT NULL)",
        "INSERT INTO ws_restart_source VALUES (1, 20), (2, 10)",
    ])
    .await;
    db.create_st(
        "ws_restart_st",
        "SELECT id, value, row_number() OVER (ORDER BY value, id) AS rn \
         FROM ws_restart_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ws_restart_st'",
        )
        .await;

    sqlx::query(
        "UPDATE pgtrickle.pgt_stream_tables SET window_strategy = \
             jsonb_set(jsonb_set(jsonb_set(window_strategy, \
                 '{nodes,0,functions,0,strategy}', '\"ordered_suffix\"'), \
                 '{nodes,0,functions,0,runtime_enabled}', 'true'), \
                 '{nodes,0,functions,0,fallback_reason}', 'null'), \
             needs_reinit = true WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .execute(&db.pool)
    .await
    .expect("enable recovery-test state candidate");
    db.refresh_st("ws_restart_st").await;
    let (
        before_partition_oid,
        before_row_oid,
        partition_name,
        row_name,
        before_generation,
        before_frontier,
        before_history_count,
    ): (i64, i64, String, String, i64, serde_json::Value, i64) = sqlx::query_as(
        "SELECT ws.partition_relid::bigint, ws.row_relid::bigint, \
                parts.relname::text, rows.relname::text, ws.state_generation, st.frontier, \
                (SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
                 WHERE h.pgt_id = ws.pgt_id) \
         FROM pgtrickle.pgt_window_states ws \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         JOIN pg_catalog.pg_class parts ON parts.oid = ws.partition_relid \
         JOIN pg_catalog.pg_class rows ON rows.oid = ws.row_relid \
         WHERE ws.pgt_id = $1",
    )
    .bind(pgt_id)
    .fetch_one(&db.pool)
    .await
    .expect("load production state before restart");
    db.execute("INSERT INTO ws_restart_source VALUES (3, 15)")
        .await;

    db.execute(
        "CREATE FUNCTION public.ws_block_generation() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN \
             PERFORM pg_advisory_xact_lock(890089); \
             RETURN NEW; \
         END $$",
    )
    .await;
    db.execute(&format!(
        "CREATE TRIGGER ws_block_generation \
         BEFORE UPDATE OF state_generation ON pgtrickle.{row_name} \
         FOR EACH ROW EXECUTE FUNCTION public.ws_block_generation()"
    ))
    .await;
    let mut blocker = db.pool.acquire().await.expect("acquire blocker connection");
    sqlx::query("SELECT pg_advisory_lock(890089)")
        .execute(&mut *blocker)
        .await
        .expect("hold generation failpoint lock");
    let mut refresh = db.pool.acquire().await.expect("acquire refresh connection");
    let refresh_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *refresh)
        .await
        .expect("load refresh backend PID");
    let refresh_task = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('ws_restart_st')")
            .execute(&mut *refresh)
            .await
    });
    let mut blocked = false;
    for _ in 0..100 {
        blocked = sqlx::query_scalar(
            "SELECT EXISTS ( \
                 SELECT 1 FROM pg_catalog.pg_stat_activity \
                 WHERE pid = $1 AND wait_event = 'advisory' \
             )",
        )
        .bind(refresh_pid)
        .fetch_one(&db.pool)
        .await
        .expect("inspect blocked refresh");
        if blocked {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(blocked, "refresh did not reach the state finalizer");

    let killed = tokio::process::Command::new("docker")
        .args(["kill", "--signal", "KILL", db.container_id()])
        .output()
        .await
        .expect("kill dedicated PostgreSQL container");
    assert!(killed.status.success(), "docker kill failed");
    let refresh_result = tokio::time::timeout(std::time::Duration::from_secs(10), refresh_task)
        .await
        .expect("refresh connection did not close after SIGKILL")
        .expect("refresh task panicked");
    assert!(
        refresh_result.is_err(),
        "refresh survived PostgreSQL SIGKILL"
    );
    drop(blocker);
    tokio::time::timeout(std::time::Duration::from_secs(10), db.pool.close())
        .await
        .expect("old connection pool did not close after SIGKILL");
    let started = tokio::process::Command::new("docker")
        .args(["start", db.container_id()])
        .output()
        .await
        .expect("start dedicated PostgreSQL container");
    assert!(started.status.success(), "docker start failed");

    let restored_pool = db.reconnect_after_restart().await;

    let (partition_oid, row_oid, generation, status, frontier): (
        i64,
        i64,
        i64,
        String,
        serde_json::Value,
    ) = sqlx::query_as(
        "SELECT ws.partition_relid::bigint, ws.row_relid::bigint, \
                ws.state_generation, ws.status, st.frontier \
         FROM pgtrickle.pgt_window_states ws \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE ws.pgt_id = $1",
    )
    .bind(pgt_id)
    .fetch_one(&restored_pool)
    .await
    .expect("load durable READY state after restart");
    assert_eq!(
        (partition_oid, row_oid),
        (before_partition_oid, before_row_oid)
    );
    assert_eq!(generation, before_generation);
    assert_eq!(status, "READY");
    assert_eq!(frontier, before_frontier, "restart advanced the frontier");
    let rolled_back: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT (SELECT count(*) FROM public.ws_restart_st) = 2 \
             AND (SELECT count(*) FROM pgtrickle.{row_name}) = 2 \
             AND (SELECT row_count FROM pgtrickle.{partition_name}) = 2 \
             AND (SELECT count(*) FROM pgtrickle.pgt_refresh_history \
                  WHERE pgt_id = {pgt_id}) = {before_history_count}"
    )))
    .fetch_one(&restored_pool)
    .await
    .expect("inspect rolled-back refresh state");
    assert!(rolled_back, "crash exposed a partially committed refresh");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TRIGGER ws_block_generation ON pgtrickle.{row_name}"
    )))
    .execute(&restored_pool)
    .await
    .expect("drop state failpoint trigger");
    sqlx::query("DROP FUNCTION public.ws_block_generation()")
        .execute(&restored_pool)
        .await
        .expect("drop state failpoint function");

    sqlx::query("SELECT pgtrickle.refresh_stream_table('ws_restart_st')")
        .execute(&restored_pool)
        .await
        .expect("differential refresh after restart");
    let (row_name, partition_name, generation_after): (String, String, i64) = sqlx::query_as(
        "SELECT rows.relname::text, parts.relname::text, ws.state_generation \
         FROM pgtrickle.pgt_window_states ws \
         JOIN pg_catalog.pg_class rows ON rows.oid = ws.row_relid \
         JOIN pg_catalog.pg_class parts ON parts.oid = ws.partition_relid \
         WHERE ws.pgt_id = $1",
    )
    .bind(pgt_id)
    .fetch_one(&restored_pool)
    .await
    .expect("load state after replay");
    assert_eq!(generation_after, generation + 1);
    assert!(
        row_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    );
    let converged: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT NOT EXISTS ( \
             (SELECT id, value, rn FROM public.ws_restart_st \
              EXCEPT ALL SELECT id, value, row_number() OVER (ORDER BY value, id) \
                         FROM public.ws_restart_source) \
             UNION ALL \
             (SELECT id, value, row_number() OVER (ORDER BY value, id) \
              FROM public.ws_restart_source \
              EXCEPT ALL SELECT id, value, rn FROM public.ws_restart_st) \
         ) AND NOT EXISTS ( \
             (SELECT __pgt_row_id, id, value, rn FROM public.ws_restart_st \
              EXCEPT ALL SELECT __pgt_row_id, id, value, rn FROM pgtrickle.{row_name}) \
             UNION ALL \
             (SELECT __pgt_row_id, id, value, rn FROM pgtrickle.{row_name} \
              EXCEPT ALL SELECT __pgt_row_id, id, value, rn FROM public.ws_restart_st) \
         ) AND NOT EXISTS ( \
             SELECT 1 FROM pgtrickle.{row_name} \
             WHERE state_generation IS DISTINCT FROM {generation_after} \
         ) AND NOT EXISTS ( \
             SELECT 1 FROM pgtrickle.{partition_name} \
             WHERE state_generation IS DISTINCT FROM {generation_after} \
         )"
    )))
    .fetch_one(&restored_pool)
    .await
    .expect("compare target, state, and PostgreSQL oracle after restart");
    assert!(converged);
}
