//! v0.104 Graph V1 and Delta V1 public-contract conformance.

mod e2e;
#[allow(dead_code)]
#[path = "conformance/reference_clients.rs"]
mod reference_clients;

use e2e::E2eDb;
use sqlx::Row;

const CREATE_STREAM_ARGS: &str = "text, text, text, text, boolean, text, text, text, boolean, boolean, \
     text, integer, double precision, text, boolean, text, integer, text, text";

#[tokio::test]
async fn test_graph_refresh_scopes_nested_join_snapshots_and_empty_stages() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(
        "CREATE TABLE stage_left (id int NOT NULL, value int NOT NULL);
         CREATE TABLE stage_right (id int PRIMARY KEY, value int NOT NULL);
         CREATE TABLE stage_meta (id int PRIMARY KEY);
         INSERT INTO stage_left SELECT i, i FROM generate_series(1, 4000) i;
         INSERT INTO stage_right SELECT i, 0 FROM generate_series(1, 4000) i;
         INSERT INTO stage_meta VALUES (1);
         CREATE FUNCTION stage_value(value int) RETURNS TABLE (normalized int)
         LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
         BEGIN RETURN QUERY SELECT value; END $$",
    )
    .execute(&db.pool)
    .await
    .expect("install staged-join fixture");
    db.execute(
        "SELECT pgtrickle.create_stream_table(
             name => 'stage_join',
             query => 'SELECT l.id AS left_id, r.id AS right_id, l.value, r.value AS right_value
                       FROM (SELECT s.id, n.normalized AS value FROM stage_left s
                             CROSS JOIN LATERAL public.stage_value(s.value) AS n(normalized)) l
                       JOIN stage_right r ON l.id = r.id
                       JOIN stage_meta m ON m.id = 1',
             schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false,
             orchestration_mode => 'EXTERNAL')",
    )
    .await;
    let root = "public.stage_join";
    let digest = reference_clients::graph_digest(&db.pool, root).await;
    reference_clients::refresh_graph(&db.pool, root, &digest).await;

    let mut tx = db.pool.begin().await.expect("begin staged refresh");
    sqlx::raw_sql(
        "SET LOCAL track_functions = 'all';
         UPDATE stage_left SET value = 200 WHERE id = 1;
         UPDATE stage_right SET value = 201 WHERE id = 1",
    )
    .execute(&mut *tx)
    .await
    .expect("change both inputs while leaving metadata unchanged");
    reference_clients::refresh_graph_without_full(&mut *tx, root, &digest).await;
    let calls: i64 = sqlx::query_scalar(
        "SELECT calls FROM pg_stat_xact_user_functions WHERE funcid = 'stage_value(int)'::regprocedure",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("read function execution count");
    eprintln!("normalization calls: {calls}");
    // Reconciliation still evaluates 4000 rows; delta joins must expand
    // only affected keys, even with an empty metadata delta.
    assert!((4000..4500).contains(&calls), "unexpected calls: {calls}");
    let values: (i32, i32) =
        sqlx::query_as("SELECT value, right_value FROM stage_join WHERE left_id = 1")
            .fetch_one(&mut *tx)
            .await
            .expect("read refreshed value");
    assert_eq!(values, (200, 201));
    tx.rollback().await.expect("rollback staged refresh");
    let value: i32 = db
        .query_scalar("SELECT value FROM stage_join WHERE left_id = 1")
        .await;
    assert_eq!(value, 1, "rollback must restore the graph output");

    db.execute("UPDATE stage_right SET id = 4001 WHERE id = 1")
        .await;
    reference_clients::refresh_graph_without_full(&db.pool, root, &digest).await;
    assert_eq!(
        db.count(root).await,
        3999,
        "right-only changes must survive"
    );
}

#[tokio::test]
async fn test_v104_capabilities_are_stable_by_default() {
    let db = E2eDb::new().await.with_extension().await;

    let rows = sqlx::query(
        "SELECT capability, enabled, details->>'status' AS status, details->>'phase' AS phase \
         FROM pgtrickle.integration_capabilities() \
         WHERE capability IN ('external_graph_refresh', 'output_delta_consumer') \
         ORDER BY capability",
    )
    .fetch_all(&db.pool)
    .await
    .expect("capability discovery should be public and queryable");

    assert_eq!(rows.len(), 2);
    for row in rows {
        let capability: String = row.try_get("capability").expect("capability name");
        let enabled: bool = row.try_get("enabled").expect("enabled flag");
        let status: String = row.try_get("status").expect("capability status");
        let phase: String = row.try_get("phase").expect("release phase");
        assert!(enabled, "{capability} must be enabled in v0.104");
        assert_eq!(status, "stable", "{capability} must be stable");
        assert_eq!(phase, "v0.104_conformance");
    }
}

#[tokio::test]
async fn test_v104_graph_reference_coordinator_commits_publication() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE v104_graph_source (id INT PRIMARY KEY, value TEXT)")
        .await;
    db.execute("INSERT INTO v104_graph_source VALUES (1, 'before')")
        .await;
    db.execute("CREATE TABLE v104_graph_publication (graph_refresh_id BIGINT NOT NULL)")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'v104_graph_st',\
             query => 'SELECT id, value FROM v104_graph_source',\
             schedule => '1h',\n             refresh_mode => 'DIFFERENTIAL',\
             initialize => false,\
             orchestration_mode => 'EXTERNAL'\
         )",
    )
    .await;

    let root = "public.v104_graph_st";
    let digest = reference_clients::graph_digest(&db.pool, root).await;

    let mut tx = db.pool.begin().await.expect("begin rollback transaction");
    let refresh_id = reference_clients::refresh_graph(&mut *tx, root, &digest).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM public.v104_graph_st")
        .fetch_one(&mut *tx)
        .await
        .expect("stream table should be readable in the coordinator transaction");
    assert_eq!(count, 1);
    sqlx::query("INSERT INTO v104_graph_publication VALUES ($1)")
        .bind(refresh_id)
        .execute(&mut *tx)
        .await
        .expect("publication should share the coordinator transaction");
    tx.rollback().await.expect("rollback should succeed");

    let stream_rows: i64 = db.count("public.v104_graph_st").await;
    let publications: i64 = db.count("public.v104_graph_publication").await;
    assert_eq!(stream_rows, 0, "rollback must hide the graph publication");
    assert_eq!(publications, 0, "rollback must hide the publication row");

    let mut tx = db.pool.begin().await.expect("begin commit transaction");
    let refresh_id = reference_clients::refresh_graph(&mut *tx, root, &digest).await;
    sqlx::query("INSERT INTO v104_graph_publication VALUES ($1)")
        .bind(refresh_id)
        .execute(&mut *tx)
        .await
        .expect("publication should commit with the graph refresh");
    tx.commit().await.expect("commit should succeed");

    assert_eq!(db.count("public.v104_graph_st").await, 1);
    assert_eq!(db.count("public.v104_graph_publication").await, 1);
}

#[tokio::test]
async fn test_v104_graph_source_owner_can_delegate_coordinator() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "DO $$ BEGIN CREATE ROLE v104_graph_coordinator LOGIN; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$",
    )
    .await;
    db.execute("GRANT USAGE ON SCHEMA public, pgtrickle TO v104_graph_coordinator")
        .await;
    db.execute("GRANT CREATE ON SCHEMA public TO v104_graph_coordinator")
        .await;
    for function in [
        format!("create_stream_table({CREATE_STREAM_ARGS})"),
        "graph_contract(regclass[])".to_string(),
        "refresh_graph_strict(regclass[], bytea, text)".to_string(),
    ] {
        db.execute(&format!(
            "GRANT EXECUTE ON FUNCTION pgtrickle.{function} TO v104_graph_coordinator"
        ))
        .await;
    }
    db.execute("CREATE TABLE v104_delegated_source (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO v104_delegated_source VALUES (1)")
        .await;
    db.execute("GRANT SELECT ON v104_delegated_source TO v104_graph_coordinator")
        .await;

    db.try_execute_with_role(
        "SET ROLE v104_graph_coordinator",
        "SELECT pgtrickle.create_stream_table(\
             name => 'v104_delegated_st',\
             query => 'SELECT id FROM v104_delegated_source',\
             schedule => '1h',\
             refresh_mode => 'DIFFERENTIAL',\
             initialize => false,\
             orchestration_mode => 'EXTERNAL'\
         )",
        "RESET ROLE",
    )
    .await
    .expect("the coordinator should create its graph member");

    let contract_sql = "SELECT graph_digest FROM pgtrickle.graph_contract(\
        ARRAY['public.v104_delegated_st'::regclass])";
    let denied = db
        .try_execute_with_role(
            "SET ROLE v104_graph_coordinator",
            contract_sql,
            "RESET ROLE",
        )
        .await
        .expect_err("SELECT alone must not delegate source coordination");
    assert!(
        denied.to_string().contains("SELECT and MAINTAIN"),
        "delegation error should name the required grants: {denied}"
    );

    db.execute("GRANT MAINTAIN ON v104_delegated_source TO v104_graph_coordinator")
        .await;
    db.try_execute_with_role(
        "SET ROLE v104_graph_coordinator",
        "SELECT * FROM pgtrickle.refresh_graph_strict(\
             ARRAY['public.v104_delegated_st'::regclass],\
             (SELECT graph_digest FROM pgtrickle.graph_contract(\
                 ARRAY['public.v104_delegated_st'::regclass]))\
         )",
        "RESET ROLE",
    )
    .await
    .expect("the delegated coordinator should refresh its graph");
    assert_eq!(db.count("public.v104_delegated_st").await, 1);

    db.execute("REVOKE MAINTAIN ON v104_delegated_source FROM v104_graph_coordinator")
        .await;
    assert!(
        db.try_execute_with_role(
            "SET ROLE v104_graph_coordinator",
            contract_sql,
            "RESET ROLE",
        )
        .await
        .is_err(),
        "revoking MAINTAIN must revoke source coordination"
    );
}

#[tokio::test]
async fn test_v104_delta_reference_consumer_reads_and_acknowledges() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE v104_delta_source (id INT PRIMARY KEY, value TEXT)")
        .await;
    db.execute("INSERT INTO v104_delta_source VALUES (1, 'before')")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'v104_delta_st',\
             query => 'SELECT a.id, a.value FROM v104_delta_source a \
                       JOIN v104_delta_source b USING (id) \
                       JOIN v104_delta_source c USING (id) \
                       JOIN v104_delta_source d USING (id) \
                       JOIN v104_delta_source e USING (id)',\
             schedule => '1h',\n             refresh_mode => 'DIFFERENTIAL',\
             initialize => false,\
             orchestration_mode => 'EXTERNAL'\
         )",
    )
    .await;

    let root = "public.v104_delta_st";
    let contract_digest = reference_clients::stream_contract_digest(&db.pool, root).await;
    let registration =
        reference_clients::register(&db.pool, root, "v104_reference_consumer", &contract_digest)
            .await;
    assert_eq!(registration.state, "RESNAPSHOT_REQUIRED");

    let graph_digest = reference_clients::graph_digest(&db.pool, root).await;
    reference_clients::refresh_graph(&db.pool, root, &graph_digest).await;

    let mut tx = db.pool.begin().await.expect("begin baseline transaction");
    let (resnapshot_token, log_head) =
        reference_clients::begin_resnapshot(&mut *tx, &registration.consumer_id).await;
    assert!(
        log_head > 0,
        "baseline must capture the initial output batch"
    );
    let state =
        reference_clients::ack_resnapshot(&mut *tx, &registration.consumer_id, &resnapshot_token)
            .await;
    assert_eq!(state, "ACTIVE");
    tx.commit().await.expect("baseline should commit");

    db.execute("INSERT INTO v104_delta_source VALUES (2, 'after')")
        .await;
    let graph_digest = reference_clients::graph_digest(&db.pool, root).await;
    let mut tx = db.pool.begin().await.expect("begin delta transaction");
    sqlx::raw_sql(
        "SET LOCAL pg_trickle.refresh_strategy = 'differential';
         SET LOCAL pg_trickle.delta_work_mem_cap_mb = 256;
         SET LOCAL temp_file_limit = '64MB'",
    )
    .execute(&mut *tx)
    .await
    .expect("configure capped deep-join delta");
    let refresh_id = reference_clients::refresh_graph(&mut *tx, root, &graph_digest).await;
    let batch = reference_clients::pending_batch(&mut *tx, &registration.consumer_id).await;
    assert_eq!(batch.mode, "EXACT");
    assert_eq!(batch.graph_refresh_id, Some(refresh_id));
    assert_eq!(batch.row_count, 1);
    let payload: Vec<(String, i32, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT action, id, value FROM {} WHERE batch_token = $1 ORDER BY ordinal",
        registration.delta_relation
    )))
    .bind(batch.batch_token)
    .fetch_all(&mut *tx)
    .await
    .expect("read complete typed delta payload");
    assert_eq!(payload, vec![("INSERT".into(), 2, "after".into())]);
    let rows: Vec<(i32, String)> =
        sqlx::query_as("SELECT id, value FROM v104_delta_st ORDER BY id")
            .fetch_all(&mut *tx)
            .await
            .expect("read complete deep-join result");
    assert_eq!(rows, vec![(1, "before".into()), (2, "after".into())]);
    let action: String = sqlx::query_scalar(
        "SELECT h.action FROM pgtrickle.pgt_refresh_history h
         JOIN pgtrickle.pgt_stream_tables t USING (pgt_id)
         WHERE t.pgt_name = 'v104_delta_st' ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("read effective deep-join action");
    assert_eq!(action, "DIFFERENTIAL");
    assert_eq!(
        reference_clients::ack(&mut *tx, &registration.consumer_id, batch.batch_token).await,
        "APPLIED"
    );
    tx.commit()
        .await
        .expect("delta acknowledgement should commit");

    assert_eq!(
        reference_clients::batch_lag(&db.pool, &registration.consumer_id).await,
        0
    );
}

#[tokio::test]
async fn test_v106_graph_refresh_propagates_same_pass_with_active_consumer() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE v106_graph_source (id INT PRIMARY KEY, grp INT NOT NULL, val INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO v106_graph_source VALUES (1, 0, 10), (2, 0, 20), (3, 1, 30)")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'v106_graph_middle',\
             query => 'SELECT grp, SUM(val)::bigint AS total, COUNT(*)::bigint AS n \
                       FROM v106_graph_source GROUP BY grp',\
             schedule => '1h',\
             refresh_mode => 'DIFFERENTIAL',\
             initialize => false,\
             orchestration_mode => 'EXTERNAL'\
         )",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'v106_graph_leaf',\
             query => 'SELECT grp, total, n FROM v106_graph_middle',\
             schedule => '1h',\
             refresh_mode => 'DIFFERENTIAL',\
             initialize => false,\
             orchestration_mode => 'EXTERNAL'\
         )",
    )
    .await;

    let root = "public.v106_graph_leaf";
    let contract_digest = reference_clients::stream_contract_digest(&db.pool, root).await;
    let consumer =
        reference_clients::register(&db.pool, root, "v106_graph_consumer", &contract_digest).await;
    let graph_digest = reference_clients::graph_digest(&db.pool, root).await;
    reference_clients::refresh_graph(&db.pool, root, &graph_digest).await;

    let mut tx = db.pool.begin().await.expect("begin graph resnapshot");
    let (token, _) = reference_clients::begin_resnapshot(&mut *tx, &consumer.consumer_id).await;
    assert_eq!(
        reference_clients::ack_resnapshot(&mut *tx, &consumer.consumer_id, &token).await,
        "ACTIVE"
    );
    tx.commit().await.expect("commit graph resnapshot");

    db.execute("UPDATE v106_graph_source SET val = val + 5 WHERE id = 1")
        .await;
    db.execute("INSERT INTO v106_graph_source VALUES (4, 1, 7)")
        .await;
    let mut tx = db.pool.begin().await.expect("begin graph update");
    let refresh_id = reference_clients::refresh_graph(&mut *tx, root, &graph_digest).await;
    let batch = reference_clients::pending_batch(&mut *tx, &consumer.consumer_id).await;
    assert_eq!(batch.graph_refresh_id, Some(refresh_id));
    assert_eq!(batch.mode, "EXACT");
    assert_eq!(
        reference_clients::ack(&mut *tx, &consumer.consumer_id, batch.batch_token).await,
        "APPLIED"
    );
    tx.commit().await.expect("commit graph refresh and ack");

    db.assert_st_matches_query(
        "public.v106_graph_middle",
        "SELECT grp, SUM(val)::bigint AS total, COUNT(*)::bigint AS n \
         FROM v106_graph_source GROUP BY grp",
    )
    .await;
    db.assert_st_matches_query(root, "SELECT grp, total, n FROM public.v106_graph_middle")
        .await;
}

#[tokio::test]
async fn test_graph_refresh_propagates_same_pass_through_distinct_self_join() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE graph_pair_source (\
             source_record_id INT NOT NULL, field_name TEXT NOT NULL, state TEXT NOT NULL, \
             canonical_value TEXT NOT NULL, source_sort_key INT NOT NULL, \
             PRIMARY KEY (source_record_id, field_name))",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'graph_pair_blocks',\
             query => 'SELECT ''email''::text AS channel_id, canonical_value AS block_key, \
                              source_record_id, source_sort_key, field_name \
                       FROM graph_pair_source \
                       WHERE field_name = ''email'' AND state = ''value''',\
             schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false,\
             orchestration_mode => 'EXTERNAL')",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'graph_pair_stats',\
             query => 'SELECT channel_id, block_key, COUNT(*)::bigint AS block_records \
                       FROM graph_pair_blocks GROUP BY channel_id, block_key',\
             schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false,\
             orchestration_mode => 'EXTERNAL')",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'graph_pairs',\
             query => 'SELECT DISTINCT \
                           l.source_record_id AS left_source_record_id, \
                           r.source_record_id AS right_source_record_id \
                       FROM graph_pair_blocks l \
                       JOIN graph_pair_stats s \
                         ON s.channel_id = l.channel_id AND s.block_key = l.block_key \
                       JOIN graph_pair_blocks r \
                         ON r.channel_id = l.channel_id AND r.block_key = l.block_key \
                        AND l.source_sort_key < r.source_sort_key \
                       WHERE s.block_records <= 100',\
             schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false,\
             orchestration_mode => 'EXTERNAL')",
    )
    .await;

    let root = "public.graph_pairs";
    let digest = reference_clients::graph_digest(&db.pool, root).await;
    reference_clients::refresh_graph(&db.pool, root, &digest).await;

    db.execute(
        "INSERT INTO graph_pair_source VALUES \
             (1, 'email', 'value', 'same@example.test', 1), \
             (2, 'email', 'value', 'same@example.test', 2)",
    )
    .await;
    reference_clients::refresh_graph(&db.pool, root, &digest).await;

    db.assert_st_matches_query(
        root,
        "SELECT 1 AS left_source_record_id, 2 AS right_source_record_id",
    )
    .await;
    for name in ["graph_pair_blocks", "graph_pair_stats", "graph_pairs"] {
        let mode: String = db
            .query_scalar(&format!(
                "SELECT effective_refresh_mode FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = '{name}'"
            ))
            .await;
        assert_eq!(mode, "DIFFERENTIAL", "{name} silently fell back to {mode}");
    }
}
