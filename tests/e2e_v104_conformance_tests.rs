//! v0.104 Graph V1 and Delta V1 public-contract conformance.

mod e2e;
#[path = "conformance/reference_clients.rs"]
mod reference_clients;

use e2e::E2eDb;
use sqlx::Row;

const CREATE_STREAM_ARGS: &str = "text, text, text, text, boolean, text, text, text, boolean, boolean, \
     text, integer, double precision, text, boolean, text, integer, text, text";

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
             query => 'SELECT id, value FROM v104_delta_source',\
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
    let refresh_id = reference_clients::refresh_graph(&mut *tx, root, &graph_digest).await;
    let batch = reference_clients::pending_batch(&mut *tx, &registration.consumer_id).await;
    assert_eq!(batch.mode, "EXACT");
    assert_eq!(batch.graph_refresh_id, Some(refresh_id));
    assert_eq!(batch.row_count, 1);
    assert_eq!(
        reference_clients::read_payload(&mut *tx, &registration.delta_relation).await,
        1
    );
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
