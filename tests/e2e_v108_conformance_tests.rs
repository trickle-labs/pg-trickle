//! v0.108 Graph V1.2 and Delta V1.1 public-contract conformance.

mod e2e;
#[allow(dead_code)]
#[path = "conformance/reference_clients.rs"]
mod reference_clients;

use e2e::E2eDb;
use reference_clients::{DeltaRegistration, graph_digest, refresh_graph_without_full};
use sqlx::Row;

async fn active_consumer(prefix: &str) -> (E2eDb, DeltaRegistration, Vec<u8>) {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(&format!(
        "CREATE TABLE {prefix}_source (id INT PRIMARY KEY, value TEXT NOT NULL)"
    ))
    .await;
    db.execute(&format!("INSERT INTO {prefix}_source VALUES (1, 'before')"))
        .await;
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table(\
             name => '{prefix}_stream', query => 'SELECT id, value FROM {prefix}_source',\
             schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false,\
             orchestration_mode => 'EXTERNAL')"
    ))
    .await;

    let root = format!("public.{prefix}_stream");
    let graph = graph_digest(&db.pool, &root).await;
    reference_clients::refresh_graph(&db.pool, &root, &graph).await;
    let contract = reference_clients::stream_contract_digest(&db.pool, &root).await;
    let consumer =
        reference_clients::register(&db.pool, &root, &format!("{prefix}_consumer"), &contract)
            .await;
    assert_eq!(consumer.state, "RESNAPSHOT_REQUIRED");

    let mut tx = db.pool.begin().await.expect("begin baseline transaction");
    let (token, _) = reference_clients::begin_resnapshot(&mut *tx, &consumer.consumer_id).await;
    assert_eq!(
        reference_clients::ack_resnapshot(&mut *tx, &consumer.consumer_id, &token).await,
        "ACTIVE"
    );
    tx.commit().await.expect("commit baseline transaction");
    (db, consumer, graph)
}

async fn pending_consumer(
    prefix: &str,
) -> (E2eDb, DeltaRegistration, reference_clients::DeltaBatch) {
    let (db, consumer, graph) = active_consumer(prefix).await;
    db.execute(&format!(
        "INSERT INTO {prefix}_source VALUES (2, 'pending')"
    ))
    .await;
    refresh_graph_without_full(&db.pool, &format!("public.{prefix}_stream"), &graph).await;
    let batch = reference_clients::pending_batch(&db.pool, &consumer.consumer_id).await;
    (db, consumer, batch)
}

async fn assert_persistently_invalidated(db: &E2eDb, consumer: &DeltaRegistration, reason: &str) {
    for _ in 0..2 {
        let status = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
        assert_eq!(status.state, "INVALIDATED");
        assert_eq!(status.state_reason.as_deref(), Some(reason));
    }
}

#[tokio::test]
async fn test_v108_capability_details_publish_graph_1_2_and_delta_1_1() {
    let db = E2eDb::new().await.with_extension().await;
    let rows = sqlx::query(
        "SELECT capability, major_version, minor_version, details \
         FROM pgtrickle.integration_capabilities() \
         WHERE capability IN ('external_graph_refresh', 'output_delta_consumer')",
    )
    .fetch_all(&db.pool)
    .await
    .expect("integration capabilities should be queryable");

    assert_eq!(rows.len(), 2);
    for row in rows {
        let capability: String = row.get("capability");
        let major: i16 = row.get("major_version");
        let minor: i16 = row.get("minor_version");
        let details: serde_json::Value = row.get("details");
        assert_eq!(major, 1);
        match capability.as_str() {
            "external_graph_refresh" => {
                assert_eq!(minor, 2);
                assert_eq!(
                    details["differential_features"],
                    serde_json::json!([
                        "stable_row_identity_encoder_v2",
                        "custom_table_srf_out_columns",
                        "lateral_immutable_composite_function"
                    ])
                );
            }
            "output_delta_consumer" => {
                assert_eq!(minor, 1);
                assert_eq!(details["consumer_recovery_version"], 1);
                assert_eq!(details["public_resnapshot_request"], true);
                assert_eq!(details["public_consumer_validation"], true);
                assert_eq!(
                    details["qualification_api"],
                    "qualify_output_delta_recovery"
                );
                assert_eq!(details["typed_delta_encoding_version"], 1);
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn test_v108_graph_row_identity_and_table_srf_stay_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE v108_graph_source (\
             id INT PRIMARY KEY, entity_id UUID NOT NULL, \
             source_identity_id UUID NOT NULL, source_key TEXT NOT NULL, \
             raw_value TEXT, source_state TEXT NOT NULL)",
    )
    .await;
    db.execute(
        "CREATE FUNCTION public.v108_normalize_text(\
             raw_value TEXT, field_kind TEXT, version INT, source_state TEXT, options JSONB) \
         RETURNS TABLE (canonical_value TEXT, normalized_state TEXT) \
         LANGUAGE SQL IMMUTABLE PARALLEL SAFE \
         AS 'SELECT CASE WHEN raw_value IS NULL THEN NULL ELSE lower(trim(raw_value)) END, \
                    CASE WHEN source_state = ''deleted'' THEN ''deleted'' \
                         WHEN raw_value IS NULL THEN ''null'' ELSE ''value'' END'",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'v108_records',\
             query => 'SELECT id, pgtrickle.encode_row_id_v2(''MDM_SOURCE_KEY_V1'', \
                              ROW(entity_id, source_identity_id, source_key)) \
                              AS source_record_id, raw_value, source_state \
                       FROM v108_graph_source',\
             schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false,\
             orchestration_mode => 'EXTERNAL')",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'v108_normalized',\
             query => 'SELECT r.id, r.source_record_id, n.canonical_value, n.normalized_state \
                       FROM public.v108_records AS r \
                       CROSS JOIN LATERAL public.v108_normalize_text(\
                           r.raw_value, ''email'', 1, r.source_state, ''{}''::jsonb) AS n',\
             schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false,\
             orchestration_mode => 'EXTERNAL')",
    )
    .await;

    let root = "public.v108_normalized";
    let digest = graph_digest(&db.pool, root).await;
    db.execute(
        "INSERT INTO v108_graph_source VALUES \
             (1, '10000000-0000-4000-8000-000000000001', \
                 '20000000-0000-4000-8000-000000000001', 'crm-1', \
                 ' One@Example.Test ', 'active'), \
             (2, '10000000-0000-4000-8000-000000000001', \
                 '20000000-0000-4000-8000-000000000001', 'crm-2', \
                 NULL, 'active')",
    )
    .await;
    reference_clients::refresh_graph(&db.pool, root, &digest).await;
    db.assert_st_matches_query(
        root,
        "SELECT r.id, r.source_record_id, n.canonical_value, n.normalized_state \
         FROM public.v108_records AS r \
         CROSS JOIN LATERAL public.v108_normalize_text(\
             r.raw_value, 'email', 1, r.source_state, '{}'::jsonb) AS n",
    )
    .await;

    db.execute("UPDATE v108_graph_source SET raw_value = 'two@example.test' WHERE id = 1")
        .await;
    db.execute("DELETE FROM v108_graph_source WHERE id = 2")
        .await;
    refresh_graph_without_full(&db.pool, root, &digest).await;
    db.assert_st_matches_query(
        root,
        "SELECT r.id, r.source_record_id, n.canonical_value, n.normalized_state \
         FROM public.v108_records AS r \
         CROSS JOIN LATERAL public.v108_normalize_text(\
             r.raw_value, 'email', 1, r.source_state, '{}'::jsonb) AS n",
    )
    .await;

    for name in ["v108_records", "v108_normalized"] {
        let mode: String = db
            .query_scalar(&format!(
                "SELECT effective_refresh_mode FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = '{name}'"
            ))
            .await;
        assert_eq!(mode, "DIFFERENTIAL", "{name} fell back to {mode}");
    }
}

#[tokio::test]
async fn test_v108_non_pristine_registration_and_public_resnapshot() {
    let (db, consumer, _) = active_consumer("v108_resnapshot").await;
    let before = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
    assert_eq!(before.state, "ACTIVE");

    let requested = reference_clients::request_resnapshot(&db.pool, &consumer.consumer_id).await;
    assert_eq!(requested.state, "RESNAPSHOT_REQUIRED");
    assert_eq!(requested.state_reason.as_deref(), Some("ADMIN_REQUESTED"));
    assert_eq!(
        requested.acknowledged_batch_token,
        before.acknowledged_batch_token
    );
    assert_eq!(requested.log_head, before.log_head);
    assert_eq!(
        requested.output_contract_digest,
        before.output_contract_digest
    );
    assert_eq!(requested.row_identity_version, before.row_identity_version);

    let repeated = reference_clients::request_resnapshot(&db.pool, &consumer.consumer_id).await;
    assert_eq!(repeated.state, "RESNAPSHOT_REQUIRED");
    let (token, head) = reference_clients::begin_resnapshot(&db.pool, &consumer.consumer_id).await;
    assert_eq!(head, requested.log_head);
    assert_eq!(
        reference_clients::ack_resnapshot(&db.pool, &consumer.consumer_id, &token).await,
        "ACTIVE"
    );
}

#[tokio::test]
async fn test_v108_exact_batch_acknowledgement_rolls_back() {
    let (db, consumer, graph) = active_consumer("v108_exact").await;
    db.execute("INSERT INTO v108_exact_source VALUES (2, 'after')")
        .await;
    refresh_graph_without_full(&db.pool, "public.v108_exact_stream", &graph).await;
    let batch = reference_clients::pending_batch(&db.pool, &consumer.consumer_id).await;
    assert_eq!(batch.mode, "EXACT");
    assert_eq!(batch.row_count, 1);

    let payload: (i64, i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT count(*)::bigint, min(ordinal)::bigint, max(ordinal)::bigint \
         FROM {} WHERE batch_token = {} AND action IN ('DELETE', 'INSERT')",
        consumer.delta_relation, batch.batch_token
    )))
    .fetch_one(&db.pool)
    .await
    .expect("typed payload should be readable");
    assert_eq!(payload, (batch.row_count, 0, batch.row_count - 1));

    let mut tx = db.pool.begin().await.expect("begin rollback transaction");
    assert_eq!(
        reference_clients::ack(&mut *tx, &consumer.consumer_id, batch.batch_token).await,
        "APPLIED"
    );
    tx.rollback().await.expect("roll back acknowledgement");
    assert_eq!(
        reference_clients::batch_lag(&db.pool, &consumer.consumer_id).await,
        1
    );
    assert_eq!(
        reference_clients::ack(&db.pool, &consumer.consumer_id, batch.batch_token).await,
        "APPLIED"
    );
}

#[tokio::test]
async fn test_v108_full_invalidation_requires_resynchronized_ack() {
    let (db, consumer, _) = active_consumer("v108_full_invalidation").await;
    assert_eq!(
        reference_clients::qualify(&db.pool, &consumer.consumer_id, "FULL_INVALIDATION").await,
        "FULL_INVALIDATION"
    );
    let batch = reference_clients::pending_batch(&db.pool, &consumer.consumer_id).await;
    assert_eq!(batch.mode, "FULL_INVALIDATION");
    assert_eq!(batch.row_count, 0);
    assert_eq!(
        reference_clients::read_payload(&db.pool, &consumer.delta_relation).await,
        0
    );
    let error = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle.ack_output_delta($1::uuid, $2, 'APPLIED')",
    )
    .bind(&consumer.consumer_id)
    .bind(batch.batch_token)
    .fetch_one(&db.pool)
    .await
    .expect_err("APPLIED must reject a full invalidation");
    assert!(error.to_string().contains("RESYNCHRONIZED"));
    assert_eq!(
        reference_clients::ack_with_disposition(
            &db.pool,
            &consumer.consumer_id,
            batch.batch_token,
            "RESYNCHRONIZED",
        )
        .await,
        "RESYNCHRONIZED"
    );
}

#[tokio::test]
async fn test_v108_qualification_requires_resnapshot_for_contract_mismatch() {
    let (db, consumer, _) = active_consumer("v108_contract_mismatch").await;
    reference_clients::qualify(&db.pool, &consumer.consumer_id, "CONTRACT_MISMATCH").await;
    let status = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
    assert_eq!(status.state, "RESNAPSHOT_REQUIRED");
    assert_eq!(status.state_reason.as_deref(), Some("CONTRACT_MISMATCH"));
}

#[tokio::test]
async fn test_v108_qualification_requires_resnapshot_for_delta_gap() {
    let (db, consumer, _) = active_consumer("v108_delta_gap").await;
    reference_clients::qualify(&db.pool, &consumer.consumer_id, "DELTA_GAP").await;
    let status = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
    assert_eq!(status.state, "RESNAPSHOT_REQUIRED");
    assert_eq!(status.state_reason.as_deref(), Some("DELTA_GAP"));
    let error = sqlx::query("SELECT * FROM pgtrickle.output_delta_batches($1::uuid, NULL)")
        .bind(&consumer.consumer_id)
        .fetch_all(&db.pool)
        .await
        .expect_err("a consumer that requires resnapshot must not read a partial range");
    assert!(error.to_string().contains("RESNAPSHOT_REQUIRED"));
}

#[tokio::test]
async fn test_v108_qualification_rejects_unknown_scenario_and_non_superuser() {
    let (db, consumer, _) = active_consumer("v108_qualification_guards").await;
    let unknown = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle.qualify_output_delta_recovery($1::uuid, 'UNKNOWN')",
    )
    .bind(&consumer.consumer_id)
    .fetch_one(&db.pool)
    .await
    .expect_err("unknown qualification scenarios must be rejected");
    assert!(unknown.to_string().contains("PGT_EXT_TOKEN_INVALID"));

    db.execute("CREATE ROLE v108_qualification_user").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO v108_qualification_user")
        .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.qualify_output_delta_recovery(uuid, text)
             TO v108_qualification_user",
    )
    .await;
    let mut connection = db
        .pool
        .acquire()
        .await
        .expect("acquire qualification connection");
    sqlx::query("SET SESSION AUTHORIZATION v108_qualification_user")
        .execute(&mut *connection)
        .await
        .expect("assume non-superuser identity");
    let denied = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle.qualify_output_delta_recovery($1::uuid, 'INVALIDATED')",
    )
    .bind(&consumer.consumer_id)
    .fetch_one(&mut *connection)
    .await
    .expect_err("non-superusers must not invoke qualification scenarios");
    sqlx::query("RESET SESSION AUTHORIZATION")
        .execute(&mut *connection)
        .await
        .expect("restore superuser identity");
    assert!(denied.to_string().contains("PGT_EXT_AUTHORIZATION"));
}

#[tokio::test]
async fn test_v108_invalidated_consumer_returns_no_partial_batch_range() {
    let (db, consumer, _) = active_consumer("v108_invalidated").await;
    reference_clients::qualify(&db.pool, &consumer.consumer_id, "INVALIDATED").await;
    let rows = sqlx::query("SELECT * FROM pgtrickle.output_delta_batches($1::uuid, NULL)")
        .bind(&consumer.consumer_id)
        .fetch_all(&db.pool)
        .await
        .expect("validation should return no partial range after invalidation");
    assert!(rows.is_empty());
    let status = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
    assert_eq!(status.state, "INVALIDATED");
    assert_eq!(status.state_reason.as_deref(), Some("INVALIDATED"));
}

#[tokio::test]
async fn test_v108_clone_adoption_allows_new_consumer_baseline() {
    let (db, consumer, graph) = active_consumer("v108_clone_delta").await;
    db.execute("INSERT INTO v108_clone_delta_source VALUES (2, 'pending')")
        .await;
    refresh_graph_without_full(&db.pool, "public.v108_clone_delta_stream", &graph).await;
    let pending = reference_clients::pending_batch(&db.pool, &consumer.consumer_id).await;

    db.execute(
        "UPDATE pgtrickle.pgt_capture_instance
            SET database_oid = (database_oid::bigint + 1)::oid
          WHERE singleton",
    )
    .await;
    let _: String = db
        .query_scalar("SELECT pgtrickle.validate_recovery()")
        .await;
    let _: String = db
        .query_scalar("SELECT pgtrickle.recover_capture_instance()")
        .await;

    let invalidated = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
    assert_eq!(invalidated.state, "INVALIDATED");
    assert_eq!(
        invalidated.state_reason.as_deref(),
        Some("DATABASE_INSTANCE_CHANGED")
    );
    let unreadable =
        sqlx::query("SELECT * FROM pgtrickle.output_delta_batches($1::uuid, $2::bigint)")
            .bind(&consumer.consumer_id)
            .bind(pending.batch_token)
            .fetch_all(&db.pool)
            .await
            .expect("invalidated reads return no pre-clone batches");
    assert!(unreadable.is_empty());

    let (token, head) = reference_clients::begin_resnapshot(&db.pool, &consumer.consumer_id).await;
    assert!(head >= pending.batch_token);
    assert_eq!(
        reference_clients::ack_resnapshot(&db.pool, &consumer.consumer_id, &token).await,
        "ACTIVE"
    );
    let active = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
    assert_eq!(active.state, "ACTIVE");
    assert_eq!(active.acknowledged_batch_token, head);
    assert_eq!(active.batch_lag, 0);
}

#[tokio::test]
async fn test_v108_contract_and_row_identity_mismatches_can_resnapshot() {
    let (db, consumer, _) = active_consumer("v108_contract_recovery").await;
    let pgt_id_sql = format!(
        "SELECT pgt_id FROM pgtrickle.pgt_output_delta_consumers \
         WHERE consumer_id = '{}'::uuid",
        consumer.consumer_id
    );
    let pgt_id: i64 = db.query_scalar(&pgt_id_sql).await;

    db.execute(&format!(
        "UPDATE pgtrickle.pgt_output_delta_logs \
         SET output_contract_digest = decode('00', 'hex') WHERE pgt_id = {pgt_id}"
    ))
    .await;
    let mismatch = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
    assert_eq!(mismatch.state, "INVALIDATED");
    assert_eq!(mismatch.state_reason.as_deref(), Some("CONTRACT_MISMATCH"));
    let (token, _) = reference_clients::begin_resnapshot(&db.pool, &consumer.consumer_id).await;
    reference_clients::ack_resnapshot(&db.pool, &consumer.consumer_id, &token).await;

    db.execute(&format!(
        "UPDATE pgtrickle.pgt_output_delta_logs \
         SET row_identity_version = row_identity_version + 1 WHERE pgt_id = {pgt_id}"
    ))
    .await;
    let mismatch = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
    assert_eq!(mismatch.state, "INVALIDATED");
    assert_eq!(
        mismatch.state_reason.as_deref(),
        Some("ROW_IDENTITY_VERSION_MISMATCH")
    );
    let (token, head) = reference_clients::begin_resnapshot(&db.pool, &consumer.consumer_id).await;
    reference_clients::ack_resnapshot(&db.pool, &consumer.consumer_id, &token).await;
    let active = reference_clients::validate_consumer(&db.pool, &consumer.consumer_id).await;
    assert_eq!(active.state, "ACTIVE");
    assert_eq!(active.acknowledged_batch_token, head);
}

#[tokio::test]
async fn test_v108_validation_persists_detected_delta_gap() {
    let (db, consumer, batch) = pending_consumer("v108_detect_gap").await;
    sqlx::query(
        "DELETE FROM pgtrickle.pgt_output_delta_batches \
         WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_output_delta_consumers \
                         WHERE consumer_id = $1::uuid) \
           AND batch_token = $2",
    )
    .bind(&consumer.consumer_id)
    .bind(batch.batch_token)
    .execute(&db.pool)
    .await
    .expect("test corruption should remove one pending batch");
    assert_persistently_invalidated(&db, &consumer, "DELTA_GAP").await;
}

#[tokio::test]
async fn test_v108_validation_persists_detected_payload_mismatch() {
    let (db, consumer, batch) = pending_consumer("v108_detect_payload").await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {} WHERE batch_token = {} AND ordinal = 0",
        consumer.delta_relation, batch.batch_token
    )))
    .execute(&db.pool)
    .await
    .expect("test corruption should remove one typed payload row");
    assert_persistently_invalidated(&db, &consumer, "PAYLOAD_INCONSISTENT").await;
}

#[tokio::test]
async fn test_v108_validation_persists_detected_contract_mismatch() {
    let (db, consumer, _) = active_consumer("v108_detect_contract").await;
    sqlx::query(
        "UPDATE pgtrickle.pgt_output_delta_consumers \
         SET output_contract_digest = decode('00', 'hex') \
         WHERE consumer_id = $1::uuid",
    )
    .bind(&consumer.consumer_id)
    .execute(&db.pool)
    .await
    .expect("test corruption should replace the consumer contract digest");
    assert_persistently_invalidated(&db, &consumer, "CONTRACT_MISMATCH").await;
}

#[tokio::test]
async fn test_v108_validation_persists_detected_row_identity_version_mismatch() {
    let (db, consumer, _) = active_consumer("v108_detect_row_version").await;
    sqlx::query(
        "UPDATE pgtrickle.pgt_output_delta_consumers \
         SET row_identity_version = row_identity_version + 1 \
         WHERE consumer_id = $1::uuid",
    )
    .bind(&consumer.consumer_id)
    .execute(&db.pool)
    .await
    .expect("test corruption should replace the consumer row identity version");
    assert_persistently_invalidated(&db, &consumer, "ROW_IDENTITY_VERSION_MISMATCH").await;
}

#[tokio::test]
async fn test_v108_validation_persists_detected_database_instance_change() {
    let (db, consumer, _) = active_consumer("v108_detect_instance").await;
    sqlx::query(
        "UPDATE pgtrickle.pgt_output_delta_consumers \
         SET database_instance_id = database_instance_id || '-changed' \
         WHERE consumer_id = $1::uuid",
    )
    .bind(&consumer.consumer_id)
    .execute(&db.pool)
    .await
    .expect("test corruption should replace the consumer database instance ID");
    assert_persistently_invalidated(&db, &consumer, "DATABASE_INSTANCE_CHANGED").await;
}
