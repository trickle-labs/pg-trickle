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
                        "stable_row_identity_encoder_v3",
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
async fn test_v108_cleanup_overlapping_registration_requires_fresh_resnapshot() {
    let prefix = "v108_cleanup_registration";
    let (db, first, graph) = active_consumer(prefix).await;
    let stream = format!("public.{prefix}_stream");
    let source = format!("public.{prefix}_source");
    let contract = reference_clients::stream_contract_digest(&db.pool, &stream).await;
    db.execute(&format!("INSERT INTO {source} VALUES (2, 'pending')"))
        .await;
    refresh_graph_without_full(&db.pool, &stream, &graph).await;
    let batch = reference_clients::pending_batch(&db.pool, &first.consumer_id).await;
    assert_eq!(batch.row_count, 1);

    // Registration holds the log row while the existing consumer's ack tries
    // to prune the now-acknowledged batch range.
    let mut registration = db.pool.begin().await.expect("begin registration");
    let registration_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *registration)
        .await
        .expect("registration backend PID should be available");
    let late_row = sqlx::query(
        "SELECT consumer_id::text AS consumer_id, delta_relation, state \
         FROM pgtrickle.register_output_delta_consumer(\
             $1::regclass, $2, $3::bytea, 'RESNAPSHOT_REQUIRED')",
    )
    .bind(&stream)
    .bind(format!("{prefix}_late_consumer"))
    .bind(&contract)
    .fetch_one(&mut *registration)
    .await
    .expect("late consumer registration should succeed");
    let late = DeltaRegistration {
        consumer_id: late_row.try_get("consumer_id").expect("consumer ID"),
        delta_relation: late_row.try_get("delta_relation").expect("delta relation"),
        state: late_row.try_get("state").expect("consumer state"),
    };
    assert_eq!(late.state, "RESNAPSHOT_REQUIRED");

    let mut ack_connection = db.pool.acquire().await.expect("acquire ack backend");
    sqlx::query("SET application_name = 'quality_cleanup_registration_ack'")
        .execute(&mut *ack_connection)
        .await
        .expect("name ack backend");
    let first_id = first.consumer_id.clone();
    let batch_token = batch.batch_token;
    let ack = tokio::spawn(async move {
        reference_clients::ack(&mut *ack_connection, &first_id, batch_token).await
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let blocked_by_registration: bool = sqlx::query_scalar(
            "SELECT $1::int = ANY(pg_blocking_pids(pid)) \
               FROM pg_stat_activity \
              WHERE application_name = 'quality_cleanup_registration_ack' \
                AND wait_event_type = 'Lock'",
        )
        .bind(registration_pid)
        .fetch_optional(&db.pool)
        .await
        .expect("inspect witnessed lock wait")
        .unwrap_or(false);
        if blocked_by_registration {
            break;
        }
        assert!(
            !ack.is_finished(),
            "cleanup ack completed before the registration lock was observed"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "cleanup ack never waited for the uncommitted registration"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    registration
        .commit()
        .await
        .expect("commit new consumer registration");
    assert_eq!(
        ack.await.expect("cleanup ack task should complete"),
        "APPLIED"
    );

    let pgt_id: i64 = db
        .query_scalar(&format!(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{prefix}_stream'"
        ))
        .await;
    let retained: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_output_delta_batches \
         WHERE pgt_id = $1 AND batch_token <= $2",
    )
    .bind(pgt_id)
    .bind(batch_token)
    .fetch_one(&db.pool)
    .await
    .expect("read pruned batches");
    assert_eq!(retained, 0, "acknowledged batches should be pruned");
    let late_state: String = sqlx::query_scalar(
        "SELECT state FROM pgtrickle.pgt_output_delta_consumers WHERE consumer_id = $1::uuid",
    )
    .bind(&late.consumer_id)
    .fetch_one(&db.pool)
    .await
    .expect("new consumer should remain registered");
    assert_eq!(late_state, "RESNAPSHOT_REQUIRED");

    let mut baseline = db.pool.begin().await.expect("begin fresh baseline");
    let (token, head) =
        reference_clients::begin_resnapshot(&mut *baseline, &late.consumer_id).await;
    assert_eq!(head, batch_token);
    let stream_rows: Vec<(i32, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, value FROM {stream} ORDER BY id"
    )))
    .fetch_all(&mut *baseline)
    .await
    .expect("read stable stream-table baseline");
    let source_rows: Vec<(i32, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, value FROM {source} ORDER BY id"
    )))
    .fetch_all(&mut *baseline)
    .await
    .expect("read matching source baseline");
    assert_eq!(stream_rows, source_rows);
    assert_eq!(
        reference_clients::ack_resnapshot(&mut *baseline, &late.consumer_id, &token).await,
        "ACTIVE"
    );
    baseline.commit().await.expect("commit fresh baseline");

    db.execute(&format!("INSERT INTO {source} VALUES (3, 'after')"))
        .await;
    refresh_graph_without_full(&db.pool, &stream, &graph).await;
    let next = reference_clients::pending_batch(&db.pool, &late.consumer_id).await;
    assert_eq!(next.batch_token, head + 1);
    assert_eq!(next.row_count, 1);
    let payload: Vec<(String, i32, String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT action, id, value, ordinal FROM {} WHERE batch_token = $1 ORDER BY ordinal",
        late.delta_relation
    )))
    .bind(next.batch_token)
    .fetch_all(&db.pool)
    .await
    .expect("read post-resnapshot delta");
    assert_eq!(payload, vec![("INSERT".into(), 3, "after".into(), 0)]);
    db.assert_st_matches_query(&stream, &format!("SELECT id, value FROM {source}"))
        .await;
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
async fn test_v108_lagging_consumer_retains_exact_history_until_catch_up() {
    let (db, fast, graph) = active_consumer("v108_lagging").await;
    let root = "public.v108_lagging_stream";
    let contract = reference_clients::stream_contract_digest(&db.pool, root).await;
    let slow = reference_clients::register(&db.pool, root, "v108_lagging_slow", &contract).await;
    let (baseline_token, _) =
        reference_clients::begin_resnapshot(&db.pool, &slow.consumer_id).await;
    assert_eq!(
        reference_clients::ack_resnapshot(&db.pool, &slow.consumer_id, &baseline_token).await,
        "ACTIVE"
    );

    db.execute("DELETE FROM v108_lagging_source WHERE id = 1")
        .await;
    db.execute("INSERT INTO v108_lagging_source VALUES (1, 'after')")
        .await;
    refresh_graph_without_full(&db.pool, root, &graph).await;
    let first = reference_clients::pending_batch(&db.pool, &fast.consumer_id).await;
    assert_eq!(first.mode, "EXACT");
    assert_eq!(first.row_count, 2);
    assert_eq!(
        reference_clients::ack(&db.pool, &fast.consumer_id, first.batch_token).await,
        "APPLIED"
    );

    db.execute("INSERT INTO v108_lagging_source VALUES (2, 'new')")
        .await;
    refresh_graph_without_full(&db.pool, root, &graph).await;
    let second = reference_clients::pending_batch(&db.pool, &fast.consumer_id).await;
    assert_eq!(second.mode, "EXACT");
    assert_eq!(second.batch_token, first.batch_token + 1);
    assert_eq!(second.row_count, 1);
    assert_eq!(
        reference_clients::ack(&db.pool, &fast.consumer_id, second.batch_token).await,
        "APPLIED"
    );

    let batches: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT batch_token, mode, row_count \
         FROM pgtrickle.output_delta_batches($1::uuid, NULL) ORDER BY batch_token",
    )
    .bind(&slow.consumer_id)
    .fetch_all(&db.pool)
    .await
    .expect("lagging consumer should retain contiguous batches");
    assert_eq!(
        batches,
        vec![
            (first.batch_token, "EXACT".to_string(), 2),
            (second.batch_token, "EXACT".to_string(), 1),
        ]
    );

    let payload: Vec<(i64, String, i32, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT batch_token, action, id, value FROM {} \
         WHERE batch_token > 0 ORDER BY batch_token, ordinal",
        slow.delta_relation
    )))
    .fetch_all(&db.pool)
    .await
    .expect("lagging consumer should read its typed payload");
    let mut replayed = std::collections::BTreeMap::from([(1, "before".to_string())]);
    for (_, action, id, value) in payload {
        match action.as_str() {
            "DELETE" => assert_eq!(replayed.remove(&id).as_deref(), Some(value.as_str())),
            "INSERT" => assert!(replayed.insert(id, value).is_none()),
            _ => unreachable!("typed payload action is constrained"),
        }
    }
    let source: std::collections::BTreeMap<i32, String> =
        sqlx::query_as("SELECT id, value FROM v108_lagging_source ORDER BY id")
            .fetch_all(&db.pool)
            .await
            .expect("committed source should be queryable")
            .into_iter()
            .collect();
    assert_eq!(replayed, source);
    db.assert_st_matches_query(root, "SELECT id, value FROM v108_lagging_source")
        .await;

    assert_eq!(
        reference_clients::ack(&db.pool, &slow.consumer_id, second.batch_token).await,
        "APPLIED"
    );
    let retained_batches: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgtrickle.pgt_output_delta_batches \
         WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_output_delta_consumers \
                        WHERE consumer_id = $1::uuid)",
    )
    .bind(&slow.consumer_id)
    .fetch_one(&db.pool)
    .await
    .expect("acknowledged batches should be pruned");
    assert_eq!(retained_batches, 0);
    assert_eq!(
        reference_clients::read_payload(&db.pool, &slow.delta_relation).await,
        0
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
