//! v0.89 private window-state catalog contract.

mod e2e;

use e2e::E2eDb;

async fn enable_row_number_runtime(db: &E2eDb, pgt_id: i64) {
    sqlx::query(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET window_strategy = jsonb_set(jsonb_set(jsonb_set( \
                 window_strategy, '{nodes,0,functions,0,strategy}', '\"ordered_suffix\"'), \
                 '{nodes,0,functions,0,runtime_enabled}', 'true'), \
                 '{nodes,0,functions,0,fallback_reason}', 'null'), \
             needs_reinit = true \
         WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .execute(&db.pool)
    .await
    .expect("enable ROW_NUMBER runtime state");
}

async fn assert_runtime_state_matches_target(db: &E2eDb, pgt_id: i64) -> i64 {
    let (generation, status, row_name, partition_name, no_peer, measured, owner_access): (
        i64,
        String,
        String,
        String,
        bool,
        bool,
        bool,
    ) = sqlx::query_as(
        "SELECT ws.state_generation, ws.status, rows.relname::text, parts.relname::text, \
                ws.peer_relid IS NULL, ws.estimated_bytes > 0, \
                pg_catalog.has_table_privilege(target.relowner, ws.row_relid, 'SELECT') \
                AND pg_catalog.has_table_privilege(target.relowner, ws.partition_relid, 'SELECT') \
         FROM pgtrickle.pgt_window_states ws \
         JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = ws.pgt_id \
         JOIN pg_catalog.pg_class target ON target.oid = st.pgt_relid \
         JOIN pg_catalog.pg_class rows ON rows.oid = ws.row_relid \
         JOIN pg_catalog.pg_class parts ON parts.oid = ws.partition_relid \
         WHERE ws.pgt_id = $1 AND ws.node_ordinal = 0 AND ws.spec_ordinal = 0",
    )
    .bind(pgt_id)
    .fetch_one(&db.pool)
    .await
    .expect("load runtime window state");
    assert_eq!(status, "READY");
    assert!(no_peer, "ROW_NUMBER must not create peer state");
    assert!(measured, "state size was not measured");
    assert!(
        owner_access,
        "stream owner cannot read private window state"
    );
    assert!(
        row_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            && partition_name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "state relation names are not generated identifiers"
    );

    let rows_match: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT NOT EXISTS ( \
             (SELECT __pgt_row_id, id, dept, salary, rn \
              FROM pgtrickle.{row_name} \
              EXCEPT ALL \
              SELECT __pgt_row_id, id, dept, salary, rn \
              FROM public.ws_lifecycle_st) \
             UNION ALL \
             (SELECT __pgt_row_id, id, dept, salary, rn \
              FROM public.ws_lifecycle_st \
              EXCEPT ALL \
              SELECT __pgt_row_id, id, dept, salary, rn \
              FROM pgtrickle.{row_name}) \
         ) AND NOT EXISTS ( \
             SELECT 1 FROM pgtrickle.{row_name} \
             WHERE state_generation IS DISTINCT FROM {generation} \
         )"
    )))
    .fetch_one(&db.pool)
    .await
    .expect("compare row state with target");
    assert!(rows_match);

    let partitions_match: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT NOT EXISTS ( \
             (SELECT dept, row_count FROM pgtrickle.{partition_name} \
              EXCEPT ALL \
              SELECT dept, count(*)::bigint FROM public.ws_lifecycle_st GROUP BY dept) \
             UNION ALL \
             (SELECT dept, count(*)::bigint FROM public.ws_lifecycle_st GROUP BY dept \
              EXCEPT ALL \
              SELECT dept, row_count FROM pgtrickle.{partition_name}) \
         ) AND NOT EXISTS ( \
             SELECT 1 FROM pgtrickle.{partition_name} \
             WHERE state_generation IS DISTINCT FROM {generation} \
         )"
    )))
    .fetch_one(&db.pool)
    .await
    .expect("compare partition state with target");
    assert!(partitions_match);

    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT pg_catalog.pg_get_indexdef(indexrelid) \
         FROM pg_catalog.pg_index \
         WHERE indrelid = ( \
             SELECT row_relid FROM pgtrickle.pgt_window_states \
             WHERE pgt_id = $1 AND node_ordinal = 0 AND spec_ordinal = 0 \
         ) ORDER BY indexrelid",
    )
    .bind(pgt_id)
    .fetch_all(&db.pool)
    .await
    .expect("inspect row-state indexes");
    assert!(indexes.iter().any(|index| {
        index.contains("USING btree (dept, salary DESC, id)")
            || index.contains("USING btree (dept, salary DESC NULLS FIRST, id)")
    }));
    assert!(
        indexes
            .iter()
            .any(|index| index.contains("USING btree (__pgt_row_id)"))
    );
    assert!(indexes.iter().any(|index| {
        index.contains("UNIQUE") && index.contains("USING btree (id) NULLS NOT DISTINCT")
    }));
    generation
}

#[tokio::test]
async fn test_window_state_catalog_is_logged_private_and_fail_closed() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ws_catalog_source (id bigint PRIMARY KEY)")
        .await;
    let pgt_id: i64 = db
        .query_scalar(
            "INSERT INTO pgtrickle.pgt_stream_tables \
             (pgt_relid, pgt_name, pgt_schema, defining_query, defining_search_path, \
              refresh_mode, window_strategy) \
             VALUES ('ws_catalog_source'::regclass, 'ws_catalog_st', 'public', \
                     'SELECT id FROM ws_catalog_source', 'public', 'DIFFERENTIAL', \
                     '{\"schema_version\":1,\"strategy_version\":1,\"query_hash\":0,\
                        \"identity_version\":1,\"semantic_fingerprint\":\"test\",\"nodes\":[]}'::jsonb) \
             RETURNING pgt_id",
        )
        .await;

    let invalid_json = sqlx::query(
        "UPDATE pgtrickle.pgt_stream_tables SET window_strategy = '[]'::jsonb \
         WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .execute(&db.pool)
    .await;
    assert!(
        invalid_json.is_err(),
        "array strategy metadata was accepted"
    );

    let persistence: String = db
        .query_scalar(
            "SELECT relpersistence::text \
             FROM pg_catalog.pg_class \
             WHERE oid = 'pgtrickle.pgt_window_states'::regclass",
        )
        .await;
    assert_eq!(persistence, "p");

    let public_access_revoked: bool = db
        .query_scalar(
            "SELECT NOT EXISTS ( \
                 SELECT 1 \
                 FROM pg_catalog.pg_class c, \
                      LATERAL pg_catalog.aclexplode(COALESCE( \
                          c.relacl, pg_catalog.acldefault('r', c.relowner) \
                      )) acl \
                 WHERE c.oid = 'pgtrickle.pgt_window_states'::regclass \
                   AND acl.grantee = 0 \
             )",
        )
        .await;
    assert!(public_access_revoked);

    let dump_filter: String = db
        .query_scalar(
            "SELECT cfg.condition \
             FROM pg_catalog.pg_extension e, \
                  LATERAL unnest(e.extconfig, e.extcondition) \
                      cfg(relid, condition) \
             WHERE e.extname = 'pg_trickle' \
               AND cfg.relid = 'pgtrickle.pgt_window_states'::regclass",
        )
        .await;
    assert_eq!(dump_filter, "WHERE false");

    sqlx::query(
        "INSERT INTO pgtrickle.pgt_window_states \
         (pgt_id, node_ordinal, spec_ordinal, partition_relid, row_relid, \
          schema_version, strategy_version, query_hash, state_generation, status) \
         VALUES ($1, 0, 0, 'ws_catalog_source'::regclass, \
                 'ws_catalog_source'::regclass, 1, 1, 7, 1, 'READY')",
    )
    .bind(pgt_id)
    .execute(&db.pool)
    .await
    .expect("insert valid window registry row");

    let invalid_status = sqlx::query(
        "INSERT INTO pgtrickle.pgt_window_states \
         (pgt_id, node_ordinal, spec_ordinal, partition_relid, row_relid, \
          schema_version, strategy_version, query_hash, state_generation, status) \
         VALUES ($1, 0, 1, 'ws_catalog_source'::regclass, \
                 'ws_catalog_source'::regclass, 1, 1, 7, 1, 'BROKEN')",
    )
    .bind(pgt_id)
    .execute(&db.pool)
    .await;
    assert!(
        invalid_status.is_err(),
        "invalid registry status was accepted"
    );

    let invalid_size = sqlx::query(
        "INSERT INTO pgtrickle.pgt_window_states \
         (pgt_id, node_ordinal, spec_ordinal, partition_relid, row_relid, \
          schema_version, strategy_version, query_hash, state_generation, status, estimated_bytes) \
         VALUES ($1, 0, 1, 'ws_catalog_source'::regclass, \
                 'ws_catalog_source'::regclass, 1, 1, 7, 1, 'READY', -1)",
    )
    .bind(pgt_id)
    .execute(&db.pool)
    .await;
    assert!(invalid_size.is_err(), "negative state size was accepted");

    sqlx::query("DELETE FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1")
        .bind(pgt_id)
        .execute(&db.pool)
        .await
        .expect("delete stream table");
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtrickle.pgt_window_states WHERE pgt_id = $1")
            .bind(pgt_id)
            .fetch_one(&db.pool)
            .await
            .expect("count remaining window registry rows");
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn test_window_state_build_sync_reinitialize_and_repair_lifecycle() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE ws_lifecycle_source ( \
             id bigint PRIMARY KEY, dept text NOT NULL, salary int NOT NULL \
         )",
    )
    .await;
    db.execute(
        "INSERT INTO ws_lifecycle_source VALUES \
         (1, 'eng', 100), (2, 'eng', 80), (3, 'sales', 90)",
    )
    .await;
    db.create_st(
        "ws_lifecycle_st",
        "SELECT id, dept, salary, \
                row_number() OVER (PARTITION BY dept ORDER BY salary DESC, id) AS rn \
         FROM public.ws_lifecycle_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'ws_lifecycle_st'",
        )
        .await;

    enable_row_number_runtime(&db, pgt_id).await;
    db.refresh_st("ws_lifecycle_st").await;
    let generation = assert_runtime_state_matches_target(&db, pgt_id).await;
    db.refresh_st("ws_lifecycle_st").await;
    assert_eq!(
        assert_runtime_state_matches_target(&db, pgt_id).await,
        generation,
        "no-change refresh advanced the shared generation"
    );

    db.execute_seq(&[
        "UPDATE ws_lifecycle_source SET dept = 'sales', salary = 110 WHERE id = 1",
        "DELETE FROM ws_lifecycle_source WHERE id = 2",
        "INSERT INTO ws_lifecycle_source VALUES (4, 'eng', 120)",
    ])
    .await;
    db.refresh_st("ws_lifecycle_st").await;
    db.assert_st_matches_query(
        "public.ws_lifecycle_st",
        "SELECT id, dept, salary, \
                row_number() OVER (PARTITION BY dept ORDER BY salary DESC, id) AS rn \
         FROM public.ws_lifecycle_source",
    )
    .await;
    assert_eq!(
        assert_runtime_state_matches_target(&db, pgt_id).await,
        generation + 1,
        "differential sync did not advance the shared generation"
    );

    sqlx::query("UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = true WHERE pgt_id = $1")
        .bind(pgt_id)
        .execute(&db.pool)
        .await
        .expect("schedule protected reinitialization");
    db.refresh_st("ws_lifecycle_st").await;
    assert_eq!(
        assert_runtime_state_matches_target(&db, pgt_id).await,
        generation + 2,
        "protected rebuild did not advance the shared generation"
    );

    let (partition_name, row_name): (String, String) = sqlx::query_as(
        "SELECT parts.relname::text, rows.relname::text \
         FROM pgtrickle.pgt_window_states ws \
         JOIN pg_catalog.pg_class parts ON parts.oid = ws.partition_relid \
         JOIN pg_catalog.pg_class rows ON rows.oid = ws.row_relid \
         WHERE ws.pgt_id = $1",
    )
    .bind(pgt_id)
    .fetch_one(&db.pool)
    .await
    .expect("load state names before repair");
    let summary: String = db
        .query_scalar("SELECT pgtrickle.repair_stream_table('ws_lifecycle_st')")
        .await;
    assert!(summary.contains("window state reset"));
    let (registry_rows, relations_gone): (i64, bool) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM pgtrickle.pgt_window_states WHERE pgt_id = $1), \
                pg_catalog.to_regclass($2) IS NULL \
                AND pg_catalog.to_regclass($3) IS NULL",
    )
    .bind(pgt_id)
    .bind(format!("pgtrickle.{partition_name}"))
    .bind(format!("pgtrickle.{row_name}"))
    .fetch_one(&db.pool)
    .await
    .expect("inspect repair cleanup");
    assert_eq!(registry_rows, 0);
    assert!(relations_gone);
}

#[tokio::test]
async fn test_window_state_full_build_over_budget_keeps_target_and_falls_back() {
    let db = E2eDb::new().await.with_extension().await;
    let mut connection = db.pool.acquire().await.expect("acquire test connection");
    sqlx::query("SET pg_trickle.memory_budget_mb = 16")
        .execute(&mut *connection)
        .await
        .expect("set minimum window-state budget");
    sqlx::query(
        "CREATE TABLE public.ws_budget_source ( \
             id bigint PRIMARY KEY, payload text NOT NULL \
         )",
    )
    .execute(&mut *connection)
    .await
    .expect("create budget source");
    sqlx::query(
        "INSERT INTO public.ws_budget_source \
         SELECT id, repeat(md5(id::text), 8) \
         FROM generate_series(1, 80000) id",
    )
    .execute(&mut *connection)
    .await
    .expect("populate budget source");
    sqlx::query(
        "SELECT pgtrickle.create_stream_table( \
             'ws_budget_st', \
             'SELECT id, payload, row_number() OVER (ORDER BY id) AS rn \
              FROM public.ws_budget_source', \
             '1m', \
             'DIFFERENTIAL' \
         )",
    )
    .execute(&mut *connection)
    .await
    .expect("target FULL must survive window-state budget fallback");
    let pgt_id: i64 = sqlx::query_scalar(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ws_budget_st'",
    )
    .fetch_one(&mut *connection)
    .await
    .expect("load budget test pgt_id");
    sqlx::query(
        "UPDATE pgtrickle.pgt_stream_tables SET window_strategy = \
             jsonb_set(jsonb_set(jsonb_set(window_strategy, \
                 '{nodes,0,functions,0,strategy}', '\"ordered_suffix\"'), \
                 '{nodes,0,functions,0,runtime_enabled}', 'true'), \
                 '{nodes,0,functions,0,fallback_reason}', 'null'), \
             needs_reinit = true WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .execute(&mut *connection)
    .await
    .expect("enable budget-test candidate");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('ws_budget_st')")
        .execute(&mut *connection)
        .await
        .expect("target reinitialization must survive state budget fallback");

    let (target_rows, registry_rows, runtime_enabled, fallback_reason): (i64, i64, bool, String) =
        sqlx::query_as(
            "SELECT (SELECT count(*) FROM ws_budget_st), \
                (SELECT count(*) FROM pgtrickle.pgt_window_states ws \
                 WHERE ws.pgt_id = st.pgt_id), \
                (st.window_strategy #>> '{nodes,0,functions,0,runtime_enabled}')::boolean, \
                st.window_strategy #>> '{nodes,0,functions,0,fallback_reason}' \
         FROM pgtrickle.pgt_stream_tables st \
         WHERE st.pgt_name = 'ws_budget_st'",
        )
        .fetch_one(&mut *connection)
        .await
        .expect("inspect budget fallback");
    assert_eq!(target_rows, 80000);
    assert_eq!(registry_rows, 0);
    assert!(!runtime_enabled);
    assert_eq!(fallback_reason, "WINDOW_STATE_BUDGET_EXCEEDED");
}

#[tokio::test]
async fn test_window_rejected_runtime_plan_is_persisted_and_explained() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE ws_plan_source (id bigint PRIMARY KEY, dept text NOT NULL, salary int NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO ws_plan_source VALUES (1, 'eng', 100), (2, 'eng', 80), (3, 'sales', 90)",
    )
    .await;
    db.create_st(
        "ws_plan_st",
        "SELECT id, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, id) AS rn FROM ws_plan_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let (pgt_id, strategy): (i64, serde_json::Value) = sqlx::query_as(
        "SELECT pgt_id, window_strategy FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_schema = 'public' AND pgt_name = 'ws_plan_st'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("load persisted window strategy");
    assert_eq!(strategy["schema_version"], 1);
    assert_eq!(strategy["strategy_version"], 1);
    assert_eq!(strategy["identity_version"], 1);
    let function = &strategy["nodes"][0]["functions"][0];
    assert_eq!(function["kind"], "row_number");
    assert_eq!(function["eligible"], true);
    assert_eq!(function["strategy"], "partition_recompute");
    assert_eq!(function["runtime_enabled"], false);
    assert_eq!(function["fallback_reason"], "WINDOW_RECOMPUTE_CHEAPER");

    let state_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtrickle.pgt_window_states WHERE pgt_id = $1")
            .bind(pgt_id)
            .fetch_one(&db.pool)
            .await
            .expect("count private window state");
    assert_eq!(state_rows, 0, "rejected ROW_NUMBER must not create state");

    db.execute("INSERT INTO ws_plan_source VALUES (4, 'eng', 120)")
        .await;
    db.refresh_st("ws_plan_st").await;
    db.assert_st_matches_query(
        "public.ws_plan_st",
        "SELECT id, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, id) AS rn FROM ws_plan_source",
    )
    .await;

    let (action, full_fallback, reason, detail): (String, bool, String, String) = sqlx::query_as(
        "SELECT action, was_full_fallback, refresh_reason, refresh_reason_detail \
               FROM pgtrickle.pgt_refresh_history \
              WHERE pgt_id = $1 AND status = 'COMPLETED' \
              ORDER BY refresh_id DESC LIMIT 1",
    )
    .bind(pgt_id)
    .fetch_one(&db.pool)
    .await
    .expect("load latest window refresh evidence");
    assert_eq!(action, "DIFFERENTIAL");
    assert!(!full_fallback);
    assert_eq!(reason, "WINDOW_RECOMPUTE_CHEAPER");
    let detail: serde_json::Value = serde_json::from_str(&detail).expect("window reason JSON");
    assert_eq!(detail["strategy"], "partition_recompute");
    assert!(detail["crossover_evidence"].is_null());

    let explanation: serde_json::Value = db
        .query_scalar("SELECT pgtrickle.explain_json('public.ws_plan_st')")
        .await;
    assert_eq!(explanation["window"]["strategy"], strategy);
    assert_eq!(
        explanation["window"]["last_actual_strategy"],
        "partition_recompute"
    );
    assert_eq!(explanation["window"]["last_fallback_reason"], reason);
    assert!(explanation["window"]["crossover_evidence"].is_null());
    assert_eq!(
        explanation["window"]["states"].as_array().map(Vec::len),
        Some(0)
    );
    assert_eq!(explanation["window"]["reinitialization_required"], false);

    let text: String = db
        .query_scalar("SELECT pgtrickle.explain('public.ws_plan_st')")
        .await;
    assert!(text.contains("Window strategy: row_number=partition_recompute"));
    assert!(text.contains("WINDOW_RECOMPUTE_CHEAPER"));

    let planned: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.explain_delta_plan($1) -> 'window_strategy'")
            .bind(pgt_id)
            .fetch_one(&db.pool)
            .await
            .expect("load delta plan window strategy");
    assert_eq!(planned, strategy);
}

#[tokio::test]
async fn test_window_unsupported_frame_and_stale_version_fail_closed() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE ws_frame_source (id bigint PRIMARY KEY, dept text NOT NULL, salary int NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO ws_frame_source VALUES (1, 'eng', 100), (2, 'eng', 80)")
        .await;
    db.create_st(
        "ws_frame_st",
        "SELECT id, dept, salary, SUM(salary) OVER (PARTITION BY dept ORDER BY salary, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS nearby FROM ws_frame_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let (pgt_id, strategy): (i64, serde_json::Value) = sqlx::query_as(
        "SELECT pgt_id, window_strategy FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_schema = 'public' AND pgt_name = 'ws_frame_st'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("load unsupported-frame plan");
    assert_eq!(
        strategy["nodes"][0]["functions"][0]["fallback_reason"],
        "WINDOW_UNSUPPORTED_FRAME"
    );

    db.execute("INSERT INTO ws_frame_source VALUES (3, 'eng', 90)")
        .await;
    db.refresh_st("ws_frame_st").await;
    let (reason, full_fallback): (String, bool) = sqlx::query_as(
        "SELECT refresh_reason, was_full_fallback \
         FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = $1 AND status = 'COMPLETED' \
         ORDER BY refresh_id DESC LIMIT 1",
    )
    .bind(pgt_id)
    .fetch_one(&db.pool)
    .await
    .expect("load unsupported-frame fallback");
    assert_eq!(reason, "WINDOW_UNSUPPORTED_FRAME");
    assert!(!full_fallback);

    sqlx::query(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET window_strategy = jsonb_set(window_strategy, '{strategy_version}', '99') \
         WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .execute(&db.pool)
    .await
    .expect("install stale strategy version");
    db.execute("INSERT INTO ws_frame_source VALUES (4, 'eng', 70)")
        .await;
    let failed = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('public.ws_frame_st')")
        .await;
    assert!(
        failed.is_err(),
        "stale strategy version did not fail closed"
    );
    assert_eq!(db.count("public.ws_frame_st").await, 3);

    sqlx::query("UPDATE pgtrickle.pgt_stream_tables SET window_strategy = $1 WHERE pgt_id = $2")
        .bind(strategy)
        .bind(pgt_id)
        .execute(&db.pool)
        .await
        .expect("restore current strategy version");
    db.refresh_st("ws_frame_st").await;
    db.assert_st_matches_query(
        "public.ws_frame_st",
        "SELECT id, dept, salary, SUM(salary) OVER (PARTITION BY dept ORDER BY salary, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS nearby FROM ws_frame_source",
    )
    .await;
}

#[tokio::test]
async fn test_window_v088_null_strategy_is_lazily_replanned() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ws_legacy_source (id bigint PRIMARY KEY, value int NOT NULL)")
        .await;
    db.execute("INSERT INTO ws_legacy_source VALUES (1, 10), (2, 20)")
        .await;
    db.create_st(
        "ws_legacy_st",
        "SELECT id, value, ROW_NUMBER() OVER (ORDER BY value, id) AS rn FROM ws_legacy_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET window_strategy = NULL \
         WHERE pgt_schema = 'public' AND pgt_name = 'ws_legacy_st'",
    )
    .await;

    db.execute("INSERT INTO ws_legacy_source VALUES (3, 15)")
        .await;
    db.refresh_st("ws_legacy_st").await;
    let (strategy_is_object, needs_reinit, reason): (bool, bool, String) = sqlx::query_as(
        "SELECT jsonb_typeof(window_strategy) = 'object', needs_reinit, refresh_reason \
         FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_schema = 'public' AND pgt_name = 'ws_legacy_st'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("load lazily planned legacy row");
    assert!(strategy_is_object);
    assert!(!needs_reinit);
    assert_eq!(reason, "WINDOW_RECOMPUTE_CHEAPER");
    let state_rows: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_window_states ws \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = 'ws_legacy_st'",
        )
        .await;
    assert_eq!(state_rows, 0);
    db.execute("INSERT INTO ws_legacy_source VALUES (4, 12)")
        .await;
    db.refresh_st("ws_legacy_st").await;
    let state_rows_after_finalize: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_window_states ws \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = 'ws_legacy_st'",
        )
        .await;
    assert_eq!(state_rows_after_finalize, 0);
    db.assert_st_matches_query(
        "public.ws_legacy_st",
        "SELECT id, value, ROW_NUMBER() OVER (ORDER BY value, id) AS rn FROM ws_legacy_source",
    )
    .await;
}
