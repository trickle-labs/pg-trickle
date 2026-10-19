//! Graph snapshots must include pending changes from keyless stream-table leaves.

mod e2e;
#[allow(dead_code)]
#[path = "conformance/reference_clients.rs"]
mod reference_clients;

use e2e::E2eDb;

#[tokio::test]
async fn test_graph_join_keeps_row_security_admission_barrier_after_reconciled_baseline() {
    const QUERY: &str =
        "SELECT l.id, r.label FROM public.rls_left l JOIN public.rls_right r USING (id)";
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE ROLE graph_rls_reader").await;
    sqlx::raw_sql(
        "CREATE TABLE rls_left(id int NOT NULL);
         CREATE TABLE rls_right(id int NOT NULL, label text NOT NULL);
         CREATE TABLE rls_hidden(id int NOT NULL);
         INSERT INTO rls_left VALUES (1), (2);
         INSERT INTO rls_right VALUES (1,'old'), (2,'old');
         ALTER TABLE rls_left ENABLE ROW LEVEL SECURITY;
         CREATE POLICY visible_rows ON rls_left FOR SELECT USING
             (NOT EXISTS (SELECT 1 FROM public.rls_hidden h WHERE h.id = rls_left.id));
         GRANT USAGE ON SCHEMA public, pgtrickle TO graph_rls_reader;
         GRANT SELECT ON rls_left, rls_right, rls_hidden TO graph_rls_reader",
    )
    .execute(&db.pool)
    .await
    .expect("create policy-controlled inputs");
    sqlx::query("SELECT pgtrickle.create_stream_table(name => 'rls_join', query => $1, schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false, orchestration_mode => 'EXTERNAL')")
        .bind(QUERY).execute(&db.pool).await.expect("create row-security graph");
    let root = "public.rls_join";
    let digest = reference_clients::graph_digest(&db.pool, root).await;
    reference_clients::refresh_graph(&db.pool, root, &digest).await;
    db.execute("UPDATE rls_right SET label = 'warm'").await;
    reference_clients::refresh_graph_without_full(&db.pool, root, &digest).await;
    let before: String = db
        .query_scalar(
            "SELECT frontier::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rls_join'",
        )
        .await;
    db.execute("ALTER TABLE rls_join OWNER TO graph_rls_reader")
        .await;
    let digest = reference_clients::graph_digest(&db.pool, root).await;
    // This policy dependency is absent from the defining SQL's captured delta.
    db.execute("INSERT INTO rls_hidden VALUES (1)").await;
    db.execute("UPDATE rls_right SET label = 'new' WHERE id = 2")
        .await;
    let error = sqlx::query("SELECT graph_refresh_id FROM pgtrickle.refresh_graph_strict(ARRAY[$1::regclass], $2::bytea, 'ERROR')")
        .bind(root).bind(&digest).execute(&db.pool).await.expect_err("effective RLS must retain its admission barrier");
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("0A000")
    );
    assert!(error.to_string().contains("row-level security"));
    let actual: Vec<(i32, String)> = sqlx::query_as("SELECT id, label FROM rls_join ORDER BY id")
        .fetch_all(&db.pool)
        .await
        .expect("read materialized policy result");
    let mut tx = db.pool.begin().await.expect("begin owner reference query");
    sqlx::query("SET LOCAL ROLE graph_rls_reader")
        .execute(&mut *tx)
        .await
        .expect("assume defining owner");
    let mut expected: Vec<(i32, String)> = sqlx::query_as(QUERY)
        .fetch_all(&mut *tx)
        .await
        .expect("evaluate complete policy-controlled query");
    expected.sort();
    tx.rollback().await.expect("restore reference role");
    assert_eq!(expected, vec![(2, "new".to_string())]);
    assert_eq!(
        actual,
        vec![(1, "warm".to_string()), (2, "warm".to_string())]
    );
    let after: String = db
        .query_scalar(
            "SELECT frontier::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rls_join'",
        )
        .await;
    assert_eq!(
        after, before,
        "rejected policy change must not advance the frontier"
    );
}

#[tokio::test]
async fn test_graph_rowwise_join_delivers_bulk_duplicates_across_mutation_cycles() {
    use std::collections::BTreeMap;
    type Row = (i32, i32, Option<String>);
    const QUERY: &str = "SELECT l.id, g.value, r.label FROM public.point_left l
        CROSS JOIN LATERAL generate_series(1, l.n) g(value)
        JOIN public.point_right r ON r.id = l.id";
    async fn rows(db: &E2eDb, query: &str) -> Vec<Row> {
        let mut rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(query.to_string()))
            .fetch_all(&db.pool)
            .await
            .expect("read complete rowwise result");
        rows.sort();
        rows
    }
    'profiles: for (extra, exact_allowed, reduced_join_limits, nested, three_inputs) in [
        ("", Some(true), false, false, false),
        (
            ", current_setting('server_version') AS runtime_setting",
            None,
            false,
            false,
            false,
        ),
        (
            ", (SELECT 1) AS scalar_guard",
            Some(false),
            false,
            false,
            false,
        ),
        ("", Some(false), true, false, false),
        ("", Some(false), false, true, false),
        ("", Some(true), false, false, true),
    ] {
        let reference_query = if three_inputs {
            format!(
                "{} JOIN public.point_dim d1 ON d1.id = r.id",
                QUERY.replacen("SELECT l.id", "SELECT r.id + 0 AS id", 1)
            )
        } else if nested {
            "SELECT l.id, l.n AS value, r.label FROM public.point_left l JOIN public.point_right r ON r.id = l.id JOIN public.point_dim d1 ON d1.id = l.id JOIN public.point_dim d2 ON d2.id = l.id".to_string()
        } else {
            QUERY.to_string()
        };
        let query = reference_query.replacen("r.label FROM", &format!("r.label{extra} FROM"), 1);
        let mut db = E2eDb::new().await.with_extension().await;
        if reduced_join_limits {
            let options = db.pool.connect_options().as_ref().clone().options([
                ("pg_trickle.deep_join_l0_scan_threshold", "1"),
                ("pg_trickle.part3_max_scan_count", "1"),
            ]);
            db.pool.close().await;
            db.pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect_with(options)
                .await
                .expect("connect with reduced join limits");
        }
        sqlx::raw_sql(
            "CREATE TABLE point_left (id int NOT NULL, n int NOT NULL);
        CREATE TABLE point_right (id int NOT NULL, label text);
        CREATE TABLE point_dim (id int NOT NULL);
        INSERT INTO point_left VALUES (1,2), (2,1);
        INSERT INTO point_right VALUES (1,'old'), (2,NULL);
        INSERT INTO point_dim VALUES (1), (2)",
        )
        .execute(&db.pool)
        .await
        .expect("create rowwise sources");
        for (name, query) in [
            ("point_join", query.as_str()),
            (
                "point_terminal",
                "SELECT id, value, label FROM public.point_join",
            ),
        ] {
            let created = sqlx::query("SELECT pgtrickle.create_stream_table(name => $1, query => $2, schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false, orchestration_mode => 'EXTERNAL')")
                .bind(name).bind(query).execute(&db.pool).await;
            if exact_allowed.is_none() {
                assert!(
                    created
                        .expect_err("stable expressions must retain their admission barrier")
                        .to_string()
                        .contains("STABLE")
                );
                continue 'profiles;
            }
            created.expect("create rowwise graph");
        }
        let root = "public.point_terminal";
        let digest = reference_clients::graph_digest(&db.pool, root).await;
        reference_clients::refresh_graph(&db.pool, root, &digest).await;
        let contract = reference_clients::stream_contract_digest(&db.pool, root).await;
        let consumer = reference_clients::register(&db.pool, root, root, &contract).await;
        let mut tx = db.pool.begin().await.expect("begin rowwise baseline");
        let (token, _) = reference_clients::begin_resnapshot(&mut *tx, &consumer.consumer_id).await;
        reference_clients::ack_resnapshot(&mut *tx, &consumer.consumer_id, &token).await;
        tx.commit().await.expect("commit rowwise baseline");
        for (cycle, mutation) in [
        "INSERT INTO point_left VALUES (1,2), (1,2); UPDATE point_right SET label = 'new' WHERE id = 1",
        "DELETE FROM point_right WHERE id = 1",
        "INSERT INTO point_right VALUES (1,NULL), (1,NULL)",
        "DELETE FROM point_left WHERE ctid IN (SELECT ctid FROM point_left WHERE id = 1 LIMIT 2)",
    ].into_iter().enumerate() {
        let before = rows(&db, &reference_query).await;
        sqlx::raw_sql(mutation).execute(&db.pool).await.expect("commit simultaneous rowwise changes");
        if (nested || three_inputs) && cycle == 0 {
            db.execute("INSERT INTO point_dim VALUES (1)").await;
        }
        let after = rows(&db, &reference_query).await;
        let mut weights = BTreeMap::<Row, i64>::new();
        for row in before { *weights.entry(row).or_default() -= 1; }
        for row in after.clone() { *weights.entry(row).or_default() += 1; }
        let mut expected = Vec::new();
        for ((id, value, label), weight) in weights {
            for _ in 0..weight.unsigned_abs() {
                expected.push((if weight < 0 { "DELETE".to_string() } else { "INSERT".to_string() }, id, value, label.clone()));
            }
        }
        expected.sort();
        let mut tx = db.pool.begin().await.expect("begin exact rowwise refresh");
        reference_clients::refresh_graph_without_full(&mut *tx, root, &digest).await;
        let batch = reference_clients::pending_batch(&mut *tx, &consumer.consumer_id).await;
        let mut actual: Vec<(String, i32, i32, Option<String>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT action, id, value, label FROM {} WHERE batch_token = $1", consumer.delta_relation
        ))).bind(batch.batch_token).fetch_all(&mut *tx).await.expect("read every rowwise delta occurrence");
        actual.sort();
        assert_eq!(batch.mode, "EXACT");
        assert_eq!(actual, expected, "cycle={cycle}, extra={extra}, reduced_limits={reduced_join_limits}, nested={nested}, three_inputs={three_inputs}");
        assert_eq!(batch.row_count, actual.len() as i64);
        reference_clients::ack(&mut *tx, &consumer.consumer_id, batch.batch_token).await;
        tx.commit().await.expect("commit rowwise refresh and exact acknowledgement");
        assert_eq!(rows(&db, "SELECT id, value, label FROM point_join").await, after);
        assert_eq!(rows(&db, "SELECT id, value, label FROM point_terminal").await, after);
        let strategy: String = db.query_scalar("SELECT merge_strategy_used FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'point_join' AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1").await;
            if exact_allowed == Some(true) {
            assert_eq!(strategy, if cycle == 0 { "explicit_dml_reconciled" } else { "explicit_dml_exact_join" }, "cycle={cycle}, three_inputs={three_inputs}");
        } else {
            assert_ne!(strategy, "explicit_dml_exact_join", "extra={extra}, reduced_limits={reduced_join_limits}");
        }
    }
        reference_clients::refresh_graph_without_full(&db.pool, root, &digest).await;
        assert_eq!(
            rows(&db, "SELECT id, value, label FROM point_terminal").await,
            rows(&db, &reference_query).await
        );
    }
}

#[tokio::test]
async fn test_graph_large_join_delta_preserves_null_duplicates_rollback_and_delivery() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(
        "CREATE TABLE recon_rows (value text, active bool DEFAULT true, level smallint DEFAULT 42, payload bytea DEFAULT decode('00ff', 'hex'), typed_value varchar, key_uuid uuid DEFAULT '00000000-0000-0000-0000-000000000001'::uuid);
         INSERT INTO recon_rows(value) VALUES (NULL), (NULL), ('kept');
         CREATE TABLE recon_dim (id int PRIMARY KEY, label text);
         INSERT INTO recon_dim VALUES (1, 'before')",
    )
    .execute(&db.pool)
    .await
    .expect("create join sources");
    let joins = (1..=8)
        .map(|i| format!(" JOIN dim_leaf d{i} ON d{i}.id = 1"))
        .collect::<String>();
    let columns = (1..=8)
        .map(|i| format!(", d{i}.label AS label{i}"))
        .collect::<String>();
    let query = format!(
        "SELECT r.value AS value{columns}, r.active, r.level, r.payload, r.typed_value, r.key_uuid FROM row_leaf r{joins}"
    );
    let count_query =
        format!("SELECT count(r.value IS NULL)::bigint AS total_rows FROM row_leaf r{joins}");
    let group_query =
        format!("SELECT r.value, count(*) AS copies FROM row_leaf r{joins} GROUP BY r.value");
    let terminal_query = format!(
        "SELECT j.value, {}, j.active, j.level, j.payload, j.typed_value, j.key_uuid FROM recon_join j LEFT JOIN recon_count c ON false LEFT JOIN recon_groups g ON false",
        (1..=8)
            .map(|i| format!("j.label{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    for (name, query) in [
        (
            "row_leaf",
            "SELECT value, active, level, payload, typed_value, key_uuid FROM recon_rows",
        ),
        ("dim_leaf", "SELECT id, label FROM recon_dim"),
        ("recon_join", query.as_str()),
        ("recon_count", count_query.as_str()),
        ("recon_groups", group_query.as_str()),
        ("recon_terminal", terminal_query.as_str()),
    ] {
        sqlx::query("SELECT pgtrickle.create_stream_table(name => $1, query => $2, schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false, orchestration_mode => 'EXTERNAL')")
            .bind(name).bind(query).execute(&db.pool).await.expect("create join graph");
    }
    let root = "public.recon_terminal";
    let digest = reference_clients::graph_digest(&db.pool, root).await;
    reference_clients::refresh_graph(&db.pool, root, &digest).await;
    db.execute("INSERT INTO recon_rows(value) VALUES ('bootstrap')")
        .await;
    reference_clients::refresh_graph_without_full(&db.pool, root, &digest).await;
    db.assert_st_matches_query("recon_join", &query).await;
    db.assert_st_matches_query("recon_terminal", &query).await;
    db.execute("DELETE FROM recon_rows WHERE value = 'bootstrap'")
        .await;
    reference_clients::refresh_graph_without_full(&db.pool, root, &digest).await;
    let count_strategy: String = db.query_scalar("SELECT merge_strategy_used FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'recon_count' AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert_eq!(
        count_strategy, "reconciliation_delta",
        "scalar COUNT bigint projection should reuse comparison"
    );
    sqlx::raw_sql(
        "CREATE UNIQUE INDEX recon_count_rows_key ON recon_count(total_rows);
        CREATE TABLE recon_foreign_ref (id int PRIMARY KEY, total_rows bigint NOT NULL
            REFERENCES recon_count(total_rows) ON UPDATE CASCADE ON DELETE CASCADE);
        INSERT INTO recon_foreign_ref VALUES (1,3)",
    )
    .execute(&db.pool)
    .await
    .expect("install UPDATE-sensitive constraint");
    let digest = reference_clients::graph_digest(&db.pool, root).await;
    let mut consumers = Vec::new();
    for relation in ["public.recon_join", root] {
        let contract = reference_clients::stream_contract_digest(&db.pool, relation).await;
        let consumer = reference_clients::register(&db.pool, relation, relation, &contract).await;
        let mut tx = db.pool.begin().await.expect("begin baseline");
        let (token, _) = reference_clients::begin_resnapshot(&mut *tx, &consumer.consumer_id).await;
        reference_clients::ack_resnapshot(&mut *tx, &consumer.consumer_id, &token).await;
        tx.commit().await.expect("ack baseline");
        consumers.push(consumer);
    }
    db.execute("INSERT INTO recon_rows(value) VALUES (NULL)")
        .await;
    db.execute("UPDATE recon_rows SET active = false, level = 43, payload = decode('ff00', 'hex'), typed_value = 'after', key_uuid = '00000000-0000-0000-0000-000000000002'::uuid").await;
    db.execute("UPDATE recon_dim SET label = 'after'").await;
    let before: String = db
        .query_scalar(
            "SELECT frontier::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'recon_join'",
        )
        .await;
    for commit in [false, true] {
        let mut tx = db.pool.begin().await.expect("begin exact refresh");
        sqlx::query("SET LOCAL enable_nestloop = off")
            .execute(&mut *tx)
            .await
            .expect("set caller planner preference");
        reference_clients::refresh_graph_without_full(&mut *tx, root, &digest).await;
        let no_snapshot: bool = sqlx::query_scalar("SELECT to_regclass(format('pg_temp.__pgt_pre_%s', pgt_id)) IS NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'recon_join'")
            .fetch_one(&mut *tx).await.expect("inspect exact replacement capture");
        assert!(
            no_snapshot,
            "exact bag replacement must reuse its delta for capture"
        );
        let nestloop: String = sqlx::query_scalar("SHOW enable_nestloop")
            .fetch_one(&mut *tx)
            .await
            .expect("read caller planner preference");
        assert_eq!(nestloop, "off");
        let referenced_rows: i64 =
            sqlx::query_scalar("SELECT total_rows FROM recon_foreign_ref WHERE id = 1")
                .fetch_one(&mut *tx)
                .await
                .expect("aggregate UPDATE must preserve the referencing row");
        assert_eq!(referenced_rows, 4);
        for consumer in &consumers {
            let batch = reference_clients::pending_batch(&mut *tx, &consumer.consumer_id).await;
            let rows: Vec<(String, Option<String>, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT action, value, label8 FROM {} WHERE batch_token = $1 ORDER BY action, value NULLS FIRST", consumer.delta_relation
            ))).bind(batch.batch_token).fetch_all(&mut *tx).await.expect("read exact join delta");
            assert_eq!(batch.mode, "EXACT");
            assert_eq!(batch.row_count, 7);
            assert_eq!(
                rows,
                vec![
                    ("DELETE".into(), None, "before".into()),
                    ("DELETE".into(), None, "before".into()),
                    ("DELETE".into(), Some("kept".into()), "before".into()),
                    ("INSERT".into(), None, "after".into()),
                    ("INSERT".into(), None, "after".into()),
                    ("INSERT".into(), None, "after".into()),
                    ("INSERT".into(), Some("kept".into()), "after".into()),
                ]
            );
            reference_clients::ack(&mut *tx, &consumer.consumer_id, batch.batch_token).await;
        }
        if commit {
            tx.commit().await.expect("commit exact delivery");
        } else {
            tx.rollback()
                .await
                .expect("rollback refresh and acknowledgements");
            let after: String = db.query_scalar("SELECT frontier::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'recon_join'").await;
            assert_eq!(after, before);
            assert_eq!(db.count("recon_terminal").await, 3);
            assert_eq!(
                db.query_scalar::<i64>("SELECT total_rows FROM recon_foreign_ref WHERE id=1")
                    .await,
                3
            );
        }
    }
    db.assert_st_matches_query("recon_join", &query).await;
    db.assert_st_matches_query("recon_terminal", &query).await;
    assert_eq!(
        db.query_scalar::<i64>("SELECT total_rows FROM recon_count")
            .await,
        4
    );
    let groups: Vec<(Option<String>, i64, i64)> = sqlx::query_as(
        "SELECT value, copies, __pgt_count FROM recon_groups ORDER BY value NULLS FIRST",
    )
    .fetch_all(&db.pool)
    .await
    .expect("read group maintenance state");
    assert_eq!(groups, vec![(None, 3, 3), (Some("kept".into()), 1, 1)]);
    let strategy: String = db.query_scalar("SELECT merge_strategy_used FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'recon_join' AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert_eq!(strategy, "reconciliation_delta");
}

#[tokio::test]
async fn test_graph_keyless_snapshot_preserves_duplicate_multiplicity() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE source (value INT NOT NULL)").await;
    db.execute("INSERT INTO source VALUES (1), (1)").await;
    db.execute("CREATE TABLE lookup (id INT PRIMARY KEY, label TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO lookup VALUES (1, 'label')").await;
    db.execute("CREATE TABLE categories (id INT PRIMARY KEY, category TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO categories VALUES (1, 'old')").await;
    for (name, query) in [
        (
            "enriched",
            "SELECT n.value, b.label, c.category FROM source n JOIN lookup b ON n.value = b.id JOIN lookup b2 ON b2.id = b.id JOIN lookup b3 ON b3.id = b.id JOIN categories c ON c.id = b.id",
        ),
        ("terminal", "SELECT value, label, category FROM enriched"),
    ] {
        sqlx::query("SELECT pgtrickle.create_stream_table(name => $1, query => $2, schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false, orchestration_mode => 'EXTERNAL')")
            .bind(name).bind(query).execute(&db.pool).await.expect("create graph member");
    }
    let root = "public.terminal";
    let digest = reference_clients::graph_digest(&db.pool, root).await;
    let mut consumers = Vec::new();
    for relation in ["public.enriched", root] {
        let contract = reference_clients::stream_contract_digest(&db.pool, relation).await;
        consumers.push(reference_clients::register(&db.pool, relation, relation, &contract).await);
    }
    reference_clients::refresh_graph(&db.pool, root, &digest).await;
    let mut tx = db.pool.begin().await.expect("begin baseline");
    for consumer in &consumers {
        let (token, _) = reference_clients::begin_resnapshot(&mut *tx, &consumer.consumer_id).await;
        assert_eq!(
            reference_clients::ack_resnapshot(&mut *tx, &consumer.consumer_id, &token).await,
            "ACTIVE"
        );
    }
    tx.commit().await.expect("commit baseline");
    db.execute("INSERT INTO source VALUES (1)").await;
    db.execute("UPDATE categories SET category = 'new'").await;
    let mut tx = db.pool.begin().await.expect("begin mutation refresh");
    sqlx::query("SET LOCAL pg_trickle.refresh_strategy = 'differential'")
        .execute(&mut *tx)
        .await
        .expect("select differential strategy");
    reference_clients::refresh_graph_without_full(&mut *tx, root, &digest).await;
    for consumer in &consumers {
        let batch = reference_clients::pending_batch(&mut *tx, &consumer.consumer_id).await;
        let payload: Vec<(String, i32, String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT action, value, label, category FROM {} WHERE batch_token = $1 ORDER BY action, category", consumer.delta_relation
        ))).bind(batch.batch_token).fetch_all(&mut *tx).await.expect("read complete duplicate delta");
        assert_eq!(
            (batch.mode.as_str(), batch.row_count, payload),
            (
                "EXACT",
                5,
                vec![
                    ("DELETE".to_owned(), 1, "label".to_owned(), "old".to_owned()),
                    ("DELETE".to_owned(), 1, "label".to_owned(), "old".to_owned()),
                    ("INSERT".to_owned(), 1, "label".to_owned(), "new".to_owned()),
                    ("INSERT".to_owned(), 1, "label".to_owned(), "new".to_owned()),
                    ("INSERT".to_owned(), 1, "label".to_owned(), "new".to_owned()),
                ]
            )
        );
        assert_eq!(
            reference_clients::ack(&mut *tx, &consumer.consumer_id, batch.batch_token).await,
            "APPLIED"
        );
    }
    let rows: Vec<(i32, String, String)> = sqlx::query_as(
        "SELECT value, label, category FROM terminal ORDER BY value, label, category",
    )
    .fetch_all(&mut *tx)
    .await
    .expect("read complete duplicate output");
    assert_eq!(rows, vec![(1, "label".to_owned(), "new".to_owned()); 3]);
    tx.commit()
        .await
        .expect("commit exact refresh and acknowledgement");
}

#[tokio::test]
async fn test_graph_keyless_snapshots_preserve_exact_connect_split_and_delete() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE source (id INT PRIMARY KEY, value TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO source VALUES (1, 'same'), (2, 'same')")
        .await;
    for (name, query) in [
        ("normalized", "SELECT id, value FROM source"),
        (
            "raw_pairs",
            "SELECT l.id AS left_id, r.id AS right_id FROM normalized l JOIN normalized r ON l.value = r.value AND l.id < r.id",
        ),
        (
            "pair_set",
            "SELECT left_id, right_id FROM raw_pairs GROUP BY left_id, right_id",
        ),
        (
            "enriched",
            "SELECT p.left_id, p.right_id, l.value AS left_value, r.value AS right_value FROM pair_set p LEFT JOIN normalized l ON l.id = p.left_id LEFT JOIN normalized r ON r.id = p.right_id",
        ),
        (
            "terminal",
            "SELECT left_id, right_id, left_value, right_value FROM enriched",
        ),
    ] {
        sqlx::query("SELECT pgtrickle.create_stream_table(name => $1, query => $2, schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false, orchestration_mode => 'EXTERNAL')")
            .bind(name)
            .bind(query)
            .execute(&db.pool)
            .await
            .expect("create graph member");
    }
    let root = "public.terminal";
    let digest = reference_clients::graph_digest(&db.pool, root).await;
    let mut consumers = Vec::new();
    for relation in ["public.enriched", root] {
        let contract = reference_clients::stream_contract_digest(&db.pool, relation).await;
        consumers.push(reference_clients::register(&db.pool, relation, relation, &contract).await);
    }
    reference_clients::refresh_graph(&db.pool, root, &digest).await;
    let mut tx = db.pool.begin().await.expect("begin baseline");
    for consumer in &consumers {
        let (token, _) = reference_clients::begin_resnapshot(&mut *tx, &consumer.consumer_id).await;
        assert_eq!(
            reference_clients::ack_resnapshot(&mut *tx, &consumer.consumer_id, &token).await,
            "ACTIVE"
        );
    }
    tx.commit().await.expect("commit baseline");

    for (mutation, action, expected_rows) in [
        (
            "UPDATE source SET value = 'other' WHERE id = 2",
            Some("DELETE"),
            Vec::new(),
        ),
        (
            "UPDATE source SET value = 'same' WHERE id = 2",
            Some("INSERT"),
            vec![(1, 2, "same".to_owned(), "same".to_owned())],
        ),
        (
            "DELETE FROM source WHERE id = 2",
            Some("DELETE"),
            Vec::new(),
        ),
        ("INSERT INTO source VALUES (3, 'other')", None, Vec::new()),
    ] {
        db.execute(mutation).await;
        let mut tx = db.pool.begin().await.expect("begin mutation refresh");
        sqlx::query("SET LOCAL pg_trickle.refresh_strategy = 'differential'")
            .execute(&mut *tx)
            .await
            .expect("select differential strategy");
        reference_clients::refresh_graph_without_full(&mut *tx, root, &digest).await;
        for consumer in &consumers {
            let batch = reference_clients::pending_batch(&mut *tx, &consumer.consumer_id).await;
            let payload: Vec<(String, i32, i32, String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT action, left_id, right_id, left_value, right_value FROM {} WHERE batch_token = $1 ORDER BY ordinal",
                consumer.delta_relation
            ))).bind(batch.batch_token).fetch_all(&mut *tx).await.expect("read full delta payload");
            let expected_payload = action
                .map(|action| {
                    vec![(
                        action.to_owned(),
                        1,
                        2,
                        "same".to_owned(),
                        "same".to_owned(),
                    )]
                })
                .unwrap_or_default();
            assert_eq!(
                (batch.mode.as_str(), batch.row_count, payload),
                ("EXACT", expected_payload.len() as i64, expected_payload),
                "{mutation}"
            );
            assert_eq!(
                reference_clients::ack(&mut *tx, &consumer.consumer_id, batch.batch_token).await,
                "APPLIED"
            );
        }
        let rows: Vec<(i32, i32, String, String)> = sqlx::query_as("SELECT left_id, right_id, left_value, right_value FROM terminal ORDER BY left_id, right_id")
            .fetch_all(&mut *tx).await.expect("read complete terminal output");
        assert_eq!(rows, expected_rows, "{mutation}");
        tx.commit()
            .await
            .expect("commit exact refresh and acknowledgement");
    }
}
