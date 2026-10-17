//! Graph snapshots must include pending changes from keyless stream-table leaves.

mod e2e;
#[allow(dead_code)]
#[path = "conformance/reference_clients.rs"]
mod reference_clients;

use e2e::E2eDb;

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
