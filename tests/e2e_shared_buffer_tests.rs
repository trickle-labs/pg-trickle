//! E2E tests for D-4: Shared Change Buffers.
//!
//! Validates that multiple stream tables referencing the same source table
//! share a single change buffer (`pgtrickle_changes.changes_{oid}`), with
//! correct multi-frontier cleanup coordination.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_cdc_deferred_replacement_preserves_complete_payload() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    for mode in ["row", "statement"] {
        let source = format!("deferred_{mode}");
        let wide = format!("{source}_rows");
        let counts = format!("{source}_counts");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY DEFERRABLE INITIALLY DEFERRED, v INT, n NUMERIC, j JSON)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES (1,10,1.0,'{{\"x\":1}}'),(2,20,NULL,NULL)"
        ))
        .await;
        let wide_query = format!("SELECT id,v,n,j FROM {source}");
        let count_query = format!("SELECT COUNT(*) AS n,SUM(v) AS total FROM {source}");
        for (name, query) in [(&wide, &wide_query), (&counts, &count_query)] {
            db.try_execute_with_config(&[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")], &format!("SELECT pgtrickle.create_stream_table('{name}',$q${query}$q$,'24h','DIFFERENTIAL')")).await.unwrap();
        }
        let script = format!(
            "BEGIN; INSERT INTO {source} VALUES (1,99,1.00,'{{\"x\":2}}'); UPDATE {source} SET v=v+1 WHERE id=1; DELETE FROM {source} WHERE id=1 AND v=11; SAVEPOINT s; UPDATE {source} SET v=0 WHERE id=2; ROLLBACK TO s; INSERT INTO {source} VALUES (3,5,NULL,NULL); UPDATE {source} SET v=6 WHERE id=3; DELETE FROM {source} WHERE id=3; COMMIT"
        );
        sqlx::raw_sql(sqlx::AssertSqlSafe(script))
            .execute(&db.pool)
            .await
            .unwrap();
        for (name, query) in [(&wide, &wide_query), (&counts, &count_query)] {
            let before: i64 = db
                .query_scalar(
                    "SELECT coalesce(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history",
                )
                .await;
            db.try_execute_with_config(
                &[
                    "SET pg_trickle.refresh_strategy='differential'",
                    "SET pg_trickle.differential_max_change_ratio=0",
                ],
                &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
            )
            .await
            .unwrap();
            let changed: bool = db.query_scalar(&format!("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='{name}' AND h.refresh_id>{before} AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback)")).await;
            assert!(
                changed,
                "deferred replacement must execute differential maintenance"
            );
            e2e::oracle::assert_st_query_exact(
                &db,
                name,
                query,
                "deferred replacement and genuine transient cancellation",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn test_cdc_committed_history_preserves_latest_complete_payload() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("committed_history_{mode}");
        let output = format!("{source}_rows");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, v INT, n NUMERIC, j JSON)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES (1,10,1.0,'{{\"revision\":0}}')"
        ))
        .await;

        let query = format!("SELECT id,v,n,j FROM {source}");
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{output}',$q${query}$q$,'24h','DIFFERENTIAL')"
            ),
        )
        .await
        .unwrap();

        // Each statement commits separately before the consumer advances its cut.
        db.execute(&format!(
            "UPDATE {source} SET v=11,n=1.00,j='{{\"revision\":1}}' WHERE id=1"
        ))
        .await;
        db.execute(&format!(
            "UPDATE {source} SET v=12,n=2.00,j='{{\"revision\":2}}' WHERE id=1"
        ))
        .await;
        db.execute(&format!("DELETE FROM {source} WHERE id=1"))
            .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES (1,99,9.00,'{{\"revision\":99}}')"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES (2,20,2.00,'{{\"transient\":true}}')"
        ))
        .await;
        db.execute(&format!("DELETE FROM {source} WHERE id=2"))
            .await;

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let images: i64 = db
            .query_scalar(&format!(
                "SELECT count(*)::bigint FROM {buffer} WHERE action IN ('I','D')"
            ))
            .await;
        let transactions: i64 = db
            .query_scalar(&format!(
                "SELECT count(DISTINCT source_xid::text)::bigint FROM {buffer} \
                 WHERE action IN ('I','D')"
            ))
            .await;
        assert_eq!(
            images, 8,
            "six separately committed DML statements capture eight images"
        );
        assert_eq!(
            transactions, 6,
            "each committed statement retains its transaction identity"
        );

        let before: i64 = db
            .query_scalar("SELECT coalesce(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{output}')"),
        )
        .await
        .unwrap();
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{output}' AND h.refresh_id>{before} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            differential,
            "the committed history must be reduced differentially"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &output,
            &query,
            "multiple committed replacements and transient cancellation in one consumer window",
        )
        .await;
    }
}

#[tokio::test]
async fn test_cdc_compaction_keeps_final_insert_after_insert_update() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET log_min_messages = 'info'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("insert_update_compaction_{mode}");
        let output = format!("{source}_rows");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
        ))
        .await;
        db.execute(&format!("INSERT INTO {source} VALUES (1, 'existing')"))
            .await;

        let query = format!("SELECT id,value FROM {source}");
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{output}',$q${query}$q$,'24h','DIFFERENTIAL')"
            ),
        )
        .await
        .unwrap();
        assert_eq!(db.count(&output).await, 1, "initial baseline is populated");

        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "BEGIN; INSERT INTO {source} VALUES (2, 'a_intermediate'); \
             UPDATE {source} SET value='z_final' WHERE id=2; COMMIT"
        )))
        .execute(&db.pool)
        .await
        .unwrap();

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let captured: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT action,value FROM {buffer} WHERE id=2 AND action IN ('I','D') \
             ORDER BY change_id"
        )))
        .fetch_all(&db.pool)
        .await
        .unwrap();
        assert_eq!(
            captured,
            vec![
                ("I".to_string(), "a_intermediate".to_string()),
                ("D".to_string(), "a_intermediate".to_string()),
                ("I".to_string(), "z_final".to_string()),
            ],
            "one committed insert then update must capture its complete I/D/I history"
        );
        let transaction_count: i64 = db
            .query_scalar(&format!(
                "SELECT count(DISTINCT source_xid::text)::bigint FROM {buffer} \
                 WHERE id=2 AND action IN ('I','D')"
            ))
            .await;
        assert_eq!(
            transaction_count, 1,
            "all captured images share one source transaction"
        );

        let before: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{output}')"),
        )
        .await
        .unwrap();
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{output}' AND h.refresh_id>{before} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            differential,
            "the captured history must use differential compaction"
        );
        let logs = tokio::process::Command::new("docker")
            .args(["logs", db.container_id()])
            .output()
            .await
            .expect("read PostgreSQL logs to witness compaction");
        let logs = format!(
            "{}\n{}",
            String::from_utf8_lossy(&logs.stdout),
            String::from_utf8_lossy(&logs.stderr)
        );
        assert!(
            logs.contains(&format!(
                "C-4: compacted changes_{source_oid} — removed 2 rows"
            )),
            "expected the production base-buffer compactor to remove the superseded insert and its matching delete"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &output,
            &query,
            "compacted insert-update history retains only the final inserted image",
        )
        .await;

        let upstream_source = format!("{source}_st_source");
        let upstream = format!("{source}_upstream");
        let downstream = format!("{source}_downstream");
        db.execute(&format!(
            "CREATE TABLE {upstream_source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {upstream_source} VALUES (1, 'existing')"
        ))
        .await;
        let upstream_query = format!("SELECT id,value FROM {upstream_source}");
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{upstream}',$q${upstream_query}$q$,'24h','DIFFERENTIAL')"
            ),
        )
        .await
        .unwrap();
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{downstream}',$q$SELECT id,value FROM {upstream}$q$,'24h','DIFFERENTIAL')"
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            db.count(&downstream).await,
            1,
            "downstream baseline is populated"
        );

        db.execute(&format!(
            "INSERT INTO {upstream_source} VALUES (2, 'a_intermediate')"
        ))
        .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1000",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{upstream}')"),
        )
        .await
        .unwrap();
        db.execute(&format!(
            "UPDATE {upstream_source} SET value='z_final' WHERE id=2"
        ))
        .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1000",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{upstream}')"),
        )
        .await
        .unwrap();

        let upstream_id: i64 = db
            .query_scalar(&format!(
                "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name='{upstream}'"
            ))
            .await;
        let st_buffer = format!("pgtrickle_changes.changes_pgt_{upstream_id}");
        let st_captured: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT action,value FROM {st_buffer} WHERE id=2 AND action IN ('I','D') \
             ORDER BY change_id"
        )))
        .fetch_all(&db.pool)
        .await
        .unwrap();
        assert_eq!(
            st_captured,
            vec![
                ("I".to_string(), "a_intermediate".to_string()),
                ("D".to_string(), "a_intermediate".to_string()),
                ("I".to_string(), "z_final".to_string()),
            ],
            "the stream-table buffer must retain the complete I/D/I history before reduction"
        );
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{downstream}')"),
        )
        .await
        .unwrap();
        let st_compact_seen = tokio::process::Command::new("docker")
            .args(["logs", db.container_id()])
            .output()
            .await
            .expect("read PostgreSQL logs to witness ST compaction");
        let st_compact_seen = format!(
            "{}\n{}",
            String::from_utf8_lossy(&st_compact_seen.stdout),
            String::from_utf8_lossy(&st_compact_seen.stderr)
        );
        assert!(
            st_compact_seen.contains(&format!(
                "DAG-5: compacted changes_pgt_{upstream_id} — removed 2 rows"
            )),
            "expected the production ST-buffer compactor to remove both superseded events"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &downstream,
            &format!("SELECT id,value FROM {upstream}"),
            "compacted ST insert-update history retains only the final inserted image",
        )
        .await;
    }
}

#[tokio::test]
async fn test_cdc_compaction_matches_all_short_valid_row_histories() {
    fn extend_paths(
        histories: &mut Vec<Vec<Option<&'static str>>>,
        path: &mut Vec<Option<&'static str>>,
        remaining: usize,
    ) {
        if remaining == 0 {
            histories.push(path.clone());
            return;
        }
        let current = path[path.len() - 1];
        for next in [None, Some("a"), Some("b")] {
            if next != current {
                path.push(next);
                extend_paths(histories, path, remaining - 1);
                path.pop();
            }
        }
    }

    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET log_min_messages = 'info'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    let mut histories = Vec::new();
    for initial in [None, Some("a"), Some("b")] {
        for transitions in 1..=4 {
            extend_paths(&mut histories, &mut vec![initial], transitions);
        }
    }
    assert_eq!(histories.len(), 90, "all paths through four transitions");

    for mode in ["row", "statement"] {
        let source = format!("ranked_compaction_{mode}");
        let output = format!("{source}_rows");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, value TEXT NOT NULL)"
        ))
        .await;
        let initial_rows = histories
            .iter()
            .enumerate()
            .filter_map(|(index, path)| path[0].map(|value| format!("({}, '{value}')", index + 1)))
            .collect::<Vec<_>>();
        db.execute(&format!(
            "INSERT INTO {source} VALUES {}",
            initial_rows.join(",")
        ))
        .await;

        let query = format!("SELECT id,value FROM {source}");
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{output}',$q${query}$q$,'24h','DIFFERENTIAL')"
            ),
        )
        .await
        .unwrap();

        for transition in 1..=4 {
            let statements = histories
                .iter()
                .enumerate()
                .filter_map(|(index, path)| {
                    if path.len() <= transition {
                        return None;
                    }
                    let id = index + 1;
                    match (path[transition - 1], path[transition]) {
                        (None, Some(next)) => {
                            Some(format!("INSERT INTO {source} VALUES ({id}, '{next}')"))
                        }
                        (Some(_), None) => Some(format!("DELETE FROM {source} WHERE id={id}")),
                        (Some(_), Some(next)) => {
                            Some(format!("UPDATE {source} SET value='{next}' WHERE id={id}"))
                        }
                        (None, None) => None,
                    }
                })
                .collect::<Vec<_>>();
            if !statements.is_empty() {
                let transaction = format!("BEGIN; {}; COMMIT", statements.join(";"));
                sqlx::raw_sql(sqlx::AssertSqlSafe(transaction))
                    .execute(&db.pool)
                    .await
                    .unwrap();
            }
        }

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        for (index, path) in histories.iter().enumerate() {
            let mut expected = Vec::new();
            for transition in 1..path.len() {
                match (path[transition - 1], path[transition]) {
                    (None, Some(next)) => expected.push(("I".to_string(), next.to_string())),
                    (Some(previous), None) => {
                        expected.push(("D".to_string(), previous.to_string()))
                    }
                    (Some(previous), Some(next)) => {
                        expected.push(("D".to_string(), previous.to_string()));
                        expected.push(("I".to_string(), next.to_string()));
                    }
                    (None, None) => unreachable!("generated histories have no no-op transition"),
                }
            }
            let actual: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT action,value FROM {buffer} WHERE id={} AND action IN ('I','D') ORDER BY change_id",
                index + 1
            )))
            .fetch_all(&db.pool)
            .await
            .unwrap();
            assert_eq!(actual, expected, "captured history for row {}", index + 1);

            let transactions: i64 = db
                .query_scalar(&format!(
                    "SELECT count(DISTINCT source_xid::text)::bigint FROM {buffer} \
                     WHERE id={} AND action IN ('I','D')",
                    index + 1
                ))
                .await;
            assert_eq!(
                transactions,
                (path.len() - 1) as i64,
                "every state transition for row {} commits atomically",
                index + 1
            );
        }

        let before: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{output}')"),
        )
        .await
        .unwrap();
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{output}' AND h.refresh_id>{before} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(differential, "all cases must use differential compaction");

        let logs = tokio::process::Command::new("docker")
            .args(["logs", db.container_id()])
            .output()
            .await
            .expect("read PostgreSQL logs to witness compaction");
        let logs = format!(
            "{}\n{}",
            String::from_utf8_lossy(&logs.stdout),
            String::from_utf8_lossy(&logs.stderr)
        );
        assert!(
            logs.contains(&format!("C-4: compacted changes_{source_oid} — removed ")),
            "the production base-buffer compactor must prune redundant ranked events"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &output,
            &query,
            "all short valid row histories after ranked compaction",
        )
        .await;
    }
}

#[tokio::test]
async fn test_cdc_compaction_immediate_key_changes_preserve_per_identity_endpoints() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET log_min_messages = 'info'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("moving_key_{mode}");
        let output = format!("{source}_rows");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, payload TEXT NOT NULL)"
        ))
        .await;
        db.execute(&format!("INSERT INTO {source} VALUES (1, 'initial')"))
            .await;
        let query = format!("SELECT id, payload FROM {source}");
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{output}',$q${query}$q$,'24h','DIFFERENTIAL')"
            ),
        )
        .await
        .unwrap();

        db.execute(&format!(
            "UPDATE {source} SET id=2, payload='intermediate' WHERE id=1"
        ))
        .await;
        db.execute(&format!(
            "UPDATE {source} SET id=3, payload='final' WHERE id=2"
        ))
        .await;

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let captured: Vec<(String, i32, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT action,id,payload FROM {buffer} WHERE action IN ('I','D') ORDER BY change_id"
        )))
        .fetch_all(&db.pool)
        .await
        .unwrap();
        assert_eq!(
            captured,
            vec![
                ("D".to_string(), 1, "initial".to_string()),
                ("I".to_string(), 2, "intermediate".to_string()),
                ("D".to_string(), 2, "intermediate".to_string()),
                ("I".to_string(), 3, "final".to_string()),
            ],
            "each immediate-key update must capture complete images under their own identities"
        );

        let before: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{output}')"),
        )
        .await
        .unwrap();
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                 WHERE s.pgt_name='{output}' AND h.refresh_id>{before} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(differential, "key changes must use differential compaction");

        let logs = tokio::process::Command::new("docker")
            .args(["logs", db.container_id()])
            .output()
            .await
            .expect("read PostgreSQL logs to witness immediate-key compaction");
        let logs = format!(
            "{}\n{}",
            String::from_utf8_lossy(&logs.stdout),
            String::from_utf8_lossy(&logs.stderr)
        );
        assert!(
            logs.contains(&format!(
                "C-4: compacted changes_{source_oid} — removed 2 rows"
            )),
            "the intermediate identity's I/D pair must be pruned"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &output,
            &query,
            "immediate primary-key changes retain the initial delete and final insert",
        )
        .await;
    }
}

#[tokio::test]
async fn test_cdc_immediate_key_move_preserves_protected_consumer_cut() {
    async fn refresh_differential(db: &E2eDb, name: &str) {
        let before: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
        )
        .await
        .unwrap();
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                 WHERE s.pgt_name='{name}' AND h.refresh_id>{before} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(differential, "{name} must refresh differentially");
    }

    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET log_min_messages = 'info'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("moving_key_cut_{mode}");
        let fast = format!("{source}_fast");
        let slow = format!("{source}_slow");
        let query = format!("SELECT id, payload FROM {source}");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, payload TEXT NOT NULL)"
        ))
        .await;
        db.execute(&format!("INSERT INTO {source} VALUES (1, 'initial')"))
            .await;
        for name in [&fast, &slow] {
            db.try_execute_with_config(
                &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
                &format!(
                    "SELECT pgtrickle.create_stream_table('{name}',$q${query}$q$,'24h','DIFFERENTIAL')"
                ),
            )
            .await
            .unwrap();
        }

        db.execute(&format!(
            "UPDATE {source} SET id=2, payload='intermediate' WHERE id=1"
        ))
        .await;
        refresh_differential(&db, &fast).await;
        e2e::oracle::assert_st_query_exact(
            &db,
            &fast,
            "SELECT 2::int AS id, 'intermediate'::text AS payload",
            "fast consumer materializes the intermediate primary key",
        )
        .await;
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            "SELECT 1::int AS id, 'initial'::text AS payload",
            "slow consumer remains at the earlier protected cut",
        )
        .await;

        db.execute(&format!(
            "UPDATE {source} SET id=3, payload='final' WHERE id=2"
        ))
        .await;
        refresh_differential(&db, &slow).await;
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            &query,
            "slow consumer catches up across the intermediate identity",
        )
        .await;

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let retained: Vec<(String, i32, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT action,id,payload FROM {buffer} WHERE action IN ('D','I') ORDER BY change_id"
        )))
        .fetch_all(&db.pool)
        .await
        .unwrap();
        assert!(
            retained.iter().any(|(action, id, payload)| {
                action == "D" && *id == 2 && payload == "intermediate"
            }),
            "the delete after the fast consumer's cut must survive slow-window processing"
        );

        refresh_differential(&db, &fast).await;
        for name in [&fast, &slow] {
            e2e::oracle::assert_st_query_exact(
                &db,
                name,
                &query,
                "both consumers reach the final key after protected-cut catch-up",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn test_cdc_interleaved_identity_history_preserves_complete_bag() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("interleaved_images_{mode}");
        let output = format!("{source}_rows");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, category TEXT, amount NUMERIC, metadata TEXT)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES \
             (1, 'alpha', 1.0, '{{\"image\":\"a\"}}'), \
             (2, 'beta', 2.00, '{{\"image\":\"b\"}}')"
        ))
        .await;

        // Exclude the source key from the output so different identities can
        // contribute to the same complete row-image bucket in the output bag.
        let query = format!(
            "SELECT category, amount, scale(amount) AS amount_scale, metadata FROM {source}"
        );
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{output}',$q${query}$q$,'24h','DIFFERENTIAL')"
            ),
        )
        .await
        .unwrap();

        // Each statement commits independently. The rows exchange and then
        // revisit complete images while their source identities stay distinct.
        db.execute(&format!(
            "UPDATE {source} SET \
               category = CASE id WHEN 1 THEN 'beta' ELSE 'gamma' END, \
               amount = CASE id WHEN 1 THEN 2.00 ELSE 3.00 END, \
               metadata = CASE id WHEN 1 THEN '{{\"image\":\"b\"}}' \
                                  ELSE '{{\"image\":\"c\"}}' END"
        ))
        .await;
        db.execute(&format!(
            "UPDATE {source} SET \
               category = CASE id WHEN 1 THEN 'gamma' ELSE 'alpha' END, \
               amount = CASE id WHEN 1 THEN 3.00 ELSE 1.0 END, \
               metadata = CASE id WHEN 1 THEN '{{\"image\":\"c\"}}' \
                                  ELSE '{{\"image\":\"a\"}}' END"
        ))
        .await;
        db.execute(&format!(
            "UPDATE {source} SET \
               category = CASE id WHEN 1 THEN 'delta' ELSE 'beta' END, \
               amount = CASE id WHEN 1 THEN 4.0 ELSE 2.00 END, \
               metadata = CASE id WHEN 1 THEN '{{\"image\":\"d\"}}' \
                                  ELSE '{{\"image\":\"b\"}}' END"
        ))
        .await;

        let before: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{output}')"),
        )
        .await
        .unwrap();
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{output}' AND h.refresh_id>{before} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            differential,
            "interleaved committed identities should use differential compaction"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &output,
            &query,
            "interleaved identities sharing complete row-image buckets",
        )
        .await;
    }
}

#[tokio::test]
async fn test_cdc_compaction_preserves_multi_identity_bag_delta() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("multi_identity_compact_{mode}");
        let output = format!("{source}_rows");
        let query = format!("SELECT payload FROM {source}");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, payload TEXT NOT NULL)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES \
             (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'c'),(6,'c')"
        ))
        .await;
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{output}',$q${query}$q$,'24h','DIFFERENTIAL')"
            ),
        )
        .await
        .unwrap();

        // Every row changes in every committed update. Distinct source
        // identities repeatedly share complete output-image buckets.
        for update in [
            "UPDATE SOURCE SET payload = CASE id \
             WHEN 1 THEN 'b' WHEN 2 THEN 'c' WHEN 3 THEN 'a' \
             WHEN 4 THEN 'd' WHEN 5 THEN 'd' WHEN 6 THEN 'a' END",
            "UPDATE SOURCE SET payload = CASE id \
             WHEN 1 THEN 'd' WHEN 2 THEN 'd' WHEN 3 THEN 'c' \
             WHEN 4 THEN 'a' WHEN 5 THEN 'b' WHEN 6 THEN 'e' END",
            "UPDATE SOURCE SET payload = CASE id \
             WHEN 1 THEN 'e' WHEN 2 THEN 'e' WHEN 3 THEN 'a' \
             WHEN 4 THEN 'b' WHEN 5 THEN 'c' WHEN 6 THEN 'd' END",
        ] {
            db.execute(&update.replace("SOURCE", &source)).await;
        }

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let pending_before: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        assert_eq!(
            pending_before, 36,
            "six identities across three updates must capture 36 signed events"
        );

        let before: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{output}')"),
        )
        .await
        .unwrap();
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{output}' AND h.refresh_id>{before} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            differential,
            "multi-identity compaction must stay differential"
        );

        let pending_after: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        assert_eq!(
            pending_after, 12,
            "ranked compaction must retain the first delete and final insert for each identity"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &output,
            &query,
            "six-identity complete-image bag after ranked compaction",
        )
        .await;
    }
}

#[tokio::test]
async fn test_cdc_interleaved_identity_history_preserves_protected_cuts() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("interleaved_cuts_{mode}");
        let fast = format!("{source}_fast");
        let slow = format!("{source}_slow");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, category TEXT, amount NUMERIC, metadata TEXT)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES \
             (1, 'alpha', 1.0, '{{\"image\":\"a\"}}'), \
             (2, 'beta', 2.00, '{{\"image\":\"b\"}}')"
        ))
        .await;

        // Different source identities can contribute to the same output image.
        let query = format!(
            "SELECT category, amount, scale(amount) AS amount_scale, metadata FROM {source}"
        );
        for name in [&fast, &slow] {
            db.try_execute_with_config(
                &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
                &format!(
                    "SELECT pgtrickle.create_stream_table('{name}',$q${query}$q$,'24h','DIFFERENTIAL')"
                ),
            )
            .await
            .unwrap();
        }

        // The slow consumer holds the cut after the first committed image exchange.
        db.execute(&format!(
            "UPDATE {source} SET \
               category = CASE id WHEN 1 THEN 'beta' ELSE 'gamma' END, \
               amount = CASE id WHEN 1 THEN 2.00 ELSE 3.00 END, \
               metadata = CASE id WHEN 1 THEN '{{\"image\":\"b\"}}' \
                                  ELSE '{{\"image\":\"c\"}}' END"
        ))
        .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{slow}')"),
        )
        .await
        .unwrap();
        let slow_cut = "SELECT * FROM (VALUES \
             ('beta'::text,2.00::numeric,2::int,'{\"image\":\"b\"}'::text), \
             ('gamma'::text,3.00::numeric,2::int,'{\"image\":\"c\"}'::text) \
             ) AS expected(category,amount,amount_scale,metadata)";
        e2e::oracle::assert_st_query_exact(&db, &slow, slow_cut, "slow consumer at first cut")
            .await;

        // Later committed updates revisit images from the first window while the
        // fast consumer crosses both windows and the slow consumer stays behind.
        db.execute(&format!(
            "UPDATE {source} SET \
               category = CASE id WHEN 1 THEN 'gamma' ELSE 'alpha' END, \
               amount = CASE id WHEN 1 THEN 3.00 ELSE 1.0 END, \
               metadata = CASE id WHEN 1 THEN '{{\"image\":\"c\"}}' \
                                  ELSE '{{\"image\":\"a\"}}' END"
        ))
        .await;
        db.execute(&format!(
            "UPDATE {source} SET \
               category = CASE id WHEN 1 THEN 'delta' ELSE 'beta' END, \
               amount = CASE id WHEN 1 THEN 4.0 ELSE 2.00 END, \
               metadata = CASE id WHEN 1 THEN '{{\"image\":\"d\"}}' \
                                  ELSE '{{\"image\":\"b\"}}' END"
        ))
        .await;

        let before_fast: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{fast}')"),
        )
        .await
        .unwrap();
        let fast_differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{fast}' AND h.refresh_id>{before_fast} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            fast_differential,
            "fast consumer must cross both cuts differentially"
        );

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let retained_for_slow: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        assert!(
            retained_for_slow >= 8,
            "both interleaved identities' later images must remain for the slow consumer"
        );
        e2e::oracle::assert_st_query_exact(&db, &fast, &query, "fast consumer after both windows")
            .await;
        e2e::oracle::assert_st_query_exact(&db, &slow, slow_cut, "slow consumer retains first cut")
            .await;

        let before_slow: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{slow}')"),
        )
        .await
        .unwrap();
        let slow_differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{slow}' AND h.refresh_id>{before_slow} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            slow_differential,
            "slow consumer must catch up differentially"
        );
        for name in [&fast, &slow] {
            e2e::oracle::assert_st_query_exact(
                &db,
                name,
                &query,
                "both consumers after protected-window catch-up",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn test_cdc_many_identity_histories_preserve_staggered_consumer_cuts() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    async fn refresh_differential(db: &E2eDb, name: &str) {
        let before: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
        )
        .await
        .unwrap();
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{name}' AND h.refresh_id>{before} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            differential,
            "{name} must refresh differentially without fallback"
        );
    }

    for mode in ["row", "statement"] {
        let source = format!("many_identity_cuts_{mode}");
        let fast = format!("{source}_fast");
        let slow = format!("{source}_slow");
        let query = format!("SELECT payload FROM {source}");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, payload TEXT NOT NULL)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES \
             (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'c'),(6,'c')"
        ))
        .await;
        for name in [&fast, &slow] {
            db.try_execute_with_config(
                &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
                &format!(
                    "SELECT pgtrickle.create_stream_table('{name}',$q${query}$q$,'24h','DIFFERENTIAL')"
                ),
            )
            .await
            .unwrap();
        }

        // Six source identities repeatedly share the same visible complete-row
        // images. The slow consumer takes the first cut while the fast consumer
        // advances twice through overlapping image buckets.
        db.execute(&format!(
            "UPDATE {source} SET payload = CASE id \
             WHEN 1 THEN 'b' WHEN 2 THEN 'c' WHEN 3 THEN 'a' \
             WHEN 4 THEN 'd' WHEN 5 THEN 'd' WHEN 6 THEN 'a' END"
        ))
        .await;
        refresh_differential(&db, &slow).await;
        let slow_first_cut = "SELECT * FROM (VALUES \
            ('b'::text),('c'::text),('a'::text),('d'::text),('d'::text),('a'::text)\
            ) AS expected(payload)";
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            slow_first_cut,
            "six identities at the protected first consumer cut",
        )
        .await;

        db.execute(&format!(
            "UPDATE {source} SET payload = CASE id \
             WHEN 1 THEN 'd' WHEN 2 THEN 'd' WHEN 3 THEN 'c' \
             WHEN 4 THEN 'a' WHEN 5 THEN 'b' WHEN 6 THEN 'e' END"
        ))
        .await;
        refresh_differential(&db, &fast).await;
        let fast_second_cut = "SELECT * FROM (VALUES \
            ('d'::text),('d'::text),('c'::text),('a'::text),('b'::text),('e'::text)\
            ) AS expected(payload)";
        e2e::oracle::assert_st_query_exact(
            &db,
            &fast,
            fast_second_cut,
            "fast consumer crossing two committed multi-row histories",
        )
        .await;

        db.execute(&format!(
            "UPDATE {source} SET payload = CASE id \
             WHEN 1 THEN 'e' WHEN 2 THEN 'e' WHEN 3 THEN 'a' \
             WHEN 4 THEN 'b' WHEN 5 THEN 'c' WHEN 6 THEN 'd' END"
        ))
        .await;
        refresh_differential(&db, &fast).await;
        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let retained: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        assert!(
            retained >= 24,
            "the two later six-row histories must remain available to the slow consumer"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &fast,
            &query,
            "fast consumer at the latest six-identity cut",
        )
        .await;
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            slow_first_cut,
            "slow consumer remains at its earlier protected cut",
        )
        .await;

        refresh_differential(&db, &slow).await;
        for name in [&fast, &slow] {
            e2e::oracle::assert_st_query_exact(
                &db,
                name,
                &query,
                "both consumers after six-identity catch-up",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn test_cdc_deferred_key_swap_preserves_complete_images() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("key_swap_{mode}");
        let output = format!("{source}_rows");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY DEFERRABLE INITIALLY DEFERRED, v TEXT, n NUMERIC, j JSON)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES (1,'alpha',1.0,'{{\"x\":1}}'),(2,'beta',2.0,'{{\"x\":2}}')"
        ))
        .await;
        let query = format!("SELECT id,v,n,j FROM {source}");
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{output}',$q${query}$q$,'24h','DIFFERENTIAL')"
            ),
        )
        .await
        .unwrap();

        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "BEGIN; SET CONSTRAINTS ALL DEFERRED; UPDATE {source} \
             SET id=CASE id WHEN 1 THEN 2 ELSE 1 END WHERE id IN (1,2); COMMIT"
        )))
        .execute(&db.pool)
        .await
        .unwrap();

        let before_refresh: i64 = db
            .query_scalar("SELECT coalesce(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{output}')"),
        )
        .await
        .unwrap();
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
             WHERE s.pgt_name='{output}' AND h.refresh_id>{before_refresh} \
             AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            differential,
            "deferred key swap must execute differential maintenance"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &output,
            &query,
            "deferred primary-key swap with complete payload images",
        )
        .await;
    }
}

#[tokio::test]
async fn test_cdc_deferred_key_swap_preserves_protected_consumer_cuts() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("window_key_swap_{mode}");
        let fast = format!("{source}_fast");
        let slow = format!("{source}_slow");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY DEFERRABLE INITIALLY DEFERRED, v NUMERIC, label TEXT)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES (1,1.0,'alpha'),(2,2.0,'beta')"
        ))
        .await;

        let fast_query = format!("SELECT id,v,scale(v) AS v_scale,label FROM {source}");
        let slow_query = format!("SELECT id,label FROM {source}");
        for (name, query) in [(&fast, &fast_query), (&slow, &slow_query)] {
            db.try_execute_with_config(
                &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
                &format!(
                    "SELECT pgtrickle.create_stream_table('{name}',$q${query}$q$,'24h','DIFFERENTIAL')"
                ),
            )
            .await
            .unwrap();
        }

        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "BEGIN; SET CONSTRAINTS ALL DEFERRED; UPDATE {source} \
             SET id=CASE id WHEN 1 THEN 2 ELSE 1 END WHERE id IN (1,2); COMMIT"
        )))
        .execute(&db.pool)
        .await
        .unwrap();

        let before_fast: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{fast}')"),
        )
        .await
        .unwrap();
        let fast_differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{fast}' AND h.refresh_id>{before_fast} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            fast_differential,
            "fast consumer must execute differential work"
        );

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let retained_for_slow: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM {buffer}"))
            .await;
        assert!(
            retained_for_slow >= 4,
            "the slow consumer's deferred swap images must remain after compaction is considered"
        );

        e2e::oracle::assert_st_query_exact(
            &db,
            &fast,
            &fast_query,
            "fast consumer after deferred key swap",
        )
        .await;
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            "SELECT 1::int AS id,'alpha'::text AS label \
             UNION ALL SELECT 2::int,'beta'::text",
            "slow consumer remains at its original cut",
        )
        .await;

        db.execute(&format!(
            "UPDATE {source} SET v=1.00,label='alpha-new' WHERE id=2"
        ))
        .await;
        let before_slow: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{slow}')"),
        )
        .await
        .unwrap();
        let slow_differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{slow}' AND h.refresh_id>{before_slow} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            slow_differential,
            "slow consumer must execute differential catch-up"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &fast,
            "SELECT 2::int AS id,1.0::numeric AS v,1::int AS v_scale,'alpha'::text AS label \
             UNION ALL SELECT 1::int,2.0::numeric,1::int,'beta'::text",
            "fast consumer remains at the deferred-swap cut",
        )
        .await;
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            &slow_query,
            "slow consumer catches up through the payload update",
        )
        .await;

        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{fast}')"),
        )
        .await
        .unwrap();
        for (name, query) in [(&fast, &fast_query), (&slow, &slow_query)] {
            e2e::oracle::assert_st_query_exact(
                &db,
                name,
                query,
                "both consumers converge after protected-window catch-up",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn test_cdc_shared_source_skip_compaction_preserves_interior_consumer_cut() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("interior_cut_{mode}");
        let fast = format!("{source}_fast");
        let slow = format!("{source}_slow");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, v INT, label TEXT)"
        ))
        .await;
        db.execute(&format!("INSERT INTO {source} VALUES (1,1,'initial')"))
            .await;

        let query = format!("SELECT id,v,label FROM {source}");
        for name in [&fast, &slow] {
            db.try_execute_with_config(
                &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
                &format!(
                    "SELECT pgtrickle.create_stream_table('{name}',$q${query}$q$,'24h','DIFFERENTIAL')"
                ),
            )
            .await
            .unwrap();
        }

        // Advance the slow consumer to an interior cut while the fast
        // consumer still starts at the initial snapshot.
        db.execute(&format!(
            "UPDATE {source} SET v=2,label='middle' WHERE id=1"
        ))
        .await;
        let before_slow: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{slow}')"),
        )
        .await
        .unwrap();
        let slow_advanced: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{slow}' AND h.refresh_id>{before_slow} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            slow_advanced,
            "slow consumer must advance to the middle cut"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            "SELECT 1::int AS id,2::int AS v,'middle'::text AS label",
            "slow consumer at its interior cut",
        )
        .await;

        // With two consumers, production excludes this buffer from keyed
        // compaction. All four D/I images must remain for the slow consumer's
        // protected suffix.
        db.execute(&format!("UPDATE {source} SET v=3,label='final' WHERE id=1"))
            .await;
        let before_fast: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{fast}')"),
        )
        .await
        .unwrap();
        let fast_advanced: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{fast}' AND h.refresh_id>{before_fast} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(fast_advanced, "fast consumer must span both changes");

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let retained_change_images: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        let retained_snapshot_markers: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM {buffer} WHERE action = 'S'"))
            .await;
        assert_eq!(
            retained_change_images, 4,
            "shared consumer frontier must retain all four D/I images across the interior cut"
        );
        assert_eq!(
            retained_snapshot_markers, 1,
            "the source snapshot marker remains in its protected history"
        );

        e2e::oracle::assert_st_query_exact(&db, &fast, &query, "fast consumer after both changes")
            .await;
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            "SELECT 1::int AS id,2::int AS v,'middle'::text AS label",
            "slow consumer remains at its interior cut",
        )
        .await;

        let before_slow_catchup: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{slow}')"),
        )
        .await
        .unwrap();
        let slow_caught_up: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{slow}' AND h.refresh_id>{before_slow_catchup} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(slow_caught_up, "slow consumer must consume its own suffix");
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            &query,
            "slow consumer after protected-window catch-up",
        )
        .await;
    }
}

#[tokio::test]
async fn test_cdc_keyless_shared_consumers_retain_three_protected_windows() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        let source = format!("keyless_cut_{mode}");
        let fast = format!("{source}_fast");
        let middle = format!("{source}_middle");
        let slow = format!("{source}_slow");
        db.execute(&format!("CREATE TABLE {source} (v INT, label TEXT)"))
            .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES \
             (1,'duplicate'),(1,'duplicate'),(NULL,NULL),(NULL,NULL)"
        ))
        .await;

        let query = format!("SELECT v,label FROM {source}");
        for name in [&fast, &middle, &slow] {
            db.try_execute_with_config(
                &[&format!("SET pg_trickle.cdc_trigger_mode='{mode}'")],
                &format!(
                    "SELECT pgtrickle.create_stream_table('{name}',$q${query}$q$,'24h','DIFFERENTIAL')"
                ),
            )
            .await
            .unwrap();
        }

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let before_rollback: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "BEGIN; INSERT INTO {source} VALUES (1,'rolled back'); \
             DELETE FROM {source} WHERE v=1 AND label='duplicate'; ROLLBACK"
        )))
        .execute(&db.pool)
        .await
        .unwrap();
        let after_rollback: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        assert_eq!(
            after_rollback, before_rollback,
            "rolled-back row and statement trigger images must not remain in the shared buffer"
        );
        let source_rows: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM {source}"))
            .await;
        assert_eq!(
            source_rows, 4,
            "rollback must restore duplicate and NULL rows"
        );

        // The middle consumer advances past one insert transaction while the
        // fast and slow consumers still start at the initial bag.
        db.execute(&format!(
            "INSERT INTO {source} VALUES (1,'duplicate'),(NULL,NULL)"
        ))
        .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{middle}')"),
        )
        .await
        .unwrap();
        e2e::oracle::assert_st_query_exact(
            &db,
            &middle,
            "SELECT * FROM (VALUES \
             (1,'duplicate'),(1,'duplicate'),(1,'duplicate'), \
             (NULL::int,NULL::text),(NULL::int,NULL::text),(NULL::int,NULL::text) \
             ) AS expected(v,label)",
            "middle keyless consumer after duplicate and NULL insertions",
        )
        .await;
        let retained_after_middle: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        assert_eq!(
            retained_after_middle, 2,
            "shared keyless history remains intact after the middle consumer refresh"
        );

        // The slow consumer advances to an intermediate cut after one
        // duplicate retraction.
        db.execute(&format!(
            "DELETE FROM {source} WHERE ctid = ( \
             SELECT ctid FROM {source} WHERE v=1 AND label='duplicate' LIMIT 1)"
        ))
        .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{slow}')"),
        )
        .await
        .unwrap();
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            "SELECT * FROM (VALUES \
             (1,'duplicate'),(1,'duplicate'), \
             (NULL::int,NULL::text),(NULL::int,NULL::text),(NULL::int,NULL::text) \
             ) AS expected(v,label)",
            "slow keyless consumer after the duplicate retraction",
        )
        .await;
        let retained_after_slow: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        assert_eq!(
            retained_after_slow, 3,
            "shared keyless history retains both consumer windows after the later cut"
        );

        // Delete one NULL occurrence. The fast consumer now sees a net-zero
        // bag change across the insert and both delete transactions.
        db.execute(&format!(
            "DELETE FROM {source} WHERE ctid = ( \
             SELECT ctid FROM {source} WHERE v IS NULL AND label IS NULL LIMIT 1)"
        ))
        .await;
        let before_fast: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{fast}')"),
        )
        .await
        .unwrap();
        let fast_advanced: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{fast}' AND h.refresh_id>{before_fast} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            fast_advanced,
            "fast keyless consumer must advance differentially"
        );
        let retained_after_fast: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM {buffer} WHERE action IN ('D','I')"
            ))
            .await;
        assert_eq!(
            retained_after_fast, 4,
            "shared keyless history must retain every image while consumer cuts differ"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &fast,
            &query,
            "fast keyless consumer after net-zero duplicate history",
        )
        .await;
        e2e::oracle::assert_st_query_exact(
            &db,
            &middle,
            "SELECT * FROM (VALUES \
             (1,'duplicate'),(1,'duplicate'),(1,'duplicate'), \
             (NULL::int,NULL::text),(NULL::int,NULL::text),(NULL::int,NULL::text) \
             ) AS expected(v,label)",
            "middle keyless consumer remains at its protected cut",
        )
        .await;
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            "SELECT * FROM (VALUES \
             (1,'duplicate'),(1,'duplicate'), \
             (NULL::int,NULL::text),(NULL::int,NULL::text),(NULL::int,NULL::text) \
             ) AS expected(v,label)",
            "slow keyless consumer remains at its later protected cut",
        )
        .await;

        let before_middle: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{middle}')"),
        )
        .await
        .unwrap();
        let middle_caught_up: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{middle}' AND h.refresh_id>{before_middle} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            middle_caught_up,
            "middle keyless consumer must catch up differentially from its protected cut"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &middle,
            "SELECT * FROM (VALUES \
             (1,'duplicate'),(1,'duplicate'), \
             (NULL::int,NULL::text),(NULL::int,NULL::text) \
             ) AS expected(v,label)",
            "middle consumer catches up through both keyless retractions",
        )
        .await;

        let before_slow: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
                "SET pg_trickle.compact_threshold=1",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{slow}')"),
        )
        .await
        .unwrap();
        let slow_caught_up: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) \
                 WHERE s.pgt_name='{slow}' AND h.refresh_id>{before_slow} \
                   AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(
            slow_caught_up,
            "slow keyless consumer must catch up differentially from its protected cut"
        );
        e2e::oracle::assert_st_query_exact(
            &db,
            &slow,
            "SELECT * FROM (VALUES \
             (1,'duplicate'),(1,'duplicate'), \
             (NULL::int,NULL::text),(NULL::int,NULL::text) \
             ) AS expected(v,label)",
            "slow consumer consumes the NULL retraction from its later cut",
        )
        .await;
        for name in [&fast, &middle, &slow] {
            e2e::oracle::assert_st_query_exact(
                &db,
                name,
                &query,
                "keyless consumers converge after protected-window compaction",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn test_cdc_statement_payload_names_do_not_shadow_row_aliases() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled=false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE alias_source (id INT PRIMARY KEY,n NUMERIC,o JSON)")
        .await;
    db.execute("INSERT INTO alias_source VALUES (1,1.0,'{\"a\":1}')")
        .await;
    let query = "SELECT id,n,o FROM alias_source";
    db.try_execute_with_config(&["SET pg_trickle.cdc_trigger_mode='statement'"],&format!("SELECT pgtrickle.create_stream_table('alias_result',$q${query}$q$,'24h','DIFFERENTIAL')")).await.unwrap();
    db.execute("UPDATE alias_source SET n=1.00,o='{\"a\":2}'")
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('alias_result')",
    )
    .await
    .unwrap();
    e2e::oracle::assert_st_query_exact(
        &db,
        "alias_result",
        query,
        "payload names versus transition aliases",
    )
    .await;
    let scale: i32 = db.query_scalar("SELECT scale(n) FROM alias_result").await;
    assert_eq!(scale, 2);
}

#[tokio::test]
async fn test_cdc_traced_refreshes_consume_each_current_window() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE TABLE traced_source (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO traced_source VALUES (1,10)").await;
    let query = "SELECT id,v FROM traced_source";
    db.create_st("traced_result", query, "24h", "DIFFERENTIAL")
        .await;
    let mut conn = db.pool.acquire().await.unwrap();
    for sql in [
        "SELECT pgtrickle.clear_caches()",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SET pg_trickle.dvm_decision_trace = on",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql.to_string()))
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    for value in [20, 30, 40] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE traced_source SET v={value} WHERE id=1"
        )))
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(
            "SELECT pgtrickle.refresh_stream_table('traced_result')".to_string(),
        ))
        .execute(&mut *conn)
        .await
        .unwrap();
        e2e::oracle::assert_st_query_exact(
            &db,
            "traced_result",
            query,
            "traced current capture window",
        )
        .await;
    }
}

#[tokio::test]
async fn test_shared_domain_over_admitted_base_retains_changed_bit() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute("CREATE DOMAIN image_int4 AS integer").await;

    let domain_oid: i64 = db
        .query_scalar("SELECT 'image_int4'::regtype::oid::bigint")
        .await;
    assert_ne!(
        domain_oid, 23,
        "the source column must retain its domain OID"
    );

    for mode in ["row", "statement"] {
        let source = format!("domain_image_{mode}");
        let stream = format!("{source}_st");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, v image_int4)"
        ))
        .await;
        db.execute(&format!("INSERT INTO {source} VALUES (1, 1)"))
            .await;
        db.try_execute_with_config(
            &[&format!("SET pg_trickle.cdc_trigger_mode = '{mode}'")],
            &format!(
                "SELECT pgtrickle.create_stream_table('{stream}', 'SELECT id, v FROM {source}', '24h', 'DIFFERENTIAL')"
            ),
        )
        .await
        .expect("admit domain-backed source with selected trigger mode");
        let expected_query = format!("SELECT id, v FROM {source}");
        db.assert_st_matches_query(&stream, &expected_query).await;

        let source_oid: i64 = db
            .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
            .await;
        let buffer = db.change_buffer_table(source_oid).await;
        let before: i64 = db
            .query_scalar(&format!("SELECT COALESCE(max(change_id), 0) FROM {buffer}"))
            .await;
        db.execute(&format!("UPDATE {source} SET v = 2 WHERE id = 1"))
            .await;

        let (captured, changed): (i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT count(*), count(*) FILTER (WHERE get_bit(changed_cols, 1) = 1) \
             FROM {buffer} WHERE change_id > {before} AND action IN ('D', 'I')"
        )))
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(captured, 2, "{mode}: capture both UPDATE images");
        assert_eq!(changed, 2, "{mode}: retain the changed domain value");

        let before_refresh: i64 = db
            .query_scalar("SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
            .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy = 'differential'",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{stream}')"),
        )
        .await
        .expect("consume the changed unknown-type image incrementally");
        let differential: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                 WHERE s.pgt_name = '{stream}' AND h.refresh_id > {before_refresh} \
                   AND h.status = 'COMPLETED' AND h.action = 'DIFFERENTIAL' \
                   AND NOT h.was_full_fallback)"
            ))
            .await;
        assert!(differential, "{mode}: update used the differential route");
        e2e::oracle::assert_st_query_exact(
            &db,
            &stream,
            &expected_query,
            "unknown domain type update",
        )
        .await;

        let before: i64 = db
            .query_scalar(&format!("SELECT COALESCE(max(change_id), 0) FROM {buffer}"))
            .await;
        db.execute(&format!("UPDATE {source} SET v = 2 WHERE id = 1"))
            .await;
        let (captured, changed): (i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT count(*), count(*) FILTER (WHERE get_bit(changed_cols, 1) = 1) \
             FROM {buffer} WHERE change_id > {before} AND action IN ('D', 'I')"
        )))
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(captured, 2, "{mode}: capture same-value UPDATE images");
        assert_eq!(changed, 2, "{mode}: retain the same-value domain bit");
    }
}

#[tokio::test]
async fn test_shared_numeric_images_preserve_scale_for_both_trigger_modes_and_orders() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    for mode in ["row", "statement"] {
        for all_first in [true, false] {
            let source = format!("image_{mode}_{all_first}");
            let wide = format!("{source}_wide");
            let narrow = format!("{source}_narrow");
            db.execute(&format!(
                "CREATE TABLE {source} (id INT PRIMARY KEY, pad TEXT, v NUMERIC)"
            ))
            .await;
            db.execute(&format!("INSERT INTO {source} VALUES (1,'x',1.0)"))
                .await;
            let wide_query = format!("SELECT id,v,pad,scale(v) AS observed_scale FROM {source}");
            let narrow_query = format!("SELECT id,scale(v) AS observed_scale FROM {source}");
            let consumers = if all_first {
                [(&wide, &wide_query), (&narrow, &narrow_query)]
            } else {
                [(&narrow, &narrow_query), (&wide, &wide_query)]
            };
            for (name, query) in consumers {
                db.try_execute_with_config(&[&format!("SET pg_trickle.cdc_trigger_mode = '{mode}'")], &format!("SELECT pgtrickle.create_stream_table('{name}', $q${query}$q$, '24h', 'DIFFERENTIAL')")).await.expect("admit consumer with selected trigger mode");
            }
            let row_update_trigger: bool = db.query_scalar(&format!("SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid='{source}'::regclass AND tgname LIKE 'pg_trickle_cdc_%' AND (tgtype::int & 16) <> 0 AND (tgtype::int & 1) <> 0)")).await;
            assert_eq!(row_update_trigger, mode == "row", "observed capture route");
            let oid: i64 = db
                .query_scalar(&format!("SELECT '{source}'::regclass::oid::bigint"))
                .await;
            let buffer = db.change_buffer_table(oid).await;
            for (cycle, literal) in ["1.00", "1.0", "NULL", "1.00", "1.00"].iter().enumerate() {
                let before: i64 = db
                    .query_scalar(&format!("SELECT COALESCE(max(change_id),0) FROM {buffer}"))
                    .await;
                db.execute(&format!("UPDATE {source} SET v={literal} WHERE id=1"))
                    .await;
                let (captured, changed): (i64,i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT count(*),count(*) FILTER (WHERE get_bit(changed_cols,2)=1) FROM {buffer} WHERE change_id>{before} AND action IN ('D','I')"))).fetch_one(&db.pool).await.unwrap();
                assert_eq!(captured, 2, "{mode}/{all_first}/{cycle}: D+I capture");
                assert_eq!(changed, 2, "representation-aware numeric bit");
                let before_refresh: i64 = db
                    .query_scalar(
                        "SELECT COALESCE(max(refresh_id),0) FROM pgtrickle.pgt_refresh_history",
                    )
                    .await;
                for name in [&wide, &narrow] {
                    db.try_execute_with_config(
                        &[
                            "SET pg_trickle.refresh_strategy = 'differential'",
                            "SET pg_trickle.differential_max_change_ratio = 0",
                        ],
                        &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
                    )
                    .await
                    .expect("consume captured numeric image change");
                    if cycle < 4 {
                        let differential: bool = db.query_scalar(&format!("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_name='{name}' AND h.refresh_id>{before_refresh} AND h.status='COMPLETED' AND h.action='DIFFERENTIAL' AND NOT h.was_full_fallback)")).await;
                        assert!(differential, "{name}: fresh executed differential route");
                    }
                }
                e2e::oracle::assert_st_query_exact(&db, &wide, &wide_query, "wide image consumer")
                    .await;
                e2e::oracle::assert_st_query_exact(
                    &db,
                    &narrow,
                    &narrow_query,
                    "narrow scale consumer after wide refresh",
                )
                .await;
            }
        }
    }
}

#[tokio::test]
async fn test_shared_float_images_preserve_sql_equal_distinct_binary_values() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    for mode in ["row", "statement"] {
        for wide_first in [true, false] {
            let source = format!("float_images_{mode}_{wide_first}");
            let wide = format!("{source}_wide");
            let narrow = format!("{source}_narrow");
            let wide_query = format!("SELECT id, v, pad FROM {source}");
            let narrow_query = format!("SELECT id, v FROM {source}");
            db.execute(&format!(
                "CREATE TABLE {source} (id INT PRIMARY KEY, v DOUBLE PRECISION NOT NULL, pad INT NOT NULL)"
            ))
            .await;
            db.execute(&format!("INSERT INTO {source} VALUES (1, 0.0, 7)"))
                .await;

            let consumers = if wide_first {
                [(&wide, &wide_query), (&narrow, &narrow_query)]
            } else {
                [(&narrow, &narrow_query), (&wide, &wide_query)]
            };
            for (name, query) in consumers {
                db.try_execute_with_config(
                    &[&format!("SET pg_trickle.cdc_trigger_mode = '{mode}'")],
                    &format!(
                        "SELECT pgtrickle.create_stream_table('{name}', $q${query}$q$, '24h', 'DIFFERENTIAL')"
                    ),
                )
                .await
                .expect("admit consumer with selected trigger mode");
            }

            let equal_with_positive_zero: bool = db
                .query_scalar(&format!(
                    "SELECT v = 0.0::float8 FROM {source} WHERE id = 1"
                ))
                .await;
            let binary_matches_positive_zero: bool = db
                .query_scalar(&format!(
                    "SELECT pg_catalog.float8send(v) IS NOT DISTINCT FROM \
                     pg_catalog.float8send(0.0::float8) FROM {source} WHERE id = 1"
                ))
                .await;
            assert!(equal_with_positive_zero);
            assert!(binary_matches_positive_zero);

            let source_oid = db.table_oid(&source).await as i64;
            let buffer = db.change_buffer_table(source_oid).await;
            let before: i64 = db
                .query_scalar(&format!("SELECT COALESCE(max(change_id), 0) FROM {buffer}"))
                .await;
            db.execute(&format!(
                "UPDATE {source} SET v = '-0'::float8 WHERE id = 1"
            ))
            .await;
            let equal_with_positive_zero: bool = db
                .query_scalar(&format!(
                    "SELECT v = 0.0::float8 FROM {source} WHERE id = 1"
                ))
                .await;
            let binary_differs_from_positive_zero: bool = db
                .query_scalar(&format!(
                    "SELECT pg_catalog.float8send(v) IS DISTINCT FROM \
                     pg_catalog.float8send(0.0::float8) FROM {source} WHERE id = 1"
                ))
                .await;
            assert!(equal_with_positive_zero);
            assert!(binary_differs_from_positive_zero);
            let (captured, changed): (i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT count(*), count(*) FILTER (WHERE get_bit(changed_cols, 1) = 1) \
                     FROM {buffer} WHERE change_id > {before} AND action IN ('D', 'I')"
            )))
            .fetch_one(&db.pool)
            .await
            .unwrap();
            assert_eq!(captured, 2, "{mode}/{wide_first}: capture both images");
            assert_eq!(
                changed, 2,
                "{mode}/{wide_first}: retain float8's changed bit"
            );

            for (name, query) in [(&wide, &wide_query), (&narrow, &narrow_query)] {
                let before_refresh: i64 = db
                    .query_scalar(
                        "SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history",
                    )
                    .await;
                db.try_execute_with_config(
                    &[
                        "SET pg_trickle.refresh_strategy = 'differential'",
                        "SET pg_trickle.differential_max_change_ratio = 0",
                    ],
                    &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
                )
                .await
                .expect("consume the signed-zero image change");
                let differential: bool = db
                    .query_scalar(&format!(
                        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                         WHERE s.pgt_name = '{name}' AND h.refresh_id > {before_refresh} \
                           AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' \
                           AND NOT h.was_full_fallback)"
                    ))
                    .await;
                assert!(differential, "{name}: execute the differential route");
                e2e::oracle::assert_st_query_exact(
                    &db,
                    name,
                    query,
                    "float8 signed-zero source result",
                )
                .await;
                let stored_image_matches_source: bool = db
                    .query_scalar(&format!(
                        "SELECT pg_catalog.float8send(st.v) = pg_catalog.float8send(src.v) \
                         AND pg_catalog.float8send(st.v) <> pg_catalog.float8send(0.0::float8) \
                         FROM public.{name} st JOIN public.{source} src USING (id)"
                    ))
                    .await;
                assert!(
                    stored_image_matches_source,
                    "{name}: preserve negative-zero payload despite SQL equality"
                );
            }
        }
    }
}

#[tokio::test]
async fn test_compaction_shared_source_preserves_consumer_boundaries() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE compact_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO compact_src SELECT generate_series(1, 100)")
        .await;
    let query = "SELECT id FROM compact_src";
    for name in ["compact_fast", "compact_slow"] {
        db.create_st(name, query, "1h", "DIFFERENTIAL").await;
    }

    db.execute("INSERT INTO compact_src VALUES (101)").await;
    db.refresh_st_until_converged("compact_fast").await;
    assert_eq!(db.count("compact_fast").await, 101);
    db.execute("DELETE FROM compact_src WHERE id = 101").await;
    db.execute_seq(&[
        "SET pg_trickle.compact_threshold = 1",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('compact_slow')",
    ])
    .await;
    e2e::oracle::assert_st_query_exact(
        &db,
        "compact_fast",
        "SELECT id FROM compact_src UNION ALL SELECT 101",
        "fast source consumer remains at its earlier cut",
    )
    .await;
    db.assert_st_matches_query("compact_slow", query).await;
    db.refresh_st("compact_fast").await;

    for name in ["compact_fast", "compact_slow"] {
        db.assert_st_matches_query(name, query).await;
    }
}

#[tokio::test]
async fn test_compaction_shared_stream_preserves_consumer_boundaries() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE compact_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO compact_src SELECT generate_series(1, 100)")
        .await;
    db.create_st(
        "compact_upstream",
        "SELECT id FROM compact_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let query = "SELECT id FROM compact_upstream";
    for name in ["compact_fast", "compact_slow"] {
        db.create_st(name, query, "1h", "DIFFERENTIAL").await;
    }

    db.execute("INSERT INTO compact_src VALUES (101)").await;
    db.refresh_st_with_retry("compact_upstream").await;
    db.refresh_st_with_retry("compact_fast").await;
    assert_eq!(db.count("compact_fast").await, 101);
    db.execute("DELETE FROM compact_src WHERE id = 101").await;
    db.refresh_st("compact_upstream").await;
    db.execute_seq(&[
        "SET pg_trickle.compact_threshold = 1",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('compact_slow')",
    ])
    .await;
    e2e::oracle::assert_st_query_exact(
        &db,
        "compact_fast",
        "SELECT id FROM compact_upstream UNION ALL SELECT 101",
        "fast stream consumer remains at its earlier cut",
    )
    .await;
    db.assert_st_matches_query("compact_slow", query).await;
    db.refresh_st("compact_fast").await;

    for name in ["compact_fast", "compact_slow"] {
        db.assert_st_matches_query(name, query).await;
    }
}

#[tokio::test]
async fn test_compaction_single_consumer_stream_preserves_latest_payload() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE compact_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO compact_src VALUES (1, 0)").await;
    db.create_st(
        "compact_upstream",
        "SELECT id, val FROM compact_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "compact_downstream",
        "SELECT id, val FROM compact_upstream",
        "1h",
        "DIFFERENTIAL",
    )
    .await;

    for val in 1..=3 {
        db.execute(&format!("UPDATE compact_src SET val = {val} WHERE id = 1"))
            .await;
        db.refresh_st_until_converged("compact_upstream").await;
    }

    db.execute_seq(&[
        "SET pg_trickle.compact_threshold = 1",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('compact_downstream')",
    ])
    .await;
    db.assert_st_matches_query("compact_downstream", "SELECT id, val FROM compact_src")
        .await;
    let differential: bool = db
        .query_scalar(
            "SELECT h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' \
             AND NOT h.was_full_fallback \
             FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = 'compact_downstream' \
             ORDER BY h.refresh_id DESC LIMIT 1",
        )
        .await;
    assert!(
        differential,
        "single-consumer ST compaction stays differential"
    );
}

#[tokio::test]
async fn test_compaction_keyless_source_and_stream_preserve_duplicates() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE compact_src (val INT)").await;
    db.execute("INSERT INTO compact_src SELECT generate_series(1, 100)")
        .await;
    db.create_st(
        "compact_upstream",
        "SELECT val FROM compact_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let query = "SELECT val, COUNT(*) AS cnt FROM compact_upstream GROUP BY val";
    db.create_st("compact_counts", query, "1h", "DIFFERENTIAL")
        .await;

    db.execute("INSERT INTO compact_src VALUES (101), (101), (101)")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.compact_threshold = 1",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('compact_upstream')",
        "SELECT pgtrickle.refresh_stream_table('compact_counts')",
    ])
    .await;

    db.assert_st_matches_query("compact_upstream", "SELECT val FROM compact_src")
        .await;
    db.assert_st_matches_query("compact_counts", query).await;
    let count: i64 = db
        .query_scalar("SELECT cnt FROM compact_counts WHERE val = 101")
        .await;
    assert_eq!(count, 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// D-4.1 — Fan-out: 10 STs from one source share a single change buffer
// ═══════════════════════════════════════════════════════════════════════════

/// Create 10 stream tables from the same source table. Verify:
/// 1. Only one change buffer table exists (`changes_{oid}`).
/// 2. All 10 STs produce correct results after full + differential refresh.
/// 3. The `pgtrickle.shared_buffer_stats()` function reports accurate metrics.
#[tokio::test]
async fn test_d4_fanout_10_sts_shared_buffer() {
    let db = E2eDb::new().await.with_extension().await;

    // Source table with a few columns.
    db.execute(
        "CREATE TABLE d4_src (
            id   SERIAL PRIMARY KEY,
            grp  TEXT NOT NULL,
            val  INT NOT NULL,
            note TEXT DEFAULT 'x'
        )",
    )
    .await;
    db.execute(
        "INSERT INTO d4_src (grp, val) VALUES
            ('a', 10), ('b', 20), ('c', 30), ('a', 40), ('b', 50)",
    )
    .await;

    // Create 10 STs with different defining queries over the same source.
    let st_names = [
        "d4_sum_grp",
        "d4_cnt_grp",
        "d4_max_grp",
        "d4_min_grp",
        "d4_avg_grp",
        "d4_sum_all",
        "d4_cnt_all",
        "d4_filter_a",
        "d4_filter_b",
        "d4_id_note",
    ];
    let st_queries = [
        "SELECT grp, SUM(val) AS total FROM d4_src GROUP BY grp",
        "SELECT grp, COUNT(*) AS cnt FROM d4_src GROUP BY grp",
        "SELECT grp, MAX(val) AS mx FROM d4_src GROUP BY grp",
        "SELECT grp, MIN(val) AS mn FROM d4_src GROUP BY grp",
        "SELECT grp, AVG(val) AS av FROM d4_src GROUP BY grp",
        "SELECT SUM(val) AS total FROM d4_src",
        "SELECT COUNT(*) AS cnt FROM d4_src",
        "SELECT id, val FROM d4_src WHERE grp = 'a'",
        "SELECT id, val FROM d4_src WHERE grp = 'b'",
        "SELECT id, note FROM d4_src",
    ];

    for (name, query) in st_names.iter().zip(st_queries.iter()) {
        db.create_st(name, query, "1m", "DIFFERENTIAL").await;
    }

    // Full refresh all.
    for name in &st_names {
        db.refresh_st(name).await;
    }

    // Verify: only one change buffer table exists for this source.
    let buf_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'pgtrickle_changes'
               AND c.relkind IN ('r', 'p')
               AND c.relname LIKE 'changes_%'
               AND c.relname NOT LIKE 'changes_pgt_%'",
        )
        .await;
    assert_eq!(
        buf_count, 1,
        "should have exactly 1 change buffer for the source"
    );

    // Verify: shared_buffer_stats reports 10 consumers.
    let consumer_count: i32 = db
        .query_scalar("SELECT consumer_count FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    assert_eq!(consumer_count, 10, "shared buffer should have 10 consumers");

    // Insert new rows and differentially refresh all.
    db.execute("INSERT INTO d4_src (grp, val) VALUES ('a', 100), ('c', 200)")
        .await;

    for name in &st_names {
        db.refresh_st(name).await;
    }

    // Validate a few STs.
    let sum_a: i64 = db
        .query_scalar("SELECT total FROM d4_sum_grp WHERE grp = 'a'")
        .await;
    assert_eq!(sum_a, 150); // 10 + 40 + 100

    let cnt_all: i64 = db.query_scalar("SELECT cnt FROM d4_cnt_all").await;
    assert_eq!(cnt_all, 7);

    let filter_a_cnt: i64 = db.query_scalar("SELECT count(*) FROM d4_filter_a").await;
    assert_eq!(filter_a_cnt, 3); // id 1, 4, and the new one
}

// ═══════════════════════════════════════════════════════════════════════════
// D-4.2 — Multi-frontier cleanup: slowest consumer protects buffer rows
// ═══════════════════════════════════════════════════════════════════════════

/// Two STs share a buffer. Refresh only ST1 (ST2 falls behind). Insert more
/// rows. Refresh ST1 again. Buffer rows needed by ST2 must NOT be cleaned up.
/// Then refresh ST2 — it should see all changes.
#[tokio::test]
async fn test_d4_multi_frontier_slowest_consumer_protection() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE d4_mf_src (
            id  SERIAL PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO d4_mf_src (val) VALUES (10), (20)")
        .await;

    db.create_st(
        "d4_mf_fast",
        "SELECT SUM(val) AS total FROM d4_mf_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "d4_mf_slow",
        "SELECT COUNT(*) AS cnt FROM d4_mf_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Full refresh both.
    db.refresh_st("d4_mf_fast").await;
    db.refresh_st("d4_mf_slow").await;

    // Wave 1: insert + refresh only the fast consumer.
    db.execute("INSERT INTO d4_mf_src (val) VALUES (30)").await;
    db.refresh_st("d4_mf_fast").await;

    let fast_total: i64 = db.query_scalar("SELECT total FROM d4_mf_fast").await;
    assert_eq!(fast_total, 60); // 10 + 20 + 30

    // Wave 2: more inserts + refresh only fast consumer again.
    db.execute("INSERT INTO d4_mf_src (val) VALUES (40)").await;
    db.refresh_st("d4_mf_fast").await;

    let fast_total2: i64 = db.query_scalar("SELECT total FROM d4_mf_fast").await;
    assert_eq!(fast_total2, 100); // 10 + 20 + 30 + 40

    // Now refresh the slow consumer — it should catch up with ALL changes.
    db.refresh_st("d4_mf_slow").await;

    let slow_cnt: i64 = db.query_scalar("SELECT cnt FROM d4_mf_slow").await;
    assert_eq!(slow_cnt, 4); // all 4 rows
}

// ═══════════════════════════════════════════════════════════════════════════
// D-4.3 — Column superset: buffer tracks union of all consumer columns
// ═══════════════════════════════════════════════════════════════════════════

/// Two STs reference different columns from the same source. The shared buffer
/// should track the union of both column sets.
#[tokio::test]
async fn test_d4_column_superset_tracked() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE d4_cs_src (
            id   SERIAL PRIMARY KEY,
            col_a TEXT NOT NULL DEFAULT 'x',
            col_b INT NOT NULL DEFAULT 0,
            col_c NUMERIC NOT NULL DEFAULT 0
        )",
    )
    .await;
    db.execute("INSERT INTO d4_cs_src (col_a, col_b, col_c) VALUES ('hello', 1, 10.5)")
        .await;

    // ST1 uses col_a, ST2 uses col_b and col_c.
    db.create_st(
        "d4_cs_st1",
        "SELECT id, col_a FROM d4_cs_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "d4_cs_st2",
        "SELECT id, col_b, col_c FROM d4_cs_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.refresh_st("d4_cs_st1").await;
    db.refresh_st("d4_cs_st2").await;

    // The shared buffer should track col_a, col_b, col_c (superset).
    let columns_tracked: i32 = db
        .query_scalar("SELECT columns_tracked FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    // id (PK) + col_a + col_b + col_c = 4 columns
    assert!(
        columns_tracked >= 3,
        "buffer should track at least 3 user columns (union), got {}",
        columns_tracked
    );

    // Both STs work correctly with differential refresh.
    db.execute("INSERT INTO d4_cs_src (col_a, col_b, col_c) VALUES ('world', 2, 20.5)")
        .await;

    db.refresh_st("d4_cs_st1").await;
    db.refresh_st("d4_cs_st2").await;

    let st1_cnt: i64 = db.query_scalar("SELECT count(*) FROM d4_cs_st1").await;
    assert_eq!(st1_cnt, 2);

    let st2_sum: i64 = db
        .query_scalar("SELECT SUM(col_b)::bigint FROM d4_cs_st2")
        .await;
    assert_eq!(st2_sum, 3); // 1 + 2
}

// ═══════════════════════════════════════════════════════════════════════════
// D-4.4 — Shared buffer stats function returns correct data
// ═══════════════════════════════════════════════════════════════════════════

/// Verify that `pgtrickle.shared_buffer_stats()` returns accurate metadata.
#[tokio::test]
async fn test_d4_shared_buffer_stats_accuracy() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE d4_stats_src (
            id  SERIAL PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO d4_stats_src (val) VALUES (1), (2), (3)")
        .await;

    db.create_st(
        "d4_stats_st1",
        "SELECT SUM(val) AS total FROM d4_stats_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "d4_stats_st2",
        "SELECT COUNT(*) AS cnt FROM d4_stats_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.refresh_st("d4_stats_st1").await;
    db.refresh_st("d4_stats_st2").await;

    // Insert data to put rows in the buffer.
    db.execute("INSERT INTO d4_stats_src (val) VALUES (4), (5)")
        .await;

    // Verify stats function returns a row.
    let consumer_count: i32 = db
        .query_scalar("SELECT consumer_count FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    assert_eq!(consumer_count, 2, "should have 2 consumers");

    let source_table: String = db
        .query_scalar("SELECT source_table FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    assert!(
        source_table.contains("d4_stats_src"),
        "source_table should contain the table name, got: {}",
        source_table
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// D-4.5 — DROP ST reduces consumer count
// ═══════════════════════════════════════════════════════════════════════════

/// When a stream table is dropped, the consumer count for the shared buffer
/// should decrease. When the last consumer is dropped, the buffer is removed.
#[tokio::test]
async fn test_d4_drop_st_reduces_consumer_count() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE d4_drop_src (
            id  SERIAL PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO d4_drop_src (val) VALUES (1)").await;

    db.create_st(
        "d4_drop_st1",
        "SELECT SUM(val) AS s FROM d4_drop_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "d4_drop_st2",
        "SELECT COUNT(*) AS c FROM d4_drop_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.refresh_st("d4_drop_st1").await;
    db.refresh_st("d4_drop_st2").await;

    let cnt_before: i32 = db
        .query_scalar("SELECT consumer_count FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    assert_eq!(cnt_before, 2);

    // Drop one ST.
    db.drop_st("d4_drop_st1").await;

    let cnt_after: i32 = db
        .query_scalar("SELECT consumer_count FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    assert_eq!(cnt_after, 1);

    // Drop the last ST — buffer tracking should be removed.
    db.drop_st("d4_drop_st2").await;

    let stats_count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.shared_buffer_stats()")
        .await;
    assert_eq!(stats_count, 0, "no shared buffers should remain");
}

// ═══════════════════════════════════════════════════════════════════════════
// PERF-2 — Auto Buffer Partitioning
// ═══════════════════════════════════════════════════════════════════════════

/// Verify that `buffer_partitioning = 'on'` creates a partitioned change buffer
/// from the start and that differential refresh works correctly through it.
#[tokio::test]
async fn test_perf2_buffer_partitioning_on_creates_partitioned() {
    let db = E2eDb::new().await.with_extension().await;

    // Enable buffer partitioning + create ST on the SAME connection so
    // the session-level SET is visible to the create_stream_table call.
    db.execute(
        "CREATE TABLE perf2_src (
            id   SERIAL PRIMARY KEY,
            grp  TEXT NOT NULL,
            val  INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO perf2_src (grp, val) VALUES ('a', 10), ('b', 20), ('c', 30)")
        .await;

    db.execute_seq(&[
        "SET pg_trickle.buffer_partitioning = 'on'",
        "SELECT pgtrickle.create_stream_table('perf2_sum', \
         $$SELECT grp, SUM(val) AS total FROM perf2_src GROUP BY grp$$, \
         '1m', 'DIFFERENTIAL')",
    ])
    .await;

    // Full refresh.
    db.refresh_st("perf2_sum").await;

    // Verify the change buffer is partitioned (relkind = 'p').
    let is_partitioned: bool = db
        .query_scalar("SELECT is_partitioned FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    assert!(
        is_partitioned,
        "buffer should be partitioned with mode='on'"
    );

    // Insert new rows and differential refresh.
    db.execute("INSERT INTO perf2_src (grp, val) VALUES ('a', 100)")
        .await;
    db.refresh_st("perf2_sum").await;

    let sum_a: i64 = db
        .query_scalar("SELECT total FROM perf2_sum WHERE grp = 'a'")
        .await;
    assert_eq!(
        sum_a, 110,
        "differential refresh through partitioned buffer"
    );
}

/// Verify that `buffer_partitioning = 'auto'` starts with an unpartitioned
/// buffer for normal workloads (below compact_threshold).
#[tokio::test]
async fn test_perf2_auto_mode_starts_unpartitioned() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE perf2_auto_src (
            id   SERIAL PRIMARY KEY,
            val  INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO perf2_auto_src (val) VALUES (1), (2), (3)")
        .await;

    // Enable auto mode + create ST on the SAME connection.
    // Use a short schedule (< 30s) so auto mode does NOT partition at creation time.
    db.execute_seq(&[
        "SET pg_trickle.buffer_partitioning = 'auto'",
        "SELECT pgtrickle.create_stream_table('perf2_auto_cnt', \
         $$SELECT COUNT(*) AS cnt FROM perf2_auto_src$$, \
         '10s', 'DIFFERENTIAL')",
    ])
    .await;

    db.refresh_st("perf2_auto_cnt").await;

    // With only 3 rows, the buffer should NOT be auto-promoted.
    let is_partitioned: bool = db
        .query_scalar("SELECT is_partitioned FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    assert!(
        !is_partitioned,
        "with low volume, auto mode should keep buffer unpartitioned"
    );

    // Normal differential refresh should still work.
    db.execute("INSERT INTO perf2_auto_src (val) VALUES (4)")
        .await;
    db.refresh_st_until_converged("perf2_auto_cnt").await;

    let cnt: i64 = db.query_scalar("SELECT cnt FROM perf2_auto_cnt").await;
    assert_eq!(cnt, 4);
}

/// Verify that `buffer_partitioning = 'auto'` promotes to partitioned mode
/// when the buffer fill rate exceeds compact_threshold within a single cycle.
///
/// This test lowers the compact_threshold to a small value, inserts enough
/// rows to exceed it, and verifies the auto-promotion happens during
/// differential refresh.
#[tokio::test]
async fn test_perf2_auto_mode_promotes_on_high_throughput() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE perf2_promo_src (
            id   SERIAL PRIMARY KEY,
            val  INT NOT NULL
        )",
    )
    .await;
    db.execute("INSERT INTO perf2_promo_src (val) VALUES (1)")
        .await;

    // Set auto mode + low threshold + create ST on the SAME connection.
    // Use a short schedule (< 30s) so auto mode does NOT partition at creation time —
    // we want to test runtime promotion via compact_threshold.
    db.execute_seq(&[
        "SET pg_trickle.buffer_partitioning = 'auto'",
        "SET pg_trickle.compact_threshold = 50",
        "SELECT pgtrickle.create_stream_table('perf2_promo_cnt', \
         $$SELECT COUNT(*) AS cnt FROM perf2_promo_src$$, \
         '10s', 'DIFFERENTIAL')",
    ])
    .await;

    // Full refresh to establish baseline (needs auto mode visible).
    db.execute_seq(&[
        "SET pg_trickle.buffer_partitioning = 'auto'",
        "SELECT pgtrickle.refresh_stream_table('perf2_promo_cnt')",
    ])
    .await;

    // Verify buffer starts unpartitioned.
    let is_part_before: bool = db
        .query_scalar("SELECT is_partitioned FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    assert!(
        !is_part_before,
        "buffer should start unpartitioned in auto mode"
    );

    // Insert >50 rows to exceed the lowered compact_threshold.
    db.execute(
        "INSERT INTO perf2_promo_src (val)
         SELECT generate_series(1, 100)",
    )
    .await;

    // Differential refresh with auto mode + low threshold on same connection.
    db.execute_seq(&[
        "SET pg_trickle.buffer_partitioning = 'auto'",
        "SET pg_trickle.compact_threshold = 50",
        "SELECT pgtrickle.refresh_stream_table('perf2_promo_cnt')",
    ])
    .await;

    // Verify buffer was promoted to partitioned.
    let is_part_after: bool = db
        .query_scalar("SELECT is_partitioned FROM pgtrickle.shared_buffer_stats() LIMIT 1")
        .await;
    assert!(
        is_part_after,
        "buffer should be auto-promoted after exceeding compact_threshold"
    );

    // Verify data correctness after promotion.
    let cnt: i64 = db.query_scalar("SELECT cnt FROM perf2_promo_cnt").await;
    assert_eq!(cnt, 101, "1 initial + 100 inserted = 101");

    // Verify subsequent differential refresh still works through the
    // partitioned buffer.
    db.execute("INSERT INTO perf2_promo_src (val) VALUES (999)")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.buffer_partitioning = 'auto'",
        "SELECT pgtrickle.refresh_stream_table('perf2_promo_cnt')",
    ])
    .await;

    let cnt_after: i64 = db.query_scalar("SELECT cnt FROM perf2_promo_cnt").await;
    assert_eq!(cnt_after, 102, "post-promotion differential refresh");
}
