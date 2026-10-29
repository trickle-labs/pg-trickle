//! Polling CDC baseline continuity regressions.

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_polling_full_baseline_does_not_replay_materialized_changes() {
    let db = E2eDb::new().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    let db_name: String = db.query_scalar("SELECT current_database()").await;
    db.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .await;
    db.execute("CREATE TABLE poll_baseline_source (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO poll_baseline_source VALUES (1)")
        .await;
    db.execute(&format!(
        "CREATE SERVER poll_baseline_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname '{db_name}', host '127.0.0.1', port '5432')"
    ))
    .await;
    db.execute(
        "CREATE USER MAPPING FOR CURRENT_USER SERVER poll_baseline_server OPTIONS (user 'postgres')",
    )
    .await;
    db.execute(
        "CREATE FOREIGN TABLE poll_baseline_remote (id INT) SERVER poll_baseline_server OPTIONS (table_name 'poll_baseline_source')",
    )
    .await;
    db.alter_system_set_and_wait("pg_trickle.foreign_table_polling", "on", "on")
        .await;

    let query = "SELECT count(*) AS n FROM poll_baseline_remote";
    for name in ["poll_baseline_a", "poll_baseline_b"] {
        db.execute(&format!(
            "SELECT pgtrickle.create_stream_table(name => '{name}', query => '{query}', schedule => '24h', refresh_mode => 'DIFFERENTIAL', initialize => false)"
        ))
        .await;
    }
    db.execute("SELECT pgtrickle.refresh_stream_table('poll_baseline_b')")
        .await;
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_b")
            .await,
        1
    );

    db.execute("INSERT INTO poll_baseline_source VALUES (2)")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('poll_baseline_a')")
        .await;
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_a")
            .await,
        2
    );

    let prior_refresh: i64 = db
        .query_scalar("SELECT coalesce(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('poll_baseline_a')",
    )
    .await
    .expect("unchanged-source refresh should succeed");
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_a")
            .await,
        2,
        "A's FULL baseline already included the second source row"
    );
    let actual_differential: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'poll_baseline_a' AND h.refresh_id > {prior_refresh} AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback)"
        ))
        .await;
    assert!(actual_differential, "A must use the differential path");

    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('poll_baseline_a')",
    )
    .await
    .expect("repeated unchanged-source refresh should succeed");
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_a")
            .await,
        2
    );

    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('poll_baseline_b')",
    )
    .await
    .expect("sibling refresh should succeed as differential");
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_b")
            .await,
        2,
        "the sibling consumer must still receive its pending source change"
    );

    db.execute("INSERT INTO poll_baseline_source VALUES (3)")
        .await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = true WHERE pgt_name = 'poll_baseline_a'",
    )
    .await;
    let prior_refresh: i64 = db
        .query_scalar("SELECT coalesce(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('poll_baseline_a')")
        .await;
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_a")
            .await,
        3
    );
    let reinitialized: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) WHERE st.pgt_name = 'poll_baseline_a' AND h.refresh_id > {prior_refresh} AND h.action = 'REINITIALIZE' AND h.status = 'COMPLETED')"
        ))
        .await;
    assert!(
        reinitialized,
        "the second FULL baseline must reinitialize A"
    );
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('poll_baseline_a')",
    )
    .await
    .expect("post-reinitialization differential refresh should succeed");
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_a")
            .await,
        3
    );
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('poll_baseline_b')",
    )
    .await
    .expect("sibling differential refresh after reinitialization should succeed");
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_b")
            .await,
        3
    );

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             name => 'poll_baseline_c',\
             query => 'SELECT count(*) AS n FROM poll_baseline_remote',\
             schedule => '1s', refresh_mode => 'DIFFERENTIAL')",
    )
    .await;
    db.execute("INSERT INTO poll_baseline_source VALUES (4)")
        .await;
    let initial_timestamp: String = db
        .query_scalar(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'poll_baseline_c'",
        )
        .await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = true WHERE pgt_name = 'poll_baseline_c'",
    )
    .await;
    db.alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;
    assert!(
        db.wait_for_auto_refresh_since(
            "poll_baseline_c",
            Some(initial_timestamp),
            std::time::Duration::from_secs(30),
        )
        .await
    );
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_c")
            .await,
        4
    );
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy='differential'",
            "SET pg_trickle.differential_max_change_ratio=0",
        ],
        "SELECT pgtrickle.refresh_stream_table('poll_baseline_c')",
    )
    .await
    .expect("post-scheduler-reinitialization differential refresh should succeed");
    assert_eq!(
        db.query_scalar::<i64>("SELECT n FROM poll_baseline_c")
            .await,
        4
    );
    for name in ["poll_baseline_a", "poll_baseline_b"] {
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy='differential'",
                "SET pg_trickle.differential_max_change_ratio=0",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
        )
        .await
        .expect("scheduler baseline must preserve sibling polling changes");
        assert_eq!(
            db.query_scalar::<i64>(&format!("SELECT n FROM {name}"))
                .await,
            4
        );
    }

    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;
    db.alter_system_reset_and_wait("pg_trickle.foreign_table_polling", "off")
        .await;
}
