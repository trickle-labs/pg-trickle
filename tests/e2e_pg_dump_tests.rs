mod e2e;
use e2e::E2eDb;
use std::process::Command;

#[tokio::test]
async fn test_pg_dump_restore_recreates_oid_shifted_dag() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO source VALUES (1, 'one'), (2, 'two')")
        .await;

    db.create_st(
        "dump_test_st",
        "SELECT id, val FROM source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "dump_test_downstream",
        "SELECT id, val FROM dump_test_st",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.dump_test_st").await, 2);
    assert_eq!(db.count("public.dump_test_downstream").await, 2);
    let upstream_ddl: String = db
        .query_scalar("SELECT pgtrickle.export_definition('public.dump_test_st')")
        .await;
    let downstream_ddl: String = db
        .query_scalar("SELECT pgtrickle.export_definition('public.dump_test_downstream')")
        .await;
    let source_recovery: String = db
        .query_scalar("SELECT pgtrickle.validate_recovery()")
        .await;
    assert!(
        source_recovery.contains("\"status\":\"SAFE\""),
        "source capture identity must be initialized before dump: {source_recovery}"
    );

    // A logical restore must run with the scheduler stopped. Otherwise the
    // restored catalog can be initialized before pg_restore loads its data.
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = 'off'")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'dump_test_st'",
        )
        .await;
    db.execute_seq(&[
        "CREATE TABLE pgtrickle.__pgt_dump_window_partitions \
             (state_generation bigint NOT NULL)",
        "CREATE TABLE pgtrickle.__pgt_dump_window_rows \
             (state_generation bigint NOT NULL)",
        "ALTER EXTENSION pg_trickle ADD TABLE pgtrickle.__pgt_dump_window_partitions",
        "ALTER EXTENSION pg_trickle ADD TABLE pgtrickle.__pgt_dump_window_rows",
    ])
    .await;
    sqlx::query(
        "INSERT INTO pgtrickle.pgt_window_states \
         (pgt_id, node_ordinal, spec_ordinal, partition_relid, row_relid, \
          schema_version, strategy_version, query_hash, state_generation, status) \
         SELECT $1, 0, 0, \
                'pgtrickle.__pgt_dump_window_partitions'::regclass, \
                'pgtrickle.__pgt_dump_window_rows'::regclass, \
                1, 1, defining_query_hash, 1, 'STALE' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .execute(&db.pool)
    .await
    .expect("register derived window state before logical dump");

    let container_id = db.container_id();
    let source_database: String = db.query_scalar("SELECT current_database()").await;

    // 1. pg_dump the database
    let dump_output = Command::new("docker")
        .args([
            "exec",
            container_id,
            "pg_dump",
            "-U",
            "postgres",
            "-d",
            source_database.as_str(),
            "-F",
            "c",
            "-f",
            "/tmp/dump.backup",
        ])
        .output()
        .expect("Failed to execute docker exec");
    assert!(
        dump_output.status.success(),
        "pg_dump failed: {:?}",
        String::from_utf8_lossy(&dump_output.stderr)
    );

    // 2. Drop the original schema to simulate starting fresh
    let create_db_output = Command::new("docker")
        .args([
            "exec",
            container_id,
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-c",
            "CREATE DATABASE restored_db",
        ])
        .output()
        .expect("Failed to create restored_db");
    assert!(create_db_output.status.success(), "create db failed");

    let filler_output = Command::new("docker")
        .args([
            "exec",
            container_id,
            "psql",
            "-U",
            "postgres",
            "-d",
            "restored_db",
            "-c",
            "CREATE TABLE public.restore_oid_filler (id bigint)",
        ])
        .output()
        .expect("Failed to create restore OID filler");
    assert!(filler_output.status.success(), "create filler failed");

    // 3. Section 1 Validate Restoring Pre-Data
    let restore_output = Command::new("docker")
        .args([
            "exec",
            container_id,
            "pg_restore",
            "-U",
            "postgres",
            "-d",
            "restored_db",
            "--section=pre-data",
            "/tmp/dump.backup",
        ])
        .output()
        .expect("Failed to execute pg_restore pre-data");
    assert!(
        restore_output.status.success(),
        "pg_restore pre-data failed"
    );

    // 4. Section 2 Validate Restoring Data
    let restore_data = Command::new("docker")
        .args([
            "exec",
            container_id,
            "pg_restore",
            "-U",
            "postgres",
            "-d",
            "restored_db",
            "--section=data",
            "/tmp/dump.backup",
        ])
        .output()
        .expect("Failed to execute pg_restore data");
    assert!(
        restore_data.status.success(),
        "pg_restore data failed: {}",
        String::from_utf8_lossy(&restore_data.stderr)
    );

    // 5. Connect to restored DB to manually heal metadata buffer
    let connection_base = db
        .connection_string()
        .rsplit_once('/')
        .expect("E2E connection string must include a database")
        .0;
    let restored_conn_str = format!("{connection_base}/restored_db");
    let restored_pool = sqlx::PgPool::connect(&restored_conn_str).await.unwrap();

    let (registry_rows, derived_relations_absent, strategy_preserved): (i64, bool, bool) =
        sqlx::query_as(
            "SELECT (SELECT count(*) FROM pgtrickle.pgt_window_states), \
                    to_regclass('pgtrickle.__pgt_dump_window_partitions') IS NULL \
                    AND to_regclass('pgtrickle.__pgt_dump_window_rows') IS NULL, \
                    EXISTS ( \
                        SELECT 1 FROM pgtrickle.pgt_stream_tables \
                        WHERE pgt_name = 'dump_test_st' AND window_strategy IS NOT NULL \
                    )",
        )
        .fetch_one(&restored_pool)
        .await
        .expect("inspect restored window metadata");
    assert_eq!(registry_rows, 0, "derived registry rows must not restore");
    assert!(
        derived_relations_absent,
        "derived extension-member relations must not restore"
    );
    assert!(strategy_preserved, "durable strategy metadata must restore");

    // v0.92 validates the restored identity and keeps capture quarantined
    // until a superuser explicitly adopts the restored database.
    let recovery_report: String = sqlx::query_scalar("SELECT pgtrickle.validate_recovery()")
        .fetch_one(&restored_pool)
        .await
        .expect("validate restored capture state");
    assert!(
        recovery_report.contains("CDC_CLONE_DETECTED")
            && recovery_report.contains("OPERATOR_INTERVENTION_REQUIRED"),
        "logical restore must be quarantined: {recovery_report}"
    );
    let capture_state: String =
        sqlx::query_scalar("SELECT state FROM pgtrickle.pgt_capture_instance WHERE singleton")
            .fetch_one(&restored_pool)
            .await
            .expect("inspect restored capture state");
    assert_eq!(capture_state, "QUARANTINED");

    // The legacy bulk restore helper remains disabled. The supported workflow
    // explicitly adopts the database, drops restored stream tables, and
    // recreates them from definitions resolved against the restored OIDs.
    let restore_error = sqlx::query("SELECT pgtrickle.restore_stream_tables()")
        .execute(&restored_pool)
        .await
        .expect_err("restore_stream_tables() must fail closed");
    assert!(
        restore_error
            .to_string()
            .contains("restore_stream_tables() is disabled"),
        "unexpected restore error: {restore_error}"
    );

    let adoption: String = sqlx::query_scalar("SELECT pgtrickle.recover_capture_instance()")
        .fetch_one(&restored_pool)
        .await
        .expect("adopt restored capture instance");
    assert!(adoption.contains("capture ownership adopted"));

    sqlx::query(
        "SELECT pgtrickle.bulk_drop_stream_tables(
             ARRAY['public.dump_test_st', 'public.dump_test_downstream'])",
    )
    .execute(&restored_pool)
    .await
    .expect("drop restored stream-table DAG");

    let recreate_upstream = upstream_ddl
        .split_once('\n')
        .map_or_else(|| upstream_ddl.clone(), |(_, ddl)| ddl.to_string());
    let recreate_downstream = downstream_ddl
        .split_once('\n')
        .map_or_else(|| downstream_ddl.clone(), |(_, ddl)| ddl.to_string());
    sqlx::raw_sql(sqlx::AssertSqlSafe(recreate_upstream))
        .execute(&restored_pool)
        .await
        .expect("recreate upstream stream table");
    sqlx::raw_sql(sqlx::AssertSqlSafe(recreate_downstream))
        .execute(&restored_pool)
        .await
        .expect("recreate downstream stream table");
    sqlx::query("SELECT pgtrickle.rebuild_cdc_triggers()")
        .execute(&restored_pool)
        .await
        .expect("rebuild restored CDC triggers");

    sqlx::query("INSERT INTO source VALUES (3, 'three')")
        .execute(&restored_pool)
        .await
        .expect("write after logical restore recreation");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('dump_test_st')")
        .execute(&restored_pool)
        .await
        .expect("refresh recreated upstream stream table");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('dump_test_downstream')")
        .execute(&restored_pool)
        .await
        .expect("refresh recreated downstream stream table");

    let recovery_report: String = sqlx::query_scalar("SELECT pgtrickle.validate_recovery()")
        .fetch_one(&restored_pool)
        .await
        .expect("validate recreated capture state");
    assert!(
        recovery_report.contains("\"status\":\"SAFE\""),
        "recreated DAG must validate SAFE: {recovery_report}"
    );
    let (upstream_count, downstream_count): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM dump_test_st),
                (SELECT count(*) FROM dump_test_downstream)",
    )
    .fetch_one(&restored_pool)
    .await
    .expect("inspect recreated stream tables");
    assert_eq!(upstream_count, 3);
    assert_eq!(downstream_count, 3);
}
