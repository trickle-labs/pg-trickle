mod e2e;
use e2e::E2eDb;
use std::process::Command;

#[tokio::test]
async fn test_pg_dump_restore_fails_closed() {
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

    assert_eq!(db.count("public.dump_test_st").await, 2);

    let container_id = db.container_id();

    // 1. pg_dump the database
    let dump_output = Command::new("docker")
        .args([
            "exec",
            container_id,
            "pg_dump",
            "-U",
            "postgres",
            "-d",
            "postgres",
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
    assert!(restore_data.status.success(), "pg_restore data failed");

    // 5. Connect to restored DB to manually heal metadata buffer
    let restored_conn_str = db
        .connection_string()
        .replace("/postgres?", "/restored_db?");
    let restored_pool = sqlx::PgPool::connect(&restored_conn_str).await.unwrap();

    // Unsafe logical restore is deliberately rejected until protected
    // reinitialization can guarantee catalog, CDC, and frontier consistency.
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
}
