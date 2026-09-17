//! v0.107 executable evidence for opt-in automatic WAL capture.

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

async fn cdc_mode(db: &E2eDb, source: &str) -> String {
    let source_oid = db.table_oid(source).await;
    db.query_scalar(&format!(
        "SELECT cdc_mode FROM pgtrickle.pgt_dependencies \
         WHERE source_relid = {source_oid} LIMIT 1"
    ))
    .await
}

#[tokio::test]
async fn test_v107_auto_wal_transition_captures_changes() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'auto'", "auto")
        .await;

    db.execute("CREATE TABLE v107_wal_source (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("ALTER TABLE v107_wal_source REPLICA IDENTITY FULL")
        .await;
    db.execute("INSERT INTO v107_wal_source VALUES (1, 'initial')")
        .await;
    db.create_st(
        "v107_wal_stream",
        "SELECT id, val FROM v107_wal_source",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let effective_capture = loop {
        let mode = cdc_mode(&db, "v107_wal_source").await;
        if mode == "WAL" || std::time::Instant::now() >= deadline {
            break mode;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(effective_capture, "WAL");

    let source_oid = db.table_oid("v107_wal_source").await;
    let stable_name: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({source_oid}::oid)"
        ))
        .await;
    let slot_exists: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots \
             WHERE slot_name = 'pgtrickle_{stable_name}')"
        ))
        .await;
    assert!(slot_exists, "WAL transition must create a replication slot");

    let _ = db
        .wait_for_auto_refresh("v107_wal_stream", Duration::from_secs(15))
        .await;
    db.execute("INSERT INTO v107_wal_source VALUES (2, 'captured'), (3, 'also captured')")
        .await;
    assert!(
        db.wait_for_auto_refresh("v107_wal_stream", Duration::from_secs(30))
            .await,
        "WAL changes must schedule a refresh"
    );
    db.assert_st_matches_query("v107_wal_stream", "SELECT id, val FROM v107_wal_source")
        .await;
}
