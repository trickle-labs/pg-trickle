//! v0.103 stability contract: WAL admission is receipt-backed and replayable.

mod common;
mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_v103_wal_admission_is_receipt_backed() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE v098_wal_src (id INT PRIMARY KEY, value TEXT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                name => 'v098_wal_st',\
                query => $$SELECT id, value FROM v098_wal_src$$,\
                schedule => '1m',\
                refresh_mode => 'FULL',\
                cdc_mode => 'wal',\
                initialize => false\
            )",
        )
        .await;
    assert!(
        result.is_ok(),
        "receipt-backed WAL admission should succeed: {result:?}"
    );

    let stream_exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'v098_wal_st')",
        )
        .await;
    assert!(
        stream_exists,
        "WAL admission must create the catalog entry before asynchronous slot setup"
    );

    let slot_count: i64 = db
        .query_scalar(
            "SELECT count(*)::bigint FROM pg_replication_slots \
             WHERE slot_name LIKE 'pgtrickle_%'",
        )
        .await;
    assert_eq!(slot_count, 0, "WAL admission must not create a slot");
}

#[tokio::test]
async fn test_v098_integration_capabilities_are_disabled() {
    let db = E2eDb::new().await.with_extension().await;

    let disabled: i64 = db
        .query_scalar(
            "SELECT count(*)::bigint FROM pgtrickle.integration_capabilities() \
             WHERE enabled = false AND details->>'status' = 'experimental'",
        )
        .await;
    assert_eq!(disabled, 2);
}
