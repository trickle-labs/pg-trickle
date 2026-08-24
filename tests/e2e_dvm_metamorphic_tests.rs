//! v0.87.4 state-directed and metamorphic DVM correctness checks.

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

use dvm_fuzz::metamorphic::MetamorphicFamily;
use dvm_fuzz::mutation::{ChangedLeaves, plan};
use e2e::E2eDb;

#[test]
fn test_v0874_metamorphic_inventory_and_state_coverage() {
    assert!(MetamorphicFamily::all().len() >= 6);
    assert_eq!(plan(0x874, ChangedLeaves::One).len(), 10);
    assert_eq!(plan(0x874, ChangedLeaves::Two)[0].leaves.len(), 2);
    assert_eq!(plan(0x874, ChangedLeaves::All)[0].leaves.len(), 3);
}

/// A split refresh history and a single batched refresh must converge to the
/// same exact result when they start from the same source state.
#[tokio::test]
async fn test_v0874_refresh_batching_and_multi_source_history() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    db.execute_seq(&[
        "CREATE TABLE v0874_left (id INT PRIMARY KEY, grp TEXT, value INT)",
        "CREATE TABLE v0874_right (id INT PRIMARY KEY, grp TEXT, value INT)",
        "INSERT INTO v0874_left VALUES (1, 'a', 10), (2, 'b', 20)",
        "INSERT INTO v0874_right VALUES (1, 'a', 100), (2, 'b', 200)",
    ])
    .await;

    let query = "WITH l AS (SELECT grp, SUM(value) AS total_left FROM v0874_left GROUP BY grp), r AS (SELECT grp, SUM(value) AS total_right FROM v0874_right GROUP BY grp) SELECT COALESCE(l.grp, r.grp) AS grp, l.total_left, r.total_right FROM l FULL JOIN r ON l.grp = r.grp";
    db.create_st("v0874_split", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st("v0874_batch", query, "1h", "DIFFERENTIAL")
        .await;
    db.refresh_st("v0874_split").await;
    db.refresh_st("v0874_batch").await;

    db.execute("UPDATE v0874_left SET value = value + 1 WHERE id = 1")
        .await;
    db.refresh_st("v0874_split").await;

    db.execute("UPDATE v0874_right SET value = value + 1 WHERE id = 1")
        .await;
    db.refresh_st("v0874_split").await;
    db.refresh_st("v0874_batch").await;

    e2e::oracle::assert_effective_refresh_mode(&db, "v0874_split", "DIFFERENTIAL")
        .await
        .expect("split history must remain differential");
    e2e::oracle::assert_effective_refresh_mode(&db, "v0874_batch", "DIFFERENTIAL")
        .await
        .expect("batched history must remain differential");
    e2e::oracle::assert_st_query_exact(&db, "v0874_split", query, "split history").await;
    e2e::oracle::assert_st_query_exact(&db, "v0874_batch", query, "batched history").await;
    e2e::oracle::compare_sts(&db, "v0874_split", "v0874_batch")
        .await
        .expect("equivalent refresh histories must converge");

    db.refresh_st("v0874_split").await;
    db.refresh_st("v0874_batch").await;
    e2e::oracle::compare_sts(&db, "v0874_split", "v0874_batch")
        .await
        .expect("refresh idempotence must preserve exact equality");
}
