//! v0.87.4 state-directed and metamorphic DVM correctness checks.

use std::collections::BTreeSet;

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

use dvm_fuzz::metamorphic::MetamorphicFamily;
use dvm_fuzz::mutation::{ChangedLeaves, plan};
use e2e::E2eDb;

#[test]
fn test_v0874_state_planner_covers_changed_leaf_buckets() {
    assert_eq!(plan(0x874, ChangedLeaves::One).len(), 10);
    assert_eq!(plan(0x874, ChangedLeaves::Two)[0].leaves.len(), 2);
    assert_eq!(plan(0x874, ChangedLeaves::All)[0].leaves.len(), 3);
}

#[test]
fn test_v08714_metamorphic_families_execute_and_record() {
    let scenario = dvm_fuzz::metamorphic::Scenario::new(
        "WITH source AS (SELECT * FROM input) SELECT key, value FROM input",
        [(1, 10), (2, 20)],
        [
            dvm_fuzz::metamorphic::Mutation::Update { key: 1, value: 99 },
            dvm_fuzz::metamorphic::Mutation::Delete { key: 2 },
        ],
    );
    let executed = MetamorphicFamily::all()
        .iter()
        .map(|family| family.execute(&scenario))
        .collect::<Vec<_>>();
    assert!(executed.len() >= 6);
    assert!(executed.iter().all(|(_, passed)| *passed));
    assert!(executed.iter().take(6).all(|(name, _)| !name.is_empty()));
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

#[tokio::test]
async fn test_v08714_live_metamorphic_families() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TABLE v08714_update_a (id INT PRIMARY KEY, value INT, unused INT)",
        "CREATE TABLE v08714_update_b (id INT PRIMARY KEY, value INT, unused INT)",
        "CREATE TABLE v08714_cte_a (id INT PRIMARY KEY, value INT, unused INT)",
        "CREATE TABLE v08714_cte_b (id INT PRIMARY KEY, value INT, unused INT)",
        "CREATE TABLE v08714_alias_a (id INT PRIMARY KEY, value INT, unused INT)",
        "CREATE TABLE v08714_alias_b (id INT PRIMARY KEY, value INT, unused INT)",
        "CREATE TABLE v08714_widen_a (id INT PRIMARY KEY, value INT)",
        "CREATE TABLE v08714_widen_b (id INT PRIMARY KEY, value INT, unused INT)",
        "CREATE TABLE v08714_join_la (id INT PRIMARY KEY, value INT)",
        "CREATE TABLE v08714_join_ra (id INT PRIMARY KEY, value INT)",
        "CREATE TABLE v08714_join_lb (id INT PRIMARY KEY, value INT)",
        "CREATE TABLE v08714_join_rb (id INT PRIMARY KEY, value INT)",
        "CREATE TABLE v08714_projection_a (id INT PRIMARY KEY, value INT, unused INT)",
        "CREATE TABLE v08714_projection_b (id INT PRIMARY KEY, value INT, unused INT)",
        "INSERT INTO v08714_update_a VALUES (1, 10, 0), (2, 20, 0)",
        "INSERT INTO v08714_update_b VALUES (1, 10, 0), (2, 20, 0)",
        "INSERT INTO v08714_cte_a VALUES (1, 10, 0), (2, 20, 0)",
        "INSERT INTO v08714_cte_b VALUES (1, 10, 0), (2, 20, 0)",
        "INSERT INTO v08714_alias_a VALUES (1, 10, 0), (2, 20, 0)",
        "INSERT INTO v08714_alias_b VALUES (1, 10, 0), (2, 20, 0)",
        "INSERT INTO v08714_widen_a VALUES (1, 10), (2, 20)",
        "INSERT INTO v08714_widen_b VALUES (1, 10, 0), (2, 20, 0)",
        "INSERT INTO v08714_join_la VALUES (1, 10), (2, 20)",
        "INSERT INTO v08714_join_ra VALUES (1, 100), (2, 200)",
        "INSERT INTO v08714_join_lb VALUES (1, 10), (2, 20)",
        "INSERT INTO v08714_join_rb VALUES (1, 100), (2, 200)",
        "INSERT INTO v08714_projection_a VALUES (1, 10, 0), (2, 20, 0)",
        "INSERT INTO v08714_projection_b VALUES (1, 10, 0), (2, 20, 0)",
    ])
    .await;

    let families = [
        (
            "update_vs_delete_insert",
            "v08714_update_a",
            "v08714_update_b",
            "SELECT id, value FROM v08714_update_a",
            "SELECT id, value FROM v08714_update_b",
        ),
        (
            "cte_vs_inline",
            "v08714_cte_a",
            "v08714_cte_b",
            "WITH source AS (SELECT id, value FROM v08714_cte_a) SELECT id, value FROM source",
            "SELECT id, value FROM v08714_cte_b",
        ),
        (
            "alias_renaming",
            "v08714_alias_a",
            "v08714_alias_b",
            "SELECT a.id, a.value FROM v08714_alias_a AS a",
            "SELECT b.id, b.value FROM v08714_alias_b AS b",
        ),
        (
            "irrelevant_column_widening",
            "v08714_widen_a",
            "v08714_widen_b",
            "SELECT id, value FROM v08714_widen_a",
            "SELECT id, value FROM v08714_widen_b",
        ),
        (
            "inner_join_reorder",
            "v08714_join_la",
            "v08714_join_lb",
            "SELECT l.id, l.value AS left_value, r.value AS right_value FROM v08714_join_la l JOIN v08714_join_ra r ON l.id = r.id",
            "SELECT l.id, l.value AS left_value, r.value AS right_value FROM v08714_join_rb r JOIN v08714_join_lb l ON l.id = r.id",
        ),
        (
            "projection_placement",
            "v08714_projection_a",
            "v08714_projection_b",
            "SELECT id, value FROM v08714_projection_a",
            "SELECT id, value FROM (SELECT id, value FROM v08714_projection_b) AS projected",
        ),
    ];
    let mut executed_families = BTreeSet::new();

    for (index, (family, source_a, source_b, query_a, query_b)) in families.iter().enumerate() {
        let st_a = format!("v08714_meta_a_{index}");
        let st_b = format!("v08714_meta_b_{index}");
        db.create_st(&st_a, query_a, "1h", "DIFFERENTIAL").await;
        db.create_st(&st_b, query_b, "1h", "DIFFERENTIAL").await;
        db.refresh_st(&st_a).await;
        db.refresh_st(&st_b).await;
        e2e::oracle::assert_effective_refresh_mode(&db, &st_a, "DIFFERENTIAL")
            .await
            .unwrap_or_else(|failure| panic!("{family}: {failure:?}"));
        e2e::oracle::assert_effective_refresh_mode(&db, &st_b, "DIFFERENTIAL")
            .await
            .unwrap_or_else(|failure| panic!("{family}: {failure:?}"));
        db.execute(&format!(
            "UPDATE {source_a} SET value = value + 5 WHERE id = 1"
        ))
        .await;
        if index == 0 {
            db.execute(&format!("DELETE FROM {source_b} WHERE id = 1"))
                .await;
            db.execute(&format!("INSERT INTO {source_b} VALUES (1, 15, 0)"))
                .await;
        } else {
            db.execute(&format!(
                "UPDATE {source_b} SET value = value + 5 WHERE id = 1"
            ))
            .await;
        }
        db.refresh_st(&st_a).await;
        db.refresh_st(&st_b).await;
        e2e::oracle::assert_st_query_exact(&db, &st_a, query_a, family).await;
        e2e::oracle::assert_st_query_exact(&db, &st_b, query_b, family).await;
        e2e::oracle::compare_sts(&db, &st_a, &st_b)
            .await
            .unwrap_or_else(|diff| panic!("{family}: {diff}"));
        executed_families.insert(*family);
    }

    assert_eq!(executed_families.len(), families.len());
}
