//! Executable semantic oracle controls: each candidate mutation must be
//! rejected as a value/bag mismatch, never by a schema or comparator error.
//! These test detector sensitivity; they do not mutate the production engine.

mod e2e;

use e2e::{E2eDb, oracle};

#[tokio::test]
async fn test_semantic_mutants_are_killed_by_exact_bag_oracle() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TABLE semantic_parent (id INT PRIMARY KEY, active BOOLEAN, payload TEXT)",
        "CREATE TABLE semantic_child (id INT PRIMARY KEY, parent_id INT, qty BIGINT)",
        "CREATE TABLE semantic_receipts (id INT, committed BOOLEAN)",
        "INSERT INTO semantic_parent VALUES (1, true, 'correct'), (2, false, 'other'), (3, true, 'last')",
        "INSERT INTO semantic_child VALUES (10, 1, 2), (11, 1, 3), (12, 2, 7)",
        "INSERT INTO semantic_receipts VALUES (1, true), (1, true), (2, false)",
    ]).await;

    let cases = [
        (
            "predicate",
            "SELECT id, payload FROM semantic_parent WHERE active",
            "SELECT id, payload FROM semantic_parent WHERE NOT active",
        ),
        (
            "count",
            "SELECT parent_id, count(*) AS n FROM semantic_child GROUP BY parent_id",
            "SELECT parent_id, count(*) + 1 AS n FROM semantic_child GROUP BY parent_id",
        ),
        (
            "payload",
            "SELECT id, payload FROM semantic_parent",
            "SELECT id, CASE WHEN id = 1 THEN 'corrupt' ELSE payload END AS payload FROM semantic_parent",
        ),
        (
            "join",
            "SELECT p.id, c.qty FROM semantic_parent p JOIN semantic_child c ON c.parent_id = p.id",
            "SELECT p.id, c.qty FROM semantic_parent p JOIN semantic_child c ON c.parent_id = p.id + 1",
        ),
        (
            "receipt",
            "SELECT id FROM semantic_receipts WHERE committed",
            "SELECT DISTINCT id FROM semantic_receipts WHERE committed",
        ),
    ];

    for (name, canonical, mutant) in cases {
        // Materialize the correct semantics as the candidate result and compare
        // with the mutated semantics, preserving column names and types.
        db.execute(&format!("CREATE TABLE semantic_actual AS {canonical}"))
            .await;
        oracle::compare_st_to_query(&db, "semantic_actual", canonical)
            .await
            .unwrap_or_else(|diff| panic!("{name}: canonical control failed: {diff}"));
        let diff = oracle::compare_st_to_query(&db, "semantic_actual", mutant)
            .await
            .unwrap_err();
        assert!(
            diff.comparison_error.is_none(),
            "{name}: comparator error is not a semantic kill: {diff}"
        );
        assert!(
            diff.schema_mismatch.is_none(),
            "{name}: schema error is not a semantic kill: {diff}"
        );
        assert!(
            diff.extra_count > 0 || diff.missing_count > 0,
            "{name}: no semantic bag delta: {diff}"
        );
        db.execute("DROP TABLE semantic_actual").await;
    }
}
