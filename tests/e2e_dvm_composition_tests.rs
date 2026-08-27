//! v0.87.3 mandatory composition matrix (COR-7–COR-9).

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

use dvm_fuzz::coverage::{MANDATORY_CASE_COUNT, mandatory_cases, p0_dimensions, pairwise_cover};
use dvm_fuzz::query::RelNode;
use e2e::E2eDb;

const COVERAGE_REQUIREMENTS: &str = include_str!("corpus/dvm_coverage_requirements.json");

#[test]
fn test_v0873_matrix_matches_published_requirements() {
    let requirements: serde_json::Value =
        serde_json::from_str(COVERAGE_REQUIREMENTS).expect("coverage requirements must be JSON");
    assert_eq!(
        requirements["required_mandatory_cases"].as_u64(),
        Some(MANDATORY_CASE_COUNT as u64)
    );

    let cases = mandatory_cases();
    let expected_ids = requirements["mandatory_cases"]
        .as_array()
        .expect("mandatory_cases must be an array")
        .iter()
        .map(|value| value.as_str().expect("case IDs must be strings"))
        .collect::<Vec<_>>();
    assert_eq!(
        cases.iter().map(|case| case.id).collect::<Vec<_>>(),
        expected_ids
    );

    let assignments = cases
        .iter()
        .map(|case| case.dimensions.clone())
        .collect::<Vec<_>>();
    let report = pairwise_cover(&p0_dimensions(), &assignments);
    assert_eq!(report.uncovered_pairs, Vec::new());
    assert_eq!(report.total_pairs, report.covered_pairs);
    assert_eq!(report.total_pairs, 24);
    assert_eq!(
        requirements["generated_coverage"]["uncovered_pairs"]
            .as_array()
            .map(Vec::len),
        Some(0)
    );
}

#[test]
fn test_v0873_generated_queries_have_schemas_and_render_sql() {
    let mut shapes = std::collections::HashSet::new();
    for case in mandatory_cases() {
        assert!(
            shapes.insert(case.shape),
            "duplicate named shape {}",
            case.shape
        );
        assert_eq!(
            case.query.ctes.len(),
            2,
            "{} must have two named CTEs",
            case.id
        );
        match case.id {
            "nested_join_subtree" => assert!(matches!(case.query.body, RelNode::Subquery { .. })),
            "wide_source_narrow_projection" => {
                assert!(matches!(case.query.body, RelNode::Project { .. }))
            }
            _ => assert!(matches!(
                case.query.body,
                RelNode::Subquery { .. } | RelNode::Join { .. } | RelNode::Project { .. }
            )),
        }
        let schema = case
            .query
            .schema()
            .unwrap_or_else(|error| panic!("{} has invalid schema: {error}", case.id));
        assert!(
            !schema.columns.is_empty(),
            "{} has no output columns",
            case.id
        );
        let sql = case
            .query
            .render_sql()
            .unwrap_or_else(|error| panic!("{} has invalid SQL: {error}", case.id));
        assert!(sql.contains(case.shape), "{} lost its named shape", case.id);
        assert!(sql.starts_with("WITH "), "{} is not a CTE query", case.id);
    }
}

/// Run the mandatory query shapes against PostgreSQL and compare each accepted
/// differential stream table with the direct defining query.  The matrix is
/// intentionally small and deterministic so it can run on every PR.
#[tokio::test]
async fn test_v0873_mandatory_composition_matrix() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    create_matrix_sources(&db).await;

    for case in mandatory_cases() {
        let stream_table = format!("dvm_comp_{}", case.id);
        let query = case
            .query
            .render_sql()
            .unwrap_or_else(|error| panic!("{} render failed: {error}", case.id));
        db.create_st(&stream_table, &query, "1h", "DIFFERENTIAL")
            .await;
        db.refresh_st(&stream_table).await;
        e2e::oracle::assert_effective_refresh_mode(&db, &stream_table, "DIFFERENTIAL")
            .await
            .unwrap_or_else(|failure| panic!("{} did not stay differential: {failure:?}", case.id));
        e2e::oracle::assert_st_query_exact(&db, &stream_table, &query, case.id).await;

        let id = (case
            .id
            .as_bytes()
            .iter()
            .map(|byte| *byte as usize)
            .sum::<usize>()
            % 3)
            + 1;
        let left_delta = (case.id.len() % 5 + 1) as i32;
        let right_delta = (case.id.bytes().next().unwrap_or(1) % 5 + 1) as i32;
        let mut mutations = vec![format!(
            "UPDATE composition_left SET value = value + {left_delta} WHERE id = {id}"
        )];
        if case.changed_leaves != "one" {
            mutations.push(format!(
                "UPDATE composition_right SET value = value + {right_delta} WHERE id = {id}"
            ));
        }
        db.execute_seq(&mutations.iter().map(String::as_str).collect::<Vec<_>>())
            .await;
        db.refresh_st(&stream_table).await;
        e2e::oracle::assert_st_query_exact(&db, &stream_table, &query, case.id).await;
    }
}

async fn create_matrix_sources(db: &E2eDb) {
    db.execute_seq(&[
        "CREATE TABLE composition_left (
            id INT PRIMARY KEY,
            grp TEXT,
            value INT,
            note TEXT,
            unused_1 INT, unused_2 INT, unused_3 INT, unused_4 INT,
            unused_5 INT, unused_6 INT, unused_7 INT, unused_8 INT,
            unused_9 INT, unused_10 INT, unused_11 INT, unused_12 INT,
            unused_13 INT, unused_14 INT, unused_15 INT, unused_16 INT
        )",
        "CREATE TABLE composition_right (
            id INT PRIMARY KEY,
            grp TEXT,
            value INT,
            note TEXT,
            unused_1 INT, unused_2 INT, unused_3 INT, unused_4 INT,
            unused_5 INT, unused_6 INT, unused_7 INT, unused_8 INT,
            unused_9 INT, unused_10 INT, unused_11 INT, unused_12 INT,
            unused_13 INT, unused_14 INT, unused_15 INT, unused_16 INT
        )",
        "INSERT INTO composition_left (id, grp, value, note)
        VALUES (1, 'a', 10, 'left-a'), (2, NULL, 20, 'left-null'),
               (3, 'b', 30, 'left-b')",
        "INSERT INTO composition_right (id, grp, value, note)
        VALUES (1, 'a', 100, 'right-a'), (2, NULL, 200, 'right-null'),
               (3, 'b', 300, 'right-b')",
    ])
    .await;
}
