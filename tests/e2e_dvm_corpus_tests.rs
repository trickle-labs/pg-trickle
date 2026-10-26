//! PR-gated replay of the permanent v0.87.2 DVM regression corpus.

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

use dvm_fuzz::{load_scenario, replay};
use e2e::E2eDb;
use std::path::PathBuf;

const CORPUS: &[&str] = &[
    "cor938_physical_width.json",
    "cor939_two_leaf_snapshot.json",
];

fn corpus_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/corpus/dvm_regressions")
        .join(name)
}

#[tokio::test]
async fn test_dvm_corpus_replays_without_generator() {
    let db = E2eDb::new().await.with_extension().await;
    for name in CORPUS {
        let path = corpus_path(name);
        let scenario = load_scenario(&path).unwrap_or_else(|error| {
            panic!("invalid DVM corpus scenario {}: {error}", path.display())
        });
        replay(&db, &scenario)
            .await
            .unwrap_or_else(|error| panic!("DVM corpus replay failed for {name}: {error}"));
    }
}

#[tokio::test]
async fn test_dvm_generated_semantic_boundaries_execute_differentially() {
    let db = E2eDb::new().await.with_extension().await;
    let max_size = std::env::var("DVM_SEMANTIC_MAX_BAG_SIZE")
        .map(|value| value.parse::<usize>().expect("integer bag bound"))
        .unwrap_or(2);
    let cases = dvm_fuzz::semantic::cases_for_bags(max_size);
    let requirements: serde_json::Value =
        serde_json::from_str(include_str!("corpus/dvm_coverage_requirements.json"))
            .expect("published semantic floor");
    assert_eq!(
        cases.len(),
        requirements["postgresql_boundary_floor"][if max_size == 2 {
            "pr_cases"
        } else {
            "scheduled_cases"
        }]
        .as_u64()
        .expect("nonempty semantic floor") as usize,
        "the selected floor cannot be empty"
    );
    let mut observed = std::collections::BTreeSet::new();
    let mut failures = Vec::new();
    for scenario in &cases {
        match replay(&db, scenario).await {
            Ok(()) => {
                observed.insert(scenario.scenario_id.clone());
            }
            Err(error) => failures.push(format!("{}: {error}", scenario.scenario_id)),
        }
    }
    let directory =
        std::env::var("DVM_ARTIFACT_DIR").unwrap_or_else(|_| "artifacts/dvm-fuzz".to_string());
    std::fs::create_dir_all(&directory).expect("semantic observation directory");
    let report = serde_json::json!({
        "generator": "semantic-boundaries-v1", "seed": 1217,
        "candidate_commit": std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").unwrap_or_else(|_| "unbound".to_string()),
        "selected": cases.len(), "admitted_exact_differential": observed,
        "failures": failures,
    });
    std::fs::write(
        std::path::Path::new(&directory).join("semantic-observations.json"),
        serde_json::to_string_pretty(&report).expect("serialize observed floor"),
    )
    .expect("write observed floor");
    assert!(
        failures.is_empty(),
        "semantic failures:\n{}",
        failures.join("\n")
    );
    assert_eq!(
        observed.len(),
        cases.len(),
        "all selected cases must execute"
    );
}
