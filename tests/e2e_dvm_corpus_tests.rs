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
async fn test_dvm_numeric_scale_python_replay_and_corruption_control() {
    let db = E2eDb::new().await.with_extension().await;
    let directory = PathBuf::from(
        std::env::var("DVM_ARTIFACT_DIR").unwrap_or_else(|_| "artifacts/dvm-fuzz".to_string()),
    )
    .join("numeric-python-replay");
    std::fs::create_dir_all(&directory).expect("Python replay directory");
    let cases: Vec<_> = dvm_fuzz::semantic::bounded_cases()
        .into_iter()
        .filter(|scenario| scenario.execution.observe_numeric_scale)
        .collect();
    assert_eq!(cases.len(), 3, "all declared numeric-scale cases must run");
    let mut observed = Vec::new();
    for scenario in &cases {
        let path = directory.join(format!("{}.json", scenario.scenario_id));
        std::fs::write(&path, serde_json::to_vec_pretty(scenario).unwrap()).unwrap();
        let output = std::process::Command::new("python3")
            .args([
                "scripts/dvm_replay.py",
                "--database",
                db.connection_string(),
            ])
            .arg(&path)
            .output()
            .expect("start Python replay against Testcontainers");
        assert!(
            output.status.success(),
            "{}: {}\n{}",
            scenario.scenario_id,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        observed.push(scenario.scenario_id.clone());
    }
    let mut control = serde_json::to_value(&cases[0]).unwrap();
    control["scenario_id"] = "numeric_scale_corruption".into();
    control["execution"]["require_differential_history"] = false.into();
    control["cycles"] = serde_json::json!([{
        "name": "scale_corruption",
        "mutations": [{
            "sql": "WITH injection AS MATERIALIZED (SELECT set_config('pg_trickle.internal_refresh','true',true)) UPDATE sem_numeric_observations.result SET v=1.00 FROM injection WHERE id=1",
            "expected_affected_rows": 1
        }]
    }]);
    control["negative_control"] = serde_json::json!({
        "expected_failure_class": "MultisetMismatch",
        "expected_phase": "scale_corruption exact comparison",
        "expected_cycle": 0,
        "expected_mutation": null
    });
    let path = directory.join("numeric_scale_corruption.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&control).unwrap()).unwrap();
    let output = std::process::Command::new("python3")
        .args([
            "scripts/dvm_replay.py",
            "--database",
            db.connection_string(),
            "--expect-negative-control",
        ])
        .arg(path)
        .output()
        .expect("start scale corruption control");
    assert!(
        output.status.success(),
        "scale control: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::write(
        directory.join("observations.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "selected": 3, "replayed": observed,
            "negative_control": control["negative_control"],
            "installed_candidate_commit": std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
        }))
        .unwrap(),
    )
    .unwrap();
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
