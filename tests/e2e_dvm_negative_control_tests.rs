//! Release-gate enforcement: every active negative control must fail replay.
//!
//! A negative control is a scenario that is deliberately wrong (a claimed
//! capability the implementation doesn't have, or a deliberately mismatched
//! expected result) so that replaying it MUST fail. If a negative control
//! ever stops failing, the correctness harness's detectors (exact-mismatch /
//! silent-fallback checks) have silently broken.

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

use dvm_fuzz::{load_scenario, replay};
use e2e::E2eDb;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct ExpectedFailure {
    expected_failure_class: String,
    expected_phase: String,
    expected_cycle: Option<usize>,
    expected_mutation: Option<usize>,
}

impl ExpectedFailure {
    fn matches(&self, failure: &dvm_fuzz::ReplayFailure) -> bool {
        failure.failure_class == self.expected_failure_class
            && failure.phase == self.expected_phase
            && failure.cycle == self.expected_cycle
            && failure.mutation == self.expected_mutation
    }
}

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/dvm_negative_controls")
}

#[tokio::test]
async fn test_negative_controls_are_detected() {
    let dir = corpus_dir();
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", dir.display()))
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    paths.sort();
    assert!(
        !paths.is_empty(),
        "no negative control scenarios found in {}",
        dir.display()
    );

    let db = E2eDb::new().await.with_extension().await;
    let baseline = load_scenario(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/corpus/dvm_regressions/cor939_two_leaf_snapshot.json"),
    )
    .expect("correct baseline must load");
    replay(&db, &baseline)
        .await
        .expect("detectors must accept the uncorrupted baseline");
    for path in paths {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let scenario = load_scenario(&path).unwrap_or_else(|error| {
            panic!(
                "invalid DVM negative control scenario {}: {error}",
                path.display()
            )
        });
        let metadata: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let expected: ExpectedFailure =
            serde_json::from_value(metadata["negative_control"].clone())
                .expect("negative control must declare its class and phase");
        let failure = replay(&db, &scenario)
            .await
            .expect_err(&format!("negative control {name} must fail"));
        assert!(
            expected.matches(&failure),
            "{name}: expected {expected:?}, observed {failure:?}"
        );
    }
}

#[tokio::test]
async fn test_negative_control_setup_failure_is_rejected() {
    let path = corpus_dir().join("neg_multiset_mismatch.json");
    let mut scenario = load_scenario(&path).unwrap();
    scenario.schema.setup_sql[0] = "SELECT 1 / 0".to_string();
    let metadata: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let expected: ExpectedFailure =
        serde_json::from_value(metadata["negative_control"].clone()).unwrap();
    let db = E2eDb::new().await.with_extension().await;
    let failure = replay(&db, &scenario).await.unwrap_err();
    assert_eq!(failure.phase, "setup SQL 0");
    assert!(!expected.matches(&failure), "setup is not a semantic kill");
}
