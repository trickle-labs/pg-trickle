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
use std::path::PathBuf;

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
    for path in paths {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let scenario = load_scenario(&path).unwrap_or_else(|error| {
            panic!(
                "invalid DVM negative control scenario {}: {error}",
                path.display()
            )
        });
        assert!(
            replay(&db, &scenario).await.is_err(),
            "negative control {name} must fail — if it passes, the correctness \
             harness's detectors silently broke"
        );
    }
}
