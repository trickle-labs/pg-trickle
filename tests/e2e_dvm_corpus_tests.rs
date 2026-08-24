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
