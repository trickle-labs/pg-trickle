//! Live-DB confirmation of the injected negative control (COR-17 / COR-20).
//! Structural shrinking runs in the database-free DVM harness tier.

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

use dvm_fuzz::{load_scenario, replay};
use e2e::E2eDb;
use std::path::PathBuf;

fn negctrl_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/corpus/dvm_negative_controls/negctrl_injected_939.json")
}

#[tokio::test]
async fn test_injected_defect_confirms_live_failure() {
    let scenario = load_scenario(&negctrl_path())
        .unwrap_or_else(|error| panic!("invalid negative-control scenario: {error}"));
    // Live-DB confirmation that the ORIGINAL, unshrunk fixture really is a
    // negative control: it must fail replay with GeneratorInvalid.
    let db = E2eDb::new().await.with_extension().await;
    let error = replay(&db, &scenario)
        .await
        .expect_err("negative control must fail replay");
    assert_eq!(error.failure_class, "GeneratorInvalid");
}
