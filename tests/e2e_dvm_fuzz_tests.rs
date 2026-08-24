//! Standalone entry point for internal serialized DVM correctness cases.
//!
//! Exploratory generation can evolve independently of the external SQLancer
//! adapter. The PR gate validates the canonical scenarios without regenerating
//! them.

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

use dvm_fuzz::{load_scenario, validate_scenario};
use std::path::PathBuf;

#[test]
fn test_dvm_fuzz_scenarios_are_versioned_and_replayable() {
    for name in [
        "cor938_physical_width.json",
        "cor939_two_leaf_snapshot.json",
    ] {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/corpus/dvm_regressions")
            .join(name);
        let scenario = load_scenario(&path)
            .unwrap_or_else(|error| panic!("invalid DVM scenario {}: {error}", path.display()));
        validate_scenario(&scenario).expect("loaded scenarios must remain valid");
    }
}
