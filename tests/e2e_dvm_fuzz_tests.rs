//! Standalone entry point for internal serialized DVM correctness cases.
//!
//! Exploratory generation can evolve independently of the external SQLancer
//! adapter. The PR gate validates the canonical scenarios without regenerating
//! them.

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

#[path = "e2e/dvm_fuzz/shrink.rs"]
mod shrink;

use dvm_fuzz::{MutationCycle, Scenario, load_scenario, validate_scenario};
use regex_lite::Regex;
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

fn negative_control_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/corpus/dvm_negative_controls/negctrl_injected_939.json")
}

/// Same static condition as Python's `static_injected_defect_present`: a
/// mutation whose SQL matches `UPDATE ... WHERE id = <int>` (optionally
/// followed by `RETURNING ...`) case-insensitively, declaring an
/// `expected_affected_rows` other than 1.
fn still_carries_the_injected_defect(scenario: &Scenario) -> bool {
    let pk_update =
        Regex::new(r"(?is)^\s*UPDATE\b.*\bWHERE\s+\w+\s*=\s*\d+\s*(RETURNING\b.*)?;?\s*$")
            .expect("static regex is valid");
    scenario.cycles.iter().any(|cycle: &MutationCycle| {
        cycle
            .mutations
            .iter()
            .any(|m| pk_update.is_match(&m.sql) && m.expected_affected_rows != 1)
    })
}

#[test]
fn test_shrink_preserves_injected_defect() {
    let scenario = load_scenario(&negative_control_path())
        .unwrap_or_else(|error| panic!("invalid negative-control scenario: {error}"));
    assert!(still_carries_the_injected_defect(&scenario));

    // Pure structural shrink: no DB involved, mirrors scripts/dvm_shrink.py's
    // --selftest.
    let shrunk = shrink::shrink_scenario(&scenario, still_carries_the_injected_defect);
    assert_eq!(shrunk.cycles.len(), 1, "padding cycles must be removed");
    assert_eq!(
        shrunk.cycles[0].mutations.len(),
        1,
        "only the mutation carrying the injected defect should remain"
    );
    assert!(still_carries_the_injected_defect(&shrunk));
}
