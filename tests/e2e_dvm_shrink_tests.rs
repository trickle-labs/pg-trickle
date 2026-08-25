//! Structural shrink check plus a live-DB confirmation of the injected
//! negative-control fixture (COR-17 / COR-20).

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

#[path = "e2e/dvm_fuzz/shrink.rs"]
mod shrink;

use dvm_fuzz::{MutationCycle, Scenario, load_scenario, replay};
use e2e::E2eDb;
use regex_lite::Regex;
use std::path::PathBuf;

fn negctrl_path() -> PathBuf {
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

#[tokio::test]
async fn shrinks_to_the_injected_defect_and_confirms_live_failure() {
    let scenario = load_scenario(&negctrl_path())
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

    // Live-DB confirmation that the ORIGINAL, unshrunk fixture really is a
    // negative control: it must fail replay with GeneratorInvalid.
    let db = E2eDb::new().await.with_extension().await;
    let error = replay(&db, &scenario)
        .await
        .expect_err("negative control must fail replay");
    assert_eq!(error.failure_class, "GeneratorInvalid");
}
