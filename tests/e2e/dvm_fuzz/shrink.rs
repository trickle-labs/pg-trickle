//! In-process cycle/mutation shrinking for the fuzzer-facing `Scenario` type.
//!
//! Mirrors the algorithm in `scripts/dvm_shrink.py` (COR-17). Only whole
//! cycles and individual mutations are reduced here.
//!
// ponytail: row-level shrinking of initial_data lives in scripts/dvm_shrink.py
// only; add it here too if the in-process fuzzer needs minimal repro JSON
// without shelling out to Python.

#![allow(dead_code)]

use super::{MutationCycle, Scenario};

/// Repeatedly remove whole cycles, then individual mutations, keeping a
/// reduction only when `still_fails` still returns true on the candidate.
pub fn shrink_scenario(scenario: &Scenario, still_fails: impl Fn(&Scenario) -> bool) -> Scenario {
    let mut scenario = scenario.clone();
    loop {
        let mut changed = false;

        // (a) remove whole cycles
        let mut i = scenario.cycles.len();
        while i > 0 {
            i -= 1;
            if scenario.cycles.len() <= 1 {
                break;
            }
            let mut candidate = scenario.clone();
            candidate.cycles.remove(i);
            if still_fails(&candidate) {
                scenario = candidate;
                changed = true;
            }
        }

        // (b) remove individual mutations within remaining cycles
        for ci in 0..scenario.cycles.len() {
            let mut mi = scenario.cycles[ci].mutations.len();
            while mi > 0 {
                mi -= 1;
                if scenario.cycles[ci].mutations.len() <= 1 {
                    break;
                }
                let mut candidate = scenario.clone();
                candidate.cycles[ci].mutations.remove(mi);
                if still_fails(&candidate) {
                    scenario = candidate;
                    changed = true;
                }
            }
        }

        if !changed {
            break;
        }
    }
    scenario
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm_fuzz::{
        ExecutionSettings, ExpectedCapability, FeatureVector, Mutation, QuerySpec, SchemaSpec,
    };

    fn mutation(sql: &str, expected_affected_rows: u64) -> Mutation {
        Mutation {
            sql: sql.to_string(),
            expected_affected_rows,
        }
    }

    fn cycle(name: &str, mutations: Vec<Mutation>) -> MutationCycle {
        MutationCycle {
            name: name.to_string(),
            mutations,
            changed_leaves: Vec::new(),
            mutation_intents: Vec::new(),
        }
    }

    /// Hand-built scenario shaped like cor939_two_leaf_snapshot.json, with
    /// three cycles where only the middle one carries a marker mutation.
    fn sample_scenario() -> Scenario {
        Scenario {
            scenario_id: "cor939_two_leaf_snapshot".to_string(),
            format_version: 1,
            generator_version: "test".to_string(),
            seed: 939,
            schema: SchemaSpec {
                name: "dvmf_shrink_test".to_string(),
                setup_sql: vec![
                    "CREATE TABLE dvmf_shrink_test.parent (id INT PRIMARY KEY, name TEXT)"
                        .to_string(),
                    "CREATE TABLE dvmf_shrink_test.left_leaf (id INT PRIMARY KEY, parent_id INT, score INT)"
                        .to_string(),
                    "CREATE TABLE dvmf_shrink_test.right_leaf (id INT PRIMARY KEY, parent_id INT, rating INT)"
                        .to_string(),
                ],
            },
            initial_data: vec![
                "INSERT INTO dvmf_shrink_test.parent VALUES (1, 'p1'), (2, 'p2')".to_string(),
                "INSERT INTO dvmf_shrink_test.left_leaf VALUES (1, 1, 50), (2, 2, 80)".to_string(),
                "INSERT INTO dvmf_shrink_test.right_leaf VALUES (1, 1, 4), (2, 2, 5)".to_string(),
            ],
            query: QuerySpec {
                stream_table: "dvmf_shrink_test.two_leaf_st".to_string(),
                defining_query: "SELECT p.id FROM dvmf_shrink_test.parent p".to_string(),
                columns: vec!["id".to_string()],
            },
            cycles: vec![
                cycle(
                    "warmup",
                    vec![mutation(
                        "UPDATE dvmf_shrink_test.parent SET name = 'x' WHERE id = 1",
                        1,
                    )],
                ),
                cycle(
                    "marker-cycle",
                    vec![
                        mutation("INSERT INTO dvmf_shrink_test.left_leaf VALUES (3, 1, 1)", 1),
                        mutation(
                            "UPDATE dvmf_shrink_test.right_leaf SET rating = 3 WHERE id = 1",
                            2, // MARKER: deliberately wrong expected_affected_rows
                        ),
                        mutation("INSERT INTO dvmf_shrink_test.right_leaf VALUES (3, 2, 1)", 1),
                    ],
                ),
                cycle(
                    "trailing",
                    vec![mutation(
                        "UPDATE dvmf_shrink_test.parent SET name = 'y' WHERE id = 2",
                        1,
                    )],
                ),
            ],
            execution: ExecutionSettings {
                schedule: "1m".to_string(),
                requested_refresh_mode: "DIFFERENTIAL".to_string(),
            },
            expected_capability: ExpectedCapability {
                differential: true,
                expected_mode: "DIFFERENTIAL".to_string(),
            },
            features: FeatureVector {
                aggregates: vec![],
                joins: vec![],
                simultaneous_source_changes: true,
                nullable_groups: false,
                duplicate_rows: false,
                changed_leaf_buckets: vec![],
                mutation_intents: vec![],
            },
        }
    }

    /// The marker: exactly the mutation carrying `expected_affected_rows == 2`.
    fn has_marker(scenario: &Scenario) -> bool {
        scenario
            .cycles
            .iter()
            .flat_map(|c| c.mutations.iter())
            .any(|m| m.expected_affected_rows == 2)
    }

    #[test]
    fn removes_cycles_and_mutations_without_the_marker() {
        let scenario = sample_scenario();
        assert!(has_marker(&scenario));

        let shrunk = shrink_scenario(&scenario, has_marker);

        assert_eq!(shrunk.cycles.len(), 1, "padding cycles should be removed");
        assert_eq!(
            shrunk.cycles[0].mutations.len(),
            1,
            "sibling mutations without the marker should be removed"
        );
        assert!(has_marker(&shrunk));
        assert_eq!(shrunk.cycles[0].mutations[0].expected_affected_rows, 2);
    }

    #[test]
    fn keeps_whole_scenario_when_nothing_can_be_removed() {
        let scenario = sample_scenario();
        // Oracle that requires everything -- nothing should be reducible.
        let still_fails = |candidate: &Scenario| {
            candidate.cycles.len() == scenario.cycles.len()
                && candidate
                    .cycles
                    .iter()
                    .zip(scenario.cycles.iter())
                    .all(|(a, b)| a.mutations.len() == b.mutations.len())
        };
        let shrunk = shrink_scenario(&scenario, still_fails);
        assert_eq!(shrunk.cycles.len(), scenario.cycles.len());
    }
}
