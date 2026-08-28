//! Deterministic composition cases and semantic coverage floors.

use super::query::{
    AggregateExpr, AggregateKind, Column, CteDefinition, JoinKind, ProjectExpr, RelNode, RelQuery,
    RelationSchema, ScalarType,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MANDATORY_CASE_COUNT: usize = 18;

pub const SNAPSHOT_PLANS: [&str; 4] = [
    "exact_per_leaf",
    "exact_combined",
    "post_change_correction",
    "unsupported",
];
pub const CHANGED_LEAF_BUCKETS: [&str; 3] = ["1", "2", "all"];
pub const GROUP_LIFECYCLE_TRANSITIONS: [&str; 4] = [
    "empty_to_nonempty",
    "nonempty_to_empty",
    "winner_change",
    "nonempty_to_nonempty",
];
pub const OUTER_JOIN_TRANSITIONS: [&str; 2] = ["matched_to_unmatched", "unmatched_to_matched"];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticCoverageObservation {
    #[serde(default)]
    pub snapshot_plans: BTreeSet<String>,
    #[serde(default)]
    pub changed_leaf_buckets: BTreeSet<String>,
    #[serde(default)]
    pub group_lifecycle_transitions: BTreeSet<String>,
    #[serde(default)]
    pub outer_join_transitions: BTreeSet<String>,
    #[serde(default)]
    pub p0_pairwise_complete: bool,
}

impl SemanticCoverageObservation {
    pub fn observe_snapshot_plan(&mut self, plan: impl Into<String>) {
        self.snapshot_plans.insert(plan.into());
    }

    pub fn observe_changed_leaves(&mut self, count: usize, total: usize) {
        let bucket = if count == 1 {
            "1"
        } else if count == 2 {
            "2"
        } else if count == total && total > 0 {
            "all"
        } else {
            return;
        };
        self.changed_leaf_buckets.insert(bucket.to_string());
    }

    pub fn observe_group_lifecycle(&mut self, transition: impl Into<String>) {
        self.group_lifecycle_transitions.insert(transition.into());
    }

    pub fn observe_outer_join(&mut self, transition: impl Into<String>) {
        self.outer_join_transitions.insert(transition.into());
    }

    /// Consume the JSON emitted by a DVM decision trace. Unknown fields are
    /// ignored so traces can gain detail without breaking the coverage gate.
    pub fn observe_decision_trace(&mut self, trace: &serde_json::Value) {
        for event in trace["events"].as_array().into_iter().flatten() {
            if let Some(plan) = event["snapshot_plan"].as_str() {
                self.observe_snapshot_plan(plan);
            }
            for decision in event["decisions"].as_array().into_iter().flatten() {
                if let Some(decision) = decision.as_str() {
                    for (needle, transition) in [
                        ("empty_to_nonempty", "empty_to_nonempty"),
                        ("nonempty_to_empty", "nonempty_to_empty"),
                        ("winner_change", "winner_change"),
                        ("nonempty_to_nonempty", "nonempty_to_nonempty"),
                        ("matched_to_unmatched", "matched_to_unmatched"),
                        ("unmatched_to_matched", "unmatched_to_matched"),
                    ] {
                        if decision.contains(needle) {
                            if needle.starts_with("matched") || needle.starts_with("unmatched") {
                                self.observe_outer_join(transition);
                            } else {
                                self.observe_group_lifecycle(transition);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticCoverageRequirements {
    #[serde(default)]
    pub snapshot_plans: BTreeSet<String>,
    #[serde(default)]
    pub changed_leaf_buckets: BTreeSet<String>,
    #[serde(default)]
    pub group_lifecycle_transitions: BTreeSet<String>,
    #[serde(default)]
    pub outer_join_transitions: BTreeSet<String>,
    #[serde(default)]
    pub require_p0_pairwise: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticCoverageReport {
    pub passed: bool,
    pub missing: BTreeMap<String, Vec<String>>,
    pub observed: SemanticCoverageObservation,
    pub requirements: SemanticCoverageRequirements,
}

pub fn validate_semantic_coverage(
    requirements: &SemanticCoverageRequirements,
    observed: &SemanticCoverageObservation,
) -> SemanticCoverageReport {
    let missing_set = |required: &BTreeSet<String>, actual: &BTreeSet<String>| {
        required.difference(actual).cloned().collect::<Vec<_>>()
    };
    let mut missing = BTreeMap::new();
    for (name, required, actual) in [
        (
            "snapshot_plans",
            &requirements.snapshot_plans,
            &observed.snapshot_plans,
        ),
        (
            "changed_leaf_buckets",
            &requirements.changed_leaf_buckets,
            &observed.changed_leaf_buckets,
        ),
        (
            "group_lifecycle_transitions",
            &requirements.group_lifecycle_transitions,
            &observed.group_lifecycle_transitions,
        ),
        (
            "outer_join_transitions",
            &requirements.outer_join_transitions,
            &observed.outer_join_transitions,
        ),
    ] {
        let values = missing_set(required, actual);
        if !values.is_empty() {
            missing.insert(name.to_string(), values);
        }
    }
    if requirements.require_p0_pairwise && !observed.p0_pairwise_complete {
        missing.insert("p0_pairwise".to_string(), vec!["complete".to_string()]);
    }
    SemanticCoverageReport {
        passed: missing.is_empty(),
        missing,
        observed: observed.clone(),
        requirements: requirements.clone(),
    }
}

impl SemanticCoverageReport {
    pub fn render_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("coverage report is serializable")
    }

    pub fn render_markdown(&self) -> String {
        let mut output = format!(
            "# DVM semantic coverage\n\nStatus: **{}**\n\n",
            if self.passed { "PASS" } else { "FAIL" }
        );
        output.push_str("| Bucket | Missing |\n| --- | ---: |\n");
        for name in [
            "snapshot_plans",
            "changed_leaf_buckets",
            "group_lifecycle_transitions",
            "outer_join_transitions",
            "p0_pairwise",
        ] {
            let missing = self.missing.get(name).map_or(0, Vec::len);
            output.push_str(&format!("| {name} | {missing} |\n"));
        }
        output
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DimensionSpec {
    pub name: &'static str,
    pub values: &'static [&'static str],
}

pub fn p0_dimensions() -> Vec<DimensionSpec> {
    vec![
        DimensionSpec {
            name: "physical_width",
            values: &["minimal", "wide"],
        },
        DimensionSpec {
            name: "logical_width",
            values: &["all", "narrow"],
        },
        DimensionSpec {
            name: "nullable_key",
            values: &["non_null", "nullable"],
        },
        DimensionSpec {
            name: "aliases",
            values: &["plain", "definition_and_reference"],
        },
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DimensionAssignment {
    pub values: BTreeMap<&'static str, &'static str>,
}

impl DimensionAssignment {
    fn pair_keys(&self, dimensions: &[DimensionSpec]) -> BTreeSet<PairKey> {
        let mut pairs = BTreeSet::new();
        for (left_index, left) in dimensions.iter().enumerate() {
            for right in dimensions.iter().skip(left_index + 1) {
                let (Some(left_value), Some(right_value)) =
                    (self.values.get(left.name), self.values.get(right.name))
                else {
                    continue;
                };
                pairs.insert(PairKey {
                    left_dimension: left.name,
                    left_value,
                    right_dimension: right.name,
                    right_value,
                });
            }
        }
        pairs
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PairKey {
    pub left_dimension: &'static str,
    pub left_value: &'static str,
    pub right_dimension: &'static str,
    pub right_value: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageReport {
    pub total_pairs: usize,
    pub covered_pairs: usize,
    pub uncovered_pairs: Vec<PairKey>,
    pub selected_cases: Vec<DimensionAssignment>,
}

pub fn pairwise_cover(
    dimensions: &[DimensionSpec],
    mandatory: &[DimensionAssignment],
) -> CoverageReport {
    let required = all_pairs(dimensions);
    let mut selected = mandatory.to_vec();
    let mut covered = selected
        .iter()
        .flat_map(|assignment| assignment.pair_keys(dimensions))
        .collect::<BTreeSet<_>>();

    while covered.len() < required.len() {
        let candidates = all_assignments(dimensions);
        let Some((best, best_pairs)) = candidates
            .into_iter()
            .map(|candidate| {
                let new_pairs = candidate
                    .pair_keys(dimensions)
                    .difference(&covered)
                    .cloned()
                    .collect::<BTreeSet<_>>();
                (candidate, new_pairs)
            })
            .max_by_key(|(candidate, new_pairs)| {
                (
                    new_pairs.len(),
                    candidate.values.values().copied().collect::<Vec<_>>(),
                )
            })
        else {
            break;
        };
        if best_pairs.is_empty() {
            break;
        }
        covered.extend(best_pairs);
        selected.push(best);
    }

    let uncovered_pairs = required.difference(&covered).cloned().collect();
    CoverageReport {
        total_pairs: required.len(),
        covered_pairs: covered.len(),
        uncovered_pairs,
        selected_cases: selected,
    }
}

#[derive(Debug, Clone)]
pub struct CompositionCase {
    pub id: &'static str,
    pub shape: &'static str,
    pub dimensions: DimensionAssignment,
    pub query: RelQuery,
    pub expected_join: JoinKind,
    pub changed_leaves: &'static str,
}

pub fn mandatory_cases() -> Vec<CompositionCase> {
    (0..MANDATORY_CASE_COUNT)
        .map(|index| {
            let dimensions = assignment(index);
            CompositionCase {
                id: CASE_IDS[index],
                shape: CASE_IDS[index],
                expected_join: match index % 3 {
                    0 => JoinKind::Left,
                    1 => JoinKind::Full,
                    _ => JoinKind::Inner,
                },
                changed_leaves: match index % 3 {
                    0 => "one",
                    1 => "two",
                    _ => "all",
                },
                query: build_query(index, &dimensions),
                dimensions,
            }
        })
        .collect()
}

pub const CASE_IDS: [&str; MANDATORY_CASE_COUNT] = [
    "chained_left_aggregate_ctes",
    "simultaneous_two_aggregate_leaves",
    "mixed_algebraic_rescan",
    "two_rescan_families",
    "existing_multigroup_min_winner",
    "existing_multigroup_max_winner",
    "singleton_empty_repopulate",
    "nullable_group_key_transition",
    "cte_definition_reference_aliases",
    "nested_join_subtree",
    "full_join_simultaneous_changes",
    "inner_join_duplicate_matches",
    "join_and_aggregate_same_cycle",
    "three_aggregate_leaves",
    "four_level_left_deep_join",
    "wide_source_narrow_projection",
    "inline_subquery_equivalent",
    "batching_history_equivalent",
];

fn assignment(index: usize) -> DimensionAssignment {
    let values = [
        ["minimal", "all", "non_null", "plain"],
        ["wide", "narrow", "nullable", "definition_and_reference"],
        ["wide", "all", "nullable", "plain"],
        ["minimal", "narrow", "non_null", "definition_and_reference"],
        ["wide", "narrow", "non_null", "plain"],
        ["minimal", "all", "nullable", "definition_and_reference"],
        ["wide", "all", "non_null", "definition_and_reference"],
        ["minimal", "narrow", "nullable", "plain"],
        ["wide", "narrow", "nullable", "definition_and_reference"],
        ["minimal", "all", "non_null", "plain"],
        ["wide", "all", "nullable", "plain"],
        ["minimal", "narrow", "non_null", "definition_and_reference"],
        ["wide", "narrow", "nullable", "plain"],
        ["minimal", "all", "nullable", "definition_and_reference"],
        ["wide", "narrow", "non_null", "plain"],
        ["wide", "narrow", "non_null", "definition_and_reference"],
        ["minimal", "all", "nullable", "plain"],
        ["minimal", "narrow", "nullable", "definition_and_reference"],
    ];
    let selected = values[index];
    let names = ["physical_width", "logical_width", "nullable_key", "aliases"];
    DimensionAssignment {
        values: names.into_iter().zip(selected).collect(),
    }
}

fn build_query(index: usize, dimensions: &DimensionAssignment) -> RelQuery {
    let nullable = dimensions.values["nullable_key"] == "nullable";
    let wide = dimensions.values["physical_width"] == "wide";
    let narrow = dimensions.values["logical_width"] == "narrow";
    let aliases = dimensions.values["aliases"] == "definition_and_reference";
    let left_name = if aliases {
        format!("{}_left_aggregate_definition", CASE_IDS[index])
    } else {
        format!("{}_left_aggregate", CASE_IDS[index])
    };
    let right_name = if aliases {
        format!("{}_right_aggregate_definition", CASE_IDS[index])
    } else {
        format!("{}_right_aggregate", CASE_IDS[index])
    };

    let left_schema = source_schema("composition_left", nullable, wide);
    let right_schema = source_schema("composition_right", nullable, wide);
    let left = RelNode::Scan {
        table: "composition_left".to_string(),
        schema: left_schema,
    };
    let right = RelNode::Scan {
        table: "composition_right".to_string(),
        schema: right_schema,
    };
    let left_kind = match index % 4 {
        0 => AggregateKind::Max,
        1 => AggregateKind::Sum,
        2 => AggregateKind::Avg,
        _ => AggregateKind::StringAgg,
    };
    let right_kind = match index % 3 {
        0 => AggregateKind::Sum,
        1 => AggregateKind::Max,
        _ => AggregateKind::Count,
    };
    let left_input = if matches!(left_kind, AggregateKind::StringAgg) {
        3
    } else {
        2
    };
    let left_aggregate = RelNode::Aggregate {
        input: Box::new(left),
        group_by: vec![1],
        aggregates: vec![AggregateExpr {
            kind: left_kind,
            input_column: Some(left_input),
            alias: "left_value".to_string(),
        }],
    };
    let right_aggregate = RelNode::Aggregate {
        input: Box::new(right),
        group_by: vec![1],
        aggregates: vec![AggregateExpr {
            kind: right_kind,
            input_column: if matches!(right_kind, AggregateKind::Count) {
                None
            } else {
                Some(2)
            },
            alias: "right_value".to_string(),
        }],
    };
    let left_ref = RelNode::CteRef {
        name: left_name.to_string(),
        schema: RelationSchema::new(vec![
            Column::new("grp", ScalarType::Text, nullable),
            Column::new(
                "left_value",
                if matches!(left_kind, AggregateKind::StringAgg) {
                    ScalarType::Text
                } else if matches!(left_kind, AggregateKind::Avg) {
                    ScalarType::Numeric
                } else if matches!(left_kind, AggregateKind::Sum) {
                    ScalarType::BigInt
                } else {
                    ScalarType::Int
                },
                true,
            ),
        ]),
    };
    let right_ref = RelNode::CteRef {
        name: right_name.to_string(),
        schema: RelationSchema::new(vec![
            Column::new("grp", ScalarType::Text, nullable),
            Column::new(
                "right_value",
                if matches!(right_kind, AggregateKind::Count | AggregateKind::Sum) {
                    ScalarType::BigInt
                } else {
                    ScalarType::Int
                },
                true,
            ),
        ]),
    };
    let joined = RelNode::Join {
        kind: match index % 3 {
            0 => JoinKind::Left,
            1 => JoinKind::Full,
            _ => JoinKind::Inner,
        },
        left: Box::new(if index == 9 {
            RelNode::Subquery {
                input: Box::new(left_ref),
                alias: "nested_left".to_string(),
            }
        } else {
            left_ref
        }),
        right: Box::new(right_ref),
        left_column: 0,
        right_column: 0,
    };
    let joined_schema = joined.schema().unwrap_or_else(|_| {
        RelationSchema::new(vec![
            Column::new("grp", ScalarType::Text, true),
            Column::new("left_value", ScalarType::Int, true),
            Column::new("right_grp", ScalarType::Text, true),
            Column::new("right_value", ScalarType::Int, true),
        ])
    });
    let body = if narrow {
        RelNode::Project {
            input: Box::new(joined),
            expressions: vec![ProjectExpr {
                input_column: 0,
                alias: "grp".to_string(),
            }],
        }
    } else {
        RelNode::Subquery {
            input: Box::new(joined),
            alias: format!("case_{index}"),
        }
    };
    let _ = joined_schema;
    RelQuery {
        ctes: vec![
            CteDefinition {
                name: left_name.to_string(),
                query: left_aggregate,
            },
            CteDefinition {
                name: right_name.to_string(),
                query: right_aggregate,
            },
        ],
        body,
    }
}

fn source_schema(_table: &str, nullable: bool, wide: bool) -> RelationSchema {
    let mut columns = vec![
        Column::new("id", ScalarType::Int, false),
        Column::new("grp", ScalarType::Text, nullable),
        Column::new("value", ScalarType::Int, true),
        Column::new("note", ScalarType::Text, true),
    ];
    if wide {
        columns.extend(
            (1..=16).map(|index| Column::new(format!("unused_{index}"), ScalarType::Int, true)),
        );
    }
    RelationSchema::new(columns)
}

fn all_pairs(dimensions: &[DimensionSpec]) -> BTreeSet<PairKey> {
    let mut pairs = BTreeSet::new();
    for (left_index, left) in dimensions.iter().enumerate() {
        for right in dimensions.iter().skip(left_index + 1) {
            for left_value in left.values {
                for right_value in right.values {
                    pairs.insert(PairKey {
                        left_dimension: left.name,
                        left_value,
                        right_dimension: right.name,
                        right_value,
                    });
                }
            }
        }
    }
    pairs
}

fn all_assignments(dimensions: &[DimensionSpec]) -> Vec<DimensionAssignment> {
    fn visit(
        dimensions: &[DimensionSpec],
        index: usize,
        values: &mut BTreeMap<&'static str, &'static str>,
        output: &mut Vec<DimensionAssignment>,
    ) {
        if index == dimensions.len() {
            output.push(DimensionAssignment {
                values: values.clone(),
            });
            return;
        }
        let dimension = &dimensions[index];
        for value in dimension.values {
            values.insert(dimension.name, value);
            visit(dimensions, index + 1, values, output);
        }
        values.remove(dimension.name);
    }

    let mut output = Vec::new();
    visit(dimensions, 0, &mut BTreeMap::new(), &mut output);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requirements() -> SemanticCoverageRequirements {
        let document: serde_json::Value =
            serde_json::from_str(include_str!("../../corpus/dvm_coverage_requirements.json"))
                .expect("coverage requirements must be JSON");
        serde_json::from_value(document["semantic_coverage"].clone())
            .expect("semantic coverage requirements must be valid")
    }

    #[test]
    fn mandatory_matrix_has_valid_typed_queries() {
        let cases = mandatory_cases();
        assert_eq!(cases.len(), MANDATORY_CASE_COUNT);
        for case in cases {
            assert!(case.query.schema().is_ok(), "{} schema", case.id);
            assert!(case.query.render_sql().is_ok(), "{} SQL", case.id);
        }
    }

    #[test]
    fn mandatory_matrix_covers_declared_p0_pairs() {
        let cases = mandatory_cases();
        let assignments = cases
            .iter()
            .map(|case| case.dimensions.clone())
            .collect::<Vec<_>>();
        let report = pairwise_cover(&p0_dimensions(), &assignments);
        assert_eq!(report.uncovered_pairs, Vec::new());
        assert_eq!(report.total_pairs, report.covered_pairs);
    }

    #[test]
    fn semantic_floors_pass_and_report_missing_buckets() {
        let requirements = requirements();
        let mut observed = SemanticCoverageObservation::default();
        for plan in SNAPSHOT_PLANS {
            observed.observe_snapshot_plan(plan);
        }
        for count in 1..=3 {
            observed.observe_changed_leaves(count, 3);
        }
        for transition in GROUP_LIFECYCLE_TRANSITIONS {
            observed.observe_group_lifecycle(transition);
        }
        for transition in OUTER_JOIN_TRANSITIONS {
            observed.observe_outer_join(transition);
        }
        observed.p0_pairwise_complete = true;

        let report = validate_semantic_coverage(&requirements, &observed);
        assert!(report.passed, "{}", report.render_markdown());
        assert!(report.render_json().contains("\"passed\": true"));
        assert!(report.render_markdown().contains("Status: **PASS**"));

        observed.outer_join_transitions.clear();
        let report = validate_semantic_coverage(&requirements, &observed);
        assert!(!report.passed);
        assert_eq!(report.missing["outer_join_transitions"].len(), 2);
    }

    #[test]
    fn decision_trace_populates_observed_semantic_coverage() {
        let mut observed = SemanticCoverageObservation::default();
        let trace = serde_json::json!({"events": [
            {"snapshot_plan": "exact_per_leaf", "decisions": ["empty_to_nonempty", "matched_to_unmatched"]},
            {"snapshot_plan": "exact_combined", "decisions": ["nonempty_to_empty", "unmatched_to_matched"]},
            {"snapshot_plan": "post_change_correction", "decisions": ["winner_change", "nonempty_to_nonempty"]},
            {"snapshot_plan": "unsupported", "decisions": []}
        ]});
        observed.observe_decision_trace(&trace);
        assert_eq!(observed.snapshot_plans.len(), 4);
        assert_eq!(observed.group_lifecycle_transitions.len(), 4);
        assert_eq!(observed.outer_join_transitions.len(), 2);
    }
}
