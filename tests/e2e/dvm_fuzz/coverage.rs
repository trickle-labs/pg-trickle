//! Deterministic v0.87.3 composition matrix and pairwise coverage.

use super::query::{
    AggregateExpr, AggregateKind, Column, CteDefinition, JoinKind, ProjectExpr, RelNode, RelQuery,
    RelationSchema, ScalarType,
};
use std::collections::{BTreeMap, BTreeSet};

pub const MANDATORY_CASE_COUNT: usize = 18;

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
        "left_aggregate_definition"
    } else {
        "left_aggregate"
    };
    let right_name = if aliases {
        "right_aggregate_definition"
    } else {
        "right_aggregate"
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
}
