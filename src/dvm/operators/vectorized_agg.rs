//! Pure admission and plan construction for the MT-8 aggregate executor.

use crate::dvm::parser::{AggFunc, Expr, OpTree};

const BOOLOID: u32 = 16;
const INT8OID: u32 = 20;
const INT2OID: u32 = 21;
const INT4OID: u32 = 23;
const TEXTOID: u32 = 25;
const NAMEOID: u32 = 19;
const BPCHAROID: u32 = 1042;
const VARCHAROID: u32 = 1043;
const DATEOID: u32 = 1082;
const TIMESTAMPOID: u32 = 1114;
const TIMESTAMPTZOID: u32 = 1184;
const NUMERICOID: u32 = 1700;

/// Stable reason why MT-8 retained the SQL aggregate producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorAggregateFallback {
    NotSingleScanAggregate,
    JoinOrSubquery,
    UnsupportedProjection,
    DistinctAggregate,
    AggregateFilter,
    UnsupportedFunction,
    UnsupportedInputType,
    CollationSensitiveKey,
    GroupRescanRequired,
    ImmediateMode,
    PartitionOrTriggerApplyConstraint,
    StatisticsOrTypeMetadataUnavailable,
}

impl VectorAggregateFallback {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotSingleScanAggregate => "not_single_scan_aggregate",
            Self::JoinOrSubquery => "join_or_subquery",
            Self::UnsupportedProjection => "unsupported_projection",
            Self::DistinctAggregate => "distinct_aggregate",
            Self::AggregateFilter => "aggregate_filter",
            Self::UnsupportedFunction => "unsupported_function",
            Self::UnsupportedInputType => "unsupported_input_type",
            Self::CollationSensitiveKey => "collation_sensitive_key",
            Self::GroupRescanRequired => "group_rescan_required",
            Self::ImmediateMode => "immediate_mode",
            Self::PartitionOrTriggerApplyConstraint => "partition_or_trigger_apply_constraint",
            Self::StatisticsOrTypeMetadataUnavailable => "statistics_or_type_metadata_unavailable",
        }
    }
}

/// Fixed-width PostgreSQL values that MT-8 can copy out of an SPI page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorType {
    Bool,
    Int2,
    Int4,
    Int8,
    Date,
    Timestamp,
    TimestampTz,
}

impl VectorType {
    pub const fn oid(self) -> u32 {
        match self {
            Self::Bool => BOOLOID,
            Self::Int2 => INT2OID,
            Self::Int4 => INT4OID,
            Self::Int8 => INT8OID,
            Self::Date => DATEOID,
            Self::Timestamp => TIMESTAMPOID,
            Self::TimestampTz => TIMESTAMPTZOID,
        }
    }

    fn from_oid(oid: u32) -> Option<Self> {
        Some(match oid {
            BOOLOID => Self::Bool,
            INT2OID => Self::Int2,
            INT4OID => Self::Int4,
            INT8OID => Self::Int8,
            DATEOID => Self::Date,
            TIMESTAMPOID => Self::Timestamp,
            TIMESTAMPTZOID => Self::TimestampTz,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorColumnPlan {
    pub name: String,
    pub value_type: VectorType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorAggregateFunction {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorAggregateExprPlan {
    pub function: VectorAggregateFunction,
    pub input: Option<VectorColumnPlan>,
    pub result_type_oid: u32,
    pub output_alias: String,
    pub auxiliary_columns: Vec<String>,
}

/// Everything the refresh layer needs after pure admission succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorAggregatePlan {
    pub source_oid: u32,
    pub source_schema: String,
    pub source_table: String,
    pub change_buffer: String,
    pub group_keys: Vec<VectorColumnPlan>,
    pub aggregates: Vec<VectorAggregateExprPlan>,
    pub target_output_order: Vec<String>,
    pub frontier_placeholders: (String, String),
    /// Source output indexes in target order when a transparent project exists.
    pub output_projection: Option<Vec<usize>>,
}

/// Runtime facts deliberately kept out of the pure operator tree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorAggregateAdmission {
    pub immediate_mode: bool,
    pub constrained_apply: bool,
}

pub struct VectorizedAggregateOperator;

impl VectorizedAggregateOperator {
    pub fn plan(
        tree: &OpTree,
        change_buffer: Option<&str>,
        frontier_placeholders: Option<(&str, &str)>,
        admission: VectorAggregateAdmission,
    ) -> Result<VectorAggregatePlan, VectorAggregateFallback> {
        if admission.immediate_mode {
            return Err(VectorAggregateFallback::ImmediateMode);
        }
        if admission.constrained_apply {
            return Err(VectorAggregateFallback::PartitionOrTriggerApplyConstraint);
        }

        let (aggregate, projection) = match tree {
            OpTree::Aggregate { .. } => (tree, None),
            OpTree::Project {
                expressions,
                aliases,
                child,
            } if matches!(child.as_ref(), OpTree::Aggregate { .. }) => (
                child.as_ref(),
                Some((expressions.as_slice(), aliases.as_slice())),
            ),
            _ if contains_join_or_subquery(tree) => {
                return Err(VectorAggregateFallback::JoinOrSubquery);
            }
            _ => return Err(VectorAggregateFallback::NotSingleScanAggregate),
        };

        let OpTree::Aggregate {
            group_by,
            aggregates,
            child,
        } = aggregate
        else {
            return Err(VectorAggregateFallback::NotSingleScanAggregate);
        };
        let OpTree::Scan {
            table_oid,
            schema,
            table_name,
            columns,
            ..
        } = child.as_ref()
        else {
            return Err(if contains_join_or_subquery(child) {
                VectorAggregateFallback::JoinOrSubquery
            } else {
                VectorAggregateFallback::NotSingleScanAggregate
            });
        };

        let change_buffer = change_buffer
            .filter(|name| !name.is_empty())
            .ok_or(VectorAggregateFallback::StatisticsOrTypeMetadataUnavailable)?;
        let (prev, next) = frontier_placeholders
            .filter(|(prev, next)| !prev.is_empty() && !next.is_empty())
            .ok_or(VectorAggregateFallback::StatisticsOrTypeMetadataUnavailable)?;

        let mut group_keys = Vec::with_capacity(group_by.len());
        for expression in group_by {
            let name =
                column_name(expression).ok_or(VectorAggregateFallback::NotSingleScanAggregate)?;
            let column = columns
                .iter()
                .find(|column| column.name == name)
                .ok_or(VectorAggregateFallback::StatisticsOrTypeMetadataUnavailable)?;
            if matches!(column.type_oid, TEXTOID | NAMEOID | BPCHAROID | VARCHAROID) {
                return Err(VectorAggregateFallback::CollationSensitiveKey);
            }
            let value_type = VectorType::from_oid(column.type_oid)
                .ok_or(VectorAggregateFallback::UnsupportedInputType)?;
            group_keys.push(VectorColumnPlan {
                name: column.name.clone(),
                value_type,
                nullable: column.is_nullable,
            });
        }

        let mut aggregate_plans = Vec::with_capacity(aggregates.len());
        for aggregate in aggregates {
            if aggregate.is_distinct {
                return Err(VectorAggregateFallback::DistinctAggregate);
            }
            if aggregate.filter.is_some() {
                return Err(VectorAggregateFallback::AggregateFilter);
            }
            if aggregate.function.is_group_rescan() {
                return Err(VectorAggregateFallback::GroupRescanRequired);
            }

            let function = match aggregate.function {
                AggFunc::CountStar => VectorAggregateFunction::CountStar,
                AggFunc::Count => VectorAggregateFunction::Count,
                AggFunc::Sum => VectorAggregateFunction::Sum,
                AggFunc::Avg => VectorAggregateFunction::Avg,
                AggFunc::Min => VectorAggregateFunction::Min,
                AggFunc::Max => VectorAggregateFunction::Max,
                _ => return Err(VectorAggregateFallback::UnsupportedFunction),
            };
            let input = match function {
                VectorAggregateFunction::CountStar => {
                    if aggregate.argument.is_some() {
                        return Err(VectorAggregateFallback::UnsupportedFunction);
                    }
                    None
                }
                _ => {
                    let name = aggregate
                        .argument
                        .as_ref()
                        .and_then(column_name)
                        .ok_or(VectorAggregateFallback::UnsupportedInputType)?;
                    let column = columns
                        .iter()
                        .find(|column| column.name == name)
                        .ok_or(VectorAggregateFallback::StatisticsOrTypeMetadataUnavailable)?;
                    let value_type = VectorType::from_oid(column.type_oid)
                        .ok_or(VectorAggregateFallback::UnsupportedInputType)?;
                    Some(VectorColumnPlan {
                        name: column.name.clone(),
                        value_type,
                        nullable: column.is_nullable,
                    })
                }
            };
            let input_type = input.as_ref().map(|column| column.value_type);
            let supported = match function {
                VectorAggregateFunction::CountStar | VectorAggregateFunction::Count => true,
                VectorAggregateFunction::Sum | VectorAggregateFunction::Avg => {
                    matches!(input_type, Some(VectorType::Int2 | VectorType::Int4))
                }
                VectorAggregateFunction::Min | VectorAggregateFunction::Max => matches!(
                    input_type,
                    Some(
                        VectorType::Int2
                            | VectorType::Int4
                            | VectorType::Int8
                            | VectorType::Date
                            | VectorType::Timestamp
                            | VectorType::TimestampTz
                    )
                ),
            };
            if !supported {
                return Err(VectorAggregateFallback::UnsupportedInputType);
            }
            let result_type_oid = match function {
                VectorAggregateFunction::CountStar
                | VectorAggregateFunction::Count
                | VectorAggregateFunction::Sum => INT8OID,
                VectorAggregateFunction::Avg => NUMERICOID,
                VectorAggregateFunction::Min | VectorAggregateFunction::Max => input
                    .as_ref()
                    .map(|column| column.value_type.oid())
                    .ok_or(VectorAggregateFallback::UnsupportedInputType)?,
            };
            let auxiliary_columns = match function {
                VectorAggregateFunction::Sum => {
                    vec![format!("__pgt_aux_nonnull_{}", aggregate.alias)]
                }
                VectorAggregateFunction::Avg => vec![
                    format!("__pgt_aux_sum_{}", aggregate.alias),
                    format!("__pgt_aux_count_{}", aggregate.alias),
                ],
                _ => Vec::new(),
            };
            aggregate_plans.push(VectorAggregateExprPlan {
                function,
                input,
                result_type_oid,
                output_alias: aggregate.alias.clone(),
                auxiliary_columns,
            });
        }

        let aggregate_output = group_keys
            .iter()
            .map(|column| column.name.clone())
            .chain(
                aggregate_plans
                    .iter()
                    .map(|aggregate| aggregate.output_alias.clone()),
            )
            .collect::<Vec<_>>();
        let (target_output_order, output_projection) = match projection {
            None => (aggregate_output, None),
            Some((expressions, aliases)) => {
                if expressions.len() != aggregate_output.len()
                    || aliases.len() != aggregate_output.len()
                {
                    return Err(VectorAggregateFallback::UnsupportedProjection);
                }
                let mut indexes = Vec::with_capacity(expressions.len());
                for expression in expressions {
                    let name = column_name(expression)
                        .ok_or(VectorAggregateFallback::UnsupportedProjection)?;
                    let index = aggregate_output
                        .iter()
                        .position(|column| column == name)
                        .ok_or(VectorAggregateFallback::UnsupportedProjection)?;
                    if indexes.contains(&index) {
                        return Err(VectorAggregateFallback::UnsupportedProjection);
                    }
                    indexes.push(index);
                }
                let output_projection = (indexes.iter().copied().ne(0..indexes.len())
                    || aliases != aggregate_output)
                    .then_some(indexes);
                (aliases.to_vec(), output_projection)
            }
        };

        Ok(VectorAggregatePlan {
            source_oid: *table_oid,
            source_schema: schema.clone(),
            source_table: table_name.clone(),
            change_buffer: change_buffer.to_string(),
            group_keys,
            aggregates: aggregate_plans,
            target_output_order,
            frontier_placeholders: (prev.to_string(), next.to_string()),
            output_projection,
        })
    }
}

fn column_name(expression: &Expr) -> Option<&str> {
    match expression {
        Expr::ColumnRef { column_name, .. } => Some(column_name),
        _ => None,
    }
}

fn contains_join_or_subquery(tree: &OpTree) -> bool {
    match tree {
        OpTree::InnerJoin { .. }
        | OpTree::LeftJoin { .. }
        | OpTree::FullJoin { .. }
        | OpTree::SemiJoin { .. }
        | OpTree::AntiJoin { .. }
        | OpTree::Subquery { .. }
        | OpTree::ScalarSubquery { .. }
        | OpTree::LateralSubquery { .. } => true,
        OpTree::Project { child, .. }
        | OpTree::Filter { child, .. }
        | OpTree::Aggregate { child, .. }
        | OpTree::Distinct { child }
        | OpTree::Window { child, .. } => contains_join_or_subquery(child),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::parser::{AggExpr, Column};

    fn scan() -> OpTree {
        OpTree::Scan {
            table_oid: 42,
            table_name: "events".into(),
            schema: "public".into(),
            columns: vec![
                Column {
                    name: "account_id".into(),
                    type_oid: INT4OID,
                    is_nullable: false,
                },
                Column {
                    name: "amount".into(),
                    type_oid: INT4OID,
                    is_nullable: true,
                },
            ],
            pk_columns: vec!["account_id".into()],
            alias: "e".into(),
        }
    }

    fn aggregate(function: AggFunc, argument: Option<&str>, alias: &str) -> AggExpr {
        AggExpr {
            function,
            argument: argument.map(|column_name| Expr::ColumnRef {
                table_alias: Some("e".into()),
                column_name: column_name.into(),
            }),
            alias: alias.into(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        }
    }

    fn plan(tree: &OpTree) -> Result<VectorAggregatePlan, VectorAggregateFallback> {
        VectorizedAggregateOperator::plan(
            tree,
            Some("pgtrickle_changes.changes_42"),
            Some(("__PREV__", "__NEXT__")),
            VectorAggregateAdmission::default(),
        )
    }

    #[test]
    fn admits_fixed_width_grouped_algebraic_aggregates() {
        let tree = OpTree::Aggregate {
            group_by: vec![Expr::ColumnRef {
                table_alias: Some("e".into()),
                column_name: "account_id".into(),
            }],
            aggregates: vec![
                aggregate(AggFunc::CountStar, None, "rows"),
                aggregate(AggFunc::Sum, Some("amount"), "total"),
                aggregate(AggFunc::Avg, Some("amount"), "mean"),
            ],
            child: Box::new(scan()),
        };
        let plan = plan(&tree).expect("eligible plan");
        assert_eq!(plan.source_oid, 42);
        assert_eq!(plan.group_keys[0].value_type, VectorType::Int4);
        assert_eq!(plan.aggregates[1].result_type_oid, INT8OID);
        assert_eq!(plan.aggregates[2].result_type_oid, NUMERICOID);
        assert_eq!(
            plan.aggregates[1].auxiliary_columns,
            ["__pgt_aux_nonnull_total"]
        );
    }

    #[test]
    fn transparent_project_may_only_rename_or_reorder_all_outputs() {
        let aggregate = OpTree::Aggregate {
            group_by: vec![Expr::ColumnRef {
                table_alias: None,
                column_name: "account_id".into(),
            }],
            aggregates: vec![aggregate(AggFunc::CountStar, None, "rows")],
            child: Box::new(scan()),
        };
        let tree = OpTree::Project {
            expressions: vec![
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "rows".into(),
                },
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "account_id".into(),
                },
            ],
            aliases: vec!["n".into(), "account".into()],
            child: Box::new(aggregate),
        };
        let plan = plan(&tree).expect("rename and reorder only");
        assert_eq!(plan.target_output_order, ["n", "account"]);
        assert_eq!(plan.output_projection, Some(vec![1, 0]));
    }

    #[test]
    fn transparent_identity_project_needs_no_runtime_projection() {
        let aggregate = OpTree::Aggregate {
            group_by: Vec::new(),
            aggregates: vec![aggregate(AggFunc::CountStar, None, "rows")],
            child: Box::new(scan()),
        };
        let tree = OpTree::Project {
            expressions: vec![Expr::ColumnRef {
                table_alias: None,
                column_name: "rows".into(),
            }],
            aliases: vec!["rows".into()],
            child: Box::new(aggregate),
        };
        assert_eq!(
            plan(&tree).expect("identity project").output_projection,
            None
        );
    }

    #[test]
    fn fails_closed_with_stable_reasons() {
        let mut distinct = aggregate(AggFunc::Sum, Some("amount"), "total");
        distinct.is_distinct = true;
        let tree = OpTree::Aggregate {
            group_by: Vec::new(),
            aggregates: vec![distinct],
            child: Box::new(scan()),
        };
        assert_eq!(plan(&tree), Err(VectorAggregateFallback::DistinctAggregate));
        assert_eq!(
            VectorAggregateFallback::DistinctAggregate.as_str(),
            "distinct_aggregate"
        );
        assert_eq!(
            VectorizedAggregateOperator::plan(
                &tree,
                Some("buffer"),
                Some(("p", "n")),
                VectorAggregateAdmission {
                    immediate_mode: true,
                    constrained_apply: false,
                },
            ),
            Err(VectorAggregateFallback::ImmediateMode)
        );
    }
}
