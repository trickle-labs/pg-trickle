//! Row ID generation strategies for different operators.
//!
//! Row ID computation depends on the operator that produces the row.
//! See `PLAN.md` Phase 6.2 for the full strategy table.

use crate::dvm::parser::OpTree;

/// Strategies for computing row IDs at each operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowIdStrategy {
    /// Use the primary key columns of the source table.
    PrimaryKey { pk_columns: Vec<String> },
    /// Hash all columns (fallback when no PK is available).
    AllColumns { columns: Vec<String> },
    /// Combine two child row IDs (for joins).
    CombineChildren,
    /// Hash the group-by columns (for aggregates).
    GroupByKey { group_columns: Vec<String> },
    /// Pass through the child's row ID (for project/filter).
    PassThrough,
    /// Use an externally-provided stable row ID column.
    ExternalStableId { id_column: String },
}

/// A18 (v0.36.0): Formal schema declaration for row-id hash computation.
///
/// Every DVM operator declares its `RowIdSchema` as part of its type signature.
/// A plan-time verifier asserts that adjacent operators in the same pipeline
/// have compatible schemas — ensuring that row-id hashes are semantically
/// consistent across operator boundaries (the root cause of EC-01 class bugs).
///
/// # Compatibility Rules
/// Two schemas are compatible when:
/// - They are the same variant, or
/// - The downstream schema is `Any` (accepts any upstream schema), or
/// - The downstream schema is `Derived` (computes its own hash from scratch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowIdSchema {
    /// Row ID is derived from PRIMARY KEY columns of a specific table (the hash
    /// uses only the PK columns in table-definition order).
    PrimaryKey {
        /// Qualified table name (schema.table) for disambiguation.
        table: String,
        /// Column names in PK definition order.
        columns: Vec<String>,
    },
    /// Row ID is derived from ALL visible columns of the output tuple.
    AllColumns {
        /// Ordered list of column names included in the hash.
        columns: Vec<String>,
    },
    /// Row ID is derived from GROUP BY key columns.
    GroupByKey {
        /// GROUP BY column names in query order.
        columns: Vec<String>,
    },
    /// Row ID is computed from the hashes of two child operators (JOIN).
    Combined {
        /// Schema of the left (probe) child.
        left: Box<RowIdSchema>,
        /// Schema of the right (build) child.
        right: Box<RowIdSchema>,
    },
    /// Row ID is passed through unchanged from the single child operator.
    PassThrough,
    /// This operator accepts any upstream `RowIdSchema` without re-hashing.
    Any,
    /// This operator recomputes the row-id hash from scratch (e.g. TopK).
    Derived,
}

impl RowIdSchema {
    /// Returns `true` when `self` (downstream) is compatible with `upstream`.
    ///
    /// Compatibility is required before operators are composed in a pipeline.
    /// An incompatible pair means the downstream operator would compute a
    /// different row-id hash than the upstream emitted, leading to silent
    /// missed-update bugs.
    pub fn is_compatible_with(&self, upstream: &RowIdSchema) -> bool {
        match self {
            // Any and Derived accept everything
            RowIdSchema::Any | RowIdSchema::Derived => true,
            // PassThrough propagates the upstream schema unchanged.
            RowIdSchema::PassThrough => true,
            // PrimaryKey schemas must agree on table + column set
            RowIdSchema::PrimaryKey { table, columns } => {
                matches!(upstream, RowIdSchema::PrimaryKey { table: t, columns: c }
                    if t == table && c == columns)
            }
            // AllColumns schemas must agree on column set
            RowIdSchema::AllColumns { columns } => {
                matches!(upstream, RowIdSchema::AllColumns { columns: c } if c == columns)
            }
            // GroupByKey schemas must agree on columns
            RowIdSchema::GroupByKey { columns } => {
                matches!(upstream, RowIdSchema::GroupByKey { columns: c } if c == columns)
            }
            // Combined schemas check recursively
            RowIdSchema::Combined { left, right } => {
                matches!(upstream, RowIdSchema::Combined { left: l, right: r }
                    if left.is_compatible_with(l) && right.is_compatible_with(r))
            }
        }
    }

    /// Validate that the given sequence of operator schemas forms a compatible
    /// pipeline (each adjacent pair passes `is_compatible_with`).
    ///
    /// Returns `Ok(())` when all pairs are compatible, or `Err(String)` with a
    /// description of the first incompatible pair found.
    pub fn verify_pipeline(schemas: &[RowIdSchema]) -> Result<(), String> {
        for pair in schemas.windows(2) {
            let upstream = &pair[0];
            let downstream = &pair[1];
            if !downstream.is_compatible_with(upstream) {
                return Err(format!(
                    "RowIdSchema mismatch: operator with schema {:?} \
                     cannot follow operator with schema {:?}",
                    downstream, upstream
                ));
            }
        }
        Ok(())
    }
}

/// Infer and verify the row-id schema produced by a parsed DVM plan.
///
/// This is a lightweight plan-time guardrail for EC-01 class bugs: every
/// operator declares the row-id shape it emits, transparent operators are
/// checked as pass-through, and join operators compose the schemas from both
/// children. The returned schema is primarily diagnostic; an error means the
/// plan contains an internally inconsistent row-id pipeline and should not be
/// maintained in DIFFERENTIAL mode.
pub fn verify_plan_row_id_schema(op: &OpTree) -> Result<RowIdSchema, String> {
    infer_plan_row_id_schema(op)
}

fn infer_plan_row_id_schema(op: &OpTree) -> Result<RowIdSchema, String> {
    match op {
        OpTree::Scan {
            schema,
            table_name,
            columns,
            pk_columns,
            ..
        } => {
            if pk_columns.is_empty() {
                Ok(RowIdSchema::AllColumns {
                    columns: columns.iter().map(|c| c.name.clone()).collect(),
                })
            } else {
                Ok(RowIdSchema::PrimaryKey {
                    table: format!("{schema}.{table_name}"),
                    columns: pk_columns.clone(),
                })
            }
        }
        OpTree::Filter { child, .. } | OpTree::Subquery { child, .. } => {
            let child_schema = infer_plan_row_id_schema(child)?;
            RowIdSchema::verify_pipeline(&[child_schema.clone(), RowIdSchema::PassThrough])?;
            Ok(child_schema)
        }
        OpTree::Project { child, .. } => {
            let child_schema = infer_plan_row_id_schema(child)?;
            if let Some(key_columns) = op.row_id_key_columns() {
                if key_columns == child.output_columns() {
                    RowIdSchema::verify_pipeline(&[
                        child_schema.clone(),
                        RowIdSchema::PassThrough,
                    ])?;
                    Ok(child_schema)
                } else {
                    Ok(RowIdSchema::Derived)
                }
            } else {
                Ok(RowIdSchema::Derived)
            }
        }
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => Ok(RowIdSchema::Combined {
            left: Box::new(infer_plan_row_id_schema(left)?),
            right: Box::new(infer_plan_row_id_schema(right)?),
        }),
        OpTree::SemiJoin { left, right, .. } | OpTree::AntiJoin { left, right, .. } => {
            let left_schema = infer_plan_row_id_schema(left)?;
            let _right_schema = infer_plan_row_id_schema(right)?;
            RowIdSchema::verify_pipeline(&[left_schema.clone(), RowIdSchema::PassThrough])?;
            Ok(left_schema)
        }
        OpTree::Aggregate {
            group_by, child, ..
        } => {
            let _child_schema = infer_plan_row_id_schema(child)?;
            let columns: Vec<String> = group_by.iter().map(|expr| expr.output_name()).collect();
            if columns.is_empty() {
                Ok(RowIdSchema::Derived)
            } else {
                Ok(RowIdSchema::GroupByKey { columns })
            }
        }
        OpTree::Distinct { child } => {
            let _child_schema = infer_plan_row_id_schema(child)?;
            Ok(RowIdSchema::AllColumns {
                columns: child.output_columns(),
            })
        }
        OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
            let _left_schema = infer_plan_row_id_schema(left)?;
            let _right_schema = infer_plan_row_id_schema(right)?;
            Ok(RowIdSchema::AllColumns {
                columns: left.output_columns(),
            })
        }
        OpTree::UnionAll { children } => {
            for child in children {
                let _child_schema = infer_plan_row_id_schema(child)?;
            }
            Ok(RowIdSchema::Derived)
        }
        OpTree::CteScan { body, columns, .. } => {
            if let Some(body) = body {
                let _body_schema = infer_plan_row_id_schema(body)?;
            }
            Ok(RowIdSchema::AllColumns {
                columns: columns.clone(),
            })
        }
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            let _base_schema = infer_plan_row_id_schema(base)?;
            let _recursive_schema = infer_plan_row_id_schema(recursive)?;
            Ok(RowIdSchema::Derived)
        }
        OpTree::RecursiveSelfRef { columns, .. } | OpTree::ConstantSelect { columns, .. } => {
            Ok(RowIdSchema::AllColumns {
                columns: columns.clone(),
            })
        }
        OpTree::Window { child, .. }
        | OpTree::LateralFunction { child, .. }
        | OpTree::LateralSubquery { child, .. } => {
            let _child_schema = infer_plan_row_id_schema(child)?;
            Ok(RowIdSchema::Derived)
        }
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => {
            let _subquery_schema = infer_plan_row_id_schema(subquery)?;
            let _child_schema = infer_plan_row_id_schema(child)?;
            Ok(RowIdSchema::Derived)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RowIdSchema, RowIdStrategy};

    #[test]
    fn test_row_id_strategy_debug_includes_variant_and_columns() {
        let strategy = RowIdStrategy::PrimaryKey {
            pk_columns: vec!["tenant_id".to_string(), "order_id".to_string()],
        };

        let debug = format!("{strategy:?}");

        assert!(debug.contains("PrimaryKey"));
        assert!(debug.contains("tenant_id"));
        assert!(debug.contains("order_id"));
    }

    #[test]
    fn test_row_id_strategy_clone_preserves_group_columns() {
        let original = RowIdStrategy::GroupByKey {
            group_columns: vec!["region".to_string(), "year".to_string()],
        };

        assert_eq!(original.clone(), original);
    }

    #[test]
    fn test_row_id_strategy_all_columns_preserves_column_order() {
        let strategy = RowIdStrategy::AllColumns {
            columns: vec![
                "id".to_string(),
                "created_at".to_string(),
                "payload".to_string(),
            ],
        };

        assert_eq!(
            strategy,
            RowIdStrategy::AllColumns {
                columns: vec![
                    "id".to_string(),
                    "created_at".to_string(),
                    "payload".to_string(),
                ],
            }
        );
    }

    #[test]
    fn test_row_id_strategy_unit_variants_compare_by_variant() {
        assert_eq!(
            RowIdStrategy::CombineChildren,
            RowIdStrategy::CombineChildren
        );
        assert_eq!(RowIdStrategy::PassThrough, RowIdStrategy::PassThrough);
    }

    // ── P3: cross-variant inequality, debug coverage, edge cases ──────────

    #[test]
    fn test_row_id_strategy_variants_are_not_equal_to_each_other() {
        assert_ne!(RowIdStrategy::CombineChildren, RowIdStrategy::PassThrough);
        assert_ne!(
            RowIdStrategy::PrimaryKey { pk_columns: vec![] },
            RowIdStrategy::AllColumns { columns: vec![] },
        );
        assert_ne!(
            RowIdStrategy::PrimaryKey {
                pk_columns: vec!["id".to_string()]
            },
            RowIdStrategy::GroupByKey {
                group_columns: vec!["id".to_string()]
            },
        );
    }

    #[test]
    fn test_row_id_strategy_debug_unit_variants() {
        assert!(format!("{:?}", RowIdStrategy::CombineChildren).contains("CombineChildren"));
        assert!(format!("{:?}", RowIdStrategy::PassThrough).contains("PassThrough"));
    }

    #[test]
    fn test_row_id_strategy_empty_columns_accepted() {
        let pk = RowIdStrategy::PrimaryKey { pk_columns: vec![] };
        let all = RowIdStrategy::AllColumns { columns: vec![] };
        let grp = RowIdStrategy::GroupByKey {
            group_columns: vec![],
        };

        assert_eq!(pk, RowIdStrategy::PrimaryKey { pk_columns: vec![] });
        assert_eq!(all, RowIdStrategy::AllColumns { columns: vec![] });
        assert_eq!(
            grp,
            RowIdStrategy::GroupByKey {
                group_columns: vec![]
            }
        );
    }

    #[test]
    fn test_row_id_strategy_primary_key_clone_is_equal() {
        let original = RowIdStrategy::PrimaryKey {
            pk_columns: vec!["id".to_string(), "tenant".to_string()],
        };
        assert_eq!(original.clone(), original);
    }

    #[test]
    fn test_row_id_strategy_all_columns_clone_is_equal() {
        let original = RowIdStrategy::AllColumns {
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };
        assert_eq!(original.clone(), original);
    }

    // ── A18 (v0.36.0): RowIdSchema tests ─────────────────────────────────

    #[test]
    fn test_row_id_schema_any_compatible_with_everything() {
        let any = RowIdSchema::Any;
        assert!(any.is_compatible_with(&RowIdSchema::PassThrough));
        assert!(any.is_compatible_with(&RowIdSchema::Derived));
        assert!(any.is_compatible_with(&RowIdSchema::GroupByKey {
            columns: vec!["x".to_string()]
        }));
    }

    #[test]
    fn test_row_id_schema_derived_compatible_with_everything() {
        let derived = RowIdSchema::Derived;
        assert!(derived.is_compatible_with(&RowIdSchema::Any));
        assert!(derived.is_compatible_with(&RowIdSchema::PrimaryKey {
            table: "t".to_string(),
            columns: vec!["id".to_string()],
        }));
    }

    #[test]
    fn test_row_id_schema_primary_key_compatible_with_same() {
        let pk = RowIdSchema::PrimaryKey {
            table: "public.orders".to_string(),
            columns: vec!["id".to_string()],
        };
        assert!(pk.is_compatible_with(&RowIdSchema::PrimaryKey {
            table: "public.orders".to_string(),
            columns: vec!["id".to_string()],
        }));
    }

    #[test]
    fn test_row_id_schema_primary_key_incompatible_different_table() {
        let pk = RowIdSchema::PrimaryKey {
            table: "public.orders".to_string(),
            columns: vec!["id".to_string()],
        };
        assert!(!pk.is_compatible_with(&RowIdSchema::PrimaryKey {
            table: "public.items".to_string(),
            columns: vec!["id".to_string()],
        }));
    }

    #[test]
    fn test_row_id_schema_verify_pipeline_happy_path() {
        let schemas = vec![
            RowIdSchema::PrimaryKey {
                table: "t".to_string(),
                columns: vec!["id".to_string()],
            },
            RowIdSchema::PassThrough,
        ];
        assert!(RowIdSchema::verify_pipeline(&schemas).is_ok());
    }

    #[test]
    fn test_row_id_schema_verify_pipeline_incompatible() {
        let schemas = vec![
            RowIdSchema::GroupByKey {
                columns: vec!["region".to_string()],
            },
            RowIdSchema::PrimaryKey {
                table: "t".to_string(),
                columns: vec!["id".to_string()],
            },
        ];
        assert!(RowIdSchema::verify_pipeline(&schemas).is_err());
    }

    #[test]
    fn test_row_id_schema_verify_pipeline_empty() {
        assert!(RowIdSchema::verify_pipeline(&[]).is_ok());
    }
}
