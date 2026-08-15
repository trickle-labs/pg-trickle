//! HAVING aggregate rewrites for the DVM parser.
//!
//! Extracted from `sublinks.rs` as part of QUAL-4 (v0.61.0) module decomposition.
//! Contains `rewrite_having_expr` and `is_star_only`.

use super::*;

/// Rewrite aggregate function calls in a HAVING predicate to their output column aliases.
///
/// The aggregate CTE already has the final aggregate values computed under the alias
/// names from the SELECT list (e.g., `SUM(amount) AS total` → column `total`).  When
/// the HAVING clause references `SUM(amount)`, we rewrite it to `total` so the
/// generated Filter SQL references the already-computed column instead of trying to
/// re-invoke the aggregate function.
pub(crate) fn rewrite_having_expr(expr: &Expr, aggregates: &[AggExpr]) -> Expr {
    match expr {
        Expr::FuncCall { func_name, args } => {
            let name_lower = func_name.to_lowercase();
            for agg in aggregates {
                let agg_name = agg.function.sql_name().to_lowercase();
                if name_lower != agg_name {
                    continue;
                }
                // Match by argument SQL representation.
                // COUNT(*) is represented as FuncCall { args: [Raw("*")] } when parsed
                // from a HAVING clause, but as argument = None in the target-list AggExpr.
                let args_match = match &agg.argument {
                    None => {
                        args.is_empty()
                            || (args.len() == 1 && matches!(&args[0], Expr::Raw(s) if s == "*"))
                    }
                    Some(agg_arg) => args.len() == 1 && args[0].to_sql() == agg_arg.to_sql(),
                };
                if args_match {
                    return Expr::ColumnRef {
                        table_alias: None,
                        column_name: agg.alias.clone(),
                    };
                }
            }
            // No match – keep as-is but recurse into args.
            Expr::FuncCall {
                func_name: func_name.clone(),
                args: args
                    .iter()
                    .map(|a| rewrite_having_expr(a, aggregates))
                    .collect(),
            }
        }
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op: op.clone(),
            left: Box::new(rewrite_having_expr(left, aggregates)),
            right: Box::new(rewrite_having_expr(right, aggregates)),
        },
        _ => expr.clone(),
    }
}

/// Check if expressions are just `*` (select all).
pub(crate) fn is_star_only(exprs: &[Expr]) -> bool {
    exprs.len() == 1 && matches!(exprs[0], Expr::Star { table_alias: None })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::parser::types::{AggExpr, AggFunc};

    fn make_count_star_agg(alias: &str) -> AggExpr {
        AggExpr {
            function: AggFunc::CountStar,
            argument: None,
            alias: alias.to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        }
    }

    fn make_sum_agg(col: &str, alias: &str) -> AggExpr {
        AggExpr {
            function: AggFunc::Sum,
            argument: Some(Expr::ColumnRef {
                table_alias: None,
                column_name: col.to_string(),
            }),
            alias: alias.to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        }
    }

    // ── is_star_only ─────────────────────────────────────────────────────

    #[test]
    fn test_is_star_only_single_star() {
        assert!(is_star_only(&[Expr::Star { table_alias: None }]));
    }

    #[test]
    fn test_is_star_only_qualified_star_is_not_star_only() {
        assert!(!is_star_only(&[Expr::Star {
            table_alias: Some("t".to_string()),
        }]));
    }

    #[test]
    fn test_is_star_only_empty_exprs() {
        assert!(!is_star_only(&[]));
    }

    #[test]
    fn test_is_star_only_multiple_exprs() {
        assert!(!is_star_only(&[
            Expr::Star { table_alias: None },
            Expr::ColumnRef {
                table_alias: None,
                column_name: "id".to_string(),
            },
        ]));
    }

    #[test]
    fn test_is_star_only_non_star_expr() {
        assert!(!is_star_only(&[Expr::ColumnRef {
            table_alias: None,
            column_name: "id".to_string(),
        }]));
    }

    // ── rewrite_having_expr ──────────────────────────────────────────────

    #[test]
    fn test_rewrite_having_expr_count_star_rewritten() {
        let aggs = vec![make_count_star_agg("__pgt_cnt")];
        let expr = Expr::FuncCall {
            func_name: "count".to_string(),
            args: vec![Expr::Raw("*".to_string())],
        };
        let result = rewrite_having_expr(&expr, &aggs);
        match result {
            Expr::ColumnRef {
                table_alias,
                column_name,
            } => {
                assert!(table_alias.is_none());
                assert_eq!(column_name, "__pgt_cnt");
            }
            other => panic!("expected ColumnRef, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_having_expr_sum_with_arg_rewritten() {
        let aggs = vec![make_sum_agg("amount", "__pgt_total")];
        let expr = Expr::FuncCall {
            func_name: "sum".to_string(),
            args: vec![Expr::ColumnRef {
                table_alias: None,
                column_name: "amount".to_string(),
            }],
        };
        let result = rewrite_having_expr(&expr, &aggs);
        match result {
            Expr::ColumnRef {
                table_alias,
                column_name,
            } => {
                assert!(table_alias.is_none());
                assert_eq!(column_name, "__pgt_total");
            }
            other => panic!("expected ColumnRef, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_having_expr_no_match_unchanged() {
        let aggs = vec![make_count_star_agg("__pgt_cnt")];
        let expr = Expr::FuncCall {
            func_name: "lower".to_string(),
            args: vec![Expr::ColumnRef {
                table_alias: None,
                column_name: "name".to_string(),
            }],
        };
        let result = rewrite_having_expr(&expr, &aggs);
        match result {
            Expr::FuncCall { func_name, args } => {
                assert_eq!(func_name, "lower");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected FuncCall, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_having_expr_binary_op_recurses() {
        let aggs = vec![make_count_star_agg("__pgt_cnt")];
        let expr = Expr::BinaryOp {
            op: ">".to_string(),
            left: Box::new(Expr::FuncCall {
                func_name: "count".to_string(),
                args: vec![Expr::Raw("*".to_string())],
            }),
            right: Box::new(Expr::Literal("10".to_string())),
        };
        let result = rewrite_having_expr(&expr, &aggs);
        match result {
            Expr::BinaryOp { op, left, .. } => {
                assert_eq!(op, ">");
                match *left {
                    Expr::ColumnRef { column_name, .. } => {
                        assert_eq!(column_name, "__pgt_cnt");
                    }
                    other => panic!("expected ColumnRef inside BinaryOp.left, got {other:?}"),
                }
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn test_rewrite_having_expr_literal_passes_through() {
        let aggs = vec![make_count_star_agg("__pgt_cnt")];
        let expr = Expr::Literal("42".to_string());
        match rewrite_having_expr(&expr, &aggs) {
            Expr::Literal(v) => assert_eq!(v, "42"),
            other => panic!("expected Literal, got {other:?}"),
        }
    }
}
